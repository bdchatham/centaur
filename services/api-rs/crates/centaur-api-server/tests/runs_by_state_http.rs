//! Story: `GET /api/workflows/runs?state=<state>` lists only runs in that state,
//! filtered in SQL per queue before `limit`, combined with `workflow_name`, and
//! an unknown state is a 400 naming the six valid states.
//!
//! Everything goes through the HTTP route of a router built with
//! `build_router_with_session_and_workflow_runtime`; no type added by the story
//! is used. Runs are seeded straight into the Absurd queue tables.

use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use centaur_api_server::{
    ApiAuthConfig, SandboxRuntime, SessionRuntime, build_router_with_session_and_workflow_runtime,
};
use centaur_iron_control::{IronControlClient, SessionRegistrar};
use centaur_sandbox_core::{
    ObservedSandbox, SandboxBackend, SandboxError, SandboxHandle, SandboxId, SandboxIo,
    SandboxResult, SandboxSpec, SandboxStatus,
};
use centaur_session_sqlx::PgSessionStore;
use centaur_workflows::{
    WORKFLOW_ETL_BACKFILL_QUEUE, WORKFLOW_ETL_QUEUE, WORKFLOW_QUEUE, WORKFLOW_SLACK_LIVE_QUEUE,
    WorkflowPrincipalRegistrar, WorkflowRuntime,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::{Value, json};
use tower::ServiceExt;

const STATES: [&str; 6] = [
    "pending",
    "running",
    "sleeping",
    "completed",
    "failed",
    "cancelled",
];
const QUEUES: [&str; 4] = [
    WORKFLOW_QUEUE,
    WORKFLOW_SLACK_LIVE_QUEUE,
    WORKFLOW_ETL_QUEUE,
    WORKFLOW_ETL_BACKFILL_QUEUE,
];
const FAR_FUTURE: &str = "2099-01-01T00:00:00Z";

static SETUP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Default)]
struct NoSandboxBackend;

#[async_trait]
impl SandboxBackend for NoSandboxBackend {
    fn name(&self) -> &'static str {
        "test"
    }

    async fn create(&self, _spec: SandboxSpec) -> SandboxResult<SandboxHandle> {
        Ok(SandboxHandle::new(SandboxId::new("test-1"), self.name()))
    }

    async fn open_io(&self, _id: &SandboxId) -> SandboxResult<SandboxIo> {
        unreachable!("runs by state tests never open sandbox I/O")
    }

    async fn status(&self, _id: &SandboxId) -> SandboxResult<SandboxStatus> {
        Ok(SandboxStatus::Running)
    }

    async fn observe(&self, id: &SandboxId) -> SandboxResult<ObservedSandbox> {
        Ok(ObservedSandbox::new(
            id.clone(),
            self.name(),
            SandboxStatus::Running,
        ))
    }

    async fn list_observed(&self) -> SandboxResult<Vec<ObservedSandbox>> {
        Ok(Vec::new())
    }

    async fn stop(&self, _id: &SandboxId) -> SandboxResult<()> {
        Ok(())
    }

    async fn pause(&self, _id: &SandboxId) -> SandboxResult<()> {
        Err(SandboxError::Unsupported {
            backend: self.name(),
            operation: "pause",
        })
    }

    async fn resume(&self, _id: &SandboxId) -> SandboxResult<()> {
        Err(SandboxError::Unsupported {
            backend: self.name(),
            operation: "resume",
        })
    }
}

fn database_url() -> Option<String> {
    std::env::var("SESSION_SQLX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("SESSION_RUNTIME_TEST_DATABASE_URL"))
        .or_else(|_| std::env::var("ABSURD_TEST_DATABASE_URL"))
        .ok()
}

async fn install_absurd_schema(pool: &sqlx::PgPool) {
    let mut conn = pool.acquire().await.expect("acquire connection");
    sqlx::query("select pg_advisory_lock(7_700_042)")
        .execute(&mut *conn)
        .await
        .expect("take schema lock");
    let installed: bool =
        sqlx::query_scalar("select exists(select 1 from pg_namespace where nspname = 'absurd')")
            .fetch_one(&mut *conn)
            .await
            .expect("check absurd schema");
    if !installed {
        sqlx::raw_sql(include_str!(
            "../../centaur-session-sqlx/migrations/0007_absurd_workflows.sql"
        ))
        .execute(&mut *conn)
        .await
        .expect("install absurd schema");
    }
    sqlx::query("select pg_advisory_unlock(7_700_042)")
        .execute(&mut *conn)
        .await
        .expect("release schema lock");
}

fn iron() -> IronControlClient {
    IronControlClient::new("http://127.0.0.1:1", "test-key")
}

fn session_runtime(pool: sqlx::PgPool) -> SessionRuntime {
    SessionRuntime::new(
        PgSessionStore::new(pool),
        SandboxRuntime::backend(Arc::new(NoSandboxBackend), SandboxSpec::new("test")),
        SessionRegistrar::new(iron()),
    )
}

/// Router with no workflow runtime and a pool that never connects.
fn router_without_workflow_runtime() -> Router {
    let pool =
        sqlx::PgPool::connect_lazy("postgres://postgres:postgres@127.0.0.1:1/runs_by_state")
            .expect("lazy pool");
    build_router_with_session_and_workflow_runtime(
        session_runtime(pool),
        None,
        ApiAuthConfig::testing("test-secret"),
    )
}

struct Harness {
    router: Router,
    pool: sqlx::PgPool,
}

async fn harness(url: &str) -> Harness {
    let pool = sqlx::PgPool::connect(url).await.expect("connect database");
    install_absurd_schema(&pool).await;
    let session_runtime = session_runtime(pool.clone());
    let runtime = WorkflowRuntime::new(
        PgSessionStore::new(pool.clone()),
        session_runtime.clone(),
        None,
        WorkflowPrincipalRegistrar::new(iron()),
    )
    .await
    .expect("build workflow runtime");
    let router = build_router_with_session_and_workflow_runtime(
        session_runtime,
        Some(runtime),
        ApiAuthConfig::testing("test-secret"),
    );
    Harness { router, pool }
}

fn console_token() -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &json!({
            "iss": "centaur-console",
            "sub": "centaur-console",
            "aud": "centaur-api",
            "iat": 1_700_000_000i64,
            "exp": 4_102_444_800i64,
            "token_use": "console_service",
        }),
        &EncodingKey::from_secret(b"test-secret"),
    )
    .unwrap()
}

async fn get(router: &Router, uri: &str) -> (StatusCode, String) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {}", console_token()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

async fn list(router: &Router, query: &str) -> Vec<Value> {
    let uri = format!("/api/workflows/runs?{query}");
    let (status, body) = get(router, &uri).await;
    assert_eq!(status, StatusCode::OK, "GET {uri}: {body}");
    let body: Value = serde_json::from_str(&body).unwrap();
    body["runs"].as_array().expect("runs array").clone()
}

fn run_ids(runs: &[Value]) -> BTreeSet<String> {
    runs.iter()
        .map(|run| run["run_id"].as_str().expect("run_id").to_owned())
        .collect()
}

async fn assert_unknown_state_is_400(router: &Router, query: &str) {
    let uri = format!("/api/workflows/runs?{query}");
    let (status, body) = get(router, &uri).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "GET {uri}: {body}");
    for state in STATES {
        assert!(
            body.contains(state),
            "GET {uri}: 400 message should name `{state}`: {body}"
        );
    }
}

/// Inserts a one-attempt task in `queue` and returns its last run id. Larger
/// `age_minutes` means an older `enqueue_at`.
async fn seed(
    pool: &sqlx::PgPool,
    queue: &str,
    workflow_name: &str,
    state: &str,
    age_minutes: i32,
) -> String {
    let task_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .unwrap();
    let run_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .unwrap();
    let started = state != "pending";
    sqlx::query(&format!(
        "insert into absurd.t_{queue}
            (task_id, task_name, params, max_attempts, enqueue_at, first_started_at,
             state, attempts, last_attempt_run, completed_payload, cancelled_at)
         values ($1::uuid, 'centaur.workflow', $2::jsonb, 3,
                 now() - make_interval(mins => $3),
                 case when $4 then now() - make_interval(mins => $3) end,
                 $5, 1, $6::uuid, $7::jsonb,
                 case when $5 = 'cancelled' then now() - make_interval(mins => $3) end)"
    ))
    .bind(&task_id)
    .bind(json!({"workflow_name": workflow_name, "input": {}, "harness_type": "codex"}))
    .bind(age_minutes)
    .bind(started)
    .bind(state)
    .bind(&run_id)
    .bind((state == "completed").then(|| json!({"ok": true})))
    .execute(pool)
    .await
    .expect("insert task");
    sqlx::query(&format!(
        "insert into absurd.r_{queue}
            (run_id, task_id, attempt, state, claimed_by, claim_expires_at, available_at,
             started_at, completed_at, failed_at, result, failure_reason, created_at)
         values ($1::uuid, $2::uuid, 1, $3, $4, $5::timestamptz,
                 case when $3 = 'pending' then $5::timestamptz
                      else now() - make_interval(mins => $6) end,
                 case when $7 then now() - make_interval(mins => $6) end,
                 case when $3 in ('completed', 'cancelled') then now() end,
                 case when $3 = 'failed' then now() end,
                 $8::jsonb, $9::jsonb, now() - make_interval(mins => $6))"
    ))
    .bind(&run_id)
    .bind(&task_id)
    .bind(state)
    .bind((state == "running").then_some("runs-by-state-test"))
    .bind((state == "running").then_some(FAR_FUTURE))
    .bind(age_minutes)
    .bind(started)
    .bind((state == "completed").then(|| json!({"ok": true})))
    .bind((state == "failed").then(|| json!({"name": "Error", "message": "boom"})))
    .execute(pool)
    .await
    .expect("insert run");
    run_id
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", uuid_suffix())
}

fn uuid_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{nanos}_{}_{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Seeds one run per state on every queue under one fresh workflow name.
/// Returns `(workflow_name, state -> run ids across queues)`.
async fn seed_every_state_on_every_queue(
    pool: &sqlx::PgPool,
) -> (String, Vec<(&'static str, BTreeSet<String>)>) {
    let workflow_name = unique_name("runs_by_state");
    let mut by_state = Vec::new();
    for (index, state) in STATES.into_iter().enumerate() {
        let mut ids = BTreeSet::new();
        for queue in QUEUES {
            ids.insert(seed(pool, queue, &workflow_name, state, 10 + index as i32).await);
        }
        by_state.push((state, ids));
    }
    (workflow_name, by_state)
}

#[tokio::test]
async fn runs_by_state_unknown_state_is_400_naming_all_six_states() {
    assert_unknown_state_is_400(&router_without_workflow_runtime(), "state=bogus").await;
}

#[tokio::test]
async fn runs_by_state_empty_state_is_400_naming_all_six_states() {
    assert_unknown_state_is_400(&router_without_workflow_runtime(), "state=").await;
}

#[tokio::test]
async fn runs_by_state_wrongly_cased_state_is_400_naming_all_six_states() {
    assert_unknown_state_is_400(&router_without_workflow_runtime(), "state=Running").await;
}

#[tokio::test]
async fn runs_by_state_unknown_state_is_400_even_with_other_filters() {
    assert_unknown_state_is_400(
        &router_without_workflow_runtime(),
        "limit=5&workflow_name=anything&state=COMPLETED",
    )
    .await;
}

#[tokio::test]
async fn runs_by_state_each_state_returns_exactly_the_runs_in_that_state() {
    let Some(url) = database_url() else {
        eprintln!("skipping runs by state tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, by_state) = seed_every_state_on_every_queue(&harness.pool).await;
    for (state, expected) in &by_state {
        let runs = list(
            &harness.router,
            &format!("state={state}&limit=100&workflow_name={workflow_name}"),
        )
        .await;
        assert_eq!(&run_ids(&runs), expected, "state={state}");
        for run in &runs {
            assert_eq!(run["status"], json!(state), "state={state}: {run}");
        }
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_by_state_without_workflow_name_returns_only_that_state() {
    let Some(url) = database_url() else {
        eprintln!("skipping runs by state tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (_, by_state) = seed_every_state_on_every_queue(&harness.pool).await;
    for (state, expected) in &by_state {
        let runs = list(&harness.router, &format!("state={state}&limit=1000")).await;
        assert!(!runs.is_empty(), "state={state}: no runs returned");
        for run in &runs {
            assert_eq!(run["status"], json!(state), "state={state}: {run}");
        }
        let returned = run_ids(&runs);
        // Other tests share the database; seeded runs are recent enough to be
        // listed unless the limit cut them off, which it can only do for
        // other runs of the same state.
        assert!(
            expected.is_subset(&returned) || returned.len() == 1000,
            "state={state}: seeded runs missing from {returned:?}"
        );
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_by_state_combines_with_limit_and_workflow_name() {
    let Some(url) = database_url() else {
        eprintln!("skipping runs by state tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, by_state) = seed_every_state_on_every_queue(&harness.pool).await;
    let other_name = unique_name("runs_by_state_other");
    for queue in QUEUES {
        seed(&harness.pool, queue, &other_name, "running", 1).await;
    }
    let (_, running) = by_state.iter().find(|(s, _)| *s == "running").unwrap();

    let limited = list(
        &harness.router,
        &format!("state=running&limit=2&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(limited.len(), 2, "limit counts matching runs: {limited:?}");
    assert!(run_ids(&limited).is_subset(running));
    for run in &limited {
        assert_eq!(run["status"], json!("running"));
        assert_eq!(run["workflow_name"], json!(workflow_name));
    }

    let named = list(
        &harness.router,
        &format!("state=running&limit=100&workflow_name={other_name}"),
    )
    .await;
    assert_eq!(named.len(), 4, "one running run per queue: {named:?}");
    assert!(run_ids(&named).is_disjoint(running));
    for run in &named {
        assert_eq!(run["status"], json!("running"));
        assert_eq!(run["workflow_name"], json!(other_name));
    }

    let none = list(
        &harness.router,
        &format!("state=failed&limit=100&workflow_name={other_name}"),
    )
    .await;
    assert!(none.is_empty(), "no failed runs for {other_name}: {none:?}");
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_by_state_live_run_behind_more_than_limit_newer_finished_runs_is_returned() {
    let Some(url) = database_url() else {
        eprintln!("skipping runs by state tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    for queue in QUEUES {
        let workflow_name = unique_name("runs_by_state_buried");
        let live = seed(&harness.pool, queue, &workflow_name, "running", 600).await;
        for age in 1..=5 {
            seed(&harness.pool, queue, &workflow_name, "completed", age).await;
        }
        let runs = list(
            &harness.router,
            &format!("state=running&limit=2&workflow_name={workflow_name}"),
        )
        .await;
        assert_eq!(
            run_ids(&runs),
            BTreeSet::from([live]),
            "{queue}: live run hidden behind newer finished runs"
        );
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_by_state_omitting_state_returns_every_state_as_before() {
    let Some(url) = database_url() else {
        eprintln!("skipping runs by state tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, by_state) = seed_every_state_on_every_queue(&harness.pool).await;
    let all: BTreeSet<String> = by_state.iter().flat_map(|(_, ids)| ids.clone()).collect();
    let runs = list(
        &harness.router,
        &format!("limit=100&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(run_ids(&runs), all);
    let statuses: BTreeSet<&str> = runs.iter().map(|r| r["status"].as_str().unwrap()).collect();
    assert_eq!(statuses, BTreeSet::from(STATES));
    harness.pool.close().await;
}
