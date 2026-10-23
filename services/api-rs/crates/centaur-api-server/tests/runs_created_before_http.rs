//! Story: `GET /api/workflows/runs?created_before=<RFC 3339>` lists only runs
//! whose `created_at` is strictly before that instant, newest first, filtered in
//! SQL per queue before `limit`, combined with `limit`, `workflow_name`, `state` and `created_after`; an
//! invalid value is a 400 naming `created_before` and RFC 3339.
//!
//! Everything goes through the HTTP route of a router built with
//! `build_router_with_session_and_workflow_runtime`; no type added by the story
//! is used. Runs are seeded straight into the Absurd queue tables at known times.

use std::sync::Arc;

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

const QUEUES: [&str; 4] = [
    WORKFLOW_QUEUE,
    WORKFLOW_SLACK_LIVE_QUEUE,
    WORKFLOW_ETL_QUEUE,
    WORKFLOW_ETL_BACKFILL_QUEUE,
];
const FAR_FUTURE: &str = "2099-01-01T00:00:00Z";
const BASE: &str = "2024-01-01T00:00:00Z";

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
        unreachable!("runs created before tests never open sandbox I/O")
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
        sqlx::PgPool::connect_lazy("postgres://postgres:postgres@127.0.0.1:1/runs_created_before")
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

fn run_ids(runs: &[Value]) -> Vec<String> {
    runs.iter()
        .map(|run| run["run_id"].as_str().expect("run_id").to_owned())
        .collect()
}

async fn assert_invalid_created_before_is_400(router: &Router, query: &str) {
    let uri = format!("/api/workflows/runs?{query}");
    let (status, body) = get(router, &uri).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "GET {uri}: {body}");
    assert!(
        body.contains("created_before"),
        "GET {uri}: 400 message should name `created_before`: {body}"
    );
    assert!(
        body.contains("RFC 3339"),
        "GET {uri}: 400 message should name RFC 3339: {body}"
    );
}

/// Inserts a one-attempt task in `queue` whose `created_at` (the task's
/// `enqueue_at`) is `BASE + minute` minutes, and returns its last run id.
/// Pending and sleeping runs get a far-future `available_at` so the live
/// workers of any `WorkflowRuntime` sharing the database cannot claim them.
async fn seed(
    pool: &sqlx::PgPool,
    queue: &str,
    workflow_name: &str,
    state: &str,
    minute: i32,
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
                 '{BASE}'::timestamptz + make_interval(mins => $3),
                 case when $4 then '{BASE}'::timestamptz + make_interval(mins => $3) end,
                 $5, 1, $6::uuid, $7::jsonb,
                 case when $5 = 'cancelled'
                      then '{BASE}'::timestamptz + make_interval(mins => $3) end)"
    ))
    .bind(&task_id)
    .bind(json!({"workflow_name": workflow_name, "input": {}, "harness_type": "codex"}))
    .bind(minute)
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
                 case when $3 in ('pending', 'sleeping') then $10::timestamptz
                      else '{BASE}'::timestamptz + make_interval(mins => $6) end,
                 case when $7 then '{BASE}'::timestamptz + make_interval(mins => $6) end,
                 case when $3 in ('completed', 'cancelled') then now() end,
                 case when $3 = 'failed' then now() end,
                 $8::jsonb, $9::jsonb, '{BASE}'::timestamptz + make_interval(mins => $6))"
    ))
    .bind(&run_id)
    .bind(&task_id)
    .bind(state)
    .bind((state == "running").then_some("runs-created-after-test"))
    .bind((state == "running").then_some(FAR_FUTURE))
    .bind(minute)
    .bind(started)
    .bind((state == "completed").then(|| json!({"ok": true})))
    .bind((state == "failed").then(|| json!({"name": "Error", "message": "boom"})))
    .bind(FAR_FUTURE)
    .execute(pool)
    .await
    .expect("insert run");
    run_id
}

fn unique_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{prefix}_{nanos}_{}_{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// RFC 3339 instant `BASE + minute` minutes in UTC, as a query value.
fn at_utc(minute: i32) -> String {
    let total = minute.rem_euclid(60);
    let hours = minute.div_euclid(60);
    format!("2024-01-01T{hours:02}:{total:02}:00Z")
}

fn query_value(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

/// Seeds `per_queue` completed runs on every queue under one fresh workflow
/// name, run `k` of queue `i` at minute `k * 4 + i` (so every run has its own
/// minute, interleaved across queues). Returns `(workflow_name, runs by minute)`.
async fn seed_interleaved(pool: &sqlx::PgPool, per_queue: i32) -> (String, Vec<(i32, String)>) {
    let workflow_name = unique_name("runs_created_before");
    let mut runs = Vec::new();
    for k in 0..per_queue {
        for (i, queue) in QUEUES.into_iter().enumerate() {
            let minute = k * 4 + i as i32;
            runs.push((
                minute,
                seed(pool, queue, &workflow_name, "completed", minute).await,
            ));
        }
    }
    (workflow_name, runs)
}

/// Ids of the runs strictly before `bound_minute`, newest first.
fn before(runs: &[(i32, String)], bound_minute: i32) -> Vec<String> {
    between(runs, i32::MIN, bound_minute)
}

/// Ids of the runs strictly between `after_minute` and `before_minute`, newest first.
fn between(runs: &[(i32, String)], after_minute: i32, before_minute: i32) -> Vec<String> {
    let mut hits: Vec<&(i32, String)> = runs
        .iter()
        .filter(|(m, _)| *m > after_minute && *m < before_minute)
        .collect();
    hits.sort_by_key(|a| std::cmp::Reverse(a.0));
    hits.into_iter().map(|(_, id)| id.clone()).collect()
}

macro_rules! database_or_skip {
    () => {{
        let Some(url) = database_url() else {
            eprintln!("skipping runs created before tests: set SESSION_SQLX_TEST_DATABASE_URL");
            return;
        };
        url
    }};
}

#[tokio::test]
async fn runs_created_before_garbage_is_400_naming_created_before_and_rfc_3339() {
    assert_invalid_created_before_is_400(
        &router_without_workflow_runtime(),
        "created_before=garbage",
    )
    .await;
}

#[tokio::test]
async fn runs_created_before_empty_is_400_naming_created_before_and_rfc_3339() {
    assert_invalid_created_before_is_400(&router_without_workflow_runtime(), "created_before=")
        .await;
}

#[tokio::test]
async fn runs_created_before_date_only_is_400_naming_created_before_and_rfc_3339() {
    assert_invalid_created_before_is_400(
        &router_without_workflow_runtime(),
        "created_before=2024-01-01",
    )
    .await;
}

#[tokio::test]
async fn runs_created_before_without_offset_is_400_naming_created_before_and_rfc_3339() {
    assert_invalid_created_before_is_400(
        &router_without_workflow_runtime(),
        "created_before=2024-01-01T00:00:00",
    )
    .await;
}

#[tokio::test]
async fn runs_created_before_invalid_is_400_even_with_other_filters() {
    assert_invalid_created_before_is_400(
        &router_without_workflow_runtime(),
        "limit=5&workflow_name=anything&state=running&created_after=2024-01-01T00:00:00Z&created_before=yesterday",
    )
    .await;
}

#[tokio::test]
async fn runs_created_before_returns_only_strictly_earlier_runs_newest_first() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 4).await;
    for bound in [1, 5, 14, 15, 16] {
        let listed = list(
            &harness.router,
            &format!(
                "limit=100&workflow_name={workflow_name}&created_before={}",
                query_value(&at_utc(bound))
            ),
        )
        .await;
        assert_eq!(
            run_ids(&listed),
            before(&runs, bound),
            "bound minute {bound}"
        );
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_created_before_excludes_a_run_created_exactly_at_the_bound() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 3).await;
    let (_, at_bound) = runs.iter().find(|(m, _)| *m == 6).unwrap();
    let listed = list(
        &harness.router,
        &format!(
            "limit=100&workflow_name={workflow_name}&created_before={}",
            at_utc(6)
        ),
    )
    .await;
    assert!(
        !run_ids(&listed).contains(at_bound),
        "run created exactly at the bound must be excluded: {listed:?}"
    );
    assert_eq!(run_ids(&listed), before(&runs, 6));

    let just_after = list(
        &harness.router,
        &format!("limit=100&workflow_name={workflow_name}&created_before=2024-01-01T00:06:00.001Z"),
    )
    .await;
    assert!(
        run_ids(&just_after).contains(at_bound),
        "a bound just after the run includes it: {just_after:?}"
    );
    assert_eq!(run_ids(&just_after), before(&runs, 7));
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_created_before_compares_offsets_as_instants() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 4).await;
    let expected = before(&runs, 10);
    assert!(!expected.is_empty() && expected.len() < runs.len());
    // 2024-01-01T00:10:00Z written with other offsets, `+` percent-encoded.
    for bound in [
        "2024-01-01T00:10:00Z",
        "2024-01-01T00:10:00+00:00",
        "2024-01-01T02:10:00+02:00",
        "2023-12-31T19:10:00-05:00",
        "2024-01-01T05:40:00+05:30",
        "2024-01-01T00:10:00.000000Z",
    ] {
        let listed = list(
            &harness.router,
            &format!(
                "limit=100&workflow_name={workflow_name}&created_before={}",
                query_value(bound)
            ),
        )
        .await;
        assert_eq!(run_ids(&listed), expected, "created_before={bound}");
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_created_before_a_bound_earlier_than_every_run_is_200_and_empty() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 3).await;
    assert_eq!(runs.len(), 12);
    let uri = format!(
        "/api/workflows/runs?limit=100&workflow_name={workflow_name}&created_before={}",
        query_value("2023-12-31T00:00:00Z")
    );
    let (status, body) = get(&harness.router, &uri).await;
    assert_eq!(status, StatusCode::OK, "GET {uri}: {body}");
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["runs"], json!([]), "no run is earlier than the bound");

    let epoch = list(
        &harness.router,
        &format!(
            "limit=100&created_before={}",
            query_value("1970-01-01T00:00:00Z")
        ),
    )
    .await;
    assert!(
        epoch.is_empty(),
        "nothing is created before 1970: {epoch:?}"
    );
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_created_before_limit_counts_matching_runs_across_all_queues() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 4).await;
    let bound = at_utc(13);
    let earlier = before(&runs, 13);
    assert_eq!(earlier.len(), 13);

    let limited = list(
        &harness.router,
        &format!("limit=5&workflow_name={workflow_name}&created_before={bound}"),
    )
    .await;
    assert_eq!(
        run_ids(&limited),
        earlier[..5].to_vec(),
        "newest 5 before the bound"
    );

    let exact = list(
        &harness.router,
        &format!("limit=13&workflow_name={workflow_name}&created_before={bound}"),
    )
    .await;
    assert_eq!(run_ids(&exact), earlier);

    let roomy = list(
        &harness.router,
        &format!("limit=100&workflow_name={workflow_name}&created_before={bound}"),
    )
    .await;
    assert_eq!(
        run_ids(&roomy),
        earlier,
        "fewer matches than limit returns them all"
    );
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_created_before_combines_with_workflow_name_and_state() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_created_before");
    let other_name = unique_name("runs_created_before_other");
    // (queue, workflow, state, minute) with the id of each seeded run.
    let mut seeded: Vec<(&str, &str, &str, i32, String)> = Vec::new();
    for (i, queue) in QUEUES.into_iter().enumerate() {
        for (name, state, minute) in [
            (&workflow_name, "running", 2 + i as i32),
            (&workflow_name, "running", 20 + i as i32),
            (&workflow_name, "failed", 6 + i as i32),
            (&workflow_name, "failed", 30 + i as i32),
            (&other_name, "running", 8 + i as i32),
        ] {
            let id = seed(&harness.pool, queue, name, state, minute).await;
            seeded.push((queue, name.as_str(), state, minute, id));
        }
    }
    let expect = |name: &str, state: Option<&str>, bound: i32| -> Vec<String> {
        let mut rows: Vec<_> = seeded
            .iter()
            .filter(|(_, n, s, m, _)| *n == name && state.is_none_or(|st| st == *s) && *m < bound)
            .collect();
        rows.sort_by_key(|a| std::cmp::Reverse(a.3));
        rows.into_iter().map(|r| r.4.clone()).collect()
    };
    let bound = at_utc(10);

    let running = list(
        &harness.router,
        &format!("state=running&limit=100&workflow_name={workflow_name}&created_before={bound}"),
    )
    .await;
    assert_eq!(
        run_ids(&running),
        expect(&workflow_name, Some("running"), 10)
    );
    assert_eq!(running.len(), 4);
    for run in &running {
        assert_eq!(run["status"], json!("running"));
        assert_eq!(run["workflow_name"], json!(workflow_name));
    }

    let failed = list(
        &harness.router,
        &format!("state=failed&limit=100&workflow_name={workflow_name}&created_before={bound}"),
    )
    .await;
    assert_eq!(run_ids(&failed), expect(&workflow_name, Some("failed"), 10));
    assert_eq!(failed.len(), 4);

    let any_state = list(
        &harness.router,
        &format!("limit=100&workflow_name={workflow_name}&created_before={bound}"),
    )
    .await;
    assert_eq!(run_ids(&any_state), expect(&workflow_name, None, 10));
    assert_eq!(any_state.len(), 8);

    let other = list(
        &harness.router,
        &format!("limit=100&workflow_name={other_name}&created_before={bound}"),
    )
    .await;
    assert_eq!(run_ids(&other), expect(&other_name, None, 10));
    assert_eq!(other.len(), 2);

    let limited = list(
        &harness.router,
        &format!("state=running&limit=3&workflow_name={workflow_name}&created_before={bound}"),
    )
    .await;
    assert_eq!(
        run_ids(&limited),
        expect(&workflow_name, Some("running"), 10)[..3].to_vec()
    );

    let none = list(
        &harness.router,
        &format!("state=cancelled&limit=100&workflow_name={workflow_name}&created_before={bound}"),
    )
    .await;
    assert!(none.is_empty(), "no cancelled runs: {none:?}");
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_created_before_combines_with_created_after_as_a_strict_window() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 4).await;
    let window = between(&runs, 3, 12);
    assert_eq!(window.len(), 8);

    let listed = list(
        &harness.router,
        &format!(
            "limit=100&workflow_name={workflow_name}&created_after={}&created_before={}",
            at_utc(3),
            at_utc(12)
        ),
    )
    .await;
    assert_eq!(run_ids(&listed), window, "only runs strictly between");

    let limited = list(
        &harness.router,
        &format!(
            "limit=3&workflow_name={workflow_name}&created_after={}&created_before={}",
            at_utc(3),
            at_utc(12)
        ),
    )
    .await;
    assert_eq!(run_ids(&limited), window[..3].to_vec());

    let inverted = list(
        &harness.router,
        &format!(
            "limit=100&workflow_name={workflow_name}&created_after={}&created_before={}",
            at_utc(12),
            at_utc(3)
        ),
    )
    .await;
    assert!(
        inverted.is_empty(),
        "inverted window is empty: {inverted:?}"
    );

    let touching = list(
        &harness.router,
        &format!(
            "limit=100&workflow_name={workflow_name}&created_after={}&created_before={}",
            at_utc(6),
            at_utc(6)
        ),
    )
    .await;
    assert!(touching.is_empty(), "empty window: {touching:?}");
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_created_before_omitting_it_returns_every_run_as_before() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 4).await;
    let listed = list(
        &harness.router,
        &format!("limit=100&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(
        run_ids(&listed),
        before(&runs, i32::MAX),
        "all runs, newest first"
    );
    assert_eq!(listed.len(), 16);
    harness.pool.close().await;
}
