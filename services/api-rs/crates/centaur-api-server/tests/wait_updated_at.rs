//! Story: a workflow run waiting on an event with no timeout (stored by Absurd
//! with `available_at = 'infinity'`) lists and fetches with a finite
//! `updated_at` through `list_runs`, `get_run`, and the `/api/workflows/runs`
//! routes.
//!
//! Runs are seeded straight into the Absurd queue tables, as `run_timestamps.rs`
//! does, and read back through existing entry points only.

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
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tower::ServiceExt;


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
        unreachable!("wait updated_at tests never open sandbox I/O")
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

struct Harness {
    runtime: WorkflowRuntime,
    router: Router,
    pool: sqlx::PgPool,
}

async fn harness(url: &str) -> Harness {
    let pool = sqlx::PgPool::connect(url).await.expect("connect database");
    install_absurd_schema(&pool).await;
    let store = PgSessionStore::new(pool.clone());
    let iron = || IronControlClient::new("http://127.0.0.1:1", "test-key");
    let session_runtime = SessionRuntime::new(
        store.clone(),
        SandboxRuntime::backend(Arc::new(NoSandboxBackend), SandboxSpec::new("test")),
        SessionRegistrar::new(iron()),
    );
    let runtime = WorkflowRuntime::new(
        store,
        session_runtime.clone(),
        None,
        WorkflowPrincipalRegistrar::new(iron()),
    )
    .await
    .expect("build workflow runtime");
    let router = build_router_with_session_and_workflow_runtime(
        session_runtime,
        Some(runtime.clone()),
        ApiAuthConfig::testing("test-secret"),
    );
    Harness {
        runtime,
        router,
        pool,
    }
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

const ENQUEUED: &str = "2026-03-01T09:59:00Z";
const ATTEMPT_CREATED: &str = "2026-03-01T09:59:00Z";
const STARTED: &str = "2026-03-01T10:00:01.123456Z";
const NEVER_STARTED_CREATED: &str = "2026-03-01T10:02:30.250000Z";
const LATE_ENQUEUED: &str = "2026-03-01T10:30:00.500000Z";
const COMPLETED: &str = "2026-03-01T10:05:09.654321Z";
const FAR_FUTURE: &str = "2099-01-01T00:00:00Z";

#[derive(Clone)]
struct Seed {
    queue: &'static str,
    task_state: &'static str,
    run_state: &'static str,
    enqueue_at: &'static str,
    available_at: &'static str,
    first_started_at: Option<&'static str>,
    run_started_at: Option<&'static str>,
    run_created_at: &'static str,
    completed_at: Option<&'static str>,
    waiting: bool,
}

impl Seed {
    fn waiting(queue: &'static str) -> Self {
        Seed {
            queue,
            task_state: "sleeping",
            run_state: "sleeping",
            enqueue_at: ENQUEUED,
            available_at: "infinity",
            first_started_at: Some(STARTED),
            run_started_at: Some(STARTED),
            run_created_at: ATTEMPT_CREATED,
            completed_at: None,
            waiting: true,
        }
    }

    fn pending(queue: &'static str) -> Self {
        Seed {
            task_state: "pending",
            run_state: "pending",
            available_at: FAR_FUTURE,
            first_started_at: None,
            run_started_at: None,
            waiting: false,
            ..Seed::waiting(queue)
        }
    }

    fn completed(queue: &'static str) -> Self {
        Seed {
            task_state: "completed",
            run_state: "completed",
            available_at: ENQUEUED,
            completed_at: Some(COMPLETED),
            waiting: false,
            ..Seed::waiting(queue)
        }
    }
}

struct Seeded {
    run_id: String,
    expected_updated_at: &'static str,
    seed: Seed,
}

fn expected_updated_at(seed: &Seed) -> &'static str {
    if seed.waiting {
        let attempt = seed.run_started_at.unwrap_or(seed.run_created_at);
        let enqueue = OffsetDateTime::parse(seed.enqueue_at, &Rfc3339).unwrap();
        if OffsetDateTime::parse(attempt, &Rfc3339).unwrap() >= enqueue {
            attempt
        } else {
            seed.enqueue_at
        }
    } else {
        // Finite `available_at` keeps today's greatest(enqueue_at, available_at).
        seed.available_at
    }
}

async fn seed_run(pool: &sqlx::PgPool, workflow_name: &str, seed: Seed) -> Seeded {
    let task_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .unwrap();
    let run_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .unwrap();
    let queue = seed.queue;
    sqlx::query(&format!(
        "insert into absurd.t_{queue}
            (task_id, task_name, params, max_attempts, enqueue_at, first_started_at,
             state, attempts, last_attempt_run, completed_payload)
         values ($1::uuid, 'centaur.workflow', $2::jsonb, 3, $3::timestamptz,
                 $4::timestamptz, $5, 1, $6::uuid, $7::jsonb)"
    ))
    .bind(&task_id)
    .bind(json!({"workflow_name": workflow_name, "input": {}, "harness_type": "codex"}))
    .bind(seed.enqueue_at)
    .bind(seed.first_started_at)
    .bind(seed.task_state)
    .bind(&run_id)
    .bind((seed.task_state == "completed").then(|| json!({"ok": true})))
    .execute(pool)
    .await
    .expect("insert task");
    sqlx::query(&format!(
        "insert into absurd.r_{queue}
            (run_id, task_id, attempt, state, available_at, wake_event, started_at,
             completed_at, result, created_at)
         values ($1::uuid, $2::uuid, 1, $3, $4::timestamptz, $5, $6::timestamptz,
                 $7::timestamptz, $8::jsonb, $9::timestamptz)"
    ))
    .bind(&run_id)
    .bind(&task_id)
    .bind(seed.run_state)
    .bind(seed.available_at)
    .bind(seed.waiting.then_some("wait-updated-at-test-event"))
    .bind(seed.run_started_at)
    .bind(seed.completed_at)
    .bind((seed.run_state == "completed").then(|| json!({"ok": true})))
    .bind(seed.run_created_at)
    .execute(pool)
    .await
    .expect("insert run");
    Seeded {
        run_id,
        expected_updated_at: expected_updated_at(&seed),
        seed,
    }
}

async fn new_workflow_name(pool: &sqlx::PgPool) -> String {
    format!(
        "wait_updated_at_{}",
        sqlx::query_scalar::<_, String>("select replace(gen_random_uuid()::text, '-', '')")
            .fetch_one(pool)
            .await
            .unwrap()
    )
}

async fn http_json(router: &Router, uri: &str) -> Value {
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
    assert_eq!(response.status(), StatusCode::OK, "GET {uri}");
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn assert_updated_at(run: &Value, expected: &str, context: &str) {
    let text = run["updated_at"]
        .as_str()
        .unwrap_or_else(|| panic!("{context}: updated_at should be a string: {run}"));
    let actual = OffsetDateTime::parse(text, &Rfc3339)
        .unwrap_or_else(|error| panic!("{context}: updated_at {text:?} not RFC 3339: {error}"));
    let expected = OffsetDateTime::parse(expected, &Rfc3339).unwrap();
    assert_eq!(actual, expected, "{context}: updated_at");
}

fn find_run<'a>(runs: &'a [Value], run_id: &str, context: &str) -> &'a Value {
    runs.iter()
        .find(|run| run["run_id"] == json!(run_id))
        .unwrap_or_else(|| panic!("{context}: run {run_id} missing from list"))
}

/// Seeds `waiting` next to a pending and a completed run (same queue, same
/// workflow name), then checks every read surface for every seeded run.
async fn assert_waiting_run(waiting: Seed) {
    let Some(url) = database_url() else {
        eprintln!("skipping wait updated_at tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = new_workflow_name(&harness.pool).await;
    let queue = waiting.queue;
    let seeded = vec![
        seed_run(&harness.pool, &workflow_name, Seed::pending(queue)).await,
        seed_run(&harness.pool, &workflow_name, waiting).await,
        seed_run(&harness.pool, &workflow_name, Seed::completed(queue)).await,
    ];

    let listed = harness
        .runtime
        .list_runs(100, Some(&workflow_name))
        .await
        .expect("list_runs must succeed with a no-timeout waiting run present");
    let listed: Vec<Value> = listed
        .into_iter()
        .map(|run| serde_json::to_value(run).unwrap())
        .collect();
    assert_eq!(listed.len(), seeded.len(), "list_runs returned every run");

    let http_list = http_json(
        &harness.router,
        &format!("/api/workflows/runs?limit=100&workflow_name={workflow_name}"),
    )
    .await;
    let http_list = http_list["runs"].as_array().expect("runs array").clone();
    assert_eq!(http_list.len(), seeded.len(), "HTTP list returned every run");

    for item in &seeded {
        let context = format!("{} {}", item.seed.queue, item.seed.run_state);
        let fetched =
            serde_json::to_value(harness.runtime.get_run(&item.run_id).await.expect("get_run"))
                .unwrap();
        let http_get = http_json(
            &harness.router,
            &format!("/api/workflows/runs/{}", item.run_id),
        )
        .await["run"]
            .clone();
        for (surface, run) in [
            ("list_runs", find_run(&listed, &item.run_id, &context)),
            ("get_run", &fetched),
            (
                "GET /api/workflows/runs",
                find_run(&http_list, &item.run_id, &context),
            ),
            ("GET /api/workflows/runs/{run_id}", &http_get),
        ] {
            let context = format!("{context} via {surface}");
            assert_eq!(run["status"], json!(item.seed.run_state), "{context}");
            assert_updated_at(run, item.expected_updated_at, &context);
        }
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn wait_updated_at_waiting_run_reports_attempt_start_as_updated_at() {
    assert_waiting_run(Seed::waiting(WORKFLOW_QUEUE)).await;
}

#[tokio::test]
async fn wait_updated_at_waiting_run_never_started_reports_attempt_created_at() {
    assert_waiting_run(Seed {
        first_started_at: None,
        run_started_at: None,
        run_created_at: NEVER_STARTED_CREATED,
        ..Seed::waiting(WORKFLOW_QUEUE)
    })
    .await;
}

#[tokio::test]
async fn wait_updated_at_waiting_run_enqueued_after_attempt_reports_enqueue_at() {
    assert_waiting_run(Seed {
        enqueue_at: LATE_ENQUEUED,
        ..Seed::waiting(WORKFLOW_QUEUE)
    })
    .await;
}

#[tokio::test]
async fn wait_updated_at_slack_live_queue_waiting_run_has_finite_updated_at() {
    assert_waiting_run(Seed::waiting(WORKFLOW_SLACK_LIVE_QUEUE)).await;
}

#[tokio::test]
async fn wait_updated_at_etl_queue_waiting_run_has_finite_updated_at() {
    assert_waiting_run(Seed::waiting(WORKFLOW_ETL_QUEUE)).await;
}

#[tokio::test]
async fn wait_updated_at_etl_backfill_queue_waiting_run_has_finite_updated_at() {
    assert_waiting_run(Seed::waiting(WORKFLOW_ETL_BACKFILL_QUEUE)).await;
}
