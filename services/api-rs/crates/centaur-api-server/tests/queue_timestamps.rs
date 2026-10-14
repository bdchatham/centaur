//! Story: workflow runs expose `started_at` and `completed_at` from
//! `list_runs`, `get_run`, and the `/api/workflows/runs` HTTP routes.
//!
//! Runs are seeded straight into the Absurd queue tables (so the runtime's own
//! workers never touch them) and read back through the existing runtime and
//! router entry points.
//!
//! One test per queue, each building a single harness for all of its
//! scenarios and closing its pool at the end, so the package never holds more
//! than a handful of database connections at once.

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

const STARTED: &str = "2026-03-01T10:00:01.123456Z";
const COMPLETED: &str = "2026-03-01T10:05:09.654321Z";
const FAILED: &str = "2026-03-01T10:07:30.250000Z";
const CANCELLED: &str = "2026-03-01T10:09:45.500000Z";
const RETRY_STARTED: &str = "2026-03-01T10:20:00.000001Z";
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
        unreachable!("run timestamp tests never open sandbox I/O")
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

#[derive(Clone, Default)]
struct RunSeed {
    state: &'static str,
    available_at: Option<&'static str>,
    started_at: Option<&'static str>,
    completed_at: Option<&'static str>,
    failed_at: Option<&'static str>,
}

#[derive(Clone)]
struct Scenario {
    queue: &'static str,
    task_state: &'static str,
    first_started_at: Option<&'static str>,
    cancelled_at: Option<&'static str>,
    runs: Vec<RunSeed>,
    expect_started_at: Option<&'static str>,
    expect_completed_at: Option<&'static str>,
}

fn pending_run() -> RunSeed {
    RunSeed {
        state: "pending",
        available_at: Some(FAR_FUTURE),
        ..RunSeed::default()
    }
}

fn running_run(started_at: &'static str) -> RunSeed {
    RunSeed {
        state: "running",
        started_at: Some(started_at),
        ..RunSeed::default()
    }
}

fn completed_run(started_at: &'static str, completed_at: &'static str) -> RunSeed {
    RunSeed {
        state: "completed",
        started_at: Some(started_at),
        completed_at: Some(completed_at),
        ..RunSeed::default()
    }
}

fn failed_run(started_at: &'static str, failed_at: &'static str) -> RunSeed {
    RunSeed {
        state: "failed",
        started_at: Some(started_at),
        failed_at: Some(failed_at),
        ..RunSeed::default()
    }
}

fn pending() -> Scenario {
    Scenario {
        queue: WORKFLOW_QUEUE,
        task_state: "pending",
        first_started_at: None,
        cancelled_at: None,
        runs: vec![pending_run()],
        expect_started_at: None,
        expect_completed_at: None,
    }
}

fn running() -> Scenario {
    Scenario {
        task_state: "running",
        first_started_at: Some(STARTED),
        runs: vec![running_run(STARTED)],
        expect_started_at: Some(STARTED),
        ..pending()
    }
}

fn completed() -> Scenario {
    Scenario {
        task_state: "completed",
        first_started_at: Some(STARTED),
        runs: vec![completed_run(STARTED, COMPLETED)],
        expect_started_at: Some(STARTED),
        expect_completed_at: Some(COMPLETED),
        ..pending()
    }
}

fn failed() -> Scenario {
    Scenario {
        task_state: "failed",
        first_started_at: Some(STARTED),
        runs: vec![failed_run(STARTED, FAILED)],
        expect_started_at: Some(STARTED),
        expect_completed_at: Some(FAILED),
        ..pending()
    }
}

fn cancelled_after_start() -> Scenario {
    Scenario {
        task_state: "cancelled",
        first_started_at: Some(STARTED),
        cancelled_at: Some(CANCELLED),
        runs: vec![RunSeed {
            state: "cancelled",
            started_at: Some(STARTED),
            ..RunSeed::default()
        }],
        expect_started_at: Some(STARTED),
        expect_completed_at: Some(CANCELLED),
        ..pending()
    }
}

fn cancelled_before_start() -> Scenario {
    Scenario {
        task_state: "cancelled",
        first_started_at: None,
        cancelled_at: Some(CANCELLED),
        runs: vec![RunSeed {
            state: "cancelled",
            available_at: Some(FAR_FUTURE),
            ..RunSeed::default()
        }],
        expect_started_at: None,
        expect_completed_at: Some(CANCELLED),
        ..pending()
    }
}

fn retried_awaiting_next_attempt() -> Scenario {
    Scenario {
        task_state: "pending",
        first_started_at: Some(STARTED),
        runs: vec![failed_run(STARTED, FAILED), pending_run()],
        expect_started_at: Some(STARTED),
        expect_completed_at: None,
        ..pending()
    }
}

fn retried_running() -> Scenario {
    Scenario {
        task_state: "running",
        first_started_at: Some(STARTED),
        runs: vec![failed_run(STARTED, FAILED), running_run(RETRY_STARTED)],
        expect_started_at: Some(STARTED),
        expect_completed_at: None,
        ..pending()
    }
}

fn retried_completed() -> Scenario {
    Scenario {
        task_state: "completed",
        first_started_at: Some(STARTED),
        runs: vec![
            failed_run(STARTED, FAILED),
            completed_run(RETRY_STARTED, COMPLETED),
        ],
        expect_started_at: Some(STARTED),
        expect_completed_at: Some(COMPLETED),
        ..pending()
    }
}

/// Inserts the scenario into its Absurd queue and returns
/// `(workflow_name, run_id of the last attempt)`.
async fn seed(pool: &sqlx::PgPool, scenario: &Scenario) -> (String, String) {
    let workflow_name = format!(
        "run_timestamps_{}",
        sqlx::query_scalar::<_, String>("select replace(gen_random_uuid()::text, '-', '')")
            .fetch_one(pool)
            .await
            .unwrap()
    );
    let task_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .unwrap();
    let mut run_ids = Vec::new();
    for _ in &scenario.runs {
        run_ids.push(
            sqlx::query_scalar::<_, String>("select gen_random_uuid()::text")
                .fetch_one(pool)
                .await
                .unwrap(),
        );
    }
    let last_run_id = run_ids.last().unwrap().clone();
    let queue = scenario.queue;
    sqlx::query(&format!(
        "insert into absurd.t_{queue}
            (task_id, task_name, params, max_attempts, enqueue_at, first_started_at,
             state, attempts, last_attempt_run, completed_payload, cancelled_at)
         values ($1::uuid, 'centaur.workflow', $2::jsonb, 3, '2026-03-01T09:59:00Z'::timestamptz,
                 $3::timestamptz, $4, $5, $6::uuid, $7::jsonb, $8::timestamptz)"
    ))
    .bind(&task_id)
    .bind(json!({"workflow_name": workflow_name, "input": {}, "harness_type": "codex"}))
    .bind(scenario.first_started_at)
    .bind(scenario.task_state)
    .bind(scenario.runs.len() as i32)
    .bind(&last_run_id)
    .bind((scenario.task_state == "completed").then(|| json!({"ok": true})))
    .bind(scenario.cancelled_at)
    .execute(pool)
    .await
    .expect("insert task");
    for (index, (run, run_id)) in scenario.runs.iter().zip(&run_ids).enumerate() {
        sqlx::query(&format!(
            "insert into absurd.r_{queue}
                (run_id, task_id, attempt, state, claimed_by, claim_expires_at, available_at,
                 started_at, completed_at, failed_at, result, failure_reason, created_at)
             values ($1::uuid, $2::uuid, $3, $4, $5, $6::timestamptz,
                     coalesce($7::timestamptz, '2026-03-01T09:59:00Z'::timestamptz),
                     $8::timestamptz, $9::timestamptz, $10::timestamptz, $11::jsonb, $12::jsonb,
                     '2026-03-01T09:59:00Z'::timestamptz)"
        ))
        .bind(run_id)
        .bind(&task_id)
        .bind(index as i32 + 1)
        .bind(run.state)
        .bind((run.state == "running").then_some("run-timestamps-test"))
        .bind((run.state == "running").then_some(FAR_FUTURE))
        .bind(run.available_at)
        .bind(run.started_at)
        .bind(run.completed_at)
        .bind(run.failed_at)
        .bind((run.state == "completed").then(|| json!({"ok": true})))
        .bind((run.state == "failed").then(|| json!({"name": "Error", "message": "boom"})))
        .execute(pool)
        .await
        .expect("insert run");
    }
    (workflow_name, last_run_id)
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

fn assert_timestamp(run: &Value, key: &str, expected: Option<&str>, context: &str) {
    let actual = run
        .get(key)
        .unwrap_or_else(|| panic!("{context}: run object has no `{key}` key: {run}"));
    match expected {
        None => assert!(actual.is_null(), "{context}: {key} should be null: {run}"),
        Some(expected) => {
            let text = actual
                .as_str()
                .unwrap_or_else(|| panic!("{context}: {key} should be a string: {run}"));
            let actual = OffsetDateTime::parse(text, &Rfc3339)
                .unwrap_or_else(|error| panic!("{context}: {key} {text:?} not RFC 3339: {error}"));
            let expected = OffsetDateTime::parse(expected, &Rfc3339).unwrap();
            assert_eq!(actual, expected, "{context}: {key}");
        }
    }
}

async fn assert_scenario(harness: &Harness, scenario: Scenario) {
    let (workflow_name, run_id) = seed(&harness.pool, &scenario).await;
    let context = format!("{} {:?}", scenario.queue, scenario.task_state);

    let listed = harness
        .runtime
        .list_runs(100, Some(&workflow_name))
        .await
        .expect("list_runs");
    let listed = listed
        .into_iter()
        .map(|run| serde_json::to_value(run).unwrap())
        .find(|run| run["run_id"] == json!(run_id))
        .unwrap_or_else(|| panic!("{context}: list_runs did not return {run_id}"));
    let fetched =
        serde_json::to_value(harness.runtime.get_run(&run_id).await.expect("get_run")).unwrap();

    let http_list = http_json(
        &harness.router,
        &format!("/api/workflows/runs?limit=100&workflow_name={workflow_name}"),
    )
    .await;
    let http_list = http_list["runs"]
        .as_array()
        .expect("runs array")
        .iter()
        .find(|run| run["run_id"] == json!(run_id))
        .unwrap_or_else(|| panic!("{context}: GET runs did not return {run_id}"))
        .clone();
    let http_get = http_json(&harness.router, &format!("/api/workflows/runs/{run_id}")).await;
    let http_get = http_get["run"].clone();

    for (surface, run) in [
        ("list_runs", &listed),
        ("get_run", &fetched),
        ("GET /api/workflows/runs", &http_list),
        ("GET /api/workflows/runs/{run_id}", &http_get),
    ] {
        let context = format!("{context} via {surface}");
        assert_timestamp(run, "started_at", scenario.expect_started_at, &context);
        assert_timestamp(run, "completed_at", scenario.expect_completed_at, &context);
        // Existing fields keep their names and shapes.
        assert_eq!(run["run_id"], json!(run_id), "{context}");
        assert!(run["created_at"].is_string(), "{context}");
        assert!(run["updated_at"].is_string(), "{context}");
    }
    for key in ["started_at", "completed_at"] {
        assert_eq!(listed[key], fetched[key], "{context}: list vs get {key}");
        assert_eq!(
            http_list[key], http_get[key],
            "{context}: HTTP list vs get {key}"
        );
        assert_eq!(
            listed[key], http_get[key],
            "{context}: runtime vs HTTP {key}"
        );
    }
}

async fn assert_queue(queue: &'static str) {
    let Some(url) = database_url() else {
        eprintln!("skipping queue timestamp tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    for scenario in [
        pending(),
        running(),
        completed(),
        failed(),
        cancelled_after_start(),
        cancelled_before_start(),
        retried_awaiting_next_attempt(),
        retried_running(),
        retried_completed(),
    ] {
        assert_scenario(&harness, Scenario { queue, ..scenario }).await;
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn queue_timestamps_agree_across_list_and_get_on_workflow_queue() {
    assert_queue(WORKFLOW_QUEUE).await;
}

#[tokio::test]
async fn queue_timestamps_agree_across_list_and_get_on_slack_live_queue() {
    assert_queue(WORKFLOW_SLACK_LIVE_QUEUE).await;
}

#[tokio::test]
async fn queue_timestamps_agree_across_list_and_get_on_etl_queue() {
    assert_queue(WORKFLOW_ETL_QUEUE).await;
}

#[tokio::test]
async fn queue_timestamps_agree_across_list_and_get_on_etl_backfill_queue() {
    assert_queue(WORKFLOW_ETL_BACKFILL_QUEUE).await;
}
