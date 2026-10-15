//! Story: a completed or failed workflow run lists and fetches with
//! `updated_at` equal to its last attempt's `completed_at` (completed) or
//! `failed_at` (failed), floored at `enqueue_at`, not the attempt's start.
//! Every surface and every workflow queue agrees.
//!
//! Runs are seeded straight into the Absurd queue tables and read back through
//! the existing runtime and router entry points.

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

const ENQUEUED: &str = "2026-03-01T09:59:00Z";
const STARTED: &str = "2026-03-01T10:00:01.123456Z";
const COMPLETED: &str = "2026-03-01T10:05:09.654321Z";
const FAILED: &str = "2026-03-01T10:06:30.111222Z";

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
        unreachable!("sleeping updated_at tests never open sandbox I/O")
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

#[derive(Clone)]
struct Run {
    /// Task and run state (`completed`, `failed`).
    state: &'static str,
    started_at: &'static str,
    completed_at: Option<&'static str>,
    failed_at: Option<&'static str>,
    /// Expected `updated_at`.
    expect_updated_at: &'static str,
}

fn completed_run() -> Run {
    Run {
        state: "completed",
        started_at: STARTED,
        completed_at: Some(COMPLETED),
        failed_at: None,
        expect_updated_at: COMPLETED,
    }
}

fn failed_run() -> Run {
    Run {
        state: "failed",
        started_at: STARTED,
        completed_at: None,
        failed_at: Some(FAILED),
        expect_updated_at: FAILED,
    }
}

/// Seeds one task with one attempt; returns the run id.
async fn seed(pool: &sqlx::PgPool, queue: &str, workflow_name: &str, run: &Run) -> String {
    let task_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .unwrap();
    let run_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .unwrap();
    let completed = run.state == "completed";
    sqlx::query(&format!(
        "insert into absurd.t_{queue}
            (task_id, task_name, params, max_attempts, enqueue_at, first_started_at,
             state, attempts, last_attempt_run, completed_payload)
         values ($1::uuid, 'centaur.workflow', $2::jsonb, 1, $3::timestamptz,
                 $4::timestamptz, $5, 1, $6::uuid, $7::jsonb)"
    ))
    .bind(&task_id)
    .bind(json!({"workflow_name": workflow_name, "input": {}, "harness_type": "codex"}))
    .bind(ENQUEUED)
    .bind(run.started_at)
    .bind(run.state)
    .bind(&run_id)
    .bind(completed.then(|| json!({"ok": true})))
    .execute(pool)
    .await
    .expect("insert task");
    sqlx::query(&format!(
        "insert into absurd.r_{queue}
            (run_id, task_id, attempt, state, available_at, started_at, completed_at,
             failed_at, result, failure_reason, created_at)
         values ($1::uuid, $2::uuid, 1, $3, $4::timestamptz, $4::timestamptz,
                 $5::timestamptz, $6::timestamptz, $7::jsonb, $8::jsonb, $4::timestamptz)"
    ))
    .bind(&run_id)
    .bind(&task_id)
    .bind(run.state)
    .bind(ENQUEUED)
    .bind(run.completed_at)
    .bind(run.failed_at)
    .bind(completed.then(|| json!({"ok": true})))
    .bind((!completed).then(|| json!({"message": "boom"})))
    .execute(pool)
    .await
    .expect("insert run");
    run_id
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

/// Seeds `runs` into `queue` under one workflow name, then checks every run's
/// `updated_at` from all four read surfaces.
async fn assert_runs(queue: &'static str, runs: Vec<Run>) {
    let Some(url) = database_url() else {
        eprintln!("skipping finish updated_at tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = format!(
        "finish_updated_at_{}",
        sqlx::query_scalar::<_, String>("select replace(gen_random_uuid()::text, '-', '')")
            .fetch_one(&harness.pool)
            .await
            .unwrap()
    );
    let mut seeded = Vec::new();
    for run in &runs {
        seeded.push((seed(&harness.pool, queue, &workflow_name, run).await, run));
    }

    let listed = harness
        .runtime
        .list_runs(100, Some(&workflow_name))
        .await
        .expect("list_runs must succeed");
    assert_eq!(listed.len(), runs.len(), "{queue}: list_runs count");
    let listed: Vec<Value> = listed
        .into_iter()
        .map(|run| serde_json::to_value(run).unwrap())
        .collect();

    let http_list = http_json(
        &harness.router,
        &format!("/api/workflows/runs?limit=100&workflow_name={workflow_name}"),
    )
    .await;
    let http_list = http_list["runs"].as_array().expect("runs array").clone();
    assert_eq!(http_list.len(), runs.len(), "{queue}: GET runs count");

    for (run_id, run) in seeded {
        let context = format!("{queue} {} run", run.state);
        let find = |rows: &[Value]| {
            rows.iter()
                .find(|row| row["run_id"] == json!(run_id))
                .unwrap_or_else(|| panic!("{context}: list did not return {run_id}"))
                .clone()
        };
        let fetched = serde_json::to_value(
            harness
                .runtime
                .get_run(&run_id)
                .await
                .expect("get_run must succeed"),
        )
        .unwrap();
        let http_get = http_json(&harness.router, &format!("/api/workflows/runs/{run_id}")).await
            ["run"]
            .clone();
        let surfaces = [
            ("list_runs", find(&listed)),
            ("get_run", fetched),
            ("GET /api/workflows/runs", find(&http_list)),
            ("GET /api/workflows/runs/{run_id}", http_get),
        ];
        for (surface, value) in &surfaces {
            let context = format!("{context} via {surface}");
            assert_eq!(value["status"], json!(run.state), "{context}: status");
            assert_updated_at(value, run.expect_updated_at, &context);
        }
        for (surface, value) in &surfaces[1..] {
            assert_eq!(
                surfaces[0].1["updated_at"], value["updated_at"],
                "{context}: list_runs vs {surface} updated_at"
            );
        }
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn finish_updated_at_completed_run_reports_completion_time() {
    assert_runs(WORKFLOW_QUEUE, vec![completed_run()]).await;
}

#[tokio::test]
async fn finish_updated_at_failed_run_reports_failure_time() {
    assert_runs(WORKFLOW_QUEUE, vec![failed_run()]).await;
}

#[tokio::test]
async fn finish_updated_at_equals_completed_at_for_finished_runs() {
    let Some(url) = database_url() else {
        eprintln!("skipping finish updated_at tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = format!(
        "finish_updated_at_{}",
        sqlx::query_scalar::<_, String>("select replace(gen_random_uuid()::text, '-', '')")
            .fetch_one(&harness.pool)
            .await
            .unwrap()
    );
    for run in [completed_run(), failed_run()] {
        seed(&harness.pool, WORKFLOW_QUEUE, &workflow_name, &run).await;
    }
    let listed = harness
        .runtime
        .list_runs(100, Some(&workflow_name))
        .await
        .expect("list_runs must succeed");
    assert_eq!(listed.len(), 2);
    for run in listed {
        assert_eq!(
            Some(run.updated_at),
            run.completed_at,
            "{}: updated_at must equal completed_at for a finished run",
            run.status
        );
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn finish_updated_at_is_corrected_on_every_queue() {
    for queue in [
        WORKFLOW_QUEUE,
        WORKFLOW_SLACK_LIVE_QUEUE,
        WORKFLOW_ETL_QUEUE,
        WORKFLOW_ETL_BACKFILL_QUEUE,
    ] {
        assert_runs(queue, vec![completed_run(), failed_run()]).await;
    }
}
