//! Story: a pending workflow run whose attempt `available_at` lies in the
//! future (a scheduled start or a retry delay) lists and fetches with
//! `updated_at` equal to the later of the task's `enqueue_at` and the attempt
//! row's `created_at`, never the future time. A pending run whose
//! `available_at` is not in the future keeps it. No run in any state reports
//! an `updated_at` after the present, and every surface and queue agrees.
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
const CREATED_BEFORE_ENQUEUE: &str = "2026-03-01T09:58:00Z";
const STARTED: &str = "2026-03-01T10:00:01.123456Z";
const FAILED: &str = "2026-03-01T10:02:10.500000Z";
const RETRY_CREATED: &str = "2026-03-01T10:02:10.750000Z";
const WOKEN: &str = "2026-03-01T10:04:30.333444Z";
const FINISHED: &str = "2026-03-01T10:05:09.654321Z";
const START_FAR_FUTURE: &str = "2099-01-01T00:00:00Z";
const WAKE_FAR_FUTURE: &str = "2098-06-01T12:00:00Z";

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
        unreachable!("pending updated_at tests never open sandbox I/O")
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
    label: &'static str,
    /// Task and attempt state.
    state: &'static str,
    attempt: i32,
    /// Attempt row `available_at`.
    available_at: &'static str,
    started_at: Option<&'static str>,
    completed_at: Option<&'static str>,
    failed_at: Option<&'static str>,
    cancelled_at: Option<&'static str>,
    /// Attempt row `created_at`.
    created_at: &'static str,
    /// Expected `updated_at`.
    expect_updated_at: &'static str,
}

fn base(label: &'static str, state: &'static str, expect_updated_at: &'static str) -> Run {
    Run {
        label,
        state,
        attempt: 1,
        available_at: ENQUEUED,
        started_at: None,
        completed_at: None,
        failed_at: None,
        cancelled_at: None,
        created_at: ENQUEUED,
        expect_updated_at,
    }
}

/// Scheduled to start far in the future: reports when it was enqueued.
fn pending_scheduled() -> Run {
    Run {
        available_at: START_FAR_FUTURE,
        ..base("pending scheduled far future", "pending", ENQUEUED)
    }
}

/// Retry delay: a second attempt row created after the first failed, waiting
/// for a future `available_at`; reports the attempt row's creation time.
fn pending_retry_delay() -> Run {
    Run {
        attempt: 2,
        available_at: START_FAR_FUTURE,
        started_at: Some(STARTED),
        created_at: RETRY_CREATED,
        ..base("pending retry delay", "pending", RETRY_CREATED)
    }
}

/// An attempt row created before enqueue is floored at `enqueue_at`.
fn pending_created_before_enqueue() -> Run {
    Run {
        available_at: START_FAR_FUTURE,
        created_at: CREATED_BEFORE_ENQUEUE,
        ..base("pending created before enqueue", "pending", ENQUEUED)
    }
}

/// Woken from sleep: `available_at` is its past wake time and is kept.
fn pending_woken() -> Run {
    Run {
        available_at: WOKEN,
        started_at: Some(STARTED),
        ..base("pending woken", "pending", WOKEN)
    }
}

/// Just enqueued: `available_at` is the enqueue time and is kept.
fn pending_ready() -> Run {
    base("pending ready", "pending", ENQUEUED)
}

fn running() -> Run {
    Run {
        available_at: STARTED,
        started_at: Some(STARTED),
        ..base("running", "running", STARTED)
    }
}

fn sleeping() -> Run {
    Run {
        available_at: WAKE_FAR_FUTURE,
        started_at: Some(STARTED),
        ..base("sleeping", "sleeping", STARTED)
    }
}

fn completed() -> Run {
    Run {
        available_at: STARTED,
        started_at: Some(STARTED),
        completed_at: Some(FINISHED),
        ..base("completed", "completed", FINISHED)
    }
}

fn failed() -> Run {
    Run {
        available_at: STARTED,
        started_at: Some(STARTED),
        failed_at: Some(FINISHED),
        ..base("failed", "failed", FINISHED)
    }
}

fn cancelled_while_sleeping() -> Run {
    Run {
        available_at: WAKE_FAR_FUTURE,
        started_at: Some(STARTED),
        cancelled_at: Some(FINISHED),
        ..base("cancelled while sleeping", "cancelled", FINISHED)
    }
}

/// A pending attempt whose `available_at` is not in the future is claimable:
/// the runtime's live workers would flip it to running mid-read.
fn claimable(run: &Run) -> bool {
    run.state == "pending"
        && OffsetDateTime::parse(run.available_at, &Rfc3339).unwrap() <= OffsetDateTime::now_utc()
}

/// Seeds one task whose last attempt is `run`; returns the run id. A claimable
/// attempt is seeded with a far-future `available_at`; `assert_runs` sets the
/// real one once it holds the task rows locked against the workers.
async fn seed(pool: &sqlx::PgPool, queue: &str, workflow_name: &str, run: &Run) -> String {
    let task_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .unwrap();
    let run_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "insert into absurd.t_{queue}
            (task_id, task_name, params, max_attempts, enqueue_at, first_started_at,
             state, attempts, last_attempt_run, completed_payload, cancelled_at)
         values ($1::uuid, 'centaur.workflow', $2::jsonb, 3, $3::timestamptz,
                 $4::timestamptz, $5, $6, $7::uuid, $8::jsonb, $9::timestamptz)"
    ))
    .bind(&task_id)
    .bind(json!({"workflow_name": workflow_name, "input": {}, "harness_type": "codex"}))
    .bind(ENQUEUED)
    .bind(run.started_at)
    .bind(run.state)
    .bind(run.attempt)
    .bind(&run_id)
    .bind((run.state == "completed").then(|| json!({"ok": true})))
    .bind(run.cancelled_at)
    .execute(pool)
    .await
    .expect("insert task");
    // A retry's last attempt is preceded by the failed first attempt.
    if run.attempt > 1 {
        sqlx::query(&format!(
            "insert into absurd.r_{queue}
                (run_id, task_id, attempt, state, available_at, started_at, failed_at,
                 failure_reason, created_at)
             values (gen_random_uuid(), $1::uuid, 1, 'failed', $2::timestamptz,
                     $2::timestamptz, $3::timestamptz, $4::jsonb, $5::timestamptz)"
        ))
        .bind(&task_id)
        .bind(STARTED)
        .bind(FAILED)
        .bind(json!({"message": "boom"}))
        .bind(ENQUEUED)
        .execute(pool)
        .await
        .expect("insert failed first attempt");
    }
    let attempt_started_at = if run.attempt > 1 {
        None
    } else {
        run.started_at
    };
    sqlx::query(&format!(
        "insert into absurd.r_{queue}
            (run_id, task_id, attempt, state, available_at, started_at, completed_at,
             failed_at, result, failure_reason, created_at)
         values ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz,
                 $7::timestamptz, $8::timestamptz, $9::jsonb, $10::jsonb, $11::timestamptz)"
    ))
    .bind(&run_id)
    .bind(&task_id)
    .bind(run.attempt)
    .bind(run.state)
    .bind(if claimable(run) {
        START_FAR_FUTURE
    } else {
        run.available_at
    })
    .bind(attempt_started_at)
    .bind(run.completed_at)
    .bind(run.failed_at)
    .bind((run.state == "completed").then(|| json!({"ok": true})))
    .bind((run.state == "failed").then(|| json!({"message": "boom"})))
    .bind(run.created_at)
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

fn parse_updated_at(run: &Value, context: &str) -> OffsetDateTime {
    let text = run["updated_at"]
        .as_str()
        .unwrap_or_else(|| panic!("{context}: updated_at should be a string: {run}"));
    OffsetDateTime::parse(text, &Rfc3339)
        .unwrap_or_else(|error| panic!("{context}: updated_at {text:?} not RFC 3339: {error}"))
}

/// Seeds `runs` into `queue` under one workflow name, then checks every run's
/// `updated_at` from all four read surfaces: it equals the expected value, is
/// not after the present, and agrees across surfaces.
async fn assert_runs(queue: &'static str, runs: Vec<Run>) {
    let Some(url) = database_url() else {
        eprintln!("skipping pending updated_at tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = format!(
        "pending_updated_at_{}",
        sqlx::query_scalar::<_, String>("select replace(gen_random_uuid()::text, '-', '')")
            .fetch_one(&harness.pool)
            .await
            .unwrap()
    );
    let mut seeded = Vec::new();
    for run in &runs {
        seeded.push((seed(&harness.pool, queue, &workflow_name, run).await, run));
    }
    // Workers claim with `for update skip locked` over the attempt and task
    // rows, so holding the task rows locked keeps claimable runs pending while
    // plain reads still see them. `no key update` lets the attempt rows'
    // foreign key checks through.
    let mut hold = harness.pool.begin().await.expect("begin lock transaction");
    sqlx::query(&format!(
        "select task_id from absurd.t_{queue}
          where params->>'workflow_name' = $1
          for no key update"
    ))
    .bind(&workflow_name)
    .fetch_all(&mut *hold)
    .await
    .expect("lock seeded tasks");
    for (run_id, run) in &seeded {
        if claimable(run) {
            sqlx::query(&format!(
                "update absurd.r_{queue} set available_at = $2::timestamptz
                  where run_id = $1::uuid"
            ))
            .bind(run_id)
            .bind(run.available_at)
            .execute(&harness.pool)
            .await
            .expect("set claimable available_at");
        }
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
        let context = format!("{queue} {} run", run.label);
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
        let http_get =
            http_json(&harness.router, &format!("/api/workflows/runs/{run_id}")).await["run"]
                .clone();
        let surfaces = [
            ("list_runs", find(&listed)),
            ("get_run", fetched),
            ("GET /api/workflows/runs", find(&http_list)),
            ("GET /api/workflows/runs/{run_id}", http_get),
        ];
        let now = OffsetDateTime::now_utc();
        for (surface, value) in &surfaces {
            let context = format!("{context} via {surface}");
            assert_eq!(value["status"], json!(run.state), "{context}: status");
            let actual = parse_updated_at(value, &context);
            assert!(
                actual <= now,
                "{context}: updated_at {actual} is in the future (now {now})"
            );
            let expected = OffsetDateTime::parse(run.expect_updated_at, &Rfc3339).unwrap();
            assert_eq!(actual, expected, "{context}: updated_at");
        }
        for (surface, value) in &surfaces[1..] {
            assert_eq!(
                surfaces[0].1["updated_at"], value["updated_at"],
                "{context}: list_runs vs {surface} updated_at"
            );
        }
    }
    hold.rollback().await.expect("release task locks");
    harness.pool.close().await;
}

#[tokio::test]
async fn pending_updated_at_scheduled_future_start_reports_enqueue_time() {
    assert_runs(WORKFLOW_QUEUE, vec![pending_scheduled()]).await;
}

#[tokio::test]
async fn pending_updated_at_retry_delay_reports_attempt_created_at() {
    assert_runs(WORKFLOW_QUEUE, vec![pending_retry_delay()]).await;
}

#[tokio::test]
async fn pending_updated_at_never_precedes_enqueue_at() {
    assert_runs(WORKFLOW_QUEUE, vec![pending_created_before_enqueue()]).await;
}

#[tokio::test]
async fn pending_updated_at_keeps_past_available_at() {
    assert_runs(WORKFLOW_QUEUE, vec![pending_woken(), pending_ready()]).await;
}

#[tokio::test]
async fn pending_updated_at_no_state_reports_a_future_time() {
    assert_runs(
        WORKFLOW_QUEUE,
        vec![
            pending_scheduled(),
            pending_retry_delay(),
            pending_woken(),
            pending_ready(),
            running(),
            sleeping(),
            completed(),
            failed(),
            cancelled_while_sleeping(),
        ],
    )
    .await;
}

#[tokio::test]
async fn pending_updated_at_is_corrected_on_every_queue() {
    for queue in [
        WORKFLOW_QUEUE,
        WORKFLOW_SLACK_LIVE_QUEUE,
        WORKFLOW_ETL_QUEUE,
        WORKFLOW_ETL_BACKFILL_QUEUE,
    ] {
        assert_runs(
            queue,
            vec![pending_scheduled(), pending_retry_delay(), pending_woken()],
        )
        .await;
    }
}
