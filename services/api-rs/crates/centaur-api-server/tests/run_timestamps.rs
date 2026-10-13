use std::{sync::Arc, time::Duration};

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
use sqlx::{PgPool, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tower::ServiceExt;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct TestBackend;

#[async_trait]
impl SandboxBackend for TestBackend {
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

/// Durable Absurd state for one task, read straight from Postgres.
#[derive(Debug)]
struct Durable {
    run_id: String,
    enqueue_at: OffsetDateTime,
    first_started_at: Option<OffsetDateTime>,
    cancelled_at: Option<OffsetDateTime>,
    last_started_at: Option<OffsetDateTime>,
    completed_at: Option<OffsetDateTime>,
    failed_at: Option<OffsetDateTime>,
    state: String,
}

#[derive(Clone, Copy, Debug)]
enum Terminal {
    None,
    Completed,
    Failed,
    Cancelled,
}

struct Seeded {
    queue: &'static str,
    task_id: String,
    label: &'static str,
    terminal: Terminal,
}

struct Env {
    pool: PgPool,
    workflow_name: String,
    seeded: Vec<Seeded>,
    _serial: tokio::sync::MutexGuard<'static, ()>,
}

impl Env {
    async fn new() -> Option<Env> {
        let Some(url) = database_url() else {
            eprintln!("skipping run timestamp tests: set SESSION_SQLX_TEST_DATABASE_URL");
            return None;
        };
        let serial = SERIAL.lock().await;
        let pool = PgPool::connect(&url).await.unwrap();
        let mut conn = pool.acquire().await.unwrap();
        sqlx::query("select pg_advisory_lock(7_201_001)")
            .execute(&mut *conn)
            .await
            .unwrap();
        let installed: bool = sqlx::query_scalar(
            "select exists(select 1 from pg_namespace where nspname = 'absurd')",
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        if !installed {
            sqlx::raw_sql(include_str!(
                "../../centaur-session-sqlx/migrations/0007_absurd_workflows.sql"
            ))
            .execute(&mut *conn)
            .await
            .unwrap();
        }
        for queue in all_queues() {
            sqlx::query(
                "select absurd.create_queue($1) where not exists \
                 (select 1 from absurd.queues where queue_name = $1)",
            )
            .bind(queue)
            .execute(&mut *conn)
            .await
            .unwrap();
        }
        sqlx::query("select pg_advisory_unlock(7_201_001)")
            .execute(&mut *conn)
            .await
            .unwrap();
        drop(conn);
        Some(Env {
            pool,
            workflow_name: format!("run-timestamps-{}", uuid_like()),
            seeded: Vec::new(),
            _serial: serial,
        })
    }

    async fn spawn(&self, queue: &str, options: Value) -> (String, String) {
        let row = sqlx::query(
            "select task_id::text as task_id, run_id::text as run_id \
             from absurd.spawn_task($1, 'run_timestamps_probe', $2::jsonb, $3::jsonb)",
        )
        .bind(queue)
        .bind(json!({"workflow_name": self.workflow_name, "input": {"probe": true}}))
        .bind(options)
        .fetch_one(&self.pool)
        .await
        .unwrap();
        (row.get("task_id"), row.get("run_id"))
    }

    async fn claim(&self, queue: &str, task_id: &str) -> String {
        let rows = sqlx::query(
            "select task_id::text as task_id, run_id::text as run_id \
             from absurd.claim_task($1, 'run-timestamps-probe', 3600, 1)",
        )
        .bind(queue)
        .fetch_all(&self.pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 1, "expected to claim the seeded task");
        let claimed: String = rows[0].get("task_id");
        assert_eq!(claimed, task_id, "claimed some other task in {queue}");
        rows[0].get("run_id")
    }

    async fn complete(&self, queue: &str, run_id: &str) {
        sqlx::query("select absurd.complete_run($1, $2::uuid, '{\"ok\": true}'::jsonb)")
            .bind(queue)
            .bind(run_id)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn fail(&self, queue: &str, run_id: &str) {
        sqlx::query("select absurd.fail_run($1, $2::uuid, '{\"name\": \"Boom\"}'::jsonb, null)")
            .bind(queue)
            .bind(run_id)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn cancel(&self, queue: &str, task_id: &str) {
        sqlx::query("select absurd.cancel_task($1, $2::uuid)")
            .bind(queue)
            .bind(task_id)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    /// Spawned but never claimed. The run is pushed into the future so the
    /// workers started by `WorkflowRuntime::new` cannot claim it.
    async fn seed_pending(&mut self, queue: &'static str, label: &'static str) {
        let (task_id, run_id) = self.spawn(queue, json!({})).await;
        sqlx::query(&format!(
            "update absurd.r_{queue} set available_at = now() + interval '1 hour' \
             where run_id = $1::uuid"
        ))
        .bind(&run_id)
        .execute(&self.pool)
        .await
        .unwrap();
        self.record(queue, task_id, label, Terminal::None, "pending")
            .await;
    }

    async fn seed_running(&mut self, queue: &'static str, label: &'static str) {
        let (task_id, _) = self.spawn(queue, json!({})).await;
        self.claim(queue, &task_id).await;
        self.record(queue, task_id, label, Terminal::None, "running")
            .await;
    }

    async fn seed_completed(&mut self, queue: &'static str, label: &'static str) {
        let (task_id, _) = self.spawn(queue, json!({})).await;
        let run_id = self.claim(queue, &task_id).await;
        self.complete(queue, &run_id).await;
        self.record(queue, task_id, label, Terminal::Completed, "completed")
            .await;
    }

    async fn seed_failed(&mut self, queue: &'static str, label: &'static str) {
        let (task_id, _) = self.spawn(queue, json!({"max_attempts": 1})).await;
        let run_id = self.claim(queue, &task_id).await;
        self.fail(queue, &run_id).await;
        self.record(queue, task_id, label, Terminal::Failed, "failed")
            .await;
    }

    /// First attempt failed and a retry is scheduled an hour from now.
    async fn seed_awaiting_retry(&mut self, queue: &'static str, label: &'static str) {
        let options = json!({
            "max_attempts": 3,
            "retry_strategy": {"kind": "fixed", "base_seconds": 3600}
        });
        let (task_id, _) = self.spawn(queue, options).await;
        let run_id = self.claim(queue, &task_id).await;
        self.fail(queue, &run_id).await;
        self.record(queue, task_id, label, Terminal::None, "sleeping")
            .await;
    }

    /// First attempt failed, the second attempt started later and completed.
    async fn seed_retried_then_completed(&mut self, queue: &'static str, label: &'static str) {
        let options = json!({
            "max_attempts": 2,
            "retry_strategy": {"kind": "fixed", "base_seconds": 0}
        });
        let (task_id, _) = self.spawn(queue, options).await;
        let first = self.claim(queue, &task_id).await;
        self.fail(queue, &first).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let second = self.claim(queue, &task_id).await;
        assert_ne!(first, second);
        self.complete(queue, &second).await;
        self.record(queue, task_id, label, Terminal::Completed, "completed")
            .await;
    }

    async fn seed_cancelled_before_start(&mut self, queue: &'static str, label: &'static str) {
        let (task_id, _) = self.spawn(queue, json!({})).await;
        self.cancel(queue, &task_id).await;
        self.record(queue, task_id, label, Terminal::Cancelled, "cancelled")
            .await;
    }

    async fn seed_cancelled_while_running(&mut self, queue: &'static str, label: &'static str) {
        let (task_id, _) = self.spawn(queue, json!({})).await;
        self.claim(queue, &task_id).await;
        self.cancel(queue, &task_id).await;
        self.record(queue, task_id, label, Terminal::Cancelled, "cancelled")
            .await;
    }

    async fn record(
        &mut self,
        queue: &'static str,
        task_id: String,
        label: &'static str,
        terminal: Terminal,
        expected_state: &str,
    ) {
        let durable = self.durable(queue, &task_id).await;
        assert_eq!(
            durable.state, expected_state,
            "{label}: fixture reached the wrong state"
        );
        self.seeded.push(Seeded {
            queue,
            task_id,
            label,
            terminal,
        });
    }

    async fn durable(&self, queue: &str, task_id: &str) -> Durable {
        let row = sqlx::query(&format!(
            "select r.run_id::text as run_id, t.enqueue_at, t.first_started_at, \
                    t.cancelled_at, r.started_at, r.completed_at, r.failed_at, t.state \
             from absurd.t_{queue} t \
             join absurd.r_{queue} r on r.run_id = t.last_attempt_run \
             where t.task_id = $1::uuid"
        ))
        .bind(task_id)
        .fetch_one(&self.pool)
        .await
        .unwrap();
        Durable {
            run_id: row.get("run_id"),
            enqueue_at: row.get("enqueue_at"),
            first_started_at: row.get("first_started_at"),
            cancelled_at: row.get("cancelled_at"),
            last_started_at: row.get("started_at"),
            completed_at: row.get("completed_at"),
            failed_at: row.get("failed_at"),
            state: row.get("state"),
        }
    }

    async fn runtime_and_app(&self) -> (WorkflowRuntime, Router) {
        let empty_workflows = std::env::temp_dir().join(format!("run-timestamps-{}", uuid_like()));
        std::fs::create_dir_all(&empty_workflows).unwrap();
        // Serialized by SERIAL, so no other test mutates the environment.
        unsafe {
            std::env::set_var("WORKFLOW_DIRS", &empty_workflows);
            std::env::set_var("WORKFLOW_RECONCILE_INTERVAL_SECS", "0");
        }
        let iron = IronControlClient::new("http://127.0.0.1:9", "unused");
        let store = PgSessionStore::new(self.pool.clone());
        let session_runtime = SessionRuntime::new(
            store.clone(),
            SandboxRuntime::backend(Arc::new(TestBackend), SandboxSpec::new("test")),
            SessionRegistrar::new(iron.clone()),
        );
        let workflows = WorkflowRuntime::new(
            store,
            session_runtime.clone(),
            None,
            WorkflowPrincipalRegistrar::new(iron),
        )
        .await
        .unwrap();
        let app = build_router_with_session_and_workflow_runtime(
            session_runtime,
            Some(workflows.clone()),
            ApiAuthConfig::testing("test-secret"),
        );
        (workflows, app)
    }
}

fn all_queues() -> [&'static str; 4] {
    [
        WORKFLOW_QUEUE,
        WORKFLOW_SLACK_LIVE_QUEUE,
        WORKFLOW_ETL_QUEUE,
        WORKFLOW_ETL_BACKFILL_QUEUE,
    ]
}

fn uuid_like() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{nanos:x}-{}", std::process::id())
}

async fn http_get(app: &Router, uri: &str) -> Value {
    let response = app
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
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(status, StatusCode::OK, "{uri}: {}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

fn find_run<'a>(runs: &'a [Value], run_id: &str) -> &'a Value {
    runs.iter()
        .find(|run| run["run_id"] == run_id)
        .unwrap_or_else(|| panic!("run {run_id} missing from list"))
}

/// Reads `key` from a serialized run: the key must be present, and be either
/// null or an RFC 3339 timestamp.
fn timestamp_field(run: &Value, key: &str, context: &str) -> (Option<OffsetDateTime>, Value) {
    let object = run.as_object().unwrap_or_else(|| panic!("{context}: not an object"));
    let value = object
        .get(key)
        .unwrap_or_else(|| panic!("{context}: key `{key}` is missing from {run}"));
    match value {
        Value::Null => (None, Value::Null),
        Value::String(text) => {
            let parsed = OffsetDateTime::parse(text, &Rfc3339)
                .unwrap_or_else(|error| panic!("{context}: `{key}`={text:?} is not RFC 3339: {error}"));
            (Some(parsed), value.clone())
        }
        other => panic!("{context}: `{key}` must be a string or null, got {other}"),
    }
}

/// Checks all four views of every seeded run (list and get, over HTTP and
/// through `WorkflowRuntime`) against the durable Absurd columns.
async fn assert_views(env: &Env, runtime: &WorkflowRuntime, app: &Router) {
    let http_list = http_get(
        app,
        &format!("/api/workflows/runs?limit=1000&workflow_name={}", env.workflow_name),
    )
    .await;
    let http_runs = http_list["runs"].as_array().expect("runs array").clone();
    let direct_list = serde_json::to_value(
        runtime
            .list_runs(1000, Some(&env.workflow_name))
            .await
            .unwrap(),
    )
    .unwrap();
    let direct_runs = direct_list.as_array().expect("list_runs array").clone();
    assert_eq!(http_runs.len(), env.seeded.len());
    assert_eq!(direct_runs.len(), env.seeded.len());

    for seeded in &env.seeded {
        let durable = env.durable(seeded.queue, &seeded.task_id).await;
        let label = format!("{} ({})", seeded.label, seeded.queue);
        let expected_started = durable.first_started_at;
        let expected_completed = match seeded.terminal {
            Terminal::None => None,
            Terminal::Completed => durable.completed_at,
            Terminal::Failed => durable.failed_at,
            Terminal::Cancelled => durable.cancelled_at,
        };
        if !matches!(seeded.terminal, Terminal::None) {
            assert!(
                expected_completed.is_some(),
                "{label}: fixture has no terminal timestamp"
            );
        }
        assert!(durable.enqueue_at <= OffsetDateTime::now_utc() + Duration::from_secs(5));

        let http_get_run = http_get(app, &format!("/api/workflows/runs/{}", durable.run_id)).await;
        let direct_get = serde_json::to_value(runtime.get_run(&durable.run_id).await.unwrap()).unwrap();
        let views = [
            ("http list", find_run(&http_runs, &durable.run_id).clone()),
            ("http get", http_get_run["run"].clone()),
            ("runtime list_runs", find_run(&direct_runs, &durable.run_id).clone()),
            ("runtime get_run", direct_get),
        ];
        let mut raw = Vec::new();
        for (view, run) in &views {
            let context = format!("{label} via {view}");
            let (started, started_raw) = timestamp_field(run, "started_at", &context);
            let (completed, completed_raw) = timestamp_field(run, "completed_at", &context);
            assert_eq!(started, expected_started, "{context}: started_at");
            assert_eq!(completed, expected_completed, "{context}: completed_at");
            raw.push((started_raw, completed_raw));
        }
        assert!(
            raw.windows(2).all(|pair| pair[0] == pair[1]),
            "{label}: list and get disagree: {raw:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_pending_run_has_null_started_and_completed() {
    let Some(mut env) = Env::new().await else { return };
    env.seed_pending(WORKFLOW_QUEUE, "pending").await;
    let (runtime, app) = env.runtime_and_app().await;
    assert_views(&env, &runtime, &app).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_running_run_has_started_at_and_null_completed_at() {
    let Some(mut env) = Env::new().await else { return };
    env.seed_running(WORKFLOW_QUEUE, "running").await;
    let (runtime, app) = env.runtime_and_app().await;
    assert_views(&env, &runtime, &app).await;
    let durable = env.durable(WORKFLOW_QUEUE, &env.seeded[0].task_id).await;
    assert!(durable.first_started_at.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_completed_failed_and_cancelled_runs_have_completed_at() {
    let Some(mut env) = Env::new().await else { return };
    env.seed_completed(WORKFLOW_QUEUE, "completed").await;
    env.seed_failed(WORKFLOW_QUEUE, "failed").await;
    env.seed_cancelled_before_start(WORKFLOW_QUEUE, "cancelled before start")
        .await;
    env.seed_cancelled_while_running(WORKFLOW_QUEUE, "cancelled while running")
        .await;
    let (runtime, app) = env.runtime_and_app().await;
    assert_views(&env, &runtime, &app).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_retried_runs_keep_first_attempt_start() {
    let Some(mut env) = Env::new().await else { return };
    env.seed_awaiting_retry(WORKFLOW_QUEUE, "awaiting retry").await;
    env.seed_retried_then_completed(WORKFLOW_QUEUE, "retried then completed")
        .await;
    let retried = env.durable(WORKFLOW_QUEUE, &env.seeded[1].task_id).await;
    assert!(
        retried.first_started_at < retried.last_started_at,
        "fixture: second attempt must start after the first"
    );
    let (runtime, app) = env.runtime_and_app().await;
    assert_views(&env, &runtime, &app).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_list_and_get_agree_across_all_four_queues() {
    let Some(mut env) = Env::new().await else { return };
    env.seed_completed(WORKFLOW_SLACK_LIVE_QUEUE, "slack-live completed")
        .await;
    env.seed_running(WORKFLOW_ETL_QUEUE, "etl running").await;
    env.seed_failed(WORKFLOW_ETL_BACKFILL_QUEUE, "etl-backfill failed")
        .await;
    env.seed_pending(WORKFLOW_QUEUE, "workflow pending").await;
    env.seed_cancelled_before_start(WORKFLOW_ETL_QUEUE, "etl cancelled")
        .await;
    let (runtime, app) = env.runtime_and_app().await;
    assert_views(&env, &runtime, &app).await;
}
