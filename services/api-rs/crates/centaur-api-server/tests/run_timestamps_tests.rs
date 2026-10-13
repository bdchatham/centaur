//! Acceptance tests for `started_at` / `completed_at` on workflow runs, as
//! returned by `WorkflowRuntime::list_runs`/`get_run` and the HTTP routes
//! `GET /api/workflows/runs` and `GET /api/workflows/runs/{run_id}`.
//!
//! Runs are seeded straight into the Absurd task/run tables of all four
//! workflow queues so every lifecycle state has known, fixed timestamps.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use centaur_api_server::{
    ApiAuthConfig, build_router_with_session_and_workflow_runtime,
};
use centaur_iron_control::IronControlClient;
use centaur_sandbox_core::{
    ObservedSandbox, SandboxBackend, SandboxError, SandboxHandle, SandboxId, SandboxIo,
    SandboxResult, SandboxSpec, SandboxStatus,
};
use centaur_session_runtime::{SandboxRuntime, SessionPrincipalRegistrar, SessionRuntime};
use centaur_session_sqlx::{PgSessionStore, TextSearchBackend};
use centaur_workflows::{
    WORKFLOW_ETL_BACKFILL_QUEUE, WORKFLOW_ETL_QUEUE, WORKFLOW_QUEUE, WORKFLOW_SLACK_LIVE_QUEUE,
    WorkflowPrincipalRegistrar, WorkflowRuntime,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;
use tower::ServiceExt;

const QUEUES: [&str; 4] = [
    WORKFLOW_QUEUE,
    WORKFLOW_SLACK_LIVE_QUEUE,
    WORKFLOW_ETL_QUEUE,
    WORKFLOW_ETL_BACKFILL_QUEUE,
];

// Instants used by the seeded runs. Whole seconds, so RFC 3339 round-trips.
const T_ENQUEUED: &str = "2026-01-02T03:00:00Z";
const T_FIRST_START: &str = "2026-01-02T03:01:00Z";
const T_END: &str = "2026-01-02T03:05:00Z";
const T_RETRY_START: &str = "2026-01-02T03:07:00Z";
const FAR_FUTURE: &str = "2099-01-01T00:00:00Z";

// ---------------------------------------------------------------- harness

#[derive(Default)]
struct TestBackend {
    next_id: AtomicU64,
}

#[async_trait]
impl SandboxBackend for TestBackend {
    fn name(&self) -> &'static str {
        "test"
    }

    async fn create(&self, _spec: SandboxSpec) -> SandboxResult<SandboxHandle> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(SandboxHandle::new(
            SandboxId::new(format!("test-{id}")),
            self.name(),
        ))
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

#[derive(Clone, Copy)]
struct TestPrincipalRegistrar;

fn test_principal(id: &str) -> centaur_iron_control::Principal {
    centaur_iron_control::Principal {
        id: id.to_owned(),
        foreign_id: Some("test".to_owned()),
        name: "Test".to_owned(),
        labels: Default::default(),
        sandbox_observability_enabled: true,
    }
}

#[async_trait]
impl SessionPrincipalRegistrar for TestPrincipalRegistrar {
    async fn register_session(
        &self,
        _thread_key: &str,
        _metadata: Option<&Value>,
        _create_if_missing: bool,
    ) -> Result<centaur_iron_control::Principal, centaur_iron_control::IronControlError> {
        Ok(test_principal("prn_test"))
    }

    async fn register_requester(
        &self,
        _thread_key: &str,
        _metadata: Option<&Value>,
        _create_if_missing: bool,
    ) -> Result<Option<centaur_iron_control::Principal>, centaur_iron_control::IronControlError>
    {
        Ok(None)
    }

    async fn get_principal(
        &self,
        principal: &str,
    ) -> Result<centaur_iron_control::Principal, centaur_iron_control::IronControlError> {
        Ok(test_principal(principal))
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

/// Runtime construction creates the four Absurd queues; serialize it so
/// concurrent tests in this binary never race on DDL.
static SETUP: Mutex<()> = Mutex::const_new(());

struct Fixture {
    pool: sqlx::PgPool,
    runtime: WorkflowRuntime,
    router: Router,
    workflow_name: String,
}

/// `None` (test skipped) when no test database is configured.
async fn fixture() -> Option<Fixture> {
    let url = std::env::var("SESSION_SQLX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("SESSION_RUNTIME_TEST_DATABASE_URL"))
        .or_else(|_| std::env::var("ABSURD_TEST_DATABASE_URL"))
        .ok()?;
    let _guard = SETUP.lock().await;
    let store = PgSessionStore::connect(&url).await.unwrap();
    store
        .run_migrations(TextSearchBackend::Postgres)
        .await
        .unwrap();
    let pool = store.pool().clone();
    let session_runtime = SessionRuntime::new(
        store.clone(),
        SandboxRuntime::backend(Arc::new(TestBackend::default()), SandboxSpec::new("test")),
        TestPrincipalRegistrar,
    );
    let runtime = WorkflowRuntime::new(
        store,
        session_runtime.clone(),
        None,
        WorkflowPrincipalRegistrar::new(IronControlClient::new("http://127.0.0.1:1", "test-key")),
    )
    .await
    .unwrap();
    let router = build_router_with_session_and_workflow_runtime(
        session_runtime,
        Some(runtime.clone()),
        ApiAuthConfig::testing("test-secret"),
    );
    let suffix: String = sqlx::query_scalar("select replace(gen_random_uuid()::text, '-', '')")
        .fetch_one(&pool)
        .await
        .unwrap();
    Some(Fixture {
        pool,
        runtime,
        router,
        workflow_name: format!("run_timestamps_{suffix}"),
    })
}

struct AttemptSeed {
    state: &'static str,
    available_at: &'static str,
    started_at: Option<&'static str>,
    completed_at: Option<&'static str>,
    failed_at: Option<&'static str>,
}

struct RunSeed {
    task_state: &'static str,
    first_started_at: Option<&'static str>,
    cancelled_at: Option<&'static str>,
    /// Oldest first; the last attempt is the run the API reports.
    attempts: Vec<AttemptSeed>,
}

struct Seeded {
    queue: &'static str,
    run_id: String,
}

impl Fixture {
    async fn seed(&self, queue: &'static str, seed: &RunSeed) -> Seeded {
        let task_table = format!("absurd.t_{queue}");
        let run_table = format!("absurd.r_{queue}");
        let task_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
            .fetch_one(&self.pool)
            .await
            .unwrap();
        let mut run_ids = Vec::new();
        for _ in &seed.attempts {
            let run_id: String = sqlx::query_scalar("select gen_random_uuid()::text")
                .fetch_one(&self.pool)
                .await
                .unwrap();
            run_ids.push(run_id);
        }
        let last_run = run_ids.last().unwrap().clone();
        sqlx::query(&format!(
            "insert into {task_table}
                 (task_id, task_name, params, enqueue_at, first_started_at, state,
                  attempts, last_attempt_run, cancelled_at)
             values ($1::uuid, 'centaur.workflow', $2, $3::timestamptz, $4::timestamptz,
                     $5, $6, $7::uuid, $8::timestamptz)"
        ))
        .bind(&task_id)
        .bind(json!({"workflow_name": self.workflow_name, "input": {"queue": queue}}))
        .bind(T_ENQUEUED)
        .bind(seed.first_started_at)
        .bind(seed.task_state)
        .bind(seed.attempts.len() as i32)
        .bind(&last_run)
        .bind(seed.cancelled_at)
        .execute(&self.pool)
        .await
        .unwrap();
        for (index, attempt) in seed.attempts.iter().enumerate() {
            let claim_expires_at = (attempt.state == "running").then_some(FAR_FUTURE);
            sqlx::query(&format!(
                "insert into {run_table}
                     (run_id, task_id, attempt, state, claim_expires_at, available_at,
                      started_at, completed_at, failed_at, created_at)
                 values ($1::uuid, $2::uuid, $3, $4, $5::timestamptz, $6::timestamptz,
                         $7::timestamptz, $8::timestamptz, $9::timestamptz, $10::timestamptz)"
            ))
            .bind(&run_ids[index])
            .bind(&task_id)
            .bind(index as i32 + 1)
            .bind(attempt.state)
            .bind(claim_expires_at)
            .bind(attempt.available_at)
            .bind(attempt.started_at)
            .bind(attempt.completed_at)
            .bind(attempt.failed_at)
            .bind(T_ENQUEUED)
            .execute(&self.pool)
            .await
            .unwrap();
        }
        Seeded {
            queue,
            run_id: last_run,
        }
    }

    async fn http_get(&self, uri: &str) -> Value {
        let response = self
            .router
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

    /// The run as JSON from each of the four places it is returned.
    async fn run_everywhere(&self, run_id: &str) -> [(&'static str, Value); 4] {
        let get = serde_json::to_value(self.runtime.get_run(run_id).await.unwrap()).unwrap();
        let list = self
            .runtime
            .list_runs(1_000, Some(&self.workflow_name))
            .await
            .unwrap();
        let list = list
            .into_iter()
            .map(|run| serde_json::to_value(run).unwrap())
            .find(|run| run["run_id"] == run_id)
            .expect("run is returned by list_runs");
        let http_get = self
            .http_get(&format!("/api/workflows/runs/{run_id}"))
            .await["run"]
            .clone();
        let http_list = self
            .http_get(&format!(
                "/api/workflows/runs?limit=1000&workflow_name={}",
                self.workflow_name
            ))
            .await["runs"]
            .as_array()
            .expect("runs array")
            .iter()
            .find(|run| run["run_id"] == run_id)
            .cloned()
            .expect("run is returned by GET /api/workflows/runs");
        [
            ("WorkflowRuntime::get_run", get),
            ("WorkflowRuntime::list_runs", list),
            ("GET /api/workflows/runs/{run_id}", http_get),
            ("GET /api/workflows/runs", http_list),
        ]
    }

    /// Seed `seed` on every queue and check all four surfaces agree with the
    /// expected `started_at` / `completed_at` (`None` = JSON `null`).
    async fn check(&self, seed: RunSeed, started: Option<&str>, completed: Option<&str>) {
        for queue in QUEUES {
            let seeded = self.seed(queue, &seed).await;
            let runs = self.run_everywhere(&seeded.run_id).await;
            for (surface, run) in &runs {
                let context = format!("{surface} for a run on queue {}", seeded.queue);
                assert_timestamp(run, "started_at", started, &context);
                assert_timestamp(run, "completed_at", completed, &context);
                // Additive only: the pre-existing fields keep their meaning.
                assert_eq!(run["run_id"], seeded.run_id, "{context}");
                assert!(run["created_at"].is_string(), "{context}");
                assert!(run["updated_at"].is_string(), "{context}");
                assert_eq!(run["status"], seed.task_state, "{context}");
            }
            let (_, first) = &runs[0];
            for (surface, run) in &runs[1..] {
                assert_eq!(run, first, "{surface} differs from get_run ({queue})");
            }
        }
    }
}

fn instant(raw: &str) -> OffsetDateTime {
    OffsetDateTime::parse(raw, &Rfc3339).unwrap()
}

fn assert_timestamp(run: &Value, key: &str, expected: Option<&str>, context: &str) {
    let object = run.as_object().expect("run is a JSON object");
    let value = object
        .get(key)
        .unwrap_or_else(|| panic!("{context}: `{key}` is missing from {run}"));
    match expected {
        None => assert!(value.is_null(), "{context}: `{key}` should be null: {run}"),
        Some(expected) => {
            let actual = value
                .as_str()
                .unwrap_or_else(|| panic!("{context}: `{key}` should be a string: {run}"));
            assert_eq!(
                OffsetDateTime::parse(actual, &Rfc3339)
                    .unwrap_or_else(|e| panic!("{context}: `{key}` is not RFC 3339: {e}")),
                instant(expected),
                "{context}: `{key}`"
            );
        }
    }
}

macro_rules! skip_without_database {
    () => {
        match fixture().await {
            Some(fixture) => fixture,
            None => {
                eprintln!("skipping: set SESSION_SQLX_TEST_DATABASE_URL");
                return;
            }
        }
    };
}

// ------------------------------------------------------------------ tests

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_pending_run_has_null_started_at_and_completed_at() {
    let fx = skip_without_database!();
    fx.check(
        RunSeed {
            task_state: "pending",
            first_started_at: None,
            cancelled_at: None,
            attempts: vec![AttemptSeed {
                state: "pending",
                available_at: FAR_FUTURE,
                started_at: None,
                completed_at: None,
                failed_at: None,
            }],
        },
        None,
        None,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_running_run_has_started_at_and_null_completed_at() {
    let fx = skip_without_database!();
    fx.check(
        RunSeed {
            task_state: "running",
            first_started_at: Some(T_FIRST_START),
            cancelled_at: None,
            attempts: vec![AttemptSeed {
                state: "running",
                available_at: T_FIRST_START,
                started_at: Some(T_FIRST_START),
                completed_at: None,
                failed_at: None,
            }],
        },
        Some(T_FIRST_START),
        None,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_completed_run_has_started_at_and_completed_at() {
    let fx = skip_without_database!();
    fx.check(
        RunSeed {
            task_state: "completed",
            first_started_at: Some(T_FIRST_START),
            cancelled_at: None,
            attempts: vec![AttemptSeed {
                state: "completed",
                available_at: T_FIRST_START,
                started_at: Some(T_FIRST_START),
                completed_at: Some(T_END),
                failed_at: None,
            }],
        },
        Some(T_FIRST_START),
        Some(T_END),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_failed_run_has_completed_at_from_the_failure() {
    let fx = skip_without_database!();
    fx.check(
        RunSeed {
            task_state: "failed",
            first_started_at: Some(T_FIRST_START),
            cancelled_at: None,
            attempts: vec![AttemptSeed {
                state: "failed",
                available_at: T_FIRST_START,
                started_at: Some(T_FIRST_START),
                completed_at: None,
                failed_at: Some(T_END),
            }],
        },
        Some(T_FIRST_START),
        Some(T_END),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_cancelled_run_has_completed_at_from_the_cancellation() {
    let fx = skip_without_database!();
    fx.check(
        RunSeed {
            task_state: "cancelled",
            first_started_at: Some(T_FIRST_START),
            cancelled_at: Some(T_END),
            attempts: vec![AttemptSeed {
                state: "cancelled",
                available_at: T_FIRST_START,
                started_at: Some(T_FIRST_START),
                completed_at: None,
                failed_at: None,
            }],
        },
        Some(T_FIRST_START),
        Some(T_END),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_run_cancelled_before_starting_has_null_started_at_and_set_completed_at() {
    let fx = skip_without_database!();
    fx.check(
        RunSeed {
            task_state: "cancelled",
            first_started_at: None,
            cancelled_at: Some(T_END),
            attempts: vec![AttemptSeed {
                state: "cancelled",
                available_at: T_ENQUEUED,
                started_at: None,
                completed_at: None,
                failed_at: None,
            }],
        },
        None,
        Some(T_END),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_retried_running_run_keeps_its_first_start_and_has_no_completion() {
    let fx = skip_without_database!();
    // Attempt 1 failed at T_END; attempt 2 started later at T_RETRY_START.
    fx.check(
        RunSeed {
            task_state: "running",
            first_started_at: Some(T_FIRST_START),
            cancelled_at: None,
            attempts: vec![
                AttemptSeed {
                    state: "failed",
                    available_at: T_FIRST_START,
                    started_at: Some(T_FIRST_START),
                    completed_at: None,
                    failed_at: Some(T_END),
                },
                AttemptSeed {
                    state: "running",
                    available_at: T_RETRY_START,
                    started_at: Some(T_RETRY_START),
                    completed_at: None,
                    failed_at: None,
                },
            ],
        },
        Some(T_FIRST_START),
        None,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_run_awaiting_retry_keeps_its_first_start_and_has_no_completion() {
    let fx = skip_without_database!();
    // The failed first attempt must not make the run look finished.
    fx.check(
        RunSeed {
            task_state: "pending",
            first_started_at: Some(T_FIRST_START),
            cancelled_at: None,
            attempts: vec![
                AttemptSeed {
                    state: "failed",
                    available_at: T_FIRST_START,
                    started_at: Some(T_FIRST_START),
                    completed_at: None,
                    failed_at: Some(T_END),
                },
                AttemptSeed {
                    state: "pending",
                    available_at: FAR_FUTURE,
                    started_at: None,
                    completed_at: None,
                    failed_at: None,
                },
            ],
        },
        Some(T_FIRST_START),
        None,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_timestamps_retried_run_that_finally_completes_keeps_its_first_start() {
    let fx = skip_without_database!();
    fx.check(
        RunSeed {
            task_state: "completed",
            first_started_at: Some(T_FIRST_START),
            cancelled_at: None,
            attempts: vec![
                AttemptSeed {
                    state: "failed",
                    available_at: T_FIRST_START,
                    started_at: Some(T_FIRST_START),
                    completed_at: None,
                    failed_at: Some(T_END),
                },
                AttemptSeed {
                    state: "completed",
                    available_at: T_RETRY_START,
                    started_at: Some(T_RETRY_START),
                    completed_at: Some("2026-01-02T03:09:00Z"),
                    failed_at: None,
                },
            ],
        },
        Some(T_FIRST_START),
        Some("2026-01-02T03:09:00Z"),
    )
    .await;
}
