//! Story: `GET /api/workflows/runs` returns `next_cursor` beside `runs` on every
//! response: `null` when no matching run lies past the page (decided by reading
//! one row more than `limit`), otherwise a URL-safe opaque string naming the last
//! returned run's exact microsecond `created_at` and `task_id`. A filtered
//! request computes it from the matching runs only.
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
        unreachable!("runs next cursor tests never open sandbox I/O")
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

struct Page {
    runs: Vec<Value>,
    body: Value,
}

impl Page {
    fn ids(&self) -> Vec<String> {
        run_ids(&self.runs)
    }

    /// The `next_cursor` member, `None` only when the key is absent.
    fn next_cursor(&self) -> Option<&Value> {
        self.body.get("next_cursor")
    }

    fn assert_last_page(&self, context: &str) {
        assert_eq!(
            self.next_cursor(),
            Some(&Value::Null),
            "{context}: the last page carries `next_cursor: null`: {}",
            self.body
        );
    }

    fn cursor(&self, context: &str) -> String {
        let cursor = self
            .next_cursor()
            .unwrap_or_else(|| panic!("{context}: response has no `next_cursor`: {}", self.body));
        let cursor = cursor
            .as_str()
            .unwrap_or_else(|| panic!("{context}: `next_cursor` should be a string: {cursor}"));
        assert!(!cursor.is_empty(), "{context}: `next_cursor` is empty");
        assert!(
            cursor
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~')),
            "{context}: `next_cursor` must use URL-unreserved characters only: {cursor}"
        );
        cursor.to_owned()
    }
}

async fn page(router: &Router, query: &str) -> Page {
    let uri = format!("/api/workflows/runs?{query}");
    let (status, body) = get(router, &uri).await;
    assert_eq!(status, StatusCode::OK, "GET {uri}: {body}");
    let body: Value = serde_json::from_str(&body).unwrap();
    let runs = body["runs"].as_array().expect("runs array").clone();
    Page { runs, body }
}

fn run_ids(runs: &[Value]) -> Vec<String> {
    runs.iter()
        .map(|run| run["run_id"].as_str().expect("run_id").to_owned())
        .collect()
}

#[derive(Clone)]
struct Seeded {
    run_id: String,
    task_id: String,
    state: &'static str,
    /// Sort key: minutes since BASE, then extra microseconds.
    at: (i32, i32),
}

/// Newest first: `created_at` descending, then `task_id` descending.
fn newest_first(mut runs: Vec<Seeded>) -> Vec<Seeded> {
    runs.sort_by(|a, b| b.at.cmp(&a.at).then(b.task_id.cmp(&a.task_id)));
    runs
}

fn ids(runs: &[Seeded]) -> Vec<String> {
    runs.iter().map(|run| run.run_id.clone()).collect()
}

/// Inserts a one-attempt task in `queue` whose `created_at` (the task's
/// `enqueue_at`) is `BASE + minute` minutes plus `micros` microseconds.
/// Pending and sleeping runs get a far-future `available_at` so the live
/// workers of any `WorkflowRuntime` sharing the database cannot claim them.
async fn seed(
    pool: &sqlx::PgPool,
    queue: &str,
    workflow_name: &str,
    state: &'static str,
    minute: i32,
    micros: i32,
) -> Seeded {
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
                 '{BASE}'::timestamptz + make_interval(mins => $3) + $8 * interval '1 microsecond',
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
    .bind(f64::from(micros))
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
    .bind((state == "running").then_some("runs-next-cursor-test"))
    .bind((state == "running").then_some(FAR_FUTURE))
    .bind(minute)
    .bind(started)
    .bind((state == "completed").then(|| json!({"ok": true})))
    .bind((state == "failed").then(|| json!({"name": "Error", "message": "boom"})))
    .bind(FAR_FUTURE)
    .execute(pool)
    .await
    .expect("insert run");
    Seeded {
        run_id,
        task_id,
        state,
        at: (minute, micros),
    }
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

/// Seeds `per_queue` completed runs on every queue under one fresh workflow
/// name, run `k` of queue `i` at minute `k * 4 + i` (every run has its own
/// minute, interleaved across queues), newest first.
async fn seed_interleaved(pool: &sqlx::PgPool, per_queue: i32) -> (String, Vec<Seeded>) {
    let workflow_name = unique_name("runs_next_cursor");
    let mut runs = Vec::new();
    for k in 0..per_queue {
        for (i, queue) in QUEUES.into_iter().enumerate() {
            let minute = k * 4 + i as i32;
            runs.push(seed(pool, queue, &workflow_name, "completed", minute, 0).await);
        }
    }
    (workflow_name, newest_first(runs))
}

macro_rules! database_or_skip {
    () => {{
        let Some(url) = database_url() else {
            eprintln!("skipping runs next cursor tests: set SESSION_SQLX_TEST_DATABASE_URL");
            return;
        };
        url
    }};
}

#[tokio::test]
async fn runs_next_cursor_first_page_over_several_queues_has_todays_runs_and_a_string_cursor() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 3).await;
    assert_eq!(runs.len(), 12);

    for limit in [1usize, 5, 11] {
        let first = page(
            &harness.router,
            &format!("limit={limit}&workflow_name={workflow_name}"),
        )
        .await;
        assert_eq!(first.ids(), ids(&runs[..limit]), "limit {limit}");
        first.cursor(&format!("limit {limit} leaves {} runs", 12 - limit));
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_next_cursor_default_limit_page_keeps_the_body_shape_and_adds_only_next_cursor() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 14).await;
    assert_eq!(runs.len(), 56);

    let first = page(&harness.router, &format!("workflow_name={workflow_name}")).await;
    assert_eq!(first.ids(), ids(&runs[..50]), "default limit is 50");
    first.cursor("56 runs, default limit 50");
    let mut keys: Vec<&str> = first
        .body
        .as_object()
        .expect("body object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["next_cursor", "ok", "runs"]);
    assert_eq!(first.body["ok"], json!(true));
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_next_cursor_is_null_on_a_page_holding_all_the_runs() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 3).await;

    let roomy = page(
        &harness.router,
        &format!("limit=100&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(roomy.ids(), ids(&runs));
    roomy.assert_last_page("12 runs, limit 100");
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_next_cursor_is_null_when_the_page_holds_exactly_the_remaining_runs() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 3).await;

    let exact = page(
        &harness.router,
        &format!("limit=12&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(exact.ids(), ids(&runs));
    exact.assert_last_page("12 runs, limit 12");

    // The newest eight runs are all that remain at or after this bound.
    let bound = "2024-01-01T00:08:00Z";
    let remaining = page(
        &harness.router,
        &format!("limit=8&workflow_name={workflow_name}&created_before={bound}"),
    )
    .await;
    assert_eq!(remaining.ids(), ids(&runs[4..]));
    remaining.assert_last_page("8 runs before the bound, limit 8");
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_next_cursor_is_null_when_the_total_is_an_exact_multiple_of_limit() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 2).await;
    assert_eq!(runs.len(), 8);

    // A total of 8 against limits that divide it: only the last page of a walk
    // is null, and the first page of limit 8 is also the last.
    let whole = page(
        &harness.router,
        &format!("limit=8&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(whole.ids(), ids(&runs));
    whole.assert_last_page("8 runs, limit 8");

    // Windows of 4 runs: a page of exactly limit runs with nothing after it.
    let tail = page(
        &harness.router,
        &format!("limit=4&workflow_name={workflow_name}&created_before=2024-01-01T00:04:00Z"),
    )
    .await;
    assert_eq!(tail.ids(), ids(&runs[4..]));
    tail.assert_last_page("4 runs before the bound, limit 4");

    let head = page(
        &harness.router,
        &format!("limit=4&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(head.ids(), ids(&runs[..4]));
    head.cursor("8 runs, limit 4");
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_next_cursor_is_null_for_an_empty_result() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, _runs) = seed_interleaved(&harness.pool, 1).await;

    let unknown = page(
        &harness.router,
        &format!(
            "limit=10&workflow_name={}",
            unique_name("runs_next_cursor_none")
        ),
    )
    .await;
    assert_eq!(unknown.runs, Vec::<Value>::new());
    unknown.assert_last_page("no run has that workflow name");

    let before_all = page(
        &harness.router,
        &format!("limit=10&workflow_name={workflow_name}&created_before=2023-12-31T00:00:00Z"),
    )
    .await;
    assert_eq!(before_all.runs, Vec::<Value>::new());
    before_all.assert_last_page("bound earlier than every run");

    let no_such_state = page(
        &harness.router,
        &format!("limit=10&workflow_name={workflow_name}&state=cancelled"),
    )
    .await;
    assert_eq!(no_such_state.runs, Vec::<Value>::new());
    no_such_state.assert_last_page("no cancelled run");
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_next_cursor_filtered_request_counts_only_the_matching_runs() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_next_cursor");
    let other_name = unique_name("runs_next_cursor_other");
    let mut mine = Vec::new();
    for (i, queue) in QUEUES.into_iter().enumerate() {
        let i = i as i32;
        // Newer and older runs of the same name in other states, and runs of
        // another workflow on both sides, all non-matching for the requests below.
        for (name, state, minute) in [
            (&workflow_name, "running", 20 + i),
            (&workflow_name, "running", 40 + i),
            (&workflow_name, "failed", 10 + i),
            (&workflow_name, "failed", 30 + i),
            (&workflow_name, "failed", 50 + i),
            (&other_name, "running", 5 + i),
            (&other_name, "running", 21 + i),
            (&other_name, "running", 60 + i),
        ] {
            let run = seed(&harness.pool, queue, name, state, minute, 0).await;
            if *name == workflow_name {
                mine.push(run);
            }
        }
    }
    let running: Vec<Seeded> = newest_first(
        mine.iter()
            .filter(|run| run.state == "running")
            .cloned()
            .collect(),
    );
    assert_eq!(running.len(), 8);

    let all = page(
        &harness.router,
        &format!("state=running&limit=8&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(all.ids(), ids(&running));
    all.assert_last_page("8 matching running runs, limit 8, other runs exist");

    let short = page(
        &harness.router,
        &format!("state=running&limit=7&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(short.ids(), ids(&running[..7]));
    short.cursor("8 matching running runs, limit 7");

    let by_name = page(
        &harness.router,
        &format!("limit=20&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(by_name.ids(), ids(&newest_first(mine.clone())));
    by_name.assert_last_page("20 runs of this name, limit 20, other workflow exists");

    let other = page(
        &harness.router,
        &format!("state=running&limit=12&workflow_name={other_name}"),
    )
    .await;
    assert_eq!(other.runs.len(), 12);
    other.assert_last_page("12 runs of the other workflow, limit 12");

    // A window that leaves one matching run past the page, among unmatched ones.
    let windowed = page(
        &harness.router,
        &format!(
            "state=running&limit=3&workflow_name={workflow_name}&created_after=2024-01-01T00:19:00Z&created_before=2024-01-01T00:42:00Z"
        ),
    )
    .await;
    assert_eq!(windowed.ids(), ids(&running[2..5]));
    windowed.cursor("6 running runs in a window, limit 3");

    let window_last = page(
        &harness.router,
        &format!(
            "state=running&limit=4&workflow_name={workflow_name}&created_after=2024-01-01T00:19:00Z&created_before=2024-01-01T00:25:00Z"
        ),
    )
    .await;
    assert_eq!(window_last.ids(), ids(&running[4..8]));
    window_last.assert_last_page("4 running runs in a window, limit 4");
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_next_cursor_names_the_last_returned_run_so_it_differs_between_pages() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 3).await;

    let mut seen = Vec::new();
    for limit in 1..runs.len() {
        let first = page(
            &harness.router,
            &format!("limit={limit}&workflow_name={workflow_name}"),
        )
        .await;
        assert_eq!(first.ids(), ids(&runs[..limit]), "limit {limit}");
        seen.push(first.cursor(&format!("limit {limit}")));
    }
    let mut distinct = seen.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        seen.len(),
        "each last returned run gets its own cursor: {seen:?}"
    );

    let again = page(
        &harness.router,
        &format!("limit=5&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(
        again.cursor("repeat"),
        seen[4],
        "the cursor depends only on the position"
    );
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_next_cursor_tells_apart_runs_sharing_a_created_at_and_microsecond_neighbours() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_next_cursor");
    let mut runs = Vec::new();
    // Four runs at one instant spread over the queues, then three runs one
    // microsecond apart.
    for queue in QUEUES {
        runs.push(seed(&harness.pool, queue, &workflow_name, "completed", 7, 0).await);
    }
    for micros in [1, 2, 3] {
        runs.push(
            seed(
                &harness.pool,
                WORKFLOW_QUEUE,
                &workflow_name,
                "completed",
                9,
                micros,
            )
            .await,
        );
    }
    let runs = newest_first(runs);
    assert_eq!(runs.len(), 7);

    let mut seen = Vec::new();
    for limit in 1..runs.len() {
        let first = page(
            &harness.router,
            &format!("limit={limit}&workflow_name={workflow_name}"),
        )
        .await;
        assert_eq!(first.ids(), ids(&runs[..limit]), "limit {limit}");
        seen.push(first.cursor(&format!("limit {limit}")));
    }
    let mut distinct = seen.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        seen.len(),
        "tied and microsecond-apart runs each get their own cursor: {seen:?}"
    );
    let last = page(
        &harness.router,
        &format!("limit=7&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(last.ids(), ids(&runs));
    last.assert_last_page("7 runs, limit 7");
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_next_cursor_marks_more_runs_past_the_limit_clamp_of_1000() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_next_cursor");
    let mut runs = Vec::new();
    for n in 0..1001i32 {
        let queue = QUEUES[(n % 4) as usize];
        runs.push(seed(&harness.pool, queue, &workflow_name, "completed", n, 0).await);
    }
    let runs = newest_first(runs);

    for limit in ["1000", "5000"] {
        let first = page(
            &harness.router,
            &format!("limit={limit}&workflow_name={workflow_name}"),
        )
        .await;
        assert_eq!(first.runs.len(), 1000, "limit {limit} is clamped to 1000");
        assert_eq!(first.ids(), ids(&runs[..1000]), "limit {limit}");
        first.cursor(&format!("limit {limit}: the oldest run lies past the page"));
    }

    let newest_1000 = page(
        &harness.router,
        &format!("limit=1000&workflow_name={workflow_name}&created_after=2024-01-01T00:00:00Z"),
    )
    .await;
    assert_eq!(newest_1000.ids(), ids(&runs[..1000]));
    newest_1000.assert_last_page("1000 runs after the oldest, limit 1000");

    let one = page(
        &harness.router,
        &format!("limit=0&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(one.ids(), ids(&runs[..1]), "limit 0 is clamped to 1");
    one.cursor("limit 0 clamped to 1");
    harness.pool.close().await;
}
