//! Story: `GET /api/workflows/runs` accepts an optional `cursor` (a previous
//! response's `next_cursor`) and returns the next `limit` runs strictly after the
//! position it names, in the merged `created_at` descending, `task_id` descending
//! order, combined with every filter.
//!
//! Everything goes through the HTTP route of a router built with
//! `build_router_with_session_and_workflow_runtime`, following `next_cursor` with
//! fixed filters and `limit`; no type added by the story is used. Runs are
//! seeded straight into the Absurd queue tables at known times.

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
        unreachable!("runs cursor tests never open sandbox I/O")
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
    .bind((state == "running").then_some("runs-cursor-test"))
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
    let workflow_name = unique_name("runs_cursor");
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
            eprintln!("skipping runs cursor tests: set SESSION_SQLX_TEST_DATABASE_URL");
            return;
        };
        url
    }};
}

/// Follows `next_cursor` from the first page of `query` (filters and `limit`
/// fixed across pages, as a client sends them) until it is `null`. Returns every
/// page's run ids in order.
async fn walk(router: &Router, query: &str) -> Vec<Vec<String>> {
    let mut pages = Vec::new();
    let mut current = page(router, query).await;
    for _ in 0..2_000 {
        pages.push(current.ids());
        match current.next_cursor() {
            Some(Value::Null) => return pages,
            _ => {
                let cursor = current.cursor(&format!("page {}", pages.len()));
                current = page(router, &format!("{query}&cursor={cursor}")).await;
            }
        }
    }
    panic!("the walk of `{query}` did not end on a null next_cursor");
}

fn flatten(pages: &[Vec<String>]) -> Vec<String> {
    pages.iter().flatten().cloned().collect()
}

fn assert_each_page_full_but_the_last(pages: &[Vec<String>], limit: usize) {
    let (last, full) = pages.split_last().expect("at least one page");
    for (n, page) in full.iter().enumerate() {
        assert_eq!(page.len(), limit, "page {n} of the walk holds `limit` runs");
    }
    assert!(
        !last.is_empty() && last.len() <= limit,
        "the last page holds 1..=limit runs, not {}",
        last.len()
    );
}

#[tokio::test]
async fn runs_cursor_walk_over_more_than_1000_runs_returns_each_once_in_order() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_cursor");
    let mut runs = Vec::new();
    for n in 0..1105i32 {
        let queue = QUEUES[(n % 4) as usize];
        // Every fifth run shares its created_at with the one before it.
        runs.push(
            seed(
                &harness.pool,
                queue,
                &workflow_name,
                "completed",
                n - n % 5 / 4,
                0,
            )
            .await,
        );
    }
    let runs = newest_first(runs);
    assert_eq!(runs.len(), 1105);

    // A page of 1000 and a second page with the remaining 105.
    let pages = walk(
        &harness.router,
        &format!("limit=1000&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(pages.len(), 2, "1105 runs at limit 1000 are two pages");
    assert_eq!(pages[0], ids(&runs[..1000]));
    assert_eq!(pages[1], ids(&runs[1000..]));

    let pages = walk(
        &harness.router,
        &format!("limit=300&workflow_name={workflow_name}"),
    )
    .await;
    assert_each_page_full_but_the_last(&pages, 300);
    assert_eq!(pages.len(), 4);
    assert_eq!(flatten(&pages), ids(&runs));
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_cursor_second_page_starts_strictly_after_the_first() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 5).await;
    assert_eq!(runs.len(), 20);

    let first = page(
        &harness.router,
        &format!("limit=7&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(first.ids(), ids(&runs[..7]));
    let cursor = first.cursor("limit 7 of 20");
    let second = page(
        &harness.router,
        &format!("limit=7&workflow_name={workflow_name}&cursor={cursor}"),
    )
    .await;
    assert_eq!(second.ids(), ids(&runs[7..14]));
    let cursor = second.cursor("second of three pages");
    let third = page(
        &harness.router,
        &format!("limit=7&workflow_name={workflow_name}&cursor={cursor}"),
    )
    .await;
    assert_eq!(third.ids(), ids(&runs[14..]));
    third.assert_last_page("third page");

    // The run objects on a later page are the same objects as without a cursor.
    let whole = page(
        &harness.router,
        &format!("limit=20&workflow_name={workflow_name}"),
    )
    .await;
    assert_eq!(second.runs, whole.runs[7..14]);
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_cursor_runs_sharing_one_created_at_split_across_a_page_boundary_appear_once() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_cursor");
    let mut runs = Vec::new();
    // Six runs at one instant in the workflow queue alone, an older one, a newer one.
    for _ in 0..6 {
        runs.push(
            seed(
                &harness.pool,
                WORKFLOW_QUEUE,
                &workflow_name,
                "completed",
                30,
                0,
            )
            .await,
        );
    }
    runs.push(
        seed(
            &harness.pool,
            WORKFLOW_QUEUE,
            &workflow_name,
            "completed",
            31,
            0,
        )
        .await,
    );
    runs.push(
        seed(
            &harness.pool,
            WORKFLOW_QUEUE,
            &workflow_name,
            "completed",
            29,
            0,
        )
        .await,
    );
    let runs = newest_first(runs);
    assert_eq!(runs.len(), 8);

    for limit in 1..=8usize {
        let pages = walk(
            &harness.router,
            &format!("limit={limit}&workflow_name={workflow_name}"),
        )
        .await;
        assert_each_page_full_but_the_last(&pages, limit);
        assert_eq!(flatten(&pages), ids(&runs), "limit {limit}");
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_cursor_runs_sharing_one_created_at_in_different_queues_appear_once() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_cursor");
    let mut runs = Vec::new();
    // Three instants, each shared by two runs in every queue.
    for minute in [40, 41, 42] {
        for queue in QUEUES {
            for _ in 0..2 {
                runs.push(seed(&harness.pool, queue, &workflow_name, "completed", minute, 0).await);
            }
        }
    }
    let runs = newest_first(runs);
    assert_eq!(runs.len(), 24);

    for limit in [1usize, 2, 3, 5, 8, 9, 23] {
        let pages = walk(
            &harness.router,
            &format!("limit={limit}&workflow_name={workflow_name}"),
        )
        .await;
        assert_each_page_full_but_the_last(&pages, limit);
        assert_eq!(flatten(&pages), ids(&runs), "limit {limit}");
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_cursor_microsecond_apart_created_at_at_a_boundary_is_neither_lost_nor_repeated() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_cursor");
    let mut runs = Vec::new();
    // One minute holding runs 0..=4 microseconds apart, spread over the queues
    // (two share a microsecond), so a cursor truncated to milliseconds or
    // seconds would lose or repeat runs wherever the page ends.
    for (micros, queue) in [
        (0, QUEUES[0]),
        (1, QUEUES[1]),
        (1, QUEUES[2]),
        (2, QUEUES[3]),
        (3, QUEUES[0]),
        (4, QUEUES[1]),
        (5, QUEUES[3]),
        (999, QUEUES[2]),
        (1000, QUEUES[0]),
    ] {
        runs.push(
            seed(
                &harness.pool,
                queue,
                &workflow_name,
                "completed",
                50,
                micros,
            )
            .await,
        );
    }
    let runs = newest_first(runs);
    assert_eq!(runs.len(), 9);

    for limit in 1..=9usize {
        let pages = walk(
            &harness.router,
            &format!("limit={limit}&workflow_name={workflow_name}"),
        )
        .await;
        assert_each_page_full_but_the_last(&pages, limit);
        assert_eq!(flatten(&pages), ids(&runs), "limit {limit}");
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_cursor_runs_inserted_between_two_pages_leave_the_later_pages_unchanged() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 5).await;
    assert_eq!(runs.len(), 20);
    let query = format!("limit=6&workflow_name={workflow_name}");

    let first = page(&harness.router, &query).await;
    assert_eq!(first.ids(), ids(&runs[..6]));
    let cursor = first.cursor("first of four pages");

    // Newer than everything, in several queues, one at the very same instant as
    // the newest run: none of them is after the cursor.
    let newest_at = runs[0].at;
    for (i, queue) in QUEUES.into_iter().enumerate() {
        seed(
            &harness.pool,
            queue,
            &workflow_name,
            "completed",
            1000 + i as i32,
            0,
        )
        .await;
    }
    seed(
        &harness.pool,
        WORKFLOW_QUEUE,
        &workflow_name,
        "completed",
        newest_at.0,
        newest_at.1,
    )
    .await;

    let second = page(&harness.router, &format!("{query}&cursor={cursor}")).await;
    assert_eq!(second.ids(), ids(&runs[6..12]));
    let third = page(
        &harness.router,
        &format!("{query}&cursor={}", second.cursor("second page")),
    )
    .await;
    assert_eq!(third.ids(), ids(&runs[12..18]));
    let fourth = page(
        &harness.router,
        &format!("{query}&cursor={}", third.cursor("third page")),
    )
    .await;
    assert_eq!(fourth.ids(), ids(&runs[18..]));
    fourth.assert_last_page("fourth page");
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_cursor_workflow_name_filter_holds_across_pages() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_cursor");
    let other_name = unique_name("runs_cursor_other");
    let mut mine = Vec::new();
    for minute in 0..12i32 {
        let queue = QUEUES[(minute % 4) as usize];
        mine.push(
            seed(
                &harness.pool,
                queue,
                &workflow_name,
                "completed",
                2 * minute,
                0,
            )
            .await,
        );
        seed(
            &harness.pool,
            queue,
            &other_name,
            "completed",
            2 * minute + 1,
            0,
        )
        .await;
    }
    let mine = newest_first(mine);

    for limit in [1usize, 4, 5, 12] {
        let pages = walk(
            &harness.router,
            &format!("limit={limit}&workflow_name={workflow_name}"),
        )
        .await;
        assert_each_page_full_but_the_last(&pages, limit);
        assert_eq!(flatten(&pages), ids(&mine), "limit {limit}");
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_cursor_state_filter_holds_across_pages() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_cursor");
    let mut all = Vec::new();
    for minute in 0..18i32 {
        let queue = QUEUES[(minute % 4) as usize];
        let state = ["running", "failed", "completed"][(minute % 3) as usize];
        all.push(seed(&harness.pool, queue, &workflow_name, state, minute, 0).await);
    }
    let all = newest_first(all);

    for state in ["running", "failed", "completed"] {
        let matching: Vec<Seeded> = all
            .iter()
            .filter(|run| run.state == state)
            .cloned()
            .collect();
        assert_eq!(matching.len(), 6);
        for limit in [1usize, 4, 6] {
            let pages = walk(
                &harness.router,
                &format!("limit={limit}&state={state}&workflow_name={workflow_name}"),
            )
            .await;
            assert_each_page_full_but_the_last(&pages, limit);
            assert_eq!(flatten(&pages), ids(&matching), "{state}, limit {limit}");
        }
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_cursor_created_after_and_created_before_hold_across_pages() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let (workflow_name, runs) = seed_interleaved(&harness.pool, 6).await;
    assert_eq!(runs.len(), 24);
    // Run at minute m has created_at BASE + m minutes; minutes 0..=23.
    let within = |lo: i32, hi: i32| -> Vec<Seeded> {
        runs.iter()
            .filter(|run| run.at.0 > lo && run.at.0 < hi)
            .cloned()
            .collect()
    };

    for limit in [1usize, 3, 4] {
        let after = walk(
            &harness.router,
            &format!(
                "limit={limit}&workflow_name={workflow_name}&created_after=2024-01-01T00:05:00Z"
            ),
        )
        .await;
        assert_each_page_full_but_the_last(&after, limit);
        assert_eq!(flatten(&after), ids(&within(5, 24)), "after, limit {limit}");

        let before = walk(
            &harness.router,
            &format!(
                "limit={limit}&workflow_name={workflow_name}&created_before=2024-01-01T00:17:00Z"
            ),
        )
        .await;
        assert_each_page_full_but_the_last(&before, limit);
        assert_eq!(
            flatten(&before),
            ids(&within(-1, 17)),
            "before, limit {limit}"
        );

        let between = walk(
            &harness.router,
            &format!(
                "limit={limit}&workflow_name={workflow_name}&created_after=2024-01-01T00:05:00Z&created_before=2024-01-01T00:17:00Z"
            ),
        )
        .await;
        assert_each_page_full_but_the_last(&between, limit);
        assert_eq!(
            flatten(&between),
            ids(&within(5, 17)),
            "between, limit {limit}"
        );
    }
    harness.pool.close().await;
}

#[tokio::test]
async fn runs_cursor_combines_state_name_and_time_bounds_across_pages() {
    let url = database_or_skip!();
    let _serial = SETUP_LOCK.lock().await;
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_cursor");
    let other_name = unique_name("runs_cursor_other");
    let mut matching = Vec::new();
    for minute in 0..24i32 {
        let queue = QUEUES[(minute % 4) as usize];
        let state = if minute % 2 == 0 { "running" } else { "failed" };
        let run = seed(&harness.pool, queue, &workflow_name, state, minute, 0).await;
        if state == "running" && minute > 3 && minute < 20 {
            matching.push(run);
        }
        seed(&harness.pool, queue, &other_name, "running", minute, 0).await;
    }
    let matching = newest_first(matching);
    assert_eq!(matching.len(), 8);

    for limit in [1usize, 3, 4] {
        let pages = walk(
            &harness.router,
            &format!(
                "limit={limit}&state=running&workflow_name={workflow_name}&created_after=2024-01-01T00:03:00Z&created_before=2024-01-01T00:20:00Z"
            ),
        )
        .await;
        assert_each_page_full_but_the_last(&pages, limit);
        assert_eq!(flatten(&pages), ids(&matching), "limit {limit}");
    }
    harness.pool.close().await;
}
