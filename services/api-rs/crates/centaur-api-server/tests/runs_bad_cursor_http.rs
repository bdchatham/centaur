//! Story: `GET /api/workflows/runs?cursor=<value>` answers a `cursor` that
//! cannot be decoded (garbage, empty, truncated, wrong shape, a `task_id` that
//! is not a UUID, a timestamp out of range) with HTTP 400 whose message names
//! `cursor`, decided before the workflow runtime lookup and before any SQL; it
//! is never an empty page, a 5xx, or a restart from the newest run.
//!
//! Everything goes through the HTTP route of a router built with
//! `build_router_with_session_and_workflow_runtime`; no type added by the story
//! is used. The DB-free tests use a router without a workflow runtime; the
//! database-backed ones seed runs straight into the Absurd queue tables.

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
        unreachable!("bad cursor tests never open sandbox I/O")
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
        sqlx::PgPool::connect_lazy("postgres://postgres:postgres@127.0.0.1:1/runs_bad_cursor")
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

fn run_ids(runs: &[Value]) -> Vec<String> {
    runs.iter()
        .map(|run| run["run_id"].as_str().expect("run_id").to_owned())
        .collect()
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
    .bind((state == "running").then_some("runs-bad-cursor-test"))
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

fn query_value(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

const PRIOR: &str = "123e4567-e89b-42d3-a456-426614174000";
const MICROS: &str = "1704067200000000";
/// 4800 BC, inside `time`'s date range but before Postgres's earliest timestamp.
const BEFORE_POSTGRES_MIN: &str = "-213640588800000000";
/// 9000 BC.
const FAR_BEFORE_POSTGRES_MIN: &str = "-346179830400000000";

/// URL-safe base64 without padding, the alphabet a cursor is issued in.
fn b64(text: &str) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in text.as_bytes().chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |acc, (i, b)| acc | (u32::from(*b) << (16 - 8 * i)));
        for i in 0..=chunk.len() {
            out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
    }
    out
}

fn cursor_of(micros: &str, task_id: &str) -> String {
    b64(&format!("{micros}:{task_id}"))
}

async fn assert_bad_cursor_is_400(router: &Router, query: &str) {
    let uri = format!("/api/workflows/runs?{query}");
    let (status, body) = get(router, &uri).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "GET {uri}: {body}");
    let json: Value = serde_json::from_str(&body)
        .unwrap_or_else(|error| panic!("GET {uri}: body is not JSON ({error}): {body}"));
    assert_eq!(json["ok"], json!(false), "GET {uri}: {body}");
    assert!(
        json["error"].as_str().is_some_and(|m| m.contains("cursor")),
        "GET {uri}: 400 message should name `cursor`: {body}"
    );
    assert!(json.get("runs").is_none(), "GET {uri}: no page: {body}");
}

async fn assert_all_bad_cursors_are_400(router: &Router, extra: &str, cursors: &[String]) {
    for cursor in cursors {
        let query = format!("{extra}cursor={}", query_value(cursor));
        assert_bad_cursor_is_400(router, &query).await;
    }
}

fn garbage_cursors() -> Vec<String> {
    ["garbage", "not a cursor!!", "%%%", "....", "0", "null"]
        .map(str::to_owned)
        .to_vec()
}

fn empty_cursors() -> Vec<String> {
    vec![String::new()]
}

fn truncated_cursors() -> Vec<String> {
    let full = cursor_of(MICROS, PRIOR);
    vec![
        full[..full.len() / 2].to_owned(),
        full[..full.len() - 1].to_owned(),
        full[..1].to_owned(),
        b64(&format!("{MICROS}:{}", &PRIOR[..PRIOR.len() - 6])),
        b64(&format!("{}:{PRIOR}", &MICROS[..MICROS.len() / 2]))[..10].to_owned(),
    ]
}

fn wrong_shape_cursors() -> Vec<String> {
    vec![
        b64(MICROS),
        b64("hello:world"),
        b64(":"),
        b64(&format!(":{PRIOR}")),
        b64(&format!("{MICROS}:{PRIOR}:extra")),
        b64(&format!("{MICROS}-{PRIOR}")),
        b64(&format!("abc:{PRIOR}")),
        b64(&format!("1.5:{PRIOR}")),
        b64(&format!("{PRIOR}:{MICROS}")),
        format!("{MICROS}:{PRIOR}"),
        b64(&format!(
            "{{\"created_at\":{MICROS},\"task_id\":\"{PRIOR}\"}}"
        )),
    ]
}

fn non_uuid_task_id_cursors() -> Vec<String> {
    vec![
        cursor_of(MICROS, "not-a-uuid"),
        cursor_of(MICROS, ""),
        cursor_of(MICROS, "123"),
        cursor_of(MICROS, "123e4567-e89b-42d3-a456-42661417400g"),
        cursor_of(MICROS, "'; select 1; --"),
    ]
}

fn out_of_range_timestamp_cursors() -> Vec<String> {
    vec![
        cursor_of("1000000000000000000000000000000", PRIOR),
        cursor_of("-1000000000000000000000000000000", PRIOR),
        cursor_of("99999999999999999999999999999999999999999999", PRIOR),
        cursor_of("253402300800000000", PRIOR),
        cursor_of(BEFORE_POSTGRES_MIN, PRIOR),
        cursor_of(FAR_BEFORE_POSTGRES_MIN, PRIOR),
    ]
}

fn all_bad_cursors() -> Vec<String> {
    [
        garbage_cursors(),
        empty_cursors(),
        truncated_cursors(),
        wrong_shape_cursors(),
        non_uuid_task_id_cursors(),
        out_of_range_timestamp_cursors(),
    ]
    .concat()
}

macro_rules! database_or_skip {
    () => {{
        let Some(url) = database_url() else {
            eprintln!("skipping runs bad cursor tests: set SESSION_SQLX_TEST_DATABASE_URL");
            return;
        };
        url
    }};
}

#[tokio::test]
async fn runs_bad_cursor_garbage_is_400_naming_cursor() {
    assert_all_bad_cursors_are_400(&router_without_workflow_runtime(), "", &garbage_cursors())
        .await;
}

#[tokio::test]
async fn runs_bad_cursor_empty_is_400_naming_cursor() {
    assert_all_bad_cursors_are_400(&router_without_workflow_runtime(), "", &empty_cursors()).await;
}

#[tokio::test]
async fn runs_bad_cursor_truncated_is_400_naming_cursor() {
    assert_all_bad_cursors_are_400(&router_without_workflow_runtime(), "", &truncated_cursors())
        .await;
}

#[tokio::test]
async fn runs_bad_cursor_wrong_shape_is_400_naming_cursor() {
    assert_all_bad_cursors_are_400(
        &router_without_workflow_runtime(),
        "",
        &wrong_shape_cursors(),
    )
    .await;
}

#[tokio::test]
async fn runs_bad_cursor_non_uuid_task_id_is_400_naming_cursor() {
    assert_all_bad_cursors_are_400(
        &router_without_workflow_runtime(),
        "",
        &non_uuid_task_id_cursors(),
    )
    .await;
}

#[tokio::test]
async fn runs_bad_cursor_timestamp_out_of_range_is_400_naming_cursor() {
    assert_all_bad_cursors_are_400(
        &router_without_workflow_runtime(),
        "",
        &out_of_range_timestamp_cursors(),
    )
    .await;
}

#[tokio::test]
async fn runs_bad_cursor_before_postgres_minimum_timestamp_is_400_naming_cursor() {
    assert_all_bad_cursors_are_400(
        &router_without_workflow_runtime(),
        "",
        &[cursor_of(BEFORE_POSTGRES_MIN, PRIOR)],
    )
    .await;
}

#[tokio::test]
async fn runs_bad_cursor_is_400_even_with_other_filters() {
    assert_all_bad_cursors_are_400(
        &router_without_workflow_runtime(),
        "limit=5&workflow_name=anything&state=running&created_after=2024-01-01T00:00:00Z&created_before=2025-01-01T00:00:00Z&",
        &[
            "garbage".to_owned(),
            String::new(),
            cursor_of(MICROS, "not-a-uuid"),
            cursor_of(BEFORE_POSTGRES_MIN, PRIOR),
        ],
    )
    .await;
}

#[tokio::test]
async fn runs_bad_cursor_is_400_before_the_workflow_runtime_lookup() {
    let router = router_without_workflow_runtime();
    let (status, body) = get(&router, "/api/workflows/runs?cursor=garbage").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        !body.contains("workflow runtime"),
        "a bad cursor is reported before the runtime lookup: {body}"
    );
}

#[tokio::test]
async fn runs_bad_cursor_with_a_runtime_is_never_a_page_or_a_server_error() {
    let url = database_or_skip!();
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_bad_cursor");
    for k in 0..3 {
        seed(
            &harness.pool,
            WORKFLOW_QUEUE,
            &workflow_name,
            "completed",
            k,
        )
        .await;
    }
    let filters = format!("limit=2&workflow_name={}&", query_value(&workflow_name));
    assert_all_bad_cursors_are_400(&harness.router, &filters, &all_bad_cursors()).await;
}

#[tokio::test]
async fn runs_bad_cursor_leaves_the_next_valid_cursor_walk_intact() {
    let url = database_or_skip!();
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_bad_cursor");
    let mut runs = Vec::new();
    for k in 0..3 {
        runs.push(
            seed(
                &harness.pool,
                WORKFLOW_QUEUE,
                &workflow_name,
                "completed",
                k,
            )
            .await,
        );
    }
    runs.reverse();
    let name = query_value(&workflow_name);
    let uri = format!("/api/workflows/runs?limit=2&workflow_name={name}");
    let (status, body) = get(&harness.router, &uri).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let first: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(run_ids(first["runs"].as_array().unwrap()), runs[..2]);
    let cursor = first["next_cursor"].as_str().expect("next_cursor string");

    assert_bad_cursor_is_400(
        &harness.router,
        &format!(
            "limit=2&workflow_name={name}&cursor={}",
            query_value(&format!("{cursor}x"))
        ),
    )
    .await;

    let (status, body) = get(&harness.router, &format!("{uri}&cursor={cursor}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let second: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(run_ids(second["runs"].as_array().unwrap()), runs[2..]);
    assert_eq!(second["next_cursor"], Value::Null, "{body}");
}

#[tokio::test]
async fn runs_bad_cursor_valid_cursor_with_invalid_state_still_reports_the_state_error() {
    let url = database_or_skip!();
    let harness = harness(&url).await;
    let workflow_name = unique_name("runs_bad_cursor");
    for k in 0..3 {
        seed(
            &harness.pool,
            WORKFLOW_QUEUE,
            &workflow_name,
            "completed",
            k,
        )
        .await;
    }
    let name = query_value(&workflow_name);
    let (status, body) = get(
        &harness.router,
        &format!("/api/workflows/runs?limit=1&workflow_name={name}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let first: Value = serde_json::from_str(&body).unwrap();
    let cursor = first["next_cursor"].as_str().expect("next_cursor string");

    let uri =
        format!("/api/workflows/runs?limit=1&workflow_name={name}&state=bogus&cursor={cursor}");
    let (status, body) = get(&harness.router, &uri).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "GET {uri}: {body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["ok"], json!(false), "{body}");
    let message = json["error"].as_str().unwrap_or_default();
    assert!(message.contains("state"), "names `state`: {body}");
    assert!(
        !message.contains("cursor"),
        "valid cursor is not blamed: {body}"
    );
    assert!(json.get("runs").is_none(), "no page: {body}");
}
