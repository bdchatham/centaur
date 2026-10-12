use super::*;
use tokio::sync::Mutex;

static SERIAL: Mutex<()> = Mutex::const_new(());

const PROBE_TASK: &str = "updated_at_completion_probe";
const PROBE_RUN_MILLIS: u64 = 1500;

async fn probe_runtime() -> Option<(WorkflowRuntime, Client)> {
    let Ok(url) = env::var("SESSION_SQLX_TEST_DATABASE_URL")
        .or_else(|_| env::var("SESSION_RUNTIME_TEST_DATABASE_URL"))
        .or_else(|_| env::var("ABSURD_TEST_DATABASE_URL"))
    else {
        eprintln!("skipping updated_at completion tests: set SESSION_SQLX_TEST_DATABASE_URL");
        return None;
    };
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let installed: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'absurd')")
            .fetch_one(&pool)
            .await
            .unwrap();
    if !installed {
        sqlx::raw_sql(include_str!(
            "../../centaur-session-sqlx/migrations/0007_absurd_workflows.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    let make = |queue: &str| {
        Client::from_pool_with_options(
            pool.clone(),
            ClientOptions {
                queue_name: queue.to_owned(),
                ..ClientOptions::default()
            },
        )
        .unwrap()
    };
    let client = make(WORKFLOW_QUEUE);
    let slack_live_client = make(WORKFLOW_SLACK_LIVE_QUEUE);
    let etl_client = make(WORKFLOW_ETL_QUEUE);
    let etl_backfill_client = make(WORKFLOW_ETL_BACKFILL_QUEUE);
    let schedule_client = make(WORKFLOW_SCHEDULE_QUEUE);
    for (c, name) in [
        (&client, WORKFLOW_QUEUE),
        (&slack_live_client, WORKFLOW_SLACK_LIVE_QUEUE),
        (&etl_client, WORKFLOW_ETL_QUEUE),
        (&etl_backfill_client, WORKFLOW_ETL_BACKFILL_QUEUE),
        (&schedule_client, WORKFLOW_SCHEDULE_QUEUE),
    ] {
        c.create_queue(Some(name), CreateQueueOptions::default())
            .await
            .unwrap();
    }
    client
        .register_task(PROBE_TASK, |_input: Value, _ctx| async {
            tokio::time::sleep(Duration::from_millis(PROBE_RUN_MILLIS)).await;
            Ok(json!({"done": true}))
        })
        .unwrap();
    let worker = client.start_worker(WorkerOptions::default());
    let slack_live_worker = slack_live_client.start_worker(WorkerOptions::default());
    let etl_worker = etl_client.start_worker(WorkerOptions::default());
    let etl_backfill_worker = etl_backfill_client.start_worker(WorkerOptions::default());
    let schedule_worker = schedule_client.start_worker(WorkerOptions::default());
    let runtime = WorkflowRuntime {
        inner: Arc::new(WorkflowRuntimeInner {
            client: client.clone(),
            slack_live_client,
            etl_client,
            etl_backfill_client,
            _worker: worker,
            _slack_live_worker: slack_live_worker,
            _etl_worker: etl_worker,
            _etl_backfill_worker: etl_backfill_worker,
            _schedule_worker: schedule_worker,
            webhook_registry: Arc::new(RwLock::new(BTreeMap::new())),
            schedule_registry: Arc::new(RwLock::new(BTreeMap::new())),
        }),
    };
    Some((runtime, client))
}

async fn completed_probe(client: &Client) -> (String, OffsetDateTime) {
    let spawn = client
        .spawn(PROBE_TASK, json!({}), SpawnOptions::default())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let state: String =
            sqlx::query_scalar("select state from absurd.t_centaur_workflows where task_id = $1::uuid")
                .bind(&spawn.task_id)
                .fetch_one(client.pool())
                .await
                .unwrap();
        if state == "completed" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "probe task stuck in state {state}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let completed_at: OffsetDateTime = sqlx::query_scalar(
        "select completed_at from absurd.r_centaur_workflows where run_id = $1::uuid",
    )
    .bind(&spawn.run_id)
    .fetch_one(client.pool())
    .await
    .unwrap();
    (spawn.run_id, completed_at)
}

#[tokio::test]
async fn updated_at_completion_get_run_reflects_completion_time() {
    let _guard = SERIAL.lock().await;
    let Some((runtime, client)) = probe_runtime().await else {
        return;
    };
    let (run_id, completed_at) = completed_probe(&client).await;
    let run = runtime.get_run(&run_id).await.unwrap();
    assert_eq!(run.status, "completed");
    assert!(
        run.updated_at >= completed_at,
        "get_run updated_at {} predates completed_at {completed_at}",
        run.updated_at
    );
}

#[tokio::test]
async fn updated_at_completion_list_runs_reflects_completion_time() {
    let _guard = SERIAL.lock().await;
    let Some((runtime, client)) = probe_runtime().await else {
        return;
    };
    let (run_id, completed_at) = completed_probe(&client).await;
    let runs = runtime.list_runs(100, None).await.unwrap();
    let run = runs
        .iter()
        .find(|run| run.run_id == run_id)
        .expect("completed run is listed");
    assert_eq!(run.status, "completed");
    assert!(
        run.updated_at >= completed_at,
        "list_runs updated_at {} predates completed_at {completed_at}",
        run.updated_at
    );
}
