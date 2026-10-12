use super::*;
use absurd::WorkBatchOptions;
use time::Duration as TimeDuration;
use tokio::sync::Mutex;

static SERIAL: Mutex<()> = Mutex::const_new(());
const PROBE_TASK: &str = "mana_red_probe";

async fn completed_run_fixture() -> Option<(WorkflowRuntime, String, String, OffsetDateTime)> {
    let url = std::env::var("SESSION_SQLX_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("SESSION_RUNTIME_TEST_DATABASE_URL"))
        .or_else(|_| std::env::var("ABSURD_TEST_DATABASE_URL"))
        .ok()?;
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
        sqlx::raw_sql(include_str!(
            "../../centaur-session-sqlx/migrations/0008_absurd_complete_run_clears_failure.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }

    let make_client = |queue: &str| {
        Client::from_pool_with_options(
            pool.clone(),
            ClientOptions {
                queue_name: queue.to_owned(),
                ..ClientOptions::default()
            },
        )
        .unwrap()
    };
    let client = make_client(WORKFLOW_QUEUE);
    client
        .create_queue(Some(WORKFLOW_QUEUE), CreateQueueOptions::default())
        .await
        .unwrap();
    for queue in [
        WORKFLOW_SLACK_LIVE_QUEUE,
        WORKFLOW_ETL_QUEUE,
        WORKFLOW_ETL_BACKFILL_QUEUE,
    ] {
        make_client(queue)
            .create_queue(Some(queue), CreateQueueOptions::default())
            .await
            .unwrap();
    }
    client
        .register_task(PROBE_TASK, |_input: Value, _ctx: TaskContext| async move {
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            Ok(json!({"done": true}))
        })
        .unwrap();

    // The runtime only reads the fixed workflow tables through `pool()`;
    // its workers are parked on private queues so they never claim the probe.
    let mut idle = Vec::new();
    for name in ["a", "b", "c", "d", "e"] {
        let queue = format!("mana_red_idle_{name}_{}", uuid::Uuid::new_v4().simple());
        let idle_client = make_client(&queue);
        idle_client
            .create_queue(Some(&queue), CreateQueueOptions::default())
            .await
            .unwrap();
        let worker = idle_client.start_worker(WorkerOptions::default());
        idle.push((idle_client, worker));
    }
    let mut idle = idle.into_iter();
    let mut next = || idle.next().unwrap();
    let (slack_live_client, slack_live_worker) = next();
    let (etl_client, etl_worker) = next();
    let (etl_backfill_client, etl_backfill_worker) = next();
    let (_schedule_client, schedule_worker) = next();
    let (_spare_client, worker) = next();
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

    let spawn = client
        .spawn(PROBE_TASK, &json!({"input": {}}), SpawnOptions::default())
        .await
        .unwrap();
    client
        .work_batch(WorkBatchOptions::default())
        .await
        .unwrap();

    let run = runtime.get_run(&spawn.run_id).await.unwrap();
    let completed_at: OffsetDateTime = sqlx::query_scalar(
        "select completed_at from absurd.r_centaur_workflows where run_id = $1::uuid",
    )
    .bind(&spawn.run_id)
    .fetch_one(pool_of(&runtime))
    .await
    .unwrap();
    assert_eq!(run.status, "completed");
    Some((runtime, spawn.run_id, spawn.task_id, completed_at))
}

fn pool_of(runtime: &WorkflowRuntime) -> &sqlx::PgPool {
    runtime.inner.client.pool()
}

#[tokio::test]
async fn mana_red_get_run_updated_at_reflects_completion_time() {
    let _guard = SERIAL.lock().await;
    let Some((runtime, run_id, _task_id, completed_at)) = completed_run_fixture().await else {
        eprintln!("skipping: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let run = runtime.get_run(&run_id).await.unwrap();
    assert!(
        run.updated_at + TimeDuration::milliseconds(1) >= completed_at,
        "updated_at {} is earlier than completed_at {completed_at}",
        run.updated_at
    );
    assert!(
        run.updated_at - run.created_at >= TimeDuration::milliseconds(1_000),
        "updated_at {} stayed near created_at {}",
        run.updated_at,
        run.created_at
    );
}

#[tokio::test]
async fn mana_red_list_runs_updated_at_reflects_completion_time() {
    let _guard = SERIAL.lock().await;
    let Some((runtime, _run_id, task_id, completed_at)) = completed_run_fixture().await else {
        eprintln!("skipping: set SESSION_SQLX_TEST_DATABASE_URL");
        return;
    };
    let runs = runtime.list_runs(1_000, None).await.unwrap();
    let run = runs
        .iter()
        .find(|run| run.task_id == task_id)
        .expect("completed probe run is listed");
    assert!(
        run.updated_at + TimeDuration::milliseconds(1) >= completed_at,
        "updated_at {} is earlier than completed_at {completed_at}",
        run.updated_at
    );
    assert!(
        run.updated_at - run.created_at >= TimeDuration::milliseconds(1_000),
        "updated_at {} stayed near created_at {}",
        run.updated_at,
        run.created_at
    );
}
