//! Acknowledged delete tasks must survive concurrent writers.
//!
//! THE DEFECT THESE GUARD. v1 kept every delete task of a namespace in one JSON
//! ledger document. Submission read the whole ledger, appended and replaced it;
//! execution replaced it again from a vector loaded before the run, at every
//! state transition. Two query replicas could therefore acknowledge two GDPR
//! deletion requests (HTTP 201 each) and keep only one, and a compactor's
//! status write could erase a task submitted while it worked. Each task now
//! owns one object keyed by its uuid, so no writer holds another writer's key.

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};

use siglake_core::index_config::{FieldType, IndexConfig};
use siglake_core::{events_to_record_batch, Event};
use siglake_storage::iceberg::{DeleteTaskState, IcebergContext};

use crate::fixture_clock::fixture_base;

fn logs_index(index_id: &str) -> IndexConfig {
    let mut config = IndexConfig::builtin_events();
    config.index_id = index_id.to_string();
    config
}

fn bloom_columns(config: &IndexConfig) -> Vec<String> {
    config
        .doc_mapping
        .tag_fields
        .iter()
        .filter_map(|name| {
            config
                .doc_mapping
                .field_mappings
                .iter()
                .find(|field| field.name == *name)
                .and_then(|field| match &field.field_type {
                    FieldType::Text { .. } | FieldType::Json => Some(name.clone()),
                    _ => None,
                })
        })
        .collect()
}

fn event_at(ts: chrono::DateTime<Utc>, host: &str, raw: &str) -> Event {
    Event {
        timestamp: ts,
        host: host.to_string(),
        source: "/var/log/app.log".to_string(),
        sourcetype: "app:json".to_string(),
        index: "main".to_string(),
        raw: raw.to_string(),
        attributes: None,
    }
}

async fn append_index_events(ice: &IcebergContext, config: &IndexConfig, events: &[Event]) {
    let batch = events_to_record_batch(events).unwrap();
    let blooms = bloom_columns(config);
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    ice.append_to_table(&ice.index_table_ident(&config.index_id), batch, &bloom_refs)
        .await
        .unwrap();
}

/// Two independent contexts — separate catalog handles, as two query replicas
/// have — submit at the same instant. Both acknowledgements must be readable
/// from a third context opened afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submissions_from_independent_contexts_both_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let bootstrap = IcebergContext::open(&warehouse).await.unwrap();
    bootstrap.create_index(&logs_index("logs")).await.unwrap();
    drop(bootstrap);

    let left = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let right = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let mut set = tokio::task::JoinSet::new();
    for (replica, ice) in [("left", left.clone()), ("right", right.clone())] {
        let barrier = barrier.clone();
        set.spawn(async move {
            barrier.wait().await;
            ice.create_delete_task("logs", &format!("host = '{replica}'"), None, None)
                .await
                .unwrap()
        });
    }
    let mut acknowledged = Vec::new();
    while let Some(joined) = set.join_next().await {
        acknowledged.push(joined.expect("submitter task"));
    }
    assert_eq!(acknowledged.len(), 2);

    let reopened = IcebergContext::open(&warehouse).await.unwrap();
    let listed = reopened.list_delete_tasks(Some("logs")).await.unwrap();
    assert_eq!(
        listed.len(),
        2,
        "both acknowledged deletion requests must be discoverable after reopen: {listed:?}"
    );
    for task in &acknowledged {
        let stored = reopened
            .get_delete_task(task.task_id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("acknowledged task {} was lost", task.task_id));
        assert_eq!(&stored, task);
    }
}

/// An executor is running one index's task while another process acknowledges a
/// new one. The v1 executor's terminal status write replaced the whole ledger
/// from its pre-run vector, deleting the new task; the new one writes only its
/// own record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_executor_status_update_cannot_erase_a_task_submitted_meanwhile() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let config = logs_index("logs");
    let executor = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    executor.create_index(&config).await.unwrap();

    let now = fixture_base();
    for batch in 0..3 {
        append_index_events(
            &executor,
            &config,
            &[
                event_at(
                    now - ChronoDuration::hours(2) + ChronoDuration::minutes(batch),
                    "victim",
                    "victim row",
                ),
                event_at(
                    now - ChronoDuration::hours(2) + ChronoDuration::minutes(batch + 1),
                    "keep",
                    "kept row",
                ),
            ],
        )
        .await;
    }

    let executed = executor
        .create_delete_task("logs", "host = 'victim'", None, None)
        .await
        .unwrap();

    let submitter = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let sweep = {
        let executor = executor.clone();
        tokio::spawn(async move { executor.execute_delete_tasks("logs").await.unwrap() })
    };

    // Submit only once the sweep has moved its task off `Pending`, which it can
    // only do after loading the task list. The submission is therefore strictly
    // inside the window v1 lost: after the executor's read, before its terminal
    // write. Deadline so a wedged sweep fails the test instead of hanging.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let seen = submitter
            .get_delete_task(executed.task_id)
            .await
            .unwrap()
            .expect("the executing task is readable throughout");
        if seen.state != DeleteTaskState::Pending {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sweep never left Pending"
        );
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    let submitted = submitter
        .create_delete_task("logs", "host = 'late'", None, None)
        .await
        .unwrap();
    let outcome = sweep.await.expect("sweep task");

    assert_eq!(outcome.tasks_examined, 1);
    assert_eq!(outcome.tasks_completed, 1);
    assert_eq!(outcome.tasks_failed, 0);

    let reopened = IcebergContext::open(&warehouse).await.unwrap();
    let late = reopened
        .get_delete_task(submitted.task_id)
        .await
        .unwrap()
        .expect("the task acknowledged during the sweep must survive it");
    assert_eq!(late.state, DeleteTaskState::Pending);
    let executed = reopened
        .get_delete_task(executed.task_id)
        .await
        .unwrap()
        .expect("the executed task must keep its terminal state");
    assert_eq!(executed.state, DeleteTaskState::Done);
}

/// Two executors, one index each, transitioning at the same time. Neither
/// index's terminal state may be rolled back by the other's write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_executors_do_not_erase_each_others_index_updates() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let bootstrap = IcebergContext::open(&warehouse).await.unwrap();
    let mut tasks = Vec::new();
    for index_id in ["logs-a", "logs-b"] {
        let config = logs_index(index_id);
        bootstrap.create_index(&config).await.unwrap();
        append_index_events(
            &bootstrap,
            &config,
            &[event_at(fixture_base(), "victim", "victim row")],
        )
        .await;
        tasks.push((
            index_id,
            bootstrap
                .create_delete_task(index_id, "host = 'victim'", None, None)
                .await
                .unwrap(),
        ));
    }
    drop(bootstrap);

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut set = tokio::task::JoinSet::new();
    for (index_id, _) in &tasks {
        let index_id = index_id.to_string();
        let warehouse = warehouse.clone();
        let barrier = barrier.clone();
        set.spawn(async move {
            let ice = IcebergContext::open(&warehouse).await.unwrap();
            barrier.wait().await;
            ice.execute_delete_tasks(&index_id).await.unwrap()
        });
    }
    while let Some(joined) = set.join_next().await {
        let outcome = joined.expect("executor task");
        assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
    }

    let reopened = IcebergContext::open(&warehouse).await.unwrap();
    for (index_id, task) in &tasks {
        let stored = reopened
            .get_delete_task(task.task_id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{index_id} task was erased"));
        assert_eq!(
            stored.state,
            DeleteTaskState::Done,
            "{index_id} task lost its terminal state to the sibling executor: {stored:?}"
        );
    }
}

/// A v1 ledger keeps being read, and is never rewritten: the state transition
/// of a legacy task lands in that task's own record, and the ledger document
/// stays byte-identical (no autonomous migration).
///
/// The transition a legacy task takes is now a REFUSAL (#2837). Its record
/// carries no `table_uuid`, nothing backfills one from the reused name, so the
/// executor fails it with an error naming resubmission instead of deleting
/// rows on the authority of a name.
#[tokio::test]
async fn legacy_ledger_stays_readable_and_is_never_rewritten() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let config = logs_index("logs");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.create_index(&config).await.unwrap();
    append_index_events(
        &ice,
        &config,
        &[event_at(fixture_base(), "victim", "victim row")],
    )
    .await;

    let legacy_id = uuid::Uuid::now_v7();
    let ledger = serde_json::to_vec(&serde_json::json!([{
        "task_id": legacy_id,
        "index_id": "logs",
        "predicate_sql": "host = 'victim'",
        "start_ts": null,
        "end_ts": null,
        "created_at": Utc::now() - ChronoDuration::minutes(5),
        "state": "pending",
        "error": null,
        "files_rewritten": 0,
        "rows_deleted": 0,
    }]))
    .unwrap();
    let ledger_path = warehouse
        .join("_siglake/config/delete_tasks")
        .join(format!("{}.json", ice.namespace()));
    std::fs::create_dir_all(ledger_path.parent().unwrap()).unwrap();
    std::fs::write(&ledger_path, &ledger).unwrap();

    let listed = ice.list_delete_tasks(Some("logs")).await.unwrap();
    assert_eq!(listed.len(), 1, "legacy ledger entry must be readable");
    assert_eq!(listed[0].task_id, legacy_id);

    let fresh = ice
        .create_delete_task("logs", "host = 'other'", None, None)
        .await
        .unwrap();
    let listed = ice.list_delete_tasks(Some("logs")).await.unwrap();
    assert_eq!(
        listed.len(),
        2,
        "a new task must not hide the legacy ledger: {listed:?}"
    );

    let outcome = ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.tasks_examined, 2);
    assert_eq!(
        (outcome.tasks_completed, outcome.tasks_failed),
        (1, 1),
        "the legacy task is refused and the bound one runs: {outcome:?}"
    );
    let stored = ice.get_delete_task(legacy_id).await.unwrap().unwrap();
    assert_eq!(
        stored.state,
        DeleteTaskState::Failed,
        "a legacy task's record must shadow its ledger entry, and carry the refusal"
    );
    let error = stored.error.clone().unwrap_or_default();
    assert!(
        error.contains("bound to an index incarnation") && error.contains("resubmit"),
        "the refusal must say why and what to do instead: {error}"
    );
    assert_eq!(
        (stored.files_rewritten, stored.rows_deleted),
        (0, 0),
        "a refused task rewrote nothing: {stored:?}"
    );
    assert_eq!(
        ice.get_delete_task(fresh.task_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        DeleteTaskState::Done
    );
    assert_eq!(
        std::fs::read(&ledger_path).unwrap(),
        ledger,
        "the legacy ledger document must not be rewritten or migrated"
    );
}
