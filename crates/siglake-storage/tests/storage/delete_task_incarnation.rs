//! A queued deletion belongs to the index INCARNATION it was accepted
//! against, not to the name.
//!
//! THE DEFECT THIS GUARDS (#2837). A delete task recorded only `index_id`, and
//! an index id is reusable: `DELETE /api/v1/indexes/logs` followed by a `POST`
//! of the same id is a different Iceberg table under the same name. The
//! executor resolved its target by that name, so a task submitted against the
//! dropped incarnation — reviewed, authorised and acknowledged with a 201 for
//! rows that no longer exist — rewrote the REPLACEMENT's rows instead. Nothing
//! in the record said which table its submitter had seen.
//!
//! The record now carries the server-resolved `table_uuid`, and the executor
//! requires it to equal the table it is about to rewrite. Refusal is terminal
//! and recovers by resubmission, the same path a `failed` task takes.
//!
//! COMPATIBILITY. Records written before the binding have no `table_uuid`.
//! They stay readable, nothing rewrites them, and nothing backfills an
//! identity from the current name — a name is precisely what proves nothing
//! here. Their execution is refused with an error naming resubmission.

use chrono::Utc;
use datafusion::prelude::SessionContext;

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

async fn count_index_rows(ice: &IcebergContext, index_id: &str, where_sql: Option<&str>) -> i64 {
    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(index_id), index_id)
        .await
        .unwrap();
    let sql = match where_sql {
        Some(predicate) => format!("SELECT count(*) AS n FROM \"{index_id}\" WHERE {predicate}"),
        None => format!("SELECT count(*) AS n FROM \"{index_id}\""),
    };
    let batches = ctx
        .sql(sql.as_str())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

/// Submission records the incarnation it validated against, and it is the one
/// the catalog reports for that name.
#[tokio::test]
async fn submission_records_the_table_it_resolved() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.create_index(&logs_index("logs")).await.unwrap();

    let task = ice
        .create_delete_task("logs", "host = 'victim'", None, None)
        .await
        .unwrap();
    assert_eq!(
        task.table_uuid,
        ice.index_table_uuid("logs").await.unwrap(),
        "the acknowledged task must name the table its submitter validated"
    );

    let stored = ice.get_delete_task(task.task_id).await.unwrap().unwrap();
    assert_eq!(stored, task, "the binding must survive the round trip");
}

/// The acceptance case. A task accepted against incarnation A cannot modify
/// the B that replaced it under the same name — even though B holds rows the
/// predicate matches — and a task submitted against B still runs.
#[tokio::test]
async fn a_task_accepted_against_the_dropped_index_cannot_delete_the_replacements_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let config = logs_index("logs");
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    ice.create_index(&config).await.unwrap();
    let now = fixture_base();
    append_index_events(
        &ice,
        &config,
        &[
            event_at(now, "victim", "A victim"),
            event_at(now, "keep", "A keep"),
        ],
    )
    .await;
    let dropped_uuid = ice.index_table_uuid("logs").await.unwrap().unwrap();
    let stale = ice
        .create_delete_task("logs", "host = 'victim'", None, None)
        .await
        .unwrap();

    // Same name, different table, matching rows.
    assert!(ice.delete_index("logs").await.unwrap());
    ice.create_index(&config).await.unwrap();
    append_index_events(
        &ice,
        &config,
        &[
            event_at(now, "victim", "B victim"),
            event_at(now, "keep", "B keep"),
        ],
    )
    .await;
    let live_uuid = ice.index_table_uuid("logs").await.unwrap().unwrap();
    assert_ne!(
        dropped_uuid, live_uuid,
        "the recreation must be a new table"
    );

    let outcome = ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(
        (
            outcome.tasks_examined,
            outcome.tasks_completed,
            outcome.tasks_failed
        ),
        (1, 0, 1),
        "the stale task must be refused, not executed: {outcome:?}"
    );
    assert_eq!(
        (outcome.files_rewritten, outcome.rows_deleted),
        (0, 0),
        "a refused task must rewrite nothing: {outcome:?}"
    );
    assert_eq!(
        count_index_rows(&ice, "logs", None).await,
        2,
        "the replacement's rows must be untouched"
    );

    let refused = ice.get_delete_task(stale.task_id).await.unwrap().unwrap();
    assert_eq!(refused.state, DeleteTaskState::Failed);
    assert_eq!(
        refused.table_uuid.as_deref(),
        Some(dropped_uuid.as_str()),
        "the refusal must not rewrite the binding"
    );
    let error = refused.error.clone().unwrap_or_default();
    assert!(
        error.contains(dropped_uuid.as_str())
            && error.contains(live_uuid.as_str())
            && error.contains("resubmit"),
        "the refusal must name both incarnations and the recovery: {error}"
    );
    assert_eq!(
        (refused.files_rewritten, refused.rows_deleted),
        (0, 0),
        "terminal accounting must stay at zero: {refused:?}"
    );

    // The recovery: the same request, submitted against the index that exists.
    let fresh = ice
        .create_delete_task("logs", "host = 'victim'", None, None)
        .await
        .unwrap();
    assert_eq!(fresh.table_uuid.as_deref(), Some(live_uuid.as_str()));
    let outcome = ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(
        (
            outcome.tasks_examined,
            outcome.tasks_completed,
            outcome.rows_deleted
        ),
        (1, 1, 1),
        "the refused task is terminal and only the new one runs: {outcome:?}"
    );
    assert_eq!(count_index_rows(&ice, "logs", None).await, 1);
    assert_eq!(
        count_index_rows(&ice, "logs", Some("host = 'victim'")).await,
        0
    );
    assert_eq!(
        ice.get_delete_task(fresh.task_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        DeleteTaskState::Done
    );
}

/// A dry run reports the refusal too, and still writes nothing: previewing a
/// stale task must not claim it or move its record.
#[tokio::test]
async fn a_dry_run_reports_the_refusal_without_touching_the_record() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let config = logs_index("logs");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.create_index(&config).await.unwrap();
    append_index_events(&ice, &config, &[event_at(fixture_base(), "victim", "row")]).await;
    let stale = ice
        .create_delete_task("logs", "host = 'victim'", None, None)
        .await
        .unwrap();

    assert!(ice.delete_index("logs").await.unwrap());
    ice.create_index(&config).await.unwrap();
    append_index_events(&ice, &config, &[event_at(fixture_base(), "victim", "row")]).await;

    let outcome = ice.preview_delete_tasks("logs").await.unwrap();
    assert_eq!(
        (
            outcome.tasks_examined,
            outcome.tasks_failed,
            outcome.rows_deleted
        ),
        (1, 1, 0),
        "the preview must report the stale task as failing: {outcome:?}"
    );
    let stored = ice.get_delete_task(stale.task_id).await.unwrap().unwrap();
    assert_eq!(
        stored, stale,
        "a dry run must leave the record exactly as submitted"
    );
    assert_eq!(count_index_rows(&ice, "logs", None).await, 1);
}

/// A record written before the binding existed. It reads back unchanged, and
/// its execution is refused rather than bound to whatever the name resolves to
/// now — even when the table it would hit is the very one it was submitted
/// against.
#[tokio::test]
async fn a_uuid_less_record_is_refused_and_never_backfilled() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let config = logs_index("logs");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.create_index(&config).await.unwrap();
    append_index_events(&ice, &config, &[event_at(fixture_base(), "victim", "row")]).await;

    // Exactly the JSON a pre-#2837 build wrote: no `table_uuid` key at all.
    let legacy_id = uuid::Uuid::now_v7();
    let record = serde_json::to_vec(&serde_json::json!({
        "task_id": legacy_id,
        "index_id": "logs",
        "predicate_sql": "host = 'victim'",
        "start_ts": null,
        "end_ts": null,
        "created_at": Utc::now(),
        "state": "pending",
        "error": null,
        "files_rewritten": 0,
        "rows_deleted": 0,
    }))
    .unwrap();
    let dir = warehouse
        .join("_siglake/config/delete_tasks")
        .join(ice.namespace().to_string());
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{legacy_id}.json")), &record).unwrap();

    let stored = ice.get_delete_task(legacy_id).await.unwrap().unwrap();
    assert_eq!(
        stored.table_uuid, None,
        "an older record must read back as it was written"
    );

    let outcome = ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(
        (
            outcome.tasks_examined,
            outcome.tasks_completed,
            outcome.tasks_failed
        ),
        (1, 0, 1),
        "{outcome:?}"
    );
    assert_eq!(
        count_index_rows(&ice, "logs", None).await,
        1,
        "the row must survive a request whose incarnation cannot be established"
    );
    let refused = ice.get_delete_task(legacy_id).await.unwrap().unwrap();
    assert_eq!(refused.state, DeleteTaskState::Failed);
    assert_eq!(
        refused.table_uuid, None,
        "the refusal must not backfill an identity from the current name"
    );
    let error = refused.error.clone().unwrap_or_default();
    assert!(
        error.contains("bound to an index incarnation") && error.contains("resubmit"),
        "the refusal must say why and what to do instead: {error}"
    );
}

/// Claim exclusion is unchanged by the fence: the refused task keeps its
/// claim, so a delayed executor holding a `Pending` copy skips it instead of
/// re-running the refusal over its terminal record.
#[tokio::test]
async fn a_refused_task_keeps_its_claim_against_a_stale_executor() {
    use siglake_storage::iceberg::DeleteTaskClaimRead;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let config = logs_index("logs");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    ice.create_index(&config).await.unwrap();
    append_index_events(&ice, &config, &[event_at(fixture_base(), "victim", "row")]).await;
    let stale = ice
        .create_delete_task("logs", "host = 'victim'", None, None)
        .await
        .unwrap();

    // A second executor's view of the pending set, read before anyone runs.
    let delayed = ice
        .read_pending_delete_tasks_for_test("logs")
        .await
        .unwrap();
    assert_eq!(delayed.len(), 1);

    assert!(ice.delete_index("logs").await.unwrap());
    ice.create_index(&config).await.unwrap();
    append_index_events(&ice, &config, &[event_at(fixture_base(), "victim", "row")]).await;

    let outcome = ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.tasks_failed, 1, "{outcome:?}");
    assert!(matches!(
        ice.read_delete_task_claim(stale.task_id).await.unwrap(),
        DeleteTaskClaimRead::Present(_)
    ));

    let second = ice
        .execute_pending_delete_tasks_for_test("logs", delayed)
        .await
        .unwrap();
    assert_eq!(
        (second.tasks_already_claimed, second.tasks_failed),
        (1, 0),
        "the surviving claim, not the record, is what stops the delayed executor: {second:?}"
    );
    let refused = ice.get_delete_task(stale.task_id).await.unwrap().unwrap();
    assert_eq!(refused.state, DeleteTaskState::Failed);
    assert_eq!(count_index_rows(&ice, "logs", None).await, 1);
}
