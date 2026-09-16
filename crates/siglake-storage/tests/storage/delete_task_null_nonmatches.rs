//! A delete predicate's complement must be NULL-safe.
//!
//! THE DEFECT THESE GUARD (task #2094). `rewrite_delete_task_candidate` counts
//! the rows to delete with `WHERE {predicate}` — three-valued logic, so only
//! the rows the predicate is TRUE for — and used to select the survivors with
//! `WHERE NOT ({predicate})`, which is NULL wherever the predicate is NULL.
//! A NULL-valued nonmatch was therefore in neither set, so the conservation
//! guard (`survivors + deleted == input`) fired and the task went to `Failed`.
//!
//! `attributes` is the canonical events schema's one nullable column (WS-7
//! residual attributes, null for every non-structured source), so an ordinary
//! GDPR predicate like `attributes = '…'` over a file that holds ANY row
//! without structured attributes hit this. The guard prevented data loss, but
//! the acknowledged deletion request could never complete: the executor
//! re-read the same file on every sweep and failed the same way.
//!
//! The complement is now `({predicate}) IS NOT TRUE`, which is exact under the
//! same three-valued logic. The guard and the read-only planner stay.

use chrono::{Duration as ChronoDuration, Utc};

use siglake_core::index_config::{FieldType, IndexConfig};
use siglake_core::{events_to_record_batch, Event};
use siglake_storage::iceberg::{DeleteTaskState, IcebergContext};

use crate::fixture_clock::fixture_base;

const GONE: &str = r#"{"tenant":"gone"}"#;
const STAY: &str = r#"{"tenant":"stay"}"#;

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

/// `attributes` is nullable; `None` is what every non-structured source writes.
fn event_with_attributes(ts: chrono::DateTime<Utc>, raw: &str, attributes: Option<&str>) -> Event {
    Event {
        timestamp: ts,
        host: "web-01".to_string(),
        source: "/var/log/app.log".to_string(),
        sourcetype: "app:json".to_string(),
        index: "main".to_string(),
        raw: raw.to_string(),
        attributes: attributes.map(str::to_string),
    }
}

/// One `append_to_table` call ⇒ one data file, which is what makes the
/// TRUE/FALSE/NULL mix land in a single candidate file.
async fn append_index_events(ice: &IcebergContext, config: &IndexConfig, events: &[Event]) {
    let batch = events_to_record_batch(events).unwrap();
    let blooms = bloom_columns(config);
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    ice.append_to_table(&ice.index_table_ident(&config.index_id), batch, &bloom_refs)
        .await
        .unwrap();
}

async fn count_index_rows(ice: &IcebergContext, index_id: &str, where_sql: Option<&str>) -> i64 {
    let ctx = datafusion::prelude::SessionContext::new();
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

async fn live_paths(ice: &IcebergContext, index_id: &str) -> Vec<String> {
    let mut paths: Vec<String> = ice
        .live_data_files(&ice.index_table_ident(index_id))
        .await
        .unwrap()
        .into_iter()
        .map(|file| file.file_path().to_string())
        .collect();
    paths.sort();
    paths
}

/// A single file holding a TRUE, a FALSE and a NULL row for the predicate. Only
/// the TRUE row may go; the task must reach `Done` with `rows_deleted == 1`.
/// Before the fix this file failed the conservation guard (survivors 1 +
/// deleted 1 != input 3) and the task went to `Failed` forever.
#[tokio::test]
async fn a_null_valued_nonmatch_survives_a_match_in_the_same_file() {
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
            event_with_attributes(now - ChronoDuration::minutes(3), "true row", Some(GONE)),
            event_with_attributes(now - ChronoDuration::minutes(2), "false row", Some(STAY)),
            event_with_attributes(now - ChronoDuration::minutes(1), "null row", None),
        ],
    )
    .await;

    let task = ice
        .create_delete_task("logs", &format!("attributes = '{GONE}'"), None, None)
        .await
        .unwrap();
    let outcome = ice.execute_delete_tasks("logs").await.unwrap();

    assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
    assert_eq!(
        outcome.tasks_failed, 0,
        "a NULL-valued nonmatch must not fail the task: {outcome:?}"
    );
    assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
    assert_eq!(outcome.rows_deleted, 1, "{outcome:?}");

    let stored = ice.get_delete_task(task.task_id).await.unwrap().unwrap();
    assert_eq!(stored.state, DeleteTaskState::Done, "{stored:?}");
    assert_eq!(stored.error, None, "{stored:?}");
    assert_eq!(stored.rows_deleted, 1, "{stored:?}");
    assert_eq!(stored.files_rewritten, 1, "{stored:?}");

    assert_eq!(count_index_rows(&ice, "logs", None).await, 2);
    assert_eq!(
        count_index_rows(&ice, "logs", Some(&format!("attributes = '{GONE}'"))).await,
        0,
        "the matching row must be gone"
    );
    assert_eq!(
        count_index_rows(&ice, "logs", Some(&format!("attributes = '{STAY}'"))).await,
        1,
        "the FALSE-valued nonmatch must survive"
    );
    assert_eq!(
        count_index_rows(&ice, "logs", Some("attributes IS NULL")).await,
        1,
        "the NULL-valued nonmatch must survive"
    );
}

/// An all-NULL file next to a mixed one. The predicate is TRUE nowhere in the
/// all-NULL file, so that file must not be rewritten at all — its path has to
/// survive the sweep byte-for-byte — while the mixed file completes.
#[tokio::test]
async fn an_all_null_file_is_not_rewritten_and_keeps_every_row() {
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
            event_with_attributes(now - ChronoDuration::minutes(9), "null only 1", None),
            event_with_attributes(now - ChronoDuration::minutes(8), "null only 2", None),
        ],
    )
    .await;
    let all_null_path = live_paths(&ice, "logs").await;
    assert_eq!(all_null_path.len(), 1);
    let all_null_path = all_null_path.into_iter().next().unwrap();

    append_index_events(
        &ice,
        &config,
        &[
            event_with_attributes(now - ChronoDuration::minutes(2), "true row", Some(GONE)),
            event_with_attributes(now - ChronoDuration::minutes(1), "null row", None),
        ],
    )
    .await;

    let task = ice
        .create_delete_task("logs", &format!("attributes = '{GONE}'"), None, None)
        .await
        .unwrap();
    let outcome = ice.execute_delete_tasks("logs").await.unwrap();

    assert_eq!(outcome.tasks_failed, 0, "{outcome:?}");
    assert_eq!(
        outcome.files_rewritten, 1,
        "only the file with a TRUE row is a candidate: {outcome:?}"
    );
    assert_eq!(outcome.rows_deleted, 1, "{outcome:?}");
    assert_eq!(
        ice.get_delete_task(task.task_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        DeleteTaskState::Done
    );

    assert!(
        live_paths(&ice, "logs").await.contains(&all_null_path),
        "the all-NULL file matched nothing and must not be rewritten"
    );
    assert_eq!(count_index_rows(&ice, "logs", None).await, 3);
    assert_eq!(
        count_index_rows(&ice, "logs", Some("attributes IS NULL")).await,
        3,
        "every NULL-valued row must survive"
    );
}

/// The NULL-safe complement has to keep working for a predicate that names NULL
/// itself: `attributes IS NULL` deletes exactly the NULL rows and nothing else.
#[tokio::test]
async fn an_explicit_is_null_predicate_deletes_exactly_the_null_rows() {
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
            event_with_attributes(now - ChronoDuration::minutes(3), "null row 1", None),
            event_with_attributes(now - ChronoDuration::minutes(2), "kept row", Some(STAY)),
            event_with_attributes(now - ChronoDuration::minutes(1), "null row 2", None),
        ],
    )
    .await;

    let task = ice
        .create_delete_task("logs", "attributes IS NULL", None, None)
        .await
        .unwrap();
    let outcome = ice.execute_delete_tasks("logs").await.unwrap();

    assert_eq!(outcome.tasks_failed, 0, "{outcome:?}");
    assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
    assert_eq!(outcome.rows_deleted, 2, "{outcome:?}");
    let stored = ice.get_delete_task(task.task_id).await.unwrap().unwrap();
    assert_eq!(stored.state, DeleteTaskState::Done, "{stored:?}");
    assert_eq!(stored.rows_deleted, 2, "{stored:?}");

    assert_eq!(count_index_rows(&ice, "logs", None).await, 1);
    assert_eq!(
        count_index_rows(&ice, "logs", Some("attributes IS NULL")).await,
        0
    );
    assert_eq!(
        count_index_rows(&ice, "logs", Some(&format!("attributes = '{STAY}'"))).await,
        1,
        "the non-NULL row must survive an IS NULL deletion"
    );
}

/// The dry run reports the same NULL-safe counts and commits nothing: every row
/// and every file path is still there, and the task is still `Pending`. Before
/// the fix the preview failed the row-count guard too, so an operator could not
/// even see what the deletion would do.
#[tokio::test]
async fn a_dry_run_over_a_null_bearing_file_reports_counts_and_preserves_every_row() {
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
            event_with_attributes(now - ChronoDuration::minutes(3), "true row", Some(GONE)),
            event_with_attributes(now - ChronoDuration::minutes(2), "false row", Some(STAY)),
            event_with_attributes(now - ChronoDuration::minutes(1), "null row", None),
        ],
    )
    .await;
    let before = live_paths(&ice, "logs").await;

    let task = ice
        .create_delete_task("logs", &format!("attributes = '{GONE}'"), None, None)
        .await
        .unwrap();
    let outcome = ice.preview_delete_tasks("logs").await.unwrap();

    assert!(outcome.dry_run, "{outcome:?}");
    assert_eq!(outcome.tasks_examined, 1, "{outcome:?}");
    assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
    assert_eq!(outcome.tasks_failed, 0, "{outcome:?}");
    assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
    assert_eq!(
        outcome.rows_deleted, 1,
        "the preview must report the TRUE rows only: {outcome:?}"
    );

    assert_eq!(
        ice.get_delete_task(task.task_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        DeleteTaskState::Pending,
        "a dry run must not transition the task"
    );
    assert_eq!(live_paths(&ice, "logs").await, before, "no file may move");
    assert_eq!(count_index_rows(&ice, "logs", None).await, 3);
    assert_eq!(
        count_index_rows(&ice, "logs", Some(&format!("attributes = '{GONE}'"))).await,
        1,
        "a dry run deletes nothing"
    );
}
