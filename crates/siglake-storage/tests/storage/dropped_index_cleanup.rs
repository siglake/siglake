//! Dropped-index cleanup is addressed by immutable table UUID, never by the
//! reusable index name. Production records remain report-only until an
//! operator grants aggregate-only authority outside this API.

use chrono::{TimeZone, Utc};
use datafusion::prelude::SessionContext;
use siglake_core::index_config::IndexConfig;
use siglake_core::{events_to_record_batch, Event};
use siglake_storage::iceberg::{
    aggregate_prefix_rel_path, DroppedIndexTargetAuthorization, IcebergContext, IcebergTuning,
};

async fn context(warehouse: &std::path::Path) -> IcebergContext {
    IcebergContext::open(warehouse)
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            table_group_count_cardinality: Some(2_000_000),
            ..Default::default()
        })
}

fn config() -> IndexConfig {
    let mut config = IndexConfig::builtin_events();
    config.index_id = "logs".to_string();
    config
}

fn events(prefix: &str, base: i64) -> Vec<Event> {
    (0..3)
        .map(|i| Event {
            timestamp: Utc.timestamp_opt(base + i, 0).single().unwrap(),
            host: format!("{prefix}-{i}"),
            source: "/var/log/app.log".to_string(),
            sourcetype: "app:json".to_string(),
            index: "main".to_string(),
            raw: format!("row {i}"),
            attributes: None,
        })
        .collect()
}

async fn append(ice: &IcebergContext, prefix: &str, base: i64) {
    ice.append_to_table(
        &ice.index_table_ident("logs"),
        events_to_record_batch(&events(prefix, base)).unwrap(),
        &["host", "source", "sourcetype", "index"],
    )
    .await
    .unwrap();
    // Materialize the delta/base classes in addition to the inline object.
    ice.fold_group_count_deltas(1).await.unwrap();
}

fn files_under(dir: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return files;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(files_under(&path));
        } else {
            files.push((path.clone(), std::fs::read(path).unwrap()));
        }
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

async fn scanned_counts(ice: &IcebergContext) -> Vec<(String, u64)> {
    use datafusion::arrow::array::{Array, StringArray, UInt64Array};

    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident("logs"), "logs")
        .await
        .unwrap();
    let batches = ctx
        .sql("SELECT host, CAST(count(*) AS BIGINT UNSIGNED) FROM logs GROUP BY host ORDER BY host")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut counts = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(keys.is_valid(row));
            counts.push((keys.value(row).to_string(), values.value(row)));
        }
    }
    counts
}

#[tokio::test]
async fn authorized_cleanup_reaps_only_the_dropped_uuid_and_late_publications() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = context(&tmp.path().join("warehouse")).await;
    let config = config();
    let ident = ice.create_index(&config).await.unwrap();
    append(&ice, "a", 1_700_000_000).await;
    let table_a = ice.catalog().load_table(&ident).await.unwrap();
    let uuid_a = table_a.metadata().uuid().to_string();
    let location = url::Url::parse(table_a.metadata().location())
        .unwrap()
        .to_file_path()
        .unwrap();
    let prefix_a = location.join(aggregate_prefix_rel_path(&uuid_a));
    assert!(!files_under(&prefix_a).is_empty());
    let legacy = location.join("metadata/siglake-agg-wide.json");
    std::fs::write(&legacy, b"legacy-unowned").unwrap();

    assert!(ice.delete_index("logs").await.unwrap());
    let mut records = ice.list_dropped_index_cleanup_records().await.unwrap();
    assert_eq!(records.len(), 1);
    let record = records.first_mut().unwrap();
    assert_eq!(record.table_uuid, uuid_a);
    assert_eq!(record.table_location, table_a.metadata().location());
    assert_eq!(
        record.targets.aggregate_prefix.authorization,
        DroppedIndexTargetAuthorization::ReportOnly
    );
    assert!(
        record
            .targets
            .committed_files
            .inventory
            .iter()
            .any(|path| path.ends_with(".parquet")),
        "the drop record inventories the committed data file"
    );
    let before_report = files_under(&prefix_a);
    let report = ice.sweep_dropped_index_aggregates(1).await.unwrap();
    assert_eq!(report[0].objects_observed, before_report.len());
    assert_eq!(
        files_under(&prefix_a),
        before_report,
        "report-only changed storage"
    );

    // The authority edit is intentionally available only through a test hook;
    // production has no deletion-enablement API while policy choices are open.
    record.targets.aggregate_prefix.authorization =
        DroppedIndexTargetAuthorization::AggregateDelete;
    ice.write_dropped_index_cleanup_record_for_test(record)
        .await
        .unwrap();

    ice.create_index(&config).await.unwrap();
    append(&ice, "b", 1_800_000_000).await;
    let table_b = ice.catalog().load_table(&ident).await.unwrap();
    let uuid_b = table_b.metadata().uuid().to_string();
    assert_ne!(uuid_a, uuid_b);
    assert_eq!(table_b.metadata().location(), table_a.metadata().location());
    let prefix_b = location.join(aggregate_prefix_rel_path(&uuid_b));
    let captured_b = files_under(&prefix_b);
    assert!(!captured_b.is_empty());

    let first = ice.sweep_dropped_index_aggregates(1).await.unwrap();
    assert!(first[0].empty);
    assert!(!prefix_a.exists(), "filesystem UUID prefix was not removed");

    // A publisher paused before its PUT wakes after the first empty pass.
    let late_delta = prefix_a.join("siglake-agg-deltas/late.json");
    let late_marker = prefix_a.join("siglake-agg-deltas/late.rebuild.json");
    std::fs::create_dir_all(late_delta.parent().unwrap()).unwrap();
    std::fs::write(&late_delta, b"late-a-delta").unwrap();
    std::fs::write(&late_marker, b"late-a-marker").unwrap();
    let second = ice.sweep_dropped_index_aggregates(1).await.unwrap();
    assert_eq!(second[0].objects_deleted, 2);
    assert!(!prefix_a.exists(), "late filesystem prefix was not removed");
    assert_eq!(
        files_under(&prefix_b),
        captured_b,
        "replacement bytes changed"
    );
    assert_eq!(std::fs::read(&legacy).unwrap(), b"legacy-unowned");

    ice.invalidate_cached_table(&ident).await;
    let mut tier1: Vec<_> = ice
        .tier1_group_counts("logs", "host")
        .await
        .unwrap()
        .unwrap()
        .to_rows()
        .into_iter()
        .map(|(key, count)| (key.unwrap(), count))
        .collect();
    tier1.sort();
    assert_eq!(tier1, scanned_counts(&ice).await);
}

#[tokio::test]
async fn mismatched_record_refuses_before_any_delete_and_retry_converges() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = context(&tmp.path().join("warehouse")).await;
    let config = config();
    let ident = ice.create_index(&config).await.unwrap();
    append(&ice, "a", 1_700_000_000).await;
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let location = url::Url::parse(table.metadata().location())
        .unwrap()
        .to_file_path()
        .unwrap();
    let prefix = location.join(aggregate_prefix_rel_path(
        &table.metadata().uuid().to_string(),
    ));
    assert!(ice.delete_index("logs").await.unwrap());
    let mut record = ice
        .list_dropped_index_cleanup_records()
        .await
        .unwrap()
        .remove(0);
    record.targets.aggregate_prefix.authorization =
        DroppedIndexTargetAuthorization::AggregateDelete;
    record.targets.aggregate_prefix.relative_path =
        "metadata/siglake-agg/22222222-2222-2222-2222-222222222222/".to_string();
    ice.write_dropped_index_cleanup_record_for_test(&record)
        .await
        .unwrap();
    let before = files_under(&prefix);
    let error = ice.sweep_dropped_index_aggregates(1).await.unwrap_err();
    assert!(error.to_string().contains("does not equal"), "{error:#}");
    assert_eq!(files_under(&prefix), before, "mismatch issued a DELETE");

    let uuid = record.table_uuid.clone();
    record.table_uuid = "not-a-uuid".to_string();
    ice.write_dropped_index_cleanup_record_for_test(&record)
        .await
        .unwrap();
    let error = ice.sweep_dropped_index_aggregates(1).await.unwrap_err();
    assert!(
        error.to_string().contains("invalid table UUID"),
        "{error:#}"
    );
    assert_eq!(
        files_under(&prefix),
        before,
        "malformed record issued a DELETE"
    );

    // Repair the record. One object already absent models a crash after DELETE
    // but before recording the observation; page size 1 models a partial page.
    record.table_uuid = uuid;
    record.targets.aggregate_prefix.relative_path =
        format!("{}/", aggregate_prefix_rel_path(&record.table_uuid));
    ice.write_dropped_index_cleanup_record_for_test(&record)
        .await
        .unwrap();
    std::fs::remove_file(&before[0].0).unwrap();
    let outcome = ice.sweep_dropped_index_aggregates(1).await.unwrap();
    assert!(outcome[0].empty);
    assert!(files_under(&prefix).is_empty());
    assert!(ice.sweep_dropped_index_aggregates(1).await.unwrap()[0].empty);
}
