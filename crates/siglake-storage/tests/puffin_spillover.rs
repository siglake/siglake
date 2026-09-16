use std::path::{Path, PathBuf};

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && label.is_none_or(|(lk, lv)| {
                    key.key()
                        .labels()
                        .any(|entry| entry.key() == lk && entry.value() == lv)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            _ => 0,
        })
        .sum()
}

fn find_parquet(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(find_parquet(&path));
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            out.push(path);
        }
    }
    out
}

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn large_inverted_indexes_spill_to_puffin_and_prune_from_stats_metadata() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_footer_max_bytes: Some(1),
            ..Default::default()
        });
    ice.append_events(&[
        Event::now("database timeout retry"),
        Event::now("healthy startup"),
        Event::now("database migration"),
    ])
    .await
    .unwrap();

    let parquet_files = find_parquet(&warehouse);
    assert_eq!(parquet_files.len(), 1, "expected one data file");
    let bytes = std::fs::read(&parquet_files[0]).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let kv = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("footer key_value_metadata present");
    assert!(
        kv.iter()
            .all(|entry| entry.key != siglake_index::INVERTED_INDEX_KV_KEY),
        "raw index should spill out of footer when the threshold is forced low"
    );

    let table = ice
        .catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap();
    let statistics: Vec<_> = table.metadata().statistics_iter().collect();
    assert_eq!(
        statistics.len(),
        1,
        "expected one registered Puffin statistics file"
    );
    let stats = statistics[0];
    assert_eq!(stats.blob_metadata.len(), 1, "one raw-column blob");
    assert_eq!(stats.blob_metadata[0].r#type, "siglake-inverted-v1");
    assert_eq!(
        stats.blob_metadata[0]
            .properties
            .get("data_file")
            .map(String::as_str),
        Some(ice.live_data_files(ice.events_table_ident()).await.unwrap()[0].file_path())
    );
    assert!(
        table
            .file_io()
            .exists(&stats.statistics_path)
            .await
            .unwrap(),
        "registered Puffin sidecar should exist on disk"
    );

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) FROM events WHERE raw LIKE '%database%'",
        )
        .await,
        2
    );

    let snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter_sum(
            &snapshot,
            "siglake_iceberg_inverted_index_used_total",
            Some(("storage", "puffin")),
        ) > 0,
        "row-level pruning should come from the Puffin blob when the footer KV is absent"
    );
}
