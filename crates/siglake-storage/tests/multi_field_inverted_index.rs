use std::path::{Path, PathBuf};

use arrow_array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::Utc;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == name)
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
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

fn field(name: &str, field_type: FieldType, required: bool) -> FieldMapping {
    FieldMapping {
        name: name.to_string(),
        field_type,
        required,
    }
}

fn logs_config() -> IndexConfig {
    IndexConfig {
        index_id: "logs".to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                field("ts", FieldType::Datetime, true),
                field(
                    "message",
                    FieldType::Text {
                        tokenizer: Some("default".to_string()),
                    },
                    false,
                ),
                field(
                    "title",
                    FieldType::Text {
                        tokenizer: Some("stem".to_string()),
                    },
                    false,
                ),
                field(
                    "service",
                    FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    false,
                ),
            ],
            timestamp_field: "ts".to_string(),
            tag_fields: vec!["service".to_string()],
            default_search_fields: vec!["message".to_string(), "title".to_string()],
        },
        retention: None,
        index_at_flush: None,
    }
}

fn logs_batch() -> RecordBatch {
    let schema = logs_config().to_arrow_schema();
    RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(
                TimestampMicrosecondArray::from(vec![
                    Some(Utc::now().timestamp_micros()),
                    Some(Utc::now().timestamp_micros() + 1),
                    Some(Utc::now().timestamp_micros() + 2),
                ])
                .with_timezone("+00:00"),
            ),
            std::sync::Arc::new(StringArray::from(vec![
                Some("database timeout retry"),
                Some("healthy startup"),
                Some("database migration"),
            ])),
            std::sync::Arc::new(StringArray::from(vec![
                Some("timeouts overview"),
                Some("steady state"),
                Some("migration complete"),
            ])),
            std::sync::Arc::new(StringArray::from(vec![
                Some("svc-a"),
                Some("svc-b"),
                Some("svc-c"),
            ])),
            std::sync::Arc::new(StringArray::from(vec![None::<&str>, None, None])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn events_path_keeps_only_the_legacy_raw_index_key() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    // Explicit per-context config instead of `SIGLAKE_INVERTED_INDEX=1`: the
    // other test in this binary runs on a parallel thread and configures its
    // own context differently.
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true);
    ice.append_events(&[Event::now("database timeout")])
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
    let keys: Vec<&str> = kv
        .iter()
        .filter_map(|entry| {
            entry
                .key
                .starts_with("siglake.inverted_index.v1")
                .then_some(entry.key.as_str())
        })
        .collect();
    assert_eq!(keys, vec![siglake_index::INVERTED_INDEX_KV_KEY]);
}

#[tokio::test]
async fn custom_indexes_write_per_column_blobs_and_prune_non_raw_queries() {
    use futures::TryStreamExt;
    use iceberg::arrow::{ArrowReaderBuilder, RawPruneSpec};
    use iceberg::scan::{FileScanTask, FileScanTaskStream};
    use iceberg::spec::DataFileFormat;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    // Explicit per-context config instead of `SIGLAKE_INVERTED_INDEX=1` and
    // `SIGLAKE_ICEBERG_METADATA_CACHE_TTL_SECS=0`: the sibling test does not
    // want the zero TTL, and both run on parallel threads.
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO);
    let config = logs_config();
    let ident = ice.create_index(&config).await.unwrap();
    ice.append_to_table(&ident, logs_batch(), &["service"])
        .await
        .unwrap();

    let parquet_files = find_parquet(&warehouse);
    assert_eq!(
        parquet_files.len(),
        1,
        "expected one custom-index data file"
    );
    let bytes = std::fs::read(&parquet_files[0]).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let kv = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("footer key_value_metadata present");
    let keys: Vec<&str> = kv
        .iter()
        .filter_map(|entry| {
            entry
                .key
                .starts_with("siglake.inverted_index.v1")
                .then_some(entry.key.as_str())
        })
        .collect();
    assert!(keys.contains(&"siglake.inverted_index.v1.message"));
    assert!(keys.contains(&"siglake.inverted_index.v1.title"));
    assert!(!keys.contains(&"siglake.inverted_index.v1.service"));
    assert!(!keys.contains(&siglake_index::INVERTED_INDEX_KV_KEY));

    let table = ice.catalog().load_table(&ident).await.unwrap();
    let files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(files.len(), 1, "expected one data file");
    let file = files[0].clone();
    let message_field_id = table
        .metadata()
        .current_schema()
        .field_id_by_name("message")
        .unwrap();
    let task = FileScanTask {
        file_size_in_bytes: file.file_size_in_bytes(),
        start: 0,
        length: 0,
        record_count: Some(file.record_count()),
        data_file_path: file.file_path().to_string(),
        data_file_format: DataFileFormat::Parquet,
        schema: table.metadata().current_schema().clone(),
        project_field_ids: vec![message_field_id],
        predicate: None,
        deletes: vec![],
        partition: Some(file.partition().clone()),
        partition_spec: Some(table.metadata().default_partition_spec().clone()),
        name_mapping: None,
        case_sensitive: false,
        statistics_blobs: vec![],
    };
    let reader = ArrowReaderBuilder::new(table.file_io().clone(), iceberg::Runtime::current())
        .with_row_selection_enabled(true)
        .with_raw_prune_spec(Some(RawPruneSpec {
            column: "message".to_string(),
            all_terms: vec!["database".to_string()],
            ..Default::default()
        }))
        .build();
    let batches = reader
        .read(Box::pin(futures::stream::iter(vec![Ok(task)].into_iter())) as FileScanTaskStream)
        .unwrap()
        .stream()
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap();
    let count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(count, 2);
    let messages: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..column.len())
                .map(|idx| column.value(idx).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(messages.iter().all(|message| message.contains("database")));

    let snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter_sum(&snapshot, "siglake_iceberg_inverted_index_used_total") > 0,
        "expected non-raw indexed column to trigger row-level pruning"
    );
}
