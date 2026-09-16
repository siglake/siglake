//! Does a TYPED (`long`) column keep its group-count footer through a merge?
//!
//! `group_count_recluster.rs` proves the footer KEY survives re-clustering, but
//! it only ever asserts on a Utf8 column. The footer column set is chosen by
//! `group_count_columns_for`, which adds Int64/Float64/Boolean fields on top of
//! the (Utf8-only) bloom columns — so a typed column reaches the footer by a
//! DIFFERENT route than a text one, and the merge writer derives its schema from
//! the table rather than the batch. That combination is the untested cell.
//!
//! It matters because a typed column can never be served by Tier-1: both
//! side-aggregate builders downcast to `StringArray`, so `status` is absent from
//! the inline and wide aggregates by construction. The footer sum is the ONLY
//! cheap tier it has. If a merge drops it, `count(*) WHERE status = 404` falls
//! to the raw-page RLE decode — still `rows_scanned: 0`, so it stays invisible
//! in every scan-based assertion, and only shows up as latency.

use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::Utc;
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_storage::iceberg::{IcebergContext, ReclusterMergeOptions, GROUP_COUNTS_KV_KEY};
use siglake_storage::ScanShard;

fn field(name: &str, field_type: FieldType, required: bool) -> FieldMapping {
    FieldMapping {
        name: name.to_string(),
        field_type,
        required,
    }
}

/// The bench http_logs shape in miniature: a raw-tokenized text dim (`method`,
/// a tag field ⇒ a bloom column) beside a typed `status`, which is NOT.
fn httplogs_config() -> IndexConfig {
    IndexConfig {
        index_id: "httplogs".to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                field("timestamp", FieldType::Datetime, true),
                field(
                    "method",
                    FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    false,
                ),
                field("status", FieldType::Long, false),
                field(
                    "host",
                    FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    false,
                ),
                field(
                    "path",
                    FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    false,
                ),
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec!["method".to_string(), "host".to_string(), "path".to_string()],
            default_search_fields: vec![],
        },
        retention: None,
        index_at_flush: None,
    }
}

fn batch(config: &IndexConfig, base_us: i64, n: i64) -> RecordBatch {
    let wide: i64 = std::env::var("WIDE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let schema = config.to_arrow_schema();
    let ts: Vec<Option<i64>> = (0..n).map(|i| Some(base_us + i)).collect();
    let methods: Vec<Option<&str>> = (0..n)
        .map(|i| {
            if i % 2 == 0 {
                Some("GET")
            } else {
                Some("POST")
            }
        })
        .collect();
    // Nine distinct statuses, exactly like the bench corpus.
    let statuses: Vec<Option<i64>> = (0..n)
        .map(|i| Some([200i64, 200, 200, 304, 404, 404, 500, 302, 206][(i % 9) as usize]))
        .collect();
    let hosts: Vec<String> = (0..n).map(|i| format!("host-{:06}", i % wide)).collect();
    let paths: Vec<String> = (0..n)
        .map(|i| format!("/a/b/c/resource-{:06}", i % wide))
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("+00:00")),
            std::sync::Arc::new(StringArray::from(methods)),
            std::sync::Arc::new(Int64Array::from(statuses)),
            std::sync::Arc::new(StringArray::from(hosts)),
            std::sync::Arc::new(StringArray::from(paths)),
            // Dynamic mapping mode appends the WS-7 residual `attributes` column.
            std::sync::Arc::new(StringArray::from(vec![None::<&str>; n as usize])),
        ],
    )
    .unwrap()
}

/// The group-count footer's column list for a local-fs Parquet file.
fn footer_group_count_columns(path: &str) -> Vec<String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let local = path.strip_prefix("file://").unwrap_or(path);
    let file = std::fs::File::open(local).expect("open parquet file");
    let reader = SerializedFileReader::new(file).expect("parquet reader");
    let blob = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|kvs| {
            kvs.iter()
                .find(|e| e.key == GROUP_COUNTS_KV_KEY)
                .and_then(|e| e.value.clone())
        })
        .unwrap_or_else(|| panic!("{path} carries no group-count footer at all"));
    let mut cols = siglake_bloom::group_counts::decode_column_names(&blob)
        .unwrap_or_else(|| panic!("{path} group-count footer did not decode"));
    cols.sort();
    cols
}

#[tokio::test]
async fn a_typed_column_keeps_its_group_count_footer_through_a_merge() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let config = httplogs_config();
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident(&config.index_id);

    let base = Utc::now().timestamp_micros();
    for b in 0..3i64 {
        ice.append_to_table(
            &ident,
            batch(&config, base + b * 1_000_000, 90),
            &["method", "host", "path"],
        )
        .await
        .unwrap();
    }

    // Ingest side: the typed column must reach the footer, alongside the text one.
    let pre = ice.live_data_files(&ident).await.unwrap();
    assert!(pre.len() >= 2, "need >=2 files to merge, got {}", pre.len());
    for f in &pre {
        let cols = footer_group_count_columns(f.file_path());
        assert!(
            cols.iter().any(|c| c == "status"),
            "APPEND dropped the typed column from the group-count footer: {cols:?}"
        );
    }

    // The whole point: it must still be there after a merge.
    let merge = ReclusterMergeOptions {
        force_streaming: Some(true),
        ..ReclusterMergeOptions::default()
    };
    let stats = ice
        .recluster_files_with(&ident, pre, &["method", "host", "path"], &merge)
        .await
        .expect("streaming recluster");
    assert_eq!(stats.rows, 270, "recluster conserves rows");

    let post = ice.live_data_files(&ident).await.unwrap();
    assert!(!post.is_empty());
    for f in &post {
        let cols = footer_group_count_columns(f.file_path());
        assert!(
            cols.iter().any(|c| c == "status"),
            "MERGE dropped the typed column from the group-count footer: {cols:?} \
             — every `WHERE status = ...` count now falls to a raw-page RLE decode"
        );
    }

    // And the typed column must be served by the SAME cheap tier as the text
    // one. Before the side-aggregate builders learned the footer writer's cast,
    // `status` was absent from the aggregate entirely and this read
    // "materialized" — the footer-sum fallback, one footer read per live file,
    // which the field measured at 34-89ms against 2.6ms for `method`.
    for (column, want) in [("status", "tier1_inline"), ("method", "tier1_inline")] {
        let g = ice
            .grouped_counts_with_summary("httplogs", column, None, None)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("grouped counts for {column}"));
        assert_eq!(
            g.source_label(),
            want,
            "{column} must be served by the table-level aggregate, not a per-file fallback"
        );
    }

    // And the counts must be right whichever tier served them.
    let rows = ice
        .grouped_counts_with_summary("httplogs", "status", None, None)
        .await
        .unwrap()
        .expect("grouped counts for the typed column");
    let mut got = rows.to_rows();
    got.sort();
    assert_eq!(
        got,
        vec![
            (Some("200".into()), 90),
            (Some("206".into()), 30),
            (Some("302".into()), 30),
            (Some("304".into()), 30),
            (Some("404".into()), 60),
            (Some("500".into()), 30),
        ],
        "typed group counts after merge"
    );
}

/// What does the ONLY cheap tier a typed column has actually cost per file?
///
/// Run explicitly: `cargo test -p siglake-storage --test storage
/// typed_group_count_footer::report_tier2_cost_per_file -- --ignored --nocapture`. Reports µs per live file for the Tier-2 footer sum
/// with the footer cache warm, which is the regime the bench suite measures
/// (`served_by: "materialized"`, `rows_scanned: 0`, no S3).
#[tokio::test]
#[ignore]
async fn report_tier2_cost_per_file() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let config = httplogs_config();
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident(&config.index_id);

    let files: i64 = std::env::var("FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(170);

    let rows_per_file: i64 = std::env::var("ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(90);

    let rows = rows_per_file;
    let base = Utc::now().timestamp_micros();
    for b in 0..files {
        ice.append_to_table(
            &ident,
            batch(&config, base + b * 1_000_000, rows),
            &["method", "host", "path"],
        )
        .await
        .unwrap();
    }
    let live = ice.live_data_files(&ident).await.unwrap().len();

    // Compare the manifest plan Tier-2 used to repeat on every query with the
    // full warm-call cost after the per-snapshot task-list cache was added.
    {
        use futures::TryStreamExt;
        let ident2 = ice.index_table_ident("httplogs");
        let iters = 20;

        // (a) catalog load_table — what a cache MISS on the table entry costs.
        let start = std::time::Instant::now();
        for _ in 0..iters {
            let _ = iceberg::Catalog::load_table(ice.catalog().as_ref(), &ident2)
                .await
                .unwrap();
        }
        let load_per = start.elapsed() / iters;

        // (b) plan_files on an ALREADY-loaded table — the work a cold snapshot
        // still pays once, and warm Tier-2 calls now skip.
        let t = iceberg::Catalog::load_table(ice.catalog().as_ref(), &ident2)
            .await
            .unwrap();
        let start = std::time::Instant::now();
        let mut n = 0usize;
        for _ in 0..iters {
            let tasks: Vec<_> = t
                .scan()
                .select(vec!["status".to_string()])
                .build()
                .unwrap()
                .plan_files()
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            n = tasks.len();
        }
        let plan_per = start.elapsed() / iters;
        println!(
            "load_table  per_call={:>9.3}ms    plan_files(cached table) files={n} per_call={:>9.3}ms",
            load_per.as_secs_f64() * 1000.0,
            plan_per.as_secs_f64() * 1000.0
        );
    }

    // A one-way shard owns every file but cannot use the whole-table Tier-1 or
    // snapshot-result caches, keeping every timed call on Tier-2.
    let shard = Some(ScanShard { index: 0, count: 1 });
    for column in ["status", "method"] {
        // Warm the footer cache, then measure.
        for _ in 0..3 {
            ice.grouped_counts_with_summary("httplogs", column, shard, None)
                .await
                .unwrap();
        }
        let mut label = "";
        let start = std::time::Instant::now();
        let iters = 20;
        for _ in 0..iters {
            let g = ice
                .grouped_counts_with_summary("httplogs", column, shard, None)
                .await
                .unwrap()
                .unwrap();
            label = g.source_label();
        }
        let per_call = start.elapsed() / iters;
        println!(
            "column={column:<7} served_by={label:<12} files={live} \
             per_call={:>9.3}ms per_file={:>7.1}us",
            per_call.as_secs_f64() * 1000.0,
            per_call.as_secs_f64() * 1e6 / live as f64,
        );
    }
}
