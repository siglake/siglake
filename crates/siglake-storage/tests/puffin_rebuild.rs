use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, StringArray};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, IcebergTuning, ReclusterMergeOptions};

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

/// `siglake_iceberg_inverted_index_used_total` is a process-wide counter and
/// `DebuggingRecorder` reports it as a delta since the last snapshot, so a test
/// that attributes it to its own queries can only do so while no other test in
/// this binary is running a Puffin-indexed text query. Both sides take this
/// gate. Serializing them costs nothing: the whole file runs in under two
/// seconds.
static PUFFIN_QUERY_GATE: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

async fn ordered_text_rows(ctx: &SessionContext, term: &str) -> Vec<String> {
    let sql = format!(
        "SELECT timestamp, raw FROM events WHERE raw LIKE '%{term}%' \
         ORDER BY timestamp DESC LIMIT 20"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let raw = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| raw.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn ordered_text_context() -> SessionContext {
    let base = siglake_storage::session_context_with_order(
        Some(1),
        None,
        Some(siglake_storage::PreferredScanOrder { descending: true }),
    );
    let mut state = base.state();
    state
        .config_mut()
        .set_extension(std::sync::Arc::new(siglake_storage::OrderedScanLimit {
            limit: 20,
        }));
    // Exercise the ordered drain as well as the ordinary filtered TopK plan.
    // The query server can set this when it has an independent selectivity
    // estimate; LIKE itself is normally excluded from that policy.
    state
        .config_mut()
        .set_extension(std::sync::Arc::new(siglake_storage::OrderedResidualHint {
            allow: true,
        }));
    SessionContext::new_with_state(state)
}

/// What the query server builds for a text query carrying a literal `LIMIT n`
/// with no `ORDER BY`: `ClippedScanLimit` and neither ordering extension. See
/// `sql.rs::clipping_scan_limit` for which statements qualify.
fn clipped_text_context(limit: usize) -> SessionContext {
    let base = siglake_storage::session_context_with_order(Some(4), None, None);
    let mut state = base.state();
    state
        .config_mut()
        .set_extension(std::sync::Arc::new(siglake_storage::ClippedScanLimit {
            limit,
        }));
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(ScalarUDF::from(MatchTermsUdf::new()));
    ctx
}

async fn rewritten_text_fixture(
    path: &std::path::Path,
    rebuild: bool,
    events: &[Event],
) -> IcebergContext {
    let ice = IcebergContext::open(path)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(IcebergTuning {
            index_rebuild: Some(rebuild),
            ..Default::default()
        });
    for chunk in events.chunks(100) {
        ice.append_events(chunk).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();
    let files = ice.live_data_files(&ident).await.unwrap();
    let blooms = ice.events_bloom_columns();
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    ice.recluster_files_with(
        &ident,
        files,
        &bloom_refs,
        &ReclusterMergeOptions {
            force_streaming: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    ice
}

async fn assert_ordered_limit_declines_puffin_row_selection(snapshotter: &Snapshotter) {
    use chrono::{Duration, TimeZone, Utc};

    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let events: Vec<Event> = (0..400)
        .map(|row| {
            let rare = if row % 50 == 0 { " rareneedle" } else { "" };
            let common = if row % 2 == 0 { "common" } else { "other" };
            let mut event = Event::now(format!("{common}{rare} row-{row:04}"));
            event.timestamp = base + Duration::seconds(row);
            event
        })
        .collect();

    let tmp = tempfile::tempdir().unwrap();
    let indexed = rewritten_text_fixture(&tmp.path().join("indexed"), true, &events).await;
    let unindexed = rewritten_text_fixture(&tmp.path().join("unindexed"), false, &events).await;

    let indexed_files = indexed
        .live_data_files(indexed.events_table_ident())
        .await
        .unwrap();
    let indexed_table = indexed
        .catalog()
        .load_table(indexed.events_table_ident())
        .await
        .unwrap();
    assert!(
        !indexed_files.is_empty()
            && indexed_files
                .iter()
                .all(|file| puffin_indexes_file(&indexed_table, file)),
        "indexed arm must carry Puffin registrations"
    );
    let unindexed_files = unindexed
        .live_data_files(unindexed.events_table_ident())
        .await
        .unwrap();
    let unindexed_table = unindexed
        .catalog()
        .load_table(unindexed.events_table_ident())
        .await
        .unwrap();
    assert!(
        !unindexed_files.is_empty()
            && unindexed_files
                .iter()
                .all(|file| !puffin_indexes_file(&unindexed_table, file)),
        "control arm must have the same rewrite without Puffin registrations"
    );
    let layout = |files: &[DataFile]| {
        files
            .iter()
            .map(|file| (file.record_count(), file.file_size_in_bytes()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        layout(&indexed_files),
        layout(&unindexed_files),
        "indexed and control arms must have identical Parquet layout"
    );

    let indexed_ctx = ordered_text_context();
    indexed
        .register_with_datafusion(&indexed_ctx)
        .await
        .unwrap();
    let unindexed_ctx = ordered_text_context();
    unindexed
        .register_with_datafusion(&unindexed_ctx)
        .await
        .unwrap();

    let ordered = indexed_ctx
        .sql(
            "SELECT timestamp, raw FROM events WHERE raw LIKE '%common%' \
             ORDER BY timestamp DESC LIMIT 20",
        )
        .await
        .unwrap();
    let plan = format!(
        "{}",
        datafusion::physical_plan::displayable(
            ordered.create_physical_plan().await.unwrap().as_ref()
        )
        .indent(true)
    );
    assert!(
        !plan.contains("SortExec"),
        "fixture must exercise the ordered early-stop drain:\n{plan}"
    );

    // Clear compaction counters before attributing the queries below.
    let _ = snapshotter.snapshot();
    for term in ["common", "rareneedle"] {
        let expected = ordered_text_rows(&unindexed_ctx, term).await;
        let cold = ordered_text_rows(&indexed_ctx, term).await;
        let warm = ordered_text_rows(&indexed_ctx, term).await;
        assert_eq!(cold, expected, "indexed cold {term} result diverged");
        assert_eq!(warm, expected, "indexed warm {term} result diverged");
    }
    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(
            &snapshot,
            "siglake_iceberg_inverted_index_used_total",
            Some(("storage", "puffin")),
        ),
        0,
        "ordered LIMIT must not load/apply the Puffin row index"
    );
    assert!(
        counter_sum(
            &snapshot,
            "siglake_query_inverted_index_declined_total",
            Some(("reason", "ordered_limit")),
        ) > 0,
        "planner must attribute the ordered-LIMIT decline"
    );
}

async fn footer_has_raw_index(table: &Table, file: &DataFile) -> bool {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let bytes = table
        .file_io()
        .new_input(file.file_path())
        .unwrap()
        .read()
        .await
        .unwrap();
    ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry.key == siglake_index::INVERTED_INDEX_KV_KEY)
        })
}

fn puffin_indexes_file(table: &Table, file: &DataFile) -> bool {
    table.metadata().statistics_iter().any(|stats_file| {
        stats_file.blob_metadata.iter().any(|blob| {
            blob.r#type == "siglake-inverted-v1"
                && blob
                    .properties
                    .get("data_file")
                    .is_some_and(|path| path == file.file_path())
        })
    })
}

/// The opt-in arm: `SIGLAKE_INDEX_REBUILD=1` in deployment terms, spelled as
/// explicit tuning so the test states what it exercises and survives the
/// default flipping under it (it has, twice).
#[tokio::test]
async fn streaming_recluster_rebuilds_once_and_survives_snapshot_expiry() {
    // This test attributes a process-wide counter to its own queries.
    let _gate = PUFFIN_QUERY_GATE.lock().await;
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
        .with_tuning(IcebergTuning {
            index_rebuild: Some(true),
            ..Default::default()
        });
    for chunk in [
        vec![
            Event::now("database timeout"),
            Event::now("healthy startup"),
        ],
        vec![
            Event::now("database migration"),
            Event::now("info heartbeat"),
        ],
        vec![Event::now("database retry"), Event::now("steady state")],
    ] {
        ice.append_events(&chunk).await.unwrap();
    }

    let ident = ice.events_table_ident().clone();
    let before = ice.live_data_files(&ident).await.unwrap();
    assert!(before.len() >= 2, "need multiple files to recluster");

    let bloom = ice.events_bloom_columns();
    let bloom_refs: Vec<&str> = bloom.iter().map(String::as_str).collect();
    let stats = ice
        .recluster_files_with(
            &ident,
            before,
            &bloom_refs,
            &siglake_storage::iceberg::ReclusterMergeOptions {
                force_streaming: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(stats.files_removed >= 2);

    let after = ice.live_data_files(&ident).await.unwrap();
    assert!(
        !after.is_empty(),
        "recluster should leave replacement files behind"
    );
    let table = ice.catalog().load_table(&ident).await.unwrap();
    for file in &after {
        assert!(
            !footer_has_raw_index(&table, file).await,
            "streaming rewrite should not write whole-file footer indexes directly"
        );
        assert!(
            puffin_indexes_file(&table, file),
            "the rebuild pass should register each streaming rewrite output"
        );
    }
    let stats_files: Vec<_> = table.metadata().statistics_iter().collect();
    let before_stats_count = stats_files.len();
    assert_eq!(
        ice.rebuild_inverted_indexes_for_files(&ident, &after)
            .await
            .unwrap(),
        0,
        "a second rebuild pass should detect that the rewritten files are already indexed"
    );
    let reloaded = ice.catalog().load_table(&ident).await.unwrap();
    assert_eq!(
        reloaded.metadata().statistics_iter().len(),
        before_stats_count,
        "a no-op rebuild must not register a second statistics file"
    );

    ice.append_events(&[Event::now("database after rewrite")])
        .await
        .unwrap();
    let current = ice.live_data_files(&ident).await.unwrap();
    let inline: Vec<_> = current
        .iter()
        .filter(|file| {
            !after
                .iter()
                .any(|rewritten| rewritten.file_path() == file.file_path())
        })
        .collect();
    assert_eq!(
        inline.len(),
        1,
        "the append should add one inline-indexed file"
    );
    let table = ice.catalog().load_table(&ident).await.unwrap();
    assert!(footer_has_raw_index(&table, inline[0]).await);

    assert!(ice.expire_snapshots(&ident, 1).await.unwrap() > 0);
    let expired = ice.catalog().load_table(&ident).await.unwrap();
    assert!(
        after.iter().all(|file| puffin_indexes_file(&expired, file)),
        "Puffin statistics registration should survive expiry of its data snapshot"
    );
    let expired_stats_count = expired.metadata().statistics_iter().count();
    assert_eq!(
        ice.rebuild_inverted_indexes_for_files(&ident, &current)
            .await
            .unwrap(),
        0,
        "retained Puffin registrations and appended footer indexes should both skip rebuild"
    );
    assert_eq!(
        ice.catalog()
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .statistics_iter()
            .count(),
        expired_stats_count,
        "the no-op rebuild after expiry must not register another statistics file"
    );

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    assert_eq!(count(&ctx, "SELECT count(*) FROM events").await, 7);
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) FROM events WHERE raw LIKE '%database%'",
        )
        .await,
        4
    );
    let snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter_sum(
            &snapshot,
            "siglake_iceberg_inverted_index_used_total",
            Some(("storage", "puffin")),
        ) > 0,
        "queries over rewritten files should row-prune from rebuilt Puffin blobs"
    );

    assert_ordered_limit_declines_puffin_row_selection(&snapshotter).await;
}

#[tokio::test]
async fn inram_recluster_keeps_footer_indexes_without_rebuilding_puffin() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_footer_max_bytes: Some(usize::MAX),
            // The skip is what this test is about, so the rebuild has to be on:
            // with the shipped default it would have nothing to skip.
            index_rebuild: Some(true),
            ..Default::default()
        });
    ice.append_events(&[Event::now("database timeout"), Event::now("healthy")])
        .await
        .unwrap();
    ice.append_events(&[Event::now("database retry"), Event::now("steady")])
        .await
        .unwrap();

    let ident = ice.events_table_ident().clone();
    let before = ice.live_data_files(&ident).await.unwrap();
    let bloom = ice.events_bloom_columns();
    let bloom_refs: Vec<&str> = bloom.iter().map(String::as_str).collect();
    ice.recluster_files_with(
        &ident,
        before,
        &bloom_refs,
        &siglake_storage::iceberg::ReclusterMergeOptions {
            force_streaming: Some(false),
            inram_max_bytes: Some(u64::MAX),
            inram_max_rows: Some(u64::MAX),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let after = ice.live_data_files(&ident).await.unwrap();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    for file in &after {
        assert!(footer_has_raw_index(&table, file).await);
        assert!(!puffin_indexes_file(&table, file));
    }
    assert_eq!(
        ice.rebuild_inverted_indexes_for_files(&ident, &after)
            .await
            .unwrap(),
        0,
        "footer-indexed rewrite outputs should skip Puffin rebuild"
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
}

async fn text_rows(ctx: &SessionContext, term: &str) -> Vec<String> {
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{term}%'");
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut rows: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let raw = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| raw.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    rows
}

/// #3896: a text query over a Puffin-indexed file deserialized the whole index
/// again on every execution, however warm the blob cache was. The second
/// execution must be served from the parsed index the first one left behind,
/// and both must return what the same rewrite without indexes returns.
#[tokio::test]
async fn repeated_text_query_reuses_the_parsed_puffin_index() {
    use chrono::{Duration, TimeZone, Utc};

    let _gate = PUFFIN_QUERY_GATE.lock().await;

    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let events: Vec<Event> = (0..600)
        .map(|row| {
            let rare = if row % 50 == 0 { " rareneedle" } else { "" };
            let common = if row % 2 == 0 { "common" } else { "other" };
            let mut event = Event::now(format!("{common}{rare} row-{row:04}"));
            event.timestamp = base + Duration::seconds(row);
            event
        })
        .collect();

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("indexed");
    let indexed = rewritten_text_fixture(&warehouse, true, &events).await;
    let control = rewritten_text_fixture(&tmp.path().join("control"), false, &events).await;

    let table = indexed
        .catalog()
        .load_table(indexed.events_table_ident())
        .await
        .unwrap();
    let files = indexed
        .live_data_files(indexed.events_table_ident())
        .await
        .unwrap();
    assert!(!files.is_empty());
    for file in &files {
        assert!(
            puffin_indexes_file(&table, file) && !footer_has_raw_index(&table, file).await,
            "the fixture must read its index from Puffin, not from the footer"
        );
    }

    let ctx = SessionContext::new();
    indexed.register_with_datafusion(&ctx).await.unwrap();
    let control_ctx = SessionContext::new();
    control
        .register_with_datafusion(&control_ctx)
        .await
        .unwrap();

    // Statistics paths under this warehouse only: other tests query their own
    // warehouses in this process.
    let warehouse = warehouse.to_string_lossy().to_string();
    let cache_stats = || iceberg::arrow::parsed_inverted_index_cache_stats(&warehouse);
    assert_eq!(
        cache_stats(),
        (0, 0),
        "nothing cached before the first query"
    );

    for term in ["rareneedle", "common", "absentterm"] {
        let expected = text_rows(&control_ctx, term).await;
        let cold = text_rows(&ctx, term).await;
        let (entries, hits_after_cold) = cache_stats();
        assert!(
            entries > 0,
            "the {term} query must have parsed and kept an index"
        );

        let warm = text_rows(&ctx, term).await;
        let (warm_entries, hits_after_warm) = cache_stats();

        assert_eq!(
            cold, expected,
            "cold {term} result diverged from the control"
        );
        assert_eq!(
            warm, expected,
            "warm {term} result diverged from the control"
        );
        assert_eq!(
            warm_entries, entries,
            "the warm {term} query must reuse the cached indexes, not decode new ones"
        );
        assert!(
            hits_after_warm > hits_after_cold,
            "the warm {term} query must be served from the parsed index \
             ({hits_after_cold} -> {hits_after_warm} lookups)"
        );
    }
    assert_eq!(
        text_rows(&ctx, "rareneedle").await.len(),
        12,
        "the fixture's needle is on every fiftieth row"
    );

    // The serialized blobs are retained alongside the parsed indexes, and the
    // running total the byte bound (#3966) is enforced against counts them.
    // Without this the bound would be a structure nothing on the query path
    // feeds.
    let (blob_entries, blob_bytes, total_bytes) =
        iceberg::arrow::puffin_blob_cache_stats(&warehouse);
    assert!(
        blob_entries > 0 && blob_bytes > 0,
        "the queried files' index blobs must be cached ({blob_entries} entries, {blob_bytes} B)"
    );
    assert!(
        total_bytes >= blob_bytes,
        "the cache's running byte total ({total_bytes}) must cover this warehouse's \
         retained blobs ({blob_bytes})"
    );
}

/// Appends only, one file per call: the flush path puts a file's index in the
/// Parquet footer whenever the serialized blob fits `index_footer_max_bytes`,
/// so this leaves the footer-KV storage shape with no Puffin registration
/// anywhere. `indexed` off is the control — the same files, no index.
async fn footer_text_fixture(
    path: &std::path::Path,
    indexed: bool,
    events: &[Event],
) -> IcebergContext {
    let ice = IcebergContext::open(path)
        .await
        .unwrap()
        .with_inverted_index(indexed)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(IcebergTuning {
            index_rebuild: Some(false),
            ..Default::default()
        });
    for chunk in events.chunks(200) {
        ice.append_events(chunk).await.unwrap();
    }
    ice
}

/// #3965: #3896 stopped a text query re-deserializing a Puffin-stored index,
/// and left the other storage shape alone — a file whose index rides the
/// Parquet footer (`siglake.inverted_index.v1`, what the flush path writes)
/// was hex-decoded and parsed on every execution, and the parsed form dropped
/// at scope end.
///
/// THE DEFECT THIS GUARDS. Repeat the same text query over footer-indexed
/// files and the second execution must deserialize nothing: the entry count
/// stays put while the lookups it served grow, and the process's decode
/// counter does not move. One entry per file is the key-separation half — a
/// key that collapsed the files onto each other would leave a single entry and
/// wrong rows. (The column half of the key is a fork unit test,
/// `parsed_index_cache_keys_separate_storage_shapes_files_and_columns`: the
/// events table indexes `raw` alone.)
///
/// Run it with `SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES=0` for the pre-change
/// arm, where every execution decodes again and the warm assertions fail.
#[tokio::test]
async fn repeated_text_query_reuses_the_parsed_footer_index() {
    use chrono::{Duration, TimeZone, Utc};

    // This test attributes a process-wide decode counter to its own queries.
    let _gate = PUFFIN_QUERY_GATE.lock().await;

    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let events: Vec<Event> = (0..600)
        .map(|row| {
            let rare = if row % 50 == 0 { " rareneedle" } else { "" };
            let common = if row % 2 == 0 { "common" } else { "other" };
            let mut event = Event::now(format!("{common}{rare} row-{row:04}"));
            event.timestamp = base + Duration::seconds(row);
            event
        })
        .collect();

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("indexed");
    let indexed = footer_text_fixture(&warehouse, true, &events).await;
    let control = footer_text_fixture(&tmp.path().join("control"), false, &events).await;

    let table = indexed
        .catalog()
        .load_table(indexed.events_table_ident())
        .await
        .unwrap();
    let files = indexed
        .live_data_files(indexed.events_table_ident())
        .await
        .unwrap();
    assert!(
        files.len() > 1,
        "the fixture needs several files, so a per-file key is distinguishable \
         from a colliding one ({} files)",
        files.len()
    );
    for file in &files {
        assert!(
            footer_has_raw_index(&table, file).await && !puffin_indexes_file(&table, file),
            "the fixture must read its index from the footer, not from Puffin"
        );
    }

    let ctx = SessionContext::new();
    indexed.register_with_datafusion(&ctx).await.unwrap();
    let control_ctx = SessionContext::new();
    control
        .register_with_datafusion(&control_ctx)
        .await
        .unwrap();

    // Data-file paths under this warehouse only: other tests in this binary
    // query their own warehouses.
    let warehouse = warehouse.to_string_lossy().to_string();
    let cache_stats = || iceberg::arrow::parsed_inverted_index_cache_stats(&warehouse);
    let decodes = || iceberg::arrow::inverted_index_decode_counts().0;
    assert_eq!(
        cache_stats(),
        (0, 0),
        "nothing cached before the first query"
    );

    for (position, term) in ["rareneedle", "common", "absentterm"].iter().enumerate() {
        let expected = text_rows(&control_ctx, term).await;

        let before_decodes = decodes();
        let (_, before_lookups) = cache_stats();
        let cold = text_rows(&ctx, term).await;
        let cold_decodes = decodes() - before_decodes;
        let (entries, cold_lookups) = cache_stats();

        let warm = text_rows(&ctx, term).await;
        let warm_decodes = decodes() - before_decodes - cold_decodes;
        let (warm_entries, warm_lookups) = cache_stats();

        // Files whose index the cold execution actually consulted — decoded or
        // read from the cache. A term the file-level trigram bloom rules out
        // never reaches the index, and then neither execution looks one up.
        let consulted = cold_decodes + (cold_lookups - before_lookups);

        assert_eq!(
            cold, expected,
            "cold {term} result diverged from the control"
        );
        assert_eq!(
            warm, expected,
            "warm {term} result diverged from the control"
        );
        assert_eq!(
            warm_decodes, 0,
            "the warm {term} query must not hex-decode and parse a footer index again"
        );
        assert_eq!(
            warm_entries, entries,
            "the warm {term} query must reuse the cached indexes, not decode new ones"
        );
        assert_eq!(
            warm_lookups - cold_lookups,
            consulted,
            "the warm {term} query must be served from the parsed index of every \
             file the cold one consulted ({consulted} files)"
        );
        if position == 0 {
            // Nothing is cached yet, so this execution pays the decode — once
            // per file, which is what every execution used to pay.
            assert_eq!(
                cold_decodes,
                files.len() as u64,
                "{term}: the first query parses each file's footer index exactly once"
            );
            assert_eq!(
                cold_lookups, before_lookups,
                "{term}: a decode is not a cache hit"
            );
        } else {
            assert_eq!(
                cold_decodes, 0,
                "{term}: a different term over the same files must reuse their parsed indexes"
            );
        }
    }

    let (entries, _) = cache_stats();
    assert_eq!(
        entries,
        files.len(),
        "each file's footer index is its own entry"
    );
    assert_eq!(
        text_rows(&ctx, "rareneedle").await.len(),
        12,
        "the fixture's needle is on every fiftieth row"
    );
}

/// #4056: the budget the query server resolves from the pod's memory limit must
/// reach the caches, not just the number the pool subtracts.
///
/// THE DEFECT THIS GUARDS. Both text-index caches were flat constants — 1 GiB
/// parsed plus 256 MiB of blobs on every pod, which is the whole of what the
/// packaged 4Gi query pod leaves outside its pool and its other caches. The
/// fork cannot read the cgroup limit itself (that lives in this crate, which
/// depends on the fork), so the budget arrives through
/// `configure_text_index_caches`. The first arm is the negative control: before
/// that push existed, a budget of one byte cached exactly as much as a budget
/// of a gigabyte.
#[tokio::test]
async fn configured_text_index_budgets_govern_what_is_cached() {
    use chrono::{Duration, TimeZone, Utc};

    let _gate = PUFFIN_QUERY_GATE.lock().await;

    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let events: Vec<Event> = (0..600)
        .map(|row| {
            let rare = if row % 50 == 0 { " rareneedle" } else { "" };
            let mut event = Event::now(format!("common{rare} row-{row:04}"));
            event.timestamp = base + Duration::seconds(row);
            event
        })
        .collect();

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("indexed");
    let indexed = rewritten_text_fixture(&path, true, &events).await;
    let ctx = SessionContext::new();
    indexed.register_with_datafusion(&ctx).await.unwrap();

    // Statistics paths under this warehouse only: other tests query their own.
    let warehouse = path.to_string_lossy().to_string();
    let parsed_entries = || iceberg::arrow::parsed_inverted_index_cache_stats(&warehouse).0;
    let blob_entries = || iceberg::arrow::puffin_blob_cache_stats(&warehouse).0;

    // A budget too small for one index retains nothing on either side.
    siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
        parsed_index_max_bytes: 1,
        puffin_blob_max_bytes: 1,
    });
    assert_eq!(text_rows(&ctx, "rareneedle").await.len(), 12);
    assert_eq!(
        (parsed_entries(), blob_entries()),
        (0, 0),
        "a one-byte budget must retain neither the parsed index nor its blob"
    );

    // What a 16Gi pod derives holds both. (The packaged 4Gi pod derives
    // nothing: its whole remainder is the pool's first-file decode
    // reservation.)
    let derived =
        siglake_storage::resolve_text_index_cache_config(Some(16 * 1024 * 1024 * 1024), None, None);
    siglake_storage::configure_text_index_caches(derived);
    assert_eq!(text_rows(&ctx, "rareneedle").await.len(), 12);
    assert!(
        parsed_entries() > 0 && blob_entries() > 0,
        "the derived 16Gi budget must hold this fixture's index ({} parsed, {} blobs)",
        parsed_entries(),
        blob_entries()
    );

    // Leave the process as it was found: a configured budget wins over the
    // environment, including the `SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES=0` A/B
    // the measurement below is run under.
    iceberg::arrow::clear_text_index_cache_max_bytes();
}

/// Corpus-shaped rows for the measurement below: ten tokens, one rare needle
/// on every thousandth row.
fn measurement_event(row: usize, base: chrono::DateTime<chrono::Utc>) -> Event {
    let rare = if row.is_multiple_of(1000) {
        " rareneedle"
    } else {
        ""
    };
    Event {
        timestamp: base + chrono::Duration::seconds(row as i64),
        host: format!("host-{:04}", row % 2000),
        source: format!("/var/log/service-{}.log", row % 20),
        sourcetype: ["debug", "info", "warn", "error"][row % 4].into(),
        index: "main".into(),
        raw: format!(
            "service-{} status {} region reg{} bucket{:02} request complete{}",
            row % 20,
            200 + row % 5,
            row % 8,
            row % 20,
            rare
        ),
        attributes: Some(format!(r#"{{"region":"r{}"}}"#, row % 8)),
    }
}

/// Cold/warm probe for #3896, at two file sizes. Run it twice to A/B the
/// parsed-index cache — `SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES=0` is the
/// pre-#3896 behaviour, where every execution deserializes the whole index:
///
///   cargo test -p siglake-storage --release --test puffin_rebuild \
///     report_text_query_cold_warm_cost -- --ignored --nocapture
#[tokio::test]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn report_text_query_cold_warm_cost() {
    use chrono::{TimeZone, Utc};
    use std::time::Instant;

    // A measurement wants the box to itself as much as the counter test does.
    let _gate = PUFFIN_QUERY_GATE.lock().await;
    const RUNS: usize = 5;
    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();

    for rows in [1_000_000usize, 7_000_000] {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let build = Instant::now();
        let ice = IcebergContext::open(&warehouse)
            .await
            .unwrap()
            .with_inverted_index(true)
            .with_table_cache_ttl(std::time::Duration::ZERO)
            // The probe measures the Puffin storage shape, so it opts in.
            .with_tuning(IcebergTuning {
                index_rebuild: Some(true),
                ..Default::default()
            });
        for chunk in (0..rows).step_by(250_000) {
            let events: Vec<Event> = (chunk..(chunk + 250_000).min(rows))
                .map(|row| measurement_event(row, base))
                .collect();
            ice.append_events(&events).await.unwrap();
        }
        let ident = ice.events_table_ident().clone();
        let files = ice.live_data_files(&ident).await.unwrap();
        let blooms = ice.events_bloom_columns();
        let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
        ice.recluster_files_with(
            &ident,
            files,
            &bloom_refs,
            &ReclusterMergeOptions {
                force_streaming: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let files = ice.live_data_files(&ident).await.unwrap();
        let indexed = files
            .iter()
            .filter(|file| puffin_indexes_file(&table, file))
            .count();
        let file_rows: Vec<u64> = files.iter().map(|file| file.record_count()).collect();
        let build_s = build.elapsed().as_secs_f64();

        let ctx = SessionContext::new();
        ice.register_with_datafusion(&ctx).await.unwrap();
        let warehouse = warehouse.to_string_lossy().to_string();
        let mut samples = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            let started = Instant::now();
            let found = text_rows(&ctx, "rareneedle").await.len();
            samples.push((started.elapsed().as_secs_f64() * 1000.0, found));
        }
        let (entries, hits) = iceberg::arrow::parsed_inverted_index_cache_stats(&warehouse);
        let (decodes, cache_hits) = iceberg::arrow::inverted_index_decode_counts();
        let mut warm: Vec<f64> = samples[1..].iter().map(|(ms, _)| *ms).collect();
        warm.sort_by(f64::total_cmp);
        println!(
            "text query cold/warm: rows={rows} files={} indexed_files={indexed} \
             file_rows={file_rows:?} build_s={build_s:.1} matches={} \
             cold_ms={:.1} warm_median_ms={:.1} all_ms={:?} \
             cached_indexes={entries} cache_lookups={hits} \
             process_decodes={decodes} process_cache_hits={cache_hits}",
            files.len(),
            samples[0].1,
            samples[0].0,
            warm[warm.len() / 2],
            samples
                .iter()
                .map(|(ms, _)| format!("{ms:.1}"))
                .collect::<Vec<_>>(),
        );
        assert!(indexed > 0, "the measurement needs Puffin-indexed files");
    }
}

/// The same cold/warm probe for the footer-KV storage shape (#3965), which the
/// flush path writes and #3896 left decoding on every execution. The fixture
/// raises `index_footer_max_bytes` so half-million-row indexes stay in the
/// footer instead of spilling to Puffin; the deployed threshold is 1 MiB, so
/// this reports the per-row cost, not a deployed file size.
///
///   cargo test -p siglake-storage --release --test puffin_rebuild \
///     report_footer_text_query_cold_warm_cost -- --ignored --nocapture
///
/// Run it twice to A/B the cache, as with the Puffin probe:
/// `SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES=0` is the pre-#3965 behaviour.
#[tokio::test]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn report_footer_text_query_cold_warm_cost() {
    use chrono::{TimeZone, Utc};
    use std::time::Instant;

    let _gate = PUFFIN_QUERY_GATE.lock().await;
    const ROWS_PER_FILE: usize = 500_000;
    const FILES: usize = 4;
    const RUNS: usize = 5;
    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let build = Instant::now();
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(IcebergTuning {
            index_rebuild: Some(false),
            index_footer_max_bytes: Some(512 * 1024 * 1024),
            ..Default::default()
        });
    for file in 0..FILES {
        let events: Vec<Event> = (file * ROWS_PER_FILE..(file + 1) * ROWS_PER_FILE)
            .map(|row| measurement_event(row, base))
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    let ident = ice.events_table_ident().clone();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let files = ice.live_data_files(&ident).await.unwrap();
    let mut footer_indexed = 0usize;
    for file in &files {
        if footer_has_raw_index(&table, file).await {
            footer_indexed += 1;
        }
    }
    let file_rows: Vec<u64> = files.iter().map(|file| file.record_count()).collect();
    let build_s = build.elapsed().as_secs_f64();

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let warehouse = warehouse.to_string_lossy().to_string();
    let mut samples = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let started = Instant::now();
        let found = text_rows(&ctx, "rareneedle").await.len();
        samples.push((started.elapsed().as_secs_f64() * 1000.0, found));
    }
    let (entries, hits) = iceberg::arrow::parsed_inverted_index_cache_stats(&warehouse);
    let (decodes, cache_hits) = iceberg::arrow::inverted_index_decode_counts();
    let mut warm: Vec<f64> = samples[1..].iter().map(|(ms, _)| *ms).collect();
    warm.sort_by(f64::total_cmp);
    println!(
        "footer text query cold/warm: files={} footer_indexed={footer_indexed} \
         file_rows={file_rows:?} build_s={build_s:.1} matches={} \
         cold_ms={:.1} warm_median_ms={:.1} all_ms={:?} \
         cached_indexes={entries} cache_lookups={hits} \
         process_decodes={decodes} process_cache_hits={cache_hits}",
        files.len(),
        samples[0].1,
        samples[0].0,
        warm[warm.len() / 2],
        samples
            .iter()
            .map(|(ms, _)| format!("{ms:.1}"))
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        footer_indexed,
        files.len(),
        "the measurement needs every file's index in its footer"
    );
}

/// The load semaphore's share of text-query startup: five indexed files and
/// four permits means two waves of whole-index deserialization on the cold
/// execution, and — since #3896 — none on the warm one, which takes no permit.
/// Same A/B as [`report_text_query_cold_warm_cost`].
#[tokio::test]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn report_text_query_index_load_concurrency() {
    use chrono::{TimeZone, Utc};
    use std::time::Instant;

    let _gate = PUFFIN_QUERY_GATE.lock().await;
    const FILES: usize = 5;
    const ROWS_PER_FILE: usize = 1_000_000;
    const RUNS: usize = 5;
    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        // Five Puffin-indexed files is the point of the probe, so it opts in.
        .with_tuning(IcebergTuning {
            index_rebuild: Some(true),
            ..Default::default()
        });
    let ident = ice.events_table_ident().clone();
    let blooms = ice.events_bloom_columns();
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    let build = Instant::now();
    for file in 0..FILES {
        let before: Vec<String> = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();
        for half in 0..2 {
            let start = file * ROWS_PER_FILE + half * ROWS_PER_FILE / 2;
            let events: Vec<Event> = (start..start + ROWS_PER_FILE / 2)
                .map(|row| measurement_event(row, base))
                .collect();
            ice.append_events(&events).await.unwrap();
        }
        // Rewrite only this round's appends, so each round leaves one more
        // Puffin-indexed file of ROWS_PER_FILE rows.
        let fresh: Vec<DataFile> = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .into_iter()
            .filter(|f| !before.contains(&f.file_path().to_string()))
            .collect();
        ice.recluster_files_with(
            &ident,
            fresh,
            &bloom_refs,
            &ReclusterMergeOptions {
                force_streaming: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    let build_s = build.elapsed().as_secs_f64();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let files = ice.live_data_files(&ident).await.unwrap();
    let indexed = files
        .iter()
        .filter(|file| puffin_indexes_file(&table, file))
        .count();
    let file_rows: Vec<u64> = files.iter().map(|file| file.record_count()).collect();

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let warehouse = warehouse.to_string_lossy().to_string();
    let mut samples = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let started = Instant::now();
        let found = text_rows(&ctx, "rareneedle").await.len();
        samples.push((started.elapsed().as_secs_f64() * 1000.0, found));
    }
    let (entries, hits) = iceberg::arrow::parsed_inverted_index_cache_stats(&warehouse);
    let mut warm: Vec<f64> = samples[1..].iter().map(|(ms, _)| *ms).collect();
    warm.sort_by(f64::total_cmp);
    println!(
        "text query load concurrency: files={} indexed_files={indexed} file_rows={file_rows:?} \
         permits={} build_s={build_s:.1} matches={} cold_ms={:.1} warm_median_ms={:.1} \
         all_ms={:?} cached_indexes={entries} cache_lookups={hits}",
        files.len(),
        std::env::var("SIGLAKE_INDEX_LOAD_CONCURRENCY").unwrap_or_else(|_| "4".into()),
        samples[0].1,
        samples[0].0,
        warm[warm.len() / 2],
        samples
            .iter()
            .map(|(ms, _)| format!("{ms:.1}"))
            .collect::<Vec<_>>(),
    );
    assert!(indexed >= FILES, "the measurement needs one index per file");
}

/// `match_terms(raw, 'term')`, the call the benchmark's keyword shapes make.
/// The real UDF lives in `siglake-query-server`, which depends on this crate;
/// what the scan reads off the filter is the function name and its literal, so
/// this stub — the one `cold_read_budget.rs` and `storage/iceberg_round_trip.rs`
/// already register — drives the same pushdown.
#[derive(Debug, Eq, Hash, PartialEq)]
struct MatchTermsUdf {
    signature: Signature,
}

impl MatchTermsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    datafusion::arrow::datatypes::DataType::Utf8,
                    datafusion::arrow::datatypes::DataType::Utf8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MatchTermsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "match_terms"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> datafusion::error::Result<datafusion::arrow::datatypes::DataType> {
        Ok(datafusion::arrow::datatypes::DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let rows = args.number_rows;
        let cells = match &args.args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(rows)?,
        };
        let cells = cells
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("lhs must be Utf8".into())
            })?
            .clone();
        let query = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => s.clone(),
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "query must be a Utf8 literal".into(),
                ));
            }
        };
        let tokens: Vec<String> = query
            .split_whitespace()
            .map(|token| token.to_ascii_lowercase())
            .collect();
        let mut builder = BooleanBuilder::with_capacity(rows);
        for row in 0..rows {
            if cells.is_null(row) {
                builder.append_value(false);
                continue;
            }
            let haystack = cells.value(row).to_ascii_lowercase();
            builder.append_value(tokens.iter().all(|token| haystack.contains(token)));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

const TEXT_SHAPE_FILES: usize = 3;
const TEXT_SHAPE_ROWS_PER_FILE: usize = 400;

/// Corpus shaped like the benchmark's: a `queen` keyword on every fiftieth row
/// (24 matches over the fixture) and a `checkout` substring on every twentieth
/// (60). Both sit under the shapes' `LIMIT 100`, so a query returns the whole
/// match set and can be compared against the unindexed control exactly.
fn bench_shaped_event(row: usize, base: chrono::DateTime<chrono::Utc>) -> Event {
    let queen = if row.is_multiple_of(50) { " queen" } else { "" };
    let checkout = if row.is_multiple_of(20) {
        " checkout"
    } else {
        ""
    };
    let mut event = Event::now(format!(
        "service-{} status {}{queen}{checkout} row-{row:06}",
        row % 20,
        200 + row % 5
    ));
    event.timestamp = base + chrono::Duration::seconds(row as i64);
    event
}

/// One Puffin-indexed file per round (`rebuild: false` gives the same Parquet
/// layout with no index at all — the correctness control), rows in timestamp
/// order across rounds so a window predicate can name one file's range.
async fn bench_shaped_fixture(
    path: &std::path::Path,
    rebuild: bool,
    base: chrono::DateTime<chrono::Utc>,
) -> IcebergContext {
    let ice = IcebergContext::open(path)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(IcebergTuning {
            index_rebuild: Some(rebuild),
            ..Default::default()
        });
    let ident = ice.events_table_ident().clone();
    let blooms = ice.events_bloom_columns();
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    for file in 0..TEXT_SHAPE_FILES {
        let before: Vec<String> = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .iter()
            .map(|data_file| data_file.file_path().to_string())
            .collect();
        for half in 0..2 {
            let start = file * TEXT_SHAPE_ROWS_PER_FILE + half * TEXT_SHAPE_ROWS_PER_FILE / 2;
            let events: Vec<Event> = (start..start + TEXT_SHAPE_ROWS_PER_FILE / 2)
                .map(|row| bench_shaped_event(row, base))
                .collect();
            ice.append_events(&events).await.unwrap();
        }
        // Rewrite only this round's appends, so each round leaves one more
        // indexed file behind instead of merging everything into one.
        let fresh: Vec<DataFile> = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .into_iter()
            .filter(|data_file| !before.contains(&data_file.file_path().to_string()))
            .collect();
        ice.recluster_files_with(
            &ident,
            fresh,
            &bloom_refs,
            &ReclusterMergeOptions {
                force_streaming: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    ice
}

/// What the query server builds for a query with no `ORDER BY`: no
/// `PreferredScanOrder`, and therefore no `OrderedScanLimit` either — `sql.rs`
/// derives the ordered limit from the scan order, so one cannot appear without
/// the other.
fn unordered_text_context() -> SessionContext {
    let ctx = siglake_storage::session_context_with_order(Some(4), None, None);
    ctx.register_udf(ScalarUDF::from(MatchTermsUdf::new()));
    ctx
}

async fn raw_column(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch.schema().index_of("raw").unwrap();
            let raw = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| raw.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    rows
}

/// #3895: the text shapes run #73 measured are bare `LIMIT 100` — no
/// `ORDER BY`, so the session carries neither ordering extension and #3771's
/// ordered-LIMIT decline (`query_provider.rs`) never applies to them. What
/// they must not do, and did until #3896, is deserialize each planned file's
/// whole index again on every execution, a cost proportional to the file's
/// rows and independent of the `LIMIT`.
///
/// Since #4375 the query server declines the index for exactly this shape, so
/// what runs here is the index path WITHOUT that hint — the session sets no
/// `ClippedScanLimit`, which is all it takes. The parse-once property still has
/// to hold: it is what every remaining index-eligible shape depends on, and
/// `clipped_text_limit_shapes_decline_the_whole_file_index` covers the hinted
/// half of the same three shapes.
///
/// The three shapes are the ones in the benchmark's `queries.json`: a
/// `match_terms` keyword, a `LIKE` substring, and a keyword inside a timestamp
/// window. A bare `LIMIT` promises no row order, so every result is compared as
/// a set against the same rewrite without indexes.
#[tokio::test]
async fn unordered_text_limit_shapes_keep_the_index_and_parse_it_once() {
    use chrono::{Duration, TimeZone, Utc};

    // This test attributes a process-wide decode counter to its own queries.
    let _gate = PUFFIN_QUERY_GATE.lock().await;

    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("indexed");
    let indexed = bench_shaped_fixture(&warehouse, true, base).await;
    let control = bench_shaped_fixture(&tmp.path().join("control"), false, base).await;

    let table = indexed
        .catalog()
        .load_table(indexed.events_table_ident())
        .await
        .unwrap();
    let files = indexed
        .live_data_files(indexed.events_table_ident())
        .await
        .unwrap();
    assert_eq!(
        files.len(),
        TEXT_SHAPE_FILES,
        "each round must leave exactly one file, so a decode count is a per-file count"
    );
    for file in &files {
        assert!(
            puffin_indexes_file(&table, file) && !footer_has_raw_index(&table, file).await,
            "the fixture must read its index from Puffin, not from the footer"
        );
    }
    let control_table = control
        .catalog()
        .load_table(control.events_table_ident())
        .await
        .unwrap();
    let control_files = control
        .live_data_files(control.events_table_ident())
        .await
        .unwrap();
    assert!(
        control_files
            .iter()
            .all(|file| !puffin_indexes_file(&control_table, file)),
        "the control arm must be the same rewrite without indexes"
    );
    let layout = |files: &[DataFile]| {
        files
            .iter()
            .map(|file| file.record_count())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        layout(&files),
        layout(&control_files),
        "indexed and control arms must have identical Parquet layout"
    );

    let ctx = unordered_text_context();
    indexed.register_with_datafusion(&ctx).await.unwrap();
    let control_ctx = unordered_text_context();
    control
        .register_with_datafusion(&control_ctx)
        .await
        .unwrap();
    let state = ctx.state();
    assert!(
        state
            .config()
            .get_extension::<siglake_storage::PreferredScanOrder>()
            .is_none()
            && state
                .config()
                .get_extension::<siglake_storage::OrderedScanLimit>()
                .is_none(),
        "the measured shapes have no ORDER BY: neither ordering extension is set"
    );

    // The window names the middle file's range exactly.
    let window_start = (base + Duration::seconds(TEXT_SHAPE_ROWS_PER_FILE as i64)).to_rfc3339();
    let window_end = (base + Duration::seconds(2 * TEXT_SHAPE_ROWS_PER_FILE as i64)).to_rfc3339();
    let keyword = "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') LIMIT 100";
    // Third element: how many of the fixture's files the shape plans, which is
    // how many index lookups one execution owes.
    let shapes = [
        ("keyword", keyword.to_string(), TEXT_SHAPE_FILES as u64),
        (
            "substring_scan",
            "SELECT timestamp, raw FROM events WHERE raw LIKE '%checkout%' LIMIT 100".to_string(),
            TEXT_SHAPE_FILES as u64,
        ),
        (
            "keyword_window",
            format!(
                "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') \
                 AND timestamp >= TIMESTAMP '{window_start}' \
                 AND timestamp < TIMESTAMP '{window_end}' LIMIT 100"
            ),
            1,
        ),
    ];

    // Statistics paths under this warehouse only: other tests in this binary
    // query their own warehouses.
    let warehouse = warehouse.to_string_lossy().to_string();
    let cache_stats = || iceberg::arrow::parsed_inverted_index_cache_stats(&warehouse);
    let decodes = || iceberg::arrow::inverted_index_decode_counts().0;
    assert_eq!(
        cache_stats(),
        (0, 0),
        "nothing cached before the first query"
    );

    for (position, (name, sql, planned_files)) in shapes.iter().enumerate() {
        let expected = raw_column(&control_ctx, sql).await;
        assert!(
            !expected.is_empty() && expected.len() < 100,
            "{name}: the fixture must return a whole, unclipped match set ({} rows)",
            expected.len()
        );

        let before_decodes = decodes();
        let (_, before_lookups) = cache_stats();
        let cold = raw_column(&ctx, sql).await;
        let cold_decodes = decodes() - before_decodes;
        let (entries, cold_lookups) = cache_stats();
        let warm = raw_column(&ctx, sql).await;
        let warm_decodes = decodes() - before_decodes - cold_decodes;
        let (warm_entries, warm_lookups) = cache_stats();

        assert_eq!(
            cold, expected,
            "{name}: cold result diverged from the control"
        );
        assert_eq!(
            warm, expected,
            "{name}: warm result diverged from the control"
        );
        assert_eq!(
            warm_lookups - cold_lookups,
            *planned_files,
            "{name}: the repeat must select its rows from the parsed index of \
             each planned file, once per file"
        );
        assert_eq!(
            warm_decodes, 0,
            "{name}: repeating the shape must not deserialize an index again"
        );
        assert_eq!(
            warm_entries, entries,
            "{name}: the warm execution must reuse the cached indexes"
        );
        if position == 0 {
            // Nothing is cached yet, so this one execution pays the whole-index
            // load the round measured — once per file, not once per query.
            assert_eq!(
                cold_decodes, *planned_files,
                "{name}: the first shape parses each planned file's index exactly once"
            );
            assert_eq!(
                cold_lookups, before_lookups,
                "{name}: a decode is not a cache hit"
            );
            assert_eq!(
                entries, TEXT_SHAPE_FILES,
                "{name}: one cached index per file"
            );
        } else {
            assert_eq!(
                cold_decodes, 0,
                "{name}: a different shape over the same files must reuse their parsed indexes"
            );
            assert_eq!(
                cold_lookups - before_lookups,
                *planned_files,
                "{name}: it must be served from the cache, once per planned file"
            );
        }
    }

    // A `LIMIT` below the match count returns some of the matches in no
    // promised order; what it owes is the count and membership.
    let clipped = raw_column(
        &ctx,
        "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') LIMIT 10",
    )
    .await;
    let matches: std::collections::BTreeSet<String> = raw_column(&control_ctx, keyword)
        .await
        .into_iter()
        .collect();
    assert_eq!(
        clipped.len(),
        10,
        "a clipped LIMIT returns exactly its rows"
    );
    assert!(
        clipped.iter().all(|row| matches.contains(row)),
        "every clipped row must be one of the control's matches"
    );
}

/// #4375: the same three shapes with the session hint the query server now
/// carries for them. A literal `LIMIT` over a plain select clips the scan row
/// for row, so the scan owes a sliver of the first file and must not pay a
/// whole file's index decode for it — #4329 measured that cost at 20.3 ms
/// against 6.1 ms scanned for `keyword`, and 813.1 ms against 4.5 ms for
/// `substring_scan`, with every index already resident.
///
/// What this owes beyond the counters: the same rows. The decline changes
/// which rows the reader DECODES, never which rows the query returns, so every
/// shape is compared against the unindexed control, and the shape with no
/// `LIMIT` is run in the same process to show the index is declined per
/// EXECUTION rather than switched off for the table.
#[tokio::test]
async fn clipped_text_limit_shapes_decline_the_whole_file_index() {
    use chrono::{Duration, TimeZone, Utc};

    // This test attributes process-wide decode counters to its own queries.
    let _gate = PUFFIN_QUERY_GATE.lock().await;

    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("indexed");
    let indexed = bench_shaped_fixture(&warehouse, true, base).await;
    let control = bench_shaped_fixture(&tmp.path().join("control"), false, base).await;

    let table = indexed
        .catalog()
        .load_table(indexed.events_table_ident())
        .await
        .unwrap();
    let files = indexed
        .live_data_files(indexed.events_table_ident())
        .await
        .unwrap();
    assert_eq!(files.len(), TEXT_SHAPE_FILES);
    assert!(
        files.iter().all(|file| puffin_indexes_file(&table, file)),
        "the decline must be measured against a table that HAS indexes"
    );

    // One warehouse, two sessions: the hint is per request, not per table.
    let clipped_ctx = clipped_text_context(100);
    indexed
        .register_with_datafusion(&clipped_ctx)
        .await
        .unwrap();
    let unclipped_ctx = unordered_text_context();
    indexed
        .register_with_datafusion(&unclipped_ctx)
        .await
        .unwrap();
    let control_ctx = unordered_text_context();
    control
        .register_with_datafusion(&control_ctx)
        .await
        .unwrap();

    let plan_of = |ctx: &SessionContext, sql: &str| {
        let ctx = ctx.clone();
        let sql = sql.to_string();
        async move {
            let plan = ctx.sql(&sql).await.unwrap().create_physical_plan().await;
            format!(
                "{}",
                datafusion::physical_plan::displayable(plan.unwrap().as_ref()).indent(true)
            )
        }
    };

    let window_start = (base + Duration::seconds(TEXT_SHAPE_ROWS_PER_FILE as i64)).to_rfc3339();
    let window_end = (base + Duration::seconds(2 * TEXT_SHAPE_ROWS_PER_FILE as i64)).to_rfc3339();
    let shapes = [
        (
            "keyword",
            "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') LIMIT 100"
                .to_string(),
        ),
        (
            "substring_scan",
            "SELECT timestamp, raw FROM events WHERE raw LIKE '%checkout%' LIMIT 100".to_string(),
        ),
        (
            "keyword_window",
            format!(
                "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') \
                 AND timestamp >= TIMESTAMP '{window_start}' \
                 AND timestamp < TIMESTAMP '{window_end}' LIMIT 100"
            ),
        ),
    ];

    // Statistics scoped to this warehouse; the control arm has no index to
    // parse and the other tests in this binary query their own warehouses.
    let warehouse = warehouse.to_string_lossy().to_string();
    let cache_stats = || iceberg::arrow::parsed_inverted_index_cache_stats(&warehouse);
    let decodes = || iceberg::arrow::inverted_index_decode_counts().0;
    assert_eq!(
        cache_stats(),
        (0, 0),
        "nothing parsed before the first query"
    );

    for (name, sql) in &shapes {
        let plan = plan_of(&clipped_ctx, sql).await;
        assert!(
            plan.contains("text_index:[declined:clipped_limit]"),
            "{name}: the plan must say which path it took:\n{plan}"
        );

        let before_decodes = decodes();
        let rows = raw_column(&clipped_ctx, sql).await;
        assert_eq!(
            rows,
            raw_column(&control_ctx, sql).await,
            "{name}: declining the index changed the result"
        );
        assert_eq!(
            decodes() - before_decodes,
            0,
            "{name}: a declined shape must not deserialize an index"
        );
        assert_eq!(
            cache_stats(),
            (0, 0),
            "{name}: and must not look one up either"
        );
    }

    // The regime the index is FOR, in the same process against the same files:
    // no `LIMIT`, so nothing clips the scan and the hint is absent.
    let unclipped = "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen')";
    let plan = plan_of(&unclipped_ctx, unclipped).await;
    assert!(
        plan.contains("text_index:[allowed]"),
        "an unclipped text scan keeps the index:\n{plan}"
    );
    let before_decodes = decodes();
    assert_eq!(
        raw_column(&unclipped_ctx, unclipped).await,
        raw_column(&control_ctx, unclipped).await,
        "the index path must agree with the control"
    );
    assert_eq!(
        decodes() - before_decodes,
        TEXT_SHAPE_FILES as u64,
        "the unclipped scan selects its rows from each planned file's index"
    );
}

/// The opt-out spelled out. Since #4162 this is also what the shipped default
/// does; the test keeps the explicit tuning so it says which behaviour it
/// covers regardless of where the default sits.
#[tokio::test]
async fn explicit_rebuild_opt_out_leaves_streaming_outputs_queryable() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_rebuild: Some(false),
            ..Default::default()
        });
    ice.append_events(&[Event::now("database timeout"), Event::now("healthy")])
        .await
        .unwrap();
    ice.append_events(&[Event::now("database retry"), Event::now("steady")])
        .await
        .unwrap();

    let ident = ice.events_table_ident().clone();
    let before = ice.live_data_files(&ident).await.unwrap();
    let bloom = ice.events_bloom_columns();
    let bloom_refs: Vec<&str> = bloom.iter().map(String::as_str).collect();
    ice.recluster_files_with(
        &ident,
        before,
        &bloom_refs,
        &siglake_storage::iceberg::ReclusterMergeOptions {
            force_streaming: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let after = ice.live_data_files(&ident).await.unwrap();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    for file in &after {
        assert!(!footer_has_raw_index(&table, file).await);
        assert!(!puffin_indexes_file(&table, file));
    }
    assert_eq!(
        ice.rebuild_inverted_indexes_for_files(&ident, &after)
            .await
            .unwrap(),
        0
    );

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    assert_eq!(count(&ctx, "SELECT count(*) FROM events").await, 4);
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) FROM events WHERE raw LIKE '%database%'",
        )
        .await,
        2
    );
}

/// The shipped default (#4162, 2026-09-14): with nothing configured, a
/// streaming rewrite leaves its output unindexed — and that is the whole of
/// what the default changes. The flush path still writes footer indexes, no
/// index that already exists is dropped, and a text query still selects its
/// rows from the indexes the table has.
///
/// `index_rebuild_defaults_off_and_only_literal_one_enables_it` (in
/// `iceberg.rs`) pins the resolver; this pins what an unconfigured context
/// does to a table.
#[tokio::test]
async fn the_default_leaves_a_rewrite_unindexed_and_still_reads_existing_indexes() {
    use chrono::{Duration, TimeZone, Utc};

    // Attributes this warehouse's index-cache statistics to its own queries.
    let _gate = PUFFIN_QUERY_GATE.lock().await;
    assert!(
        std::env::var("SIGLAKE_INDEX_REBUILD").is_err(),
        "this test reads the shipped default out of an unset environment"
    );

    let base = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    // No `index_rebuild` in the tuning: the default is what is under test.
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO);

    let ident = ice.events_table_ident().clone();
    let mut needles: Vec<String> = Vec::new();
    for batch in 0..4usize {
        let events: Vec<Event> = (0..100usize)
            .map(|row| {
                let ordinal = batch * 100 + row;
                let needle = if ordinal.is_multiple_of(25) {
                    " rareneedle"
                } else {
                    ""
                };
                let mut event = Event::now(format!(
                    "service-{} status 200{needle} row-{ordinal:04}",
                    ordinal % 7
                ));
                event.timestamp = base + Duration::seconds(ordinal as i64);
                event
            })
            .collect();
        needles.extend(
            events
                .iter()
                .filter(|event| event.raw.contains("rareneedle"))
                .map(|event| event.raw.clone()),
        );
        ice.append_events(&events).await.unwrap();
    }
    needles.sort();

    let appended = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(appended.len(), 4, "one file per append");
    let table = ice.catalog().load_table(&ident).await.unwrap();
    for file in &appended {
        assert!(
            footer_has_raw_index(&table, file).await,
            "the flush path's footer index is not what this default touches"
        );
    }

    // Rewrite half of them, streaming — the arm that has no footer index of
    // its own and would need a Puffin sidecar.
    let bloom = ice.events_bloom_columns();
    let bloom_refs: Vec<&str> = bloom.iter().map(String::as_str).collect();
    let inputs: Vec<DataFile> = appended.iter().take(2).cloned().collect();
    ice.recluster_files_with(
        &ident,
        inputs,
        &bloom_refs,
        &ReclusterMergeOptions {
            force_streaming: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let after = ice.live_data_files(&ident).await.unwrap();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let mut rewritten = 0usize;
    let mut retained = 0usize;
    for file in &after {
        if appended
            .iter()
            .any(|before| before.file_path() == file.file_path())
        {
            assert!(
                footer_has_raw_index(&table, file).await,
                "a file the rewrite did not touch keeps the index it was written with"
            );
            retained += 1;
        } else {
            assert!(
                !footer_has_raw_index(&table, file).await && !puffin_indexes_file(&table, file),
                "the default must leave a streaming rewrite's output unindexed"
            );
            rewritten += 1;
        }
    }
    assert_eq!(retained, 2, "the two untouched files are still there");
    assert!(rewritten >= 1, "the rewrite produced at least one file");
    assert_eq!(
        table.metadata().statistics_iter().count(),
        0,
        "the default registers no Puffin statistics file at all"
    );

    // What the table still has, it still uses.
    let warehouse = warehouse.to_string_lossy().to_string();
    let cache_stats = || iceberg::arrow::parsed_inverted_index_cache_stats(&warehouse);
    assert_eq!(
        cache_stats(),
        (0, 0),
        "nothing cached before the first query"
    );
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    assert_eq!(
        count(&ctx, "SELECT count(*) FROM events").await,
        400,
        "the rewrite is row-preserving"
    );
    assert_eq!(
        text_rows(&ctx, "rareneedle").await,
        needles,
        "exact rows over a part-indexed, part-scanned table"
    );
    let (entries, cold_lookups) = cache_stats();
    assert_eq!(
        entries, retained,
        "one parsed index per file that still carries one — the rewritten files \
         contribute none, and the query reads the rest"
    );
    // A decode is not a cache hit, so the repeat is what shows the retained
    // indexes being read: one lookup per file that has one.
    assert_eq!(text_rows(&ctx, "rareneedle").await, needles);
    let (warm_entries, warm_lookups) = cache_stats();
    assert_eq!(warm_entries, entries, "the repeat decoded nothing new");
    assert_eq!(
        warm_lookups - cold_lookups,
        retained as u64,
        "the retained indexes served row selection"
    );
}

/// The measurement corpus: [`bench_shaped_event`]'s text on a timestamp that
/// keeps each file inside ONE day partition. The events table is partitioned
/// `day(timestamp)`, and a rewrite stamps its output with a single partition
/// value, so a fixture that merged a multi-day round into one file would hide
/// every row outside that day from a partition-pruned window predicate. File
/// `i` therefore fills day `i`, uniformly, which is also the shape the
/// benchmark's `window_fraction` assumes: a time fraction is a row fraction.
///
/// `rare_every` adds `AB_RARE_TERM` to one row in that many. The benchmark's
/// own terms are dense — `queen` is on 2% of rows here and ~3% of the
/// benchmark harness's own corpus — and every gate shape clips at
/// `LIMIT 100`, so nothing in the corpus
/// exercised the regime an index is built for: a term so sparse that a scan
/// has to read the whole file to find it (#4329).
fn ab_shaped_event(
    row: usize,
    rows_per_file: usize,
    base: chrono::DateTime<chrono::Utc>,
    rare_every: usize,
) -> Event {
    const DAY_US: i64 = 86_400_000_000;

    let mut event = bench_shaped_event(row, base);
    if rare_every > 0 && row.is_multiple_of(rare_every) {
        // Before the ordinal, so `row-NNNNNN` stays the last token.
        let ordinal = event.raw.rfind(" row-").expect("bench text ends in row-N");
        event.raw.insert_str(ordinal, &format!(" {AB_RARE_TERM}"));
    }
    let file = (row / rows_per_file) as i64;
    let within = (row % rows_per_file) as i64;
    event.timestamp = base
        + chrono::Duration::days(file)
        + chrono::Duration::microseconds(within * (DAY_US / rows_per_file as i64));
    event
}

/// The sparse term of the rare-term arm. Not a substring of any other token in
/// the corpus, so `LIKE '%…%'` and `match_terms` select exactly the rows the
/// generator marked.
const AB_RARE_TERM: &str = "rareneedle";

/// One round per file, appended in bounded chunks so a multi-million-row arm
/// does not hold its corpus in memory, then a streaming rewrite of exactly
/// that round's output. `rebuild` is the only difference between the two arms
/// of the measurement below.
async fn sized_bench_fixture(
    path: &std::path::Path,
    rebuild: bool,
    base: chrono::DateTime<chrono::Utc>,
    files: usize,
    rows_per_file: usize,
    rare_every: usize,
) -> IcebergContext {
    const APPEND_CHUNK: usize = 250_000;

    let ice = open_sized_bench_fixture(path, rebuild).await;
    let ident = ice.events_table_ident().clone();
    let blooms = ice.events_bloom_columns();
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    for file in 0..files {
        let before: Vec<String> = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .iter()
            .map(|data_file| data_file.file_path().to_string())
            .collect();
        let start = file * rows_per_file;
        for chunk in (start..start + rows_per_file).step_by(APPEND_CHUNK) {
            let events: Vec<Event> = (chunk..(chunk + APPEND_CHUNK).min(start + rows_per_file))
                .map(|row| ab_shaped_event(row, rows_per_file, base, rare_every))
                .collect();
            ice.append_events(&events).await.unwrap();
        }
        let fresh: Vec<DataFile> = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .into_iter()
            .filter(|data_file| !before.contains(&data_file.file_path().to_string()))
            .collect();
        ice.recluster_files_with(
            &ident,
            fresh,
            &bloom_refs,
            &ReclusterMergeOptions {
                force_streaming: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    ice
}

/// Open one arm with the same read settings whether this process just built it
/// or is re-timing a completed large fixture after an interrupted cache pass.
async fn open_sized_bench_fixture(path: &std::path::Path, rebuild: bool) -> IcebergContext {
    IcebergContext::open(path)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(IcebergTuning {
            index_rebuild: Some(rebuild),
            ..Default::default()
        })
}

fn median(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[sorted.len() / 2]
}

fn ab_reuse_dir_from(raw: Option<&std::ffi::OsStr>) -> Option<std::path::PathBuf> {
    raw.filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
}

#[test]
fn ab_reuse_dir_resolves_without_process_environment() {
    use std::ffi::OsStr;

    assert_eq!(ab_reuse_dir_from(None), None);
    assert_eq!(ab_reuse_dir_from(Some(OsStr::new(""))), None);
    assert_eq!(
        ab_reuse_dir_from(Some(OsStr::new("/measurement/fixture"))),
        Some(std::path::PathBuf::from("/measurement/fixture"))
    );
}

/// One shape of the on/off comparison below.
struct AbShape {
    name: &'static str,
    sql: String,
    /// The term every returned row must carry.
    term: &'static str,
    /// The lowest row ordinal the shape's window admits.
    window_floor: i64,
    /// How many corpus rows the predicate matches, from the generator's
    /// periods — the shape's selectivity, independent of any `LIMIT`.
    corpus_matches: i64,
    /// Whole result sets are compared row for row. A bare `LIMIT 100` over
    /// thousands of matches promises no order, so those are compared on count
    /// and membership instead.
    exact: bool,
    /// The shape's literal `LIMIT`, when it has one that clips the scan — so
    /// the `policy` arm can set the same `ClippedScanLimit` the query server
    /// derives from the statement (`sql.rs::clipping_scan_limit`).
    clip: Option<usize>,
}

/// #4162's local on/off comparison, extended by #4329 with the rare-term
/// regime: the four text shapes the 50G gate fails (`keyword`,
/// `keyword_last25`, `keyword_last5`, `substring_scan`, verbatim from the
/// benchmark's `queries-events.json`) plus two unclipped scans for a term on
/// one row in `SIGLAKE_REBUILD_AB_RARE_EVERY`. Measured on freshly written
/// files with the post-rewrite Puffin rebuild ON and OFF. Both arms write the
/// same corpus through the same streaming rewrite and must end with the same
/// Parquet layout; the only difference is whether the outputs carry sidecars,
/// which is what disabling the rebuild on an already-indexed table would NOT
/// have shown.
///
/// #4375 adds a third arm, `policy`: the indexed warehouse queried the way the
/// query server now queries it — `ClippedScanLimit` set for a shape whose
/// literal `LIMIT` clips the scan, absent for one nothing clips. Two arms
/// could only ever say whether the index helps; three say whether the rule
/// that decides per execution picks the better one, which is the claim. Its
/// `rare_keyword` shape is there to price the rule's one known loss: a
/// clipped query over a term rare enough that the index would have won.
///
/// Why the two regimes belong in one run: the gate shapes stop at 100 rows, so
/// the scan they race against reads a sliver of the file and the index's
/// whole-file decode can only lose. The rare-term shapes have no `LIMIT` and
/// no scan shortcut, which is the regime the index is built for. The boundary
/// between them is the number this reports, and it is the input to the 0.2.0
/// redesign rather than a defaults change.
///
///   cargo test -p siglake-storage --release --test puffin_rebuild \
///     report_rebuild_on_off_text_shapes -- --ignored --nocapture
///
/// Knobs, read from the environment (nothing here sets one):
///   SIGLAKE_REBUILD_AB_FILES          files per arm (default 4)
///   SIGLAKE_REBUILD_AB_ROWS_PER_FILE  rows per file (default 1,000,000)
///   SIGLAKE_REBUILD_AB_RUNS           executions per shape per arm (default 9)
///   SIGLAKE_REBUILD_AB_RARE_EVERY     one rare-term row in this many (100,000)
///   SIGLAKE_REBUILD_AB_PARSED_BYTES   parsed-index cache budget, bytes
///   SIGLAKE_REBUILD_AB_BLOB_BYTES     Puffin blob cache budget, bytes
///   SIGLAKE_REBUILD_AB_PASSES         re-time the ON arm at further budgets:
///                                     `name=parsed:blob`, comma-separated
///   SIGLAKE_REBUILD_AB_REUSE_DIR      completed fixture root with off/ and on/
///
/// The cache knobs are how a local corpus reproduces the deployed
/// working-set-to-cache ratio. Left unset, the caches keep whatever the process
/// defaults to and the arms differ only in decode work, not in eviction.
///
/// `_PASSES` is what separates the two costs the capped ON arm adds together:
/// re-decoding an evicted index, and using one. Each extra pass re-times the
/// same indexed arm at its own budget, so
/// `resident=12884901888:2147483648,nocache=0:0` brackets the deployed range —
/// the whole parsed working set held, and the packaged 4Gi pod, which derives
/// no parsed cache at all (`derive_text_index_cache_bytes` returns `(0, 0)`
/// once the pool's first-file decode reservation has taken the remainder).
/// Each table prints as its pass finishes, so a later pass that cannot fit its
/// budget on the box does not cost the earlier ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn report_rebuild_on_off_text_shapes() {
    use chrono::{Duration, TimeZone, Utc};
    use std::time::Instant;

    // A measurement wants the box to itself as much as the counter tests do.
    let _gate = PUFFIN_QUERY_GATE.lock().await;

    let knob = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .unwrap_or(default)
    };
    let files = knob("SIGLAKE_REBUILD_AB_FILES", 4);
    let rows_per_file = knob("SIGLAKE_REBUILD_AB_ROWS_PER_FILE", 1_000_000);
    let runs = knob("SIGLAKE_REBUILD_AB_RUNS", 9);
    let rare_every = knob("SIGLAKE_REBUILD_AB_RARE_EVERY", 100_000).max(1);
    let extra_passes: Vec<(String, u64, u64)> = std::env::var("SIGLAKE_REBUILD_AB_PASSES")
        .unwrap_or_default()
        .split(',')
        .filter(|spec| !spec.trim().is_empty())
        .map(|spec| {
            let (name, budgets) = spec
                .trim()
                .split_once('=')
                .unwrap_or_else(|| panic!("pass {spec:?} is not name=parsed:blob"));
            let (parsed, blob) = budgets
                .split_once(':')
                .unwrap_or_else(|| panic!("pass {spec:?} is not name=parsed:blob"));
            let bytes = |raw: &str| {
                raw.trim()
                    .parse::<u64>()
                    .unwrap_or_else(|_| panic!("pass {spec:?} has an unparsable budget"))
            };
            (name.trim().to_string(), bytes(parsed), bytes(blob))
        })
        .collect();
    let parsed_bytes = std::env::var("SIGLAKE_REBUILD_AB_PARSED_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok());
    let blob_bytes = std::env::var("SIGLAKE_REBUILD_AB_BLOB_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok());
    let reuse_dir = ab_reuse_dir_from(std::env::var_os("SIGLAKE_REBUILD_AB_REUSE_DIR").as_deref());
    if let (Some(parsed), Some(blob)) = (parsed_bytes, blob_bytes) {
        siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
            parsed_index_max_bytes: parsed as u64,
            puffin_blob_max_bytes: blob as u64,
        });
    }

    // Midnight UTC: the corpus is one day per file (see `ab_shaped_event`).
    let base = Utc.timestamp_opt(1_700_006_400, 0).unwrap();
    const DAY_US: i64 = 86_400_000_000;
    let span_us = files as i64 * DAY_US;
    let corpus_rows = (files * rows_per_file) as i64;
    let low_us = |fraction: f64| span_us - (span_us as f64 * fraction) as i64;
    let window = |fraction: f64| {
        let lo = base + Duration::microseconds(low_us(fraction));
        let hi = base + Duration::microseconds(span_us);
        (lo.to_rfc3339(), hi.to_rfc3339())
    };
    let keyword = "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') LIMIT 100";
    let windowed = |fraction: f64| {
        let (lo, hi) = window(fraction);
        format!(
            "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') \
             AND timestamp >= TIMESTAMP '{lo}' AND timestamp < TIMESTAMP '{hi}' LIMIT 100"
        )
    };
    let floor = |fraction: f64| corpus_rows - (corpus_rows as f64 * fraction) as i64;
    // `bench_shaped_event` marks one row in 50 with `queen` and one in 20 with
    // `checkout`; `ab_shaped_event` marks one in `rare_every`. How many of those
    // a window admits is counted against the generator's own timestamp
    // arithmetic rather than a fraction of the total: the per-row step is an
    // integer division, so a fraction is off by tens of rows and could not
    // carry an equality.
    let offset_us = |row: i64| {
        (row / rows_per_file as i64) * DAY_US
            + (row % rows_per_file as i64) * (DAY_US / rows_per_file as i64)
    };
    let matches = |every: usize, fraction: f64| {
        let lo = low_us(fraction);
        (0..corpus_rows)
            .step_by(every)
            .filter(|row| offset_us(*row) >= lo)
            .count() as i64
    };
    let rare = |fraction: f64| {
        let predicate = format!("match_terms(raw, '{AB_RARE_TERM}')");
        if fraction >= 1.0 {
            format!("SELECT timestamp, raw FROM events WHERE {predicate}")
        } else {
            let (lo, hi) = window(fraction);
            format!(
                "SELECT timestamp, raw FROM events WHERE {predicate} \
                 AND timestamp >= TIMESTAMP '{lo}' AND timestamp < TIMESTAMP '{hi}'"
            )
        }
    };
    let shapes = [
        AbShape {
            name: "keyword",
            sql: keyword.to_string(),
            term: "queen",
            window_floor: 0,
            corpus_matches: matches(50, 1.0),
            exact: false,
            clip: Some(100),
        },
        AbShape {
            name: "keyword_last25",
            sql: windowed(0.25),
            term: "queen",
            window_floor: floor(0.25),
            corpus_matches: matches(50, 0.25),
            exact: false,
            clip: Some(100),
        },
        AbShape {
            name: "keyword_last5",
            sql: windowed(0.05),
            term: "queen",
            window_floor: floor(0.05),
            corpus_matches: matches(50, 0.05),
            exact: false,
            clip: Some(100),
        },
        AbShape {
            name: "substring_scan",
            sql: "SELECT timestamp, raw FROM events WHERE raw LIKE '%checkout%' LIMIT 100"
                .to_string(),
            term: "checkout",
            window_floor: 0,
            corpus_matches: matches(20, 1.0),
            exact: false,
            clip: Some(100),
        },
        // The rare-term regime (#4329): no `LIMIT`, so the scan arm reads every
        // row of every file and the index arm reads the rows it selected.
        AbShape {
            name: "rare_scan",
            sql: rare(1.0),
            term: AB_RARE_TERM,
            window_floor: 0,
            corpus_matches: matches(rare_every, 1.0),
            exact: true,
            clip: None,
        },
        AbShape {
            name: "rare_scan_last25",
            sql: rare(0.25),
            term: AB_RARE_TERM,
            window_floor: floor(0.25),
            corpus_matches: matches(rare_every, 0.25),
            exact: true,
            clip: None,
        },
        // What the conservative decline COSTS, stated rather than left to be
        // discovered: the one shape where a clipped query would have wanted
        // the index. The scan has to read until it finds 100 matches of a
        // 0.001%-density term, which is most of the corpus.
        AbShape {
            name: "rare_keyword",
            sql: format!(
                "SELECT timestamp, raw FROM events WHERE \
                 match_terms(raw, '{AB_RARE_TERM}') LIMIT 100"
            ),
            term: AB_RARE_TERM,
            window_floor: 0,
            corpus_matches: matches(rare_every, 1.0),
            exact: false,
            clip: Some(100),
        },
    ];

    let tmp = reuse_dir.is_none().then(|| tempfile::tempdir().unwrap());
    let fixture_root = reuse_dir
        .as_deref()
        .unwrap_or_else(|| tmp.as_ref().unwrap().path());
    let mut contexts = Vec::new();
    for (label, rebuild) in [("off", false), ("on", true)] {
        let warehouse = fixture_root.join(label);
        let build = Instant::now();
        let ice = if reuse_dir.is_some() {
            open_sized_bench_fixture(&warehouse, rebuild).await
        } else {
            sized_bench_fixture(&warehouse, rebuild, base, files, rows_per_file, rare_every).await
        };
        let build_s = build.elapsed().as_secs_f64();
        let ident = ice.events_table_ident().clone();
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let data_files = ice.live_data_files(&ident).await.unwrap();
        let indexed = data_files
            .iter()
            .filter(|file| puffin_indexes_file(&table, file))
            .count();
        let file_rows: Vec<u64> = data_files.iter().map(|file| file.record_count()).collect();
        let bytes: u64 = data_files
            .iter()
            .map(|file| file.file_size_in_bytes())
            .sum();
        let puffin_bytes: i64 = table
            .metadata()
            .statistics_iter()
            .map(|stats| stats.file_size_in_bytes)
            .sum();
        println!(
            "arm={label} rebuild={rebuild} build_s={build_s:.1} files={} indexed_files={indexed} \
             parquet_bytes={bytes} puffin_bytes={puffin_bytes} file_rows={file_rows:?}",
            data_files.len()
        );
        if rebuild {
            assert_eq!(
                indexed,
                data_files.len(),
                "the ON arm must carry a sidecar per file"
            );
        } else {
            assert_eq!(indexed, 0, "the OFF arm must carry no sidecar at all");
        }
        let ctx = unordered_text_context();
        ice.register_with_datafusion(&ctx).await.unwrap();
        let path = warehouse.to_string_lossy().to_string();
        // #4375's arm: the SAME indexed warehouse, queried through the session
        // the query server now builds for a clipped statement. It is a third
        // ARM rather than a replacement for `on` because the question is what
        // the policy is worth against the index it declines, and `off` alone
        // cannot answer that — it has no index to decline.
        let policy = if rebuild {
            let clipped = clipped_text_context(100);
            ice.register_with_datafusion(&clipped).await.unwrap();
            Some(("policy", clipped, file_rows.clone(), path.clone()))
        } else {
            None
        };
        contexts.push((label, ctx, file_rows, path));
        contexts.extend(policy);
    }
    assert_eq!(
        contexts[0].2, contexts[1].2,
        "the arms must have identical Parquet layout, or the shapes are not comparable"
    );

    let mut samples: std::collections::BTreeMap<(&str, &str), Vec<f64>> = Default::default();
    let mut decodes: std::collections::BTreeMap<(&str, &str), (u64, u64)> = Default::default();
    let mut results: std::collections::BTreeMap<(&str, &str), Vec<String>> = Default::default();

    // Interleave the arms per execution: a drift on this box hits both.
    for run in 0..runs {
        for shape in &shapes {
            for (label, ctx, _, _) in &contexts {
                // The policy arm decides per EXECUTION, which is the whole
                // claim: a shape nothing clips runs in the unhinted session
                // and keeps the index it would have used.
                let ctx = match (*label, shape.clip) {
                    ("policy", None) => &contexts[1].1,
                    _ => ctx,
                };
                let (elapsed, found, delta) = time_ab_shape(shape, label, ctx, run == 0).await;
                samples
                    .entry((shape.name, label))
                    .or_default()
                    .push(elapsed);
                let counted = decodes.entry((shape.name, label)).or_default();
                counted.0 += delta.0;
                counted.1 += delta.1;
                if run == 0 {
                    results.insert((shape.name, label), found);
                }
            }
        }
    }

    for shape in &shapes {
        let name = shape.name;
        let off = &results[&(name, "off")];
        for arm in ["on", "policy"] {
            let found = &results[&(name, arm)];
            assert_eq!(
                off.len(),
                found.len(),
                "{name}/{arm}: the arms must return the same number of rows"
            );
            if shape.exact {
                // `raw_column` sorts, so an unclipped shape compares row for row.
                assert_eq!(
                    off, found,
                    "{name}/{arm}: the arms must return the same rows"
                );
            }
        }
    }

    // `policy` shares the indexed warehouse with `on`, so it has no cache
    // statistics of its own; its decodes are attributed per execution below.
    for (label, _, _, warehouse) in contexts.iter().filter(|(label, ..)| *label != "policy") {
        let (entries, lookups) = iceberg::arrow::parsed_inverted_index_cache_stats(warehouse);
        println!("arm={label} cached_indexes={entries} cache_lookups={lookups}");
    }
    let footprint = iceberg::arrow::parsed_inverted_index_cache_footprint();
    println!(
        "parsed_cache entries={} bytes={} evictions={} oversized_skips={}",
        footprint.entries, footprint.bytes, footprint.evictions, footprint.oversized_skips
    );
    // One row per (shape, arm) since #4375 added the third arm: a fixed
    // column per arm stops being readable at three and would have to change
    // again at four.
    println!(
        "shape,arm,clip,corpus_matches,selectivity,rows_returned,cold_ms,p50_ms,over_off,\
         decodes,cache_hits,all_ms"
    );
    let format_all = |values: &[f64]| {
        values
            .iter()
            .map(|ms| format!("{ms:.1}"))
            .collect::<Vec<_>>()
    };
    for shape in &shapes {
        let name = shape.name;
        // The first execution of each arm is its cold one; the p50 is over the
        // rest, which is what the benchmark's repeated iterations report.
        let off_p50 = median(&samples[&(name, "off")][1..]);
        for arm in ["off", "on", "policy"] {
            let arm_samples = &samples[&(name, arm)];
            let p50 = median(&arm_samples[1..]);
            let (arm_decodes, arm_hits) = decodes[&(name, arm)];
            println!(
                "{name},{arm},{},{},{:.6},{},{:.1},{p50:.1},{:.2}x,{arm_decodes},{arm_hits},{:?}",
                shape
                    .clip
                    .map_or_else(|| "none".to_string(), |n| n.to_string()),
                shape.corpus_matches,
                shape.corpus_matches as f64 / corpus_rows as f64,
                results[&(name, arm)].len(),
                arm_samples[0],
                p50 / off_p50,
                format_all(arm_samples),
            );
        }
    }
    // Further passes over the SAME indexed arm at other cache budgets. Each one
    // prints as it finishes: the last is the largest, and a box that cannot
    // hold it must not cost the reader the passes that already ran.
    for (pass_name, parsed, blob) in &extra_passes {
        siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
            parsed_index_max_bytes: *parsed,
            puffin_blob_max_bytes: *blob,
        });
        let (label, ctx, _, _) = &contexts[1];
        assert_eq!(*label, "on", "an extra pass re-times the indexed arm");
        let mut pass_samples: std::collections::BTreeMap<&str, Vec<f64>> = Default::default();
        let mut pass_decodes: std::collections::BTreeMap<&str, (u64, u64)> = Default::default();
        for _ in 0..runs {
            for shape in &shapes {
                let (elapsed, _, delta) = time_ab_shape(shape, pass_name, ctx, false).await;
                pass_samples.entry(shape.name).or_default().push(elapsed);
                let counted = pass_decodes.entry(shape.name).or_default();
                counted.0 += delta.0;
                counted.1 += delta.1;
            }
        }
        println!("pass={pass_name} parsed_bytes={parsed} blob_bytes={blob}");
        println!("shape,cold_ms,p50_ms,over_off,decodes,cache_hits,all_ms");
        for shape in &shapes {
            let name = shape.name;
            let pass = &pass_samples[name];
            let p50 = median(&pass[1..]);
            let off_p50 = median(&samples[&(name, "off")][1..]);
            let (decoded, hits) = pass_decodes[name];
            println!(
                "{name},{:.1},{p50:.1},{:.2}x,{decoded},{hits},{:?}",
                pass[0],
                p50 / off_p50,
                format_all(pass),
            );
        }
        let footprint = iceberg::arrow::parsed_inverted_index_cache_footprint();
        println!(
            "parsed_cache_{pass_name} entries={} bytes={} evictions={} oversized_skips={}",
            footprint.entries, footprint.bytes, footprint.evictions, footprint.oversized_skips
        );
    }

    // Leave the process's cache budgets as they were found.
    iceberg::arrow::clear_text_index_cache_max_bytes();
}

/// One execution of one shape against one arm: time it, attribute the whole
/// index decodes and parsed-cache hits it caused, and — on the first run —
/// check the result against the shape's contract. Returns the elapsed
/// milliseconds, the rows, and `(decodes, cache hits)` for this execution
/// alone, which is how an eviction shows itself: a shape that decodes again on
/// every run held nothing between them.
async fn time_ab_shape(
    shape: &AbShape,
    label: &str,
    ctx: &SessionContext,
    first: bool,
) -> (f64, Vec<String>, (u64, u64)) {
    let before = iceberg::arrow::inverted_index_decode_counts();
    let started = std::time::Instant::now();
    let found = raw_column(ctx, &shape.sql).await;
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    let after = iceberg::arrow::inverted_index_decode_counts();
    let name = shape.name;
    if first {
        assert!(
            !found.is_empty(),
            "{name}/{label}: the shape must return rows to be worth timing"
        );
        for row in &found {
            assert!(
                row.contains(shape.term),
                "{name}/{label}: returned a row without {:?}: {row}",
                shape.term
            );
            let ordinal: i64 = row
                .rsplit_once("row-")
                .and_then(|(_, tail)| tail.trim().parse().ok())
                .unwrap_or_else(|| panic!("{name}/{label}: unparsable row {row}"));
            assert!(
                ordinal >= shape.window_floor,
                "{name}/{label}: row {ordinal} is outside the shape's window"
            );
        }
        if shape.exact {
            assert_eq!(
                found.len() as i64,
                shape.corpus_matches,
                "{name}/{label}: an unclipped shape must return every corpus match"
            );
        }
    }
    (elapsed, found, (after.0 - before.0, after.1 - before.1))
}
