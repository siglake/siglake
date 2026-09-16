//! A merge run that SPANS several fetched parts must emit its rows in order.
//!
//! The page-bounded merge used to `concat_batches` each input's fetched parts
//! into one batch per chunk, so a run was always one contiguous slice of one
//! batch. On 2026-08-27 that copy was removed (it cost ~14% of peak RSS on a
//! converged layout): parts are kept as they arrive and a run is emitted as one
//! zero-copy slice per part it spans, addressed through a per-input prefix sum.
//!
//! The failure that introduces is silent. Get the prefix-sum arithmetic wrong
//! and the merge emits real rows in the wrong ORDER, under a Parquet footer
//! that still declares the declared sort order — and the ordered-scan early-stop
//! trusts that footer, so a browse would return wrong rows with no error
//! anywhere.
//!
//! Row conservation alone does not catch it: a transposition conserves rows.
//! This asserts ORDER, and that the values are the ones that belong.
//!
//! The path only engages when an input contributes more rows to a chunk than
//! the 65,536-row decode batch, so the fixture has to be large enough to make
//! runs span parts — smaller merges take a single-part path and prove nothing.
//!
//! The merge-path selection is pinned per call so the fixture does not depend
//! on process-global configuration.

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, ReclusterMergeOptions, BLOOM_FILTER_COLUMNS};

const FILES: usize = 4;
const PER_FILE: usize = 300_000; // 1.2M rows; chunks of 524,288 over 65,536-row parts

#[tokio::test]
async fn a_run_spanning_parts_emits_rows_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    // Nanosecond spacing keeps all 1.2M rows in ONE day partition; the table
    // partitions by day, and at coarser spacing each file splits into per-day
    // files far too small to make a run span parts.
    let base_ns = Utc
        .timestamp_opt(1_767_225_600, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    // DISJOINT time ranges per file, so each input contributes one LONG
    // contiguous run. Interleaving them instead makes every run a single row,
    // which routes through the per-row interleave path and never exercises the
    // multi-part slice this test is named for — the first version of this
    // fixture did exactly that and passed in 2.3s having proved nothing.
    // At 300,000 rows a run spans five 65,536-row parts.
    for f in 0..FILES {
        let evs: Vec<Event> = (0..PER_FILE)
            .map(|i| {
                let seq = (f * PER_FILE + i) as i64;
                let mut e = Event::now(format!("row {seq}"));
                e.timestamp = chrono::DateTime::from_timestamp_nanos(base_ns + seq);
                e.sourcetype = "app:json".into();
                e
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }

    let ident = ice.events_table_ident();
    let live = ice.live_data_files(ident).await.unwrap();
    let merge = ReclusterMergeOptions {
        // Fan-in 2 with 4 inputs forces merge_files_page_bounded (<= fan-in
        // would take the slice-streaming path, which this test does not touch).
        merge_fanin: Some(2),
        inram_max_rows: Some(1),
        inram_max_bytes: Some(1024 * 1024),
        ..ReclusterMergeOptions::default()
    };
    ice.recluster_files_with(ident, live, BLOOM_FILTER_COLUMNS, &merge)
        .await
        .expect("page-bounded recluster");

    // Read the merged output back in file order and check the timestamps.
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    fn walk(p: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                    out.push(path);
                }
            }
        }
    }
    walk(&warehouse, &mut files);
    files.sort_by_key(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0));
    let merged = files.last().expect("a merged parquet file");

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(
        std::fs::read(merged).unwrap(),
    ))
    .unwrap();
    let total = builder.metadata().file_metadata().num_rows() as usize;
    assert_eq!(
        total,
        FILES * PER_FILE,
        "the merge did not produce one file holding every row; this fixture is \
         not exercising a multi-part run"
    );

    let mut seen = 0usize;
    let mut prev: Option<i64> = None;
    for batch in builder.build().unwrap() {
        let batch = batch.unwrap();
        let idx = batch
            .schema()
            .index_of(siglake_core::nanos_source_column(
                batch.schema().as_ref(),
                "timestamp",
            ))
            .unwrap();
        let ts = siglake_core::column_nanos(batch.column(idx)).unwrap();
        for i in 0..ts.len() {
            let v = ts.value(i);
            // Every row is a distinct nanosecond, so ascending is STRICT — a
            // transposition of any two rows fails here even though the row count
            // and the multiset of values are both untouched by it.
            if let Some(p) = prev {
                assert!(
                    v > p,
                    "row {seen} is out of order: {v} followed {p}. A run spanning \
                     parts was emitted with its pieces in the wrong order, under a \
                     footer that still claims the declared sort."
                );
            }
            assert_eq!(
                v,
                base_ns + seen as i64,
                "row {seen} holds the wrong timestamp — the prefix sum mapped a \
                 chunk-local row to the wrong (part, row)"
            );
            prev = Some(v);
            seen += 1;
        }
    }
    assert_eq!(
        seen,
        FILES * PER_FILE,
        "read back fewer rows than the footer claims"
    );
}
