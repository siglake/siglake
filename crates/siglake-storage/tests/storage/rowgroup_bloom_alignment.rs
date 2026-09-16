//! Row-group blooms must stay ALIGNED with the row groups they describe, when a
//! row group is assembled from many input batches.
//!
//! The writer used to `concat_batches` the whole carry buffer into one
//! `RecordBatch` per row group, purely so the bloom could see a single batch. On
//! 2026-08-27 that copy was removed: the bloom now folds over a slice of batches
//! and the inner writer forms the row group from a run of writes. That is a
//! change to how row groups are FORMED, and the failure it risks is silent —
//! a bloom shifted by one row group prunes a row group that holds matching rows
//! and the reader returns nothing, with no error anywhere.
//!
//! `iceberg_round_trip.rs` already covers a single large batch straddling a
//! boundary. This covers the case the change actually introduced: MANY batches
//! per row group, which only the compaction merge path produces.
//!
//! The merge-path selection is pinned per call so the fixture does not depend
//! on process-global configuration.

use chrono::{TimeZone, Utc};
use siglake_bloom::RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, ReclusterMergeOptions, BLOOM_FILTER_COLUMNS};

/// Distinct per-input-file marker, chosen so its trigrams appear nowhere else.
fn marker(file: usize) -> String {
    format!("zqxmarker{file}zqx")
}

fn events_for(file: usize, n: usize, base_ns: i64) -> Vec<Event> {
    (0..n)
        .map(|i| {
            let mut e = Event::now(format!(
                "GET /api/v1/logs 200 in 13ms {} seq={i}",
                marker(file)
            ));
            // Disjoint, ascending time ranges per file so the merged output is
            // contiguous per file and row-group boundaries are predictable —
            // and spaced in NANOSECONDS so all 1.2M rows land in ONE day
            // partition. At one row per second they spread over 14 days, the
            // table partitions by day, and every file came out at exactly
            // 86,400 rows: one row group each, and the test could not see what
            // it exists to see.
            e.timestamp = chrono::DateTime::from_timestamp_nanos(base_ns + (file * n + i) as i64);
            e.sourcetype = "app:json".into();
            e
        })
        .collect()
}

fn find_parquet(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(find_parquet(&p));
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(p);
            }
        }
    }
    out
}

#[tokio::test]
async fn rowgroup_blooms_stay_aligned_when_a_row_group_spans_many_batches() {
    // The merge path sizes row groups with `target_row_group_rows(None)`, which
    // is a FIXED 1,048,576 rows — `SIGLAKE_PARQUET_TARGET_ROW_GROUP_BYTES` only
    // applies where a sample batch is available, which the merge writer has no
    // reason to have. So the fixture has to clear 1,048,576 rows for real; there
    // is no knob that makes this cheaper, and the assertion below refuses to
    // pass if it ever stops clearing it.
    // Force the STREAMING merge path, which is the one that buffers batches and
    // forms row groups itself. Left to itself a 1.2M-row bin takes the in-RAM
    // path, whose writer sizes row groups from a sample batch and produced a
    // single row group covering everything — so the fixture silently tested a
    // path this change does not touch.
    const FILES: usize = 4;
    const PER_FILE: usize = 300_000; // 1.2M total => 2 row groups (1,048,576 + 151,424)

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();

    let base_ns = Utc
        .timestamp_opt(1_767_225_600, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    for f in 0..FILES {
        ice.append_events(&events_for(f, PER_FILE, base_ns))
            .await
            .unwrap();
    }

    // Merge them into one file through the real compaction path — this is what
    // feeds the writer many batches per row group.
    let ident = ice.events_table_ident();
    let before = ice.live_data_files(ident).await.unwrap();
    assert!(before.len() >= FILES, "expected {FILES} input files");
    let merge = ReclusterMergeOptions {
        merge_fanin: Some(64),
        inram_max_rows: Some(1),
        inram_max_bytes: Some(1024 * 1024),
        ..ReclusterMergeOptions::default()
    };
    ice.recluster_files_with(ident, before, BLOOM_FILTER_COLUMNS, &merge)
        .await
        .expect("slice-streaming recluster");

    // The merged output: the largest parquet file in the warehouse.
    let mut files = find_parquet(&warehouse);
    files.sort_by_key(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0));
    let merged = files.last().expect("a merged parquet file");

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let bytes = std::fs::read(merged).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let meta = builder.metadata().clone();
    let rg_count = meta.num_row_groups();

    // Without more than one row group this test asserts nothing about alignment.
    assert!(
        rg_count > 1,
        "fixture produced {rg_count} row group(s); it cannot detect a shifted bloom"
    );

    let hex = meta
        .file_metadata()
        .key_value_metadata()
        .and_then(|kv| {
            kv.iter()
                .find(|e| e.key == RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY)
                .and_then(|e| e.value.clone())
        })
        .expect("merged file carries row-group blooms");
    let blooms = siglake_bloom::rowgroup_blooms_from_hex(&hex).expect("blooms decode");

    // ONE BLOOM PER ROW GROUP. A mismatch here is the misalignment itself: the
    // reader indexes blooms by row-group ordinal.
    assert_eq!(
        blooms.len(),
        rg_count,
        "bloom count does not match row-group count — every prune decision after \
         this point is made against the wrong row group's bloom"
    );

    // And each bloom must describe ITS OWN row group: every marker actually
    // present in a row group must be accepted, and at least one marker absent
    // from it must be rejected. The rejection is the half that catches a shift;
    // a bloom that accepted everything would pass the first check alone.
    let mut rejections = 0;
    for (rg, bloom) in blooms.iter().enumerate() {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(
            std::fs::read(merged).unwrap(),
        ))
        .unwrap()
        .with_row_groups(vec![rg])
        .build()
        .unwrap();
        let mut present = std::collections::HashSet::new();
        for batch in reader {
            let batch = batch.unwrap();
            let idx = batch.schema().index_of("raw").unwrap();
            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for v in col.iter().flatten() {
                for f in 0..FILES {
                    if v.contains(&marker(f)) {
                        present.insert(f);
                    }
                }
            }
        }
        assert!(!present.is_empty(), "row group {rg} held no marker at all");

        for f in 0..FILES {
            let grams = siglake_bloom::query_trigrams(&marker(f)).unwrap();
            let accepted = grams.iter().all(|g| bloom.maybe_contains(g));
            if present.contains(&f) {
                assert!(
                    accepted,
                    "row group {rg} CONTAINS {} but its bloom rejects it — rows that \
                     match will be silently pruned",
                    marker(f)
                );
            } else if !accepted {
                rejections += 1;
            }
        }
    }
    assert!(
        rejections > 0,
        "no bloom rejected any absent marker; this fixture cannot distinguish an \
         aligned bloom from one that accepts everything"
    );
}
