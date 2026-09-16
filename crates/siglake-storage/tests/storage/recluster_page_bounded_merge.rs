//! Page-bounded plan merge — the DEFAULT >fan-in recluster path. Instead of
//! tiering through intermediate sorted Parquet (which re-writes every byte once
//! per tier), the merge plans an RLE run sequence from a timestamps-only read
//! and executes it in row-bounded chunks, re-opening each contributing input
//! with a `RowSelection` per chunk — so decoded memory is bounded by the chunk,
//! not the fan-in, and NO intermediate files are ever written. This test forces
//! a tiny fan-in and a tiny chunk so a many-file overlapping cluster exercises
//! the chunked executor end-to-end through `recluster_files`, and checks the
//! same invariants the tiered path guarantees: rows conserved, group-count
//! footers on the final output, nothing but the originals + final output on
//! disk.
//!

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, ReclusterMergeOptions, BLOOM_FILTER_COLUMNS};

fn ev(secs: i64, sourcetype: &str) -> Event {
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: "h1".into(),
        source: "src".into(),
        sourcetype: sourcetype.into(),
        index: "main".into(),
        raw: format!("event at {secs} {sourcetype}"),
        attributes: None,
    }
}

#[tokio::test]
async fn page_bounded_recluster_conserves_rows_and_disjoins() {
    // Force a tiny fan-in so 8 files take the default >fan-in page-bounded
    // route, and a tiny chunk so the 800-row merge runs through many chunks
    // with run splits at chunk boundaries.
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    // 8 appends, each covering the SAME 100s range [base, base+99] → 8 files
    // that all OVERLAP in time (the pathological cluster the merge must
    // disjoin — every chunk draws rows from all 8 inputs).
    let mut total = 0u64;
    for _ in 0..8 {
        let batch: Vec<Event> = (0..100)
            .map(|i| ev(base + i, if i % 5 < 3 { "app:json" } else { "syslog" }))
            .collect();
        total += batch.len() as u64;
        ice.append_events(&batch).await.unwrap();
    }
    assert_eq!(total, 800);

    let ident = ice.events_table_ident().clone();
    let files = ice.live_data_files(&ident).await.unwrap();
    assert!(
        files.len() >= 8,
        "need the 8 overlapping files, got {}",
        files.len()
    );

    let stats = ice
        .recluster_files_with(
            &ident,
            files,
            BLOOM_FILTER_COLUMNS,
            &ReclusterMergeOptions {
                force_streaming: Some(true),
                merge_fanin: Some(2),
                force_tiered: Some(false),
                merge_chunk_rows: Some(1024),
                ..Default::default()
            },
        )
        .await
        .expect("page-bounded recluster");
    assert_eq!(
        stats.rows as u64, total,
        "page-bounded merge conserves rows"
    );

    // The final output's group-count footers survive (else this falls to a scan).
    let counts = {
        let rows = ice
            .grouped_counts_with_summary("events", "sourcetype", None, None)
            .await
            .unwrap()
            .expect("grouped counts after page-bounded recluster");
        let mut v = rows.to_rows();
        v.sort();
        v
    };
    // 60% app:json, 40% syslog of 800.
    assert_eq!(
        counts,
        vec![(Some("app:json".into()), 480), (Some("syslog".into()), 320)]
    );

    // The page-bounded merge writes NO intermediates at any point: on disk we
    // expect exactly the 8 GC-pending originals (rewrite_files un-references
    // but doesn't physically delete them) + the committed final output.
    let out = ice.live_data_files(&ident).await.unwrap();
    let warehouse = tmp.path().join("warehouse");
    let on_disk = walk_parquet(&warehouse.join("siglake/events/data")).len();
    assert_eq!(
        on_disk,
        8 + out.len(),
        "expected 8 GC-pending originals + {} final output file(s) and nothing else",
        out.len()
    );

    // Merged output is globally time-sorted across (sorted) output files and
    // conserves the exact multiset: 8 copies of each of the 100 timestamps.
    let mut all_ts: Vec<i64> = Vec::new();
    for df in &out {
        let path = df.file_path().trim_start_matches("file://").to_string();
        let bytes = std::fs::read(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(bytes),
        )
        .unwrap()
        .build()
        .unwrap();
        for rb in reader {
            let rb = rb.unwrap();
            let idx = rb
                .schema()
                .index_of(siglake_core::nanos_source_column(
                    rb.schema().as_ref(),
                    "timestamp",
                ))
                .unwrap();
            let col = siglake_core::column_nanos(rb.column(idx)).unwrap();
            for r in 0..rb.num_rows() {
                all_ts.push(col.value(r));
            }
        }
    }
    assert_eq!(all_ts.len(), 800);
    let expected: Vec<i64> = (0..100)
        .flat_map(|i| std::iter::repeat_n((base + i) * 1_000_000_000, 8))
        .collect();
    assert_eq!(all_ts, expected, "sorted, lossless, duplicate-preserving");
}

fn walk_parquet(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk_parquet(&p));
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(p);
            }
        }
    }
    out
}
