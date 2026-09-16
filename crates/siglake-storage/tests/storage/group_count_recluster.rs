//! Does the group-count fast path survive re-clustering? WI-15 (50 G AWS) found
//! count_by_level stayed ~350 ms after recluster even with the fast path enabled.
//! This reproduces the ingest → recluster sequence locally and inspects which
//! tier serves: the Tier-1 snapshot aggregate (zero file reads) and the Tier-2
//! per-file footer summaries are the cheap tiers; if neither survives a
//! re-cluster the query falls back to a raw-page RLE column scan (~the cost of a
//! full GROUP BY).

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::{
    IcebergContext, ReclusterMergeOptions, BLOOM_FILTER_COLUMNS, GROUP_COUNTS_KV_KEY,
    TIME_BUCKETS_KV_KEY,
};

/// Does file `path` (a local-fs Parquet data file) carry footer KV `key`?
fn file_has_footer_key(path: &str, key: &str) -> bool {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let local = path.strip_prefix("file://").unwrap_or(path);
    let file = std::fs::File::open(local).expect("open reclustered parquet file");
    let reader = SerializedFileReader::new(file).expect("parquet reader");
    reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .map(|kvs| kvs.iter().any(|e| e.key == key))
        .unwrap_or(false)
}

/// Proves the writer stamped the per-file group-count footer (streaming-recluster fix).
fn file_has_group_count_footer(path: &str) -> bool {
    file_has_footer_key(path, GROUP_COUNTS_KV_KEY)
}

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

/// (sorted (value, count) pairs) for `sourcetype`, summed across whatever tier
/// served — must always equal the true counts.
fn sorted(rows: &[(Option<String>, u64)]) -> Vec<(Option<String>, u64)> {
    let mut v = rows.to_vec();
    v.sort();
    v
}

#[tokio::test]
async fn group_count_fast_path_survives_streaming_recluster() {
    // Force the memory-bounded streaming merge path — the path WI-15's large 50 G
    // bins took, and the one that (before the fix) stamped no per-file group-count
    // footer.

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    // Three appends → three live files, two sourcetypes (60/40 split per batch).
    let mut total = 0u64;
    for b in 0..3 {
        let batch: Vec<Event> = (0..100)
            .map(|i| {
                let st = if i % 5 < 3 { "app:json" } else { "syslog" };
                ev(base + (b * 100 + i) as i64, st)
            })
            .collect();
        total += batch.len() as u64;
        ice.append_events(&batch).await.unwrap();
    }

    let ident = ice.events_table_ident().clone();
    let true_counts = sorted(
        &ice.grouped_counts_with_summary("events", "sourcetype", None, None)
            .await
            .unwrap()
            .expect("grouped counts before recluster")
            .to_rows(),
    );
    // 60% app:json, 40% syslog of 300.
    assert_eq!(
        true_counts,
        vec![(Some("app:json".into()), 180), (Some("syslog".into()), 120)]
    );

    // Tier-1 must be valid BEFORE recluster (ingest maintains it).
    let pre = ice.table_group_counts_summary("events").await.unwrap();
    assert_eq!(
        pre.as_ref().and_then(|a| a.column_total("sourcetype")),
        Some(total),
        "Tier-1 aggregate should cover sourcetype with total == row count after ingest"
    );

    // Re-cluster all three files via the (forced) streaming path.
    let files = ice.live_data_files(&ident).await.unwrap();
    assert!(
        files.len() >= 2,
        "need >=2 files to recluster, got {}",
        files.len()
    );
    let merge = ReclusterMergeOptions {
        force_streaming: Some(true),
        ..ReclusterMergeOptions::default()
    };
    let stats = ice
        .recluster_files_with(&ident, files, BLOOM_FILTER_COLUMNS, &merge)
        .await
        .expect("streaming recluster");
    assert_eq!(stats.rows as u64, total, "recluster conserves rows");

    // The fix: every streaming-reclustered output file must carry the per-file
    // group-count footer (so Tier-2 footer-summing serves count_by_level instead
    // of a raw-page RLE column scan). Before the writer-level change the streaming
    // merge stamped no footer here.
    let reclustered = ice.live_data_files(&ident).await.unwrap();
    assert!(!reclustered.is_empty());
    for f in &reclustered {
        assert!(
            file_has_group_count_footer(f.file_path()),
            "streaming-reclustered file {} must carry the group-count footer",
            f.file_path()
        );
        // …and the per-file time-bucket footer (date_histogram re-buckets it
        // instead of scanning the timestamp column of boundary-straddling files).
        assert!(
            file_has_footer_key(f.file_path(), TIME_BUCKETS_KV_KEY),
            "streaming-reclustered file {} must carry the time-bucket footer",
            f.file_path()
        );
    }

    // Correctness MUST hold regardless of tier.
    let after = sorted(
        &ice.grouped_counts_with_summary("events", "sourcetype", None, None)
            .await
            .unwrap()
            .expect("grouped counts after recluster")
            .to_rows(),
    );
    assert_eq!(after, true_counts, "counts must match after recluster");

    // The crux: a cheap tier must survive the streaming recluster. Tier-1 (the
    // snapshot aggregate) is carried forward by recluster_files; assert it is
    // still valid (total == record count) so whole-table count_by_level is served
    // from manifest metadata with zero file reads.
    let post = ice.table_group_counts_summary("events").await.unwrap();
    assert_eq!(
        post.as_ref().and_then(|a| a.column_total("sourcetype")),
        Some(total),
        "Tier-1 aggregate must remain valid after a streaming recluster"
    );
}
