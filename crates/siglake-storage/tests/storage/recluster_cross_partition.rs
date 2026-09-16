//! Regression (#4200): a streaming `recluster_files_with` must refuse a bin
//! whose files sit in different `day(timestamp)` partitions, and it must refuse
//! before writing anything.
//!
//! The streaming merge executors stamp their whole output with
//! `files[0].partition()`, so a two-day bin used to commit one day's rows under
//! the other day's partition value. The rewrite's row-count guard passes (no row
//! is lost), but a query whose timestamp predicate resolves against the real day
//! prunes the file and the rows are invisible. Measured on this fixture with the
//! guard removed: the slice-streaming and page-bounded merges each wrote ONE
//! file under `day_ts=2026-06-01` holding all 12 rows, `count(*)` still answered
//! 12 from the manifest, and `WHERE timestamp >= '2026-06-02T12:00:00Z'`
//! answered 0 where 4 rows match. The prune is the partition filter, not file
//! stats: the merged file's `timestamp` bounds cover both days.
//!
//! The in-RAM concat splits its output by partition value, so on the same input
//! it wrote two correctly-stamped files and every window stayed exact. That path
//! keeps taking mixed bins (in-tree callers rewrite a whole multi-day index
//! table through it); only the streaming dispatches refuse.
//!
//! Timestamps are fixed calendar instants, never `Event::now`, so the partition
//! values do not depend on when the test runs.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, TimeZone, Utc};
use datafusion::prelude::SessionContext;
use iceberg::TableIdent;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, ReclusterMergeOptions, BLOOM_FILTER_COLUMNS};

/// 2026-06-01T12:00:00Z — the `day(timestamp)` partition the two same-day files
/// share.
fn day_a() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 1, 12, 0, 0).unwrap()
}

/// 2026-06-02T12:00:00Z — a different `day(timestamp)` partition value.
fn day_b() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 2, 12, 0, 0).unwrap()
}

fn events_at(base: DateTime<Utc>, offsets: &[i64]) -> Vec<Event> {
    offsets
        .iter()
        .map(|&s| {
            let at = base + Duration::seconds(s);
            Event {
                timestamp: at,
                host: "h1".into(),
                source: "src".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: format!("row at {s}"),
                attributes: None,
            }
        })
        .collect()
}

/// Three files: two interleaved inside day A, one in day B. Three (not two) so
/// the fan-in cap can be driven below the bin size, which is what selects the
/// page-bounded and tiered merges. Returns every event timestamp written, so
/// window expectations are derived from the fixture instead of hand-counted.
async fn three_file_two_day_warehouse(
    warehouse: &Path,
) -> (IcebergContext, TableIdent, Vec<DateTime<Utc>>) {
    let ice = IcebergContext::open(warehouse)
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO);
    let batches = [
        events_at(day_a(), &[0, 2, 4, 6]),
        events_at(day_a(), &[1, 3, 5, 7]),
        events_at(day_b(), &[0, 1, 2, 3]),
    ];
    let mut written = Vec::new();
    for batch in &batches {
        ice.append_events(batch).await.unwrap();
        written.extend(batch.iter().map(|e| e.timestamp));
    }
    let ident = ice.events_table_ident().clone();
    let files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(files.len(), 3, "one file per append");
    let partitions: std::collections::BTreeSet<String> = files
        .iter()
        .map(|f| format!("{:?}", f.partition()))
        .collect();
    assert_eq!(
        partitions.len(),
        2,
        "fixture must straddle exactly two day partitions, got {partitions:?}"
    );
    (ice, ident, written)
}

fn parquet_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "parquet") {
                out.push(path);
            }
        }
    }
    out.sort();
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

/// `count(*)` with an interior timestamp predicate: the shape that made the
/// stamped-over rows disappear. The unfiltered count answers from manifest
/// record counts and cannot see a wrong partition value.
async fn window_count(ice: &IcebergContext, from: DateTime<Utc>) -> i64 {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let sql = format!(
        "SELECT count(*) AS n FROM events WHERE timestamp >= TIMESTAMP '{}'",
        from.to_rfc3339()
    );
    count(&ctx, &sql).await
}

async fn total_count(ice: &IcebergContext) -> i64 {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    count(&ctx, "SELECT count(*) AS n FROM events").await
}

#[tokio::test]
async fn cross_partition_bin_is_refused_on_every_streaming_dispatch() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let (ice, ident, written) = three_file_two_day_warehouse(&warehouse).await;

    // The windows a wrong partition stamp destroys: one inside day A (survives
    // the defect, since the stamp would be day A's) and the day-B ones, which
    // the defect drops to 0 by pruning the whole merged file.
    let cutoffs: Vec<DateTime<Utc>> = vec![
        day_a() + Duration::seconds(3),
        day_b(),
        day_b() + Duration::seconds(2),
    ];
    let expected: Vec<i64> = cutoffs
        .iter()
        .map(|c| written.iter().filter(|t| *t >= c).count() as i64)
        .collect();
    assert_eq!(expected, vec![9, 4, 2], "fixture window expectations");
    for (cutoff, want) in cutoffs.iter().zip(&expected) {
        assert_eq!(window_count(&ice, *cutoff).await, *want);
    }

    let snapshot_before = ice.current_events_snapshot_id().await.unwrap();
    let files_before = ice.live_data_files(&ident).await.unwrap();
    let on_disk_before = parquet_files(&warehouse);

    // One arm per streaming merge implementation `recluster_files_with` can
    // dispatch to, each pinned by explicit options so the environment cannot
    // steer it: the slice-streaming k-way merge (bin within the fan-in cap), the
    // page-bounded plan merge and the legacy tiered merge (both above it). Every
    // one of them writes through a single partition-stamped writer.
    let arms = [
        (
            "slice streaming",
            ReclusterMergeOptions {
                force_streaming: Some(true),
                merge_fanin: Some(8),
                ..ReclusterMergeOptions::default()
            },
        ),
        (
            "page-bounded",
            ReclusterMergeOptions {
                force_streaming: Some(true),
                merge_fanin: Some(2),
                force_tiered: Some(false),
                ..ReclusterMergeOptions::default()
            },
        ),
        (
            "tiered",
            ReclusterMergeOptions {
                force_streaming: Some(true),
                merge_fanin: Some(2),
                force_tiered: Some(true),
                ..ReclusterMergeOptions::default()
            },
        ),
    ];
    for (label, merge) in arms {
        let err = ice
            .recluster_files_with(&ident, files_before.clone(), BLOOM_FILTER_COLUMNS, &merge)
            .await
            .expect_err(&format!("{label}: cross-partition bin must be refused"));
        let msg = format!("{err:#}");
        assert!(
            msg.contains("bin spanning 2 partitions"),
            "{label}: error must name the partition span, got {msg}"
        );
        assert!(
            msg.contains("group the files by partition value"),
            "{label}: error must name the remedy, got {msg}"
        );

        // Refused before the first write: no new data file on disk, no commit.
        assert_eq!(
            parquet_files(&warehouse),
            on_disk_before,
            "{label}: refusal must not leave (or commit) output files"
        );
        assert_eq!(
            ice.current_events_snapshot_id().await.unwrap(),
            snapshot_before,
            "{label}: refusal must not move the snapshot"
        );
        let after: Vec<String> = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();
        let before: Vec<String> = files_before
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();
        assert_eq!(after, before, "{label}: live file set unchanged");
        assert_eq!(
            total_count(&ice).await,
            written.len() as i64,
            "{label}: row count unchanged"
        );
        for (cutoff, want) in cutoffs.iter().zip(&expected) {
            assert_eq!(
                window_count(&ice, *cutoff).await,
                *want,
                "{label}: window visibility from {cutoff} unchanged"
            );
        }
    }
}

/// The other half of the contract: the in-RAM merge still accepts a two-day bin
/// — in-tree callers rewrite whole multi-day tables through it — and it is
/// correct because it splits its output by partition value. This is what the
/// streaming refusal is measured against, so it is pinned rather than assumed.
#[tokio::test]
async fn in_ram_merge_of_a_two_day_bin_splits_by_partition() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let (ice, ident, written) = three_file_two_day_warehouse(&warehouse).await;

    let merge = ReclusterMergeOptions {
        force_streaming: Some(false),
        inram_max_bytes: Some(u64::MAX),
        inram_max_rows: Some(u64::MAX),
        ..ReclusterMergeOptions::default()
    };
    let files = ice.live_data_files(&ident).await.unwrap();
    let stats = ice
        .recluster_files_with(&ident, files, BLOOM_FILTER_COLUMNS, &merge)
        .await
        .expect("in-RAM merge takes a mixed bin");
    assert_eq!(stats.files_removed, 3);
    assert_eq!(stats.files_added, 2, "one output file per day partition");
    assert_eq!(stats.rows, written.len(), "every row carried through");

    let merged = ice.live_data_files(&ident).await.unwrap();
    let stamped: std::collections::BTreeSet<String> = merged
        .iter()
        .map(|f| format!("{:?}", f.partition()))
        .collect();
    assert_eq!(stamped.len(), 2, "both partition values kept: {stamped:?}");

    assert_eq!(total_count(&ice).await, written.len() as i64);
    for cutoff in [
        day_a() + Duration::seconds(3),
        day_b(),
        day_b() + Duration::seconds(2),
    ] {
        let expect = written.iter().filter(|t| **t >= cutoff).count() as i64;
        assert_eq!(
            window_count(&ice, cutoff).await,
            expect,
            "window from {cutoff} exact after the in-RAM rewrite"
        );
    }
}

#[tokio::test]
async fn same_partition_bin_still_merges_with_exact_window_visibility() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let (ice, ident, written) = three_file_two_day_warehouse(&warehouse).await;

    // The two day-A files only. This is what a caller that groups by partition
    // hands in, and it is the streaming path (force_streaming) that the guard
    // sits in front of.
    let day_a_partition = {
        let files = ice.live_data_files(&ident).await.unwrap();
        format!("{:?}", files[0].partition())
    };
    let bin: Vec<_> = ice
        .live_data_files(&ident)
        .await
        .unwrap()
        .into_iter()
        .filter(|f| format!("{:?}", f.partition()) == day_a_partition)
        .collect();
    assert_eq!(bin.len(), 2, "two files in day A");

    let merge = ReclusterMergeOptions {
        force_streaming: Some(true),
        merge_fanin: Some(8),
        ..ReclusterMergeOptions::default()
    };
    let stats = ice
        .recluster_files_with(&ident, bin, BLOOM_FILTER_COLUMNS, &merge)
        .await
        .expect("single-partition bin merges");
    assert_eq!(stats.files_removed, 2);
    assert!(stats.files_added >= 1);
    assert_eq!(stats.rows, 8, "both day-A files carried through");

    // Every window is exact after the rewrite, including ones that land inside
    // the merged file's range and inside the untouched day-B file's range.
    assert_eq!(total_count(&ice).await, written.len() as i64);
    for offset in 0..8 {
        let cutoff = day_a() + Duration::seconds(offset);
        let expected = written.iter().filter(|t| **t >= cutoff).count() as i64;
        assert_eq!(
            window_count(&ice, cutoff).await,
            expected,
            "day-A window from +{offset}s"
        );
    }
    for offset in 0..4 {
        let cutoff = day_b() + Duration::seconds(offset);
        let expected = written.iter().filter(|t| **t >= cutoff).count() as i64;
        assert_eq!(
            window_count(&ice, cutoff).await,
            expected,
            "day-B window from +{offset}s"
        );
    }
}
