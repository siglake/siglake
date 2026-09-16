//! Differential tests for the Phase 1 windowed footer-aggregation fast paths:
//! `IcebergContext::grouped_counts_with_summary(..., Some(window))` and
//! `date_histogram_counts(..., Some(window))` must return EXACTLY what a full
//! DataFusion scan+aggregate over the same `WHERE timestamp ∈ [lo,hi)` predicate
//! produces — across a multi-file table and several window shapes (aligned to a
//! file boundary, straddling one boundary, straddling both, empty, whole-range).
//!
//! The fast path combines per-file footer partials for files fully inside the
//! window and scans only the ~2 boundary files; the oracle is the planner. They
//! must agree byte-for-byte (modulo ordering, which the query layer applies).

use arrow_array::Array;
use chrono::{Duration, TimeZone, Utc};
use datafusion::prelude::SessionContext;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, TimeBounds};
use std::collections::BTreeMap;

/// Build a multi-file events table spanning a known time range. Each append is a
/// separate Iceberg data file with its own footer + manifest [min,max]; the files
/// are deliberately time-disjoint so a window can land aligned to, or straddle,
/// the file boundaries. `host` cycles over a small set (the group column).
async fn multi_file_table() -> (tempfile::TempDir, IcebergContext, chrono::DateTime<Utc>) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

    let at = |secs: i64, host: &str| -> Event {
        let mut e = Event::now(format!("row at {secs}s host {host}"));
        e.timestamp = base + Duration::seconds(secs);
        e.host = host.to_string();
        e
    };
    let hosts = ["h0", "h1", "h2"];

    // Four time-disjoint files, each a 1-hour span at hours 0,1,2,3.
    // File k covers [k*3600, k*3600 + 3540], one row per minute, host cycling.
    for k in 0..4i64 {
        let start = k * 3600;
        let evs: Vec<Event> = (0..60)
            .map(|m| {
                let secs = start + m * 60;
                at(secs, hosts[(secs as usize / 60) % hosts.len()])
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }
    (tmp, ice, base)
}

fn ns(base: chrono::DateTime<Utc>, secs: i64) -> i64 {
    (base + Duration::seconds(secs))
        .timestamp_nanos_opt()
        .unwrap()
}

fn window(base: chrono::DateTime<Utc>, lo_secs: i64, hi_secs: i64) -> TimeBounds {
    TimeBounds {
        start: Some(base + Duration::seconds(lo_secs)),
        end: Some(base + Duration::seconds(hi_secs)),
    }
}

/// DataFusion ground-truth grouped count over `WHERE timestamp ∈ [lo,hi) GROUP BY host`.
async fn datafusion_group_counts(
    ice: &IcebergContext,
    base: chrono::DateTime<Utc>,
    lo_secs: i64,
    hi_secs: i64,
) -> BTreeMap<Option<String>, i64> {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let lo = (base + Duration::seconds(lo_secs)).to_rfc3339();
    let hi = (base + Duration::seconds(hi_secs)).to_rfc3339();
    let sql = format!(
        "SELECT host, count(*) AS n FROM events \
         WHERE \"timestamp\" >= TIMESTAMP '{lo}' AND \"timestamp\" < TIMESTAMP '{hi}' \
         GROUP BY host"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut out = BTreeMap::new();
    for batch in &batches {
        let hosts = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let key = if hosts.is_null(i) {
                None
            } else {
                Some(hosts.value(i).to_string())
            };
            out.insert(key, counts.value(i));
        }
    }
    out
}

/// DataFusion ground-truth histogram over `WHERE timestamp ∈ [lo,hi)`.
async fn datafusion_histogram(
    ice: &IcebergContext,
    base: chrono::DateTime<Utc>,
    interval_sql: &str,
    lo_secs: i64,
    hi_secs: i64,
) -> Vec<(i64, i64)> {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let lo = (base + Duration::seconds(lo_secs)).to_rfc3339();
    let hi = (base + Duration::seconds(hi_secs)).to_rfc3339();
    let sql = format!(
        "SELECT date_bin(INTERVAL '{interval_sql}', \"timestamp\", TIMESTAMP '1970-01-01T00:00:00Z') \
         AS bucket, count(*) AS n FROM events \
         WHERE \"timestamp\" >= TIMESTAMP '{lo}' AND \"timestamp\" < TIMESTAMP '{hi}' \
         GROUP BY bucket ORDER BY bucket"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        let buckets = siglake_core::column_nanos(batch.column(0)).unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.push((buckets.value(i), counts.value(i)));
        }
    }
    out
}

fn as_map(rows: &[(Option<String>, u64)]) -> BTreeMap<Option<String>, i64> {
    rows.iter().map(|(k, v)| (k.clone(), *v as i64)).collect()
}

#[tokio::test]
async fn windowed_group_counts_match_datafusion() {
    let (_tmp, ice, base) = multi_file_table().await;

    // Each window: (lo_secs, hi_secs, label).
    let cases = [
        // Aligned to a file boundary: exactly files 1 and 2 (hours 1,2) — both
        // fully-contained, zero boundary scans.
        (3600, 10800, "aligned to file boundaries"),
        // Straddles ONE boundary: starts mid-file-0, ends at the file-2 boundary.
        (1800, 7200, "straddles the lower boundary"),
        // Straddles BOTH boundaries: starts mid-file-0, ends mid-file-3.
        (1800, 12600, "straddles both boundaries"),
        // Empty future window: zero rows, must not error.
        (100000, 100100, "empty window"),
        // Whole range (and a touch beyond) — every file fully contained.
        (0, 20000, "whole range"),
        // Sub-file narrow window entirely inside file 0.
        (600, 1200, "narrow interior window"),
    ];

    for (lo, hi, label) in cases {
        let fast = as_map(
            &ice.grouped_counts_with_summary("events", "host", None, Some(window(base, lo, hi)))
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("windowed group count returned None ({label})"))
                .to_rows(),
        );
        let truth = datafusion_group_counts(&ice, base, lo, hi).await;
        assert_eq!(fast, truth, "windowed group count mismatch: {label}");
    }
}

#[tokio::test]
async fn windowed_group_counts_open_ended_window() {
    // A single-sided bound (`timestamp >= X`, no upper bound) must still be exact.
    let (_tmp, ice, base) = multi_file_table().await;
    let open = TimeBounds {
        start: Some(base + Duration::seconds(5400)), // mid file-1
        end: None,
    };
    let fast = as_map(
        &ice.grouped_counts_with_summary("events", "host", None, Some(open))
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );
    // Oracle: same predicate, no upper bound.
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let lo = (base + Duration::seconds(5400)).to_rfc3339();
    let sql = format!(
        "SELECT host, count(*) AS n FROM events WHERE \"timestamp\" >= TIMESTAMP '{lo}' GROUP BY host"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut truth = BTreeMap::new();
    for batch in &batches {
        let hosts = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            truth.insert(Some(hosts.value(i).to_string()), counts.value(i));
        }
    }
    assert_eq!(fast, truth, "open-ended windowed group count must be exact");
}

#[tokio::test]
async fn windowed_histogram_matches_datafusion() {
    let (_tmp, ice, base) = multi_file_table().await;

    // Intervals exercise both the footer re-bucket path (≥1 min, base-aligned)
    // and the sub-minute scan path.
    let intervals = ["1 hour", "30 minute", "10 minute", "30 second"];
    let cases = [
        (3600, 10800, "aligned to file boundaries"),
        (1800, 7200, "straddles the lower boundary"),
        (1800, 12600, "straddles both boundaries"),
        (100000, 100100, "empty window"),
        (0, 20000, "whole range"),
        (600, 1200, "narrow interior window"),
        // A window-edge mid-bucket: hi at 90s lands inside the first minute bucket.
        (0, 90, "mid-bucket upper edge"),
    ];

    for interval in intervals {
        let interval_ns = match interval {
            "1 hour" => 3_600_000_000_000,
            "30 minute" => 1_800_000_000_000,
            "10 minute" => 600_000_000_000,
            "30 second" => 30_000_000_000,
            other => panic!("unhandled interval {other}"),
        };
        for (lo, hi, label) in cases {
            let fast = ice
                .date_histogram_counts("events", interval_ns, 0, None, Some(window(base, lo, hi)))
                .await
                .unwrap()
                .unwrap_or_else(|| {
                    panic!("windowed histogram returned None ({interval} / {label})")
                });
            let truth = datafusion_histogram(&ice, base, interval, lo, hi).await;
            assert_eq!(
                fast, truth,
                "windowed histogram mismatch: interval {interval}, {label}"
            );
        }
    }
}

#[tokio::test]
async fn unwindowed_path_unchanged() {
    // Regression: `None` window must still produce the full-table result (the
    // Tier-1/Tier-2 footer path), unchanged by Phase 1.
    let (_tmp, ice, _base) = multi_file_table().await;
    let full = as_map(
        &ice.grouped_counts_with_summary("events", "host", None, None)
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );
    // A whole-range window must equal the unwindowed full result.
    let whole = as_map(
        &ice.grouped_counts_with_summary(
            "events",
            "host",
            None,
            Some(TimeBounds {
                start: Some(Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap()),
                end: Some(Utc.with_ymd_and_hms(2026, 6, 2, 0, 0, 0).unwrap()),
            }),
        )
        .await
        .unwrap()
        .unwrap()
        .to_rows(),
    );
    assert_eq!(
        full, whole,
        "whole-range window must equal the unwindowed result"
    );
    let total: i64 = full.values().sum();
    assert_eq!(total, 240, "4 files × 60 rows");
}

/// `ns` is exercised indirectly via the boundary-row precision check below.
#[tokio::test]
async fn windowed_group_counts_exact_at_row_boundaries() {
    let (_tmp, ice, base) = multi_file_table().await;
    // File 0 has rows at 0,60,120,...,3540 s. Window [60, 180) → exactly rows at
    // 60 and 120 (two rows). Verifies inclusive-lower / exclusive-upper at the
    // boundary-scan path. (The `ns` helper keeps this self-documenting.)
    let lo_ns = ns(base, 60);
    let hi_ns = ns(base, 180);
    assert!(hi_ns > lo_ns);
    let fast = as_map(
        &ice.grouped_counts_with_summary("events", "host", None, Some(window(base, 60, 180)))
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );
    let total: i64 = fast.values().sum();
    assert_eq!(
        total, 2,
        "[60s,180s) over a 1-row-per-minute file is exactly 2 rows"
    );
    let truth = datafusion_group_counts(&ice, base, 60, 180).await;
    assert_eq!(fast, truth);
}
