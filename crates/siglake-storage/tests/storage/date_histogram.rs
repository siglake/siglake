//! Differential test for the date_histogram (time-bucket count) fast path:
//! `IcebergContext::date_histogram_counts` must return EXACTLY the buckets +
//! counts that DataFusion's `date_bin(...) GROUP BY bucket` produces, across
//! both code paths — files whose whole time range sits in one bucket (counted
//! from manifest stats, no scan) and files that straddle a bucket boundary
//! (scanned). Run over several intervals to exercise both.

use chrono::{Duration, TimeZone, Utc};
use datafusion::prelude::SessionContext;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

/// Ground truth via DataFusion: (bucket_start_ns, count) ascending.
async fn datafusion_histogram(ice: &IcebergContext, interval_sql: &str) -> Vec<(i64, i64)> {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let sql = format!(
        "SELECT date_bin(INTERVAL '{interval_sql}', \"timestamp\", TIMESTAMP '1970-01-01T00:00:00Z') \
         AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket"
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

#[tokio::test]
async fn date_histogram_counts_matches_datafusion() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

    let at = |secs: i64| -> Event {
        let mut e = Event::now(format!("row at {secs}s"));
        e.timestamp = base + Duration::seconds(secs);
        e
    };

    // File A: all inside hour 0 (0..1800s) → whole-file stats path, bucket 0.
    ice.append_events(&(0..1800).step_by(60).map(at).collect::<Vec<_>>())
        .await
        .unwrap();
    // File B: straddles hours 0→2 (1700..7300s) → boundary scan path, 3 buckets.
    ice.append_events(&(1700..7300).step_by(200).map(at).collect::<Vec<_>>())
        .await
        .unwrap();
    // File C: all inside hour 5 (18000..19800s) → whole-file stats path, bucket 5.
    ice.append_events(&(18000..19800).step_by(60).map(at).collect::<Vec<_>>())
        .await
        .unwrap();
    // File D: a single row (exercises a degenerate single-value file).
    ice.append_events(&[at(40000)]).await.unwrap();

    // 1-hour buckets: mix of stats-path and boundary-scan files.
    let want_1h = datafusion_histogram(&ice, "1 hour").await;
    let got_1h = ice
        .date_histogram_counts("events", 3_600_000_000_000, 0, None, None)
        .await
        .unwrap()
        .expect("fast path answers the histogram");
    assert_eq!(
        got_1h, want_1h,
        "1h histogram must match DataFusion exactly"
    );

    // 24-hour buckets: every file now sits in bucket 0 → pure stats path.
    let want_24h = datafusion_histogram(&ice, "24 hour").await;
    let got_24h = ice
        .date_histogram_counts("events", 86_400_000_000_000, 0, None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        got_24h, want_24h,
        "24h histogram must match DataFusion exactly"
    );

    // 10-minute buckets: finer than several files' spans → more boundary scans.
    let want_10m = datafusion_histogram(&ice, "10 minute").await;
    let got_10m = ice
        .date_histogram_counts("events", 600_000_000_000, 0, None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        got_10m, want_10m,
        "10m histogram must match DataFusion exactly"
    );

    // Total across buckets equals the row count (no loss/dupe across paths).
    let total: i64 = got_1h.iter().map(|(_, c)| c).sum();
    assert_eq!(total, 30 + 28 + 30 + 1);
}

#[tokio::test]
async fn date_histogram_counts_rejects_nonpositive_interval() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    assert!(ice
        .date_histogram_counts("events", 0, 0, None, None)
        .await
        .unwrap()
        .is_none());
    assert!(ice
        .date_histogram_counts("events", -1, 0, None, None)
        .await
        .unwrap()
        .is_none());
}
