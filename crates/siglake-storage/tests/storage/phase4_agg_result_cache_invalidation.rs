//! Phase 4 (aggregation fast path) — windowed result-cache INVALIDATION + data
//! correctness (no metrics). The result cache is
//! keyed by snapshot id and dropped in `invalidate_cached_table` on commit, so an
//! aggregate over a window must reflect newly-appended in-window data, never a
//! stale cached value.

use chrono::{Duration, TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, TimeBounds};

async fn multi_file_table() -> (tempfile::TempDir, IcebergContext, chrono::DateTime<Utc>) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let hosts = ["h0", "h1", "h2"];
    for k in 0..4i64 {
        let start = k * 3600;
        let evs: Vec<Event> = (0..60)
            .map(|m| {
                let secs = start + m * 60;
                let mut e = Event::now(format!("row at {secs}s"));
                e.timestamp = base + Duration::seconds(secs);
                e.host = hosts[(secs as usize / 60) % hosts.len()].to_string();
                e
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }
    (tmp, ice, base)
}

fn window(base: chrono::DateTime<Utc>, lo_secs: i64, hi_secs: i64) -> TimeBounds {
    TimeBounds {
        start: Some(base + Duration::seconds(lo_secs)),
        end: Some(base + Duration::seconds(hi_secs)),
    }
}

fn total(rows: &[(Option<String>, u64)]) -> u64 {
    rows.iter().map(|(_, c)| *c).sum()
}

/// A cached windowed group-by must NOT serve a stale value after an in-window
/// append: the post-commit aggregate reflects the new data.
#[tokio::test]
async fn windowed_group_by_invalidated_on_in_window_append() {
    let (_tmp, ice, base) = multi_file_table().await;
    let win = window(base, 1800, 12600);

    let before = ice
        .grouped_counts_with_summary("events", "host", None, Some(win))
        .await
        .unwrap()
        .unwrap();
    let before_total = total(&before.to_rows());
    // Warm the cache with a second identical call (now cached).
    let _ = ice
        .grouped_counts_with_summary("events", "host", None, Some(win))
        .await
        .unwrap()
        .unwrap();

    // Append 5 new in-window rows on a NEW host => new snapshot/commit.
    let new_evs: Vec<Event> = (0..5)
        .map(|i| {
            let mut e = Event::now(format!("new {i}"));
            e.timestamp = base + Duration::seconds(3600 + i * 60);
            e.host = "h-new".to_string();
            e
        })
        .collect();
    ice.append_events(&new_evs).await.unwrap();

    let after = ice
        .grouped_counts_with_summary("events", "host", None, Some(win))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        total(&after.to_rows()),
        before_total + 5,
        "post-append windowed group-by must reflect the new in-window rows, not a stale cache"
    );
    assert!(
        after.iter().any(|(h, c)| h == Some("h-new") && c == 5),
        "the new host's 5 in-window rows must appear: {after:?}"
    );
}

/// Same invalidation guarantee for the windowed date-histogram.
#[tokio::test]
async fn windowed_histogram_invalidated_on_in_window_append() {
    let (_tmp, ice, base) = multi_file_table().await;
    let win = window(base, 1800, 12600);
    let interval_30m = 1_800_000_000_000;

    let before = ice
        .date_histogram_counts("events", interval_30m, 0, None, Some(win))
        .await
        .unwrap()
        .unwrap();
    let before_total: i64 = before.iter().map(|(_, c)| *c).sum();
    // Warm the cache.
    let _ = ice
        .date_histogram_counts("events", interval_30m, 0, None, Some(win))
        .await
        .unwrap()
        .unwrap();

    let new_evs: Vec<Event> = (0..7)
        .map(|i| {
            let mut e = Event::now(format!("new {i}"));
            e.timestamp = base + Duration::seconds(3600 + i * 60);
            e.host = "h-new".to_string();
            e
        })
        .collect();
    ice.append_events(&new_evs).await.unwrap();

    let after = ice
        .date_histogram_counts("events", interval_30m, 0, None, Some(win))
        .await
        .unwrap()
        .unwrap();
    let after_total: i64 = after.iter().map(|(_, c)| *c).sum();
    assert_eq!(
        after_total,
        before_total + 7,
        "post-append windowed histogram must reflect the new in-window rows, not a stale cache"
    );
}

/// Two windowed group-bys with DIFFERENT windows must not collide in the cache:
/// each returns its own correct result even when both are cached.
#[tokio::test]
async fn distinct_windows_do_not_collide() {
    let (_tmp, ice, base) = multi_file_table().await;
    let win_a = window(base, 0, 3600); // file 0 only
    let win_b = window(base, 3600, 7200); // file 1 only

    let a1 = ice
        .grouped_counts_with_summary("events", "host", None, Some(win_a))
        .await
        .unwrap()
        .unwrap();
    let b1 = ice
        .grouped_counts_with_summary("events", "host", None, Some(win_b))
        .await
        .unwrap()
        .unwrap();
    // Re-run both (now cached) and confirm each window still returns its own value.
    let a2 = ice
        .grouped_counts_with_summary("events", "host", None, Some(win_a))
        .await
        .unwrap()
        .unwrap();
    let b2 = ice
        .grouped_counts_with_summary("events", "host", None, Some(win_b))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(total(&a1.to_rows()), 60, "file 0 window has 60 rows");
    assert_eq!(total(&b1.to_rows()), 60, "file 1 window has 60 rows");
    assert_eq!(a1, a2, "window A warm result must equal its cold result");
    assert_eq!(b1, b2, "window B warm result must equal its cold result");
}
