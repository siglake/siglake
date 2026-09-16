//! Phase 2 — the snapshot-keyed unwindowed-aggregate cache must be INVALIDATED on
//! commit. Aggregate once (populates the cache), append new data (a new snapshot
//! via `invalidate_cached_table` on the commit path), aggregate again — the result
//! must reflect the new data, never a stale cached value. No metrics here; this is
//! a pure data-correctness test, so it doesn't contend for the global recorder.

use chrono::{Duration, TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use std::collections::BTreeMap;

fn as_map(rows: &[(Option<String>, u64)]) -> BTreeMap<Option<String>, u64> {
    rows.iter().cloned().collect()
}

#[tokio::test]
async fn snapshot_aggregate_cache_invalidated_on_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

    let make = |host: &str, n: usize, offset: i64| -> Vec<Event> {
        (0..n)
            .map(|i| {
                let mut e = Event::now(format!("row {host} {i}"));
                e.timestamp = base + Duration::seconds(offset + i as i64);
                e.host = host.to_string();
                e
            })
            .collect()
    };

    // First snapshot: 3 × h0, 2 × h1.
    ice.append_events(&make("h0", 3, 0)).await.unwrap();
    ice.append_events(&make("h1", 2, 100)).await.unwrap();

    let first = as_map(
        &ice.grouped_counts_with_summary("events", "host", None, None)
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );
    assert_eq!(first.get(&Some("h0".to_string())), Some(&3));
    assert_eq!(first.get(&Some("h1".to_string())), Some(&2));

    // Warm the cache once more (proves repeat is stable).
    let first_warm = as_map(
        &ice.grouped_counts_with_summary("events", "host", None, None)
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );
    assert_eq!(
        first, first_warm,
        "warm repeat before commit must be identical"
    );

    // Append new data → new snapshot → commit path calls invalidate_cached_table,
    // which must drop the stale snapshot-aggregate entry.
    ice.append_events(&make("h0", 5, 1000)).await.unwrap();
    ice.append_events(&make("h2", 4, 2000)).await.unwrap();

    let after = as_map(
        &ice.grouped_counts_with_summary("events", "host", None, None)
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );
    assert_eq!(
        after.get(&Some("h0".to_string())),
        Some(&8),
        "post-commit aggregate must reflect the new h0 rows (3+5), not the stale 3"
    );
    assert_eq!(after.get(&Some("h1".to_string())), Some(&2));
    assert_eq!(
        after.get(&Some("h2".to_string())),
        Some(&4),
        "the brand-new h2 group must appear after the commit"
    );
    let total: u64 = after.values().sum();
    assert_eq!(total, 14, "3+2+5+4 rows after both commits");
}
