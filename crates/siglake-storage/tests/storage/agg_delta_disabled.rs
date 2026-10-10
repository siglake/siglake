//! The incremental group-count aggregate's rollback story, asserted rather than
//! assumed: at or below the inline ceiling the mechanism is entirely inert.
//!
//! `docs/DESIGN_incremental_group_count_aggregate.md` promises that dropping
//! `SIGLAKE_TABLE_GROUP_COUNT_CARDINALITY` back to its default restores exactly
//! the behaviour that shipped before the feature existed — a slow
//! high-cardinality `GROUP BY` and an untouched commit path. That is only a
//! rollback if no delta objects are written, nothing new is read, and a wide
//! column stays uncovered; a mechanism that is merely unused still costs a
//! listing per snapshot and a PUT per commit.
//!
//! The raised-cap case lives in `agg_delta_writer.rs`.

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, SnapshotAggregates};

const DISTINCT_HOSTS: usize = 9_000;
const ROWS: usize = 18_000;

fn ev(secs: i64, host: &str) -> Event {
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: host.to_string(),
        source: "src".into(),
        sourcetype: if secs % 2 == 0 {
            "app:json".into()
        } else {
            "syslog".into()
        },
        index: "main".into(),
        raw: format!("request from {host}"),
        attributes: None,
    }
}

/// The one incarnation directory under the events table's `metadata/` — every
/// aggregate artifact of that incarnation lives directly under it (#2919).
/// Found rather than hardcoded, so the test keeps working if the layout moves.
fn aggregate_dir(root: &std::path::Path) -> std::path::PathBuf {
    fn find(dir: &std::path::Path) -> Option<std::path::PathBuf> {
        for e in std::fs::read_dir(dir).ok()? {
            let p = e.ok()?.path();
            if !p.is_dir() {
                continue;
            }
            if p.join("siglake-aggregates.json").exists() {
                return Some(p);
            }
            if let Some(found) = find(&p) {
                return Some(found);
            }
        }
        None
    }
    find(root).expect("events table aggregate dir")
}

#[tokio::test]
async fn the_default_cap_writes_no_deltas_and_covers_no_wide_column() {
    // Set the default explicitly so the test states the value it asserts.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&root).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(4096),
            ..Default::default()
        },
    );
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    let evs: Vec<Event> = (0..ROWS)
        .map(|i| ev(base + i as i64, &format!("host-{:06}", i % DISTINCT_HOSTS)))
        .collect();
    for chunk in evs.chunks(ROWS / 2) {
        ice.append_events(chunk).await.unwrap();
    }

    let meta = aggregate_dir(&root);
    assert!(
        !meta.join("siglake-agg-deltas").exists(),
        "no delta objects below the inline ceiling"
    );
    assert!(
        !meta.join("siglake-agg-wide.json").exists(),
        "no wide aggregate object below the inline ceiling"
    );

    // `host` is over the inline cap, so it is uncovered — which is precisely
    // today's behaviour, and what "rollback" has to mean.
    let bytes = std::fs::read(meta.join("siglake-aggregates.json")).unwrap();
    let inline: SnapshotAggregates = serde_json::from_slice(&bytes).unwrap();
    let inline_gc = inline.group_counts.expect("inline aggregate exists");
    assert_eq!(inline_gc.column_total("host"), None);
    assert_eq!(inline_gc.column_total("sourcetype"), Some(ROWS as u64));

    // Still answers, just by scanning rather than from an aggregate.
    let rows = ice
        .grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .expect("grouped counts served");
    assert_eq!(rows.len(), DISTINCT_HOSTS);
    let mut total = 0u64;
    rows.for_each(|_, count| total = total.saturating_add(count));
    assert_eq!(total, ROWS as u64);
}
