//! A high-cardinality column must stay in the Tier-1 snapshot aggregate.
//!
//! This is the behaviour behind the 2026-07-29 http_logs finding: `top_hosts`
//! took 4,710 ms against Quickwit's 7.55 ms. The cause was not a slow merge or
//! a slow top-K — the query-side selection is already an O(n) quickselect over
//! borrowed keys. The cause was that `host` (~1.1M distinct) exceeded the
//! table-level cardinality cap of 4,096, so the column was dropped from the
//! aggregate AND from every per-file footer, leaving a full DataFusion scan
//! over 247M rows with nothing to accelerate it.
//!
//! An exact top-K has to touch every distinct key once, so it can only be
//! milliseconds if that work happened at commit time. That is what the Tier-1
//! aggregate is for, and the cap was silently excluding exactly the columns
//! that need it most.

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

fn ev(secs: i64, host: &str) -> Event {
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: host.to_string(),
        source: "src".into(),
        sourcetype: "app:json".into(),
        index: "main".into(),
        raw: format!("request from {host}"),
        attributes: None,
    }
}

/// More distinct hosts than the OLD cap (4,096), so this table is exactly the
/// shape that used to fall off the cliff.
const DISTINCT_HOSTS: usize = 12_000;
const ROWS: usize = 48_000;

#[tokio::test]
async fn high_cardinality_column_survives_in_the_tier1_aggregate() {
    // The DEFAULT cap is deliberately low — raising it globally collapsed
    // commit throughput at scale (see `table_group_count_cardinality`). This
    // test covers the capability behind the knob: with the cap raised, a
    // high-cardinality column is covered and answers exactly.
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(2_000_000),
            ..Default::default()
        });
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    // Skewed like real traffic: host i appears (i % 7) + 1 times, so there is a
    // clear, checkable leaderboard rather than a flat distribution.
    let mut expected: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let mut evs = Vec::with_capacity(ROWS);
    for i in 0..ROWS {
        let h = format!("host-{:06}", i % DISTINCT_HOSTS);
        *expected.entry(h.clone()).or_default() += 1;
        evs.push(ev(base + i as i64, &h));
    }
    for chunk in evs.chunks(12_000) {
        ice.append_events(chunk).await.unwrap();
    }

    let total: u64 = expected.values().sum();
    assert_eq!(total, ROWS as u64);

    // The load-bearing assertion: the aggregate COVERS the high-cardinality
    // column. Under the old 4,096 cap this returned None for `host`, which is
    // what forced the full scan.
    let agg = ice
        .table_group_counts_summary("events")
        .await
        .unwrap()
        .expect("snapshot aggregate exists");
    assert_eq!(
        agg.column_total("host"),
        Some(total),
        "the aggregate must cover `host` and account for every row — if this is \
         None the column was dropped by the cardinality cap and any GROUP BY on \
         it falls back to a full scan"
    );

    // …and the served answer is exact, not merely present.
    let rows = ice
        .grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .expect("grouped counts served");
    assert_eq!(
        rows.len(),
        DISTINCT_HOSTS,
        "every distinct host must appear exactly once"
    );
    let got: std::collections::HashMap<String, u64> = rows
        .iter()
        .map(|(v, c)| (v.unwrap_or_default().to_string(), c))
        .collect();
    assert_eq!(got, expected, "counts must be exact for every host");

    // Top-K over that aggregate is the shape the board measures.
    let mut leaderboard: Vec<(&String, &u64)> = got.iter().collect();
    leaderboard.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    assert_eq!(
        *leaderboard[0].1,
        *expected.values().max().unwrap(),
        "the top host is the true maximum"
    );
}
