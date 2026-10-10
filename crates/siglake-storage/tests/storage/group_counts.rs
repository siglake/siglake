//! Differential test for the precomputed per-file group-count summary fast path:
//! `IcebergContext::grouped_counts_with_summary` must return EXACTLY the per-value
//! counts DataFusion's `GROUP BY <col> count(*)` produces — across the
//! footer-summary path, the raw-page RLE fallback (a high-cardinality column whose
//! footer summary is omitted), a mix of both in one query, and per-shard partials.

use std::collections::BTreeMap;

use arrow_array::Array;
use chrono::Utc;
use datafusion::prelude::SessionContext;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, ReclusterPolicy, BLOOM_FILTER_COLUMNS};
use siglake_storage::ScanShard;

fn ev(host: &str, source: &str, sourcetype: &str) -> Event {
    Event {
        timestamp: Utc::now(),
        host: host.into(),
        source: source.into(),
        sourcetype: sourcetype.into(),
        index: "main".into(),
        raw: "r".into(),
        attributes: None,
    }
}

/// Ground truth via DataFusion: value (NULL -> None) -> count.
async fn datafusion_group_counts(ice: &IcebergContext, col: &str) -> BTreeMap<Option<String>, i64> {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let sql = format!("SELECT \"{col}\" AS g, count(*) AS n FROM events GROUP BY \"{col}\"");
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut out = BTreeMap::new();
    for batch in &batches {
        let g = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let n = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let key = if g.is_null(i) {
                None
            } else {
                Some(g.value(i).to_string())
            };
            out.insert(key, n.value(i));
        }
    }
    out
}

fn as_map(rows: &[(Option<String>, u64)]) -> BTreeMap<Option<String>, i64> {
    rows.iter().map(|(k, v)| (k.clone(), *v as i64)).collect()
}

#[tokio::test]
async fn summary_grouped_counts_match_datafusion_across_files() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    // Three files; the low-cardinality dimensional values are spread across them so
    // the cross-file SUM of per-file footer summaries is exercised.
    ice.append_events(&[
        ev("h1", "a", "app:json"),
        ev("h1", "b", "app:json"),
        ev("h2", "a", "app:json"),
    ])
    .await
    .unwrap();
    ice.append_events(&[ev("h2", "b", "app:json"), ev("h3", "a", "sys:log")])
        .await
        .unwrap();
    ice.append_events(&[
        ev("h1", "c", "sys:log"),
        ev("h3", "c", "sys:log"),
        ev("h2", "a", "app:json"),
    ])
    .await
    .unwrap();

    for col in ["source", "sourcetype", "host", "index"] {
        let fast = ice
            .grouped_counts_with_summary("events", col, None, None)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("fast path returned None for {col}"));
        assert_eq!(
            as_map(&fast.to_rows()),
            datafusion_group_counts(&ice, col).await,
            "column {col}"
        );
    }
}

#[tokio::test]
async fn summary_and_rle_fallback_mix_matches_datafusion() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    // File A: low-card host -> summarized in the footer.
    ice.append_events(&[
        ev("h1", "a", "app:json"),
        ev("h1", "b", "app:json"),
        ev("h2", "a", "app:json"),
    ])
    .await
    .unwrap();
    // File B: more distinct hosts than BOTH the per-file cap (1024) and the
    // table-level cap (4096), so host is omitted from the snapshot aggregate
    // (Tier 1 skips it) AND from file B's footer — forcing the full chain: Tier 1
    // miss -> file A from its footer summary, file B from the raw-page RLE scan.
    // The merged total must still equal DataFusion's GROUP BY.
    let big: Vec<Event> = (0..4200)
        .map(|i| ev(&format!("host-{i:04}"), "a", "app:json"))
        .collect();
    ice.append_events(&big).await.unwrap();

    let fast = ice
        .grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .expect("fast path (summary + RLE fallback)");
    assert_eq!(
        as_map(&fast.to_rows()),
        datafusion_group_counts(&ice, "host").await
    );
}

#[tokio::test]
async fn summary_grouped_counts_shard_union_matches_full() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    for _ in 0..6 {
        ice.append_events(&[
            ev("h1", "a", "app:json"),
            ev("h2", "b", "sys:log"),
            ev("h3", "a", "app:json"),
        ])
        .await
        .unwrap();
    }
    let full = as_map(
        &ice.grouped_counts_with_summary("events", "source", None, None)
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );
    let mut union: BTreeMap<Option<String>, i64> = BTreeMap::new();
    for index in 0..3 {
        let part = ice
            .grouped_counts_with_summary(
                "events",
                "source",
                Some(ScanShard { index, count: 3 }),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        part.for_each(|k, v| {
            *union.entry(k.map(str::to_string)).or_default() += v as i64;
        });
    }
    assert_eq!(
        union, full,
        "per-shard partials must sum to the full result"
    );
}

#[tokio::test]
async fn summary_survives_reclustering() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    // ≥ min_files_per_partition (4) small files so a re-cluster pass merges them
    // into one file — the production shape where the fast path must keep working.
    for _ in 0..5 {
        ice.append_events(&[
            ev("h1", "a", "app:json"),
            ev("h2", "b", "sys:log"),
            ev("h1", "c", "app:json"),
        ])
        .await
        .unwrap();
    }
    let oracle_before = datafusion_group_counts(&ice, "source").await;

    // Re-cluster: tiny bins go through the in-RAM concat write path, which
    // re-stamps the per-file group-count summary on the merged output.
    let ident = ice.events_table_ident().clone();
    let stats = ice
        .recluster_pass(&ident, BLOOM_FILTER_COLUMNS, ReclusterPolicy::default())
        .await
        .unwrap();
    let files_removed: usize = stats.iter().map(|s| s.files_removed).sum();
    assert!(
        files_removed >= 4,
        "re-cluster should have merged the small files"
    );

    let fast = ice
        .grouped_counts_with_summary("events", "source", None, None)
        .await
        .unwrap()
        .expect("fast path after re-clustering");
    let after = datafusion_group_counts(&ice, "source").await;
    assert_eq!(
        as_map(&fast.to_rows()),
        after,
        "summary matches DataFusion after re-cluster"
    );
    assert_eq!(after, oracle_before, "re-cluster conserves rows");
}

#[tokio::test]
async fn snapshot_aggregate_maintained_and_survives_reclustering() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    // 5 appends x 3 rows; source = a,b,a -> a=10, b=5 over 15 rows.
    for _ in 0..5 {
        ice.append_events(&[
            ev("h1", "a", "app:json"),
            ev("h2", "b", "sys:log"),
            ev("h1", "a", "app:json"),
        ])
        .await
        .unwrap();
    }
    // The table-level aggregate is maintained in the snapshot property on append.
    let agg = ice
        .table_group_counts_summary("events")
        .await
        .unwrap()
        .expect("snapshot aggregate present after appends");
    assert_eq!(
        agg.column_total("source"),
        Some(15),
        "Tier-1 guard: total == rows"
    );
    let from_agg: BTreeMap<Option<String>, i64> = agg
        .column_rows("source")
        .unwrap()
        .into_iter()
        .map(|(k, v)| (k, v as i64))
        .collect();
    assert_eq!(from_agg, datafusion_group_counts(&ice, "source").await);

    // Re-cluster preserves rows, so it carries the aggregate forward unchanged.
    let ident = ice.events_table_ident().clone();
    let stats = ice
        .recluster_pass(&ident, BLOOM_FILTER_COLUMNS, ReclusterPolicy::default())
        .await
        .unwrap();
    assert!(
        stats.iter().map(|s| s.files_removed).sum::<usize>() >= 4,
        "re-cluster merged files"
    );
    let agg2 = ice
        .table_group_counts_summary("events")
        .await
        .unwrap()
        .expect("snapshot aggregate survives re-cluster carry-forward");
    assert_eq!(
        agg2.column_total("source"),
        Some(15),
        "carried forward unchanged"
    );
    let from_agg2: BTreeMap<Option<String>, i64> = agg2
        .column_rows("source")
        .unwrap()
        .into_iter()
        .map(|(k, v)| (k, v as i64))
        .collect();
    assert_eq!(from_agg2, datafusion_group_counts(&ice, "source").await);
}
