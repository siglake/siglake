//! Bounded-fan-in tiered streaming recluster. A single overlapping cluster can
//! reach hundreds of files (a whole overlapping day at 1TB); the streaming merge
//! opens one Parquet decoder per file, so an unbounded fan-in can OOM. The merge
//! tiers through `SIGLAKE_RECLUSTER_MERGE_FANIN`: intermediate rounds merge
//! chunks of ≤fan-in into transient sorted Parquet, then merge those, until the
//! final round writes the real (footer-stamped, time-disjoint) output. This test
//! forces a tiny fan-in so many-file reclusters exercise the multi-tier path, and
//! checks the invariants: rows conserved, the final output carries group-count
//! footers (so Tier-1/Tier-2 survive), and no intermediate files are left behind.
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
async fn tiered_streaming_recluster_conserves_rows_and_disjoins() {
    // Force the streaming path and a tiny fan-in so 8 files tier through 2+
    // rounds (8 -> chunks of 2 -> 4 -> 2 -> final). The forced tiered option
    // pins the escape hatch; the default >fan-in path is covered by
    // `recluster_page_bounded_merge.rs`.
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    // 8 appends, each covering the SAME 100s range [base, base+99] → 8 files that
    // all OVERLAP in time (the pathological cluster the tiered merge must disjoin).
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
                force_tiered: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("tiered streaming recluster");
    assert_eq!(stats.rows as u64, total, "tiered merge conserves rows");

    // The final output's group-count footers survive (else this falls to a scan).
    let counts = {
        let rows = ice
            .grouped_counts_with_summary("events", "sourcetype", None, None)
            .await
            .unwrap()
            .expect("grouped counts after tiered recluster");
        let mut v = rows.to_rows();
        v.sort();
        v
    };
    // 60% app:json, 40% syslog of 800.
    assert_eq!(
        counts,
        vec![(Some("app:json".into()), 480), (Some("syslog".into()), 320)]
    );

    // Intermediate tier files were deleted (not leaked). recluster_files swaps the
    // manifest via rewrite_files, which un-references the 8 originals but doesn't
    // physically delete them (the orphan GC does), so on disk we expect exactly the
    // 8 GC-pending originals + the committed final output — NOT the ~6 intermediate
    // tier files, which `delete_intermediate_files` removed after each tier.
    let out = ice.live_data_files(&ident).await.unwrap();
    let warehouse = tmp.path().join("warehouse");
    let on_disk = walk_parquet(&warehouse.join("siglake/events/data")).len();
    assert_eq!(
        on_disk,
        8 + out.len(),
        "expected 8 GC-pending originals + {} final output file(s); leaked tier intermediates would add more",
        out.len()
    );
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
