//! A leveled pass must compact EVERY due level, not just the most-pressured one.
//!
//! The compactor's per-tier cadence hands `recluster_pass_leveled` a *list* of
//! levels whose interval has elapsed, and then stamps `level_last_run` for all
//! of them ("the levels this pass covered"). The planner used to `max_by_key` a
//! single level out of that list, so the rest waited a full interval having done
//! no work — and a level that kept winning the pressure comparison could starve
//! its neighbours while their tickers were repeatedly reset.
//!
//! It also left bin concurrency with nothing to do: the 2026-08-05 200G round
//! ran exactly one bin in 13 of 14 productive passes.
//!
//! Own test binary: it installs a process-global metrics recorder to read the
//! per-level compaction counter.

use chrono::{TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use siglake_core::Event;
use siglake_storage::iceberg::{
    IcebergContext, LevelPolicy, LeveledPassOptions, ReclusterPolicy, BLOOM_FILTER_COLUMNS,
};

/// Levels that ran a compaction this pass, read from
/// `siglake_compactor_level_compactions_total{level}`.
fn levels_compacted(snapshotter: &metrics_util::debugging::Snapshotter) -> Vec<String> {
    let mut out: Vec<String> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(k, _, _, v)| {
            k.key().name() == "siglake_compactor_level_compactions_total"
                && matches!(v, DebugValue::Counter(c) if *c > 0)
        })
        .filter_map(|(k, _, _, _)| {
            k.key()
                .labels()
                .find(|l| l.key() == "level")
                .map(|l| l.value().to_string())
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

fn ev(secs: i64, pad: &str) -> Event {
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: "h1".into(),
        source: "src".into(),
        sourcetype: "app:json".into(),
        index: "main".into(),
        raw: format!("event at {secs} {pad}"),
        attributes: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn leveled_pass_compacts_every_due_level() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::set_global_recorder(recorder).expect("install recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let ident = ice.events_table_ident().clone();
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    // Two size classes, each with enough files to meet the trigger, separated
    // enough that a level ceiling sits cleanly between them. A Parquet file
    // carries ~150 KB of fixed footer/bloom overhead regardless of row count,
    // so the big class needs real, *incompressible* content — a repeated pad
    // string compresses to nothing and both classes come out the same size.
    // Time ranges are disjoint per file so the packer can seal bins at gaps.
    let varied = |r: i64| {
        let mut s = String::with_capacity(72);
        let mut x = (r as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x5EED;
        for _ in 0..4 {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            s.push_str(&format!("{x:016x}"));
        }
        s
    };
    for f in 0..8i64 {
        ice.append_events(&[ev(base + f, "s")]).await.unwrap();
    }
    for f in 0..8i64 {
        let start = base + 1000 + f * 1000;
        let batch: Vec<Event> = (0..20_000)
            .map(|r| ev(start + r % 900, &varied(f * 100_000 + r)))
            .collect();
        ice.append_events(&batch).await.unwrap();
    }

    let live = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(live.len(), 16, "expected 8 small + 8 big files");
    let mut sizes: Vec<u64> = live.iter().map(|f| f.file_size_in_bytes()).collect();
    sizes.sort_unstable();
    let small_max = sizes[7];
    let big_min = sizes[8];
    assert!(
        big_min > small_max * 2,
        "fixture must separate the two size classes: small_max={small_max} big_min={big_min}"
    );
    // Ceiling between the classes => small files are L0, big files are L1.
    let split = (small_max + big_min) / 2;
    let levels = LevelPolicy {
        level_ceilings: vec![split, big_min * 64, big_min * 512],
        trigger_files: 8,
        max_fanin: 64,
        ..LevelPolicy::default()
    };
    let policy = ReclusterPolicy {
        max_files_per_pass: 64,
        max_bins_per_pass: 16,
        ..ReclusterPolicy::default()
    };

    // Both levels are due — exactly what the per-tier cadence passes when both
    // intervals have elapsed.
    let opts = LeveledPassOptions {
        allowed_levels: Some(vec![0, 1]),
        ..LeveledPassOptions::default()
    };
    let rows_before: u64 = live.iter().map(|f| f.record_count()).sum();
    let stats = ice
        .recluster_pass_leveled(&ident, BLOOM_FILTER_COLUMNS, &levels, policy, &opts)
        .await
        .expect("leveled pass");

    let compacted = levels_compacted(&snapshotter);
    assert!(
        compacted.len() >= 2,
        "a pass with two due levels must compact both; compacted levels = {compacted:?}, \
         bins = {}",
        stats.len()
    );
    assert!(
        compacted.contains(&"0".to_string()) && compacted.contains(&"1".to_string()),
        "expected both L0 and L1 to compact, got {compacted:?}"
    );

    // The pass must still conserve rows across everything it rewrote.
    let after = ice.live_data_files(&ident).await.unwrap();
    let rows_after: u64 = after.iter().map(|f| f.record_count()).sum();
    assert_eq!(rows_before, rows_after, "pass must conserve rows");
    assert!(
        after.len() < 16,
        "pass must reduce the file count, got {}",
        after.len()
    );
}
