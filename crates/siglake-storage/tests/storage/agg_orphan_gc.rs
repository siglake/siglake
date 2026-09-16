//! The orphan GC must not eat the group-count aggregate.
//!
//! Its rule is "delete everything under `data/` and `metadata/` that the
//! Iceberg manifest tree does not reach", and the aggregate's objects are
//! invisible to that tree by construction — they are siglake's, not Iceberg's.
//! So they look exactly like garbage, and the failure is silent in the worst
//! way: deleting them breaks no query, it only makes every high-cardinality
//! `GROUP BY` fall back to a full scan, with nothing in the logs but a GC
//! report saying it reclaimed some files.
//! These objects only exist above the inline ceiling.

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::{aggregate_prefix_rel_path, GcOptions, IcebergContext};

const DISTINCT_HOSTS: usize = 9_000;
const ROWS: usize = 9_000;

#[tokio::test]
async fn the_group_count_aggregate_survives_an_orphan_gc() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap().with_tuning(
        siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(2_000_000),
            ..Default::default()
        },
    );
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    let evs: Vec<Event> = (0..ROWS)
        .map(|i| Event {
            timestamp: Utc.timestamp_opt(base + i as i64, 0).single().unwrap(),
            host: format!("host-{:06}", i % DISTINCT_HOSTS),
            source: "src".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: "req".into(),
            attributes: None,
        })
        .collect();
    for chunk in evs.chunks(ROWS / 2) {
        ice.append_events(chunk).await.unwrap();
    }
    // A fold, so BOTH shapes exist: the wide base object and delta objects that
    // it has already absorbed but not yet deleted.
    ice.fold_group_count_deltas(1).await.unwrap();

    let ident = ice.events_table_ident().clone();
    // The artifacts sit under this incarnation's own prefix (#2919), which the
    // GC must recognize as siglake-owned exactly as it did the flat names.
    let agg_dir: std::path::PathBuf = {
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let location = table
            .metadata()
            .location()
            .trim_start_matches("file://")
            .to_string();
        let uuid = table.metadata().uuid().to_string();
        std::path::Path::new(&location).join(aggregate_prefix_rel_path(&uuid))
    };
    let delta_dir = agg_dir.join("siglake-agg-deltas");
    let before: Vec<std::path::PathBuf> = std::fs::read_dir(&delta_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert!(!before.is_empty(), "deltas exist to be at risk");
    assert!(agg_dir.join("siglake-agg-wide.json").exists());

    // min_age ZERO: no safety window to hide behind. Everything the GC
    // considers an orphan, it takes.
    let report = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: std::time::Duration::ZERO,
                apply: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        report.deleted, 0,
        "a healthy table has no orphans; the aggregate is not garbage"
    );

    for p in &before {
        assert!(p.exists(), "delta deleted by orphan GC: {}", p.display());
    }
    assert!(
        agg_dir.join("siglake-agg-wide.json").exists(),
        "wide base deleted by orphan GC"
    );
    assert!(agg_dir.join("siglake-aggregates.json").exists());

    // And the aggregate still answers — the point of keeping the objects.
    ice.invalidate_cached_table(&ident).await;
    assert_eq!(
        ice.table_group_counts_summary("events")
            .await
            .unwrap()
            .and_then(|g| g.column_total("host")),
        Some(ROWS as u64),
    );
}
