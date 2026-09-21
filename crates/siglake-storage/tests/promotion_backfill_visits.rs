//! Task #5057: `ensure_promotion_backfill_property` visits each table once.
//!
//! The wrapper checks the events table, then loops `list_indexes()`. But
//! `list_indexes` reports the events table among the indexes (no stored doc
//! mapping ⇒ `IndexConfig::builtin_events`, see
//! `index_manager.rs::index_config_from_table`) and
//! `index_table_ident("events")` IS `self.table_ident`, so events was checked
//! twice on every compactor pass. While the backfill is still incomplete the
//! second check is a second manifest walk over the same snapshot: the property
//! compare that short-circuits a completed table does not match, so
//! `live_data_files` runs again.
//!
//! The observable is `siglake_iceberg_table_cache_requests_total`:
//! `ensure_promotion_backfill_property_for` calls `cached_table_entry` exactly
//! once per table it visits, and nothing else in the pass touches the table
//! cache (`list_indexes` and `live_data_files` go straight to
//! `catalog().load_table`). So the counter's movement across one call is the
//! visit count, whatever the hit/refresh mix.
//!
//! Own test binary: it installs a metrics recorder, which is process-global.

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::index_config::IndexConfig;
use siglake_core::{Event, PromotedColumn, PromotedType};
use siglake_storage::iceberg::{
    IcebergContext, LevelPolicy, LeveledPassOptions, ReclusterPolicy, PROMOTION_BACKFILL_PROP,
};

/// Sum every series of `name`. `Snapshotter::snapshot` SWAPS counters to zero,
/// so one snapshot per phase is already the phase's delta — do not subtract a
/// baseline, drain first instead.
fn counter_delta(snap: &Snapshotter, name: &str) -> u64 {
    snap.snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == name)
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => c,
            _ => 0,
        })
        .sum()
}

const TABLE_CACHE_REQUESTS: &str = "siglake_iceberg_table_cache_requests_total";

#[tokio::test]
async fn promotion_backfill_check_visits_each_table_once() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};

    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    let _ = metrics::set_global_recorder(recorder);

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().to_path_buf();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );

    // Two files ingested with NO promotion declared, so the backfill starts
    // incomplete: the key lives only in the residual attributes JSON.
    let legacy = IcebergContext::open(&warehouse).await.unwrap();
    for k in 0..2i64 {
        let events: Vec<Event> = (0..10)
            .map(|j| {
                let mut e = Event::now(format!("row {k}-{j}"))
                    .with_attributes(Some(format!(r#"{{"k8s.pod":"pod-{k}"}}"#)));
                e.timestamp = base + Duration::seconds(k * 100 + j);
                e
            })
            .collect();
        legacy.append_events(&events).await.unwrap();
    }

    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(vec![PromotedColumn {
            attr_key: "k8s.pod".into(),
            name: "k8s_pod".into(),
            ty: PromotedType::Utf8,
        }]);
    ice.ensure_promoted_columns().await.unwrap();

    // A second table, so a skip that dropped the whole loop would show up.
    let mut extra = IndexConfig::builtin_events();
    extra.index_id = "logs_extra".to_string();
    ice.create_index(&extra).await.unwrap();

    let listed: Vec<String> = ice
        .list_indexes()
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.index_id)
        .collect();
    // Non-vacuity: the duplicate exists only because `list_indexes` reports
    // the events table, and the loop resolves it to `self.table_ident`.
    assert!(
        listed.iter().any(|id| id == "events"),
        "list_indexes must report the events table for this to be the duplicate \
         under test; it reported {listed:?}"
    );
    assert!(
        listed.iter().any(|id| id == "logs_extra"),
        "the index table must be listed; it reported {listed:?}"
    );
    let tables = listed.len() as u64;
    assert_eq!(tables, 2, "expected events + logs_extra, got {listed:?}");

    // Phase 1 — backfill INCOMPLETE. This is the expensive duplicate: the
    // property compare cannot short-circuit, so a second visit to events
    // re-walks its manifests.
    let _ = counter_delta(&snap, TABLE_CACHE_REQUESTS);
    assert!(
        !ice.ensure_promotion_backfill_property().await.unwrap(),
        "pre-promotion files are live; the property must not flip yet"
    );
    let visits = counter_delta(&snap, TABLE_CACHE_REQUESTS);
    assert_eq!(
        visits, tables,
        "incomplete backfill: expected one visit per table ({tables}), saw {visits}"
    );

    // Complete the backfill: rewrite the pre-promotion files so every live
    // file carries the promoted column.
    let ident = ice.events_table_ident().clone();
    let levels = LevelPolicy {
        trigger_files: 100, // count triggers never fire
        max_merge_gen: 0,
        ..Default::default()
    };
    let stats = ice
        .recluster_pass_leveled(
            &ident,
            &["host", "k8s_pod"],
            &levels,
            ReclusterPolicy::default(),
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert!(!stats.is_empty(), "backfill bin must fire");

    // Phase 2 — the flipping call. Still one visit per table.
    let _ = counter_delta(&snap, TABLE_CACHE_REQUESTS);
    assert!(
        ice.ensure_promotion_backfill_property().await.unwrap(),
        "every live file carries the promoted column; the property must flip"
    );
    let visits = counter_delta(&snap, TABLE_CACHE_REQUESTS);
    assert_eq!(
        visits, tables,
        "flipping call: expected one visit per table ({tables}), saw {visits}"
    );

    // Phase 3 — already flipped: the verdict is unchanged and still one visit
    // each, now all short-circuited on the property compare.
    let _ = counter_delta(&snap, TABLE_CACHE_REQUESTS);
    assert!(
        !ice.ensure_promotion_backfill_property().await.unwrap(),
        "the property is already at its desired value"
    );
    let visits = counter_delta(&snap, TABLE_CACHE_REQUESTS);
    assert_eq!(
        visits, tables,
        "already-flipped call: expected one visit per table ({tables}), saw {visits}"
    );

    let table = ice.catalog().load_table(&ident).await.unwrap();
    assert!(
        table
            .metadata()
            .properties()
            .contains_key(PROMOTION_BACKFILL_PROP),
        "completion property recorded"
    );
}
