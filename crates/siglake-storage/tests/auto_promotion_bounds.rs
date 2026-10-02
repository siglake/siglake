//! Task #3052: the opt-out and the column ceiling of opt-in WS-7 attribute
//! auto-promotion, at the table.
//!
//! Auto-promotion is the one path in the system that mutates a table's SCHEMA
//! with no operator in the loop: `auto_promote_hot_keys` decides from a bounded
//! sample and calls `declare_promotions_for`, which records the property and
//! widens the schema additively. Additive widening cannot be undone, so the
//! assertions that matter most here are the negative ones: when the pass
//! declines to promote, the table must come back identical in schema and
//! properties, not merely without a return value.
//!
//! The selection arithmetic is unit-tested in `iceberg.rs`
//! (`auto_promotion_sampling_tests`). Mixed-type keys, backfill convergence
//! and query equivalence need the `attr_get` UDF, which lives in the query
//! server: `siglake-query-server`'s `auto_promotion_bounds.rs`,
//! `promoted_prune.rs` and `typed_promotion.rs`.

use iceberg::table::Table;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, LevelPolicy, LeveledPassOptions, ReclusterPolicy};

type Snapshot = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

/// The sample bound every test here uses: one pass over everything these
/// fixtures write, so a verdict is about the threshold and not about which
/// files the sample happened to reach.
const FILES: usize = 4;
const ROWS: usize = 4096;

const SAMPLE_READS: &str = "siglake_auto_promotion_sample_reads_total";
const SAMPLE_BYTES: &str = "siglake_auto_promotion_sample_bytes_total";
const SAMPLE_DURATION: &str = "siglake_auto_promotion_pass_duration_seconds";
const BACKFILL_FILES: &str = "siglake_compactor_promotion_backfill_files_total";
const BACKFILL_BYTES_IN: &str = "siglake_compactor_promotion_backfill_bytes_in_total";
const BACKFILL_BYTES_OUT: &str = "siglake_compactor_promotion_backfill_bytes_out_total";
const BACKFILL_DURATION: &str = "siglake_compactor_promotion_backfill_duration_seconds";

fn counter(snapshot: &Snapshot, name: &str, phase: Option<&str>) -> Option<u64> {
    let values = snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && phase.is_none_or(|phase| {
                    key.key()
                        .labels()
                        .any(|label| label.key() == "phase" && label.value() == phase)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            other => panic!("{name} has wrong metric type: {other:?}"),
        })
        .collect::<Vec<_>>();
    (!values.is_empty()).then(|| values.into_iter().sum())
}

fn histogram_samples(snapshot: &Snapshot, name: &str) -> Option<usize> {
    snapshot
        .iter()
        .find(|(key, _, _, _)| key.key().name() == name)
        .map(|(_, _, _, value)| match value {
            DebugValue::Histogram(samples) => samples.len(),
            other => panic!("{name} has wrong metric type: {other:?}"),
        })
}

fn assert_unrecorded(snapshot: &Snapshot, names: &[&str]) {
    for name in names {
        for (_, _, _, value) in snapshot
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == *name)
        {
            match value {
                DebugValue::Counter(value) => {
                    assert_eq!(*value, 0, "{name} must not increment")
                }
                DebugValue::Histogram(samples) => {
                    assert!(samples.is_empty(), "{name} must not record a sample")
                }
                other => panic!("{name} has wrong metric type: {other:?}"),
            }
        }
    }
}

async fn events_table(ice: &IcebergContext) -> Table {
    ice.catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap()
}

/// The table's schema column names and its promotion property — the whole of
/// what a promotion changes.
async fn schema_fingerprint(ice: &IcebergContext) -> (Vec<String>, Option<String>) {
    let table = events_table(ice).await;
    let names = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    let property = table
        .metadata()
        .properties()
        .get(siglake_core::PROMOTED_PROPERTY_KEY)
        .cloned();
    (names, property)
}

fn events(n: usize, attributes: impl Fn(usize) -> String) -> Vec<Event> {
    (0..n)
        .map(|i| Event::now(format!("row {i}")).with_attributes(Some(attributes(i))))
        .collect()
}

/// The off switches emit no sampling work, while a successful bounded pass
/// attributes its reads and a promotion backfill attributes only its own bin.
///
/// This is one test because the metrics recorder and the Iceberg byte counter
/// are process-global; each snapshot drains the preceding phase.
#[tokio::test(flavor = "multi_thread")]
async fn auto_promotion_bounds_and_costs_are_attributed() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::set_global_recorder(recorder).expect("install recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let attrs: String = format!(
        "{{{}}}",
        (0..20)
            .map(|key| format!(r#""key{key}":"v""#))
            .collect::<Vec<_>>()
            .join(",")
    );
    for _ in 0..FILES {
        ice.append_events(&events(25, |_| attrs.clone()))
            .await
            .unwrap();
    }
    let _ = snapshotter.snapshot();

    let before = schema_fingerprint(&ice).await;
    assert_eq!(before.1, None, "a fresh table carries no promotions");

    let disabled = ice
        .auto_promote_hot_keys_for_report(ice.events_table_ident(), 0.0, 16, FILES, ROWS)
        .await
        .unwrap();
    assert_eq!(
        disabled.candidates, None,
        "disabled is not a zero-key sample"
    );
    assert!(disabled.promoted.is_empty(), "{disabled:?}");
    assert_unrecorded(
        &snapshotter.snapshot().into_vec(),
        &[SAMPLE_READS, SAMPLE_BYTES, SAMPLE_DURATION],
    );
    assert_eq!(
        schema_fingerprint(&ice).await,
        before,
        "the default configuration must not widen a schema"
    );

    let zero_cap = ice
        .auto_promote_hot_keys_for_report(ice.events_table_ident(), 0.5, 0, FILES, ROWS)
        .await
        .unwrap();
    assert_eq!(zero_cap.candidates, None, "a zero cap does not sample");
    assert!(zero_cap.promoted.is_empty(), "{zero_cap:?}");
    assert_unrecorded(
        &snapshotter.snapshot().into_vec(),
        &[SAMPLE_READS, SAMPLE_BYTES, SAMPLE_DURATION],
    );
    assert_eq!(schema_fingerprint(&ice).await, before);

    let bytes_before = iceberg::arrow::object_store_bytes_read();
    let report = ice
        .auto_promote_hot_keys_for_report(ice.events_table_ident(), 0.5, 5, FILES, ROWS)
        .await
        .unwrap();
    let bytes_after = iceberg::arrow::object_store_bytes_read();
    let sample_metrics = snapshotter.snapshot().into_vec();
    assert_eq!(report.candidates, Some(20));
    assert_eq!(report.declined.len(), 15);
    assert_eq!(report.promoted.len(), 5, "{report:?}");
    assert_eq!(report.sample_files, FILES);
    let reads = counter(&sample_metrics, SAMPLE_READS, None).expect("sample reads series");
    assert!(reads >= FILES as u64, "reads={reads} files={FILES}");
    assert_eq!(reads, report.reads);
    let bytes = ["footer", "index", "data"]
        .into_iter()
        .map(|phase| counter(&sample_metrics, SAMPLE_BYTES, Some(phase)).unwrap_or(0))
        .sum::<u64>();
    assert!(bytes > 0, "a sampled pass must read Parquet bytes");
    assert_eq!(bytes, bytes_after - bytes_before);
    assert_eq!(bytes, report.bytes);
    assert_eq!(histogram_samples(&sample_metrics, SAMPLE_DURATION), Some(1));

    let (after, property) = schema_fingerprint(&ice).await;
    assert_eq!(
        after.len(),
        before.0.len() + 5,
        "the schema grew past the ceiling: {after:?}"
    );
    assert_eq!(
        siglake_core::promoted_columns_from_property(property.as_deref()).len(),
        5
    );

    // At the ceiling, the next pass is a no-op — it does not even re-declare
    // the list it already holds.
    let again = ice
        .auto_promote_hot_keys_for_report(ice.events_table_ident(), 0.5, 5, FILES, ROWS)
        .await
        .unwrap();
    assert_eq!(again.candidates, None, "an at-cap pass does not sample");
    assert!(again.promoted.is_empty(), "{again:?}");
    assert_unrecorded(
        &snapshotter.snapshot().into_vec(),
        &[SAMPLE_READS, SAMPLE_BYTES, SAMPLE_DURATION],
    );
    assert_eq!(schema_fingerprint(&ice).await.0, after);

    // Backfill positive control: two files predate the declaration, so one
    // backfill bin rewrites exactly those two files and flips the gate.
    let backfill_tmp = tempfile::tempdir().unwrap();
    let backfill = IcebergContext::open(&backfill_tmp.path().join("warehouse"))
        .await
        .unwrap();
    let backfill_attrs = |i: usize| format!(r#"{{"k8s.namespace":"ns-{}"}}"#, i % 4);
    const BACKFILL_FILES_COUNT: usize = 2;
    for _ in 0..BACKFILL_FILES_COUNT {
        backfill
            .append_events(&events(100, backfill_attrs))
            .await
            .unwrap();
    }
    let newly = backfill
        .auto_promote_hot_keys(0.5, 16, FILES, ROWS)
        .await
        .unwrap();
    assert_eq!(newly.len(), 1, "{newly:?}");
    assert!(!backfill.ensure_promotion_backfill_property().await.unwrap());
    let ident = backfill.events_table_ident().clone();
    let pre_pass = backfill.live_data_files(&ident).await.unwrap();
    assert_eq!(pre_pass.len(), BACKFILL_FILES_COUNT);
    let expected_bytes_in = pre_pass
        .iter()
        .map(|file| file.file_size_in_bytes())
        .sum::<u64>();
    let _ = snapshotter.snapshot();
    let stats = backfill
        .recluster_pass_leveled(
            &ident,
            &["host"],
            &LevelPolicy {
                trigger_files: 100,
                max_merge_gen: 0,
                ..Default::default()
            },
            ReclusterPolicy::default(),
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(stats.len(), 1, "one backfill bin should cover the fixture");
    let backfill_metrics = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter(&backfill_metrics, BACKFILL_FILES, None),
        Some(BACKFILL_FILES_COUNT as u64)
    );
    assert_eq!(
        counter(&backfill_metrics, BACKFILL_BYTES_IN, None),
        Some(expected_bytes_in)
    );
    assert!(
        counter(&backfill_metrics, BACKFILL_BYTES_OUT, None).is_some_and(|bytes| bytes > 0),
        "backfill output bytes must be published"
    );
    assert_eq!(
        histogram_samples(&backfill_metrics, BACKFILL_DURATION),
        Some(1)
    );
    assert_eq!(
        counter(
            &backfill_metrics,
            "siglake_compactor_promotion_backfill_bins_total",
            None
        ),
        Some(1)
    );
    assert!(
        backfill.ensure_promotion_backfill_property().await.unwrap(),
        "the completed rewrite must flip the backfill property"
    );

    // Negative control: without a declaration the ordinary count-triggered
    // merge still commits, but none of the backfill-only series exist.
    let control_tmp = tempfile::tempdir().unwrap();
    let control = IcebergContext::open(&control_tmp.path().join("warehouse"))
        .await
        .unwrap();
    for _ in 0..2 {
        control
            .append_events(&events(20, |_| r#"{"ordinary":"value"}"#.to_string()))
            .await
            .unwrap();
    }
    let control_ident = control.events_table_ident().clone();
    let _ = snapshotter.snapshot();
    let control_stats = control
        .recluster_pass_leveled(
            &control_ident,
            &["host"],
            &LevelPolicy {
                trigger_files: 2,
                max_merge_gen: 0,
                ..Default::default()
            },
            ReclusterPolicy::default(),
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert!(!control_stats.is_empty(), "ordinary merge must commit");
    let control_metrics = snapshotter.snapshot().into_vec();
    assert_unrecorded(
        &control_metrics,
        &[
            BACKFILL_FILES,
            BACKFILL_BYTES_IN,
            BACKFILL_BYTES_OUT,
            BACKFILL_DURATION,
        ],
    );
    assert_eq!(
        counter(
            &control_metrics,
            "siglake_compactor_bins_committed_total",
            None
        ),
        Some(control_stats.len() as u64)
    );
}
