//! The unfiltered scan's exact timestamp statistics come from a direct
//! manifest walk. Cache that pure result by immutable snapshot so repeated
//! browse planning does not reload and reparse every manifest.

use std::sync::Arc;

use arrow_array::TimestampMicrosecondArray;
use chrono::{TimeZone, Utc};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::{OrderedScanLimit, PreferredScanOrder};

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn event(secs: i64) -> Event {
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: "h1".into(),
        source: "src".into(),
        sourcetype: "app:json".into(),
        index: "main".into(),
        raw: format!("row {secs}"),
        attributes: None,
    }
}

fn manifest_reads(snapshot: &SnapshotVec) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "siglake_object_store_reads_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "phase" && label.value() == "manifest")
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            _ => 0,
        })
        .sum()
}

fn browse_context() -> SessionContext {
    let base = siglake_storage::session_context_with_order(
        Some(8),
        None,
        Some(PreferredScanOrder { descending: true }),
    );
    let mut state = base.state();
    state
        .config_mut()
        .set_extension(Arc::new(OrderedScanLimit { limit: 100 }));
    SessionContext::new_with_state(state)
}

async fn physical_plan(ctx: &SessionContext, sql: &str) -> String {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let text = displayable(plan.as_ref()).indent(true).to_string();
    text
}

#[tokio::test]
async fn unchanged_snapshot_reuses_global_timestamp_bounds() {
    // Make the manifest counter describe physical reads in this test rather
    // than byte-cache hits. This test owns its process and the global setting.
    siglake_storage::configure_object_cache_max_bytes(0);
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = 1_700_000_000i64;
    for offset in 0..4 {
        ice.append_events(&[event(base + offset)]).await.unwrap();
    }

    let ctx = browse_context();
    ice.register_with_datafusion(&ctx).await.unwrap();
    snapshotter.snapshot(); // discard fixture and registration reads

    let browse = "SELECT timestamp, raw FROM events ORDER BY timestamp DESC LIMIT 100";
    let first_plan = physical_plan(&ctx, browse).await;
    assert!(
        first_plan.contains("SiglakeIcebergTableScan partitions:[1]"),
        "fixture must take the advertised ordered path:\n{first_plan}"
    );
    let first_reads = manifest_reads(&snapshotter.snapshot().into_vec());
    assert!(
        first_reads > 0,
        "the first plan must populate timestamp bounds from manifests"
    );

    let second_plan = physical_plan(&ctx, browse).await;
    assert!(
        second_plan.contains("SiglakeIcebergTableScan partitions:[1]"),
        "the cached plan must retain ordered advertisement:\n{second_plan}"
    );
    assert_eq!(
        manifest_reads(&snapshotter.snapshot().into_vec()),
        0,
        "an unchanged snapshot must not reload manifests on its second plan"
    );

    let filtered = physical_plan(
        &ctx,
        "SELECT timestamp, raw FROM events WHERE raw LIKE '%row%' LIMIT 100",
    )
    .await;
    assert!(
        filtered.contains("SiglakeIcebergTableScan"),
        "filtered control must still plan a scan:\n{filtered}"
    );
    assert_eq!(
        manifest_reads(&snapshotter.snapshot().into_vec()),
        0,
        "the filtered control must not request global timestamp bounds"
    );

    let stats_plan = physical_plan(&ctx, "SELECT min(timestamp), max(timestamp) FROM events").await;
    assert!(
        stats_plan.contains("PlaceholderRowExec")
            && !stats_plan.contains("SiglakeIcebergTableScan"),
        "min/max must still resolve from exact statistics:\n{stats_plan}"
    );

    // A commit creates a new cache key. Re-register because the provider is a
    // static snapshot, then confirm both a fresh walk and the new exact bound.
    ice.append_events(&[event(base + 100)]).await.unwrap();
    let refreshed = browse_context();
    ice.register_with_datafusion(&refreshed).await.unwrap();
    snapshotter.snapshot(); // discard commit and registration reads

    let df = refreshed
        .sql("SELECT min(timestamp) AS lo, max(timestamp) AS hi FROM events")
        .await
        .unwrap();
    let refreshed_plan = displayable(df.clone().create_physical_plan().await.unwrap().as_ref())
        .indent(true)
        .to_string();
    assert!(
        refreshed_plan.contains("PlaceholderRowExec")
            && !refreshed_plan.contains("SiglakeIcebergTableScan"),
        "new-snapshot min/max must still resolve from statistics:\n{refreshed_plan}"
    );
    assert!(
        manifest_reads(&snapshotter.snapshot().into_vec()) > 0,
        "a new snapshot must populate its own timestamp bounds"
    );

    let batches = df.collect().await.unwrap();
    let lo = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
        .value(0);
    let hi = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
        .value(0);
    assert_eq!(lo, base * 1_000_000);
    assert_eq!(hi, (base + 100) * 1_000_000);
}
