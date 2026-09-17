use std::sync::Arc;

use chrono::{Duration, NaiveTime, TimeZone, Utc};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::OrderedMergeGlobalBudget;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

const MERGE_PARTITIONS: &str = "siglake_query_scan_ordered_merge_partitions_total";

/// One recorder per process — `install` refuses a second — and one planned
/// query at a time under it: both tests here plan ordered scans over the same
/// fixture shape, and the over-budget one asserts a counter stays at ZERO, so
/// the sibling's advertised merge must not be in flight. `Snapshotter::snapshot`
/// drains the registry, so the snapshot a gate holder takes is its own.
static METRICS: std::sync::LazyLock<(tokio::sync::Mutex<()>, Snapshotter)> =
    std::sync::LazyLock::new(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install debugging recorder");
        (tokio::sync::Mutex::new(()), snapshotter)
    });

fn counter_sum(snapshot: &SnapshotVec, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && label.is_none_or(|(lk, lv)| {
                    key.key().labels().any(|l| l.key() == lk && l.value() == lv)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

/// A query context whose ordered scans are held to `fan_in` total overlap
/// streams. Explicit per-session config rather than
/// `SIGLAKE_ORDERED_MERGE_GLOBAL_FANIN`: the two tests in this binary need two
/// different budgets and run on parallel threads, so a process-wide env var
/// would let either test plan under the other's value.
fn context_with_global_budget(target_partitions: usize, fan_in: usize) -> SessionContext {
    let ctx = siglake_storage::session_context_with_target_partitions(Some(target_partitions));
    let mut state = ctx.state();
    state
        .config_mut()
        .set_extension(Arc::new(OrderedMergeGlobalBudget { fan_in }));
    SessionContext::new_with_state(state)
}

fn collect_ts(batches: Vec<arrow_array::RecordBatch>) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let a = siglake_core::column_nanos(b.column(0)).unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

async fn overlap_fixture() -> (tempfile::TempDir, chrono::DateTime<Utc>, IcebergContext) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let mk = |offs: &[i64]| -> Vec<Event> {
        offs.iter()
            .map(|&s| {
                let mut e = Event::now(format!("row @{s}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    for offs in [[0, 100], [1, 101], [2, 102], [3, 103], [4, 104], [5, 105]] {
        ice.append_events(&mk(&offs)).await.unwrap();
    }
    (tmp, base, ice)
}

#[tokio::test]
async fn overlapping_partitions_over_global_budget_refuse_ordering_and_stay_correct() {
    let (gate, snapshotter) = &*METRICS;
    let _held = gate.lock().await;
    snapshotter.snapshot();

    let (_tmp, base, ice) = overlap_fixture().await;
    let ctx = context_with_global_budget(3, 4);
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" ASC LIMIT 4")
        .await
        .unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        plan_str.contains("SortExec"),
        "global overlap fan-in should refuse advertised ordering and keep a SortExec:\n{plan_str}"
    );

    let want: Vec<i64> = [0, 1, 2, 3]
        .iter()
        .map(|s| {
            (base + Duration::seconds(*s))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();
    assert_eq!(collect_ts(df.collect().await.unwrap()), want);

    let snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter_sum(
            &snapshot,
            "siglake_query_scan_output_ordering_total",
            Some(("outcome", "global_fan_in")),
        ) > 0,
        "global fan-in refusal should increment the dedicated outcome metric"
    );
    // #4366: the arrangement this scan built was discarded by the gate above,
    // so no overlap partition was ever merged. Pre-fix the counter was charged
    // while the arrangement was built and read 1 here.
    assert_eq!(
        counter_sum(&snapshot, MERGE_PARTITIONS, None),
        0,
        "a plan refused on the global fan-in budget must not report an overlap merge"
    );
}

#[tokio::test]
async fn overlapping_partitions_within_global_budget_still_advertise_ordering() {
    // Layered merges (26ac4f0) made ordered fan-in = overlap DEPTH: the
    // fixture's 6 mutually-overlapping files need 6 streams no matter how
    // they split across partitions, so "within budget" means a budget >= 6
    // (a budget of 4 correctly refuses under the depth model — that is the
    // over-budget test's job).
    let (gate, snapshotter) = &*METRICS;
    let _held = gate.lock().await;
    snapshotter.snapshot();

    let (_tmp, base, ice) = overlap_fixture().await;
    let ctx = context_with_global_budget(4, 8);
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" ASC LIMIT 4")
        .await
        .unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        plan_str.contains("SortPreservingMergeExec"),
        "overlap streams at the global budget should still advertise ordering:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("SortExec"),
        "overlap streams at the global budget should not need a blocking sort:\n{plan_str}"
    );

    let want: Vec<i64> = [0, 1, 2, 3]
        .iter()
        .map(|s| {
            (base + Duration::seconds(*s))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();
    assert_eq!(collect_ts(df.collect().await.unwrap()), want);

    // Positive control for the over-budget zero above: an advertised plan does
    // charge one increment per overlap partition it merges.
    assert!(
        counter_sum(&snapshotter.snapshot().into_vec(), MERGE_PARTITIONS, None) > 0,
        "an advertised overlap arrangement must report its merge partitions"
    );
}
