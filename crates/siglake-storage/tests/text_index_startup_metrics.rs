//! #3969: a text query's per-file index startup has to be attributable from a
//! deployed pod's `/metrics`, not just from #3896's process-local test hooks.
//!
//! What a round needs to answer is WHICH stage a first-batch regression is in —
//! the load queue, the object-store read, the deserialization, or the postings
//! and row-selection work above the index — and whether the parsed index was
//! decoded again because the cache had thrown it away. This file holds the
//! emitters to that: one cold execution must produce a sample for every stage
//! and exactly one cache miss, and the warm execution after it must produce a
//! hit, no decode at all, and still a `selection` sample.
//!
//! Its own test binary: the parsed-index cache, the blob cache and the global
//! metrics recorder are all process-wide, and the counters here are attributed
//! to these two queries.

use std::collections::BTreeSet;

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

const STARTUP_SECONDS: &str = "siglake_iceberg_text_index_startup_seconds";
const LOOKUPS_TOTAL: &str = "siglake_iceberg_parsed_index_cache_lookups_total";

/// One recorder per process — `install` refuses a second — and one query at a
/// time under it: every counter here is process-wide and attributed to the
/// queries of the test holding the gate. The parsed-index cache is process-wide
/// too, so each test sets the budget it needs and puts it back.
static METRICS: std::sync::LazyLock<(tokio::sync::Mutex<()>, Snapshotter)> =
    std::sync::LazyLock::new(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install debugging recorder");
        (tokio::sync::Mutex::new(()), snapshotter)
    });

fn label(key: &metrics_util::CompositeKey, name: &str) -> Option<String> {
    key.key()
        .labels()
        .find(|entry| entry.key() == name)
        .map(|entry| entry.value().to_string())
}

fn counter_sum(snapshot: &SnapshotVec, name: &str, label_match: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && label_match.is_none_or(|(lk, lv)| {
                    key.key()
                        .labels()
                        .any(|entry| entry.key() == lk && entry.value() == lv)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            _ => 0,
        })
        .sum()
}

/// How many durations the stage histogram recorded in this snapshot, per
/// `(stage, storage)`. `Snapshotter::snapshot` drains the registry, so each
/// call reports one execution's worth.
fn stage_samples(snapshot: &SnapshotVec) -> std::collections::BTreeMap<(String, String), usize> {
    let mut out = std::collections::BTreeMap::new();
    for (key, _, _, value) in snapshot {
        if key.key().name() != STARTUP_SECONDS {
            continue;
        }
        let DebugValue::Histogram(samples) = value else {
            continue;
        };
        // A drained snapshot leaves the series behind with no samples, so an
        // empty one is a stage that did NOT run in this execution.
        if samples.is_empty() {
            continue;
        }
        let stage = label(key, "stage").expect("every stage sample carries `stage`");
        let storage = label(key, "storage").expect("every stage sample carries `storage`");
        *out.entry((stage, storage)).or_insert(0) += samples.len();
    }
    out
}

fn stages_seen(snapshot: &SnapshotVec) -> BTreeSet<String> {
    stage_samples(snapshot)
        .into_keys()
        .map(|(stage, _)| stage)
        .collect()
}

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

/// A warehouse whose raw index is forced out of the Parquet footer and into a
/// Puffin sidecar (`index_footer_max_bytes: 1`), which is the only form that
/// pays a `blob_fetch`: a footer-KV index arrives with the metadata the scan
/// has already read.
async fn puffin_indexed_warehouse(warehouse: &std::path::Path) -> IcebergContext {
    let ice = IcebergContext::open(warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_footer_max_bytes: Some(1),
            ..Default::default()
        });
    ice.append_events(&[
        Event::now("database timeout retry"),
        Event::now("healthy startup"),
        Event::now("database migration"),
    ])
    .await
    .unwrap();
    ice
}

#[tokio::test]
async fn a_cold_text_query_times_every_startup_stage_and_a_warm_one_decodes_nothing() {
    let (gate, snapshotter) = &*METRICS;
    let _held = gate.lock().await;
    // The shipped budget, whatever the other test left configured.
    iceberg::arrow::clear_text_index_cache_max_bytes();

    let tmp = tempfile::tempdir().unwrap();
    let ice = puffin_indexed_warehouse(&tmp.path().join("warehouse")).await;
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();

    // The decoded-file cache is off by default and this file never turns it on:
    // a hit there serves whole-file batches and never consults the index, which
    // would leave the second execution with nothing to attribute.
    let sql = "SELECT count(*) FROM events WHERE raw LIKE '%database%'";

    // Everything the append and the registration recorded is not this query's.
    let _ = snapshotter.snapshot();
    assert_eq!(count(&ctx, sql).await, 2, "cold result");
    let cold = snapshotter.snapshot().into_vec();

    let samples = stage_samples(&cold);
    assert_eq!(
        stages_seen(&cold),
        iceberg::arrow::TEXT_INDEX_STARTUP_STAGES
            .iter()
            .map(|stage| stage.to_string())
            .collect::<BTreeSet<_>>(),
        "a cold Puffin load must time all four stages: {samples:?}"
    );
    for ((stage, storage), count) in &samples {
        assert_eq!(
            storage, "puffin",
            "the forced-spill fixture has no footer-KV index: {stage} recorded {storage}"
        );
        assert_eq!(count, &1, "one sample per stage per file: {stage}");
    }
    assert_eq!(
        counter_sum(&cold, LOOKUPS_TOTAL, Some(("outcome", "miss"))),
        1,
        "a cold load is ONE miss, not one per cache lookup it makes"
    );
    assert_eq!(
        counter_sum(&cold, LOOKUPS_TOTAL, Some(("outcome", "hit"))),
        0,
        "nothing was warm yet"
    );

    // Warm: the parsed index is handed over before the load permit, so the
    // three load stages have nothing to record and `selection` still does.
    assert_eq!(count(&ctx, sql).await, 2, "warm result diverged");
    let warm = snapshotter.snapshot().into_vec();
    assert_eq!(
        stages_seen(&warm),
        BTreeSet::from(["selection".to_string()]),
        "a warm query pays selection and nothing else: {:?}",
        stage_samples(&warm)
    );
    assert_eq!(
        counter_sum(&warm, LOOKUPS_TOTAL, Some(("outcome", "hit"))),
        1,
        "the warm execution must report the parsed-cache hit"
    );
    assert_eq!(
        counter_sum(&warm, LOOKUPS_TOTAL, Some(("outcome", "miss"))),
        0,
        "nothing should have been decoded again"
    );
}

/// The byte bound's own arm. A budget below one parsed index admits nothing, so
/// every execution decodes again — which reads exactly like a cold cache unless
/// something counts it. `oversized` is the arm that says so.
#[tokio::test]
async fn an_index_over_the_whole_budget_reports_why_it_was_never_kept() {
    let (gate, snapshotter) = &*METRICS;
    let _held = gate.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let ice = puffin_indexed_warehouse(&tmp.path().join("warehouse")).await;
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let sql = "SELECT count(*) FROM events WHERE raw LIKE '%database%'";

    // One byte of budget: the index is admitted nowhere, but the lookups and
    // the decode still happen.
    siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
        parsed_index_max_bytes: 1,
        puffin_blob_max_bytes: 64 * 1024 * 1024,
    });
    let _ = snapshotter.snapshot();
    for round in ["first", "second"] {
        assert_eq!(count(&ctx, sql).await, 2, "{round} result");
    }
    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(&snapshot, LOOKUPS_TOTAL, Some(("outcome", "miss"))),
        2,
        "both executions must decode: nothing was ever kept"
    );
    assert_eq!(
        counter_sum(
            &snapshot,
            "siglake_iceberg_parsed_index_cache_evictions_total",
            Some(("reason", "oversized")),
        ),
        2,
        "each refused admission must name the bound that refused it"
    );

    // Leave the process's cache budgets as they were found.
    iceberg::arrow::clear_text_index_cache_max_bytes();
}

/// The panels read `rate()` over both counters, so their series have to exist
/// at 0 on a query server that has not served a text query yet — otherwise the
/// chart says "No data" where the truth is "nothing has happened", which is the
/// reading the panel exists to make (#3969). The label values are variables at
/// the emitters, so `scripts/check-chart.py` sees dynamic sites and cannot hold
/// the catalog to them; the reader's own exported vocabularies can.
#[test]
fn text_index_startup_series_are_preregistered() {
    let listed = |name: &str| -> BTreeSet<BTreeSet<(&'static str, &'static str)>> {
        siglake_core::metrics::QUERY_SERVER_ALERTED_COUNTERS
            .iter()
            .filter(|counter| counter.name == name)
            .flat_map(|counter| counter.series.iter())
            .map(|labels| labels.iter().copied().collect())
            .collect()
    };

    let lookups: BTreeSet<BTreeSet<(&str, &str)>> = iceberg::arrow::PARSED_INDEX_CACHE_OUTCOMES
        .iter()
        .flat_map(|outcome| {
            iceberg::arrow::TEXT_INDEX_STORAGE_FORMS
                .iter()
                .map(move |storage| BTreeSet::from([("outcome", *outcome), ("storage", *storage)]))
        })
        .collect();
    assert_eq!(
        listed(LOOKUPS_TOTAL),
        lookups,
        "every (outcome, storage) the reader records must be created at 0: update \
         QUERY_SERVER_ALERTED_COUNTERS in siglake_core::metrics"
    );

    let drops: BTreeSet<BTreeSet<(&str, &str)>> = iceberg::arrow::PARSED_INDEX_CACHE_DROP_REASONS
        .iter()
        .map(|reason| BTreeSet::from([("reason", *reason)]))
        .collect();
    assert_eq!(
        listed("siglake_iceberg_parsed_index_cache_evictions_total"),
        drops,
        "every drop reason the cache records must be created at 0: update \
         QUERY_SERVER_ALERTED_COUNTERS in siglake_core::metrics"
    );

    // The stage histogram needs no pre-registration (a histogram has no
    // `increase()` reader and renders nothing useful at zero samples), but the
    // panel groups by `stage` rather than matching on it, so a stage added here
    // must be one the dashboard can name.
    assert_eq!(
        iceberg::arrow::TEXT_INDEX_STARTUP_STAGES,
        ["permit_wait", "blob_fetch", "decode", "selection"],
        "the stage vocabulary changed; update panel 159's description in \
         deploy/grafana/siglake-overview.json and the README's metric list"
    );
}
