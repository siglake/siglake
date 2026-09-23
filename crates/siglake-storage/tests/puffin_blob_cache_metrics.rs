//! #4718: what the Puffin blob cache does has to be readable from a deployed
//! pod's `/metrics`, not only from `puffin_blob_fetch_counts()` in a test.
//!
//! The fact a round could not read is whether a re-decode also had to re-read
//! the blob: #4182's refetch regression was inferred from index-phase bytes and
//! `first_batch_ms` a round after it landed. This file holds the emitters to a
//! fixture — a cold execution fetches and misses, a repeat decode is served
//! from the held bytes, and a full cache names the rule that chose its victim.
//!
//! Its own test binary, and one test: the blob cache, the parsed-index cache
//! and the metrics recorder are all process-wide, and the counters asserted
//! here are attributed to these queries and to the cache state the phases
//! before them left.

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

const FETCHES_TOTAL: &str = "siglake_iceberg_puffin_blob_fetches_total";
const LOOKUPS_TOTAL: &str = "siglake_iceberg_puffin_blob_cache_lookups_total";
const EVICTIONS_TOTAL: &str = "siglake_iceberg_puffin_blob_cache_evictions_total";
const RESIDENT_BYTES: &str = "siglake_iceberg_puffin_blob_cache_bytes";
const BUDGET_BYTES: &str = "siglake_iceberg_puffin_blob_cache_max_bytes";

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

/// The last value a gauge was set to within the phase this snapshot covers.
///
/// `Snapshotter::snapshot()` swaps gauges to zero the way it swaps counters, so
/// a pair read from a later snapshot than the execution that emitted it reads
/// zero and asserts nothing. `None` is a gauge that has never been set at all.
fn gauge_value(snapshot: &SnapshotVec, name: &str) -> Option<f64> {
    snapshot
        .iter()
        .find(|(key, _, _, _)| key.key().name() == name)
        .map(|(_, _, _, value)| match value {
            DebugValue::Gauge(value) => **value,
            other => panic!("{name} is {other:?}, not a gauge"),
        })
}

/// The resident/budget pair the panel charts, from one phase's snapshot.
fn blob_cache_pair(snapshot: &SnapshotVec, phase: &str) -> (f64, f64) {
    (
        gauge_value(snapshot, RESIDENT_BYTES)
            .unwrap_or_else(|| panic!("{phase} published no resident bytes")),
        gauge_value(snapshot, BUDGET_BYTES)
            .unwrap_or_else(|| panic!("{phase} published no enforced budget")),
    )
}

/// A warehouse of `appends` Puffin-indexed files: `index_footer_max_bytes: 1`
/// forces every per-file index out of the Parquet footer and into a sidecar,
/// which is the only form that pays a fetch and therefore the only one this
/// cache holds.
async fn puffin_indexed_warehouse(warehouse: &std::path::Path, appends: usize) -> IcebergContext {
    let ice = IcebergContext::open(warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_footer_max_bytes: Some(1),
            ..Default::default()
        });
    for append in 0..appends {
        ice.append_events(&[
            Event::now(format!("database timeout retry {append}")),
            Event::now(format!("healthy startup {append}")),
            Event::now(format!("database migration {append}")),
        ])
        .await
        .unwrap();
    }
    ice
}

const SQL: &str = "SELECT count(*) FROM events WHERE raw LIKE '%database%'";

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

/// Two executions with the parsed cache refusing every index, then a third
/// warehouse queried against a blob budget that cannot hold it beside the
/// first, then two more against a budget one byte short of a single blob and
/// against no budget at all — the readings the panel is built on, in one
/// process so the cache state each phase starts from is known.
#[tokio::test]
async fn a_text_query_reports_its_blob_fetches_hits_and_the_rule_that_evicted() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();

    // Phase 1. Nothing is ever kept parsed (one byte of budget refuses every
    // index outright), so the blob cache is the only thing standing between a
    // repeat decode and a repeat read — which is what it was added for.
    siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
        parsed_index_max_bytes: 1,
        puffin_blob_max_bytes: 64 * 1024 * 1024,
    });
    let held = puffin_indexed_warehouse(&tmp.path().join("held"), 2).await;
    let held_ctx = SessionContext::new();
    held.register_with_datafusion(&held_ctx).await.unwrap();

    // Everything the appends and the registration recorded is not this query's.
    let _ = snapshotter.snapshot();
    assert_eq!(count(&held_ctx, SQL).await, 4, "cold result");
    let cold = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(&cold, FETCHES_TOTAL, None),
        2,
        "a cold execution reads both files' blobs from the store"
    );
    assert_eq!(
        counter_sum(&cold, LOOKUPS_TOTAL, Some(("outcome", "miss"))),
        2,
        "and consults the cache once per file before it does"
    );
    assert_eq!(
        counter_sum(&cold, LOOKUPS_TOTAL, Some(("outcome", "hit"))),
        0,
        "nothing was held yet"
    );
    assert_eq!(
        counter_sum(&cold, EVICTIONS_TOTAL, None),
        0,
        "64 MiB holds this fixture's blobs many times over"
    );
    assert_eq!(
        counter_sum(&cold, EVICTIONS_TOTAL, Some(("reason", "oversized"))),
        0,
        "a budget that holds the blobs refuses none of them: the healthy zero \
         this arm has to be able to sit at"
    );

    assert_eq!(count(&held_ctx, SQL).await, 4, "warm result diverged");
    let warm = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(&warm, LOOKUPS_TOTAL, Some(("outcome", "hit"))),
        2,
        "the repeat decode must be served from the held bytes"
    );
    assert_eq!(
        counter_sum(&warm, FETCHES_TOTAL, None),
        0,
        "and must read nothing: this is the #4182 regression's own counter"
    );

    // What those two blobs cost, so the next phase can pick a budget that holds
    // one of them beside its own and not two.
    let (entries, bytes, total) = iceberg::arrow::puffin_blob_cache_stats("held");
    assert_eq!(entries, 2, "both files' blobs are held");
    assert_eq!(
        bytes, total,
        "this binary's only cached blobs are the fixture's"
    );
    let blob = bytes / 2;

    // The pair the panel charts, from the execution that admitted those blobs:
    // the maintained resident figure, not a walk of the map, and the budget
    // this phase configured rather than the 256 MiB environment default.
    let (resident, budget) = blob_cache_pair(&cold, "the cold execution");
    assert_eq!(
        resident as usize, total,
        "resident bytes must be what the cache actually holds"
    );
    assert_eq!(
        budget as usize,
        64 * 1024 * 1024,
        "and the budget must be the one in force, which is what makes an \
         eviction rate readable: the same rate under half this budget is a \
         different pod"
    );

    // Phase 2. Parsed indexes are kept this time, so every blob admitted has a
    // resident twin and cannot be read while that twin lives: the `redundant`
    // arm. The budget leaves room for one more blob than the two already held,
    // so the first of this warehouse's files is admitted without evicting and
    // the second has to drop something.
    siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
        parsed_index_max_bytes: u64::MAX / 2,
        puffin_blob_max_bytes: (bytes + blob + blob / 2) as u64,
    });
    let full = puffin_indexed_warehouse(&tmp.path().join("full"), 2).await;
    let full_ctx = SessionContext::new();
    full.register_with_datafusion(&full_ctx).await.unwrap();
    let _ = snapshotter.snapshot();
    assert_eq!(count(&full_ctx, SQL).await, 4, "crowded result");
    let crowded = snapshotter.snapshot().into_vec();

    assert_eq!(
        counter_sum(&crowded, EVICTIONS_TOTAL, None),
        1,
        "one blob over the budget is one eviction, charged once"
    );
    assert_eq!(
        counter_sum(&crowded, EVICTIONS_TOTAL, Some(("reason", "redundant"))),
        1,
        "the victim was the blob whose parsed twin is resident, and the counter \
         has to say so: a `fifo` rate here would be the pre-#4182 rule"
    );
    // The two blobs from phase 1 have no parsed twin and are older, but neither
    // is stale — protection lasts `BLOB_PROTECTION_TURNOVERS` turnovers of the
    // cache, and this fixture admits four blobs in total.
    assert_eq!(
        counter_sum(&crowded, EVICTIONS_TOTAL, Some(("reason", "stale"))),
        0,
        "nothing has been resident long enough to have left the working set"
    );
    assert_eq!(
        counter_sum(&crowded, FETCHES_TOTAL, None),
        2,
        "a warehouse this cache has never seen costs one read per file"
    );

    // Nothing is admitted between the snapshot and this read, so the cache's
    // own total is the truth value for the gauge the phase last published.
    let crowded_total = iceberg::arrow::puffin_blob_cache_stats("").2;
    let (resident, budget) = blob_cache_pair(&crowded, "the crowded execution");
    assert_eq!(
        resident as usize, crowded_total,
        "the eviction has to come off the resident figure: a gauge that only \
         ever rose would chart this cache as permanently over its budget"
    );
    assert_eq!(
        budget as usize,
        bytes + blob + blob / 2,
        "against the budget that produced the eviction"
    );
    assert!(
        resident <= budget,
        "{resident} resident over a {budget} budget means the bound was not \
         enforced where it is published"
    );

    // Phase 3. A budget one byte short of a single blob, which is the pod this
    // counter exists for: every bound is positive, the cache is consulted, and
    // nothing is ever admitted. Parsed indexes are refused again so each
    // execution really decodes and really tries the admission.
    siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
        parsed_index_max_bytes: 1,
        puffin_blob_max_bytes: (blob - 1) as u64,
    });
    let refused = puffin_indexed_warehouse(&tmp.path().join("refused"), 2).await;
    let refused_ctx = SessionContext::new();
    refused
        .register_with_datafusion(&refused_ctx)
        .await
        .unwrap();
    let _ = snapshotter.snapshot();
    assert_eq!(count(&refused_ctx, SQL).await, 4, "refused result");
    let refused_once = snapshotter.snapshot().into_vec();

    assert_eq!(
        counter_sum(
            &refused_once,
            EVICTIONS_TOTAL,
            Some(("reason", "oversized"))
        ),
        2,
        "one refusal per file whose blob alone exceeds the budget, charged once \
         each — without it this execution is a rising fetch rate with no \
         eviction and no hit, which is also what a cold cache draws"
    );
    assert_eq!(
        counter_sum(&refused_once, EVICTIONS_TOTAL, None),
        2,
        "a refusal evicts nothing: the resident blobs are not disturbed to make \
         room for a blob that cannot fit"
    );
    let (held_entries, held_bytes, _) = iceberg::arrow::puffin_blob_cache_stats("held");
    assert_eq!(
        (held_entries, held_bytes),
        (2, bytes),
        "and the entries admitted under the larger budget are still resident"
    );
    assert_eq!(
        iceberg::arrow::puffin_blob_cache_stats("refused").0,
        0,
        "while the warehouse that was refused is held not at all"
    );

    assert_eq!(count(&refused_ctx, SQL).await, 4, "second refused result");
    let refused_twice = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(
            &refused_twice,
            EVICTIONS_TOTAL,
            Some(("reason", "oversized"))
        ),
        2,
        "the cache is inert for these files, so the next decode refuses again: \
         the rate is per admission attempt, not per file"
    );
    assert_eq!(
        counter_sum(&refused_twice, FETCHES_TOTAL, None),
        2,
        "and re-reads both blobs, which is the cost the counter attributes"
    );

    // The refusal arm the parsed side has no equivalent of. Nothing was
    // admitted, so a pair published only after a successful insert would hold
    // phase 2's budget here and chart this pod as the one before it — the
    // reading `dropped oversized` is meant to be read against.
    for (phase, snapshot) in [
        ("the first refused execution", &refused_once),
        ("the second refused execution", &refused_twice),
    ] {
        let (resident, budget) = blob_cache_pair(snapshot, phase);
        assert_eq!(
            budget as usize,
            blob - 1,
            "{phase} must publish the budget that refused it, not the larger \
             one the resident entries were admitted under"
        );
        assert_eq!(
            resident as usize, crowded_total,
            "{phase} admits and evicts nothing, so the resident figure is \
             exactly what the previous budget left behind"
        );
        assert!(
            resident > budget,
            "{phase} is the shape this pair exists to make legible: entries \
             admitted under a larger budget, sitting over a smaller one that \
             now refuses every new blob"
        );
    }

    // Phase 4. The same query with the cache switched off. A disabled cache
    // refuses every blob of every file, which the absent lookup series already
    // says (#4718); charging that as a refusal would make `oversized` track the
    // fetch rate and stop meaning "this pod is one blob short".
    siglake_storage::configure_text_index_caches(siglake_storage::TextIndexCacheConfig {
        parsed_index_max_bytes: 1,
        puffin_blob_max_bytes: 0,
    });
    let off = puffin_indexed_warehouse(&tmp.path().join("off"), 2).await;
    let off_ctx = SessionContext::new();
    off.register_with_datafusion(&off_ctx).await.unwrap();
    let _ = snapshotter.snapshot();
    assert_eq!(count(&off_ctx, SQL).await, 4, "disabled result");
    let disabled = snapshotter.snapshot().into_vec();

    assert_eq!(
        counter_sum(&disabled, FETCHES_TOTAL, None),
        2,
        "a disabled cache reads every blob it is asked for"
    );
    assert_eq!(
        counter_sum(&disabled, LOOKUPS_TOTAL, None),
        0,
        "and is never consulted"
    );
    assert_eq!(
        counter_sum(&disabled, EVICTIONS_TOTAL, None),
        0,
        "a zero bound is not a refusal this family charges"
    );

    // A switched-off cache is the one pod the eviction and lookup families are
    // all silent on, and it is the reading the budget gauge gives directly: 0,
    // not the 256 MiB environment default and not the 64 MiB this process
    // configured earlier. The entry bound's own zero is unreachable in-process
    // — it is read from the environment at the cache — and is held instead by
    // `a_zero_entry_bound_enforces_no_blob_budget` in the Iceberg fork.
    let (resident, budget) = blob_cache_pair(&disabled, "the disabled execution");
    assert_eq!(budget as usize, 0, "a disabled cache enforces no budget");
    assert_eq!(
        resident as usize, crowded_total,
        "while the blobs earlier budgets admitted are still held and still \
         costing the pod memory, which is the other half of the reading"
    );

    iceberg::arrow::clear_text_index_cache_max_bytes();
}

/// The panel reads `rate()` over all three families, so their series have to
/// exist at 0 on a query server that has not served a text query yet:
/// a `hit` arm that is absent and a `hit` arm that is flat at zero are the same
/// chart, and the second is the reading (#4182 hid exactly that for a round).
/// The label values are variables at the emitters, so `scripts/check-chart.py`
/// sees dynamic sites and cannot hold the catalog to them; the reader's own
/// exported vocabularies can.
#[test]
fn puffin_blob_cache_series_are_preregistered() {
    use std::collections::BTreeSet;

    let listed = |name: &str| -> BTreeSet<BTreeSet<(&'static str, &'static str)>> {
        siglake_core::metrics::QUERY_SERVER_ALERTED_COUNTERS
            .iter()
            .filter(|counter| counter.name == name)
            .flat_map(|counter| counter.series.iter())
            .map(|labels| labels.iter().copied().collect())
            .collect()
    };

    let lookups: BTreeSet<BTreeSet<(&str, &str)>> = iceberg::arrow::PUFFIN_BLOB_CACHE_OUTCOMES
        .iter()
        .map(|outcome| BTreeSet::from([("outcome", *outcome)]))
        .collect();
    assert_eq!(
        listed(LOOKUPS_TOTAL),
        lookups,
        "every outcome the cache records must be created at 0: update \
         QUERY_SERVER_ALERTED_COUNTERS in siglake_core::metrics"
    );

    let drops: BTreeSet<BTreeSet<(&str, &str)>> = iceberg::arrow::PUFFIN_BLOB_CACHE_DROP_REASONS
        .iter()
        .map(|reason| BTreeSet::from([("reason", *reason)]))
        .collect();
    assert_eq!(
        listed(EVICTIONS_TOTAL),
        drops,
        "every eviction reason the rule can choose must be created at 0: update \
         QUERY_SERVER_ALERTED_COUNTERS in siglake_core::metrics"
    );

    // The fetch counter carries no label, so its one series is the whole
    // vocabulary — and it is the arm a rising refetch rate shows up on.
    assert_eq!(
        listed(FETCHES_TOTAL),
        BTreeSet::from([BTreeSet::new()]),
        "the fetch counter must be created at 0 too"
    );
}
