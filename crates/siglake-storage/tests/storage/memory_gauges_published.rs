//! The memory budget must actually reach a metrics recorder.
//!
//! THE DEFECT THIS PINS. `siglake_query_memory_pool_bytes` and the cache-budget
//! gauges were set inside `session_state_template`, a `OnceLock` that
//! initialises before the Prometheus recorder is installed. `metrics::gauge!`
//! is a silent no-op with no recorder, so they were dropped and — the lock
//! having run — never set again. That gauge has existed since 2026-08-20 and
//! had never once appeared in a scrape; verified absent on a live 1TB fleet on
//! 2026-08-28 against 723 other siglake metrics.
//!
//! It then survived a FIX. The repair was written, its commit claimed it, and
//! the edit was lost to a failed assertion in a multi-step script — leaving the
//! gauges published nowhere at all, worse than before. Nothing caught it,
//! because no test asserted these names are emitted; it took scraping a second
//! live fleet.
//!
//! So this test asserts the NAMES reach a recorder. It is deliberately about
//! publication rather than values: the values are checked by the budget test in
//! `query_memory_bound.rs`, and what failed twice here was them being emitted
//! at all.

use metrics_util::debugging::{DebuggingRecorder, Snapshotter};

fn gauge_names(snapshot: &Snapshotter) -> Vec<String> {
    snapshot
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(key, _, _, _)| key.key().name().to_string())
        .collect()
}

#[test]
fn the_memory_budget_gauges_are_published() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // Install for this thread only: the process-wide recorder may already be
    // taken, and a test that silently observed nothing would repeat the exact
    // failure it exists to catch.
    let installed = metrics::with_local_recorder(&recorder, || {
        siglake_storage::sample_query_memory_pool_gauges();
        gauge_names(&snapshotter)
    });

    for name in [
        // The pool's own limit — the number the whole memory arc is about.
        "siglake_query_memory_pool_bytes",
        "siglake_query_memory_pool_reserved_bytes",
        "siglake_query_memory_pool_available_bytes",
        "siglake_query_memory_pool_used_ratio",
        // What the pool subtracts, so a budget can be compared with the
        // occupancy gauges rather than taken on trust.
        "siglake_query_memory_reserved_elsewhere_bytes",
        "siglake_cache_budget_bytes",
    ] {
        assert!(
            installed.iter().any(|n| n == name),
            "{name} was not published; the gauges emitted were {installed:?}"
        );
    }
}

/// Every cache the pool subtracts must have its own `kind`, so a budget can be
/// read back from a scrape one cache at a time.
///
/// The text-index caches (#4056) are the case this guards: they were left out
/// of the subtraction AND out of the breakdown, so the pod's own metrics could
/// not show where 1.25Gi had gone.
#[test]
fn every_subtracted_cache_has_its_own_budget_series() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let kinds = metrics::with_local_recorder(&recorder, || {
        siglake_storage::sample_query_memory_pool_gauges();
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == "siglake_cache_budget_bytes")
            .filter_map(|(key, _, _, _)| {
                key.key()
                    .labels()
                    .find(|label| label.key() == "kind")
                    .map(|label| label.value().to_string())
            })
            .collect::<Vec<_>>()
    });

    for kind in ["read", "metadata", "text_index"] {
        assert!(
            kinds.iter().any(|k| k == kind),
            "siglake_cache_budget_bytes has no kind={kind} series; it published {kinds:?}"
        );
    }
}
