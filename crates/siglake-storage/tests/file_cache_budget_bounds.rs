//! Task #3053: the configured limits of the opt-in source-file cache are the
//! bounds they claim to be.
//!
//! The cache's bytes are subtracted from the query memory pool before a query
//! runs (`QueryReadCacheConfig::reserved_bytes`), so three things have to hold
//! or the subtraction is a fiction: the installed cache never exceeds its byte
//! budget, never exceeds its entry limit, and holds nothing at all when the
//! limits are zero. Every earlier measurement of this cache ran at the bench
//! fleet's 8 GiB / 16,384 entries, where neither bound could ever bite.
//!
//! The fourth phase is the quarter rule: an entry larger than `budget / 4` is
//! refused, so a budget can be positive, reserved, and still cache nothing.
//! That is the failure mode an operator sizing this cache by hand walks into,
//! because a decoded compacted file is much larger than the budget an ordinary
//! pod can afford — see `docs/DESIGN_source_file_cache_qualification.md`.
//!
//! Isolated in its own test binary: the scan tuning, the batch cache and the
//! metrics recorder are all process-wide.

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

const FILES: usize = 6;
const FILE_ROWS: usize = 16_384;
const BATCH_ROWS: usize = 4_096;
/// A drained scan: the shape that populates this cache at all (#4494).
const SCAN_SQL: &str = "SELECT raw FROM events";

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;

#[derive(Debug, Default, PartialEq, Eq)]
struct Outcomes {
    hit: u64,
    miss: u64,
    insert: u64,
    evict: u64,
    skip_oversized: u64,
}

fn outcomes(snapshotter: &Snapshotter) -> Outcomes {
    let snapshot = snapshotter.snapshot().into_vec();
    let sum = |outcome: &str| {
        snapshot
            .iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "siglake_query_scan_file_cache_requests_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == outcome)
            })
            .map(|(_, _, _, value)| match value {
                DebugValue::Counter(count) => *count,
                _ => 0,
            })
            .sum::<u64>()
    };
    Outcomes {
        hit: sum("hit"),
        miss: sum("miss"),
        insert: sum("insert"),
        evict: sum("evict"),
        skip_oversized: sum("skip_oversized"),
    }
}

fn events(file: usize) -> Vec<Event> {
    (0..FILE_ROWS)
        .map(|i| {
            let mut event = Event::now(format!(
                "file-{file} row-{i} checkout latency={} ms status=200 region=us-east-1",
                i % 97
            ));
            event.host = "alpha".into();
            event
        })
        .collect()
}

/// One file at a time, and one partition below, so populations do not race each
/// other for the cache lock: a contended population is skipped by design
/// (`insert_skipped_contended`), which makes concurrent counts a reading of the
/// scheduler rather than of the bound under test.
fn tuning(max_bytes: Option<u64>, max_entries: Option<usize>) -> QueryScanTuning {
    QueryScanTuning {
        file_cache_max_bytes: max_bytes,
        file_cache_max_entries: max_entries,
        file_concurrency_limit: Some(1),
        batch_size: Some(BATCH_ROWS),
        ..Default::default()
    }
}

async fn drain(ctx: &SessionContext) -> usize {
    ctx.sql(SCAN_SQL)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

/// The populate streams charge their counters as they finish, and a
/// multi-partition plan's last stream can be dropped a scheduler tick after the
/// rows arrive.
async fn settle() {
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_file_cache_limits_bound_what_the_process_holds() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO);
    for file in 0..FILES {
        ice.append_events(&events(file)).await.unwrap();
    }
    let rows = FILES * FILE_ROWS;

    // Phase 0: one pass at a budget nothing can bind, to measure what one
    // entry costs. The bounds below are multiples of it, so they bite on this
    // fixture's real entry size rather than on a constant that guessed.
    siglake_storage::configure_query_scan_tuning(tuning(Some(8 * GIB), Some(16_384)));
    let probe_ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&probe_ctx).await.unwrap();
    assert_eq!(drain(&probe_ctx).await, rows);
    settle().await;
    let probe = siglake_storage::decoded_file_cache_footprint();
    assert_eq!(
        probe.entries, FILES,
        "the unbounded probe must hold one entry per file: {probe:?}"
    );
    let entry_bytes = probe.priced_bytes / probe.entries as u64;
    assert!(entry_bytes > 0, "entries priced at nothing: {probe:?}");
    // The budget's currency must not under-count what the process cannot give
    // back: `priced_bytes` charges whole buffer allocations per batch, so it is
    // an over-count of the distinct allocations, never an under-count.
    assert!(
        probe.priced_bytes >= probe.retained_bytes,
        "the cache charged its budget LESS than the distinct allocations it \
         retains, so the pool subtraction under-counts: {probe:?}"
    );

    // Phase 1: the BYTE bound. Four entries' worth of budget over six files:
    // the cache evicts and never exceeds what it was given.
    let byte_budget = 4 * entry_bytes;
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(Some(byte_budget), Some(1_024)));
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let _ = outcomes(&snapshotter);
    assert_eq!(drain(&ctx).await, rows);
    settle().await;
    let bounded = outcomes(&snapshotter);
    let footprint = siglake_storage::decoded_file_cache_footprint();
    assert!(
        footprint.priced_bytes <= byte_budget,
        "the cache holds {} bytes against a {byte_budget}-byte budget: {footprint:?}",
        footprint.priced_bytes
    );
    assert!(
        bounded.evict > 0 && bounded.insert > 4,
        "six files into a four-entry budget must insert past the budget and \
         evict: {bounded:?}"
    );
    assert!(
        footprint.priced_bytes >= footprint.retained_bytes,
        "budget-priced bytes under-count the retained allocations: {footprint:?}"
    );

    // Phase 2: the ENTRY bound, with bytes far out of reach. The entry limit is
    // the map-size guard, so it must hold on its own.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(Some(64 * entry_bytes), Some(2)));
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let _ = outcomes(&snapshotter);
    assert_eq!(drain(&ctx).await, rows);
    settle().await;
    let entry_bound = outcomes(&snapshotter);
    let footprint = siglake_storage::decoded_file_cache_footprint();
    assert!(
        footprint.entries <= 2,
        "the cache holds {} entries against a 2-entry limit: {footprint:?}",
        footprint.entries
    );
    assert!(
        entry_bound.evict > 0,
        "the entry limit evicted nothing over six files: {entry_bound:?}"
    );

    // Phase 3: the quarter rule. A positive budget smaller than four entries
    // caches NOTHING — every population is refused — while its bytes are still
    // subtracted from the query memory pool.
    let starved = siglake_storage::min_file_cache_bytes_for_entry(entry_bytes) - 1;
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(Some(starved), Some(1_024)));
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let _ = outcomes(&snapshotter);
    assert_eq!(drain(&ctx).await, rows);
    settle().await;
    let refused = outcomes(&snapshotter);
    let footprint = siglake_storage::decoded_file_cache_footprint();
    assert_eq!(
        (refused.insert, footprint.entries),
        (0, 0),
        "a budget below four entries must refuse every population and hold \
         nothing: {refused:?} {footprint:?}"
    );
    assert!(
        refused.skip_oversized >= FILES as u64 - 1,
        "every refusal must be attributed to the quarter rule: {refused:?}"
    );
    // One byte more of budget and the same entries are accepted: the rule is
    // the entry size against the budget, nothing else.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(Some(starved + 1), Some(1_024)));
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let _ = outcomes(&snapshotter);
    assert_eq!(drain(&ctx).await, rows);
    settle().await;
    let accepted = outcomes(&snapshotter);
    assert!(
        accepted.insert > 0 && accepted.skip_oversized == 0,
        "one byte above the quarter rule must accept the same entries: {accepted:?}"
    );

    // Phase 4: explicit zero. The disablement the chart, the compose file and
    // the operator all render: no requests counted at all, nothing held.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(Some(0), Some(0)));
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let _ = outcomes(&snapshotter);
    assert_eq!(drain(&ctx).await, rows);
    settle().await;
    let disabled = outcomes(&snapshotter);
    let footprint = siglake_storage::decoded_file_cache_footprint();
    assert_eq!(
        disabled,
        Outcomes::default(),
        "an explicitly zeroed cache must not even count a request"
    );
    assert_eq!(
        (footprint.entries, footprint.priced_bytes),
        (0, 0),
        "an explicitly zeroed cache must hold nothing: {footprint:?}"
    );

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
    siglake_storage::clear_decoded_file_cache();

    eprintln!(
        "#3053 bounds: one entry = {} KiB priced over {} files x {FILE_ROWS} rows; \
         byte bound {} KiB, entry bound 2, quarter rule at {} KiB",
        entry_bytes / 1024,
        FILES,
        byte_budget / 1024,
        (starved + 1) / 1024,
    );
}

/// The sizing an operator is pointed at, as arithmetic rather than prose.
///
/// `derive_file_cache_limits` recommends; it does not enable. What it is FOR is
/// the question an operator cannot otherwise answer — whether the cache can
/// hold the files this deployment reads — and at every packaged pod size the
/// answer for a compacted file is no.
#[test]
fn the_derived_limits_are_positive_and_priced_against_a_compacted_file() {
    // No container limit: no recommendation. Host RAM is not ours to spend,
    // which is the same rule `derive_read_cache_bytes` follows.
    assert_eq!(siglake_storage::derive_file_cache_limits(None), None);

    for limit_gib in [4u64, 8, 16, 64] {
        let (bytes, entries) = siglake_storage::derive_file_cache_limits(Some(limit_gib * GIB))
            .expect("a container limit derives a recommendation");
        assert_eq!(bytes, limit_gib * GIB / 8, "{limit_gib}Gi");
        assert!(
            entries as u64 >= siglake_storage::MAX_FILE_CACHE_ENTRY_FRACTION,
            "{limit_gib}Gi recommends {entries} entries, below the quarter rule's minimum"
        );
        // The entry limit must not bind before the byte budget for files of
        // ordinary size: one entry per MiB against entries measured in MiB.
        assert!(
            entries as u64 >= bytes / MIB,
            "{limit_gib}Gi: {entries} entries is tighter than {} MiB of budget",
            bytes / MIB
        );
    }

    // A compacted file is `cold_target_file_bytes` (256 MiB) at the scan's
    // decompression estimate (5) — about 1.25 GiB decoded, so the quarter rule
    // wants a 5 GiB file cache to hold ONE. An eighth of the container is that
    // only at 40 GiB, which no packaged pod is: the chart's query pod is 4Gi
    // and the operator renders 2Gi.
    let compacted_entry = 256 * MIB * 5;
    let needed = siglake_storage::min_file_cache_bytes_for_entry(compacted_entry);
    assert_eq!(needed, 5 * GIB);
    for limit_gib in [4u64, 8, 16, 32] {
        let (bytes, _) = siglake_storage::derive_file_cache_limits(Some(limit_gib * GIB)).unwrap();
        assert!(
            bytes < needed,
            "{limit_gib}Gi derives {} MiB, which would hold a decoded compacted \
             file — the qualification doc says it cannot",
            bytes / MIB
        );
    }
    let (large, _) = siglake_storage::derive_file_cache_limits(Some(40 * GIB)).unwrap();
    assert!(
        large >= needed,
        "40Gi derives {} MiB, below the 5 GiB one compacted entry needs",
        large / MIB
    );
}
