//! The query engine's memory pool must actually be INSTALLED and bounded.
//!
//! The sizing unit tests cover the arithmetic; this covers the wiring, which is
//! the part that was missing entirely. Before this, `RuntimeEnv::default()` gave
//! every session an UNBOUNDED pool: sorts and hash aggregates had no ceiling and
//! never spilled. Measured at 1TB on 2026-08-19 — three OOM kills at ~31.6 GB
//! anon-rss on a 32 GB node, from 8 concurrent `ORDER BY raw LIMIT 100`.
//!
//! The discriminator is direct: against an unbounded pool a reservation of any
//! size succeeds, so a large reservation that FAILS proves a bound exists.
//!
//! Its own process (integration tests get one each): the pool is a process-wide
//! `OnceLock`, and `setup()` pins its size through
//! `preset_query_memory_pool_bytes` before anything in this binary has touched
//! it. Nothing here reads or writes the environment.

use datafusion::execution::memory_pool::MemoryConsumer;

const POOL_BYTES: u64 = 64 * 1024 * 1024;

/// Serialises the tests in this binary.
///
/// They all reserve from the SAME process-wide pool — that is the property
/// under test — so run in parallel they race each other's reservations rather
/// than testing anything. Observed 2026-08-28:
/// `execution_and_session_share_one_pool` reserves three quarters of the pool
/// and failed with "16.0 MB remain available for the total pool" in a full
/// workspace run while passing 3/3 in isolation.
///
/// This is the same hazard as the process-wide env var that forced two
/// compaction tests to be merged on 2026-08-27: shared global state plus
/// cargo's default parallelism. A lock rather than merging, because five
/// assertions in one function would report only the first failure.
static POOL_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Held for the body of each test. Reservations must drop before the guard, so
/// the next test sees an empty pool — taking it by value at the top of a test
/// and letting normal scope order apply is enough.
fn setup() -> std::sync::MutexGuard<'static, ()> {
    // Poisoning only means another test panicked; the pool is still usable and
    // failing every subsequent test on it would hide the original failure.
    let guard = POOL_TESTS.lock().unwrap_or_else(|e| e.into_inner());
    // Explicit config, not `set_var`: the pool is built once per process, and
    // pinning it here is what makes every assertion below about a KNOWN bound
    // rather than whatever this machine's cgroup happens to derive. Idempotent
    // across tests — the first call builds the pool, the rest confirm it.
    assert!(
        siglake_storage::preset_query_memory_pool_bytes(POOL_BYTES),
        "the query memory pool was already built at another size before this \
         binary could bound it at {POOL_BYTES} bytes — nothing in this binary \
         may touch the pool before setup()"
    );
    guard
}

#[test]
fn the_pool_is_installed_and_refuses_an_oversized_reservation() {
    let _pool_guard = setup();
    let ctx = siglake_storage::session_context_with_order(None, None, None);
    let pool = ctx.runtime_env().memory_pool.clone();

    let mut reservation = MemoryConsumer::new("oversized").register(&pool);
    let too_big = (POOL_BYTES * 16) as usize;
    let result = reservation.try_grow(too_big);

    assert!(
        result.is_err(),
        "a {}-byte reservation succeeded against a {POOL_BYTES}-byte pool, which \
         means the pool is UNBOUNDED — this is the configuration that OOM-killed \
         hosts at 1TB",
        too_big
    );
}

/// The bound must not be so eager that ordinary work cannot proceed: trading an
/// OOM for universal ResourcesExhausted is a different outage, not a fix.
#[test]
fn an_ordinary_reservation_still_succeeds() {
    let _pool_guard = setup();
    let ctx = siglake_storage::session_context_with_order(None, None, None);
    let pool = ctx.runtime_env().memory_pool.clone();

    let mut reservation = MemoryConsumer::new("ordinary").register(&pool);
    reservation
        .try_grow(1024 * 1024)
        .expect("a 1 MiB reservation must fit in a 64 MiB pool");
}

/// Every per-query context must share ONE pool. A per-query limit bounds a
/// single query and still lets N concurrent ones exhaust the machine, which is
/// exactly the failure that was measured.
#[test]
fn all_query_contexts_share_the_same_pool() {
    let _pool_guard = setup();
    let a = siglake_storage::session_context_with_order(None, None, None);
    let b = siglake_storage::session_context_with_order(Some(4), None, None);

    let pool_a = a.runtime_env().memory_pool.clone();
    let pool_b = b.runtime_env().memory_pool.clone();

    // Reserve most of the pool through one context...
    let mut held = MemoryConsumer::new("holder").register(&pool_a);
    held.try_grow((POOL_BYTES as usize) * 3 / 4)
        .expect("three quarters of the pool must fit");

    // ...and the OTHER context must see it gone.
    let mut other = MemoryConsumer::new("other").register(&pool_b);
    assert!(
        other.try_grow((POOL_BYTES as usize) / 2).is_err(),
        "a second query context could still reserve half the pool while another \
         held three quarters — the contexts are not sharing a pool, so the bound \
         is per-query and N queries can still exhaust the box"
    );
}

/// THE PROPERTY THAT WAS SILENTLY FALSE. Operators take their memory pool from
/// the `TaskContext` at EXECUTION time, not from the `SessionState` the plan was
/// built with. Three call sites used `TaskContext::default()`, which mints a
/// fresh `RuntimeEnv` with an unbounded pool -- so a process could report a
/// 16.5 GiB pool, never consult it, and still be OOM-killed at 31.6 GB.
///
/// A bound that execution does not see is not a bound.
#[test]
fn the_executing_task_context_carries_the_bound() {
    let _pool_guard = setup();
    // The test IS the pool assertion, so the no-session helper is the subject.
    #[allow(clippy::disallowed_methods)]
    let task_ctx = siglake_storage::bounded_task_context();
    let pool = task_ctx.runtime_env().memory_pool.clone();

    let mut reservation = MemoryConsumer::new("exec-path").register(&pool);
    assert!(
        reservation.try_grow((POOL_BYTES * 16) as usize).is_err(),
        "the TaskContext used to EXECUTE plans has an unbounded pool — the bound \
         on the session is never consulted by the operators that allocate"
    );
}

/// And it must be the SAME pool as the session's, not merely some bounded pool:
/// two separate bounds of the same size still let the process reach twice the
/// intended total.
#[test]
fn execution_and_session_share_one_pool() {
    let _pool_guard = setup();
    let session = siglake_storage::session_context_with_order(None, None, None);
    let session_pool = session.runtime_env().memory_pool.clone();
    #[allow(clippy::disallowed_methods)]
    let exec_pool = siglake_storage::bounded_task_context()
        .runtime_env()
        .memory_pool
        .clone();

    let mut held = MemoryConsumer::new("via-session").register(&session_pool);
    held.try_grow((POOL_BYTES as usize) * 3 / 4)
        .expect("three quarters must fit");

    let mut other = MemoryConsumer::new("via-exec").register(&exec_pool);
    assert!(
        other.try_grow((POOL_BYTES as usize) / 2).is_err(),
        "execution reserved half the pool while the session held three quarters \
         — they are different pools, so the process bound is really 2x"
    );
}

/// A reservation that is still held must be NAMEABLE from outside the query.
///
/// THE GAP THIS CLOSES. On 2026-09-01 `siglake_query_memory_pool_reserved_bytes`
/// read 3.66 GiB on a pod with no query in flight, against an established idle
/// value of 0. The pool could say that bytes were held and nothing else, so the
/// reading sat uninvestigated for two days. With the consumer-tracking wrapper
/// installed (the default), the same scrape can be followed by
/// `query_memory_pool_top_consumers`, which names the operator or stream whose
/// `MemoryReservation` is alive.
#[test]
fn a_held_reservation_is_named_by_the_consumer_report() {
    let _pool_guard = setup();
    #[allow(clippy::disallowed_methods)]
    let pool = siglake_storage::bounded_task_context()
        .runtime_env()
        .memory_pool
        .clone();

    let mut held = MemoryConsumer::new("siglake-test-holder").register(&pool);
    held.try_grow(3 * 1024 * 1024)
        .expect("3 MiB must fit in a 64 MiB pool");

    let report = siglake_storage::query_memory_pool_top_consumers(5)
        .expect("consumer tracking is on by default, so the pool must produce a report");
    assert!(
        report.contains("siglake-test-holder"),
        "the report must name the consumer holding the bytes, got: {report}"
    );
    assert!(
        report.contains("consumed 3.0 MB"),
        "the report must carry the held size, got: {report}"
    );

    // And once released, the consumer is gone from the report -- the report
    // lists LIVE reservations, so a stream that was dropped cannot appear.
    drop(held);
    let after = siglake_storage::query_memory_pool_top_consumers(5).unwrap();
    assert!(
        !after.contains("siglake-test-holder"),
        "a dropped reservation must leave the report, got: {after}"
    );
    let (reserved, _) = siglake_storage::query_memory_pool_usage().unwrap();
    assert_eq!(
        reserved, 0,
        "the pool must read 0 once every reservation is dropped"
    );
}

#[cfg(test)]
mod memory_budget_tests {
    const GIB: u64 = 1024 * 1024 * 1024;

    fn mb(b: u64) -> u64 {
        b / (1024 * 1024)
    }

    /// The process must not promise itself more memory than it has.
    ///
    /// THE DEFECT THIS GUARDS. The pool subtracted two of at least eight things
    /// this process holds, so the figure it published as its budget was never
    /// the process's budget — on the packaged 4Gi query pod it claimed 1.25 GiB
    /// while another 512 MiB sat in metadata caches it had never heard of. Every
    /// over-commitment in this system's history was arithmetic nobody could
    /// evaluate without deploying.
    ///
    /// Checked across the sizes that actually ship: the operator renders 2Gi,
    /// the chart renders 4Gi, and bench nodes run 30-64 GiB.
    #[test]
    fn pool_and_caches_fit_the_pod_with_headroom() {
        for limit_gib in [2u64, 4, 8, 16, 30, 64] {
            let limit = limit_gib * GIB;
            let budget = siglake_storage::memory_budget_for(Some(limit), 0.5);
            let committed = budget.committed();
            assert!(
                committed <= limit,
                "{limit_gib}Gi pod over-commits: pool {} MB + read {} MB + metadata {} MB \
                 + text index {} MB = {} MB > {} MB",
                mb(budget.pool),
                mb(budget.read_caches),
                mb(budget.metadata_caches),
                mb(budget.text_index_caches),
                mb(committed),
                mb(limit)
            );
            // Headroom for the process itself, in-flight Arrow batches and
            // allocator slack. Without a floor here the arithmetic can satisfy
            // the assertion above and still leave nothing to actually run in.
            let headroom = budget.headroom(limit);
            assert!(
                headroom >= limit / 8,
                "{limit_gib}Gi pod leaves only {} MB of headroom after pool {} MB, \
                 read caches {} MB, metadata caches {} MB and text-index caches {} MB",
                mb(headroom),
                mb(budget.pool),
                mb(budget.read_caches),
                mb(budget.metadata_caches),
                mb(budget.text_index_caches)
            );
            // And the pool must still be big enough to run queries; trading
            // OOM-kills for universal ResourcesExhausted is not an improvement.
            assert!(
                budget.pool >= 256 * 1024 * 1024,
                "{limit_gib}Gi pod squeezed the pool to {} MB",
                mb(budget.pool)
            );
        }
    }

    /// The text-index caches must be part of the budget, not spent out of the
    /// headroom behind its back — and what they take must not come out of the
    /// pool's one-file decode reservation.
    ///
    /// THE DEFECT THIS GUARDS (#4056). The Puffin blob and parsed-index caches
    /// were flat constants — 1 GiB + 256 MiB on every pod. On the packaged 4Gi
    /// query pod that is the whole ~1.25Gi the budget leaves outside the pool
    /// and the caches it knows about, so a pod with a large text working set
    /// committed its process headroom twice; on a 16Gi pod the same constants
    /// are a quarter of what the pod can hold.
    #[test]
    fn text_index_caches_scale_with_the_pod_and_are_subtracted() {
        // 4Gi is the floor: its whole remainder is the pool's first-file decode
        // reservation, so there is nothing left to cache with.
        let floor = siglake_storage::memory_budget_for(Some(4 * GIB), 0.5);
        assert_eq!(
            floor.text_index_caches, 0,
            "the floor pod must not spend the pool's decode reservation on caches"
        );
        assert_eq!(
            mb(floor.pool),
            1280,
            "the floor pod's pool must still hold one compacted file's decode estimate"
        );

        let above = siglake_storage::memory_budget_for(Some(5 * GIB), 0.5);
        let large = siglake_storage::memory_budget_for(Some(16 * GIB), 0.5);
        assert!(
            floor.text_index_caches < above.text_index_caches
                && above.text_index_caches < large.text_index_caches,
            "text-index caches did not scale with the pod: {} MB at 4Gi, {} MB at 5Gi, \
             {} MB at 16Gi",
            mb(floor.text_index_caches),
            mb(above.text_index_caches),
            mb(large.text_index_caches)
        );
        assert!(
            mb(above.pool) >= 1280,
            "a 5Gi pod must keep the decode reservation too, got {} MB",
            mb(above.pool)
        );
        // The old constants are the CAP, reached at 16Gi: nothing has measured
        // a working set bigger than the three or four large compacted files
        // 1 GiB of parsed indexes holds, so a larger pod earns pool and
        // headroom instead.
        let huge = siglake_storage::memory_budget_for(Some(64 * GIB), 0.5);
        assert_eq!(
            huge.text_index_caches,
            large.text_index_caches,
            "a 64Gi pod grew the text-index caches past the 16Gi cap, to {} MB",
            mb(huge.text_index_caches)
        );

        // And the subtraction must have MOVED the pool; a budget that reports
        // the caches without pricing them is the defect, restated.
        let without_text = siglake_storage::query_pool_bytes_full(
            Some(16 * GIB),
            0.5,
            None,
            large.read_caches + large.metadata_caches,
        )
        .unwrap();
        assert!(
            large.pool < without_text,
            "pool unchanged at {} MB after subtracting {} MB of text-index caches",
            mb(large.pool),
            mb(large.text_index_caches)
        );
    }

    /// The override keeps its meaning, including `0`, and no limit keeps the
    /// constants the caches shipped with.
    #[test]
    fn text_index_overrides_and_the_no_limit_fallback_stand() {
        let derived = siglake_storage::resolve_text_index_cache_config(Some(8 * GIB), None, None);
        assert_eq!(mb(derived.parsed_index_max_bytes), 512);
        assert_eq!(mb(derived.puffin_blob_max_bytes), 128);

        // Raw strings rather than `set_var`: the other tests in this binary
        // read the same process environment on parallel threads.
        let configured = siglake_storage::text_index_cache_config_from(
            Some(8 * GIB),
            Some("1610612736"),
            Some("0"),
        );
        assert_eq!(
            configured.parsed_index_max_bytes,
            1536 * 1024 * 1024,
            "the parsed override was not honoured"
        );
        assert_eq!(
            configured.puffin_blob_max_bytes, 0,
            "0 must still drop the serialized copy"
        );
        assert_eq!(
            configured.reserved_bytes(),
            1536 * 1024 * 1024,
            "a disabled blob cache must reserve nothing"
        );

        // Unparseable values fall through to the derivation rather than
        // disabling a cache by accident.
        let junk =
            siglake_storage::text_index_cache_config_from(Some(8 * GIB), Some("many"), Some(""));
        assert_eq!(junk, derived);

        let no_limit = siglake_storage::resolve_text_index_cache_config(None, None, None);
        assert_eq!(
            (
                mb(no_limit.parsed_index_max_bytes),
                mb(no_limit.puffin_blob_max_bytes)
            ),
            (1024, 256),
            "without a limit to read, the caches keep the constants they shipped with"
        );
    }

    /// The pool and gauges must consume the same resolved cache configuration
    /// that is applied by the query server.
    ///
    /// THE DEFECT THIS GUARDS. The query server accepted cache overrides as CLI
    /// flags, but the pool independently read environment variables. A flag-only
    /// override therefore changed the cache without changing its subtraction.
    ///
    /// Drives the pure resolver with the three cache-setting strings instead of
    /// `set_var`: the derivation half and the pool tests in this binary read
    /// the same process environment on parallel threads.
    #[test]
    fn resolved_bytes_follow_the_configured_override_not_the_derivation() {
        let limit = Some(32 * GIB);
        let derived = siglake_storage::resolve_query_read_cache_config(limit, None, None, None);

        // Deliberately unlike the derivation, so an accidental pass is not
        // possible. The raw-string resolver is the pure twin for env inputs.
        let object = 3 * GIB + 7 * 1024 * 1024;
        let file = GIB + 11 * 1024 * 1024;
        let configured = siglake_storage::query_read_cache_config_from(
            limit,
            Some(&object.to_string()),
            Some(&file.to_string()),
            Some("512"),
        );
        let in_force = configured.reserved_bytes();

        assert_eq!(
            in_force,
            object + file,
            "reserved read-cache bytes came back as {} MB, not the {} MB the \
             environment actually configured",
            mb(in_force),
            mb(object + file)
        );
        assert_ne!(
            in_force,
            derived.reserved_bytes(),
            "the override and the derivation coincided at {} MB — this test \
             cannot tell them apart, so pick different values",
            mb(in_force)
        );

        // And the pool must RESPOND to that reservation rather than the
        // override moving only the number that gets published. Stated as
        // monotonicity so the assertion does not depend on exact metadata
        // cache sizing.
        let small = siglake_storage::query_pool_bytes_full(limit, 0.5, None, in_force).unwrap();
        let large = siglake_storage::query_pool_bytes_full(limit, 0.5, None, 2 * in_force).unwrap();
        assert!(
            large < small,
            "doubling the reservation did not shrink the pool: {} MB then {} MB",
            mb(small),
            mb(large)
        );
    }

    /// #3053: what turning the opt-in source-file cache ON does to the rest of
    /// the budget, at the sizing it is pointed at.
    ///
    /// The cache is off by default, so `memory_budget_for` reports it as zero
    /// and an operator considering it had no arithmetic to check — the same
    /// state the text-index caches were in before #4056, and the reason the
    /// packaged query pod is 4Gi is that its whole remainder is the pool's
    /// first-file decode reservation. Enabling the cache spends that remainder.
    #[test]
    fn enabling_the_file_cache_comes_out_of_the_pool_and_the_headroom() {
        for limit_gib in [4u64, 8, 16, 64] {
            let limit = limit_gib * GIB;
            let (recommended, entries) =
                siglake_storage::derive_file_cache_limits(Some(limit)).unwrap();
            assert!(entries > 0, "{limit_gib}Gi recommends no entries");

            let off = siglake_storage::memory_budget_for(Some(limit), 0.5);
            let on =
                siglake_storage::memory_budget_with_file_cache(Some(limit), 0.5, Some(recommended));

            assert_eq!(
                on.read_caches,
                off.read_caches + recommended,
                "{limit_gib}Gi: the file cache's bytes are not in the read-cache budget"
            );
            assert!(
                on.pool < off.pool,
                "{limit_gib}Gi: the pool did not shrink after reserving {} MB \
                 of file cache ({} MB then {} MB)",
                mb(recommended),
                mb(off.pool),
                mb(on.pool)
            );
            assert!(
                on.committed() <= limit,
                "{limit_gib}Gi over-commits with the file cache on: {} MB of {} MB",
                mb(on.committed()),
                mb(limit)
            );
            // The floor pod is the case worth naming: it has no room to give,
            // so the cache takes the headroom the process runs in rather than
            // finding bytes nobody was using.
            assert!(
                on.headroom(limit) < off.headroom(limit),
                "{limit_gib}Gi: the file cache cost the process no headroom, \
                 which would mean its bytes came from nowhere"
            );
        }

        // And it is not free at the floor: the packaged 4Gi query pod keeps a
        // pool holding one compacted file's decode estimate (1.25 GiB) only
        // while the cache is off.
        let floor = 4 * GIB;
        let (recommended, _) = siglake_storage::derive_file_cache_limits(Some(floor)).unwrap();
        let on =
            siglake_storage::memory_budget_with_file_cache(Some(floor), 0.5, Some(recommended));
        assert!(
            mb(on.pool) < 1280,
            "the 4Gi pod kept its {} MB decode reservation while also holding a \
             {} MB file cache; one of the two numbers is wrong",
            mb(on.pool),
            mb(recommended)
        );
    }

    /// Subtracting the metadata caches must have actually MOVED the pool — if it
    /// did not, the accounting change was cosmetic.
    #[test]
    fn accounting_for_metadata_caches_shrinks_the_pool() {
        let limit = 4 * GIB;
        let budget = siglake_storage::memory_budget_for(Some(limit), 0.5);
        assert!(budget.metadata_caches > 0, "no metadata budget was derived");
        // What the pool would have been when it knew only about the read caches.
        let without_meta = siglake_storage::query_pool_bytes_full(
            Some(limit),
            0.5,
            None,
            budget.read_caches + budget.text_index_caches,
        )
        .unwrap();
        assert!(
            budget.pool < without_meta,
            "pool unchanged at {} MB after subtracting {} MB of metadata caches",
            mb(budget.pool),
            mb(budget.metadata_caches)
        );
    }
}
