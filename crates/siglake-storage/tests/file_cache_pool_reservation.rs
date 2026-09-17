//! Task #3053: an enabled source-file cache is subtracted from the query memory
//! pool by the process that enabled it.
//!
//! The arithmetic is covered in `query_memory_bound.rs`; this covers the WIRING,
//! which is the half that can silently disagree. `reserved_cache_bytes_in_force`
//! answers from the configuration the process RECORDED, and a process that never
//! records one falls back to re-deriving the query server's read caches — which
//! is how `siglake_query_memory_reserved_elsewhere_bytes` once published 13.56
//! GiB against a pool that had subtracted 26.
//!
//! ITS OWN BINARY, ONE TEST, IN ORDER. The recorded configuration is a
//! `OnceLock` that may be set exactly once per process, so the unconfigured arm
//! has to be read before this binary configures anything. Nothing here builds
//! the pool: `query_pool_bytes_full` is the pool's own sizing, called at a
//! chosen limit rather than at this machine's.

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;

/// A pod with room for the cache — 16Gi derives a 4 GiB object cache and a 2 GiB
/// file cache, and still leaves a pool.
const LIMIT: u64 = 16 * GIB;

/// `SIGLAKE_QUERY_MEMORY_FRACTION`'s default.
const FRACTION: f64 = 0.5;

fn mib(bytes: u64) -> u64 {
    bytes / MIB
}

#[test]
fn an_enabled_file_cache_is_what_the_pool_subtracts() {
    let (bytes, entries) = siglake_storage::derive_file_cache_limits(Some(LIMIT))
        .expect("a container limit derives a recommendation");
    assert_eq!(mib(bytes), 2048);
    assert_eq!(entries, 2048);

    // ---- ARM A: before this process records anything ----------------------
    // The read caches an unconfigured process reports: the fallback re-derives
    // the query server's answer, in which the file cache is off.
    let unconfigured =
        siglake_storage::resolve_query_read_cache_config(Some(LIMIT), None, None, None);
    assert_eq!(unconfigured.file_cache_max_bytes, None);
    let object_only = unconfigured.reserved_bytes();
    assert_eq!(mib(object_only), 4096, "limit/4 of byte-range object cache");

    // ---- ARM B: the process enables the cache ------------------------------
    let configured = siglake_storage::resolve_query_read_cache_config(
        Some(LIMIT),
        None,
        Some(bytes),
        Some(entries),
    );
    assert_eq!(configured.file_cache_max_bytes, Some(bytes));
    assert_eq!(configured.file_cache_max_entries, Some(entries));
    siglake_storage::configure_query_read_caches(configured);

    let (read_in_force, _metadata, _text) = siglake_storage::reserved_cache_bytes_in_force();
    assert_eq!(
        read_in_force,
        object_only + bytes,
        "the pool subtracts {} MiB while this process configured {} MiB of \
         object cache plus {} MiB of file cache",
        mib(read_in_force),
        mib(object_only),
        mib(bytes)
    );

    // And the subtraction has to MOVE the pool; a reservation that is reported
    // without being priced is the defect this whole accounting exists to stop.
    let metadata = siglake_storage::memory_budget_for(Some(LIMIT), FRACTION).metadata_caches;
    let text = siglake_storage::memory_budget_for(Some(LIMIT), FRACTION).text_index_caches;
    let without = siglake_storage::query_pool_bytes_full(
        Some(LIMIT),
        FRACTION,
        None,
        object_only + metadata + text,
    )
    .unwrap();
    let with = siglake_storage::query_pool_bytes_full(
        Some(LIMIT),
        FRACTION,
        None,
        read_in_force + metadata + text,
    )
    .unwrap();
    assert_eq!(
        with + bytes / 2,
        without,
        "a {} MiB cache must cost the pool half of itself at fraction {FRACTION} \
         ({} MiB then {} MiB)",
        mib(bytes),
        mib(without),
        mib(with)
    );
}
