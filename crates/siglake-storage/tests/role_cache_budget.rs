//! A maintenance process's caches, and what the query memory pool subtracts for
//! them (#4082).
//!
//! #4056 resolved both text-index cache budgets from the pod's memory limit at
//! `siglake-query-server`'s startup. Every other process kept the fork's
//! env-or-constant fallback — 1 GiB of parsed indexes plus 256 MiB of blobs
//! whatever its limit — and, for the read caches, left
//! `reserved_cache_bytes_in_force` to fall back on the query server's RESOLVER,
//! which derives an object cache a process with the cache switched off does not
//! hold. Both are ceilings on caches a maintenance process cannot fill: the only
//! sites that insert are the fork reader's index-pruning path, reached from a
//! plan carrying a text predicate through the Iceberg table provider, and
//! maintenance plans none (a Tier-2 aggregate rebuild counts from manifest
//! stats, footers and raw pages; a delete task evaluates its predicate over a
//! `MemTable` of the candidate file's rows).
//!
//! ONE TEST, IN ORDER, IN ITS OWN BINARY. Both caches are process-global and the
//! read-cache configuration is a `OnceLock` that may be set exactly once, so the
//! pre-change arm has to be read before this binary configures anything. That
//! ordering is the negative control: the same assertions at a 1Gi pod, before
//! and after, in one build.
//!
//! Nothing here reads or writes the environment, and nothing here builds the
//! pool — `query_pool_bytes_full` is the pool's own sizing, called at the
//! packaged compactor limit rather than at this machine's.

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;

/// The compactor pod the chart packages (`deploy/helm/siglake/values.yaml`).
const COMPACTOR_LIMIT: u64 = GIB;

/// `SIGLAKE_QUERY_MEMORY_FRACTION`'s default.
const FRACTION: f64 = 0.5;

fn mib(bytes: u64) -> u64 {
    bytes / MIB
}

#[test]
fn a_maintenance_role_configures_zero_text_index_caches_and_the_pool_subtracts_that() {
    // The metadata caches are the one reservation a maintenance process really
    // does hold, and they are already derived from the limit. Taken from the
    // published budget so this arithmetic uses the same number the pool would at
    // a 1Gi pod, not this machine's.
    let metadata =
        siglake_storage::memory_budget_for(Some(COMPACTOR_LIMIT), FRACTION).metadata_caches;
    assert_eq!(mib(metadata), 128, "1Gi/8 of metadata caches");

    // ---- ARM A: nothing configured, as every non-query process was ----------
    let (_read_in_force, metadata_before, text_in_force) =
        siglake_storage::reserved_cache_bytes_in_force();
    assert_eq!(
        mib(text_in_force),
        1280,
        "an unconfigured process holds the fork's flat 1 GiB + 256 MiB of \
         text-index ceilings (if this fails, something in the test environment \
         set SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES or its blob twin)"
    );
    // The read caches at a 1Gi pod, by the fallback that answers for a process
    // which configured none: a derived object cache, a quarter of the pod.
    let fallback_read =
        siglake_storage::query_read_cache_config_from(Some(COMPACTOR_LIMIT), None, None, None)
            .reserved_bytes();
    assert_eq!(
        mib(fallback_read),
        256,
        "limit/4 of byte-range object cache"
    );

    let before = siglake_storage::query_pool_bytes_full(
        Some(COMPACTOR_LIMIT),
        FRACTION,
        None,
        fallback_read + metadata + text_in_force,
    );
    assert_eq!(
        before.map(mib),
        Some(256),
        "1.62 GiB of caches inside a 1Gi limit floors the pool at its minimum \
         and logs 'caches are configured to hold most of this machine'"
    );

    // ---- ARM B: the role resolves its own budgets --------------------------
    let config = siglake_storage::resolve_role_cache_config(
        siglake_storage::WarehouseRole::Maintenance,
        Some(COMPACTOR_LIMIT),
        None,
        None,
        None,
    );
    assert_eq!(config.text_index.reserved_bytes(), 0);
    assert_eq!(config.read.reserved_bytes(), 0);
    siglake_storage::configure_query_read_caches(config.read);
    siglake_storage::configure_text_index_caches(config.text_index);

    // What the pool subtracts is what this process configured — read back from
    // the caches and the recorded configuration, not re-derived.
    let (read, metadata_in_force, text) = siglake_storage::reserved_cache_bytes_in_force();
    assert_eq!(read, 0, "the object cache stays the operator's opt-in");
    assert_eq!(text, 0, "no parsed indexes, no blobs");
    assert_eq!(
        metadata_in_force, metadata_before,
        "the metadata caches are untouched by the role"
    );

    let after = siglake_storage::query_pool_bytes_full(
        Some(COMPACTOR_LIMIT),
        FRACTION,
        None,
        read + metadata + text,
    );
    assert_eq!(
        after.map(mib),
        Some(448),
        "half of what the metadata caches leave of a 1Gi pod — not floored"
    );
}

/// The zero is a budget, not a policy change: an operator who sized these caches
/// by hand keeps what they set, in either role.
#[test]
fn an_override_survives_the_maintenance_role() {
    let configured = siglake_storage::role_cache_config_from(
        siglake_storage::WarehouseRole::Maintenance,
        Some(COMPACTOR_LIMIT),
        Some("67108864"),
        Some("134217728"),
        None,
    );
    assert_eq!(mib(configured.read.object_cache_max_bytes), 64);
    assert_eq!(mib(configured.text_index.parsed_index_max_bytes), 128);
    assert_eq!(
        configured.text_index.puffin_blob_max_bytes, 0,
        "an unset bound is still the role's zero"
    );

    // Unparseable is off, exactly as the fork reads the same variable.
    let garbage = siglake_storage::role_cache_config_from(
        siglake_storage::WarehouseRole::Maintenance,
        Some(COMPACTOR_LIMIT),
        Some("lots"),
        Some(""),
        Some("  "),
    );
    assert_eq!(garbage.read.reserved_bytes(), 0);
    assert_eq!(garbage.text_index.reserved_bytes(), 0);
}

/// An in-process query role (`sql-direct`, `iceberg-demo`, `subscribe`) gets the
/// query server's derivation at its own limit — the caches it CAN fill are sized
/// from the pod, and the read caches stay opt-in because nothing in that binary
/// applies the scan tuning the decoded-file cache needs.
#[test]
fn an_in_process_query_role_derives_the_text_index_budgets() {
    for limit in [COMPACTOR_LIMIT, 4 * GIB, 8 * GIB, 16 * GIB] {
        let config = siglake_storage::resolve_role_cache_config(
            siglake_storage::WarehouseRole::InProcessQuery,
            Some(limit),
            None,
            None,
            None,
        );
        assert_eq!(
            config.text_index,
            siglake_storage::resolve_text_index_cache_config(Some(limit), None, None),
            "the query server's answer at {} MiB",
            mib(limit)
        );
        assert_eq!(
            config.read.reserved_bytes(),
            0,
            "no read cache is enabled merely to make its reservation agree"
        );
    }
    assert!(
        siglake_storage::resolve_role_cache_config(
            siglake_storage::WarehouseRole::InProcessQuery,
            Some(8 * GIB),
            None,
            None,
            None,
        )
        .text_index
        .reserved_bytes()
            > 0,
        "an 8Gi pod has the room for text-index caches"
    );
}
