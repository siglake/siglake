//! WS-6 real-time queryability: a WAL-backed in-memory buffer provider.
//!
//! Without it a query sees rows only **after** the compactor commits a
//! WAL→Iceberg snapshot, so ingest-to-queryability lag is the seal threshold
//! plus the compaction cycle. This module closes that gap by exposing the
//! **un-committed** WAL segments — those in `sealed/` and `processing/`, i.e.
//! sealed but not yet committed to Iceberg — as a queryable Arrow table, and
//! unioning it with the Iceberg snapshot under the table's own name. That
//! covers `events` ([`UnionEventsProvider::try_new`], WAL directory from
//! [`resolve_tenant_wal_dir`]) and every managed user index a query references
//! (#61: [`UnionEventsProvider::try_new_for_index`], which maps the
//! carrier-shaped WAL batches through the index's doc-mapping first, over the
//! per-index directory from [`resolve_index_wal_dir`]). `query_audit` is not
//! buffered.
//!
//! ## Correctness model
//!
//! A segment lives in exactly one of `sealed/ → processing/ → committed/` at
//! any instant (atomic renames). The compactor commits a segment's rows to
//! Iceberg as it moves the file into `committed/`. So:
//!
//! - `sealed/` + `processing/` — **not yet** in Iceberg ⇒ the buffer.
//! - `committed/` — already in Iceberg ⇒ excluded (would double-count).
//! - `active/` — not sealed (no IPC EOS, racy to read) ⇒ excluded.
//!
//! The buffer ∪ Iceberg is therefore disjoint **except** for a narrow race: a
//! segment whose Iceberg commit succeeded but whose `finish_segment` rename
//! into `committed/` hasn't landed yet would briefly be counted twice. WS-6
//! closes that window **without** a lossy row-level dedup: the compactor stamps
//! each commit's snapshot summary with the basenames of the WAL segments it
//! consumed ([`siglake_storage::iceberg::CONSUMED_SEGMENTS_PROP`]), and the
//! buffer excludes any `processing/`/`sealed/` segment in that set. Because the
//! exclusion set is read from the *same* snapshot the base provider serves, the
//! moment a segment's rows are in Iceberg the snapshot also names it, so it
//! drops out of the buffer atomically. The buffer reuses
//! `siglake_wal::read_segment`, so each segment's WS-8 CRC sidecar is validated
//! on the read path.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;

use siglake_wal::{COMMITTED_DIR, PROCESSING_DIR, SEALED_DIR};

/// How long a `committed/` segment stays buffer-eligible after its commit
/// (mtime-based). Covers the #61 transition race: the compactor moves a
/// segment to `committed/` the moment its rows are in Iceberg, but a
/// stale-while-revalidate table cache may serve a pre-commit snapshot for up
/// to the stale ceiling (12x the 5s TTL = 60s) — dropping the segment from
/// the buffer at the rename would make its rows VANISH until the refresh
/// lands. Recently-committed segments therefore stay in the buffer and are
/// excluded via the CUMULATIVE consumed set instead: the moment the serving
/// snapshot's history names a segment, it drops out — exact through the whole
/// transition. Must exceed the stale ceiling; the compactor's committed-sweep
/// floor (60s soft) should stay at or above the ceiling for the same reason.
const RECENT_COMMITTED_WINDOW: std::time::Duration = std::time::Duration::from_secs(300);

/// Resolve the WAL directory that holds a tenant's `sealed/`/`processing/`
/// subdirs. Tenant-aware layout is `<root>/<tenant>/sealed/`; the legacy
/// single-tenant layout has `<root>/sealed/`. Returns the tenant subdir when it
/// looks like a WAL root, else `root` (flat layout / default tenant).
pub fn resolve_tenant_wal_dir(root: &Path, tenant: &str) -> PathBuf {
    let candidate = root.join(tenant);
    if candidate.join(SEALED_DIR).is_dir() {
        candidate
    } else {
        root.to_path_buf()
    }
}

/// Resolve a USER INDEX's WAL directory (#61 per-index buffer). The ingester's
/// per-index writers live at `<root>/<tenant>/<index>/sealed/` (the same layout
/// the mirror key `{tenant}/{index}` reflects); a legacy/flat deployment may
/// have `<root>/<index>/sealed/`. Returns whichever exists, else the
/// tenant-scoped path (whose missing `sealed/` yields an empty buffer).
pub fn resolve_index_wal_dir(root: &Path, tenant: &str, index: &str) -> PathBuf {
    let tenant_scoped = root.join(tenant).join(index);
    if tenant_scoped.join(SEALED_DIR).is_dir() {
        return tenant_scoped;
    }
    let flat = root.join(index);
    if flat.join(SEALED_DIR).is_dir() {
        return flat;
    }
    tenant_scoped
}

/// Whether `wal_dir`'s segments belong to the table `table_uuid` identifies.
///
/// #2661: [`resolve_index_wal_dir`] keys the directory by the index NAME, and
/// `DELETE /api/v1/indexes/{id}` leaves it on disk. Recreating the id points a
/// brand-new table at the dropped incarnation's segments, and the replacement's
/// empty consumed set excludes none of them, so both the never-committed
/// segments and the ones the DROPPED table already committed read back as the
/// replacement's rows. The directory's owner marker is the identity its name
/// does not carry.
///
/// An unmarked directory serves as before: the marker is written by the drain,
/// so a deployment upgrading into this has none until its first drain cycle,
/// and refusing those would blank every in-flight buffer on rollout.
pub fn wal_dir_serves_table(wal_dir: &Path, table_uuid: &str) -> bool {
    match siglake_wal::classify_wal_owner(wal_dir, table_uuid) {
        siglake_wal::WalOwner::Owned | siglake_wal::WalOwner::Unmarked => true,
        siglake_wal::WalOwner::Stale(dropped) => {
            tracing::debug!(
                dir = %wal_dir.display(),
                dropped_table = %dropped,
                live_table = %table_uuid,
                "WAL directory belongs to a dropped incarnation of this index; \
                 serving the committed view only"
            );
            metrics::counter!("siglake_query_wal_buffer_stale_owner_total").increment(1);
            false
        }
    }
}

/// Drop the segments in `segments` that name a table other than `owner`.
///
/// #2693: the directory marker is only as current as the last drain that
/// re-stamped it, and an ingester still holding the dropped incarnation's
/// writer keeps sealing into the directory after that. Each segment carries the
/// table it was opened for in its frame header, so the refusal is per segment,
/// not per directory. Unstamped segments (legacy, or a writer with no identity
/// to bind) serve — the same "no opinion" rule the directory marker follows.
fn retain_owned_segments(segments: &mut Vec<PathBuf>, owner: Option<&str>) {
    let Some(uuid) = owner else {
        return;
    };
    let before = segments.len();
    segments.retain(|p| match siglake_wal::classify_segment_owner(p, uuid) {
        siglake_wal::WalOwner::Owned | siglake_wal::WalOwner::Unmarked => true,
        siglake_wal::WalOwner::Stale(dropped) => {
            tracing::debug!(
                segment = %p.display(),
                dropped_table = %dropped,
                live_table = %uuid,
                "WAL segment was sealed for a dropped incarnation of this index; not buffered"
            );
            false
        }
    });
    let refused = before - segments.len();
    if refused > 0 {
        metrics::counter!("siglake_query_wal_buffer_stale_segments_total")
            .increment(refused as u64);
    }
}

/// [`resolve_index_wal_dir`], refusing a directory whose owner marker names a
/// table other than `table_uuid`. `None` = this index has no buffer to serve.
pub fn resolve_owned_index_wal_dir(
    root: &Path,
    tenant: &str,
    index: &str,
    table_uuid: &str,
) -> Option<PathBuf> {
    let dir = resolve_index_wal_dir(root, tenant, index);
    wal_dir_serves_table(&dir, table_uuid).then_some(dir)
}

/// Whether `wal_dir` currently holds any un-committed segment (`sealed/` or
/// `processing/` `.arrow` file). Cheap readdir — the fast-path guard uses this
/// so metadata-served answers stay on when the buffer would contribute nothing
/// (drained steady state) and yield to the exact union plan when rows are in
/// flight.
pub fn has_unserved_segments(wal_dir: &Path, exclude: &HashSet<String>) -> bool {
    list_uncommitted_segments(wal_dir, exclude, None)
        .map(|(segments, _, _)| !segments.is_empty())
        .unwrap_or(true) // unreadable dir: conservatively assume in-flight rows
}

/// List the un-committed segment files (`sealed/` + `processing/`, `.arrow`
/// only) under `wal_dir`, skipping any whose basename is in `exclude` (segments
/// the serving Iceberg snapshot already committed — see
/// [`siglake_storage::iceberg::CONSUMED_SEGMENTS_PROP`]) and, when `time_bound`
/// is set, any WS-8 framed segment whose `[min_ts, max_ts]` header range falls
/// entirely outside `[lo, hi)` (a query with a `timestamp` predicate skips
/// segments it can't match without decoding their bodies). Legacy unframed
/// segments carry no header range and are always kept. Missing subdirs yield
/// nothing. Returns `(segments, excluded_count, pruned_count)`.
fn list_uncommitted_segments(
    wal_dir: &Path,
    exclude: &HashSet<String>,
    time_bound: Option<(i64, i64)>,
) -> Result<(Vec<PathBuf>, usize, usize)> {
    let mut out = Vec::new();
    let mut excluded = 0usize;
    let mut pruned = 0usize;
    for sub in [SEALED_DIR, PROCESSING_DIR, COMMITTED_DIR] {
        let dir = wal_dir.join(sub);
        if !dir.is_dir() {
            continue;
        }
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("read_dir {}", dir.display()))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) != Some("arrow") {
                continue;
            }
            // `committed/`: only RECENT segments are buffer-eligible (the
            // transition race window); older ones are guaranteed present in
            // any snapshot the bounded-staleness cache can serve.
            if sub == COMMITTED_DIR {
                let recent = entry
                    .metadata()
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| t.elapsed().ok())
                    .is_some_and(|age| age < RECENT_COMMITTED_WINDOW);
                if !recent {
                    continue;
                }
            }
            // A committed segment briefly lingers in `processing/` before its
            // rename to `committed/` (and recently-committed segments are
            // listed deliberately); the consumed-set exclusion keeps
            // buffer ∪ Iceberg disjoint without a lossy row-level dedup.
            if let Some(name) = p.file_name().and_then(|f| f.to_str()) {
                if exclude.contains(name) {
                    excluded += 1;
                    continue;
                }
            }
            // WS-8 time-range prune: skip a framed segment whose header min/max
            // can't intersect the query window. Conservative — only prune when
            // the segment is *entirely* outside, so a kept-but-non-matching
            // segment is just filtered by the re-applied predicate above.
            if let Some((lo, hi)) = time_bound {
                if let Ok(Some(meta)) = siglake_wal::read_segment_meta(&p) {
                    if meta.max_ts_nanos < lo || meta.min_ts_nanos >= hi {
                        pruned += 1;
                        continue;
                    }
                }
            }
            out.push(p);
        }
    }
    out.sort();
    Ok((out, excluded, pruned))
}

/// Ceiling on how many segment entries the budget gate will EXAMINE before
/// refusing the buffer (`SIGLAKE_BUFFER_DELTA_MAX_SEGMENTS`, default 4096;
/// 0 disables).
///
/// The byte budget alone does not bound the work. Many small segments can sit
/// far under the byte ceiling while still costing one syscall each, and the
/// 2026-08-16 fleet measured ~81 kept + ~81 pruned + ~80 excluded entries walked
/// PER QUERY against only 121 segments on disk. At 121 that is free; at the
/// round's peak backlog of ~50,000 it was 60s+ and every buffer-consulting query
/// returned 504 while the drain caught up.
fn buffer_delta_max_segments() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("SIGLAKE_BUFFER_DELTA_MAX_SEGMENTS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4096)
    })
}

/// Size the buffer only until the verdict is decided.
///
/// The point is that BOTH answers are reached in bounded work:
///
/// - **Over budget** — stop at the first segment that crosses the ceiling. A
///   50,000-segment backlog is refused after examining a few hundred, not
///   50,000, because whatever the total is, it is already too big.
/// - **Under budget** — the byte ceiling caps how many segments can fit, so the
///   walk is bounded by `max / segment_size`, plus the count cap for the
///   many-tiny-segments case the byte ceiling cannot bound.
///
/// This is what the previous shape lacked: it collected EVERY segment, then
/// summed. Under backlog the answer was known after a handful of entries and it
/// walked tens of thousands anyway — with a `File::open` each when a time bound
/// was present.
fn size_until_decided(
    wal_dir: &Path,
    exclude: &HashSet<String>,
    time_bound: Option<(i64, i64)>,
    max: u64,
) -> (bool, u64) {
    size_until_decided_capped(
        wal_dir,
        exclude,
        time_bound,
        max,
        buffer_delta_max_segments(),
    )
}

/// [`size_until_decided`] with the segment cap injected. The cap is read through
/// a `OnceLock`, so a test that sets the env var after any other test has called
/// it silently measures the default -- which is how a test ends up asserting
/// nothing while passing.
fn size_until_decided_capped(
    wal_dir: &Path,
    exclude: &HashSet<String>,
    time_bound: Option<(i64, i64)>,
    max: u64,
    seg_cap: usize,
) -> (bool, u64) {
    let mut total: u64 = 0;
    let mut examined: usize = 0;
    for sub in [SEALED_DIR, PROCESSING_DIR, COMMITTED_DIR] {
        let dir = wal_dir.join(sub);
        if !dir.is_dir() {
            continue;
        }
        let Ok(rd) = std::fs::read_dir(&dir) else {
            // Unsizeable means over budget: never decode what cannot be sized.
            return (false, total);
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) != Some("arrow") {
                continue;
            }
            if let Some(name) = p.file_name().and_then(|f| f.to_str()) {
                if exclude.contains(name) {
                    continue;
                }
            }
            examined += 1;
            if seg_cap > 0 && examined > seg_cap {
                metrics::counter!("siglake_query_wal_buffer_refused_segment_cap_total")
                    .increment(1);
                return (false, total);
            }
            // Time pruning needs a header read, so only do it once the byte
            // total is close enough to matter; a segment outside the window
            // contributes nothing either way.
            if let Some((lo, hi)) = time_bound {
                if let Ok(Some(meta)) = siglake_wal::read_segment_meta(&p) {
                    if meta.max_ts_nanos < lo || meta.min_ts_nanos >= hi {
                        continue;
                    }
                }
            }
            total = total.saturating_add(entry.metadata().map(|m| m.len()).unwrap_or(0));
            if max > 0 && total > max {
                metrics::counter!("siglake_query_wal_buffer_refused_bytes_total").increment(1);
                metrics::histogram!("siglake_query_wal_buffer_segments_examined")
                    .record(examined as f64);
                return (false, total);
            }
        }
    }
    metrics::histogram!("siglake_query_wal_buffer_segments_examined").record(examined as f64);
    (true, total)
}

/// Ceiling on the ON-DISK bytes of buffered segments a single request may
/// decode (`SIGLAKE_BUFFER_DELTA_MAX_BYTES`, default 2 GiB; 0 disables the
/// guard). The buffer normally holds seconds of data, but after an outage it
/// can hold the entire backlog — the Phase-4 validation round had the whole
/// 200G corpus sealed-uncommitted after a node wedge, and both buffer paths
/// (the fast-path delta and the union MemTable) MATERIALIZE the decoded rows
/// (×4–6 memory). Past the ceiling the request serves the Iceberg-only view
/// (bounded-staleness degradation: rows appear as the drain commits), which
/// is the only honest answer during backlog recovery.
fn buffer_delta_max_bytes() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("SIGLAKE_BUFFER_DELTA_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(2 * 1024 * 1024 * 1024)
    })
}

/// Whether the surviving uncommitted segments under `wal_dir` fit the decode
/// budget. Returns `(within_budget, total_bytes)`; an unreadable listing
/// counts as over budget (don't decode what you can't size).
pub fn buffer_within_budget(
    wal_dir: &Path,
    exclude: &HashSet<String>,
    time_bound: Option<(i64, i64)>,
) -> (bool, u64) {
    buffer_within_budget_with_max(wal_dir, exclude, time_bound, buffer_delta_max_bytes())
}

/// [`buffer_within_budget`] with the budget injected, so the over-budget path —
/// the only one that reads segment headers — is testable without a 2 GiB fixture.
pub(crate) fn buffer_within_budget_with_max(
    wal_dir: &Path,
    exclude: &HashSet<String>,
    time_bound: Option<(i64, i64)>,
    max: u64,
) -> (bool, u64) {
    // CHEAP PATH FIRST. An unwindowed size is a `stat` per segment; a windowed
    // one is a `File::open` + header read EACH. If the WHOLE buffer fits, no
    // window can make it not fit and no headers need reading.
    //
    // Both calls are BOUNDED (see `size_until_decided`): they stop at the first
    // segment that crosses the ceiling, or at the segment-count cap. The
    // previous shape collected every uncommitted segment and then summed, so
    // under the 2026-08-16 round's ~50,000-segment backlog every
    // buffer-consulting query walked all of them -- with an open each once a
    // time bound was present -- and returned 504 at the 60s timeout while
    // aggregates and unfiltered browses kept working.
    if time_bound.is_some() {
        let (ok, total) = size_until_decided(wal_dir, exclude, None, max);
        if ok {
            return (true, total);
        }
    }
    size_until_decided(wal_dir, exclude, time_bound, max)
}

/// #78: sorted basenames of the segments a buffer read over `wal_dir` would
/// decode right now (post consumed-set exclusion, no time pruning). This is
/// the cache key for [`BufferDeltaCache`]: sealed segments are immutable and
/// only ever *renamed* between subdirs, so the surviving basename set fully
/// determines the decoded output for a given target schema + carrier config.
pub fn uncommitted_segment_names(
    wal_dir: &Path,
    exclude: &HashSet<String>,
    owner: Option<&str>,
) -> Result<Vec<String>> {
    uncommitted_segment_names_with_bound(wal_dir, exclude, None, owner)
}

/// [`uncommitted_segment_names`] under the #2661 owner gate: a directory whose
/// marker names a table other than `owner` contributes nothing, so its listing
/// is empty however many segments a stale writer has left in it. `None` for a
/// table with no identity to check (`events`, whose directory is the tenant's
/// and is never recreated under a new table).
pub fn owned_uncommitted_segment_names(
    wal_dir: &Path,
    exclude: &HashSet<String>,
    owner: Option<&str>,
) -> Result<Vec<String>> {
    if owner.is_some_and(|uuid| !wal_dir_serves_table(wal_dir, uuid)) {
        return Ok(Vec::new());
    }
    uncommitted_segment_names(wal_dir, exclude, owner)
}

fn uncommitted_segment_names_with_bound(
    wal_dir: &Path,
    exclude: &HashSet<String>,
    time_bound: Option<(i64, i64)>,
    owner: Option<&str>,
) -> Result<Vec<String>> {
    let (mut segments, _, _) = list_uncommitted_segments(wal_dir, exclude, time_bound)?;
    retain_owned_segments(&mut segments, owner);
    let mut names: Vec<String> = segments
        .iter()
        .filter_map(|p| p.file_name().and_then(|f| f.to_str()).map(String::from))
        .collect();
    names.sort_unstable();
    Ok(names)
}

/// #78: per-table cache of decoded, schema-aligned buffer batches.
///
/// With a non-empty buffer, every fast-path request pays a full
/// decode of the sealed segments (`load_buffer_delta`) — ~17ms on the Phase-4
/// board, dwarfing the sub-ms metadata answer it decorates. The decoded
/// output is a **pure function** of (surviving segment basenames, target
/// schema, carrier config): segments are immutable once sealed, exclusion is
/// captured by which names survive, and renames don't change content. So one
/// slot per WAL dir, compared on that whole key, is exact — any seal, commit,
/// consumed-set growth, recency ageing, schema migration, or index-config
/// change alters the key and misses to a fresh read. This is *not* a TTL'd
/// result cache (standing invariant): nothing here expires by time; entries
/// are replaced on key change and evicted when the buffer drains.
///
/// Memory: at most one decoded buffer per actively-queried table, bounded by
/// the seal window — the same rows `load_buffer_delta` previously decoded
/// per request.
#[derive(Default)]
pub struct BufferDeltaCache {
    entries: std::sync::Mutex<HashMap<PathBuf, BufferDeltaCacheEntry>>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

struct BufferDeltaCacheEntry {
    names: Vec<String>,
    schema: SchemaRef,
    carrier: Option<siglake_core::index_config::IndexConfig>,
    /// The table the entry was decoded FOR (#2693). Part of the key: the same
    /// directory, the same surviving basenames and the same schema can belong
    /// to a different table after a `DELETE`+`POST`, and the per-segment owner
    /// gate that produced these batches was run against the previous one.
    owner: Option<String>,
    batches: Vec<RecordBatch>,
}

impl BufferDeltaCache {
    /// Return decoded batches for the current `(wal_dir, exclude, time_bound)`
    /// state, decoding and installing them only when the exact segment set is
    /// not cached.
    ///
    /// A bounded read may reuse a cached unbounded set when that entry exactly
    /// matches the current uncommitted names. The returned batches are then a
    /// superset of the time window; this is safe for the query path because the
    /// SQL timestamp predicate still filters rows, while avoiding a second
    /// decode after `load_buffer_delta` populated the unbounded entry.
    pub fn get_or_load_for_query(
        &self,
        wal_dir: &Path,
        target_schema: &SchemaRef,
        exclude: &HashSet<String>,
        time_bound: Option<(i64, i64)>,
        carrier: Option<&siglake_core::index_config::IndexConfig>,
        owner: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        self.get_or_load_for_query_keyed(
            wal_dir,
            target_schema,
            exclude,
            time_bound,
            carrier,
            owner,
        )
        .map(|(batches, _)| batches)
    }

    /// [`Self::get_or_load_for_query`] that also hands back the sorted segment
    /// basenames the returned batches correspond to — the listing this read
    /// actually observed.
    ///
    /// A caller that draws a CONCLUSION from the batches (e.g. "nothing is in
    /// flight", #1562) needs the set that conclusion was taken over, and it must
    /// be the one this call read: re-listing afterwards can observe a different
    /// directory. An empty batch vec still reports its listing — an empty name
    /// list proves a drained buffer, while a non-empty one that decoded to no
    /// rows does not.
    pub fn get_or_load_for_query_keyed(
        &self,
        wal_dir: &Path,
        target_schema: &SchemaRef,
        exclude: &HashSet<String>,
        time_bound: Option<(i64, i64)>,
        carrier: Option<&siglake_core::index_config::IndexConfig>,
        owner: Option<&str>,
    ) -> Result<(Vec<RecordBatch>, Vec<String>)> {
        let names = uncommitted_segment_names_with_bound(wal_dir, exclude, time_bound, owner)?;
        if names.is_empty() {
            if time_bound.is_none() {
                self.evict(wal_dir);
            }
            return Ok((Vec::new(), names));
        }
        if let Some(cached) = self.get(wal_dir, &names, target_schema, carrier, owner) {
            return Ok((cached, names));
        }

        // `load_buffer_delta` has already populated this entry on the normal
        // distributed path. A time bound only prunes segment decoding; the
        // query executed over these batches still carries the exact predicate.
        if time_bound.is_some() {
            let all_names = uncommitted_segment_names(wal_dir, exclude, owner)?;
            if all_names != names {
                if let Some(cached) = self.get(wal_dir, &all_names, target_schema, carrier, owner) {
                    return Ok((cached, all_names));
                }
            }
        }

        let (batches, read_names) =
            read_buffer_batches_keyed(wal_dir, target_schema, exclude, time_bound, carrier, owner)?;
        self.insert(
            wal_dir.to_path_buf(),
            read_names.clone(),
            target_schema.clone(),
            carrier.cloned(),
            owner.map(str::to_string),
            batches.clone(),
        );
        Ok((batches, read_names))
    }

    /// Cached batches for `wal_dir` iff the key (surviving basenames + schema
    /// + carrier) matches exactly. Batch clones are shallow (Arc'd columns).
    pub fn get(
        &self,
        wal_dir: &Path,
        names: &[String],
        schema: &SchemaRef,
        carrier: Option<&siglake_core::index_config::IndexConfig>,
        owner: Option<&str>,
    ) -> Option<Vec<RecordBatch>> {
        let entries = self.entries.lock().expect("buffer delta cache poisoned");
        let e = entries.get(wal_dir)?;
        if e.names == names
            && &e.schema == schema
            && e.carrier.as_ref() == carrier
            && e.owner.as_deref() == owner
        {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            metrics::counter!("siglake_query_buffer_delta_cache_total", "result" => "hit")
                .increment(1);
            Some(e.batches.clone())
        } else {
            None
        }
    }

    /// Install the freshly-decoded batches under the names that were actually
    /// read (single slot per dir — replaces any stale entry).
    pub fn insert(
        &self,
        wal_dir: PathBuf,
        names: Vec<String>,
        schema: SchemaRef,
        carrier: Option<siglake_core::index_config::IndexConfig>,
        owner: Option<String>,
        batches: Vec<RecordBatch>,
    ) {
        self.misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics::counter!("siglake_query_buffer_delta_cache_total", "result" => "miss")
            .increment(1);
        self.entries
            .lock()
            .expect("buffer delta cache poisoned")
            .insert(
                wal_dir,
                BufferDeltaCacheEntry {
                    names,
                    schema,
                    carrier,
                    owner,
                    batches,
                },
            );
    }

    /// Drop the entry for a drained buffer (frees the decoded rows).
    pub fn evict(&self, wal_dir: &Path) {
        self.entries
            .lock()
            .expect("buffer delta cache poisoned")
            .remove(wal_dir);
    }

    /// (hits, misses) since construction — test observability.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(std::sync::atomic::Ordering::Relaxed),
            self.misses.load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

/// Reorder/extend a batch's columns to match `target` exactly: columns are
/// selected by name, and any field absent from the batch (e.g. a promoted
/// attribute column the WAL doesn't carry) is filled with nulls. Row count is
/// preserved. Errors only if a present column's type is incompatible at
/// `RecordBatch::try_new` time.
fn align_batch_to_schema(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let bschema = batch.schema();
    // Fast path: identical field order + names ⇒ no copy.
    if bschema.fields().len() == target.fields().len()
        && bschema
            .fields()
            .iter()
            .zip(target.fields())
            .all(|(a, b)| a.name() == b.name())
    {
        return Ok(batch.clone());
    }
    let n = batch.num_rows();
    let by_name: HashMap<&str, usize> = bschema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();
    let columns = target
        .fields()
        .iter()
        .map(|field| match by_name.get(field.name().as_str()) {
            Some(&i) => batch.column(i).clone(),
            None => arrow::array::new_null_array(field.data_type(), n),
        })
        .collect();
    RecordBatch::try_new(target.clone(), columns).context("align WAL buffer batch to events schema")
}

/// Read the un-committed WAL segments under `wal_dir` into batches aligned to
/// `target_schema`. `exclude` drops already-committed segments and `time_bound`
/// (`Some((lo, hi))` ns, half-open) prunes framed segments outside the query's
/// `timestamp` window (WS-8). Returns an empty vec when nothing survives. A
/// single unreadable/corrupt segment is skipped (logged) rather than failing the
/// whole query — real-time visibility is best-effort.
pub fn read_buffer_batches(
    wal_dir: &Path,
    target_schema: &SchemaRef,
    exclude: &HashSet<String>,
    time_bound: Option<(i64, i64)>,
    carrier: Option<&siglake_core::index_config::IndexConfig>,
    owner: Option<&str>,
) -> Result<Vec<RecordBatch>> {
    read_buffer_batches_keyed(wal_dir, target_schema, exclude, time_bound, carrier, owner)
        .map(|(batches, _)| batches)
}

/// [`read_buffer_batches`] variant that also returns the sorted basenames of
/// the segments actually read — the exact [`BufferDeltaCache`] key for the
/// returned batches (re-listing between a lookup and the read could observe a
/// different set; keying on the read set keeps the entry pure).
pub fn read_buffer_batches_keyed(
    wal_dir: &Path,
    target_schema: &SchemaRef,
    exclude: &HashSet<String>,
    time_bound: Option<(i64, i64)>,
    carrier: Option<&siglake_core::index_config::IndexConfig>,
    owner: Option<&str>,
) -> Result<(Vec<RecordBatch>, Vec<String>)> {
    let (mut segments, excluded, pruned) = list_uncommitted_segments(wal_dir, exclude, time_bound)?;
    // #2693: refuse per SEGMENT, after the listing gates. The names this read
    // reports (and keys its cache on) are the ones it decoded.
    retain_owned_segments(&mut segments, owner);
    metrics::counter!("siglake_query_wal_buffer_segments_excluded_total")
        .increment(excluded as u64);
    // Both arms always emitted, including zero. The 2026-08-10 round could not
    // distinguish "pruning worked" from "pruning never ran" because the counter
    // is absent from a scrape until first incremented — and absent is exactly
    // what a total failure looks like. A metric that only appears on success
    // cannot report failure.
    metrics::counter!("siglake_query_wal_buffer_segments_time_pruned_total")
        .increment(pruned as u64);
    metrics::counter!("siglake_query_wal_buffer_segments_kept_total")
        .increment(segments.len() as u64);
    let mut names = Vec::with_capacity(segments.len());
    let mut batches = Vec::new();
    for seg in segments {
        match siglake_wal::read_segment(&seg) {
            Ok(segment_batches) => {
                if let Some(name) = seg.file_name().and_then(|f| f.to_str()) {
                    names.push(name.to_string());
                }
                for b in segment_batches {
                    if b.num_rows() == 0 {
                        continue;
                    }
                    // #61: a user index's WAL segments carry the CARRIER
                    // (events-shaped) schema; apply the same doc-mapping the
                    // compactor applies at commit so the buffered rows match
                    // what the committed rows will become.
                    let b = match carrier {
                        Some(config) => siglake_core::mapping::map_carrier_batch(&b, config)
                            .context("map carrier batch for index buffer")?,
                        None => b,
                    };
                    batches.push(align_batch_to_schema(&b, target_schema)?);
                }
            }
            Err(e) => {
                metrics::counter!("siglake_query_wal_buffer_segment_errors_total").increment(1);
                tracing::warn!(error = %e, segment = %seg.display(),
                    "skipping unreadable WAL buffer segment");
            }
        }
    }
    names.sort_unstable();
    Ok((batches, names))
}

/// Evaluate `filters` against in-memory buffer batches.
///
/// Returns the surviving rows. The buffer is small by construction (the decode
/// budget guarantees it), so this is cheap relative to the union it may avoid.
fn filter_batches(
    batches: &[RecordBatch],
    filters: &[Expr],
    schema: &SchemaRef,
    state: &dyn Session,
) -> DFResult<Vec<RecordBatch>> {
    if filters.is_empty() || batches.is_empty() {
        return Ok(batches.to_vec());
    }
    let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let mut mask: Option<arrow_array::BooleanArray> = None;
        for f in filters {
            let phys = state.create_physical_expr(f.clone(), &df_schema)?;
            let v = phys.evaluate(b)?.into_array(b.num_rows())?;
            let ba = v
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .ok_or_else(|| DataFusionError::Internal("filter did not yield boolean".into()))?
                .clone();
            mask = Some(match mask {
                None => ba,
                Some(prev) => arrow::compute::kernels::boolean::and(&prev, &ba)?,
            });
        }
        match mask {
            Some(m) => out.push(arrow::compute::filter_record_batch(b, &m)?),
            None => out.push(b.clone()),
        }
    }
    Ok(out)
}

/// A [`TableProvider`] that unions a base provider (the Iceberg `events`
/// snapshot) with an in-memory buffer of un-committed WAL rows. The buffer is
/// read **lazily in `scan`**, where DataFusion supplies the query's filters — so
/// a `timestamp` predicate prunes WAL segments by their WS-8 header range before
/// any body is decoded (and the read happens afresh per query, matching the
/// per-request `SessionContext` semantics). When nothing survives, `scan`
/// returns the base plan unchanged, so the real-time path adds zero union
/// overhead once data has been committed.
pub struct UnionEventsProvider {
    base: Arc<dyn TableProvider>,
    wal_dir: PathBuf,
    /// Segment basenames already in the serving Iceberg snapshot (excluded).
    exclude: HashSet<String>,
    schema: SchemaRef,
    /// #61: set for a USER INDEX buffer — the doc-mapping applied to the
    /// carrier-schema WAL batches (the same mapping the commit path applies).
    /// `None` for the events table, whose WAL batches are already
    /// events-shaped.
    carrier: Option<siglake_core::index_config::IndexConfig>,
    /// Injected decode ceiling; `None` reads the configured one. The sibling of
    /// `load_buffer_delta_with_max`: the two gates must be compared at ONE
    /// ceiling for a backlog regression to mean anything, and the configured
    /// value is a 2 GiB `OnceLock` no test can move.
    max_bytes: Option<u64>,
    /// #2693: the table this provider serves, when it has one. Segments whose
    /// frame header names a DIFFERENT table are a stale writer's, and are left
    /// out of the union. `None` for `events`, whose directory is the tenant's
    /// and is never handed to a new table.
    owner: Option<String>,
}

impl std::fmt::Debug for UnionEventsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionEventsProvider")
            .field("wal_dir", &self.wal_dir)
            .finish()
    }
}

impl UnionEventsProvider {
    /// Build a unified provider over `base` (Iceberg) and the WAL buffer at
    /// `wal_dir`. Construction is cheap — the WAL is read in [`scan`](Self::scan).
    pub fn try_new(
        base: Arc<dyn TableProvider>,
        wal_dir: &Path,
        exclude: &HashSet<String>,
    ) -> Result<Self> {
        Self::try_new_inner(base, wal_dir, exclude, None, None, None)
    }

    /// [`Self::try_new`] with the decode ceiling injected, so a backlog
    /// regression can price the scan gate and the fast-path delta gate against
    /// the SAME ceiling without a 2 GiB fixture.
    #[cfg(test)]
    pub(crate) fn try_new_with_max_bytes(
        base: Arc<dyn TableProvider>,
        wal_dir: &Path,
        exclude: &HashSet<String>,
        max_bytes: u64,
    ) -> Result<Self> {
        Self::try_new_inner(base, wal_dir, exclude, None, Some(max_bytes), None)
    }

    /// #61: buffer provider for a USER INDEX — WAL batches are carrier-shaped
    /// and run through `config`'s doc-mapping before the union.
    pub fn try_new_for_index(
        base: Arc<dyn TableProvider>,
        wal_dir: &Path,
        exclude: &HashSet<String>,
        config: siglake_core::index_config::IndexConfig,
        owner: Option<&str>,
    ) -> Result<Self> {
        Self::try_new_inner(base, wal_dir, exclude, Some(config), None, owner)
    }

    fn try_new_inner(
        base: Arc<dyn TableProvider>,
        wal_dir: &Path,
        exclude: &HashSet<String>,
        carrier: Option<siglake_core::index_config::IndexConfig>,
        max_bytes: Option<u64>,
        owner: Option<&str>,
    ) -> Result<Self> {
        let schema = base.schema();
        Ok(Self {
            base,
            wal_dir: wal_dir.to_path_buf(),
            exclude: exclude.clone(),
            schema,
            carrier,
            max_bytes,
            owner: owner.map(str::to_string),
        })
    }
}

/// Derive a half-open `[lo, hi)` ns window from a scan's `timestamp` filters, or
/// `None` when there's no usable bound (⇒ no pruning). Reuses the cost-model's
/// exact-bound extraction so the buffer prunes on exactly the predicates the
/// estimator already understands.
fn time_bound_from_filters(
    filters: &[Expr],
    schema: &datafusion::common::DFSchema,
) -> Option<(i64, i64)> {
    let mut lo = i64::MIN;
    let mut hi = i64::MAX;
    let mut found = false;
    for f in filters {
        if let Some(b) = crate::cost::extract_exact_time_bounds_from_expr(f, schema) {
            if let Some(s) = b.start.and_then(|d| d.timestamp_nanos_opt()) {
                lo = lo.max(s);
                found = true;
            }
            if let Some(e) = b.end.and_then(|d| d.timestamp_nanos_opt()) {
                hi = hi.min(e);
                found = true;
            }
        }
    }
    (found && lo < hi).then_some((lo, hi))
}

/// Sort the buffer batches by the FIRST column of `ordering` when that is a
/// single column of this schema — the scan's advertised event-time order,
/// whichever field the index's mapping declares (#6020). Returns the sorted
/// batches, the options and the column's NAME; `None` when the ordering is
/// anything else, and the caller unions unsorted.
fn sort_batches_like(
    batches: &[RecordBatch],
    ordering: &datafusion::physical_expr::LexOrdering,
    schema: &SchemaRef,
) -> Option<(Vec<RecordBatch>, arrow_schema::SortOptions, String)> {
    use arrow::compute::{concat_batches, lexsort_to_indices, take_record_batch, SortColumn};
    let first = ordering.first();
    let col = first
        .expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::Column>()?;
    let ts_idx = schema.index_of(col.name()).ok()?;
    let combined = concat_batches(schema, batches).ok()?;
    let sort_col = SortColumn {
        values: combined.column(ts_idx).clone(),
        options: Some(first.options),
    };
    let indices = lexsort_to_indices(&[sort_col], None).ok()?;
    take_record_batch(&combined, &indices)
        .ok()
        .map(|b| (vec![b], first.options, col.name().to_string()))
}

/// Rebuild the base scan's event-time ordering against `plan`'s (possibly
/// projected) schema. `None` when that column didn't survive the projection.
fn projected_event_time_ordering(
    plan: &Arc<dyn ExecutionPlan>,
    base_ordering: &Option<datafusion::physical_expr::LexOrdering>,
) -> Option<datafusion::physical_expr::LexOrdering> {
    use datafusion::physical_expr::{
        expressions::Column as PhysColumn, LexOrdering, PhysicalSortExpr,
    };
    let first = base_ordering.as_ref()?.first();
    let name = first
        .expr
        .as_any()
        .downcast_ref::<PhysColumn>()?
        .name()
        .to_string();
    let schema = plan.schema();
    let idx = schema.index_of(&name).ok()?;
    LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(PhysColumn::new(&name, idx)),
        first.options,
    )])
}

#[async_trait::async_trait]
impl TableProvider for UnionEventsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let base_plan = self.base.scan(state, projection, filters, limit).await?;
        // Backlog guard: a buffer past the decode budget (outage recovery)
        // serves the committed-only view instead of materializing gigabytes.
        // Derive the window FIRST so the budget gate can price what this query
        // actually reads, not the whole WAL.
        //
        // NOTE the asymmetry with `load_buffer_delta`, which prices the WHOLE
        // buffer (`time_bound: None`): a windowed query can be within budget
        // here while that gate refuses. The delta's refusal therefore proves
        // NOTHING about what this scan unions — see `BufferDeltaState`.
        let df_schema = datafusion::common::DFSchema::try_from(self.schema.as_ref().clone())?;
        let bound = time_bound_from_filters(filters, &df_schema);
        let within_budget = match self.max_bytes {
            Some(max) => buffer_within_budget_with_max(&self.wal_dir, &self.exclude, bound, max).0,
            None => buffer_within_budget(&self.wal_dir, &self.exclude, bound).0,
        };
        if !within_budget {
            return Ok(base_plan);
        }
        // Read the un-committed WAL afresh, pruning segments outside the query's
        // timestamp window (WS-8) and excluding already-committed ones (WS-6).
        let batches = read_buffer_batches(
            &self.wal_dir,
            &self.schema,
            &self.exclude,
            bound,
            self.carrier.as_ref(),
            self.owner.as_deref(),
        )
        .map_err(|e| DataFusionError::External(e.into()))?;
        if batches.is_empty() {
            return Ok(base_plan);
        }
        // Apply this query's predicates to the buffer BEFORE deciding to union.
        //
        // The union is not free: on 2026-08-17 a live fleet served this filtered
        // browse in 145 ms against an EMPTY buffer directory and 504'd at 60 s
        // against a populated one -- same binary, same data, same
        // `--query-wal-buffer-dir` flag, 75 segments totalling 316 KB. The plans
        // are identical except for the union's second child, and the predicate is
        // pushed into the Iceberg scan in BOTH, so the cost is the union itself,
        // not a lost pushdown.
        //
        // The buffer holds seconds of data, so for a selective predicate it
        // usually contributes NOTHING. Evaluating the filter here (over a few
        // hundred KB, in memory) lets those queries take the base plan unchanged
        // and pay nothing. Rows that DO match are still unioned, so the answer is
        // unchanged either way.
        let batches = match filter_batches(&batches, filters, &self.schema, state) {
            Ok(filtered) => filtered,
            // Never let a filter we cannot evaluate change the ANSWER: fall back
            // to unioning everything, which is correct but slow.
            Err(e) => {
                tracing::debug!(error = %e, "wal-buffer: filter pre-pass failed; unioning unfiltered");
                batches
            }
        };
        if batches.iter().all(|b| b.num_rows() == 0) {
            metrics::counter!("siglake_query_wal_buffer_skipped_no_match_total").increment(1);
            return Ok(base_plan);
        }
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        metrics::counter!("siglake_query_wal_buffer_used_total").increment(1);
        metrics::histogram!("siglake_query_wal_buffer_rows").record(n as f64);
        // Preserve the base scan's advertised `timestamp` ordering through the
        // union: a plain UnionExec has NO output ordering, so an ordered LIMIT
        // above it re-sorts the WHOLE table — one buffered marker segment
        // turned every newest-first browse into a full-scan TopK (Phase-4).
        // When the base advertises an ordering, sort the (small, in-memory)
        // buffer batches the same way, declare them sorted on the MemTable,
        // and merge-sort the union's partitions.
        let base_ordering = base_plan.properties().output_ordering().cloned();
        tracing::debug!(
            has_ordering = base_ordering.is_some(),
            batches = batches.len(),
            "wal-buffer union: base ordering state"
        );
        let (buffer, sorted) = match &base_ordering {
            Some(ordering) => match sort_batches_like(&batches, ordering, &self.schema) {
                Some((sorted_batches, options, field)) => {
                    // Physically sorted AND declared: the physical optimizer
                    // re-plans around whatever we return (it replaced an
                    // undeclared-but-sorted memtable with a full-scan TopK on
                    // the live cluster), so the DECLARATION is what lets
                    // EnforceSorting turn the query's Sort into a fetch-
                    // limited merge over the union's sorted partitions.
                    // `col()` would normalize an unquoted mixed-case mapping
                    // field to lower case and name a column that isn't there;
                    // the sorted column is known exactly here.
                    let sort_expr = datafusion::logical_expr::SortExpr::new(
                        datafusion::logical_expr::Expr::Column(
                            datafusion::common::Column::new_unqualified(&field),
                        ),
                        !options.descending,
                        options.nulls_first,
                    );
                    (
                        MemTable::try_new(self.schema.clone(), vec![sorted_batches])?
                            .with_sort_order(vec![vec![sort_expr]]),
                        true,
                    )
                }
                None => (
                    MemTable::try_new(self.schema.clone(), vec![batches])?,
                    false,
                ),
            },
            None => (
                MemTable::try_new(self.schema.clone(), vec![batches])?,
                false,
            ),
        };
        let buffer_plan = buffer.scan(state, projection, filters, limit).await?;
        // Both children share the (projected) events schema, so a plain union is
        // valid. UnionExec requires ≥1 inputs and identical output schemas.
        if base_plan.schema() != buffer_plan.schema() {
            return Err(DataFusionError::Internal(format!(
                "WAL buffer schema {:?} != iceberg schema {:?} after projection",
                buffer_plan.schema(),
                base_plan.schema()
            )));
        }
        let union = UnionExec::try_new(vec![base_plan, buffer_plan])?;
        if sorted {
            // Re-derive the ordering against the PROJECTED schema (column
            // indices shift under projection); fall back to the plain union
            // when the sort column was projected out.
            if let Some(ordering) = projected_event_time_ordering(&union, &base_ordering) {
                tracing::debug!("wal-buffer union: SPM applied");
                return Ok(Arc::new(
                    datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec::new(
                        ordering, union,
                    ),
                ));
            }
        }
        tracing::debug!(sorted, "wal-buffer union: plain union (no SPM)");
        Ok(union)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Inexact: the union re-applies the filter above, and the base provider
        // prunes what it can. Matches SiglakeStaticTableProvider's contract.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use siglake_core::Event;
    use siglake_wal::WalWriter;
    use std::time::Duration;

    fn write_sealed_segment(root: &Path, n: usize) {
        let mut w =
            WalWriter::with_thresholds(root, "ing", 10_000, Duration::from_secs(60)).unwrap();
        let evs: Vec<Event> = (0..n)
            .map(|i| Event::now(format!("buffered {i}")))
            .collect();
        w.append_events(&evs).unwrap();
        w.seal().unwrap().expect("sealed segment");
    }

    /// `count(*) FROM events` against a provider, optionally with a WHERE clause.
    async fn count_events(prov: Arc<UnionEventsProvider>, where_clause: &str) -> i64 {
        let ctx = SessionContext::new();
        ctx.register_table("events", prov).unwrap();
        let sql = format!("SELECT count(*) AS n FROM events {where_clause}");
        let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0)
    }

    #[test]
    fn buffer_budget_counts_surviving_segment_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        // Empty dir: trivially within budget, zero bytes.
        assert_eq!(
            buffer_within_budget(tmp.path(), &HashSet::new(), None),
            (true, 0)
        );
        write_sealed_segment(tmp.path(), 5);
        let (ok, bytes) = buffer_within_budget(tmp.path(), &HashSet::new(), None);
        assert!(ok, "a tiny buffer is within the default 2GiB budget");
        assert!(bytes > 0, "segment bytes are counted");
        // Excluded (consumed) segments don't count toward the budget.
        let names = uncommitted_segment_names(tmp.path(), &HashSet::new(), None).unwrap();
        let all: HashSet<String> = names.into_iter().collect();
        assert_eq!(buffer_within_budget(tmp.path(), &all, None), (true, 0));
    }

    /// The budget gate must price what the QUERY reads, not the whole WAL.
    ///
    /// This is why freshness collapsed under load. The gate passed `None` and
    /// sized every uncommitted segment, while the caller derived the query's
    /// window on the very next line — so a "last 5 minutes" query was refused
    /// the buffer because segments from hours ago were also sitting in the WAL.
    /// At ~141K rows/s accept the 2 GiB default is ~30 seconds of ingest, so the
    /// moment the drain fell behind, EVERY query fell to the committed-only view
    /// and waited on the drain: the 2026-08-08 round measured p50 23s with a
    /// 272s tail against 5.5s quiescent.
    /// The budget gate must price what the QUERY reads, not the whole WAL —
    /// but only pay for that when it matters.
    ///
    /// Freshness collapsed under load because the gate sized every uncommitted
    /// segment while the caller derived the query's window on the next line, so
    /// a "last 5 minutes" query was refused the buffer over segments it would
    /// never decode. At ~141K rows/s the 2 GiB default is ~30s of ingest, so the
    /// moment the drain fell behind every query fell to committed-only.
    ///
    /// The window check is NOT free: it reads each segment's header
    /// (`File::open` + read) where the unwindowed check only stats. So the cheap
    /// total is tried first, and headers are read only when the whole buffer is
    /// over budget — the case that would otherwise refuse outright.
    #[test]
    fn buffer_budget_prices_the_window_only_when_over_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        let hour_ago = chrono::Utc::now() - chrono::Duration::hours(1);
        {
            let mut w =
                WalWriter::with_thresholds(root, "old", 10_000, Duration::from_secs(60)).unwrap();
            let evs: Vec<Event> = (0..64)
                .map(|i| {
                    let mut e = Event::now(format!("old {i}"));
                    e.timestamp = hour_ago;
                    e
                })
                .collect();
            w.append_events(&evs).unwrap();
            w.seal().unwrap().expect("sealed old segment");
        }
        write_sealed_segment(root, 4); // "recent", stamped ~now

        let exclude = HashSet::new();
        let (_, all_bytes) = buffer_within_budget_with_max(root, &exclude, None, u64::MAX);
        assert!(all_bytes > 0);

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let recent_only = Some((now - 60_000_000_000, i64::MAX));

        // Under budget: the cheap path answers, and must NOT have priced the
        // window (it reports the full total, having read no headers).
        let (ok, bytes) = buffer_within_budget_with_max(root, &exclude, recent_only, u64::MAX);
        assert!(ok);
        assert_eq!(bytes, all_bytes, "cheap path must not pay for header reads");

        // Over budget for the whole WAL, but the query's window fits: the gate
        // must serve rather than refuse. This is the freshness case.
        let tight = all_bytes - 1;
        assert!(
            !buffer_within_budget_with_max(root, &exclude, None, tight).0,
            "whole buffer must be over this budget"
        );
        let (ok, windowed) = buffer_within_budget_with_max(root, &exclude, recent_only, tight);
        assert!(
            ok,
            "windowed gate must serve when only in-window segments fit \
             (windowed={windowed} all={all_bytes} budget={tight})"
        );
        assert!(windowed < all_bytes, "window must exclude the old segment");
    }

    #[test]
    fn buffer_delta_cache_key_semantics() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 5);
        let schema = siglake_core::events_schema();
        let exclude = HashSet::new();
        let cache = BufferDeltaCache::default();

        let names = uncommitted_segment_names(tmp.path(), &exclude, None).unwrap();
        assert_eq!(names.len(), 1);
        assert!(
            cache.get(tmp.path(), &names, &schema, None, None).is_none(),
            "cold cache misses"
        );

        let (batches, read_names) =
            read_buffer_batches_keyed(tmp.path(), &schema, &exclude, None, None, None).unwrap();
        assert_eq!(read_names, names);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);
        cache.insert(
            tmp.path().to_path_buf(),
            read_names,
            schema.clone(),
            None,
            None,
            batches,
        );

        // Unchanged buffer state ⇒ hit with the same rows.
        let hit = cache
            .get(tmp.path(), &names, &schema, None, None)
            .expect("hit");
        assert_eq!(hit.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
        assert_eq!(cache.stats(), (1, 1));

        // A new sealed segment changes the basename set ⇒ miss.
        write_sealed_segment(tmp.path(), 2);
        let names2 = uncommitted_segment_names(tmp.path(), &exclude, None).unwrap();
        assert_eq!(names2.len(), 2);
        assert!(cache
            .get(tmp.path(), &names2, &schema, None, None)
            .is_none());

        // A segment entering the consumed set changes the set ⇒ miss.
        let consumed: HashSet<String> = names2.iter().take(1).cloned().collect();
        let names3 = uncommitted_segment_names(tmp.path(), &consumed, None).unwrap();
        assert_eq!(names3.len(), 1);
        assert!(cache
            .get(tmp.path(), &names3, &schema, None, None)
            .is_none());

        // Same names, different target schema ⇒ miss (schema migration).
        let other_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        )]));
        assert!(cache
            .get(tmp.path(), &names, &other_schema, None, None)
            .is_none());

        // Eviction drops the entry.
        cache.evict(tmp.path());
        assert!(cache.get(tmp.path(), &names, &schema, None, None).is_none());
    }

    #[test]
    fn buffer_delta_cache_retries_segment_that_failed_to_decode() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 5);
        let schema = siglake_core::events_schema();
        let exclude = HashSet::new();
        let cache = BufferDeltaCache::default();

        let (segments, _, _) = list_uncommitted_segments(tmp.path(), &exclude, None).unwrap();
        assert_eq!(segments.len(), 1);
        let segment = &segments[0];
        let original = std::fs::read(segment).unwrap();
        std::fs::write(segment, b"not a WAL segment").unwrap();

        let (first, failed_read_names) = cache
            .get_or_load_for_query_keyed(tmp.path(), &schema, &exclude, None, None, None)
            .unwrap();
        assert!(
            first.is_empty(),
            "the unreadable segment is best-effort skipped"
        );
        assert!(
            failed_read_names.is_empty(),
            "an unreadable segment must not vouch for a complete cache entry"
        );

        std::fs::write(segment, original).unwrap();
        let restored_names = uncommitted_segment_names(tmp.path(), &exclude, None).unwrap();
        let (second, read_names) = cache
            .get_or_load_for_query_keyed(tmp.path(), &schema, &exclude, None, None, None)
            .unwrap();

        assert_eq!(read_names, restored_names);
        assert_eq!(second.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        assert_eq!(
            cache.stats(),
            (0, 2),
            "the unchanged full basename set must retry after a failed decode"
        );
    }

    #[test]
    fn buffer_delta_cache_decodes_same_time_bound_once() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 5);
        let schema = siglake_core::events_schema();
        let exclude = HashSet::new();
        let cache = BufferDeltaCache::default();
        let time_bound = Some((i64::MIN, i64::MAX));

        let first = cache
            .get_or_load_for_query(tmp.path(), &schema, &exclude, time_bound, None, None)
            .unwrap();
        let second = cache
            .get_or_load_for_query(tmp.path(), &schema, &exclude, time_bound, None, None)
            .unwrap();

        assert_eq!(first.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        assert_eq!(second.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        assert_eq!(cache.stats(), (1, 1), "second call must hit the cache");
    }

    #[test]
    fn buffer_delta_cache_reuses_unbounded_decode_for_time_bound() {
        let tmp = tempfile::tempdir().unwrap();
        let hour_ago = chrono::Utc::now() - chrono::Duration::hours(1);
        {
            let mut w =
                WalWriter::with_thresholds(tmp.path(), "old", 10_000, Duration::from_secs(60))
                    .unwrap();
            let evs: Vec<Event> = (0..2)
                .map(|i| {
                    let mut e = Event::now(format!("old {i}"));
                    e.timestamp = hour_ago;
                    e
                })
                .collect();
            w.append_events(&evs).unwrap();
            w.seal().unwrap().expect("sealed old segment");
        }
        write_sealed_segment(tmp.path(), 3);

        let schema = siglake_core::events_schema();
        let exclude = HashSet::new();
        let cache = BufferDeltaCache::default();
        let all = cache
            .get_or_load_for_query(tmp.path(), &schema, &exclude, None, None, None)
            .unwrap();
        assert_eq!(all.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let recent_only = Some((now - 60_000_000_000, i64::MAX));
        let bounded = cache
            .get_or_load_for_query(tmp.path(), &schema, &exclude, recent_only, None, None)
            .unwrap();

        // The full cached batches are a safe superset: the distributed SQL
        // still applies this timestamp predicate, and no segment is decoded a
        // second time after the Tier-1 delta load.
        assert_eq!(bounded.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        assert_eq!(cache.stats(), (1, 1), "bounded call must reuse full decode");
    }

    #[tokio::test]
    async fn empty_buffer_is_transparent() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = siglake_core::events_schema();
        // No segments written ⇒ base-only ⇒ 0 rows.
        let base = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]]).unwrap());
        let prov =
            Arc::new(UnionEventsProvider::try_new(base, tmp.path(), &HashSet::new()).unwrap());
        assert_eq!(count_events(prov, "").await, 0);
    }

    #[tokio::test]
    async fn buffer_rows_are_visible_via_union() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 5);
        let schema = siglake_core::events_schema();
        // Empty base (no committed Iceberg rows) so count(*) == buffer rows.
        let base = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]]).unwrap());
        let prov =
            Arc::new(UnionEventsProvider::try_new(base, tmp.path(), &HashSet::new()).unwrap());
        assert_eq!(
            count_events(prov, "").await,
            5,
            "the 5 un-committed WAL rows must be queryable"
        );
    }

    #[tokio::test]
    async fn union_sums_base_and_buffer() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 3);
        let schema = siglake_core::events_schema();
        // Base has 2 committed rows.
        let base_batch = {
            let evs: Vec<Event> = (0..2)
                .map(|i| Event::now(format!("committed {i}")))
                .collect();
            siglake_core::events_to_record_batch(&evs).unwrap()
        };
        let base = Arc::new(MemTable::try_new(schema.clone(), vec![vec![base_batch]]).unwrap());
        let prov =
            Arc::new(UnionEventsProvider::try_new(base, tmp.path(), &HashSet::new()).unwrap());
        assert_eq!(count_events(prov, "").await, 5, "2 committed + 3 buffered");
    }

    /// A segment named in the snapshot's consumed-set (already in Iceberg, still
    /// briefly in `sealed/`/`processing/`) is excluded from the buffer, so the
    /// union doesn't double-count it. This is WS-6's over-count-window close.
    #[tokio::test]
    async fn consumed_segment_is_excluded_from_buffer() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 4);
        // Recover the just-sealed segment's basename to exclude it.
        let seg_name = std::fs::read_dir(tmp.path().join(SEALED_DIR))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .find(|n| n.ends_with(".arrow"))
            .expect("a sealed segment");
        let schema = siglake_core::events_schema();
        let base = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]]).unwrap());

        // Without exclusion: the 4 rows are buffered.
        let included = Arc::new(
            UnionEventsProvider::try_new(base.clone(), tmp.path(), &HashSet::new()).unwrap(),
        );
        assert_eq!(
            count_events(included, "").await,
            4,
            "segment visible when not excluded"
        );

        // With the segment in the consumed-set: excluded ⇒ served from Iceberg
        // only (the empty base ⇒ 0).
        let exclude: HashSet<String> = [seg_name].into_iter().collect();
        let excluded = Arc::new(UnionEventsProvider::try_new(base, tmp.path(), &exclude).unwrap());
        assert_eq!(
            count_events(excluded, "").await,
            0,
            "consumed segment excluded from buffer"
        );
    }

    /// WS-8 time pruning: a framed segment is dropped only when its header
    /// `[min_ts, max_ts]` is entirely outside the query window — a covering or
    /// overlapping window keeps it (never drops a row that could match), and no
    /// bound keeps everything.
    #[test]
    fn list_prunes_framed_segments_outside_time_window() {
        use chrono::{TimeZone, Utc};
        let tmp = tempfile::tempdir().unwrap();
        let mut w =
            WalWriter::with_thresholds(tmp.path(), "ing", 10_000, Duration::from_secs(60)).unwrap();
        let evs: Vec<Event> = (0..3)
            .map(|i| {
                let mut e = Event::now(format!("e{i}"));
                e.timestamp = Utc.timestamp_opt(1000 + i, 0).single().unwrap();
                e
            })
            .collect();
        w.append_events(&evs).unwrap();
        w.seal().unwrap().expect("sealed");
        let ns = |s: i64| s * 1_000_000_000;
        let empty = HashSet::new();

        // Window covering [1000s, 1002s] ⇒ kept.
        let (segs, _, pruned) =
            list_uncommitted_segments(tmp.path(), &empty, Some((ns(999), ns(1003)))).unwrap();
        assert_eq!(
            (segs.len(), pruned),
            (1, 0),
            "covering window keeps the segment"
        );

        // Window entirely after the segment ⇒ pruned.
        let (segs, _, pruned) =
            list_uncommitted_segments(tmp.path(), &empty, Some((ns(2000), ns(3000)))).unwrap();
        assert_eq!((segs.len(), pruned), (0, 1), "out-of-range window prunes");

        // Boundary overlap (window starts at the segment's max) ⇒ kept.
        let (segs, _, pruned) =
            list_uncommitted_segments(tmp.path(), &empty, Some((ns(1002), ns(3000)))).unwrap();
        assert_eq!(
            (segs.len(), pruned),
            (1, 0),
            "overlap at the boundary keeps it"
        );

        // No bound ⇒ everything kept.
        let (segs, _, pruned) = list_uncommitted_segments(tmp.path(), &empty, None).unwrap();
        assert_eq!((segs.len(), pruned), (1, 0), "no bound keeps everything");
    }

    #[test]
    fn align_null_fills_missing_promoted_column() {
        // Batch has only {a}; target wants {a, promoted} ⇒ promoted null-filled.
        let src_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            src_schema,
            vec![Arc::new(StringArray::from(vec!["x", "y"])) as _],
        )
        .unwrap();
        let target: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("promoted", DataType::Int64, true),
        ]));
        let aligned = align_batch_to_schema(&batch, &target).unwrap();
        assert_eq!(aligned.num_columns(), 2);
        assert_eq!(aligned.num_rows(), 2);
        assert_eq!(
            aligned.column(1).null_count(),
            2,
            "promoted col is all-null"
        );
    }

    #[test]
    fn resolve_prefers_tenant_subdir_when_present() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // Flat layout: no tenant subdir ⇒ returns root.
        assert_eq!(resolve_tenant_wal_dir(root, "acme"), root);
        // Tenant-aware: create <root>/acme/sealed/ ⇒ returns the subdir.
        std::fs::create_dir_all(root.join("acme").join(SEALED_DIR)).unwrap();
        assert_eq!(resolve_tenant_wal_dir(root, "acme"), root.join("acme"));
    }
}

#[cfg(test)]
mod bounded_gate_tests {
    use super::*;
    use std::io::Write;

    fn seg(dir: &Path, name: &str, bytes: usize) {
        std::fs::create_dir_all(dir).unwrap();
        let mut f = std::fs::File::create(dir.join(name)).unwrap();
        f.write_all(&vec![0u8; bytes]).unwrap();
    }

    /// The defect this fixes: under a large backlog the gate walked EVERY
    /// uncommitted segment even though the verdict was decided after a handful.
    /// On 2026-08-16 that made every buffer-consulting query 504 at 50,000
    /// segments while aggregates kept working.
    #[test]
    fn over_budget_stops_early_instead_of_walking_the_whole_backlog() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = tmp.path().join(SEALED_DIR);
        // 200 segments of 1 KiB; a 4 KiB budget is blown by the 5th.
        for i in 0..200 {
            seg(&sealed, &format!("s{i:04}.arrow"), 1024);
        }
        let (ok, total) = buffer_within_budget_with_max(tmp.path(), &HashSet::new(), None, 4096);
        assert!(!ok, "200 KiB must not fit a 4 KiB budget");
        // The decisive assertion: it stopped as soon as the ceiling was crossed,
        // rather than summing all 200. Walking everything would total ~204,800.
        assert!(
            total <= 4096 + 1024,
            "sized {total} bytes — it kept walking after the verdict was known"
        );
    }

    #[test]
    fn under_budget_still_reports_the_true_total() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = tmp.path().join(SEALED_DIR);
        for i in 0..10 {
            seg(&sealed, &format!("s{i}.arrow"), 1024);
        }
        let (ok, total) =
            buffer_within_budget_with_max(tmp.path(), &HashSet::new(), None, 1024 * 1024);
        assert!(ok);
        assert_eq!(total, 10 * 1024, "an in-budget answer must be exact");
    }

    /// The byte ceiling cannot bound work when segments are tiny: 50,000 x 8 B
    /// sits far under a 2 GiB budget while still costing a syscall each. The cap
    /// is what bounds that case.
    #[test]
    fn segment_count_cap_refuses_many_tiny_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = tmp.path().join(SEALED_DIR);
        for i in 0..500 {
            seg(&sealed, &format!("s{i:04}.arrow"), 8);
        }
        // Under an effectively infinite BYTE budget, 500 tiny segments fit...
        let (ok_uncapped, _) =
            size_until_decided_capped(tmp.path(), &HashSet::new(), None, u64::MAX, 0);
        assert!(ok_uncapped, "4 KiB total fits any byte budget");
        // ...but the COUNT cap refuses them, which is the whole point.
        let (ok_capped, _) =
            size_until_decided_capped(tmp.path(), &HashSet::new(), None, u64::MAX, 50);
        assert!(
            !ok_capped,
            "500 segments must be refused by a cap of 50 — the byte ceiling cannot bound this"
        );
    }

    #[test]
    fn excluded_segments_are_not_counted_toward_the_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = tmp.path().join(SEALED_DIR);
        seg(&sealed, "keep.arrow", 1024);
        seg(&sealed, "drop.arrow", 1024);
        let mut ex = HashSet::new();
        ex.insert("drop.arrow".to_string());
        let (ok, total) = buffer_within_budget_with_max(tmp.path(), &ex, None, 1536);
        assert!(ok, "only the kept segment counts, and 1 KiB fits 1.5 KiB");
        assert_eq!(total, 1024);
    }
}

#[cfg(test)]
mod buffer_prefilter_tests {
    use super::*;
    use arrow_array::{RecordBatch, StringArray, TimestampNanosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use datafusion::prelude::{col, lit, SessionContext};

    fn batch() -> (SchemaRef, RecordBatch) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("region", DataType::Utf8, true),
        ]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1i64, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("us-west-1"),
                    Some("us-west-1"),
                    Some("us-east-2"),
                ])),
            ],
        )
        .unwrap();
        (schema, b)
    }

    /// The whole point: a predicate that matches nothing in the buffer must leave
    /// nothing, so the caller can skip the union entirely. On 2026-08-17 that
    /// union turned a 145ms query into a 60s timeout.
    #[tokio::test]
    async fn non_matching_predicate_empties_the_buffer() {
        let (schema, b) = batch();
        let state = SessionContext::new().state();
        let out = filter_batches(
            &[b],
            &[col("region").eq(lit("eu-central-1"))],
            &schema,
            &state,
        )
        .unwrap();
        assert_eq!(out.iter().map(|x| x.num_rows()).sum::<usize>(), 0);
    }

    /// And a matching predicate must KEEP its rows — skipping the union when the
    /// buffer really does hold matches would silently drop fresh data, which is
    /// far worse than the slowness being fixed.
    #[tokio::test]
    async fn matching_predicate_keeps_its_rows() {
        let (schema, b) = batch();
        let state = SessionContext::new().state();
        let out =
            filter_batches(&[b], &[col("region").eq(lit("us-east-2"))], &schema, &state).unwrap();
        assert_eq!(out.iter().map(|x| x.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn no_filters_keeps_everything() {
        let (schema, b) = batch();
        let state = SessionContext::new().state();
        let out = filter_batches(&[b], &[], &schema, &state).unwrap();
        assert_eq!(out.iter().map(|x| x.num_rows()).sum::<usize>(), 3);
    }

    #[tokio::test]
    async fn multiple_filters_are_anded() {
        let (schema, b) = batch();
        let state = SessionContext::new().state();
        let out = filter_batches(
            &[b],
            &[
                col("region").eq(lit("us-east-2")),
                col("region").eq(lit("us-west-1")),
            ],
            &schema,
            &state,
        )
        .unwrap();
        assert_eq!(
            out.iter().map(|x| x.num_rows()).sum::<usize>(),
            0,
            "contradictory filters must AND to nothing, not OR to everything"
        );
    }
}
