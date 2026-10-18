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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskPollContext, Poll};

use anyhow::{Context, Result};
use arrow::array::ArrayData;
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_array::{make_array, Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, RecordBatchStream};
use futures::Stream;

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
    list_uncommitted_segments_bounded(wal_dir, exclude, time_bound, None, 0)
}

/// Exact selected-set listing with owner filtering and bounded work. The
/// examination cap applies after consumed/recency exclusion; the kept-entry
/// cap is checked again after time and owner filtering. Crossing either is an
/// explicit committed-only refusal, never a partial generation.
fn list_uncommitted_segments_bounded(
    wal_dir: &Path,
    exclude: &HashSet<String>,
    time_bound: Option<(i64, i64)>,
    owner: Option<&str>,
    kept_cap: usize,
) -> Result<(Vec<PathBuf>, usize, usize)> {
    let mut out = Vec::new();
    let mut excluded = 0usize;
    let mut pruned = 0usize;
    let mut examined = 0usize;
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
            examined += 1;
            if kept_cap > 0 && examined > kept_cap {
                metrics::counter!("siglake_query_wal_buffer_refused_segment_cap_total")
                    .increment(1);
                anyhow::bail!("WAL segment examination exceeds request ceiling");
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
            if owner.is_some() {
                let mut candidate = vec![p];
                retain_owned_segments(&mut candidate, owner);
                let Some(p) = candidate.pop() else {
                    continue;
                };
                out.push(p);
            } else {
                out.push(p);
            }
            if kept_cap > 0 && out.len() > kept_cap {
                anyhow::bail!("selected WAL segment count exceeds request ceiling");
            }
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

/// Compatibility ceiling on the ON-DISK bytes of buffered segments a single
/// request may decode (`SIGLAKE_BUFFER_DELTA_MAX_BYTES`, default 2 GiB; 0
/// disables this compressed-file guard, not the ownership budget). The buffer
/// normally holds seconds of data, but after an outage it
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

/// Whether the surviving uncommitted segments under `wal_dir` fit the legacy
/// compressed-file budget. Returns `(within_budget, total_bytes)`; an unreadable listing
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

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Process-wide bytes reserved for WAL decode scratch and retained Arrow
/// ownership. The default is one eighth of the container limit, bounded to
/// 64 MiB..1 GiB; a host without a readable limit uses 512 MiB. Setting
/// `SIGLAKE_BUFFER_DELTA_MEMORY_BYTES=0` disables WAL-buffer reads rather than
/// disabling the guard.
///
/// This is separate from [`buffer_delta_max_bytes`]. That existing setting is
/// a compatibility ceiling on compressed files. This one accounts for the
/// allocations those files can create and is shared by simultaneous requests,
/// cache generations, and readers of replaced generations.
fn buffer_delta_memory_bytes_from(configured: Option<&str>, memory_limit: Option<u64>) -> u64 {
    if let Some(bytes) = configured.and_then(|value| value.trim().parse::<u64>().ok()) {
        return bytes;
    }
    memory_limit
        .map(|limit| (limit / 8).max(1).clamp(64 * MIB, GIB).min(limit))
        .unwrap_or(512 * MIB)
}

fn buffer_delta_memory_bytes() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        buffer_delta_memory_bytes_from(
            std::env::var("SIGLAKE_BUFFER_DELTA_MEMORY_BYTES")
                .ok()
                .as_deref(),
            siglake_storage::iceberg::cgroup_memory_limit_bytes(),
        )
    })
}

#[derive(Debug)]
struct BufferMemoryState {
    used: u64,
}

#[derive(Debug)]
struct BufferMemoryBudget {
    limit: u64,
    state: std::sync::Mutex<BufferMemoryState>,
}

impl BufferMemoryBudget {
    fn new(limit: u64) -> Arc<Self> {
        let budget = Arc::new(Self {
            limit,
            state: std::sync::Mutex::new(BufferMemoryState { used: 0 }),
        });
        metrics::gauge!("siglake_query_wal_buffer_memory_limit_bytes").set(limit as f64);
        metrics::gauge!("siglake_query_wal_buffer_memory_reserved_bytes").set(0.0);
        budget
    }

    /// Non-waiting admission. A WAL buffer is a freshness aid, so contention
    /// declines to the exact committed snapshot instead of building a queue
    /// that can starve maintenance and request watchdogs.
    fn try_reserve(self: &Arc<Self>, bytes: u64) -> Option<Arc<BufferMemoryReservation>> {
        if bytes == 0 {
            return Some(Arc::new(BufferMemoryReservation {
                budget: Arc::clone(self),
                bytes: 0,
            }));
        }
        let mut state = self.state.lock().expect("WAL memory budget poisoned");
        if bytes > self.limit.saturating_sub(state.used) {
            metrics::counter!("siglake_query_wal_buffer_refused_memory_total").increment(1);
            return None;
        }
        state.used = state.used.saturating_add(bytes);
        metrics::gauge!("siglake_query_wal_buffer_memory_reserved_bytes").set(state.used as f64);
        Some(Arc::new(BufferMemoryReservation {
            budget: Arc::clone(self),
            bytes,
        }))
    }

    #[cfg(test)]
    fn used(&self) -> u64 {
        self.state.lock().expect("WAL memory budget poisoned").used
    }
}

#[derive(Debug)]
struct BufferMemoryReservation {
    budget: Arc<BufferMemoryBudget>,
    bytes: u64,
}

impl BufferMemoryReservation {
    /// Verify measured retained Arrow ownership fits the conservative charge.
    /// The full charge remains until the last reader drops: its spare component
    /// covers filter/sort copies made from the retained decoded generation.
    fn contains_actual(&self, actual: u64) -> bool {
        if actual > self.bytes {
            metrics::counter!("siglake_query_wal_buffer_refused_actual_bytes_total").increment(1);
            return false;
        }
        true
    }
}

impl Drop for BufferMemoryReservation {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .state
            .lock()
            .expect("WAL memory budget poisoned");
        state.used = state.used.saturating_sub(self.bytes);
        metrics::gauge!("siglake_query_wal_buffer_memory_reserved_bytes").set(state.used as f64);
    }
}

/// Owns an Arrow buffer together with the reservation charged for that
/// allocation. Rebuilding an array around these owners is zero-copy: the
/// original buffer remains the storage, while its last shallow descendant now
/// controls when the charge is released.
#[derive(Debug)]
struct LeasedBufferOwner {
    _buffer: Buffer,
    _reservation: Arc<BufferMemoryReservation>,
}

fn attach_reservation_to_buffer(
    buffer: &Buffer,
    reservation: &Arc<BufferMemoryReservation>,
) -> Buffer {
    if buffer.is_empty() && buffer.capacity() == 0 {
        return buffer.clone();
    }
    let owner = Arc::new(LeasedBufferOwner {
        _buffer: buffer.clone(),
        _reservation: Arc::clone(reservation),
    });
    let ptr = std::ptr::NonNull::new(buffer.as_ptr() as *mut u8)
        .expect("an allocated Arrow buffer has a non-null pointer");
    // SAFETY: `owner` retains the original Buffer allocation, and therefore
    // keeps `ptr..ptr+len` valid until the custom Buffer's final clone drops.
    unsafe { Buffer::from_custom_allocation(ptr, buffer.len(), owner) }
}

fn attach_reservation_to_array_data(
    data: &ArrayData,
    reservation: &Arc<BufferMemoryReservation>,
) -> Result<ArrayData> {
    let buffers = data
        .buffers()
        .iter()
        .map(|buffer| attach_reservation_to_buffer(buffer, reservation))
        .collect();
    let children = data
        .child_data()
        .iter()
        .map(|child| attach_reservation_to_array_data(child, reservation))
        .collect::<Result<Vec<_>>>()?;
    let nulls = data.nulls().map(|nulls| {
        let bits = nulls.inner();
        NullBuffer::new(BooleanBuffer::new(
            attach_reservation_to_buffer(bits.inner(), reservation),
            bits.offset(),
            bits.len(),
        ))
    });
    ArrayData::builder(data.data_type().clone())
        .len(data.len())
        .offset(data.offset())
        .buffers(buffers)
        .child_data(children)
        .nulls(nulls)
        .build()
        .context("attach WAL memory reservation to Arrow array")
}

fn attach_reservation_to_batch(
    batch: &RecordBatch,
    reservation: &Arc<BufferMemoryReservation>,
) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            attach_reservation_to_array_data(&column.to_data(), reservation).map(make_array)
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(batch.schema(), columns)
        .context("attach WAL memory reservation to record batch")
}

fn attach_reservation_to_batches(
    batches: Vec<RecordBatch>,
    reservation: &Arc<BufferMemoryReservation>,
) -> Result<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| attach_reservation_to_batch(batch, reservation))
        .collect()
}

#[derive(Debug)]
struct SelectedBufferSegments {
    paths: Vec<PathBuf>,
    readers: Vec<siglake_wal::BoundedSegmentReader>,
    names: Vec<String>,
    compressed_bytes: u64,
    decode_charge_bytes: u64,
    admitted: bool,
}

/// Take the one listing that defines both admission and decode. Each selected
/// segment is charged for the compressed file buffer, its exactly sized
/// decompressed IPC body, and the IPC message buffers retained by Arrow. The
/// bounded reader proves these payload extents before allocating. This is a
/// payload-ownership budget, not a claim about allocator metadata or process
/// RSS; query transformations take separate reservations below.
fn select_buffer_segments(
    wal_dir: &Path,
    exclude: &HashSet<String>,
    time_bound: Option<(i64, i64)>,
    owner: Option<&str>,
    compressed_max: u64,
) -> Result<SelectedBufferSegments> {
    let (paths, excluded, pruned) = list_uncommitted_segments_bounded(
        wal_dir,
        exclude,
        time_bound,
        owner,
        buffer_delta_max_segments(),
    )?;
    metrics::counter!("siglake_query_wal_buffer_segments_excluded_total")
        .increment(excluded as u64);
    metrics::counter!("siglake_query_wal_buffer_segments_time_pruned_total")
        .increment(pruned as u64);
    metrics::counter!("siglake_query_wal_buffer_segments_kept_total").increment(paths.len() as u64);

    let mut names = Vec::with_capacity(paths.len());
    let mut readers = Vec::with_capacity(paths.len());
    let mut compressed_bytes = 0u64;
    let mut decode_charge_bytes = 0u64;
    for path in &paths {
        let reader = siglake_wal::BoundedSegmentReader::open(path)
            .with_context(|| format!("open selected WAL segment {}", path.display()))?;
        let physical = reader.physical_bytes();
        compressed_bytes = compressed_bytes.saturating_add(physical);
        if compressed_max > 0 && compressed_bytes > compressed_max {
            return Ok(SelectedBufferSegments {
                paths: Vec::new(),
                readers: Vec::new(),
                names: Vec::new(),
                compressed_bytes,
                decode_charge_bytes: 0,
                admitted: false,
            });
        }
        let ipc = reader.uncompressed_bytes();
        decode_charge_bytes = decode_charge_bytes
            .saturating_add(physical)
            .saturating_add(ipc.saturating_mul(2));
        readers.push(reader);
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .context("selected WAL segment has no UTF-8 basename")?;
        names.push(name.to_string());
    }
    names.sort_unstable();
    Ok(SelectedBufferSegments {
        paths,
        readers,
        names,
        compressed_bytes,
        decode_charge_bytes,
        admitted: true,
    })
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
    let (segments, _, _) = list_uncommitted_segments_bounded(
        wal_dir,
        exclude,
        time_bound,
        owner,
        buffer_delta_max_segments(),
    )?;
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
/// Every cached generation owns a process-wide memory reservation. Replacing
/// the cache slot does not release that reservation while a query, execution
/// stream, or blocking child still holds the old generation.
pub struct BufferDeltaCache {
    entries: std::sync::Mutex<HashMap<PathBuf, BufferDeltaCacheEntry>>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    memory: Arc<BufferMemoryBudget>,
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
    decoded: Arc<DecodedBuffer>,
}

#[derive(Debug)]
pub(crate) struct DecodedBuffer {
    batches: Vec<RecordBatch>,
    names: Vec<String>,
    compressed_bytes: u64,
    reservations: Vec<Arc<BufferMemoryReservation>>,
}

#[derive(Debug)]
pub(crate) struct BufferOutputLease {
    reservation: Arc<BufferMemoryReservation>,
}

impl BufferOutputLease {
    pub(crate) fn attach(self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        if !self
            .reservation
            .contains_actual(decoded_payload_bytes(&batches))
        {
            anyhow::bail!("WAL query output exceeded its admitted payload bound");
        }
        attach_reservation_to_batches(batches, &self.reservation)
    }
}

impl DecodedBuffer {
    pub(crate) fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub(crate) fn cloned_batches(&self) -> Vec<RecordBatch> {
        self.batches.clone()
    }

    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    pub(crate) fn compressed_bytes(&self) -> u64 {
        self.compressed_bytes
    }

    pub(crate) fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    pub(crate) fn payload_bytes(&self) -> u64 {
        batches_payload_bytes(&self.batches)
    }

    #[cfg(test)]
    pub(crate) fn memory_usage_for_test(&self) -> Box<dyn Fn() -> u64> {
        let budget = Arc::clone(&self.reservations[0].budget);
        Box::new(move || budget.used())
    }

    /// Reserve a caller-proved retained-output payload bound before executing
    /// the operator that creates it. DataFusion accounts aggregate state; this
    /// lease covers Arrow output overlapping the decoded generation and other
    /// cache readers.
    pub(crate) fn try_reserve_output(&self, bound: u64) -> Option<BufferOutputLease> {
        self.reservations
            .first()?
            .budget
            .try_reserve(bound)
            .map(|reservation| BufferOutputLease { reservation })
    }
}

/// Payload bound for simple mergeable partials (COUNT/SUM/MIN/MAX and direct
/// group keys). The output can be split into at most one nonempty batch per
/// input row. Price builder startup for every possible batch, not just logical
/// output length: pinned DataFusion 53.1 ArrowBytesMap starts with 8192 value
/// bytes, while Arrow 58.4 default primitive/Boolean/bytes builders start with
/// 1024 slots (DataFusion primitive group values use only 128).
///
/// MutableBuffer rounds growth to 64 bytes and doubles previous capacity;
/// Vec-backed offsets/values also double. EmitTo::First can retain an entire
/// backing array while rebuilding its suffix, so EACH possible output batch
/// is charged for the full source/row extent plus startup/rounding, rather than
/// summing only emitted logical lengths. Output literal guards exclude values
/// absent from the source. This deliberately conservative (potentially
/// quadratic) bound can refuse large deltas; it is not an efficiency claim. It accounts
/// physical Arrow buffers, consistently with BufferOutputLease::attach; it is
/// not an allocator-metadata/RSS bound. View/nested layouts remain unsupported.
pub(crate) fn bounded_output_payload(
    schema: &SchemaRef,
    max_rows: usize,
    source_variable_payload: u64,
) -> Option<u64> {
    let rows = u64::try_from(max_rows).ok()?;
    let batches = rows.max(1);
    let grow = |logical: u64, startup: u64| {
        logical
            .checked_mul(2)?
            .checked_add(startup.checked_add(64)?)?
            .checked_mul(batches)
    };
    let validity = grow(rows.div_ceil(8), 128)?;
    let mut total = 0u64;
    for field in schema.fields() {
        let data_type = field.data_type();
        let field_bytes = if matches!(data_type, DataType::Null) {
            0
        } else if matches!(data_type, DataType::Boolean) {
            grow(rows.div_ceil(8), 128)?.checked_add(validity)?
        } else if let Some(width) = data_type.primitive_width() {
            let width = u64::try_from(width).ok()?;
            grow(rows.checked_mul(width)?, 1024u64.checked_mul(width)?)?.checked_add(validity)?
        } else {
            let offset_width = match data_type {
                DataType::Utf8 | DataType::Binary => 4u64,
                DataType::LargeUtf8 | DataType::LargeBinary => 8u64,
                _ => return None,
            };
            let offsets = grow(
                rows.checked_add(batches)?.checked_mul(offset_width)?,
                1025u64.checked_mul(offset_width)?,
            )?;
            offsets
                .checked_add(grow(source_variable_payload, 8192)?)?
                .checked_add(validity)?
        };
        total = total.checked_add(field_bytes)?;
    }
    Some(total)
}

#[cfg(test)]
pub(crate) fn decoded_buffer_for_test(batches: Vec<RecordBatch>) -> Arc<DecodedBuffer> {
    let limit = batches_payload_bytes(&batches).max(1);
    decoded_buffer_with_limit_for_test(batches, limit)
}

#[cfg(test)]
pub(crate) fn decoded_buffer_with_limit_for_test(
    batches: Vec<RecordBatch>,
    limit: u64,
) -> Arc<DecodedBuffer> {
    let payload = batches_payload_bytes(&batches);
    let budget = BufferMemoryBudget::new(limit);
    let reservation = budget
        .try_reserve(payload)
        .expect("test decoded buffer reservation");
    let batches = attach_reservation_to_batches(batches, &reservation)
        .expect("attach test decoded buffer reservation");
    Arc::new(DecodedBuffer {
        batches,
        names: Vec::new(),
        compressed_bytes: 0,
        reservations: vec![reservation],
    })
}

#[derive(Debug)]
pub(crate) enum BufferReadOutcome {
    Loaded(Arc<DecodedBuffer>),
    /// Exact committed-only fallback. The selected WAL generation was not
    /// partially served, cached, or treated as proof of an empty buffer.
    Refused {
        compressed_bytes: u64,
    },
}

impl Default for BufferDeltaCache {
    fn default() -> Self {
        Self::with_memory_limit(buffer_delta_memory_bytes())
    }
}

impl BufferDeltaCache {
    fn with_memory_limit(limit: u64) -> Self {
        Self {
            entries: std::sync::Mutex::new(HashMap::new()),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            memory: BufferMemoryBudget::new(limit),
        }
    }

    /// Return decoded batches for the current `(wal_dir, exclude, time_bound)`
    /// state, decoding and installing them only when the exact segment set is
    /// not cached.
    ///
    /// A bounded read may reuse a cached unbounded set when that entry exactly
    /// matches the current uncommitted names. The returned batches are then a
    /// superset of the time window; this is safe for the query path because the
    /// SQL timestamp predicate still filters rows, while avoiding a second
    /// decode after `load_buffer_delta` populated the unbounded entry.
    pub(crate) fn get_or_load_for_query(
        &self,
        wal_dir: &Path,
        target_schema: &SchemaRef,
        exclude: &HashSet<String>,
        time_bound: Option<(i64, i64)>,
        carrier: Option<&siglake_core::index_config::IndexConfig>,
        owner: Option<&str>,
    ) -> Result<BufferReadOutcome> {
        self.get_or_load_inner(
            wal_dir,
            target_schema,
            exclude,
            time_bound,
            carrier,
            owner,
            None,
        )
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
    #[cfg(test)]
    pub(crate) fn get_or_load_for_query_keyed(
        &self,
        wal_dir: &Path,
        target_schema: &SchemaRef,
        exclude: &HashSet<String>,
        time_bound: Option<(i64, i64)>,
        carrier: Option<&siglake_core::index_config::IndexConfig>,
        owner: Option<&str>,
    ) -> Result<BufferReadOutcome> {
        self.get_or_load_inner(
            wal_dir,
            target_schema,
            exclude,
            time_bound,
            carrier,
            owner,
            None,
        )
    }

    // Keep snapshot selection and its independent compressed-byte limit explicit.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get_or_load_for_query_keyed_with_max(
        &self,
        wal_dir: &Path,
        target_schema: &SchemaRef,
        exclude: &HashSet<String>,
        time_bound: Option<(i64, i64)>,
        carrier: Option<&siglake_core::index_config::IndexConfig>,
        owner: Option<&str>,
        compressed_max: Option<u64>,
    ) -> Result<BufferReadOutcome> {
        self.get_or_load_inner(
            wal_dir,
            target_schema,
            exclude,
            time_bound,
            carrier,
            owner,
            compressed_max,
        )
    }

    // Match the explicit selection/admission boundary used by the query caller.
    #[allow(clippy::too_many_arguments)]
    fn get_or_load_inner(
        &self,
        wal_dir: &Path,
        target_schema: &SchemaRef,
        exclude: &HashSet<String>,
        time_bound: Option<(i64, i64)>,
        carrier: Option<&siglake_core::index_config::IndexConfig>,
        owner: Option<&str>,
        compressed_max: Option<u64>,
    ) -> Result<BufferReadOutcome> {
        let names = match uncommitted_segment_names_with_bound(wal_dir, exclude, time_bound, owner)
        {
            Ok(names) => names,
            Err(error) => {
                metrics::counter!("siglake_query_wal_buffer_refused_listing_total").increment(1);
                tracing::warn!(error = %error, dir = %wal_dir.display(),
                    "refusing WAL buffer generation whose listing is unavailable");
                self.evict(wal_dir);
                return Err(error);
            }
        };
        if names.is_empty() {
            if time_bound.is_none() {
                self.evict(wal_dir);
            }
            let reservation = self
                .memory
                .try_reserve(0)
                .expect("a zero-byte reservation cannot fail");
            return Ok(BufferReadOutcome::Loaded(Arc::new(DecodedBuffer {
                batches: Vec::new(),
                names,
                compressed_bytes: 0,
                reservations: vec![reservation],
            })));
        }
        if let Some(cached) = self.get(wal_dir, &names, target_schema, carrier, owner) {
            if compressed_max.is_some_and(|max| max > 0 && cached.compressed_bytes() > max) {
                return Ok(BufferReadOutcome::Refused {
                    compressed_bytes: cached.compressed_bytes(),
                });
            }
            return Ok(BufferReadOutcome::Loaded(cached));
        }

        // `load_buffer_delta` has already populated this entry on the normal
        // distributed path. A time bound only prunes segment decoding; the
        // query executed over these batches still carries the exact predicate.
        if time_bound.is_some() {
            let all_names = match uncommitted_segment_names(wal_dir, exclude, owner) {
                Ok(names) => names,
                Err(error) => {
                    metrics::counter!("siglake_query_wal_buffer_refused_listing_total")
                        .increment(1);
                    tracing::warn!(error = %error, dir = %wal_dir.display(),
                        "refusing WAL buffer generation whose full listing is unavailable");
                    self.evict(wal_dir);
                    return Err(error);
                }
            };
            if all_names != names {
                if let Some(cached) = self.get(wal_dir, &all_names, target_schema, carrier, owner) {
                    if compressed_max.is_some_and(|max| max > 0 && cached.compressed_bytes() > max)
                    {
                        return Ok(BufferReadOutcome::Refused {
                            compressed_bytes: cached.compressed_bytes(),
                        });
                    }
                    return Ok(BufferReadOutcome::Loaded(cached));
                }
            }
        }

        // A mismatched slot cannot answer this generation. Remove the cache's
        // ownership before admitting its replacement; active readers keep
        // their Arc and reservation, so overlap is still charged exactly while
        // an idle stale entry cannot cause permanent self-refusal.
        self.evict(wal_dir);

        let selected = match select_buffer_segments(
            wal_dir,
            exclude,
            time_bound,
            owner,
            compressed_max.unwrap_or_else(buffer_delta_max_bytes),
        ) {
            Ok(selected) => selected,
            Err(error) => {
                metrics::counter!("siglake_query_wal_buffer_refused_sizing_total").increment(1);
                tracing::warn!(error = %error, dir = %wal_dir.display(),
                    "refusing WAL buffer generation that cannot be sized exactly");
                return Err(error);
            }
        };
        let compressed_bytes = selected.compressed_bytes;
        if !selected.admitted {
            metrics::counter!("siglake_query_wal_buffer_refused_bytes_total").increment(1);
            return Ok(BufferReadOutcome::Refused { compressed_bytes });
        }
        let Some(decode_reservation) = self.memory.try_reserve(selected.decode_charge_bytes) else {
            return Ok(BufferReadOutcome::Refused { compressed_bytes });
        };
        let raw_batches = match read_selected_buffer_batches(&selected) {
            Ok(batches) => batches,
            Err(error) => {
                metrics::counter!("siglake_query_wal_buffer_refused_decode_total").increment(1);
                tracing::warn!(error = %error, dir = %wal_dir.display(),
                    "refusing incomplete WAL buffer generation");
                return Err(error);
            }
        };
        let raw_payload = decoded_payload_bytes(&raw_batches);
        if !decode_reservation.contains_actual(raw_payload) {
            return Ok(BufferReadOutcome::Refused { compressed_bytes });
        }
        let Some(expansion_bound) =
            transformation_expansion_bound(&raw_batches, target_schema, carrier)
        else {
            metrics::counter!("siglake_query_wal_buffer_refused_layout_total").increment(1);
            return Ok(BufferReadOutcome::Refused { compressed_bytes });
        };
        let Some(expansion_reservation) = self.memory.try_reserve(expansion_bound) else {
            return Ok(BufferReadOutcome::Refused { compressed_bytes });
        };
        let batches = match transform_selected_buffer_batches(raw_batches, target_schema, carrier) {
            Ok(batches) => batches,
            Err(error) => {
                metrics::counter!("siglake_query_wal_buffer_refused_decode_total").increment(1);
                tracing::warn!(error = %error, dir = %wal_dir.display(),
                    "refusing incomplete WAL buffer generation");
                return Err(error);
            }
        };
        let transformed_payload = decoded_payload_bytes(&batches);
        if transformed_payload > raw_payload.saturating_add(expansion_bound) {
            return Ok(BufferReadOutcome::Refused { compressed_bytes });
        }
        let batches = match attach_reservation_to_batches(batches, &decode_reservation)
            .and_then(|batches| attach_reservation_to_batches(batches, &expansion_reservation))
        {
            Ok(batches) => batches,
            Err(error) => {
                metrics::counter!("siglake_query_wal_buffer_refused_decode_total").increment(1);
                tracing::warn!(error = %error, dir = %wal_dir.display(),
                    "refusing WAL buffer generation whose ownership cannot be charged");
                return Err(error);
            }
        };
        let decoded = Arc::new(DecodedBuffer {
            batches,
            names: selected.names,
            compressed_bytes,
            reservations: vec![decode_reservation, expansion_reservation],
        });
        self.insert(
            wal_dir.to_path_buf(),
            target_schema.clone(),
            carrier.cloned(),
            owner.map(str::to_string),
            Arc::clone(&decoded),
        );
        Ok(BufferReadOutcome::Loaded(decoded))
    }

    /// Cached batches for `wal_dir` iff the key (surviving basenames + schema
    /// + carrier) matches exactly. Batch clones are shallow (Arc'd columns).
    pub(crate) fn get(
        &self,
        wal_dir: &Path,
        names: &[String],
        schema: &SchemaRef,
        carrier: Option<&siglake_core::index_config::IndexConfig>,
        owner: Option<&str>,
    ) -> Option<Arc<DecodedBuffer>> {
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
            Some(Arc::clone(&e.decoded))
        } else {
            None
        }
    }

    /// Install the freshly-decoded batches under the names that were actually
    /// read (single slot per dir — replaces any stale entry).
    fn insert(
        &self,
        wal_dir: PathBuf,
        schema: SchemaRef,
        carrier: Option<siglake_core::index_config::IndexConfig>,
        owner: Option<String>,
        decoded: Arc<DecodedBuffer>,
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
                    names: decoded.names.clone(),
                    schema,
                    carrier,
                    owner,
                    decoded,
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

fn bitmap_bytes(rows: usize) -> Option<u64> {
    Some(u64::try_from(rows).ok()?.div_ceil(8))
}

/// Upper bound for the buffers allocated by Arrow's all-null constructor.
/// Nested, dictionary, union, and run-end layouts are declined because their
/// recursive allocation has no local preallocation proof here.
fn null_array_payload_bound(data_type: &DataType, rows: usize) -> Option<u64> {
    let rows = u64::try_from(rows).ok()?;
    let validity = bitmap_bytes(usize::try_from(rows).ok()?)?;
    if matches!(data_type, DataType::Null) {
        return Some(0);
    }
    if matches!(data_type, DataType::Boolean) {
        return validity.checked_mul(2);
    }
    if let Some(width) = data_type.primitive_width() {
        return rows
            .checked_mul(u64::try_from(width).ok()?)?
            .checked_add(validity);
    }
    let offsets = match data_type {
        DataType::Binary | DataType::Utf8 => rows.checked_add(1)?.checked_mul(4)?,
        DataType::LargeBinary | DataType::LargeUtf8 => rows.checked_add(1)?.checked_mul(8)?,
        DataType::BinaryView | DataType::Utf8View => rows.checked_mul(16)?,
        DataType::FixedSizeBinary(width) if *width >= 0 => {
            rows.checked_mul(u64::try_from(*width).ok()?)?
        }
        _ => return None,
    };
    offsets.checked_add(validity)
}

fn attributes_are_all_empty(batch: &RecordBatch) -> bool {
    let Some(column) = batch.column_by_name("attributes") else {
        return true;
    };
    let Some(attributes) = column.as_any().downcast_ref::<StringArray>() else {
        return false;
    };
    (0..attributes.len()).all(|row| attributes.is_null(row) || attributes.value(row).is_empty())
}

/// Bound allocations performed before schema alignment or carrier mapping.
///
/// Empty carrier attributes are the only mapped layout admitted here. They do
/// not invoke serde's object allocator. The per-Utf8 64-byte value capacity is
/// the largest explicit builder capacity in `mapping::new_builder` (the
/// residual attributes builder); fixed-width and offset/validity buffers use
/// Arrow's layout widths. Non-empty attributes and recursive Arrow layouts
/// fail closed to the committed snapshot.
fn transformation_expansion_bound(
    batches: &[RecordBatch],
    target: &SchemaRef,
    carrier: Option<&siglake_core::index_config::IndexConfig>,
) -> Option<u64> {
    let mut total = 0u64;
    for batch in batches {
        if carrier.is_some() {
            if !attributes_are_all_empty(batch) {
                return None;
            }
            for field in target.fields() {
                if matches!(field.data_type(), DataType::Binary | DataType::LargeBinary) {
                    // BinaryBuilder grows dynamically and does not expose an
                    // exact construction-capacity bound.
                    return None;
                }
                let mut bytes = null_array_payload_bound(field.data_type(), batch.num_rows())?;
                if matches!(field.data_type(), DataType::Utf8) {
                    batch.num_rows().checked_mul(64)?;
                    bytes = bytes
                        .checked_add(u64::try_from(batch.num_rows()).ok()?.checked_mul(64)?)?;
                }
                total = total.checked_add(bytes)?;
            }
            continue;
        }
        let schema = batch.schema();
        for field in target.fields() {
            if schema.field_with_name(field.name()).is_err() {
                total = total.checked_add(null_array_payload_bound(
                    field.data_type(),
                    batch.num_rows(),
                )?)?;
            }
        }
    }
    Some(total)
}

/// Decode exactly the generation admitted by [`select_buffer_segments`]. A
/// missing, replaced, corrupt, or unmappable member refuses the whole
/// generation; serving a subset would turn corruption or a listing race into
/// a silently inexact answer.
fn read_selected_buffer_batches(selected: &SelectedBufferSegments) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for (segment, reader) in selected.paths.iter().zip(&selected.readers) {
        let segment_batches = reader
            .read(reader.uncompressed_bytes())
            .with_context(|| format!("decode selected WAL segment {}", segment.display()))?;
        for batch in segment_batches {
            if batch.num_rows() == 0 {
                continue;
            }
            batches.push(batch);
        }
    }
    Ok(batches)
}

fn transform_selected_buffer_batches(
    batches: Vec<RecordBatch>,
    target_schema: &SchemaRef,
    carrier: Option<&siglake_core::index_config::IndexConfig>,
) -> Result<Vec<RecordBatch>> {
    batches
        .into_iter()
        .map(|batch| {
            let batch = match carrier {
                Some(config) => siglake_core::mapping::map_carrier_batch(&batch, config)
                    .context("map carrier batch for index buffer")?,
                None => batch,
            };
            align_batch_to_schema(&batch, target_schema)
        })
        .collect()
}

/// Read the un-committed WAL segments under `wal_dir` into batches aligned to
/// `target_schema`. `exclude` drops already-committed segments and `time_bound`
/// (`Some((lo, hi))` ns, half-open) prunes framed segments outside the query's
/// `timestamp` window (WS-8). Returns an empty vec when nothing survives. A
/// unreadable/corrupt member fails the generation; callers must fall back to
/// committed-only rather than serve a silently incomplete WAL slice.
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
    let (segments, excluded, pruned) = list_uncommitted_segments_bounded(
        wal_dir,
        exclude,
        time_bound,
        owner,
        buffer_delta_max_segments(),
    )?;
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
        let segment_batches = siglake_wal::read_segment(&seg)
            .with_context(|| format!("decode WAL buffer segment {}", seg.display()))?;
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

/// Count unique backing allocations for decoded IPC payload, excluding Arrow
/// object metadata. IPC columns can be slices of the same allocation: summing
/// `get_array_memory_size` would count that allocation once per slice and also
/// add metadata to a payload-only admission bound. Keep full backing capacity,
/// not just the visible slice length. Query-local filter/sort estimates below
/// deliberately retain their existing conservative array-memory accounting.
fn decoded_payload_bytes(batches: &[RecordBatch]) -> u64 {
    fn visit_buffer(buffer: &Buffer, allocations: &mut HashMap<usize, u64>) {
        let extent = buffer
            .capacity()
            .max(buffer.ptr_offset().saturating_add(buffer.len())) as u64;
        if extent == 0 {
            return;
        }
        let entry = allocations
            .entry(buffer.data_ptr().as_ptr() as usize)
            .or_default();
        *entry = (*entry).max(extent);
    }
    fn visit_array(data: &ArrayData, allocations: &mut HashMap<usize, u64>) {
        for buffer in data.buffers() {
            visit_buffer(buffer, allocations);
        }
        if let Some(nulls) = data.nulls() {
            visit_buffer(nulls.buffer(), allocations);
        }
        for child in data.child_data() {
            visit_array(child, allocations);
        }
    }
    let mut allocations = HashMap::new();
    for batch in batches {
        for column in batch.columns() {
            visit_array(&column.to_data(), &mut allocations);
        }
    }
    allocations
        .values()
        .fold(0u64, |total, bytes| total.saturating_add(*bytes))
}

fn batches_payload_bytes(batches: &[RecordBatch]) -> u64 {
    batches.iter().fold(0u64, |bytes, batch| {
        bytes.saturating_add(batch.get_array_memory_size() as u64)
    })
}

fn scalar_filter_leaf(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => scalar_filter_leaf(&alias.expr),
        Expr::Column(_) | Expr::Literal(_, _) => true,
        _ => false,
    }
}

/// Arrow 58.4 evaluates these scalar patterns with borrowed equality/contains
/// predicates, without a regex, string copy, or StringView prefix/suffix Vec.
/// Restrict the input to a direct column and the pattern to a scalar literal;
/// a function or coercive expression may allocate before producing its value.
pub(crate) fn bounded_literal_like(expr: &Expr) -> bool {
    let Expr::Like(like) = expr else {
        return false;
    };
    if like.case_insensitive
        || like.escape_char.is_some()
        || !matches!(like.expr.as_ref(), Expr::Column(_))
    {
        return false;
    }
    let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(pattern)), _) =
        like.pattern.as_ref()
    else {
        return false;
    };
    let plain = |s: &str| !s.bytes().any(|b| matches!(b, b'%' | b'_' | b'\\'));
    plain(pattern)
        || (pattern.len() >= 2
            && pattern.starts_with('%')
            && pattern.ends_with('%')
            && plain(&pattern[1..pattern.len() - 1]))
}

/// Only admit predicates whose physical evaluation is limited to scalar
/// comparisons and Boolean masks. String functions, casts, regular
/// expressions, user-defined functions, and arithmetic are declined before
/// evaluation because they can allocate an intermediate larger than both the
/// input and final Boolean result.
pub(crate) fn bounded_filter_mask_count(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Alias(alias) => bounded_filter_mask_count(&alias.expr),
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            bounded_filter_mask_count(&binary.left)?
                .checked_add(bounded_filter_mask_count(&binary.right)?)?
                .checked_add(1)
        }
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
            ) =>
        {
            (scalar_filter_leaf(&binary.left) && scalar_filter_leaf(&binary.right)).then_some(1)
        }
        Expr::Like(_) if bounded_literal_like(expr) => Some(2),
        Expr::Not(inner) => bounded_filter_mask_count(inner)?.checked_add(1),
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner) => scalar_filter_leaf(inner).then_some(1),
        Expr::Between(between) => (scalar_filter_leaf(&between.expr)
            && scalar_filter_leaf(&between.low)
            && scalar_filter_leaf(&between.high))
        .then_some(3),
        _ => None,
    }
}

/// Run the query-local filter under its own reservation. Filtered value
/// buffers cannot contain more values or variable-width payload than their
/// inputs; the extra term covers one value bitmap per predicate plus the
/// combined mask. The returned arrays own the reservation through their final
/// shallow clone.
fn filter_batches_charged(
    memory: &Arc<BufferMemoryBudget>,
    batches: &[RecordBatch],
    filters: &[Expr],
    schema: &SchemaRef,
    state: &dyn Session,
) -> Option<DFResult<Vec<RecordBatch>>> {
    if filters.is_empty() || batches.is_empty() {
        return Some(Ok(batches.to_vec()));
    }
    let Some(expression_masks) = filters.iter().try_fold(0u64, |count, filter| {
        count.checked_add(bounded_filter_mask_count(filter)?)
    }) else {
        metrics::counter!("siglake_query_wal_buffer_refused_filter_workspace_total").increment(1);
        return None;
    };
    let rows = batches.iter().fold(0u64, |rows, batch| {
        rows.saturating_add(batch.num_rows() as u64)
    });
    let mask_bytes = rows
        .div_ceil(8)
        .saturating_mul(expression_masks.saturating_add(filters.len() as u64));
    let bound = batches_payload_bytes(batches).saturating_add(mask_bytes);
    let reservation = memory.try_reserve(bound)?;
    let filtered = match filter_batches(batches, filters, schema, state) {
        Ok(filtered) => filtered,
        Err(error) => return Some(Err(error)),
    };
    if !reservation.contains_actual(batches_payload_bytes(&filtered)) {
        return Some(Err(DataFusionError::ResourcesExhausted(
            "WAL filter output exceeded its admitted payload bound".to_string(),
        )));
    }
    Some(
        attach_reservation_to_batches(filtered, &reservation)
            .map_err(|error| DataFusionError::External(error.into())),
    )
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
    cache: Arc<BufferDeltaCache>,
    wal_dir: PathBuf,
    /// Segment basenames already in the serving Iceberg snapshot (excluded).
    exclude: HashSet<String>,
    schema: SchemaRef,
    /// #61: set for a USER INDEX buffer — the doc-mapping applied to the
    /// carrier-schema WAL batches (the same mapping the commit path applies).
    /// `None` for the events table, whose WAL batches are already
    /// events-shaped.
    carrier: Option<siglake_core::index_config::IndexConfig>,
    /// Injected compressed-file ceiling; `None` reads the configured one. The sibling of
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
        Self::try_new_inner(
            base,
            Arc::new(BufferDeltaCache::default()),
            wal_dir,
            exclude,
            None,
            None,
            None,
        )
    }

    pub fn try_new_with_cache(
        base: Arc<dyn TableProvider>,
        cache: Arc<BufferDeltaCache>,
        wal_dir: &Path,
        exclude: &HashSet<String>,
    ) -> Result<Self> {
        Self::try_new_inner(base, cache, wal_dir, exclude, None, None, None)
    }

    /// [`Self::try_new`] with the compressed-file ceiling injected, so a backlog
    /// regression can price the scan gate and the fast-path delta gate against
    /// the SAME ceiling without a 2 GiB fixture.
    #[cfg(test)]
    pub(crate) fn try_new_with_max_bytes(
        base: Arc<dyn TableProvider>,
        wal_dir: &Path,
        exclude: &HashSet<String>,
        max_bytes: u64,
    ) -> Result<Self> {
        Self::try_new_inner(
            base,
            Arc::new(BufferDeltaCache::default()),
            wal_dir,
            exclude,
            None,
            Some(max_bytes),
            None,
        )
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
        Self::try_new_inner(
            base,
            Arc::new(BufferDeltaCache::default()),
            wal_dir,
            exclude,
            Some(config),
            None,
            owner,
        )
    }

    pub fn try_new_for_index_with_cache(
        base: Arc<dyn TableProvider>,
        cache: Arc<BufferDeltaCache>,
        wal_dir: &Path,
        exclude: &HashSet<String>,
        config: siglake_core::index_config::IndexConfig,
        owner: Option<&str>,
    ) -> Result<Self> {
        Self::try_new_inner(base, cache, wal_dir, exclude, Some(config), None, owner)
    }

    fn try_new_inner(
        base: Arc<dyn TableProvider>,
        cache: Arc<BufferDeltaCache>,
        wal_dir: &Path,
        exclude: &HashSet<String>,
        carrier: Option<siglake_core::index_config::IndexConfig>,
        max_bytes: Option<u64>,
        owner: Option<&str>,
    ) -> Result<Self> {
        let schema = base.schema();
        Ok(Self {
            base,
            cache,
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

enum ChargedSort {
    Sorted((Vec<RecordBatch>, arrow_schema::SortOptions, String)),
    Unsupported,
    Refused,
}

/// Sort under a per-query reservation. `concat_batches` can duplicate the
/// input payload once, `take_record_batch` once more, and the sort indices use
/// one 64-bit slot per row. Keeping the full workspace charge on the returned
/// batch is conservative and makes concurrent cache-hit sorts additive.
fn sort_batches_like_charged(
    memory: &Arc<BufferMemoryBudget>,
    batches: &[RecordBatch],
    ordering: &datafusion::physical_expr::LexOrdering,
    schema: &SchemaRef,
) -> ChargedSort {
    let rows = batches.iter().fold(0u64, |rows, batch| {
        rows.saturating_add(batch.num_rows() as u64)
    });
    let bound = batches_payload_bytes(batches)
        .saturating_mul(2)
        .saturating_add(rows.saturating_mul(8));
    let Some(reservation) = memory.try_reserve(bound) else {
        return ChargedSort::Refused;
    };
    let Some((sorted, options, field)) = sort_batches_like(batches, ordering, schema) else {
        return ChargedSort::Unsupported;
    };
    if !reservation.contains_actual(batches_payload_bytes(&sorted)) {
        return ChargedSort::Refused;
    }
    match attach_reservation_to_batches(sorted, &reservation) {
        Ok(sorted) => ChargedSort::Sorted((sorted, options, field)),
        Err(error) => {
            tracing::warn!(error = %error, "refusing WAL sort output whose ownership cannot be charged");
            ChargedSort::Refused
        }
    }
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

/// Keep a decoded generation's ownership reservation alive for as long as the
/// physical plan or any stream produced from it can retain shallow Arrow
/// buffers. Optimizer child replacement preserves the same lease.
struct WalBufferLeaseExec {
    inner: Arc<dyn ExecutionPlan>,
    decoded: Arc<DecodedBuffer>,
}

impl WalBufferLeaseExec {
    fn new(inner: Arc<dyn ExecutionPlan>, decoded: Arc<DecodedBuffer>) -> Self {
        Self { inner, decoded }
    }
}

impl std::fmt::Debug for WalBufferLeaseExec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WalBufferLeaseExec")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for WalBufferLeaseExec {
    fn fmt_as(
        &self,
        display: DisplayFormatType,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        self.inner.fmt_as(display, formatter)
    }
}

impl ExecutionPlan for WalBufferLeaseExec {
    fn name(&self) -> &str {
        "WalBufferLeaseExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "WalBufferLeaseExec expected one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            Arc::clone(&self.decoded),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner = self.inner.execute(partition, context)?;
        Ok(Box::pin(WalBufferLeaseStream {
            schema: inner.schema(),
            inner,
            _decoded: Arc::clone(&self.decoded),
        }))
    }
}

struct WalBufferLeaseStream {
    schema: SchemaRef,
    inner: SendableRecordBatchStream,
    _decoded: Arc<DecodedBuffer>,
}

impl Stream for WalBufferLeaseStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut TaskPollContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(context)
    }
}

impl RecordBatchStream for WalBufferLeaseStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
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
        // The cache takes one listing, prices that exact owner/consumed/window
        // generation, and holds its reservation through every shallow reader.
        // Capacity and corrupt/listing failures are explicit committed-only
        // fallbacks; no partial generation enters the union.
        let outcome = tokio::task::spawn_blocking({
            let cache = Arc::clone(&self.cache);
            let wal_dir = self.wal_dir.clone();
            let schema = self.schema.clone();
            let exclude = self.exclude.clone();
            let carrier = self.carrier.clone();
            let owner = self.owner.clone();
            let compressed_max = self.max_bytes;
            move || {
                cache.get_or_load_inner(
                    &wal_dir,
                    &schema,
                    &exclude,
                    bound,
                    carrier.as_ref(),
                    owner.as_deref(),
                    compressed_max,
                )
            }
        })
        .await
        .map_err(|error| DataFusionError::External(error.into()))?
        .map_err(|error| DataFusionError::External(error.into()))?;
        let decoded = match outcome {
            BufferReadOutcome::Loaded(decoded) => decoded,
            BufferReadOutcome::Refused { .. } => return Ok(base_plan),
        };
        if decoded.batches().is_empty() {
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
        let batches = match filter_batches_charged(
            &self.cache.memory,
            decoded.batches(),
            filters,
            &self.schema,
            state,
        ) {
            Some(Ok(filtered)) => filtered,
            // Never let a filter we cannot evaluate change the ANSWER: fall back
            // to unioning everything, which is correct but slow.
            Some(Err(e)) => {
                tracing::debug!(error = %e, "wal-buffer: filter pre-pass failed; unioning unfiltered");
                decoded.cloned_batches()
            }
            None => return Ok(base_plan),
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
            Some(ordering) => match sort_batches_like_charged(
                &self.cache.memory,
                &batches,
                ordering,
                &self.schema,
            ) {
                ChargedSort::Sorted((sorted_batches, options, field)) => {
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
                ChargedSort::Unsupported => (
                    MemTable::try_new(self.schema.clone(), vec![batches])?,
                    false,
                ),
                ChargedSort::Refused => return Ok(base_plan),
            },
            None => (
                MemTable::try_new(self.schema.clone(), vec![batches])?,
                false,
            ),
        };
        // The pre-pass above has already applied every pushed predicate. Do
        // not ask MemTable to allocate a second filtered copy. DataFusion's
        // parent filter remains because this provider advertises `Inexact`.
        let buffer_plan = buffer.scan(state, projection, &[], limit).await?;
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
                let plan: Arc<dyn ExecutionPlan> = Arc::new(
                    datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec::new(
                        ordering, union,
                    ),
                );
                return Ok(Arc::new(WalBufferLeaseExec::new(plan, decoded)));
            }
        }
        tracing::debug!(sorted, "wal-buffer union: plain union (no SPM)");
        Ok(Arc::new(WalBufferLeaseExec::new(union, decoded)))
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

    fn loaded(outcome: BufferReadOutcome) -> Arc<DecodedBuffer> {
        match outcome {
            BufferReadOutcome::Loaded(decoded) => decoded,
            BufferReadOutcome::Refused { compressed_bytes } => {
                panic!("tiny fixture was refused after {compressed_bytes} compressed bytes")
            }
        }
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

        let decoded = loaded(
            cache
                .get_or_load_for_query_keyed(tmp.path(), &schema, &exclude, None, None, None)
                .unwrap(),
        );
        assert_eq!(decoded.names(), names);
        let rows: usize = decoded.batches().iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);

        // Unchanged buffer state ⇒ hit with the same rows.
        let hit = cache
            .get(tmp.path(), &names, &schema, None, None)
            .expect("hit");
        assert_eq!(hit.batches().iter().map(|b| b.num_rows()).sum::<usize>(), 5);
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

        let error = cache
            .get_or_load_for_query_keyed(tmp.path(), &schema, &exclude, None, None, None)
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("decode selected WAL segment"),
            "{error:#}"
        );
        assert_eq!(cache.memory.used(), 0, "failed load retains no reservation");

        std::fs::write(segment, original).unwrap();
        let restored_names = uncommitted_segment_names(tmp.path(), &exclude, None).unwrap();
        let second = loaded(
            cache
                .get_or_load_for_query_keyed(tmp.path(), &schema, &exclude, None, None, None)
                .unwrap(),
        );

        assert_eq!(second.names(), restored_names);
        assert_eq!(
            second
                .batches()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            5
        );
        assert_eq!(
            cache.stats(),
            (0, 1),
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

        let first = loaded(
            cache
                .get_or_load_for_query(tmp.path(), &schema, &exclude, time_bound, None, None)
                .unwrap(),
        );
        let second = loaded(
            cache
                .get_or_load_for_query(tmp.path(), &schema, &exclude, time_bound, None, None)
                .unwrap(),
        );

        assert_eq!(
            first
                .batches()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            5
        );
        assert_eq!(
            second
                .batches()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            5
        );
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
        let all = loaded(
            cache
                .get_or_load_for_query(tmp.path(), &schema, &exclude, None, None, None)
                .unwrap(),
        );
        assert_eq!(
            all.batches()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            5
        );

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let recent_only = Some((now - 60_000_000_000, i64::MAX));
        let bounded = loaded(
            cache
                .get_or_load_for_query(tmp.path(), &schema, &exclude, recent_only, None, None)
                .unwrap(),
        );

        // The full cached batches are a safe superset: the distributed SQL
        // still applies this timestamp predicate, and no segment is decoded a
        // second time after the Tier-1 delta load.
        assert_eq!(
            bounded
                .batches()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            5
        );
        assert_eq!(cache.stats(), (1, 1), "bounded call must reuse full decode");
    }

    #[test]
    fn wal_memory_default_tracks_the_container_without_relabeling_disk_bytes() {
        assert_eq!(
            buffer_delta_memory_bytes_from(None, Some(4 * GIB)),
            512 * MIB
        );
        assert_eq!(buffer_delta_memory_bytes_from(None, Some(64 * GIB)), GIB);
        assert_eq!(buffer_delta_memory_bytes_from(None, None), 512 * MIB);
        assert_eq!(
            buffer_delta_memory_bytes_from(Some("12345"), Some(4 * GIB)),
            12345
        );
        assert_eq!(buffer_delta_memory_bytes_from(Some("0"), Some(4 * GIB)), 0);
    }

    #[test]
    fn compressible_segment_is_refused_by_decoded_admission_before_read() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer =
            WalWriter::with_thresholds(tmp.path(), "ing", 10_000, Duration::from_secs(60)).unwrap();
        writer
            .append_events(&[Event::now("compressible".repeat(128 * 1024))])
            .unwrap();
        writer.seal().unwrap().expect("sealed segment");

        let cache = BufferDeltaCache::with_memory_limit(64 * 1024);
        let outcome = cache
            .get_or_load_for_query(
                tmp.path(),
                &siglake_core::events_schema(),
                &HashSet::new(),
                None,
                None,
                None,
            )
            .unwrap();
        assert!(matches!(outcome, BufferReadOutcome::Refused { .. }));
        assert_eq!(cache.stats(), (0, 0), "a refused generation is not cached");
        assert_eq!(cache.memory.used(), 0, "failed admission leaves no lease");
    }

    #[test]
    fn internally_compressed_ipc_is_refused_before_arrow_decode() {
        let rows = 1_000_000;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from_iter_values(
                std::iter::repeat_n(0, rows),
            ))],
        )
        .unwrap();
        let options = arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut encoded = Vec::new();
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new_with_options(
                &mut encoded,
                &schema,
                options,
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        assert!(
            encoded.len() * 16 < rows * std::mem::size_of::<i64>(),
            "fixture must have a much smaller outer IPC extent than decoded values"
        );
        let error = siglake_wal::read_segment_bytes(&encoded).unwrap_err();
        assert!(
            error.to_string().contains("IPC body compression"),
            "compressed IPC must be refused from metadata: {error:#}"
        );
        let tmp = tempfile::tempdir().unwrap();
        let sealed = tmp.path().join(SEALED_DIR);
        std::fs::create_dir_all(&sealed).unwrap();
        std::fs::write(sealed.join("compressed.arrow"), &encoded).unwrap();
        let memory_limit = (encoded.len() as u64) * 4;
        assert!(
            memory_limit * 16 < (rows * std::mem::size_of::<i64>()) as u64,
            "fixture budget must be tiny relative to decoded numeric payload"
        );
        let cache = BufferDeltaCache::with_memory_limit(memory_limit);
        let error = cache
            .get_or_load_for_query(tmp.path(), &schema, &HashSet::new(), None, None, None)
            .unwrap_err();
        assert!(format!("{error:#}").contains("IPC body compression"));
        assert_eq!(cache.memory.used(), 0, "refusal releases predecode charge");
    }

    #[test]
    fn null_ipc_alignment_is_sized_from_rows_and_target_layout() {
        let rows = 10_000_000;
        let source = Arc::new(Schema::new(vec![Field::new(
            "compact",
            DataType::Null,
            true,
        )]));
        let batch = RecordBatch::try_new(
            source,
            vec![arrow_array::new_null_array(&DataType::Null, rows)],
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![
            Field::new("compact", DataType::Null, true),
            Field::new("expanded", DataType::Int64, true),
        ]));
        let bound = transformation_expansion_bound(&[batch], &target, None).unwrap();
        assert!(bound >= 80_000_000, "fixed-width target is row-sized");
    }

    #[test]
    fn nonempty_carrier_attributes_are_refused_before_mapping() {
        let batch = siglake_core::events_to_record_batch(&[
            Event::now("mapped").with_attributes(Some(r#"{"nested":{"value":1}}"#.into()))
        ])
        .unwrap();
        let config = siglake_core::index_config::IndexConfig::builtin_events();
        assert!(
            transformation_expansion_bound(&[batch], &config.to_arrow_schema(), Some(&config),)
                .is_none()
        );
    }

    #[test]
    fn empty_carrier_mapping_keeps_parity_under_its_layout_bound() {
        let batch = siglake_core::events_to_record_batch(&[Event::now("mapped")]).unwrap();
        let config = siglake_core::index_config::IndexConfig::builtin_events();
        let target = config.to_arrow_schema();
        assert!(transformation_expansion_bound(
            std::slice::from_ref(&batch),
            &target,
            Some(&config),
        )
        .is_some());
        let expected = siglake_core::mapping::map_carrier_batch(&batch, &config).unwrap();
        let actual = transform_selected_buffer_batches(vec![batch], &target, Some(&config))
            .unwrap()
            .remove(0);
        assert_eq!(actual, expected);
    }

    #[test]
    fn partial_output_bound_covers_split_builder_capacity_and_refuses_views() {
        use arrow_array::builder::StringBuilder;
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("n", DataType::Int64, false),
        ]));
        let mut partials = Vec::new();
        for host in ["host-0", "host-1", "host-2"] {
            let mut strings = StringBuilder::with_capacity(1, 8192);
            strings.append_value(host);
            partials.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(strings.finish()),
                        Arc::new(arrow_array::Int64Array::from(vec![3])),
                    ],
                )
                .unwrap(),
            );
        }
        let old_logical_bound = 3276 + 9 * 8 + 10 * 4 + 4;
        assert!(decoded_payload_bytes(&partials) > old_logical_bound);
        let bound = bounded_output_payload(&schema, 9, 3276).unwrap();
        let budget = BufferMemoryBudget::new(bound);
        let reservation = budget.try_reserve(bound).unwrap();
        let leased = BufferOutputLease { reservation }.attach(partials).unwrap();
        assert_eq!(leased.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert_eq!(budget.used(), bound);
        drop(leased);
        assert_eq!(budget.used(), 0);
        let large = "x".repeat(32 * 1024);
        let mut suffixes = Vec::new();
        for start in 0..8 {
            let strings = StringArray::from(vec![large.as_str(); 8 - start]);
            let one = strings.slice(0, 1);
            suffixes.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(one),
                        Arc::new(arrow_array::Int64Array::from(vec![1])),
                    ],
                )
                .unwrap(),
            );
        }
        let bound = bounded_output_payload(&schema, 8, (large.len() * 8) as u64).unwrap();
        assert!(decoded_payload_bytes(&suffixes) <= bound);
        // The previous twice-total-input bound plus all its builder floors
        // is below this threshold; retained suffixes exceed it.
        assert!(decoded_payload_bytes(&suffixes) > (large.len() * 8 * 2 + 256 * 1024) as u64);
        for data_type in [DataType::Utf8View, DataType::BinaryView] {
            let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
            assert!(bounded_output_payload(&schema, 3, 3276).is_none());
        }
    }

    #[test]
    fn empty_values_backing_retains_output_reservation_after_parent_drop() {
        use arrow_array::builder::StringBuilder;
        let mut strings = StringBuilder::with_capacity(1, 8192);
        strings.append_value("");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(strings.finish())],
        )
        .unwrap();
        assert!(batch.column(0).to_data().buffers()[1].is_empty());
        assert!(batch.column(0).to_data().buffers()[1].capacity() >= 8192);
        let charge = decoded_payload_bytes(std::slice::from_ref(&batch));
        let budget = BufferMemoryBudget::new(charge);
        let reservation = budget.try_reserve(charge).unwrap();
        let leased = BufferOutputLease { reservation }
            .attach(vec![batch])
            .unwrap();
        let values = leased[0].column(0).to_data().buffers()[1].clone();
        drop(leased);
        assert_eq!(
            budget.used(),
            charge,
            "empty values still retain their allocation"
        );
        drop(values);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn bounded_like_accepts_borrowed_patterns_and_refuses_allocating_shapes() {
        use datafusion::logical_expr::{col, lit};
        for pattern in ["plain", "%row%", "%%", "%é%"] {
            assert_eq!(
                bounded_filter_mask_count(&col("raw").like(lit(pattern))),
                Some(2)
            );
            assert_eq!(
                bounded_filter_mask_count(&col("raw").not_like(lit(pattern))),
                Some(2)
            );
        }
        for pattern in ["prefix%", "%suffix", "%a%b%", "%_%", "%\\_%"] {
            assert!(bounded_filter_mask_count(&col("raw").like(lit(pattern))).is_none());
        }
        assert!(bounded_filter_mask_count(&col("raw").ilike(lit("%row%"))).is_none());
        assert!(bounded_filter_mask_count(&col("raw").like(col("pattern"))).is_none());
        let allocating =
            datafusion::functions::string::repeat().call(vec![col("raw"), lit(1_000_000i64)]);
        assert!(bounded_filter_mask_count(&allocating.like(lit("%row%"))).is_none());
    }

    #[test]
    fn decoded_payload_counts_shared_backing_once_and_retains_hidden_capacity() {
        let backing = Buffer::from(vec![7u8; 128]);
        let column = |offset| {
            make_array(
                ArrayData::builder(DataType::UInt8)
                    .len(4)
                    .add_buffer(backing.slice_with_length(offset, 4))
                    .build()
                    .unwrap(),
            )
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::UInt8, false),
                Field::new("b", DataType::UInt8, false),
            ])),
            vec![column(8), column(96)],
        )
        .unwrap();
        assert_eq!(decoded_payload_bytes(&[batch.clone(), batch]), 128);
    }

    #[test]
    fn decoded_ipc_payload_fits_admission_despite_shared_column_views() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 2);
        let selected =
            select_buffer_segments(tmp.path(), &HashSet::new(), None, None, u64::MAX).unwrap();
        let raw = read_selected_buffer_batches(&selected).unwrap();
        assert!(
            batches_payload_bytes(&raw) > selected.decode_charge_bytes,
            "fixture must falsify the old repeated-allocation/metadata accounting"
        );
        assert!(decoded_payload_bytes(&raw) <= selected.decode_charge_bytes);
        let cache = BufferDeltaCache::with_memory_limit(64 * MIB);
        let decoded = loaded(
            cache
                .get_or_load_for_query(
                    tmp.path(),
                    &siglake_core::events_schema(),
                    &HashSet::new(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        assert_eq!(
            decoded
                .batches()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
    }

    #[test]
    fn allocating_filter_is_refused_for_two_cache_hits_without_a_charge() {
        use datafusion::functions::string::repeat;
        use datafusion::prelude::{col, lit};

        let batch = siglake_core::events_to_record_batch(&[Event::now("x")]).unwrap();
        let schema = batch.schema();
        let filter = repeat()
            .call(vec![col("raw"), lit(1_000_000i64)])
            .eq(lit("never"));
        let state = SessionContext::new().state();
        let memory = BufferMemoryBudget::new(MIB);
        for _ in 0..2 {
            assert!(filter_batches_charged(
                &memory,
                std::slice::from_ref(&batch),
                std::slice::from_ref(&filter),
                &schema,
                &state,
            )
            .is_none());
            assert_eq!(memory.used(), 0);
        }
    }

    #[test]
    fn admitted_listing_is_the_only_generation_decoded() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 2);
        let selected =
            select_buffer_segments(tmp.path(), &HashSet::new(), None, None, u64::MAX).unwrap();
        write_sealed_segment(tmp.path(), 3);

        let batches = read_selected_buffer_batches(&selected).unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(
            selected.names.len(),
            1,
            "the later seal is a later generation"
        );
    }

    #[test]
    fn corrupt_or_disappeared_selected_member_refuses_the_generation() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 2);
        let selected =
            select_buffer_segments(tmp.path(), &HashSet::new(), None, None, u64::MAX).unwrap();
        std::fs::remove_file(&selected.paths[0]).unwrap();
        assert!(read_selected_buffer_batches(&selected).is_err());
    }

    #[test]
    fn replaced_generation_and_reader_are_both_charged_until_drop() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 2);
        let cache = BufferDeltaCache::with_memory_limit(64 * MIB);
        let schema = siglake_core::events_schema();
        let first = loaded(
            cache
                .get_or_load_for_query(tmp.path(), &schema, &HashSet::new(), None, None, None)
                .unwrap(),
        );
        let first_charge = cache.memory.used();
        assert!(first_charge > 0);

        write_sealed_segment(tmp.path(), 3);
        let second = loaded(
            cache
                .get_or_load_for_query(tmp.path(), &schema, &HashSet::new(), None, None, None)
                .unwrap(),
        );
        assert!(
            cache.memory.used() > first_charge,
            "old reader and replacement generation overlap in the budget"
        );
        cache.evict(tmp.path());
        assert!(
            cache.memory.used() > first_charge,
            "reader Arcs retain both leases"
        );
        drop(second);
        assert_eq!(cache.memory.used(), first_charge);
        drop(first);
        assert_eq!(cache.memory.used(), 0);
    }

    #[test]
    fn retained_plain_batch_keeps_charge_after_generation_and_cache_drop() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 2);
        let cache = BufferDeltaCache::with_memory_limit(64 * MIB);
        let decoded = loaded(
            cache
                .get_or_load_for_query(
                    tmp.path(),
                    &siglake_core::events_schema(),
                    &HashSet::new(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let retained = decoded.batches()[0].clone();
        let charged = cache.memory.used();
        assert!(charged > 0);

        cache.evict(tmp.path());
        drop(decoded);
        assert_eq!(
            cache.memory.used(),
            charged,
            "plain Arrow buffers, not the cache entry, must own the charge"
        );
        drop(retained);
        assert_eq!(cache.memory.used(), 0);
    }

    #[tokio::test]
    async fn completed_union_output_keeps_charge_after_plan_stream_and_cache_drop() {
        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 2);
        let schema = siglake_core::events_schema();
        let base = Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap());
        let cache = Arc::new(BufferDeltaCache::with_memory_limit(64 * MIB));
        let provider = UnionEventsProvider::try_new_with_cache(
            base,
            Arc::clone(&cache),
            tmp.path(),
            &HashSet::new(),
        )
        .unwrap();
        let ctx = SessionContext::new();
        let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
        let output = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        let charged = cache.memory.used();
        assert!(charged > 0);

        cache.evict(tmp.path());
        drop(provider);
        assert_eq!(
            cache.memory.used(),
            charged,
            "completed output must retain the charge after EOF and plan drop"
        );
        drop(output);
        assert_eq!(cache.memory.used(), 0);
    }

    #[tokio::test]
    async fn simultaneous_cache_hit_filters_have_additive_output_charges() {
        use datafusion::prelude::{col, lit};

        let tmp = tempfile::tempdir().unwrap();
        write_sealed_segment(tmp.path(), 4);
        let schema = siglake_core::events_schema();
        let cache = BufferDeltaCache::with_memory_limit(64 * MIB);
        let decoded = loaded(
            cache
                .get_or_load_for_query(tmp.path(), &schema, &HashSet::new(), None, None, None)
                .unwrap(),
        );
        let state = SessionContext::new().state();
        let filters = [col("raw").eq(lit("buffered 1"))];
        let generation_charge = cache.memory.used();
        let first =
            filter_batches_charged(&cache.memory, decoded.batches(), &filters, &schema, &state)
                .expect("first query admitted")
                .unwrap();
        let one_output_charge = cache.memory.used();
        assert!(one_output_charge > generation_charge);
        let second =
            filter_batches_charged(&cache.memory, decoded.batches(), &filters, &schema, &state)
                .expect("second query admitted")
                .unwrap();
        assert!(
            cache.memory.used() > one_output_charge,
            "concurrent cache hits must not share one transformation charge"
        );

        drop(first);
        assert_eq!(cache.memory.used(), one_output_charge);
        drop(second);
        assert_eq!(cache.memory.used(), generation_charge);
    }

    #[test]
    fn simultaneous_reservations_refuse_without_waiting() {
        let budget = BufferMemoryBudget::new(100);
        let first = budget.try_reserve(60).expect("first reservation");
        assert!(
            budget.try_reserve(50).is_none(),
            "no wait queue or overcommit"
        );
        drop(first);
        assert!(budget.try_reserve(50).is_some());
    }

    #[tokio::test]
    async fn cancelled_join_handle_does_not_release_blocking_child_reservation() {
        let budget = BufferMemoryBudget::new(100);
        let child_budget = Arc::clone(&budget);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let child = tokio::task::spawn_blocking(move || {
            let _reservation = child_budget.try_reserve(80).expect("child reservation");
            started_tx.send(()).unwrap();
            release_rx.blocking_recv().unwrap();
        });
        started_rx.await.unwrap();
        child.abort();
        assert_eq!(budget.used(), 80, "abort cannot stop spawn_blocking work");
        release_tx.send(()).unwrap();
        let _ = child.await;
        assert_eq!(budget.used(), 0);
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
