//! Write-Ahead Log: durably persists incoming events as sealed Arrow IPC
//! stream segments before the compactor batches them into row-group-optimized
//! Parquet via Iceberg.
//!
//! ## Lifecycle
//!
//! - **Active**: an open segment file at `{dir}/active/{ingester}-{uuid}.arrow.partial`.
//!   The [`WalWriter`] streams `RecordBatch`es into this file as events arrive.
//! - **Sealed**: when the active segment hits `max_events` or `max_age`, the
//!   writer finishes the IPC stream, fsyncs, and atomically renames it to
//!   `{dir}/sealed/{ingester}-{uuid}.arrow`.
//! - **Processing**: the compactor claims a sealed segment by renaming it
//!   into `{dir}/processing/{ingester}-{uuid}.arrow`. After a successful
//!   Iceberg commit it deletes the segment.
//!
//! Each step that crosses a durability boundary is an atomic filesystem rename,
//! so a crash at any point leaves the WAL in a consistent state. Local
//! filesystem rename is atomic; on S3 a rename is list+copy+delete and is
//! *not* atomic — phase 4 will switch the claim mechanism to a catalog-
//! tracked state row instead of relying on rename atomicity.

pub mod mirror;

use std::fs::{self, File};
use std::io::{BufWriter, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use uuid::Uuid;

use siglake_core::{events_schema, events_to_record_batch, Event};

pub const ACTIVE_DIR: &str = "active";
pub mod consumer;

pub const SEALED_DIR: &str = "sealed";

/// Sidecar extension carrying a sealed segment's integrity check.
pub const CRC_SIDECAR_EXT: &str = "crc";

// ---- WS-8 framed WAL segment ---------------------------------------------
//
// A segment is written with a fixed-size header followed by the body
// (Arrow IPC stream; zstd-compressed once sealed). The header makes a
// segment self-describing: an in-band CRC over the body (superseding the `.crc`
// sidecar), the body's min/max event timestamps so a reader can prune a
// segment by time range from the header alone without decoding it, and (v2)
// the UUID of the Iceberg table the rows were written for. Segments
// written before framing are raw Arrow IPC (± a `.crc` sidecar); the reader
// sniffs the magic and falls back to the legacy path for them.

/// Magic prefixing a framed segment. Legacy raw-IPC segments never start
/// with this (Arrow IPC streams begin with a continuation/metadata marker), so
/// it is an unambiguous format discriminator.
pub const WAL_FRAME_MAGIC: &[u8; 4] = b"LWAL";
/// v1: no owner UUID. Still read; never written.
const WAL_FRAME_VERSION_V1: u8 = 1;
/// Current framed-segment version: v1 plus the 16-byte owner UUID (#2693).
const WAL_FRAME_VERSION: u8 = 2;
/// v1 header length: magic(4) + version(1) + flags(1) + reserved(2)
/// + min_ts(8) + max_ts(8) + body_len(8) + body_crc(4).
const WAL_FRAME_HEADER_LEN_V1: usize = 36;
/// v2 header length: [`WAL_FRAME_HEADER_LEN_V1`] + owner_uuid(16).
const WAL_FRAME_HEADER_LEN: usize = 52;
/// FLAGS bit0: the body is zstd-compressed.
const WAL_FRAME_FLAG_ZSTD: u8 = 0b0000_0001;
/// FLAGS bit1: the frame is an OPEN active segment — `body_len` and `body_crc`
/// are not yet known (zero) and `min_ts`/`max_ts` carry no range. The body is
/// an Arrow IPC stream still missing its EOS marker.
///
/// An active segment is framed for one reason: identity. A `.arrow.partial`
/// promoted by [`recover_orphaned_partials`], or pulled back out of the active
/// mirror by [`mirror::recover_from_object_store`], becomes a sealed segment
/// without ever passing through [`WalWriter::seal`] — so if the header were
/// written only at seal time, exactly the populations that survive a crash
/// would be the ones with no owner.
const WAL_FRAME_FLAG_PARTIAL: u8 = 0b0000_0010;
/// The four bytes that precede an Arrow IPC message's metadata length in the
/// stream format (pre-0.15 writers omit them). `arrow-ipc` keeps its own copy
/// private, so [`ipc_stream_extent`] carries one to walk the framing.
const IPC_CONTINUATION_MARKER: [u8; 4] = [0xFF; 4];
/// zstd level for the framed body. Level 1 — the WAL is latency-sensitive and
/// short-lived; level 1 gives most of the ratio at a fraction of higher levels'
/// CPU, and the seal path is off the ingest ack hot path anyway.
const WAL_ZSTD_LEVEL: i32 = 1;

/// Header metadata of a framed segment, read without decoding the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalFrameMeta {
    /// Minimum event timestamp (ns since epoch) in the segment.
    pub min_ts_nanos: i64,
    /// Maximum event timestamp (ns since epoch) in the segment.
    pub max_ts_nanos: i64,
}

// ---- WAL segment integrity (WS-8): CRC32 (IEEE) over the segment bytes ----
//
// Computed incrementally on write (no re-read at seal) and persisted in a tiny
// `<segment>.crc` sidecar. `read_segment` validates it when present, so a
// corrupted segment is rejected instead of feeding garbage to the compactor.
// Back-compat: a segment with no sidecar is read as before.

const CRC32_TABLE: [u32; 256] = {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
};

fn crc32_update(state: u32, bytes: &[u8]) -> u32 {
    let mut c = state;
    for &b in bytes {
        c = CRC32_TABLE[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    c
}

/// CRC32 (IEEE) of a complete byte slice.
fn crc32(bytes: &[u8]) -> u32 {
    crc32_update(0xFFFF_FFFF, bytes) ^ 0xFFFF_FFFF
}

/// `Write` adapter that CRC32s every byte it forwards to the inner writer, so a
/// segment's checksum falls out of the write path with no extra I/O.
struct CrcWriter<W: std::io::Write> {
    inner: W,
    state: u32,
}

impl<W: std::io::Write> CrcWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            state: 0xFFFF_FFFF,
        }
    }
    /// Finalize the running CRC and recover the inner writer.
    fn into_parts(self) -> (u32, W) {
        (self.state ^ 0xFFFF_FFFF, self.inner)
    }
}

impl<W: std::io::Write> std::io::Write for CrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.state = crc32_update(self.state, &buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
pub const PROCESSING_DIR: &str = "processing";
/// Quarantine dir for segments the compactor abandoned mid-cycle —
/// see [`recover_orphaned_processing`]. Phase 4.13h: round 12
/// surfaced ~10% of acknowledged events sitting in `processing/`
/// at end-of-test, with no compactor log to explain it. Rather
/// than re-commit blindly (risks Iceberg-side duplicates if
/// `commit_claimed` did succeed and `finish_segment` silently
/// failed on EFS), we quarantine the orphans for ops to inspect.
pub const ORPHANS_DIR: &str = "orphans";

/// `poison/` holds segments the local drain set aside because reading them
/// failed on every attempt it gave them — a truncated restore, a bad sector,
/// or a frame this build cannot decode (#3143). Distinct from `orphans/` on
/// purpose: orphan disposition deletes or requeues its residents on every
/// cycle, and a segment that can never be read has to stay put instead of
/// taking the next batch down with it.
///
/// Layout is `poison/<segment>.arrow` beside
/// `poison/<segment>.arrow.poison.json`, a [`PoisonNote`] recording why and
/// after how many attempts. The segment bytes are never rewritten and never
/// deleted; [`requeue_poisoned_segment`] is the deliberate way back into
/// `sealed/`.
pub const POISON_DIR: &str = "poison";

/// `committed/` is where the compactor moves segments after a successful
/// Iceberg commit, *instead of deleting them*. A retention sweep
/// eventually removes them. This gives secondary consumers (notably the
/// detector) a wider window to catch up before the segment is gone.
pub const COMMITTED_DIR: &str = "committed";

/// `consumers/` holds one small file per **secondary WAL consumer** (e.g. each
/// detector shard: `detector-<index>`) whose contents are the name of the last
/// segment that consumer has fully processed — its watermark. The compactor's
/// coordinated sweep keeps a `committed/` segment until every *fresh* consumer
/// has passed it, so detection never loses a segment to reaping. A consumer
/// whose watermark hasn't been updated within the staleness window is treated
/// as dead and stops holding segments back.
pub const CONSUMERS_DIR: &str = "consumers";

/// `stale/` holds segments quarantined because the directory's [`OWNER_FILE`]
/// named a different Iceberg table than the one its index name resolves to
/// today — a dropped-and-recreated index (#2661). Kept, not deleted: the rows
/// may be the only copy of data the dropped table's files still hold, and
/// deciding that is an operator's call, not the drain's. Layout is
/// `stale/<dropped-table-uuid>/<segment>.arrow`, flat, so nothing that walks
/// the WAL layout can mistake it for a live directory.
pub const STALE_DIR: &str = "stale";

/// `owner` records the UUID of the Iceberg table this WAL directory's segments
/// are destined for, as a single line of text.
///
/// A per-index WAL directory is keyed by the index NAME only — the ingester
/// never learns a table UUID (it does not link `siglake-storage`), so a
/// `DELETE`+`POST` of the same index id leaves its directory in place with the
/// dropped incarnation's segments in it, and every reader that resolves the
/// target by name folds them into the replacement. The marker is the identity
/// the name does not carry. It is written by the drain, which is the one
/// component that both resolves the table and visits every directory each
/// cycle; readers only compare against it.
///
/// Absent means "no opinion", so a directory written before this marker
/// existed keeps serving exactly as it did and self-heals on its first drain.
pub const OWNER_FILE: &str = "owner";

/// Default size threshold: roll the segment after this many events.
pub const DEFAULT_MAX_EVENTS: usize = 4096;
/// Default age threshold: roll the segment after this much wall time has
/// elapsed since its first append, regardless of size.
pub const DEFAULT_MAX_AGE: Duration = Duration::from_secs(5);

/// A sealed WAL segment file on disk, ready for compaction.
#[derive(Debug, Clone)]
pub struct WalSegment {
    pub path: PathBuf,
    pub rows: usize,
    pub bytes: u64,
    /// Object-store mirror key suffix. Flat legacy layout uses just
    /// `<filename>`; tenant-aware layouts use `<tenant>/<filename>`.
    pub mirror_key_suffix: String,
}

/// Single-writer WAL backed by a directory of Arrow IPC segment files.
///
/// Not [`Sync`]: the ingest server should wrap a `WalWriter` in a
/// `tokio::sync::Mutex` (or spin up a dedicated writer task fed by an
/// mpsc channel — pick one).
pub struct WalWriter {
    dir: PathBuf,
    ingester_id: String,
    schema: SchemaRef,
    max_events: usize,
    max_age: Duration,
    current: Option<ActiveSegment>,
    mirror: Option<mirror::WalMirrorHandle>,
    mirror_subdir: Option<String>,
    /// The Iceberg table this writer's rows are destined for, when the process
    /// hosting it can resolve one. Stamped into every segment header it opens.
    table_uuid: Option<Uuid>,
}

struct ActiveSegment {
    /// `{dir}/active/{ingester}-{uuid}.arrow.partial`
    active_path: PathBuf,
    /// Where it will live once sealed: `{dir}/sealed/{ingester}-{uuid}.arrow`.
    final_path: PathBuf,
    writer: StreamWriter<BufWriter<CrcWriter<File>>>,
    rows: usize,
    started_at: Instant,
    /// Min/max event timestamp (ns) folded across this segment's appends, for
    /// the framed header. `min_ts > max_ts` (sentinels) ⇒ no rows seen yet.
    min_ts: i64,
    max_ts: i64,
    /// The table identity in force when this segment was OPENED, copied here so
    /// the seal stamps what the rows were written for rather than whatever the
    /// writer has been rebound to since.
    owner: Option<Uuid>,
    /// Whether `active/` has been fsynced since this segment's name was created
    /// in it. One directory sync per segment covers every later append to it;
    /// the entry does not change again until the seal renames it away.
    dir_synced: bool,
}

// ---- Directory durability (#3048) ----------------------------------------
//
// `File::sync_all` makes a file's CONTENTS durable and says nothing about the
// directory entry that names them. On the filesystems siglake supports (ext4,
// xfs, and the node-attached persistent volumes the chart mounts) a crash
// between the two can bring the volume back with the file unreachable, or —
// after a rename — still under its old name. Atomic rename orders the two
// names against each other; it does not make either of them durable.
//
// So every step an acknowledgement depends on syncs the directory it changed:
//
//   * creating a WAL tree syncs each new directory's parent, so a freshly
//     created tenant/index directory is itself durable before rows land in it;
//   * `sync_active` — the `commit=wait_for` step — syncs `active/` once per
//     segment, after the file sync;
//   * `seal` syncs `sealed/` after the rename and BEFORE unlinking the active
//     copy, so no crash window can drop both names;
//   * partial recovery syncs `sealed/` and `active/` before it reports what it
//     promoted.
//
// A required sync that fails fails its operation, always leaving the rows
// reachable under some name for the next recovery pass. `commit=auto` is
// untouched: it acks on `write(2)` and takes no fsync of any kind, directory
// ones included.
//
// #3149 extends the same rule past the acknowledgement, to every lifecycle
// move a drain or a restore reports as done:
//
//   * `claim_segment`, `release_segment`, `finish_segment`,
//     `recover_orphaned_processing`, `quarantine_stale_wal_dir` and
//     `quarantine_stale_segment` are renames between two directories, so both
//     are synced — destination first, then source. Neither order can lose the
//     segment: a crash in between leaves it under one name or, at worst, two,
//     and the two-names case is what the orphan sweep and the compactor's
//     consumed proof already reconcile. Losing the destination entry while the
//     source one is gone is the outcome worth spending the fsyncs on.
//   * `stamp_wal_owner` and `publish_consumer_watermark` publish a small file
//     by temp + rename, so the temp is fsynced before the rename and the
//     directory after it. An owner stamp that does not survive reads back as
//     "no opinion" to the next drain (#2661/#2835); a lost watermark makes
//     retention less patient than the consumer asked for.
//   * `mirror::recover_from_object_store` — the `siglake wal-recover` DR path
//     — writes each restored segment to a temp name, fsyncs it, renames it and
//     syncs the directory before counting it, so a restored name is whole or
//     absent. It skips destinations that already exist, and that skip is only
//     safe if what exists is never a half-written file.
//
// #3169 applies it to the last file the crate publishes, a secondary
// consumer's own cursor (`consumer::SegmentConsumer::persist`), and to the
// state directory that holds it. Delivery stays at-least-once; what the syncs
// buy is the size of the replay window, since a reverted cursor resumes from
// the last position that reached the device and re-reads everything after it.
//
// These moves follow an acknowledgement rather than carrying one, so none of
// them loses a row on its own: every lost rename replays. What they cost is
// two fsyncs of a directory inode per segment per move, on a commit path whose
// object-store writes and catalog commit are three orders of magnitude larger.
mod durability {
    use std::fs::{self, File};
    use std::path::Path;

    use anyhow::{Context, Result};

    /// fsync `dir` so entries created, renamed or removed in it survive a
    /// power loss.
    pub(super) fn sync_dir(dir: &Path) -> Result<()> {
        step("sync_dir", dir)?;
        let handle =
            File::open(dir).with_context(|| format!("open {} for fsync", dir.display()))?;
        handle
            .sync_all()
            .with_context(|| format!("fsync directory {}", dir.display()))
    }

    /// fsync a file's contents. `at` names it for the error and the op log.
    pub(super) fn sync_file(file: &File, at: &Path) -> Result<()> {
        step("sync_file", at)?;
        file.sync_all()
            .with_context(|| format!("fsync {}", at.display()))
    }

    /// Rename `from` onto `to`. The new name becomes durable only once the
    /// destination directory is synced — see [`sync_dir`].
    pub(super) fn rename(from: &Path, to: &Path) -> Result<()> {
        step("rename", to)?;
        fs::rename(from, to)
            .with_context(|| format!("rename {} -> {}", from.display(), to.display()))
    }

    /// Write `bytes` to `path` and fsync the file, so its contents are durable
    /// before the rename that publishes them. For the small marker files
    /// (`owner`, a consumer watermark) and for a restored mirror segment.
    pub(super) fn write_sync(path: &Path, bytes: &[u8]) -> Result<()> {
        use std::io::Write;

        step("write", path)?;
        let mut file = File::create(path).with_context(|| format!("create {}", path.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write {}", path.display()))?;
        sync_file(&file, path)
    }

    /// Publish `bytes` at `path` by writing an fsynced temp sibling, renaming
    /// it into place and syncing the directory that names it. A reader racing
    /// this sees the old contents or the new ones, and a power loss leaves one
    /// of the two rather than a truncated file.
    pub(super) fn publish_file(path: &Path, tmp: &Path, bytes: &[u8]) -> Result<()> {
        write_sync(tmp, bytes)?;
        rename(tmp, path)?;
        let dir = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("{} has no parent directory", path.display()))?;
        sync_dir(dir)
    }

    /// Remove the source of a completed, persisted rename.
    pub(super) fn remove_file(path: &Path) -> Result<()> {
        step("remove", path)?;
        fs::remove_file(path).with_context(|| format!("remove {}", path.display()))
    }

    /// Persist a completed move across two directories: the destination (which
    /// gained a name) and then the source (which lost one).
    ///
    /// Destination first. The worst a crash between the two syncs can leave is
    /// the file under both names, which the orphan sweeps and the compactor's
    /// consumed proof reconcile; the ordering rules out the one outcome that
    /// needs an operator, a name that survives nowhere.
    pub(super) fn persist_move(dest_dir: &Path, source_dir: &Path) -> Result<()> {
        sync_dir(dest_dir)?;
        sync_dir(source_dir)
    }

    /// `fs::create_dir_all` that leaves each directory it creates durable:
    /// every new component is followed by an fsync of its parent. Components
    /// that already exist cost nothing.
    pub(super) fn create_dir_all(path: &Path) -> Result<()> {
        if path.is_dir() {
            return Ok(());
        }
        match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => create_dir_all(parent)?,
            _ => {}
        }
        step("create_dir", path)?;
        match fs::create_dir(path) {
            Ok(()) => {}
            // Another writer created it between the check and the call; its
            // own sync covers the entry.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
            Err(e) => {
                return Err(anyhow::Error::new(e))
                    .with_context(|| format!("creating {}", path.display()))
            }
        }
        match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => sync_dir(parent),
            _ => Ok(()),
        }
    }

    /// Record the step in the op log and consult the fail point. Both are
    /// test-only; in a normal build this compiles away.
    #[inline]
    fn step(op: &str, path: &Path) -> Result<()> {
        #[cfg(test)]
        {
            probe::step(op, path)?;
        }
        #[cfg(not(test))]
        {
            let _ = (op, path);
        }
        Ok(())
    }

    /// Test-only instrumentation of the durability protocol: the ordered op
    /// log the ordering tests assert on, and the fail point the injected-
    /// failure tests arm. Both are thread-local, so tests running in parallel
    /// in one process do not see each other's.
    #[cfg(test)]
    pub(super) mod probe {
        use std::cell::RefCell;
        use std::path::Path;

        thread_local! {
            static LOG: RefCell<Option<Vec<String>>> = const { RefCell::new(None) };
            static FAIL: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
        }

        /// `"<op> <final path component>"` — how both the log and the armed
        /// failures name a step.
        pub(in crate::durability) fn key(op: &str, path: &Path) -> String {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("<unnamed>");
            format!("{op} {name}")
        }

        pub(in crate::durability) fn step(op: &str, path: &Path) -> anyhow::Result<()> {
            let k = key(op, path);
            LOG.with_borrow_mut(|log| {
                if let Some(entries) = log.as_mut() {
                    entries.push(k.clone());
                }
            });
            if FAIL.with_borrow(|armed| armed.contains(&k)) {
                anyhow::bail!("injected durability failure at `{k}`");
            }
            Ok(())
        }

        /// Start recording, discarding anything recorded before.
        pub(crate) fn record() {
            LOG.with_borrow_mut(|log| *log = Some(Vec::new()));
        }

        /// Take what has been recorded and keep recording.
        pub(crate) fn taken() -> Vec<String> {
            LOG.with_borrow_mut(|log| log.replace(Vec::new()).unwrap_or_default())
        }

        /// Fail the named steps (and only those) until [`disarm`].
        pub(crate) fn fail(steps: &[&str]) {
            FAIL.with_borrow_mut(|armed| {
                *armed = steps.iter().map(|s| s.to_string()).collect();
            });
        }

        pub(crate) fn disarm() {
            FAIL.with_borrow_mut(|armed| armed.clear());
        }
    }
}

impl WalWriter {
    /// Create or reopen a WAL writer at `dir`. Subdirectories `active/`,
    /// `sealed/`, and `processing/` are created if missing.
    pub fn new(dir: impl Into<PathBuf>, ingester_id: impl Into<String>) -> Result<Self> {
        Self::with_thresholds(dir, ingester_id, DEFAULT_MAX_EVENTS, DEFAULT_MAX_AGE)
    }

    pub fn with_thresholds(
        dir: impl Into<PathBuf>,
        ingester_id: impl Into<String>,
        max_events: usize,
        max_age: Duration,
    ) -> Result<Self> {
        let dir = dir.into();
        // Durable creation: the tenant/index directories on the way down are
        // new the first time a (tenant, index) pair takes traffic, and rows
        // acknowledged into a directory whose own entry has not been synced
        // are no more durable than that entry.
        for sub in [ACTIVE_DIR, SEALED_DIR, PROCESSING_DIR, COMMITTED_DIR] {
            durability::create_dir_all(&dir.join(sub))
                .with_context(|| format!("creating {}/{}", dir.display(), sub))?;
        }
        // Phase 4.13d: recover any `.arrow.partial` files left behind
        // by a previous WalWriter that exited without sealing — e.g.
        // a SIGKILL on pod rollover.  Arrow `StreamReader` is
        // tolerant of missing EOS, so a partial-but-flushed segment
        // is fully readable; we just promote it to sealed/ so the
        // compactor picks it up on the next cycle.
        let owner = ingester_id.into();
        let recovered = recover_orphaned_partials(&dir, &owner)?;
        if recovered > 0 {
            tracing::info!(recovered, dir = %dir.display(), "WAL: recovered orphaned partial segments");
            metrics::counter!("siglake_wal_partials_recovered_total").increment(recovered as u64);
        }
        Ok(Self {
            dir,
            ingester_id: owner,
            schema: events_schema(),
            max_events,
            max_age,
            current: None,
            mirror: None,
            mirror_subdir: None,
            table_uuid: None,
        })
    }

    /// Bind this writer to the Iceberg table its rows are destined for, so
    /// every segment it opens from here on carries that identity in its header.
    ///
    /// Returns the segment sealed by the rebind, if any. A rebind SEALS the
    /// open segment first: its rows were accepted for the previous table and
    /// must not be re-attributed by a later header. `Ok(None)` when the
    /// identity is unchanged, which is every call but the first and the one
    /// that follows a `DELETE`+`POST` of the index.
    pub fn bind_table_uuid(&mut self, uuid: Option<Uuid>) -> Result<Option<WalSegment>> {
        if self.table_uuid == uuid {
            return Ok(None);
        }
        let sealed = self.seal()?;
        tracing::info!(
            dir = %self.dir.display(),
            previous = ?self.table_uuid.map(|u| u.to_string()),
            table = ?uuid.map(|u| u.to_string()),
            "WAL writer rebound to a new table identity"
        );
        metrics::counter!("siglake_wal_writer_rebinds_total").increment(1);
        self.table_uuid = uuid;
        Ok(sealed)
    }

    /// The table identity currently bound, if any.
    pub fn table_uuid(&self) -> Option<Uuid> {
        self.table_uuid
    }

    /// Attach (or remove) a mirror handle. When set, every successful
    /// seal enqueues the resulting [`WalSegment`] onto the mirror's
    /// channel for background upload to an object store.
    pub fn set_mirror_handle(&mut self, handle: Option<mirror::WalMirrorHandle>) {
        self.mirror = handle;
    }

    /// Optional tenant subdir for object-store mirror uploads.
    ///
    /// Legacy single-tenant writers keep the historical flat layout
    /// (`<prefix>/<filename>`). Per-tenant writers set this to the
    /// tenant id so catalog-claim deployments can recover the tenant
    /// namespace from the mirrored object key.
    /// Whether a mirror is attached, i.e. whether a REMOTE drain may be the one
    /// that commits this writer's segments.
    ///
    /// It decides whether local `sealed/`/`processing/` file movement is a valid
    /// proxy for "committed": with a mirror, a remote drain commits out of
    /// object storage and nothing ever moves the local file.
    pub fn has_mirror(&self) -> bool {
        self.mirror.is_some()
    }

    /// The mirror subdirectory this writer stamps onto its keys —
    /// `<tenant>` or `<tenant>/<index>`. `None` for the legacy flat layout.
    pub fn mirror_subdir(&self) -> Option<&str> {
        self.mirror_subdir.as_deref()
    }

    pub fn set_mirror_subdir(&mut self, subdir: Option<String>) {
        self.mirror_subdir = subdir.map(|s| s.trim_matches('/').to_string());
    }

    /// Append events, auto-rolling the segment if either threshold is hit.
    /// Returns the sealed segment if a roll happened.
    pub fn append_events(&mut self, events: &[Event]) -> Result<Option<WalSegment>> {
        if events.is_empty() {
            return Ok(None);
        }
        metrics::histogram!("siglake_wal_append_events_rows").record(events.len() as f64);
        let build_start = Instant::now();
        let batch = events_to_record_batch(events)?;
        metrics::histogram!("siglake_wal_record_batch_build_duration_seconds")
            .record(build_start.elapsed().as_secs_f64());
        self.append_batch(&batch)
    }

    /// Append a [`RecordBatch`], auto-rolling on threshold.
    ///
    /// **Durability**: every successful `append_batch` flushes the
    /// `BufWriter`'s internal buffer to the OS (Phase 4.13d). Without
    /// the flush, accepted-but-not-yet-sealed batches sit in memory
    /// until either the buffer fills (~8 KB default) or `seal()`
    /// runs — which means a SIGKILL or pod rollover between seals
    /// drops events the ingest handler already 200'd to the client.
    /// The trade-off is one extra `write(2)` per batch boundary;
    /// in practice that's amortized over the typical multi-event
    /// batch the BackpressureRouter feeds in. fsync still only
    /// happens at seal time — flush gets the bytes to the kernel
    /// page cache, which survives container restart on the same
    /// node-attached PV.
    pub fn append_batch(&mut self, batch: &RecordBatch) -> Result<Option<WalSegment>> {
        use std::io::Write;
        metrics::histogram!("siglake_wal_append_batch_rows").record(batch.num_rows() as f64);
        let total_start = Instant::now();
        let result = (|| -> Result<Option<WalSegment>> {
            if self.current.is_none() {
                self.start_segment()?;
            }
            let active = self.current.as_mut().expect("just-started segment");
            let write_start = Instant::now();
            active.writer.write(batch).context("StreamWriter::write")?;
            metrics::histogram!("siglake_wal_append_write_duration_seconds")
                .record(write_start.elapsed().as_secs_f64());
            active.rows += batch.num_rows();
            // WS-8: fold the batch's event-time range into the segment's, for
            // the framed header's min/max so a reader can prune by time.
            if let Some((mn, mx)) = batch_ts_min_max(batch) {
                active.min_ts = active.min_ts.min(mn);
                active.max_ts = active.max_ts.max(mx);
            }
            // Push BufWriter contents to the OS. See doc-comment above
            // for why this is non-negotiable for durability in a
            // SIGTERM-aware deployment.
            let flush_start = Instant::now();
            active
                .writer
                .get_mut()
                .flush()
                .context("BufWriter::flush after append")?;
            metrics::histogram!("siglake_wal_append_flush_duration_seconds")
                .record(flush_start.elapsed().as_secs_f64());

            let should_seal =
                active.rows >= self.max_events || active.started_at.elapsed() >= self.max_age;
            if should_seal {
                self.seal()
            } else {
                Ok(None)
            }
        })();
        metrics::histogram!("siglake_wal_append_duration_seconds")
            .record(total_start.elapsed().as_secs_f64());
        metrics::counter!(
            "siglake_wal_appends_total",
            "outcome" => if result.is_ok() { "ok" } else { "error" }
        )
        .increment(1);
        result
    }

    /// Push the active segment's buffered bytes to disk and return
    /// the on-disk path plus its current byte count. Used by the
    /// active-segment mirror loop to snapshot the in-flight stream
    /// without sealing it.
    ///
    /// Returns `Ok(None)` when there's no active segment or it's
    /// rows-empty. The resulting on-disk file is a valid (partial)
    /// Arrow IPC stream — it just hasn't received the EOS marker, so
    /// readers should be tolerant of EOF.
    pub fn flush_active_for_mirror(&mut self) -> Result<Option<(PathBuf, u64)>> {
        use std::io::Write;
        let Some(active) = self.current.as_mut() else {
            return Ok(None);
        };
        if active.rows == 0 {
            return Ok(None);
        }
        // Push the BufWriter's internal buffer to the OS file. We
        // don't fsync — the mirror is best-effort and an extra
        // syscall per tick isn't worth the latency.
        active
            .writer
            .get_mut()
            .flush()
            .context("BufWriter::flush for active-mirror")?;
        let bytes = fs::metadata(&active.active_path)
            .with_context(|| format!("stat {}", active.active_path.display()))?
            .len();
        Ok(Some((active.active_path.clone(), bytes)))
    }

    /// Flush and fsync the active partial segment without sealing it.
    ///
    /// `append_batch` already flushes accepted bytes into the kernel page cache;
    /// `sync_active` is the explicit durability step behind ingest
    /// `commit=wait_for` when the append does not already seal the segment.
    ///
    /// It covers the segment's directory entry as well as its contents: the
    /// first successful sync of a segment is followed by an fsync of
    /// `active/`, without which a power loss could return a volume where the
    /// synced bytes have no name (#3048). The entry does not change again
    /// until the seal renames it, so later syncs of the same segment are the
    /// file sync alone. Either sync failing fails the acknowledgement, and
    /// the next call retries whichever half did not complete.
    pub fn sync_active(&mut self) -> Result<bool> {
        use std::io::Write;
        let Some(active) = self.current.as_mut() else {
            return Ok(false);
        };
        if active.rows == 0 {
            return Ok(false);
        }
        let buf = active.writer.get_mut();
        buf.flush().context("BufWriter::flush for sync_active")?;
        durability::sync_file(&buf.get_ref().inner, &active.active_path)
            .context("File::sync_all for sync_active")?;
        if !active.dir_synced {
            let parent = active
                .active_path
                .parent()
                .ok_or_else(|| anyhow!("active segment has no parent directory"))?;
            durability::sync_dir(parent).context("fsync active/ for sync_active")?;
            active.dir_synced = true;
        }
        Ok(true)
    }

    /// Time-based tick. Call from a periodic timer (e.g. tokio::time::interval)
    /// to age-roll idle-but-non-empty segments.
    pub fn tick(&mut self) -> Result<Option<WalSegment>> {
        let should_seal = matches!(self.current.as_ref(),
            Some(a) if a.rows > 0 && a.started_at.elapsed() >= self.max_age);
        if should_seal {
            self.seal()
        } else {
            Ok(None)
        }
    }

    /// Force-seal the current segment if any. Empty active segments are
    /// silently discarded (no sealed file is produced).
    pub fn seal(&mut self) -> Result<Option<WalSegment>> {
        let seal_start = Instant::now();
        let result = (|| -> Result<Option<WalSegment>> {
            let active = match self.current.take() {
                Some(a) => a,
                None => return Ok(None),
            };
            if active.rows == 0 {
                let _ = fs::remove_file(&active.active_path);
                return Ok(None);
            }

            let mut writer = active.writer;
            let finish_start = Instant::now();
            writer.finish().context("StreamWriter::finish")?;
            metrics::histogram!("siglake_wal_seal_finish_duration_seconds")
                .record(finish_start.elapsed().as_secs_f64());
            let buf_writer = writer.into_inner().context("StreamWriter::into_inner")?;
            let crc_writer = buf_writer
                .into_inner()
                .map_err(|e| anyhow!("BufWriter::into_inner: {e}"))?;
            // WS-8: finalize the CRC computed over the raw-IPC bytes on disk.
            let (body_crc, file) = crc_writer.into_parts();
            let sync_start = Instant::now();
            durability::sync_file(&file, &active.active_path).context("File::sync_all")?;
            metrics::histogram!("siglake_wal_seal_sync_duration_seconds")
                .record(sync_start.elapsed().as_secs_f64());
            drop(file);

            // WS-8 framed segment: read the just-written raw-IPC body back (it is
            // hot in the page cache) and write a framed final file — a 36-byte
            // header (magic/version/flags/min-ts/max-ts/len/CRC) + body. The
            // header makes the segment self-describing (in-band integrity +
            // time-range pruning), superseding the `.crc` sidecar. Crash-safe:
            // write a `.tmp` sibling, fsync it, atomically rename into place,
            // fsync `sealed/` so the final pathname itself is durable, and only
            // THEN drop the active partial. Unlinking first would leave a
            // window where a power loss loses both names (#3048).
            let frame_start = Instant::now();
            let raw = fs::read(&active.active_path)
                .with_context(|| format!("re-read active body {}", active.active_path.display()))?;
            // Strip the PARTIAL frame header `start_segment` laid down; what
            // follows it is the IPC body the CRC adapter measured.
            let raw_body = raw.get(WAL_FRAME_HEADER_LEN..).ok_or_else(|| {
                anyhow!(
                    "active segment {} is header-only",
                    active.active_path.display()
                )
            })?;
            debug_assert_eq!(
                crc32(raw_body),
                body_crc,
                "active WAL body CRC drifted on reframe"
            );
            let (min_ts, max_ts) = if active.min_ts <= active.max_ts {
                (active.min_ts, active.max_ts)
            } else {
                (0, 0)
            };
            // zstd-1 the body; the header CRC covers the compressed bytes on disk.
            let comp =
                zstd::encode_all(raw_body, WAL_ZSTD_LEVEL).context("zstd-compress WAL body")?;
            metrics::histogram!("siglake_wal_seal_compression_ratio")
                .record(raw_body.len() as f64 / comp.len().max(1) as f64);
            let framed = build_wal_frame(
                WAL_FRAME_FLAG_ZSTD,
                min_ts,
                max_ts,
                active.owner,
                &comp,
                crc32(&comp),
            );
            let bytes = framed.len() as u64;
            let mut tmp_os = active.final_path.clone().into_os_string();
            tmp_os.push(".tmp");
            let tmp_path = PathBuf::from(tmp_os);
            {
                let mut tmp = File::create(&tmp_path)
                    .with_context(|| format!("create framed tmp {}", tmp_path.display()))?;
                std::io::Write::write_all(&mut tmp, &framed)
                    .with_context(|| format!("write framed segment {}", tmp_path.display()))?;
                durability::sync_file(&tmp, &tmp_path).context("sync framed segment")?;
            }
            durability::rename(&tmp_path, &active.final_path)?;
            let sealed_dir = active
                .final_path
                .parent()
                .ok_or_else(|| anyhow!("sealed WAL segment has no parent directory"))?;
            // Fails the seal if it fails. The active partial is still in place
            // at this point, so the rows remain recoverable either way: the
            // next `recover_orphaned_partials` finds the sealed name if the
            // rename survived, and the partial if it did not.
            durability::sync_dir(sealed_dir).context("fsync sealed/ after seal rename")?;
            // The sealed copy is durable now; the active name is redundant. A
            // failed unlink is not worth failing the seal over — recovery
            // prefers the sealed file and drops a leftover partial.
            if let Err(e) = durability::remove_file(&active.active_path) {
                tracing::warn!(
                    error = %e,
                    path = %active.active_path.display(),
                    "WAL: sealed segment persisted but its active copy could not be removed"
                );
            }
            metrics::histogram!("siglake_wal_seal_rename_duration_seconds")
                .record(frame_start.elapsed().as_secs_f64());

            tracing::debug!(
                path = %active.final_path.display(),
                rows = active.rows,
                bytes,
                "WAL segment sealed"
            );
            metrics::counter!("siglake_wal_segments_sealed_total").increment(1);
            metrics::counter!("siglake_wal_bytes_written_total").increment(bytes);
            metrics::counter!("siglake_wal_rows_written_total").increment(active.rows as u64);
            metrics::histogram!("siglake_wal_seal_rows").record(active.rows as f64);
            metrics::histogram!("siglake_wal_seal_bytes").record(bytes as f64);

            let filename = active
                .final_path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| anyhow!("sealed WAL segment has no filename"))?
                .to_string();
            let mirror_key_suffix = match self.mirror_subdir.as_deref() {
                Some(subdir) if !subdir.is_empty() => format!("{subdir}/{filename}"),
                _ => filename,
            };
            let segment = WalSegment {
                path: active.final_path,
                rows: active.rows,
                bytes,
                mirror_key_suffix,
            };
            if let Some(handle) = &self.mirror {
                handle.enqueue(segment.clone());
            }
            Ok(Some(segment))
        })();
        metrics::histogram!("siglake_wal_seal_duration_seconds")
            .record(seal_start.elapsed().as_secs_f64());
        metrics::counter!(
            "siglake_wal_seals_total",
            "outcome" => if result.is_ok() { "ok" } else { "error" }
        )
        .increment(1);
        result
    }

    fn start_segment(&mut self) -> Result<()> {
        let id = format!("{}-{}", self.ingester_id, Uuid::now_v7());
        let active_path = self
            .dir
            .join(ACTIVE_DIR)
            .join(format!("{id}.arrow.partial"));
        let final_path = self.dir.join(SEALED_DIR).join(format!("{id}.arrow"));
        let mut file = File::create(&active_path)
            .with_context(|| format!("creating {}", active_path.display()))?;
        // #2693: the identity goes down BEFORE the first row, in a PARTIAL
        // frame header, so a segment recovered straight out of `active/` is as
        // self-describing as one that was sealed. The header is written to the
        // raw file, outside the CRC adapter, so the running CRC still covers
        // exactly the IPC body the seal re-reads.
        let owner = self.table_uuid;
        std::io::Write::write_all(
            &mut file,
            &build_wal_frame(WAL_FRAME_FLAG_PARTIAL, 0, 0, owner, &[], 0),
        )
        .with_context(|| format!("writing active frame header {}", active_path.display()))?;
        // CRC32 the IPC bytes as they stream to disk (WS-8 integrity).
        let buf = BufWriter::new(CrcWriter::new(file));
        let writer = StreamWriter::try_new(buf, &self.schema).context("StreamWriter::try_new")?;
        self.current = Some(ActiveSegment {
            active_path,
            final_path,
            writer,
            rows: 0,
            started_at: Instant::now(),
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            owner,
            // The name exists in `active/` but the entry is not durable yet.
            // `sync_active` covers it on the first `commit=wait_for` ack;
            // `commit=auto` never asks for it and does not pay for it.
            dir_synced: false,
        });
        Ok(())
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn current_rows(&self) -> usize {
        self.current.as_ref().map(|a| a.rows).unwrap_or(0)
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        if let Err(e) = self.seal() {
            tracing::warn!(error = %e, "failed to seal WAL segment on drop");
        }
    }
}

/// `std::fs::create_dir_all` that leaves every directory it creates durable:
/// each new component is followed by an fsync of the parent that names it.
///
/// Public because a WAL tree is not always created by [`WalWriter::new`] —
/// the ingester creates the tenant and index directories ahead of the writer,
/// plus a per-index tenant discovery directory no writer ever opens. Whoever
/// creates a directory FIRST is the one that can make it durable: a later
/// durable creation of a path that already exists is a no-op, and rows
/// acknowledged under a directory whose own entry never reached the device
/// go with it in a power loss.
pub fn create_wal_dir(path: &Path) -> Result<()> {
    durability::create_dir_all(path)
}

/// List sealed segments under `dir`, lexicographically sorted (which, for
/// uuidv7-suffixed names, is also chronological).
pub fn list_sealed(dir: &Path) -> Result<Vec<PathBuf>> {
    list_segments(dir, SEALED_DIR)
}

/// List quarantined segments under `<dir>/orphans/`, sorted. Empty when the
/// directory is missing (never quarantined) — the common case.
pub fn list_orphaned(dir: &Path) -> Result<Vec<PathBuf>> {
    list_segments(dir, ORPHANS_DIR)
}

/// List locally-committed segments under `<dir>/committed/`, sorted. These are
/// the segments whose Iceberg append returned, still held for secondary
/// consumers and for the mirror-reclamation mark (#4913).
pub fn list_committed(dir: &Path) -> Result<Vec<PathBuf>> {
    list_segments(dir, COMMITTED_DIR)
}

/// What a WAL directory's [`OWNER_FILE`] says about `expected`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalOwner {
    /// No marker: the directory predates the marker, or no drain has visited
    /// it yet. Treated as "no opinion" everywhere — readers serve it.
    Unmarked,
    /// The marker names `expected`.
    Owned,
    /// The marker names a DIFFERENT table; the payload is that table's UUID.
    /// The segments here belong to an incarnation this name no longer
    /// resolves to.
    Stale(String),
}

/// Read a WAL directory's owner marker. `None` when it is absent, empty, or
/// unreadable — an unreadable marker is indistinguishable from an absent one
/// for every decision made on it, and failing a read over it would take a
/// query or a drain down for a file that carries no rows.
pub fn read_wal_owner(dir: &Path) -> Option<String> {
    let raw = fs::read_to_string(dir.join(OWNER_FILE)).ok()?;
    let owner = raw.trim();
    (!owner.is_empty()).then(|| owner.to_string())
}

/// Compare a WAL directory's owner marker against the table `expected`.
pub fn classify_wal_owner(dir: &Path, expected: &str) -> WalOwner {
    match read_wal_owner(dir) {
        None => WalOwner::Unmarked,
        Some(owner) if owner == expected => WalOwner::Owned,
        Some(owner) => WalOwner::Stale(owner),
    }
}

/// Write `owner` into `<dir>/owner`, replacing whatever was there.
///
/// Write-to-temp + rename, so a reader racing this sees either the old UUID or
/// the new one and never a half-written line. `dir` must already exist.
///
/// Durable when it returns (#3149): the temp file is fsynced before the rename
/// and `dir` after it. A stamp that did not reach the device reads back as an
/// unmarked directory, and an unmarked directory is the one case the drain
/// treats as "no opinion" — it would adopt residents of a dropped incarnation
/// (#2661/#2835) instead of quarantining them.
pub fn stamp_wal_owner(dir: &Path, owner: &str) -> Result<()> {
    let tmp = dir.join(format!("{OWNER_FILE}.{}.tmp", std::process::id()));
    durability::publish_file(&dir.join(OWNER_FILE), &tmp, format!("{owner}\n").as_bytes())
        .with_context(|| format!("stamping owner {owner} on {}", dir.display()))
}

/// What a [`quarantine_stale_wal_dir`] sweep did with a directory's residents.
///
/// #2857: both halves are worth reporting. A recreation where three segments
/// moved and four stayed is a different situation from one where three moved
/// and the directory is now empty, and the moved count alone cannot tell them
/// apart.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalRestamp {
    /// Segments moved under `stale/<previous-owner>/`. `.arrow` files only: an
    /// `active/` `.partial` moves with them but is not a sealed segment, and a
    /// `.crc` sidecar is not a segment at all.
    pub quarantined: usize,
    /// Residents left where they are because their own frame header names
    /// `owner`. Counted once each, sidecars excluded, and — unlike
    /// `quarantined` — an open `.partial` counts: it is a segment the
    /// replacement's writer is still appending to.
    pub kept: usize,
}

/// Move the segments under `dir` that do NOT belong to `owner` into
/// `stale/<previous-owner>/` and re-stamp the directory for `owner`.
///
/// Covers `active/` as well as the three lifecycle dirs: a writer that still
/// holds the dropped incarnation open keeps appending to its partial, and
/// leaving that behind would let it seal into the replacement's `sealed/` on
/// the next roll. Anything the stale writer creates AFTER this point is
/// attributed to the replacement — the WAL carries no per-row incarnation, so
/// that residue is bounded by how long a stale writer lives, not eliminated.
///
/// # Which residents survive the re-stamp
///
/// #2835: the directory marker names one incarnation, but the files under it
/// need not all be that incarnation's. A writer that re-resolved the index
/// between the recreation and this drain is bound to `owner` and has already
/// acknowledged rows into `active/` and `sealed/` here. Those segments say so
/// in their own frame header ([`segment_owner`]), and moving them under
/// `stale/<dropped-uuid>/` would strand acknowledged data that nothing else
/// holds a copy of — no concurrent drain required.
///
/// So the verdict is per segment, and it is the DIRECTORY's rule, not
/// [`classify_segment_owner`]'s: only a segment whose header names `owner`
/// stays. An unstamped resident is quarantined, because the only thing that
/// speaks for it is the marker this call is displacing — the same reading the
/// mirror's re-stamped prefix gives an object with no UUID (#2729). A `.crc`
/// sidecar carries no header of its own and follows its segment either way.
///
/// Idempotent: a segment whose name is already quarantined under the same
/// previous owner is dropped rather than moved, so a retried cycle converges.
pub fn quarantine_stale_wal_dir(dir: &Path, owner: &str) -> Result<WalRestamp> {
    let previous = read_wal_owner(dir).unwrap_or_else(|| "unknown".to_string());
    let quarantine = dir.join(STALE_DIR).join(&previous);
    let mut outcome = WalRestamp::default();
    for sub in [
        ACTIVE_DIR,
        SEALED_DIR,
        PROCESSING_DIR,
        COMMITTED_DIR,
        ORPHANS_DIR,
    ] {
        let from = dir.join(sub);
        if !from.is_dir() {
            continue;
        }
        let mut residents = Vec::new();
        for entry in fs::read_dir(&from).with_context(|| format!("read_dir {}", from.display()))? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                residents.push(entry.path());
            }
        }
        // Read the headers first, so a sidecar's verdict is available whatever
        // order the directory listing put the pair in.
        let kept: std::collections::HashSet<std::ffi::OsString> = residents
            .iter()
            .filter(|p| segment_owner(p).is_some_and(|u| u.to_string() == owner))
            .filter_map(|p| p.file_name().map(|n| n.to_owned()))
            // A sidecar has no verdict of its own, so it never earns a place
            // here — and would double-count its segment if it did.
            .filter(|name| sidecar_segment_name(name) == *name)
            .collect();
        outcome.kept += kept.len();
        let mut moved_any = false;
        for path in &residents {
            let Some(name) = path.file_name() else {
                continue;
            };
            if kept.contains(sidecar_segment_name(name).as_os_str()) {
                continue;
            }
            durability::create_dir_all(&quarantine)
                .with_context(|| format!("creating {}", quarantine.display()))?;
            let dest = quarantine.join(name);
            if dest.exists() {
                fs::remove_file(path)
                    .with_context(|| format!("dropping re-quarantined {}", path.display()))?;
                moved_any = true;
                continue;
            }
            durability::rename(path, &dest)
                .with_context(|| format!("quarantine {} -> {}", path.display(), dest.display()))?;
            moved_any = true;
            if path.extension().and_then(|e| e.to_str()) == Some("arrow") {
                outcome.quarantined += 1;
            }
        }
        // Per source directory, once, covering every resident it lost and the
        // names they arrived under — a single fsync persists all of a
        // directory's pending entry changes, sidecars and partials included.
        if moved_any {
            durability::persist_move(&quarantine, &from)
                .with_context(|| format!("persisting quarantine of {}", from.display()))?;
        }
    }
    // Last, and durable: the marker is what says the quarantine happened, so a
    // reader that finds the new owner finds the moves too.
    stamp_wal_owner(dir, owner)?;
    Ok(outcome)
}

/// The segment whose fate a resident file shares: itself, or — for a
/// `<segment>.crc` sidecar — the segment it checks.
fn sidecar_segment_name(name: &std::ffi::OsStr) -> std::ffi::OsString {
    match name
        .to_str()
        .and_then(|n| n.strip_suffix(CRC_SIDECAR_EXT))
        .and_then(|n| n.strip_suffix('.'))
    {
        Some(segment) => std::ffi::OsString::from(segment),
        None => name.to_owned(),
    }
}

/// Quarantine every segment in `<dir>/processing/` to
/// `<dir>/orphans/`. Returns the number quarantined.
///
/// The compactor's `claim → commit → finish` lifecycle moves
/// files through `processing/` while a commit is in flight. If
/// the compactor pod is killed mid-cycle, those files are left
/// stranded — and the next pod can't tell whether
/// `commit_claimed` had already succeeded (events in Iceberg →
/// re-committing duplicates) or failed (events NOT in Iceberg
/// → re-committing recovers them). Quarantining lets ops
/// inspect each segment + decide.
///
/// Called by `Compactor::new` at startup, before any cycle runs.
/// In the single-replica compactor case (chart-default), anything
/// in `processing/` at startup is by definition from a dead
/// prior process. For multi-replica catalog-claim deployments
/// the catalog tracks claims directly + this scan is a no-op
/// (the catalog-claim path never uses the `processing/` dir).
pub fn recover_orphaned_processing(dir: &Path) -> Result<usize> {
    let processing = dir.join(PROCESSING_DIR);
    if !processing.exists() {
        return Ok(0);
    }
    let orphans = dir.join(ORPHANS_DIR);
    durability::create_dir_all(&orphans)
        .with_context(|| format!("creating {}", orphans.display()))?;
    let mut moved = 0usize;
    for entry in
        fs::read_dir(&processing).with_context(|| format!("read_dir {}", processing.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name() else {
            continue;
        };
        let dest = orphans.join(name);
        if dest.exists() {
            // Already quarantined on a prior recovery; remove the
            // duplicate to keep the directory tidy.
            let _ = fs::remove_file(&path);
            continue;
        }
        durability::rename(&path, &dest)
            .with_context(|| format!("quarantine {} -> {}", path.display(), dest.display()))?;
        moved += 1;
    }
    // #3149: the caller is a starting compactor deciding what is left to do,
    // and it reports this count as recovery having happened. Both directories
    // are synced before it can, so a power loss cannot put a segment back in
    // `processing/` after the operator was told it was quarantined.
    if moved > 0 {
        durability::persist_move(&orphans, &processing)
            .with_context(|| format!("persisting orphan recovery under {}", dir.display()))?;
    }
    Ok(moved)
}

/// How long another writer's `.partial` must sit untouched before this writer
/// will adopt it. Override with `SIGLAKE_WAL_ADOPT_PARTIAL_AFTER_SECS`.
///
/// A LIVE writer cannot leave a non-empty partial idle: `tick` age-rolls any
/// non-empty active segment older than `max_age` (5s by default) and the
/// ingester drives it every 500ms, so a live partial's mtime is at most a few
/// seconds old. 120s is that bound with ~20x margin — long enough that a
/// paused or heavily-loaded pod is not mistaken for a dead one, short enough
/// that a genuinely dead pod's rows are recovered promptly.
pub const DEFAULT_ADOPT_PARTIAL_AFTER: Duration = Duration::from_secs(120);

fn adopt_partial_after() -> Duration {
    std::env::var("SIGLAKE_WAL_ADOPT_PARTIAL_AFTER_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_ADOPT_PARTIAL_AFTER)
}

/// Promote `*.arrow.partial` files in `<dir>/active/` to `*.arrow` in
/// `<dir>/sealed/`. Used at `WalWriter` construction time to recover from a
/// prior process that exited without sealing — e.g. a SIGKILL on pod rollover.
///
/// Returns the number of partials promoted. Empty active dir → `Ok(0)`. Files
/// with 0 bytes are deleted rather than promoted once they are eligible for
/// recovery (no rows to recover).
///
/// # Which partials belong to whom
///
/// This used to promote EVERY partial it found, on a stated single-replica
/// assumption that the shipped configuration contradicts: writers are created
/// lazily per (tenant, index) on a SHARED WAL root, every ingester replica
/// mounts the same RWX PVC, and only the FILENAME is pod-scoped — the
/// directory is not. So when pod B first saw traffic for a tenant pod A was
/// already serving (a scale-up, a KEDA 1->10, a maxSurge rolling update), B
/// renamed A's OPEN partial into `sealed/`. A kept appending to the same
/// inode, so the file GREW while the compactor listed, claimed and committed
/// it; A then sealed its own full row set over the same name. If the promoted
/// copy had already been claimed, the overlapping prefix committed twice —
/// silent duplicate rows, plus torn reads of a concurrently-appended file.
///
/// Ownership is decided by two tests, and the ambiguous case is HELD rather
/// than guessed (the same discipline `dispose_orphans_at` applies to
/// filesystem orphans):
///
/// - the segment name is `<ingester_id>-<uuid7>`, so a partial prefixed with
///   THIS writer's id is unambiguously ours — a previous incarnation of this
///   same pod. Recover it immediately.
/// - a partial belonging to someone else is adopted only once it has sat
///   untouched past [`adopt_partial_after`], which a live writer cannot do
///   (see that constant). Until then it is left exactly where it is.
///
/// That covers both directions: a live sibling's segment is never stolen, and
/// a genuinely dead pod's rows are still recovered rather than stranded in
/// `active/` forever — which matters because the ingester is a Deployment, so
/// a replacement pod gets a NEW hostname and would never match by id.
pub fn recover_orphaned_partials(dir: &Path, owner: &str) -> Result<usize> {
    let active = dir.join(ACTIVE_DIR);
    if !active.exists() {
        return Ok(0);
    }
    let sealed = dir.join(SEALED_DIR);
    durability::create_dir_all(&sealed)
        .with_context(|| format!("creating {}", sealed.display()))?;
    let adopt_after = adopt_partial_after();
    let owner_prefix = format!("{owner}-");
    let mut recovered = 0usize;
    let mut left_alone = 0usize;
    for entry in fs::read_dir(&active).with_context(|| format!("read_dir {}", active.display()))? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".arrow.partial") else {
            continue;
        };
        let meta = entry.metadata()?;
        let size = meta.len();
        if !stem.starts_with(&owner_prefix) {
            // Someone else's. Do not mutate it until it cannot possibly be
            // live. Even an empty or header-only path may have a writer
            // holding its inode open before the first append; unlinking it
            // would let that writer fsync acknowledged rows into a nameless
            // file.
            let idle = meta
                .modified()
                .ok()
                .and_then(|m| m.elapsed().ok())
                .unwrap_or(Duration::ZERO);
            if idle < adopt_after {
                left_alone += 1;
                tracing::debug!(
                    segment = stem,
                    idle_secs = idle.as_secs(),
                    adopt_after_secs = adopt_after.as_secs(),
                    "WAL: leaving another writer's active partial alone (may be live)"
                );
                metrics::counter!("siglake_wal_partials_left_to_owner_total").increment(1);
                continue;
            }
            tracing::warn!(
                segment = stem,
                idle_secs = idle.as_secs(),
                owner = owner,
                "WAL: adopting another writer's abandoned partial — its writer has been \
                 silent past the adoption threshold"
            );
            metrics::counter!("siglake_wal_partials_adopted_total").increment(1);
        }
        // Nothing to recover; just remove. Reaching here means the partial is
        // ours or an abandoned foreign partial past the adoption threshold.
        // A framed partial that is header-only (#2693) is the same case: the
        // identity went down before the IPC stream did.
        if size == 0 || size <= WAL_FRAME_HEADER_LEN as u64 && is_framed_file(&path) {
            let _ = fs::remove_file(&path);
            continue;
        }
        let dest = sealed.join(format!("{stem}.arrow"));
        // If a sealed file with this name already exists (a prior
        // recovery + crash before partial-remove), prefer the
        // sealed one — partials are by definition a subset.
        if dest.exists() {
            let _ = fs::remove_file(&path);
            continue;
        }
        durability::rename(&path, &dest)?;
        recovered += 1;
    }
    // The promotions are renames across two directories, so both entries need
    // syncing before the caller may treat the segments as recovered: `sealed/`
    // for the new names, `active/` for the ones that are gone. A failure here
    // fails construction of the writer rather than reporting a recovery that
    // a power loss could undo; nothing is lost, since an unsynced rename that
    // does not survive leaves the partial in place for the next pass.
    if recovered > 0 {
        durability::sync_dir(&sealed).context("fsync sealed/ after partial recovery")?;
        durability::sync_dir(&active).context("fsync active/ after partial recovery")?;
    }
    if left_alone > 0 {
        tracing::info!(
            left_alone,
            dir = %dir.display(),
            "WAL: active partials belonging to other writers were left in place"
        );
    }
    Ok(recovered)
}

#[cfg(test)]
mod partial_recovery_ownership_tests {
    use super::*;
    use std::io::Write;

    fn append_sync_seal_and_read(writer: &mut WalWriter) {
        writer
            .append_events(&[Event::now("after recovery".to_string())])
            .unwrap();
        assert!(writer.sync_active().unwrap());
        let segment = writer.seal().unwrap().expect("writer seals its segment");
        let rows: usize = read_segment(&segment.path)
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        assert_eq!(rows, 1);
    }

    fn attach_open_segment(
        writer: &mut WalWriter,
        file: File,
        active_path: PathBuf,
        final_path: PathBuf,
    ) {
        let buf = BufWriter::new(CrcWriter::new(file));
        let stream = StreamWriter::try_new(buf, &writer.schema).unwrap();
        writer.current = Some(ActiveSegment {
            active_path,
            final_path,
            writer: stream,
            rows: 0,
            started_at: Instant::now(),
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            owner: None,
            dir_synced: false,
        });
    }

    fn write_partial_header(file: &mut File) {
        file.write_all(&build_wal_frame(WAL_FRAME_FLAG_PARTIAL, 0, 0, None, &[], 0))
            .unwrap();
    }

    #[test]
    fn foreign_recovery_preserves_an_open_zero_byte_partial() {
        let tmp = tempfile::tempdir().unwrap();
        let mut original =
            WalWriter::with_thresholds(tmp.path(), "pod-a", 1_000_000, Duration::from_secs(60))
                .unwrap();
        let id = "pod-a-zero-window";
        let active_path = tmp
            .path()
            .join(ACTIVE_DIR)
            .join(format!("{id}.arrow.partial"));
        let final_path = tmp.path().join(SEALED_DIR).join(format!("{id}.arrow"));

        // Hold the inode open at the point after create and before the frame
        // header. This is the window recovery used to unlink.
        let mut file = File::create(&active_path).unwrap();
        let sibling = WalWriter::new(tmp.path(), "pod-b").unwrap();
        assert!(active_path.exists());
        drop(sibling);

        // Resume the original start_segment sequence on the same open file.
        write_partial_header(&mut file);
        attach_open_segment(&mut original, file, active_path, final_path);
        append_sync_seal_and_read(&mut original);
    }

    #[test]
    fn foreign_recovery_preserves_an_open_header_only_partial() {
        let tmp = tempfile::tempdir().unwrap();
        let mut original =
            WalWriter::with_thresholds(tmp.path(), "pod-a", 1_000_000, Duration::from_secs(60))
                .unwrap();
        let id = "pod-a-header-window";
        let active_path = tmp
            .path()
            .join(ACTIVE_DIR)
            .join(format!("{id}.arrow.partial"));
        let final_path = tmp.path().join(SEALED_DIR).join(format!("{id}.arrow"));
        let mut file = File::create(&active_path).unwrap();
        write_partial_header(&mut file);
        assert_eq!(
            fs::metadata(&active_path).unwrap().len(),
            WAL_FRAME_HEADER_LEN as u64
        );

        let sibling = WalWriter::new(tmp.path(), "pod-b").unwrap();
        assert!(active_path.exists());
        drop(sibling);
        attach_open_segment(&mut original, file, active_path, final_path);
        append_sync_seal_and_read(&mut original);
    }
}

/// #3048: the order of the durability protocol, and what each required sync
/// costs when it fails.
///
/// The probe these tests read is thread-local, so a test only sees the steps
/// its own operations took, and an armed failure cannot reach a test running
/// in parallel with it.
#[cfg(test)]
mod directory_durability_tests {
    use super::durability::probe;
    use super::*;

    /// A writer that never rolls on its own, so every seal in these tests is
    /// one the test asked for.
    fn writer_at(dir: &Path, id: &str) -> WalWriter {
        WalWriter::with_thresholds(dir, id, 1_000_000, Duration::from_secs(3600)).unwrap()
    }

    fn one_event() -> Vec<Event> {
        vec![Event::now("durability".to_string())]
    }

    fn file_name(path: &Path) -> String {
        path.file_name().unwrap().to_str().unwrap().to_string()
    }

    /// The sole `active/*.arrow.partial` under `dir`.
    fn sole_partial(dir: &Path) -> PathBuf {
        let mut found: Vec<_> = fs::read_dir(dir.join(ACTIVE_DIR))
            .unwrap()
            .filter_map(|e| {
                let p = e.unwrap().path();
                (p.extension().and_then(|s| s.to_str()) == Some("partial")).then_some(p)
            })
            .collect();
        assert_eq!(found.len(), 1, "exactly one active partial in {dir:?}");
        found.pop().unwrap()
    }

    fn rows_in(path: &Path) -> usize {
        read_segment(path)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    /// Leave the active partial of an fsynced, unsealed segment behind, the
    /// way a SIGKILL does, and return its path.
    fn abandoned_partial(dir: &Path, id: &str) -> PathBuf {
        let mut w = writer_at(dir, id);
        w.append_events(&one_event()).unwrap();
        assert!(w.sync_active().unwrap());
        let partial = w.current.as_ref().unwrap().active_path.clone();
        // `forget`, not `drop`: `Drop` would seal, and what a crash leaves is
        // an unsealed partial.
        std::mem::forget(w);
        partial
    }

    #[test]
    fn creating_a_wal_tree_syncs_every_new_directory_into_its_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let top = file_name(tmp.path());
        let dir = tmp.path().join("wal-root").join("tenant-a").join("index-b");

        probe::record();
        let writer = writer_at(&dir, "pod-a");
        let ops = probe::taken();

        assert_eq!(
            ops,
            vec![
                "create_dir wal-root".to_string(),
                format!("sync_dir {top}"),
                "create_dir tenant-a".to_string(),
                "sync_dir wal-root".to_string(),
                "create_dir index-b".to_string(),
                "sync_dir tenant-a".to_string(),
                "create_dir active".to_string(),
                "sync_dir index-b".to_string(),
                "create_dir sealed".to_string(),
                "sync_dir index-b".to_string(),
                "create_dir processing".to_string(),
                "sync_dir index-b".to_string(),
                "create_dir committed".to_string(),
                "sync_dir index-b".to_string(),
            ],
            "each new directory is followed by an fsync of the parent that names it"
        );

        // Reopening an existing tree is free: nothing is created, so nothing
        // needs syncing.
        drop(writer);
        probe::record();
        let _reopened = writer_at(&dir, "pod-a");
        assert_eq!(probe::taken(), Vec::<String>::new());
    }

    #[test]
    fn an_append_below_the_thresholds_takes_no_sync_at_all() {
        // `commit=auto` acks off `append_batch` alone. It must stay a
        // `write(2)` into the page cache — no file sync, and no directory
        // sync either.
        let tmp = tempfile::tempdir().unwrap();
        let mut w = writer_at(tmp.path(), "pod-a");
        probe::record();
        assert!(w.append_events(&one_event()).unwrap().is_none());
        assert_eq!(probe::taken(), Vec::<String>::new());
    }

    #[test]
    fn wait_for_covers_the_active_directory_entry_once_per_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = writer_at(tmp.path(), "pod-a");
        w.append_events(&one_event()).unwrap();
        let partial = file_name(&w.current.as_ref().unwrap().active_path);

        probe::record();
        assert!(w.sync_active().unwrap());
        assert_eq!(
            probe::taken(),
            vec![
                format!("sync_file {partial}"),
                "sync_dir active".to_string()
            ],
            "the first ack of a segment makes its name durable as well as its bytes"
        );

        w.append_events(&one_event()).unwrap();
        assert!(w.sync_active().unwrap());
        assert_eq!(
            probe::taken(),
            vec![format!("sync_file {partial}")],
            "the entry does not change again until the seal renames it"
        );

        w.seal().unwrap().expect("a non-empty segment seals");
        w.append_events(&one_event()).unwrap();
        let next = file_name(&w.current.as_ref().unwrap().active_path);
        probe::taken();
        assert!(w.sync_active().unwrap());
        assert_eq!(
            probe::taken(),
            vec![format!("sync_file {next}"), "sync_dir active".to_string()],
            "a new segment is a new entry and pays for its own sync"
        );
    }

    #[test]
    fn sealing_persists_the_final_name_before_removing_the_active_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = writer_at(tmp.path(), "pod-a");
        w.append_events(&one_event()).unwrap();
        let partial = file_name(&w.current.as_ref().unwrap().active_path);

        probe::record();
        let segment = w.seal().unwrap().expect("a non-empty segment seals");
        let sealed = file_name(&segment.path);

        assert_eq!(
            probe::taken(),
            vec![
                format!("sync_file {partial}"),
                format!("sync_file {sealed}.tmp"),
                format!("rename {sealed}"),
                "sync_dir sealed".to_string(),
                format!("remove {partial}"),
            ],
            "the sealed pathname is durable before the only other copy goes"
        );
    }

    #[test]
    fn a_failed_sealed_directory_sync_fails_the_seal_and_keeps_the_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = writer_at(tmp.path(), "pod-a");
        w.append_events(&one_event()).unwrap();
        w.append_events(&one_event()).unwrap();

        probe::fail(&["sync_dir sealed"]);
        let err = w.seal().unwrap_err();
        probe::disarm();
        assert!(
            format!("{err:#}").contains("injected durability failure at `sync_dir sealed`"),
            "{err:#}"
        );

        // Both names are still on disk, so nothing acknowledged is stranded:
        // the rename landed, and the active copy was not removed.
        let sealed = list_sealed(tmp.path()).unwrap();
        assert_eq!(sealed.len(), 1);
        assert_eq!(rows_in(&sealed[0]), 2);
        let partial = sole_partial(tmp.path());

        // Recovery converges on the sealed copy and drops the redundant one.
        assert_eq!(recover_orphaned_partials(tmp.path(), "pod-a").unwrap(), 0);
        assert!(!partial.exists());
        assert_eq!(rows_in(&sealed[0]), 2);
    }

    #[test]
    fn a_failed_active_directory_sync_fails_the_ack_and_is_retried() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = writer_at(tmp.path(), "pod-a");
        w.append_events(&one_event()).unwrap();
        let partial = file_name(&w.current.as_ref().unwrap().active_path);

        probe::fail(&["sync_dir active"]);
        let err = w.sync_active().unwrap_err();
        assert!(
            format!("{err:#}").contains("injected durability failure at `sync_dir active`"),
            "{err:#}"
        );
        probe::disarm();

        // The segment is not marked as covered, so the next ack retries the
        // directory it could not sync.
        probe::record();
        assert!(w.sync_active().unwrap());
        assert_eq!(
            probe::taken(),
            vec![
                format!("sync_file {partial}"),
                "sync_dir active".to_string()
            ]
        );

        let segment = w.seal().unwrap().expect("a non-empty segment seals");
        assert_eq!(rows_in(&segment.path), 1);
    }

    #[test]
    fn recovery_persists_the_promoted_names_before_reporting_them() {
        let tmp = tempfile::tempdir().unwrap();
        let partial = abandoned_partial(tmp.path(), "pod-a");
        let promoted = file_name(&partial).replace(".arrow.partial", ".arrow");

        probe::record();
        assert_eq!(recover_orphaned_partials(tmp.path(), "pod-a").unwrap(), 1);
        assert_eq!(
            probe::taken(),
            vec![
                format!("rename {promoted}"),
                "sync_dir sealed".to_string(),
                "sync_dir active".to_string(),
            ],
            "both sides of the cross-directory rename are synced before the count is returned"
        );
        assert_eq!(rows_in(&tmp.path().join(SEALED_DIR).join(&promoted)), 1);
    }

    #[test]
    fn a_failed_recovery_sync_fails_the_writer_it_recovers_for() {
        let tmp = tempfile::tempdir().unwrap();
        let partial = abandoned_partial(tmp.path(), "pod-a");
        let promoted = file_name(&partial).replace(".arrow.partial", ".arrow");

        probe::fail(&["sync_dir sealed"]);
        let err =
            match WalWriter::with_thresholds(tmp.path(), "pod-a", 8, Duration::from_secs(3600)) {
                Ok(_) => panic!("an unsynced promotion is not a completed recovery"),
                Err(e) => e,
            };
        probe::disarm();
        assert!(
            format!("{err:#}").contains("injected durability failure at `sync_dir sealed`"),
            "{err:#}"
        );

        // The rows are under the promoted name, and a retried construction
        // finds nothing left to recover.
        assert_eq!(rows_in(&tmp.path().join(SEALED_DIR).join(&promoted)), 1);
        let _writer = writer_at(tmp.path(), "pod-a");
        assert_eq!(list_sealed(tmp.path()).unwrap().len(), 1);
    }
}

/// #3149: the lifecycle moves that follow an acknowledgement. Each one is a
/// rename between two directories, or a small file published by temp +
/// rename, and each is durable before it reports what it did — asserted on the
/// recorded operation order rather than inferred from the code.
#[cfg(test)]
mod lifecycle_durability_tests {
    use super::durability::probe;
    use super::*;

    fn sealed_segment(dir: &Path) -> PathBuf {
        let mut w =
            WalWriter::with_thresholds(dir, "pod-a", 1_000_000, Duration::from_secs(3600)).unwrap();
        w.append_events(&[Event::now("lifecycle".to_string())])
            .unwrap();
        w.seal().unwrap().expect("a non-empty segment seals").path
    }

    fn name_of(path: &Path) -> String {
        path.file_name().unwrap().to_str().unwrap().to_string()
    }

    fn rows_in(path: &Path) -> usize {
        read_segment(path)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    #[test]
    fn claiming_a_segment_persists_the_destination_then_the_source() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = sealed_segment(tmp.path());
        let name = name_of(&sealed);

        probe::record();
        let claimed = claim_segment(&sealed).unwrap();
        assert_eq!(
            probe::taken(),
            vec![
                format!("rename {name}"),
                "sync_dir processing".to_string(),
                "sync_dir sealed".to_string(),
            ],
            "the claimed name is durable before the sealed one's absence is"
        );
        assert_eq!(rows_in(&claimed), 1);
    }

    #[test]
    fn releasing_and_finishing_persist_both_their_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = sealed_segment(tmp.path());
        let name = name_of(&sealed);

        let claimed = claim_segment(&sealed).unwrap();
        probe::record();
        let released = release_segment(&claimed).unwrap();
        assert_eq!(
            probe::taken(),
            vec![
                format!("rename {name}"),
                "sync_dir sealed".to_string(),
                "sync_dir processing".to_string(),
            ],
            "a released segment is claimable again across a power loss"
        );

        let claimed = claim_segment(&released).unwrap();
        probe::taken();
        let committed = finish_segment(&claimed).unwrap();
        assert_eq!(
            probe::taken(),
            vec![
                format!("rename {name}"),
                "sync_dir committed".to_string(),
                "sync_dir processing".to_string(),
            ],
            "a finished segment does not come back to `processing/`"
        );
        assert_eq!(rows_in(&committed), 1);
    }

    /// #3143: the note is durable before the segment carries its name, and
    /// the set-aside is durable before `processing/` forgets it. Anything
    /// weaker leaves a restart with a segment it cannot explain.
    #[test]
    fn setting_a_segment_aside_publishes_the_note_before_the_move() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = sealed_segment(tmp.path());
        let name = name_of(&sealed);
        let claimed = claim_segment(&sealed).unwrap();
        let before = fs::read(&claimed).unwrap();

        probe::record();
        let dest = quarantine_poison_segment(&claimed, "reading it failed", 3).unwrap();
        let ops = probe::taken();
        let at = |op: &str| {
            ops.iter()
                .position(|o| o == op)
                .unwrap_or_else(|| panic!("{op} missing from {ops:?}"))
        };
        assert!(
            at(&format!("rename {name}.poison.json")) < at(&format!("rename {name}")),
            "a poisoned segment is never without its verdict: {ops:?}"
        );
        assert_eq!(
            ops.last().map(String::as_str),
            Some("sync_dir processing"),
            "the set-aside is durable before the claim is forgotten: {ops:?}"
        );

        assert_eq!(dest, tmp.path().join(POISON_DIR).join(&name));
        assert_eq!(
            fs::read(&dest).unwrap(),
            before,
            "the original bytes are preserved verbatim"
        );
        assert!(list_segments(tmp.path(), PROCESSING_DIR)
            .unwrap()
            .is_empty());
        assert!(list_sealed(tmp.path()).unwrap().is_empty());
        assert_eq!(list_poisoned(tmp.path()).unwrap(), vec![dest.clone()]);

        let note = read_poison_note(&dest).expect("the note is readable");
        assert_eq!(note.segment, name);
        assert_eq!(note.reason, "reading it failed");
        assert_eq!(note.attempts, 3);
        assert!(note.quarantined_at_ms > 0);
    }

    /// The set-aside never clobbers: a second verdict on a name already held
    /// is refused, which leaves the caller holding the segment it started
    /// with rather than silently losing one of the two.
    #[test]
    fn setting_aside_refuses_a_name_already_held() {
        let tmp = tempfile::tempdir().unwrap();
        let first = sealed_segment(tmp.path());
        let name = name_of(&first);
        quarantine_poison_segment(&first, "first", 3).unwrap();

        let impostor = tmp.path().join(SEALED_DIR).join(&name);
        fs::write(&impostor, b"not the same bytes").unwrap();
        let err = quarantine_poison_segment(&impostor, "second", 3).unwrap_err();
        assert!(format!("{err:#}").contains("already set aside"), "{err:#}");
        assert!(impostor.exists(), "the refused segment stays where it was");
        assert_eq!(
            read_poison_note(&tmp.path().join(POISON_DIR).join(&name))
                .unwrap()
                .reason,
            "first"
        );
    }

    /// The deliberate way back: `poison/` → `sealed/`, note dropped, and the
    /// next drain claims it like any other segment.
    #[test]
    fn requeueing_a_poisoned_segment_returns_it_to_sealed() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = sealed_segment(tmp.path());
        let name = name_of(&sealed);
        let held = quarantine_poison_segment(&sealed, "reading it failed", 3).unwrap();

        // A name already back in `sealed/` is a refusal, not a clobber.
        fs::write(tmp.path().join(SEALED_DIR).join(&name), b"live").unwrap();
        let err = requeue_poisoned_segment(&held).unwrap_err();
        assert!(
            format!("{err:#}").contains("not requeueing over it"),
            "{err:#}"
        );
        fs::remove_file(tmp.path().join(SEALED_DIR).join(&name)).unwrap();

        let back = requeue_poisoned_segment(&held).unwrap();
        assert_eq!(back, tmp.path().join(SEALED_DIR).join(&name));
        assert_eq!(rows_in(&back), 1);
        assert!(list_poisoned(tmp.path()).unwrap().is_empty());
        assert!(
            !poison_note_path(&held).exists(),
            "a requeued segment leaves no verdict behind"
        );
        assert_eq!(list_sealed(tmp.path()).unwrap(), vec![back]);
    }

    #[test]
    fn a_claim_that_cannot_be_made_durable_leaves_the_segment_claimable() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = sealed_segment(tmp.path());

        probe::fail(&["sync_dir processing"]);
        let err = claim_segment(&sealed).unwrap_err();
        probe::disarm();
        assert!(
            format!("{err:#}").contains("injected durability failure at `sync_dir processing`"),
            "{err:#}"
        );

        // The caller was told the claim failed, so the segment must be where
        // the next cycle looks: `sealed/`, not stranded in `processing/` for
        // a restart to quarantine.
        assert!(list_segments(tmp.path(), PROCESSING_DIR)
            .unwrap()
            .is_empty());
        let again = list_sealed(tmp.path()).unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(rows_in(&again[0]), 1);
        assert!(claim_segment(&again[0]).unwrap().exists());
    }

    #[test]
    fn stamping_an_owner_syncs_the_marker_and_its_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("tenant-a");
        create_wal_dir(&dir).unwrap();
        let marker = format!("{OWNER_FILE}.{}.tmp", std::process::id());

        probe::record();
        stamp_wal_owner(&dir, "table-uuid-1").unwrap();
        assert_eq!(
            probe::taken(),
            vec![
                format!("write {marker}"),
                format!("sync_file {marker}"),
                format!("rename {OWNER_FILE}"),
                "sync_dir tenant-a".to_string(),
            ],
            "a stamp a power loss can drop reads back as an unmarked directory"
        );
        assert_eq!(read_wal_owner(&dir).as_deref(), Some("table-uuid-1"));
    }

    #[test]
    fn publishing_a_watermark_syncs_it_before_retention_may_act_on_it() {
        let tmp = tempfile::tempdir().unwrap();

        probe::record();
        publish_consumer_watermark(tmp.path(), "detector", "seg-0007.arrow").unwrap();
        assert_eq!(
            probe::taken(),
            vec![
                "create_dir consumers".to_string(),
                format!("sync_dir {}", name_of(tmp.path())),
                "write .detector.tmp".to_string(),
                "sync_file .detector.tmp".to_string(),
                "rename detector".to_string(),
                "sync_dir consumers".to_string(),
            ],
        );
        assert_eq!(
            min_consumer_watermark(tmp.path(), Duration::from_secs(3600)).unwrap(),
            Some("seg-0007.arrow".to_string())
        );
    }

    #[test]
    fn quarantining_one_stale_segment_persists_both_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = sealed_segment(tmp.path());
        let name = name_of(&sealed);

        probe::record();
        let dest = quarantine_stale_segment(&sealed, "dropped-uuid").unwrap();
        assert_eq!(
            probe::taken(),
            vec![
                "create_dir stale".to_string(),
                format!("sync_dir {}", name_of(tmp.path())),
                "create_dir dropped-uuid".to_string(),
                "sync_dir stale".to_string(),
                format!("rename {name}"),
                "sync_dir dropped-uuid".to_string(),
                "sync_dir sealed".to_string(),
            ],
            "the quarantine directory is created durably and the move persisted"
        );
        assert_eq!(rows_in(&dest), 1);
    }

    #[test]
    fn quarantining_a_stale_directory_persists_each_source_it_emptied() {
        let tmp = tempfile::tempdir().unwrap();
        // One sealed resident and one active partial, so two source
        // directories lose a name and each is synced once.
        let sealed = sealed_segment(tmp.path());
        let mut w =
            WalWriter::with_thresholds(tmp.path(), "pod-b", 1_000_000, Duration::from_secs(3600))
                .unwrap();
        w.append_events(&[Event::now("open".to_string())]).unwrap();
        let partial = w.current.as_ref().unwrap().active_path.clone();
        std::mem::forget(w);
        stamp_wal_owner(tmp.path(), "dropped-uuid").unwrap();

        probe::record();
        let outcome = quarantine_stale_wal_dir(tmp.path(), "live-uuid").unwrap();
        let ops = probe::taken();
        assert_eq!(
            outcome,
            WalRestamp {
                quarantined: 1,
                kept: 0
            }
        );

        // Both residents moved, and each source directory was persisted with
        // the quarantine before the new owner marker was published.
        assert_eq!(
            ops.iter().filter(|o| *o == "sync_dir active").count(),
            1,
            "{ops:?}"
        );
        assert_eq!(
            ops.iter().filter(|o| *o == "sync_dir sealed").count(),
            1,
            "{ops:?}"
        );
        let stamp = ops
            .iter()
            .position(|o| o == &format!("rename {OWNER_FILE}"))
            .expect("the marker is published");
        let last_move_sync = ops
            .iter()
            .rposition(|o| o == "sync_dir active" || o == "sync_dir sealed")
            .unwrap();
        assert!(
            last_move_sync < stamp,
            "the owner marker is the last thing to land: {ops:?}"
        );

        let quarantine = tmp.path().join(STALE_DIR).join("dropped-uuid");
        assert!(quarantine.join(name_of(&sealed)).exists());
        assert!(quarantine.join(name_of(&partial)).exists());
    }

    #[test]
    fn recovering_orphaned_processing_persists_before_it_reports() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = sealed_segment(tmp.path());
        let name = name_of(&sealed);
        let claimed = claim_segment(&sealed).unwrap();

        probe::record();
        assert_eq!(recover_orphaned_processing(tmp.path()).unwrap(), 1);
        assert_eq!(
            probe::taken(),
            vec![
                "create_dir orphans".to_string(),
                format!("sync_dir {}", name_of(tmp.path())),
                format!("rename {name}"),
                "sync_dir orphans".to_string(),
                "sync_dir processing".to_string(),
            ],
            "an operator is told about the quarantine only once it is durable"
        );
        assert!(!claimed.exists());
        assert_eq!(rows_in(&tmp.path().join(ORPHANS_DIR).join(&name)), 1);
    }

    #[test]
    fn a_failed_orphan_sync_fails_the_recovery_and_keeps_the_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = sealed_segment(tmp.path());
        let name = name_of(&sealed);
        claim_segment(&sealed).unwrap();

        probe::fail(&["sync_dir orphans"]);
        let err = recover_orphaned_processing(tmp.path()).unwrap_err();
        probe::disarm();
        assert!(
            format!("{err:#}").contains("injected durability failure at `sync_dir orphans`"),
            "{err:#}"
        );

        // The compactor's constructor fails rather than starting a cycle over
        // an unpersisted quarantine; the rows are under the orphan name and a
        // retry converges on it.
        assert_eq!(rows_in(&tmp.path().join(ORPHANS_DIR).join(&name)), 1);
        assert_eq!(recover_orphaned_processing(tmp.path()).unwrap(), 0);
        assert_eq!(list_orphaned(tmp.path()).unwrap().len(), 1);
    }
}

fn list_layout_dirs(root: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(root).with_context(|| format!("read_dir {}", root.display()))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let tenant_dir = entry.path();
        if !tenant_dir.join(SEALED_DIR).is_dir() {
            // Not a tenant root — skip e.g. legacy single-tenant
            // `active/`, `sealed/`, `committed/` siblings.
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        // The legacy single-tenant layout has `sealed/`, `active/`,
        // etc. as direct children of `root`. We don't want to confuse
        // those with a tenant subdir.
        if matches!(
            name.as_str(),
            ACTIVE_DIR | SEALED_DIR | PROCESSING_DIR | COMMITTED_DIR | CONSUMERS_DIR
        ) {
            continue;
        }
        out.push((name, tenant_dir));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// Per-tenant WAL roots beneath `root` — every immediate child
/// subdirectory of `root` that itself contains a `sealed/` directory
/// is treated as a tenant. Returns `[(tenant_name, tenant_dir)]`.
///
/// Used by the per-tenant compactor sweep to enumerate tenants
/// without needing the ingest configuration on hand.
pub fn list_tenant_dirs(root: &Path) -> Result<Vec<(String, PathBuf)>> {
    list_layout_dirs(root)
}

/// Per-index WAL roots beneath one tenant directory. Every immediate child
/// subdirectory of `tenant_dir` that contains a `sealed/` directory is treated
/// as a custom index WAL root. Layout directories such as `active/` and the
/// events-root `sealed/` siblings are excluded.
pub fn list_index_dirs(tenant_dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    list_layout_dirs(tenant_dir)
}

/// List `sealed/`, `processing/`, and `committed/` segments by file-name,
/// returning each segment exactly once (de-duped by filename, with the
/// later-checked location winning if a file exists in both during a
/// compactor rename). Use this from secondary consumers like the
/// detector that race against the compactor: a segment claimed by the
/// compactor but not yet swept from `committed/` is still visible
/// here, so the detector has the full retention window to catch up.
pub fn list_visible(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut by_name: std::collections::BTreeMap<String, PathBuf> =
        std::collections::BTreeMap::new();
    // Order matters: a rename ATOMICALLY moves a dirent, so the file is
    // visible in exactly one location at any instant. We check in the
    // order the compactor walks them (sealed → processing → committed)
    // so the latest location wins when our reads straddle a rename.
    for sub in [SEALED_DIR, PROCESSING_DIR, COMMITTED_DIR] {
        for p in list_segments(dir, sub)? {
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                by_name.insert(name.to_string(), p);
            }
        }
    }
    Ok(by_name.into_values().collect())
}

fn list_segments(dir: &Path, sub: &str) -> Result<Vec<PathBuf>> {
    let target = dir.join(sub);
    if !target.exists() {
        return Ok(vec![]);
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(&target)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("arrow") {
                out.push(p);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Atomically claim a sealed segment by renaming it into `processing/`.
/// Returns the new path on success.
///
/// Durable when it returns (#3149): `processing/` and `sealed/` are both
/// fsynced, so the claim a caller acts on is not one a power loss can hand
/// back to the next drain as an unclaimed sealed segment.
pub fn claim_segment(sealed_path: &Path) -> Result<PathBuf> {
    let dir = sealed_path
        .parent()
        .and_then(|p| p.parent())
        .ok_or_else(|| anyhow!("not under a WAL dir: {}", sealed_path.display()))?;
    let processing = dir.join(PROCESSING_DIR);
    durability::create_dir_all(&processing)?;
    let name = sealed_path
        .file_name()
        .ok_or_else(|| anyhow!("no filename: {}", sealed_path.display()))?;
    let target = processing.join(name);
    durability::rename(sealed_path, &target)
        .with_context(|| format!("claiming {}", sealed_path.display()))?;
    move_sidecar(sealed_path, &target);
    let sealed = sealed_path
        .parent()
        .ok_or_else(|| anyhow!("no parent: {}", sealed_path.display()))?;
    if let Err(e) = durability::persist_move(&processing, sealed) {
        // The rename landed but is not durable, and the caller will not
        // process a claim it was handed an error for. Put the segment back
        // where the next cycle looks for it: the compactor releases the rest
        // of a failed batch, and a segment stranded in `processing/` instead
        // waits for a restart's orphan sweep and an operator's verdict. If
        // even the release fails, that sweep is still the backstop.
        if let Err(back) = release_segment(&target) {
            tracing::warn!(
                path = %target.display(),
                error = %back,
                "WAL: claim was not made durable and could not be released; it will be \
                 quarantined as an orphan on the next compactor start"
            );
        }
        return Err(e).with_context(|| format!("persisting claim of {}", sealed_path.display()));
    }
    Ok(target)
}

/// Move a claimed segment back into `sealed/` so the compactor can retry.
///
/// Both directories are fsynced before it returns: the release is what makes
/// the segment claimable again, and a claim the failed cycle no longer tracks
/// is only picked up from `sealed/`.
pub fn release_segment(processing_path: &Path) -> Result<PathBuf> {
    let dir = processing_path
        .parent()
        .and_then(|p| p.parent())
        .ok_or_else(|| anyhow!("not under a WAL dir: {}", processing_path.display()))?;
    let sealed = dir.join(SEALED_DIR);
    let name = processing_path
        .file_name()
        .ok_or_else(|| anyhow!("no filename: {}", processing_path.display()))?;
    let target = sealed.join(name);
    durability::rename(processing_path, &target)
        .with_context(|| format!("releasing {}", processing_path.display()))?;
    move_sidecar(processing_path, &target);
    let processing = processing_path
        .parent()
        .ok_or_else(|| anyhow!("no parent: {}", processing_path.display()))?;
    durability::persist_move(&sealed, processing)
        .with_context(|| format!("persisting release of {}", processing_path.display()))?;
    Ok(target)
}

/// Move a successfully-committed segment from `processing/` to
/// `committed/`. Replaces the previous "delete immediately" semantics:
/// the file lives in `committed/` until a retention sweep
/// ([`sweep_committed`]) removes it, which gives slow secondary
/// consumers (e.g. the detector) time to catch up.
///
/// Both directories are fsynced before it returns. The Iceberg commit has
/// already happened at this point, so the move is bookkeeping — but it is the
/// bookkeeping a secondary consumer reads (`committed/`) and the one the
/// startup orphan sweep reads (`processing/`), and a power loss that undoes it
/// sends the segment to `orphans/` for an operator to adjudicate.
pub fn finish_segment(processing_path: &Path) -> Result<PathBuf> {
    let dir = processing_path
        .parent()
        .and_then(|p| p.parent())
        .ok_or_else(|| anyhow!("not under a WAL dir: {}", processing_path.display()))?;
    let committed = dir.join(COMMITTED_DIR);
    durability::create_dir_all(&committed)?;
    let name = processing_path
        .file_name()
        .ok_or_else(|| anyhow!("no filename: {}", processing_path.display()))?;
    let target = committed.join(name);
    durability::rename(processing_path, &target)
        .with_context(|| format!("finishing {}", processing_path.display()))?;
    move_sidecar(processing_path, &target);
    let processing = processing_path
        .parent()
        .ok_or_else(|| anyhow!("no parent: {}", processing_path.display()))?;
    durability::persist_move(&committed, processing)
        .with_context(|| format!("persisting finish of {}", processing_path.display()))?;
    Ok(target)
}

/// Publish a secondary consumer's watermark: the name of the last segment
/// `consumer_id` has fully processed. Written atomically (temp + rename) into
/// `<dir>/consumers/<consumer_id>` so a concurrent reader never sees a partial
/// write. `dir` is the (per-tenant) WAL dir whose `committed/` this consumer
/// reads.
///
/// Durable when it returns (#3149): the temp is fsynced before the rename and
/// `consumers/` after it. The watermark is what holds retention back, so a
/// published value a power loss reverts leaves the sweep free to delete
/// `committed/` segments this consumer had not read — the failure mode is a
/// gap in a secondary consumer's input, not a lost row.
pub fn publish_consumer_watermark(dir: &Path, consumer_id: &str, last_segment: &str) -> Result<()> {
    let consumers = dir.join(CONSUMERS_DIR);
    durability::create_dir_all(&consumers)?;
    let tmp = consumers.join(format!(".{consumer_id}.tmp"));
    let final_path = consumers.join(consumer_id);
    durability::publish_file(&final_path, &tmp, last_segment.as_bytes())
        .with_context(|| format!("publish watermark {}", final_path.display()))
}

/// The minimum (lexically lowest) watermark across **fresh** consumers — those
/// whose watermark file was updated within `stale_after`. Returns `None` when
/// there are no fresh consumers (no `consumers/` dir, or all stale), meaning the
/// sweep is unconstrained by consumers. A `stale_after` of zero treats every
/// consumer as stale (pure time-based sweep).
pub fn min_consumer_watermark(dir: &Path, stale_after: Duration) -> Result<Option<String>> {
    let consumers = dir.join(CONSUMERS_DIR);
    if !consumers.exists() {
        return Ok(None);
    }
    let now = std::time::SystemTime::now();
    let mut min: Option<String> = None;
    for entry in fs::read_dir(&consumers)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        // Skip in-flight temp files (`.<id>.tmp`).
        if name.to_string_lossy().starts_with('.') {
            continue;
        }
        let meta = entry.metadata()?;
        let mtime = meta.modified().unwrap_or(now);
        if now.duration_since(mtime).unwrap_or(Duration::ZERO) >= stale_after {
            continue; // stale (or stale_after == 0) — ignore this consumer
        }
        let wm = fs::read_to_string(entry.path())
            .with_context(|| format!("read watermark {}", entry.path().display()))?
            .trim()
            .to_string();
        min = Some(match min {
            Some(cur) if cur <= wm => cur,
            _ => wm,
        });
    }
    Ok(min)
}

/// What one [`sweep_committed_gated`] pass removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CommittedSweep {
    /// Files removed.
    pub deleted: usize,
    /// Of those, the ones removed at the hard ceiling while the mark gate still
    /// refused them: local commit evidence destroyed before remote evidence
    /// exists. Always zero without a gate.
    pub unmarked: usize,
}

/// Delete `committed/` segments whose mtime is older than `retention`,
/// coordinated with secondary consumers: a segment is kept until every fresh
/// consumer has processed past it (its name ≤ the min consumer watermark),
/// unless it is older than `max_retention` (a hard ceiling that protects
/// against a stuck consumer letting `committed/` grow without bound). Returns
/// the count deleted. Idempotent + multi-process safe (ENOENT races ignored).
pub fn sweep_committed_coordinated(
    dir: &Path,
    retention: Duration,
    max_retention: Duration,
    stale_after: Duration,
) -> Result<usize> {
    sweep_committed_gated(dir, retention, max_retention, stale_after, None).map(|s| s.deleted)
}

/// [`sweep_committed_coordinated`] with a second gate: when `marked` is
/// `Some`, a file is removed only once its name is in that set — the names
/// whose remote commit evidence is durable (#4913). Local evidence is
/// therefore destroyed only after remote evidence exists.
///
/// The hard ceiling still overrides the gate, because coupling the local sweep
/// to a remote write means a catalog outage would otherwise grow the WAL volume
/// without bound. A file swept that way is counted in
/// [`CommittedSweep::unmarked`]: its mirror object is a leak this reclaimer can
/// no longer collect, and the caller reports it rather than hiding it.
pub fn sweep_committed_gated(
    dir: &Path,
    retention: Duration,
    max_retention: Duration,
    stale_after: Duration,
    marked: Option<&std::collections::BTreeSet<String>>,
) -> Result<CommittedSweep> {
    let committed = dir.join(COMMITTED_DIR);
    if !committed.exists() {
        return Ok(CommittedSweep::default());
    }
    let watermark = min_consumer_watermark(dir, stale_after)?;
    let now = std::time::SystemTime::now();
    let mut swept = CommittedSweep::default();
    for entry in fs::read_dir(&committed)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let p = entry.path();
        if p.extension().and_then(|e| e.to_str()) != Some("arrow") {
            continue;
        }
        let mtime = entry.metadata().and_then(|m| m.modified()).unwrap_or(now);
        let age = now.duration_since(mtime).unwrap_or(Duration::ZERO);
        let name = match p.file_name().and_then(|s| s.to_str()) {
            Some(n) => n,
            None => continue,
        };
        // Hard ceiling overrides everything; otherwise require past the soft
        // floor AND consumed by every fresh consumer (if any).
        let consumed = match &watermark {
            Some(wm) => name <= wm.as_str(),
            None => true,
        };
        let expired = age >= max_retention;
        if !(expired || (age >= retention && consumed)) {
            continue;
        }
        let gated = match marked {
            Some(set) => !set.contains(name),
            None => false,
        };
        if gated && !expired {
            continue;
        }
        match fs::remove_file(&p) {
            Ok(()) => {
                remove_sidecar(&p);
                swept.deleted += 1;
                if gated {
                    swept.unmarked += 1;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).context("sweep_committed remove"),
        }
    }
    Ok(swept)
}

/// Delete `committed/` segments whose mtime is older than `retention`,
/// **without** consumer coordination (pure time-based). Equivalent to
/// [`sweep_committed_coordinated`] with no fresh consumers.
pub fn sweep_committed(dir: &Path, retention: Duration) -> Result<usize> {
    sweep_committed_coordinated(dir, retention, retention, Duration::ZERO)
}

/// Read all `RecordBatch`es from a sealed segment file.
pub fn read_segment(path: &Path) -> Result<Vec<RecordBatch>> {
    let bytes = fs::read(path).with_context(|| format!("opening {}", path.display()))?;
    if is_framed(&bytes) {
        // WS-8 framed segment: verify the in-band header + body CRC, then decode.
        decode_wal_frame(path, &bytes)
    } else {
        // Legacy raw-IPC segment: validate against the `.crc` sidecar if present.
        validate_segment_crc(path, &bytes)?;
        read_segment_bytes(&bytes)
    }
}

/// Decode a sealed segment from its full file bytes, handling both framed
/// (WS-8) and legacy raw-IPC formats. Framed bytes have their in-band CRC
/// verified; legacy bytes decode directly (no off-disk sidecar to check). Use
/// this for object-store / mirror reads where there is no local path — e.g. the
/// compactor's catalog-claim path pulls segment bytes from object storage.
pub fn read_segment_from_bytes(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    if is_framed(bytes) {
        decode_wal_frame(Path::new("<object-store segment>"), bytes)
    } else {
        read_segment_bytes(bytes)
    }
}

/// [`is_framed`] for a file, by its first four bytes.
fn is_framed_file(path: &Path) -> bool {
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic).is_ok() && &magic == WAL_FRAME_MAGIC
}

/// Whether `bytes` is a WS-8 framed segment (vs a legacy raw-IPC one).
fn is_framed(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && &bytes[0..4] == WAL_FRAME_MAGIC
}

/// Header length for a framed-segment version, or `None` for one this build
/// does not know how to read.
fn frame_header_len(version: u8) -> Option<usize> {
    match version {
        WAL_FRAME_VERSION_V1 => Some(WAL_FRAME_HEADER_LEN_V1),
        WAL_FRAME_VERSION => Some(WAL_FRAME_HEADER_LEN),
        _ => None,
    }
}

/// Build the framed-segment bytes: header + body. `body` is the on-disk body
/// (Arrow IPC, possibly zstd-compressed per `flags`) and `body_crc` its CRC32;
/// `min_ts`/`max_ts` are the event-time bounds, `owner` the Iceberg table the
/// rows are destined for (`None` ⇒ the all-zero UUID, "no opinion").
fn build_wal_frame(
    flags: u8,
    min_ts: i64,
    max_ts: i64,
    owner: Option<Uuid>,
    body: &[u8],
    body_crc: u32,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(WAL_FRAME_HEADER_LEN + body.len());
    out.extend_from_slice(WAL_FRAME_MAGIC);
    out.push(WAL_FRAME_VERSION);
    out.push(flags);
    out.extend_from_slice(&0u16.to_le_bytes()); // reserved
    out.extend_from_slice(&min_ts.to_le_bytes());
    out.extend_from_slice(&max_ts.to_le_bytes());
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    out.extend_from_slice(&body_crc.to_le_bytes());
    out.extend_from_slice(owner.unwrap_or(Uuid::nil()).as_bytes());
    debug_assert_eq!(out.len(), WAL_FRAME_HEADER_LEN);
    out.extend_from_slice(body);
    out
}

/// Verify a framed segment's header + body CRC and return `(zstd, body)` — the
/// on-disk body slice (still compressed when `zstd`). Errors on a short/garbled
/// header, an unsupported version, a length mismatch, or a CRC mismatch.
///
/// A PARTIAL frame (an active segment recovered as-is) has no length or CRC to
/// check: it is a flushed prefix by construction, and the body is whatever
/// follows the header.
fn verify_wal_frame<'a>(path: &Path, bytes: &'a [u8]) -> Result<(bool, &'a [u8])> {
    if bytes.len() < WAL_FRAME_HEADER_LEN_V1 {
        bail!(
            "WAL frame integrity check: {} is shorter than the header",
            path.display()
        );
    }
    let version = bytes[4];
    let Some(header_len) = frame_header_len(version) else {
        bail!(
            "unsupported WAL frame version {version} in {}",
            path.display()
        );
    };
    if bytes.len() < header_len {
        bail!(
            "WAL frame integrity check: {} is shorter than its v{version} header",
            path.display()
        );
    }
    let zstd = bytes[5] & WAL_FRAME_FLAG_ZSTD != 0;
    let body = &bytes[header_len..];
    if bytes[5] & WAL_FRAME_FLAG_PARTIAL != 0 {
        return Ok((zstd, body));
    }
    let body_len = u64::from_le_bytes(bytes[24..32].try_into().unwrap()) as usize;
    let body_crc = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
    if body.len() != body_len {
        bail!(
            "WAL frame integrity check: {} body length {} != header {}",
            path.display(),
            body.len(),
            body_len
        );
    }
    let actual = crc32(body);
    if actual != body_crc {
        metrics::counter!("siglake_wal_crc_mismatch_total").increment(1);
        bail!(
            "WAL frame integrity check failed for {}: body CRC {actual:#010x} != header {body_crc:#010x}",
            path.display()
        );
    }
    Ok((zstd, body))
}

/// Verify a framed segment and decode it to `RecordBatch`es: CRC-check the body,
/// zstd-decompress it when flagged, then decode the Arrow IPC stream.
fn decode_wal_frame(path: &Path, bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let (zstd, body) = verify_wal_frame(path, bytes)?;
    if zstd {
        let raw = zstd::decode_all(body).context("zstd-decompress WAL body")?;
        read_segment_bytes(&raw)
    } else if bytes[5] & WAL_FRAME_FLAG_PARTIAL != 0 {
        read_partial_segment_bytes(path, body)
    } else {
        read_segment_bytes(body)
    }
}

/// Decode the complete batches in a recovered active segment, dropping only
/// an incomplete final IPC message. The caller restricts this tolerance to a
/// PARTIAL frame: sealed frames and legacy segments retain their all-or-nothing
/// integrity checks.
fn read_partial_segment_bytes(path: &Path, bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    // The framing walk locates the torn message without reading it, so the
    // tolerated tail costs nothing even when the crash left a length that
    // points past the end of the file (#4650).
    let (torn_at, detail) = match ipc_stream_extent(bytes) {
        IpcStreamExtent::Complete => return decode_ipc_stream(bytes),
        IpcStreamExtent::Truncated { offset, detail } => (offset, detail),
        // Bytes all present and still not a message: corruption, not a tear.
        IpcStreamExtent::Malformed { offset, detail } => bail!(
            "WAL partial-segment IPC framing check for {}: the message at byte {offset} {detail}",
            path.display()
        ),
    };

    // Tolerance is for a message cut short by the crash, and only behind at
    // least one complete batch: a garbled first append stays an error.
    let batches = decode_ipc_stream(&bytes[..torn_at]).with_context(|| {
        format!(
            "decoding the complete prefix of {} (first {torn_at} bytes)",
            path.display()
        )
    })?;
    if batches.is_empty() {
        bail!(
            "WAL partial-segment IPC framing check for {}: the message at byte {torn_at} {detail}, \
             and no complete batch precedes it",
            path.display()
        );
    }
    let dropped_bytes = bytes.len() - torn_at;
    let recovered_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    metrics::counter!("siglake_wal_partial_tail_dropped_total").increment(1);
    tracing::warn!(
        segment = %path.display(),
        recovered_rows,
        dropped_bytes,
        reason = %detail,
        "WAL: dropped an incomplete final message from a recovered partial"
    );
    Ok(batches)
}

/// Read a framed segment's header metadata (min/max event timestamp) without
/// decoding the body. Returns `None` for a legacy (unframed) or too-short
/// segment — callers treat that as "no pruning info, read it".
pub fn read_segment_meta(path: &Path) -> Result<Option<WalFrameMeta>> {
    let mut f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hdr = [0u8; WAL_FRAME_HEADER_LEN];
    // A v1 header is shorter than the buffer, so read the common prefix first
    // and only then as much more as the version calls for.
    if f.read_exact(&mut hdr[..WAL_FRAME_HEADER_LEN_V1]).is_err() {
        return Ok(None);
    }
    Ok(frame_meta(&hdr[..WAL_FRAME_HEADER_LEN_V1]))
}

/// [`read_segment_meta`] over a header already in memory.
fn frame_meta(hdr: &[u8]) -> Option<WalFrameMeta> {
    if hdr.len() < WAL_FRAME_HEADER_LEN_V1 || &hdr[0..4] != WAL_FRAME_MAGIC {
        return None;
    }
    frame_header_len(hdr[4])?;
    // An open segment's bounds are not yet folded: reporting `[0, 0]` would let
    // a time-bounded reader prune away every row it holds.
    if hdr[5] & WAL_FRAME_FLAG_PARTIAL != 0 {
        return None;
    }
    Some(WalFrameMeta {
        min_ts_nanos: i64::from_le_bytes(hdr[8..16].try_into().unwrap()),
        max_ts_nanos: i64::from_le_bytes(hdr[16..24].try_into().unwrap()),
    })
}

/// The Iceberg table UUID a segment declares in its frame header, or `None`
/// when it declares none — a legacy raw-IPC segment, a v1 frame, or a v2 frame
/// written by a writer that had not learned its table's identity.
///
/// `None` is "no opinion", not "nobody's": the same rule the directory-level
/// [`OWNER_FILE`] follows, so a deployment upgrading into this keeps serving
/// the segments already on its disks.
pub fn segment_owner_from_bytes(bytes: &[u8]) -> Option<Uuid> {
    if !is_framed(bytes) || bytes.len() < WAL_FRAME_HEADER_LEN {
        return None;
    }
    if bytes[4] != WAL_FRAME_VERSION {
        return None;
    }
    let uuid = Uuid::from_slice(&bytes[WAL_FRAME_HEADER_LEN_V1..WAL_FRAME_HEADER_LEN]).ok()?;
    (!uuid.is_nil()).then_some(uuid)
}

/// [`segment_owner_from_bytes`] reading only the header off disk. An unreadable
/// file is `None` for the same reason [`read_wal_owner`] treats an unreadable
/// marker as absent: the caller's other paths report the read failure with the
/// rows in hand, and failing here would take a query down over a header.
pub fn segment_owner(path: &Path) -> Option<Uuid> {
    let mut f = File::open(path).ok()?;
    let mut hdr = [0u8; WAL_FRAME_HEADER_LEN];
    f.read_exact(&mut hdr).ok()?;
    segment_owner_from_bytes(&hdr)
}

/// Compare a segment's own owner claim against the table `expected` — the
/// per-segment counterpart of [`classify_wal_owner`].
///
/// #2693: the directory marker is re-stamped for the replacement the first time
/// a drain visits it, so an ingester that still holds the DROPPED incarnation's
/// writer open seals into a directory that now says "replacement". The frame
/// header is the identity the directory can no longer speak for, and it is
/// bound before the first append rather than inferred at seal time.
pub fn classify_segment_owner_bytes(bytes: &[u8], expected: &str) -> WalOwner {
    match segment_owner_from_bytes(bytes) {
        None => WalOwner::Unmarked,
        Some(owner) if owner.to_string() == expected => WalOwner::Owned,
        Some(owner) => WalOwner::Stale(owner.to_string()),
    }
}

/// [`classify_segment_owner_bytes`] reading only the header off disk.
pub fn classify_segment_owner(path: &Path, expected: &str) -> WalOwner {
    match segment_owner(path) {
        None => WalOwner::Unmarked,
        Some(owner) if owner.to_string() == expected => WalOwner::Owned,
        Some(owner) => WalOwner::Stale(owner.to_string()),
    }
}

/// Whether a segment may be folded into the table `expected` identifies.
/// [`WalOwner::Unmarked`] segments serve, per the rule above.
pub fn segment_serves_table(path: &Path, expected: &str) -> bool {
    !matches!(classify_segment_owner(path, expected), WalOwner::Stale(_))
}

/// Move one segment (and its sidecars) into `stale/<owner>/`, the same
/// quarantine [`quarantine_stale_wal_dir`] gives a directory's displaced
/// residents.
///
/// Used when the DIRECTORY passes the owner check but an individual segment
/// does not: a stale writer sealed into a re-stamped directory. Nothing is
/// deleted — a never-committed segment's rows may exist nowhere else.
pub fn quarantine_stale_segment(path: &Path, owner: &str) -> Result<PathBuf> {
    // `<dir>/<lifecycle>/<segment>` ⇒ the WAL directory is two levels up.
    let root = path
        .parent()
        .and_then(|p| p.parent())
        .ok_or_else(|| anyhow!("segment {} has no WAL directory", path.display()))?;
    let quarantine = root.join(STALE_DIR).join(owner);
    durability::create_dir_all(&quarantine)
        .with_context(|| format!("creating {}", quarantine.display()))?;
    let name = path
        .file_name()
        .ok_or_else(|| anyhow!("segment {} has no filename", path.display()))?;
    let dest = quarantine.join(name);
    if dest.exists() {
        // Already quarantined under the same owner on a previous cycle.
        delete_segment(path)?;
        return Ok(dest);
    }
    durability::rename(path, &dest)
        .with_context(|| format!("quarantine {} -> {}", path.display(), dest.display()))?;
    move_sidecar(path, &dest);
    // #3149: the caller has decided this segment is not the live table's and
    // will not commit it again. The quarantined name has to outlive a power
    // loss, or the next drain finds it back in `sealed/` and re-makes the same
    // decision — with the dropped incarnation's marker already displaced.
    let from = path
        .parent()
        .ok_or_else(|| anyhow!("segment {} has no parent directory", path.display()))?;
    durability::persist_move(&quarantine, from)
        .with_context(|| format!("persisting quarantine of {}", path.display()))?;
    Ok(dest)
}

/// Why one segment sits under [`POISON_DIR`], written beside it as
/// `<segment>.poison.json`.
///
/// The verdict has to outlive the process that made it: the attempt counter
/// behind it is per-process, so without a durable note a restart would hand
/// the same segment back to the drain and re-derive the same answer, one
/// failed batch at a time. Self-describing on purpose — a file copied out of
/// `poison/` for inspection carries its own reason.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PoisonNote {
    /// Segment file name, as it was under `sealed/`.
    pub segment: String,
    /// The last read error, verbatim.
    pub reason: String,
    /// Consecutive failed drain attempts before the set-aside.
    pub attempts: u32,
    /// When the set-aside happened, ms since the epoch.
    pub quarantined_at_ms: i64,
}

/// Where a poisoned segment's [`PoisonNote`] lives: the segment path with
/// `.poison.json` appended, so the pair sorts together and nothing that lists
/// `*.arrow` picks the note up as a segment.
pub fn poison_note_path(segment: &Path) -> PathBuf {
    let mut s = segment.as_os_str().to_owned();
    s.push(".poison.json");
    PathBuf::from(s)
}

/// Set a segment aside under [`POISON_DIR`] because the drain could not read
/// it, and record why.
///
/// `path` is the segment wherever the drain holds it — `processing/` for a
/// claimed one, `sealed/` for one it never claimed. The note is published
/// first so a segment under `poison/` always has one; the rename is the
/// commit point and both directories are fsynced before this returns.
///
/// Nothing is deleted and nothing is rewritten. Unlike the `orphans/`
/// disposition this is terminal until an operator runs
/// [`requeue_poisoned_segment`]: a segment whose bytes do not decode has no
/// retry that can change the verdict, and leaving it in `sealed/` fails every
/// batch it lands in.
pub fn quarantine_poison_segment(path: &Path, reason: &str, attempts: u32) -> Result<PathBuf> {
    // `<dir>/<lifecycle>/<segment>` ⇒ the WAL directory is two levels up.
    let root = path
        .parent()
        .and_then(|p| p.parent())
        .ok_or_else(|| anyhow!("segment {} has no WAL directory", path.display()))?;
    let quarantine = root.join(POISON_DIR);
    durability::create_dir_all(&quarantine)
        .with_context(|| format!("creating {}", quarantine.display()))?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("segment {} has no filename", path.display()))?
        .to_string();
    let dest = quarantine.join(&name);
    if dest.exists() {
        return Err(anyhow!(
            "{} is already set aside under {}",
            name,
            quarantine.display()
        ));
    }
    let note = PoisonNote {
        segment: name.clone(),
        reason: reason.to_string(),
        attempts,
        quarantined_at_ms: chrono::Utc::now().timestamp_millis(),
    };
    let note_path = poison_note_path(&dest);
    let tmp = quarantine.join(format!(".{name}.poison.{}.tmp", std::process::id()));
    let body = serde_json::to_vec_pretty(&note)
        .with_context(|| format!("serializing the poison note for {name}"))?;
    durability::publish_file(&note_path, &tmp, &body)
        .with_context(|| format!("publishing {}", note_path.display()))?;
    durability::rename(path, &dest)
        .with_context(|| format!("setting aside {} -> {}", path.display(), dest.display()))?;
    move_sidecar(path, &dest);
    let from = path
        .parent()
        .ok_or_else(|| anyhow!("segment {} has no parent directory", path.display()))?;
    durability::persist_move(&quarantine, from)
        .with_context(|| format!("persisting the set-aside of {}", path.display()))?;
    Ok(dest)
}

/// List segments set aside under `<dir>/poison/`, sorted. Empty when the
/// directory is missing, which is the ordinary case.
pub fn list_poisoned(dir: &Path) -> Result<Vec<PathBuf>> {
    list_segments(dir, POISON_DIR)
}

/// Read a poisoned segment's note. `None` when it is missing or unparseable —
/// the segment itself is the thing being preserved, and a note that cannot be
/// read must not stop an operator from listing or requeueing it.
pub fn read_poison_note(segment: &Path) -> Option<PoisonNote> {
    let raw = fs::read(poison_note_path(segment)).ok()?;
    serde_json::from_slice(&raw).ok()
}

/// The operator's deliberate way out of [`POISON_DIR`]: move one segment back
/// into `sealed/` so the next drain cycle claims it again.
///
/// Refuses rather than clobbers when `sealed/` already holds that name. The
/// note is removed only after the segment's new name is durable, so a crash
/// mid-requeue leaves a note without a segment (cosmetic) rather than a
/// segment without its verdict.
///
/// Call this after fixing what made the segment unreadable — restoring the
/// file from a backup, or upgrading to a build that knows its frame version.
/// Requeueing an unchanged segment simply spends the attempt budget again and
/// sets it aside once more. `siglake wal-requeue` is the operator-facing
/// wrapper: it walks the tenant/index layout and reports each note.
pub fn requeue_poisoned_segment(path: &Path) -> Result<PathBuf> {
    let root = path
        .parent()
        .and_then(|p| p.parent())
        .ok_or_else(|| anyhow!("segment {} has no WAL directory", path.display()))?;
    let sealed = root.join(SEALED_DIR);
    durability::create_dir_all(&sealed)
        .with_context(|| format!("creating {}", sealed.display()))?;
    let name = path
        .file_name()
        .ok_or_else(|| anyhow!("segment {} has no filename", path.display()))?;
    let dest = sealed.join(name);
    if dest.exists() {
        return Err(anyhow!(
            "{} already exists; not requeueing over it",
            dest.display()
        ));
    }
    durability::rename(path, &dest)
        .with_context(|| format!("requeueing {} -> {}", path.display(), dest.display()))?;
    move_sidecar(path, &dest);
    let from = path
        .parent()
        .ok_or_else(|| anyhow!("segment {} has no parent directory", path.display()))?;
    durability::persist_move(&sealed, from)
        .with_context(|| format!("persisting the requeue of {}", path.display()))?;
    let note = poison_note_path(path);
    if note.exists() {
        durability::remove_file(&note).with_context(|| format!("removing {}", note.display()))?;
        durability::sync_dir(from)
            .with_context(|| format!("persisting the note removal in {}", from.display()))?;
    }
    Ok(dest)
}

/// The event-time min/max (ns) of a `RecordBatch`'s event-time column, or
/// `None` when the column is absent/empty/all-null.
///
/// Read from the exact `timestamp_ns` sibling where the batch has one — the
/// sealed frame header's `[min,max]` is what the WAL-delta reader prunes on,
/// and a microsecond-truncated max would drop rows inside the last microsecond.
fn batch_ts_min_max(batch: &RecordBatch) -> Option<(i64, i64)> {
    let column = siglake_core::nanos_source_column(batch.schema().as_ref(), "timestamp");
    let idx = batch.schema().index_of(column).ok()?;
    let arr = siglake_core::column_nanos(batch.column(idx))?;
    let arr = &arr;
    let mut mn = i64::MAX;
    let mut mx = i64::MIN;
    for i in 0..arr.len() {
        if arr.is_valid(i) {
            let v = arr.value(i);
            mn = mn.min(v);
            mx = mx.max(v);
        }
    }
    (mn <= mx).then_some((mn, mx))
}

/// `<segment>.crc` — the integrity sidecar path for a sealed segment.
fn crc_sidecar_path(segment: &Path) -> PathBuf {
    let mut s = segment.as_os_str().to_owned();
    s.push(".");
    s.push(CRC_SIDECAR_EXT);
    PathBuf::from(s)
}

/// Move a segment's CRC sidecar alongside a segment rename so the integrity
/// check follows the file through the `sealed → processing → committed`
/// lifecycle. Best-effort: a missing sidecar (pre-WS-8 segment) is a no-op.
fn move_sidecar(from: &Path, to: &Path) {
    let src = crc_sidecar_path(from);
    if src.exists() {
        let _ = fs::rename(&src, crc_sidecar_path(to));
    }
}

/// Delete a segment file and its CRC sidecar together.
///
/// Public so an ingester can reclaim its own local disk: in catalog-claim mode
/// the drain reads the MIRROR and never touches the ingester's filesystem, so
/// nothing else removes these. Deleting the segment while leaving the sidecar
/// would strand a `.crc` file per segment, which is the same unbounded growth
/// in miniature.
pub fn delete_segment(path: &Path) -> Result<()> {
    fs::remove_file(path).with_context(|| format!("deleting {}", path.display()))?;
    remove_sidecar(path);
    Ok(())
}

/// Remove a segment's CRC sidecar when the segment itself is removed.
fn remove_sidecar(segment: &Path) {
    let _ = fs::remove_file(crc_sidecar_path(segment));
}

/// Validate `bytes` against the segment's `.crc` sidecar when present. A missing
/// sidecar means "unchecked" (back-compat with pre-WS-8 segments); a present but
/// mismatching sidecar is a hard error so corruption never reaches the compactor.
fn validate_segment_crc(path: &Path, bytes: &[u8]) -> Result<()> {
    let Ok(expected_str) = fs::read_to_string(crc_sidecar_path(path)) else {
        return Ok(());
    };
    let expected: u32 = expected_str
        .trim()
        .parse()
        .with_context(|| format!("parse CRC sidecar for {}", path.display()))?;
    let actual = crc32(bytes);
    if actual != expected {
        metrics::counter!("siglake_wal_crc_mismatch_total").increment(1);
        anyhow::bail!(
            "WAL segment {} failed integrity check (CRC expected {expected}, got {actual})",
            path.display()
        );
    }
    Ok(())
}

/// Read all `RecordBatch`es from a sealed segment's raw bytes —
/// the in-memory analogue of [`read_segment`] used by the multi-pod
/// compactor path that fetches segments from object storage.
///
/// The IPC length prefixes are walked against the byte count first, so a
/// corrupt segment cannot drive an allocation from a length nobody checked
/// (#4650).
pub fn read_segment_bytes(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    match ipc_stream_extent(bytes) {
        IpcStreamExtent::Complete => {}
        IpcStreamExtent::Truncated { offset, detail }
        | IpcStreamExtent::Malformed { offset, detail } => {
            bail!("WAL segment IPC framing check: the message at byte {offset} {detail}")
        }
    }
    decode_ipc_stream(bytes)
}

/// Where an Arrow IPC stream's messages stop tiling `bytes`, found by walking
/// the length prefixes without decoding or allocating anything.
///
/// Arrow's `StreamReader` sizes the metadata buffer from the 4-byte length
/// prefix and the body buffer from the metadata's `bodyLength`, and allocates
/// both *before* reading the spans they describe. An unframed segment — one
/// written before WS-8, or a framed one whose magic was destroyed by the same
/// corruption that made it unreadable — reaches that decoder with no CRC
/// behind it, so four corrupt bytes become a multi-gigabyte zero-fill per read
/// attempt (#4650: an ASCII `truncated` of nine bytes cost 9.2 s a read).
/// Walking first bounds every length by the segment.
enum IpcStreamExtent {
    /// Every message's metadata and body fit. Fewer than a length prefix's
    /// worth of bytes may follow the last one; `StreamReader` treats EOF there
    /// as a clean end of stream, and so does this walk.
    Complete,
    /// The message starting at `offset` declares more bytes than the segment
    /// holds. In a sealed or legacy segment that is corruption; in a recovered
    /// PARTIAL frame it is the append the crash cut short.
    Truncated { offset: usize, detail: String },
    /// The message starting at `offset` cannot be read at all: a negative
    /// length, or metadata the flatbuffer verifier rejects. Never a torn tail —
    /// the bytes are all present and still do not describe a message.
    Malformed { offset: usize, detail: String },
}

/// Walk `bytes` as an Arrow IPC stream, checking each message's declared
/// metadata and body length against what is left of the segment. Mirrors
/// `StreamReader`'s own framing rules: an optional continuation marker, a
/// little-endian `i32` metadata length, the flatbuffer metadata, then the body
/// `bodyLength` names. A zero length is the end-of-stream marker.
fn ipc_stream_extent(bytes: &[u8]) -> IpcStreamExtent {
    let mut pos = 0usize;
    loop {
        let start = pos;
        if bytes.len() - pos < 4 {
            return IpcStreamExtent::Complete;
        }
        let mut prefix: [u8; 4] = bytes[pos..pos + 4].try_into().unwrap();
        pos += 4;
        if prefix == IPC_CONTINUATION_MARKER {
            if bytes.len() - pos < 4 {
                return IpcStreamExtent::Truncated {
                    offset: start,
                    detail: "has a continuation marker and no metadata length".to_string(),
                };
            }
            prefix = bytes[pos..pos + 4].try_into().unwrap();
            pos += 4;
        }
        let declared_meta = i32::from_le_bytes(prefix);
        if declared_meta == 0 {
            return IpcStreamExtent::Complete;
        }
        let Ok(meta_len) = usize::try_from(declared_meta) else {
            return IpcStreamExtent::Malformed {
                offset: start,
                detail: format!("declares a negative metadata length {declared_meta}"),
            };
        };
        let remaining = bytes.len() - pos;
        if meta_len > remaining {
            return IpcStreamExtent::Truncated {
                offset: start,
                detail: format!(
                    "declares {meta_len} metadata bytes with {remaining} left in the segment"
                ),
            };
        }
        let meta = &bytes[pos..pos + meta_len];
        pos += meta_len;
        let Ok(message) = arrow::ipc::root_as_message(meta) else {
            return IpcStreamExtent::Malformed {
                offset: start,
                detail: format!("has {meta_len} metadata bytes the flatbuffer verifier rejects"),
            };
        };
        let declared_body = message.bodyLength();
        let Ok(body_len) = usize::try_from(declared_body) else {
            return IpcStreamExtent::Malformed {
                offset: start,
                detail: format!("declares a negative body length {declared_body}"),
            };
        };
        let remaining = bytes.len() - pos;
        if body_len > remaining {
            return IpcStreamExtent::Truncated {
                offset: start,
                detail: format!(
                    "declares {body_len} body bytes with {remaining} left in the segment"
                ),
            };
        }
        pos += body_len;
    }
}

/// Decode an Arrow IPC stream whose framing [`ipc_stream_extent`] has already
/// checked. All-or-nothing: any decode error is the caller's error.
fn decode_ipc_stream(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .context("StreamReader::try_new")?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.context("reading IPC batch")?);
    }
    Ok(batches)
}

#[cfg(test)]
mod consumer_watermark_tests {
    use super::*;

    const HUGE: Duration = Duration::from_secs(86_400);
    const ZERO: Duration = Duration::ZERO;

    fn touch_committed(dir: &Path, name: &str) {
        let c = dir.join(COMMITTED_DIR);
        fs::create_dir_all(&c).unwrap();
        fs::write(c.join(name), b"x").unwrap();
    }
    fn committed_names(dir: &Path) -> Vec<String> {
        let c = dir.join(COMMITTED_DIR);
        let mut v: Vec<String> = fs::read_dir(&c)
            .map(|rd| {
                rd.flatten()
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .filter(|n| n.ends_with(".arrow"))
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    }

    /// A committed segment is kept until every fresh consumer has passed it.
    #[test]
    fn coordinated_sweep_respects_consumer_watermark() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        for n in ["s-a.arrow", "s-b.arrow", "s-c.arrow"] {
            touch_committed(dir, n);
        }
        // Fresh consumer has processed through s-b → s-a,s-b sweepable; s-c held.
        publish_consumer_watermark(dir, "detector-0", "s-b.arrow").unwrap();
        let n = sweep_committed_coordinated(dir, ZERO, HUGE, HUGE).unwrap();
        assert_eq!(n, 2);
        assert_eq!(committed_names(dir), vec!["s-c.arrow"]);
        // Once the consumer advances, the last segment is reapable.
        publish_consumer_watermark(dir, "detector-0", "s-c.arrow").unwrap();
        let n = sweep_committed_coordinated(dir, ZERO, HUGE, HUGE).unwrap();
        assert_eq!(n, 1);
        assert!(committed_names(dir).is_empty());
    }

    /// The slowest fresh shard gates the sweep.
    #[test]
    fn min_watermark_is_the_slowest_shard() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        publish_consumer_watermark(dir, "detector-0", "s-c.arrow").unwrap();
        publish_consumer_watermark(dir, "detector-1", "s-a.arrow").unwrap();
        assert_eq!(
            min_consumer_watermark(dir, HUGE).unwrap(),
            Some("s-a.arrow".to_string())
        );
    }

    /// A stale consumer (no recent watermark) is ignored → time-based sweep.
    #[test]
    fn stale_consumer_does_not_hold_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        touch_committed(dir, "s-a.arrow");
        publish_consumer_watermark(dir, "detector-0", "").unwrap(); // consumed nothing
                                                                    // stale_after = 0 ⇒ the consumer is treated as stale ⇒ unconstrained.
        let n = sweep_committed_coordinated(dir, ZERO, HUGE, ZERO).unwrap();
        assert_eq!(n, 1, "stale consumer must not block the sweep");
    }

    /// The hard ceiling sweeps even an unconsumed segment (stuck consumer guard).
    #[test]
    fn hard_ceiling_overrides_fresh_consumer() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        touch_committed(dir, "s-a.arrow");
        publish_consumer_watermark(dir, "detector-0", "").unwrap(); // fresh, consumed nothing
                                                                    // Without the ceiling this would be held; max_retention = 0 forces the sweep.
        let n = sweep_committed_coordinated(dir, ZERO, ZERO, HUGE).unwrap();
        assert_eq!(n, 1);
    }

    /// #4913: with a mark gate, a file whose remote evidence is not durable
    /// stays, however old the consumers say it may go.
    #[test]
    fn mark_gate_holds_unmarked_committed_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        touch_committed(dir, "s-a.arrow");
        touch_committed(dir, "s-b.arrow");
        let marked = std::collections::BTreeSet::from(["s-a.arrow".to_string()]);
        let swept = sweep_committed_gated(dir, ZERO, HUGE, HUGE, Some(&marked)).unwrap();
        assert_eq!(swept.deleted, 1);
        assert_eq!(swept.unmarked, 0);
        assert_eq!(committed_names(dir), vec!["s-b.arrow"]);
        // The mark arriving later releases it, with no leak charged.
        let marked = std::collections::BTreeSet::from(["s-b.arrow".to_string()]);
        let swept = sweep_committed_gated(dir, ZERO, HUGE, HUGE, Some(&marked)).unwrap();
        assert_eq!(swept.deleted, 1);
        assert_eq!(swept.unmarked, 0);
        assert!(committed_names(dir).is_empty());
    }

    /// The ceiling still wins over the mark gate — a catalog outage must cost a
    /// bounded leak, not an unbounded WAL volume — and says so in `unmarked`.
    #[test]
    fn hard_ceiling_overrides_the_mark_gate_and_counts_the_leak() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        touch_committed(dir, "s-a.arrow");
        let empty = std::collections::BTreeSet::new();
        let swept = sweep_committed_gated(dir, ZERO, ZERO, HUGE, Some(&empty)).unwrap();
        assert_eq!(swept.deleted, 1);
        assert_eq!(
            swept.unmarked, 1,
            "the unreclaimable object must be counted"
        );
        assert!(committed_names(dir).is_empty());
    }

    /// No gate is the pre-#4913 behaviour, byte for byte.
    #[test]
    fn no_gate_sweeps_as_before() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        touch_committed(dir, "s-a.arrow");
        let swept = sweep_committed_gated(dir, ZERO, HUGE, HUGE, None).unwrap();
        assert_eq!(swept.deleted, 1);
        assert_eq!(swept.unmarked, 0);
    }

    /// No consumers at all ⇒ pure time-based (back-compat with sweep_committed).
    #[test]
    fn no_consumers_is_time_based() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        touch_committed(dir, "s-a.arrow");
        assert_eq!(min_consumer_watermark(dir, HUGE).unwrap(), None);
        let n = sweep_committed(dir, ZERO).unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn list_layout_dirs_discovers_tenants_and_indexes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        for tenant in ["acme", "widgets"] {
            fs::create_dir_all(root.join(tenant).join(SEALED_DIR)).unwrap();
        }
        fs::create_dir_all(root.join(SEALED_DIR)).unwrap();
        fs::create_dir_all(root.join(CONSUMERS_DIR)).unwrap();
        assert_eq!(
            list_tenant_dirs(root)
                .unwrap()
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            vec!["acme".to_string(), "widgets".to_string()]
        );

        let tenant_dir = root.join("acme");
        for index in ["app1", "app2"] {
            fs::create_dir_all(tenant_dir.join(index).join(SEALED_DIR)).unwrap();
        }
        fs::create_dir_all(tenant_dir.join(SEALED_DIR)).unwrap();
        fs::create_dir_all(tenant_dir.join(PROCESSING_DIR)).unwrap();
        fs::create_dir_all(tenant_dir.join(COMMITTED_DIR)).unwrap();
        fs::create_dir_all(tenant_dir.join(CONSUMERS_DIR)).unwrap();

        assert_eq!(
            list_index_dirs(&tenant_dir)
                .unwrap()
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            vec!["app1".to_string(), "app2".to_string()]
        );
    }

    /// #2661: the owner marker, on its own. Absent is "no opinion"; a stamp is
    /// readable back; a different uuid is `Stale` and names the table the
    /// segments were written for.
    #[test]
    fn a_wal_dir_owner_marker_distinguishes_incarnations() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("acme").join("app1");
        fs::create_dir_all(dir.join(SEALED_DIR)).unwrap();

        assert_eq!(read_wal_owner(&dir), None);
        assert_eq!(classify_wal_owner(&dir, "table-a"), WalOwner::Unmarked);

        stamp_wal_owner(&dir, "table-a").unwrap();
        assert_eq!(read_wal_owner(&dir).as_deref(), Some("table-a"));
        assert_eq!(classify_wal_owner(&dir, "table-a"), WalOwner::Owned);
        assert_eq!(
            classify_wal_owner(&dir, "table-b"),
            WalOwner::Stale("table-a".to_string())
        );

        // Re-stamping replaces, so a quarantined directory is usable again.
        stamp_wal_owner(&dir, "table-b").unwrap();
        assert_eq!(classify_wal_owner(&dir, "table-b"), WalOwner::Owned);

        // The marker is not a segment: nothing that walks the layout sees it.
        assert!(list_sealed(&dir).unwrap().is_empty());
        assert_eq!(
            list_index_dirs(&tmp.path().join("acme"))
                .unwrap()
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            vec!["app1".to_string()]
        );
    }

    /// #2661: quarantining moves the dropped incarnation's segments out of the
    /// live layout dirs — including the `active/` partial a stale writer is
    /// still appending to — keeps them under `stale/<dropped-uuid>/`, and
    /// re-stamps the directory. These residents carry no header identity, so
    /// the displaced marker is all that speaks for them and they all move;
    /// `quarantining_keeps_the_replacements_own_segments_where_they_are` is the
    /// other half.
    #[test]
    fn quarantining_a_stale_wal_dir_preserves_its_segments_under_the_dropped_uuid() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        for sub in [ACTIVE_DIR, SEALED_DIR, PROCESSING_DIR, COMMITTED_DIR] {
            fs::create_dir_all(dir.join(sub)).unwrap();
            fs::write(dir.join(sub).join(format!("{sub}-seg.arrow")), b"rows").unwrap();
        }
        fs::write(dir.join(SEALED_DIR).join("sealed-seg.arrow.crc"), b"crc").unwrap();
        stamp_wal_owner(&dir, "dropped").unwrap();

        assert_eq!(
            quarantine_stale_wal_dir(&dir, "live").unwrap(),
            WalRestamp {
                quarantined: 4,
                kept: 0
            }
        );
        assert_eq!(classify_wal_owner(&dir, "live"), WalOwner::Owned);
        assert!(list_sealed(&dir).unwrap().is_empty());
        assert!(list_visible(&dir).unwrap().is_empty());

        let held = dir.join(STALE_DIR).join("dropped");
        let mut names: Vec<String> = fs::read_dir(&held)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "active-seg.arrow".to_string(),
                "committed-seg.arrow".to_string(),
                "processing-seg.arrow".to_string(),
                "sealed-seg.arrow".to_string(),
                "sealed-seg.arrow.crc".to_string(),
            ],
            "nothing is deleted — the rows may exist nowhere else — and the CRC \
             sidecar travels with its segment"
        );

        // A stale writer that seals again after the quarantine is attributed to
        // the live table; re-running converges rather than duplicating.
        fs::write(dir.join(SEALED_DIR).join("sealed-seg.arrow"), b"rows").unwrap();
        stamp_wal_owner(&dir, "dropped").unwrap();
        assert_eq!(
            quarantine_stale_wal_dir(&dir, "live").unwrap(),
            WalRestamp::default()
        );
        assert!(list_sealed(&dir).unwrap().is_empty());
    }

    /// A framed segment naming `owner`, written straight to `path`. The body is
    /// not decodable — every assertion here is about which file moves.
    fn write_stamped_segment(path: &Path, owner: Option<Uuid>) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let body = b"rows";
        let frame = build_wal_frame(0, 0, 0, owner, body, crc32(body));
        fs::write(path, frame).unwrap();
    }

    /// #2835: a directory can hold both incarnations at once. Between the
    /// recreation and the drain that notices it, a writer that re-resolved the
    /// index is bound to the REPLACEMENT and has already acknowledged rows
    /// here. Quarantine goes by each segment's own header: the replacement's
    /// segments stay (with their sidecars), everything else moves.
    #[test]
    fn quarantining_keeps_the_replacements_own_segments_where_they_are() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let dropped = Uuid::new_v4();
        let live = Uuid::new_v4();

        // The dropped incarnation's population: one sealed, one already
        // committed, and the partial its writer still holds open.
        write_stamped_segment(&dir.join(SEALED_DIR).join("a-sealed.arrow"), Some(dropped));
        write_stamped_segment(
            &dir.join(COMMITTED_DIR).join("a-committed.arrow"),
            Some(dropped),
        );
        write_stamped_segment(
            &dir.join(ACTIVE_DIR).join("a-open.arrow.partial"),
            Some(dropped),
        );
        // A resident with no identity at all: only the marker being displaced
        // ever spoke for it, so it goes too.
        fs::write(dir.join(SEALED_DIR).join("legacy.arrow"), b"raw ipc").unwrap();

        // The replacement's own, acknowledged before any drain arrived.
        write_stamped_segment(&dir.join(SEALED_DIR).join("b-sealed.arrow"), Some(live));
        fs::write(dir.join(SEALED_DIR).join("b-sealed.arrow.crc"), b"12345").unwrap();
        write_stamped_segment(
            &dir.join(ACTIVE_DIR).join("b-open.arrow.partial"),
            Some(live),
        );

        stamp_wal_owner(&dir, &dropped.to_string()).unwrap();
        assert_eq!(
            quarantine_stale_wal_dir(&dir, &live.to_string()).unwrap(),
            WalRestamp {
                quarantined: 3,
                kept: 2
            },
            "quarantined: the dropped incarnation's two `.arrow` segments and \
             the unstamped resident, with its `.partial` moved but never \
             counted. kept: the replacement's sealed segment and its open \
             partial, counted once each — the sidecar is not a segment"
        );
        assert_eq!(classify_wal_owner(&dir, &live.to_string()), WalOwner::Owned);

        assert_eq!(
            list_sealed(&dir).unwrap(),
            vec![dir.join(SEALED_DIR).join("b-sealed.arrow")],
            "the replacement's sealed segment is still drainable"
        );
        assert!(
            dir.join(SEALED_DIR).join("b-sealed.arrow.crc").exists(),
            "and its integrity sidecar stayed with it"
        );
        assert!(
            dir.join(ACTIVE_DIR).join("b-open.arrow.partial").exists(),
            "the replacement's open partial is not pulled out from under its writer"
        );

        let held = dir.join(STALE_DIR).join(dropped.to_string());
        let mut names: Vec<String> = fs::read_dir(&held)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "a-committed.arrow".to_string(),
                "a-open.arrow.partial".to_string(),
                "a-sealed.arrow".to_string(),
                "legacy.arrow".to_string(),
            ]
        );
    }

    /// The kept segments' sidecars are decided by the segment, whatever order
    /// the directory listing returns the pair in, and a sidecar whose segment
    /// is quarantined follows it rather than being left behind.
    #[test]
    fn a_crc_sidecar_follows_its_own_segments_verdict() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let dropped = Uuid::new_v4();
        let live = Uuid::new_v4();

        write_stamped_segment(&dir.join(SEALED_DIR).join("a.arrow"), Some(dropped));
        fs::write(dir.join(SEALED_DIR).join("a.arrow.crc"), b"1").unwrap();
        write_stamped_segment(&dir.join(SEALED_DIR).join("b.arrow"), Some(live));
        fs::write(dir.join(SEALED_DIR).join("b.arrow.crc"), b"2").unwrap();
        // An orphan sidecar (its segment was swept) has nothing to follow and
        // is quarantined with the rest.
        fs::write(dir.join(SEALED_DIR).join("gone.arrow.crc"), b"3").unwrap();
        stamp_wal_owner(&dir, &dropped.to_string()).unwrap();

        assert_eq!(
            quarantine_stale_wal_dir(&dir, &live.to_string()).unwrap(),
            WalRestamp {
                quarantined: 1,
                kept: 1
            },
            "`b.arrow` is kept once; neither its sidecar nor the orphan sidecar \
             is counted as a segment either way"
        );
        let held = dir.join(STALE_DIR).join(dropped.to_string());
        let mut names: Vec<String> = fs::read_dir(&held)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "a.arrow".to_string(),
                "a.arrow.crc".to_string(),
                "gone.arrow.crc".to_string(),
            ]
        );
        assert!(dir.join(SEALED_DIR).join("b.arrow.crc").exists());
        assert_eq!(
            fs::read_to_string(dir.join(SEALED_DIR).join("b.arrow.crc")).unwrap(),
            "2",
            "the kept segment keeps ITS sidecar, not the quarantined one's"
        );
    }
}

#[cfg(test)]
mod crc_integrity_tests {
    use super::*;

    fn partial_frame(batches: &[RecordBatch]) -> Vec<u8> {
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .unwrap_or_else(events_schema);
        let mut body = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut body, &schema).unwrap();
            for batch in batches {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
        }
        body.truncate(body.len() - 8); // remove the IPC EOS marker
        build_wal_frame(WAL_FRAME_FLAG_PARTIAL, 0, 0, None, &body, 0)
    }

    /// Known-answer vector: CRC32/IEEE of "123456789" is 0xCBF43926.
    #[test]
    fn crc32_known_answer() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    fn write_one_segment(dir: &Path) -> PathBuf {
        let mut w = WalWriter::new(dir, "ing").unwrap();
        let evs: Vec<Event> = (0..4).map(|i| Event::now(format!("hello {i}"))).collect();
        w.append_events(&evs).unwrap();
        // Seal deterministically rather than waiting on thresholds.
        let seg = w.seal().unwrap().expect("a sealed segment");
        seg.path
    }

    /// Synthesize a *legacy* (pre-WS-8) raw-IPC segment at `path`, optionally
    /// with a `.crc` sidecar — to exercise the reader's back-compat path.
    fn write_legacy_segment(path: &Path, with_sidecar: bool) {
        let evs: Vec<Event> = (0..4).map(|i| Event::now(format!("legacy {i}"))).collect();
        let batch = events_to_record_batch(&evs).unwrap();
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        fs::write(path, &buf).unwrap();
        if with_sidecar {
            fs::write(crc_sidecar_path(path), crc32(&buf).to_string()).unwrap();
        }
    }

    /// Seal writes a framed segment (magic header, no sidecar) and a fresh read
    /// validates clean against the in-band CRC.
    #[test]
    fn seal_writes_framed_and_reads_validate() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_one_segment(tmp.path());
        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[0..4], WAL_FRAME_MAGIC, "sealed segment is framed");
        assert_eq!(
            bytes[5] & WAL_FRAME_FLAG_ZSTD,
            WAL_FRAME_FLAG_ZSTD,
            "body is zstd-compressed"
        );
        assert!(
            !crc_sidecar_path(&path).exists(),
            "framed segments have no sidecar"
        );
        let rows: usize = read_segment(&path)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 4);
    }

    /// A byte flipped in the framed body trips the in-band integrity check.
    #[test]
    fn corrupted_framed_segment_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_one_segment(tmp.path());
        let mut bytes = fs::read(&path).unwrap();
        // Flip a byte well inside the body (past the 36-byte header).
        let pos = WAL_FRAME_HEADER_LEN + (bytes.len() - WAL_FRAME_HEADER_LEN) / 2;
        bytes[pos] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();
        let err = read_segment(&path).unwrap_err();
        assert!(
            err.to_string().contains("integrity check"),
            "expected integrity failure, got: {err}"
        );
    }

    #[test]
    fn a_partial_with_no_complete_batch_is_rejected() {
        let mut bytes = partial_frame(&[]);
        bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x08, 0x00]);
        let err = read_segment_from_bytes(&bytes).unwrap_err();
        assert!(
            format!("{err:#}").contains("no complete batch precedes it"),
            "a torn first batch is not a recoverable prefix: {err:#}"
        );
    }

    #[test]
    fn arbitrary_partial_ipc_corruption_is_rejected() {
        let batch = events_to_record_batch(&[Event::now("complete")]).unwrap();
        let mut bytes = partial_frame(&[batch]);
        bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&[0; 72]);
        let err = read_segment_from_bytes(&bytes).unwrap_err();
        assert!(
            format!("{err:#}").contains("reading IPC batch"),
            "malformed metadata is corruption, not an incomplete final message: {err:#}"
        );
    }

    /// The framed header exposes the segment's event-time min/max without
    /// decoding the body.
    #[test]
    fn framed_meta_exposes_min_max() {
        use chrono::{TimeZone, Utc};
        let tmp = tempfile::tempdir().unwrap();
        let mut w = WalWriter::new(tmp.path(), "ing").unwrap();
        let mk = |s: i64| {
            let mut e = Event::now(format!("x{s}"));
            e.timestamp = Utc.timestamp_opt(s, 0).single().unwrap();
            e
        };
        // Out-of-order arrival: header must still capture the true range.
        w.append_events(&[mk(300), mk(100), mk(200)]).unwrap();
        let path = w.seal().unwrap().unwrap().path;
        let meta = read_segment_meta(&path).unwrap().expect("framed meta");
        assert_eq!(meta.min_ts_nanos, 100_000_000_000);
        assert_eq!(meta.max_ts_nanos, 300_000_000_000);
    }

    /// A framed segment survives the claim → finish lifecycle and still reads,
    /// with no sidecar files anywhere.
    #[test]
    fn framed_segment_survives_lifecycle() {
        let tmp = tempfile::tempdir().unwrap();
        let sealed = write_one_segment(tmp.path());
        let claimed = claim_segment(&sealed).unwrap();
        let committed = finish_segment(&claimed).unwrap();
        assert!(
            !crc_sidecar_path(&committed).exists(),
            "no sidecar for framed"
        );
        assert_eq!(
            read_segment(&committed)
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            4
        );
        assert_eq!(sweep_committed(tmp.path(), Duration::ZERO).unwrap(), 1);
    }

    /// Back-compat: a legacy raw-IPC segment reads, validating against its `.crc`
    /// sidecar when present and unchecked when absent; corruption is caught.
    #[test]
    fn legacy_segments_still_read() {
        let tmp = tempfile::tempdir().unwrap();
        // With a sidecar → validated.
        let p1 = tmp.path().join("legacy-1.arrow");
        write_legacy_segment(&p1, true);
        assert!(!is_framed(&fs::read(&p1).unwrap()));
        assert_eq!(
            read_segment(&p1)
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            4
        );
        // Corrupt the body → the sidecar catches it.
        let mut bytes = fs::read(&p1).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        fs::write(&p1, &bytes).unwrap();
        assert!(read_segment(&p1)
            .unwrap_err()
            .to_string()
            .contains("integrity check"));
        // No sidecar → unchecked, still reads.
        let p2 = tmp.path().join("legacy-2.arrow");
        write_legacy_segment(&p2, false);
        assert_eq!(
            read_segment(&p2)
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            4
        );
    }
}

/// #2693: the frame header carries the Iceberg table the rows were written for.
#[cfg(test)]
mod segment_owner_tests {
    use super::*;

    const LIVE: &str = "11111111-1111-4111-8111-111111111111";
    const DROPPED: &str = "22222222-2222-4222-8222-222222222222";

    fn writer_bound_to(dir: &Path, id: &str, owner: Option<&str>) -> WalWriter {
        let mut w = WalWriter::new(dir, id).unwrap();
        w.bind_table_uuid(owner.map(|u| Uuid::parse_str(u).unwrap()))
            .unwrap();
        w
    }

    fn seal_one(w: &mut WalWriter, raw: &str) -> PathBuf {
        w.append_events(&[Event::now(raw.to_string())]).unwrap();
        w.seal().unwrap().expect("a sealed segment").path
    }

    /// A sealed segment names its table, and a reader classifies it against
    /// whatever table the directory resolves to today.
    #[test]
    fn a_sealed_segment_carries_its_table_uuid() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = writer_bound_to(tmp.path(), "ing", Some(DROPPED));
        let path = seal_one(&mut w, "row");

        assert_eq!(
            segment_owner(&path).map(|u| u.to_string()),
            Some(DROPPED.to_string())
        );
        assert_eq!(classify_segment_owner(&path, DROPPED), WalOwner::Owned);
        assert_eq!(
            classify_segment_owner(&path, LIVE),
            WalOwner::Stale(DROPPED.to_string()),
            "a segment sealed for the dropped table is refused by the replacement"
        );
        assert!(!segment_serves_table(&path, LIVE));
        // The identity does not cost the rest of the header.
        let meta = read_segment_meta(&path).unwrap().expect("framed meta");
        assert!(meta.min_ts_nanos > 0 && meta.max_ts_nanos >= meta.min_ts_nanos);
        assert_eq!(read_segment(&path).unwrap().len(), 1);
    }

    /// An unbound writer stamps nothing, and an unstamped segment serves: the
    /// same "no opinion" rule the directory marker follows, so an upgrade does
    /// not blank the segments already on disk.
    #[test]
    fn an_unstamped_segment_has_no_opinion() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = WalWriter::new(tmp.path(), "ing").unwrap();
        let path = seal_one(&mut w, "row");
        assert_eq!(segment_owner(&path), None);
        assert_eq!(classify_segment_owner(&path, LIVE), WalOwner::Unmarked);
        assert!(segment_serves_table(&path, LIVE));
    }

    /// A v1 frame (no owner field) and a legacy raw-IPC segment are both
    /// "no opinion", and a v1 frame still decodes.
    #[test]
    fn v1_frames_and_raw_ipc_read_as_unmarked() {
        let tmp = tempfile::tempdir().unwrap();
        // Rebuild a v2 segment's body under a hand-written v1 header.
        let mut w = writer_bound_to(tmp.path(), "ing", Some(DROPPED));
        let v2 = fs::read(seal_one(&mut w, "row")).unwrap();
        let mut v1 = v2[..WAL_FRAME_HEADER_LEN_V1].to_vec();
        v1[4] = WAL_FRAME_VERSION_V1;
        v1.extend_from_slice(&v2[WAL_FRAME_HEADER_LEN..]);
        let p = tmp.path().join("v1.arrow");
        fs::write(&p, &v1).unwrap();

        assert_eq!(segment_owner(&p), None);
        assert_eq!(classify_segment_owner(&p, LIVE), WalOwner::Unmarked);
        assert_eq!(read_segment(&p).unwrap().len(), 1, "a v1 frame still reads");
        assert!(read_segment_meta(&p).unwrap().is_some());

        // A version this build does not know is refused rather than guessed at.
        let mut future = v1.clone();
        future[4] = 99;
        let pf = tmp.path().join("v99.arrow");
        fs::write(&pf, &future).unwrap();
        assert!(read_segment(&pf)
            .unwrap_err()
            .to_string()
            .contains("unsupported WAL frame version"));
        assert_eq!(segment_owner(&pf), None);
    }

    /// The identity goes down before the first row, so a partial promoted out
    /// of `active/` by a crash-recovery pass carries it too — and its
    /// unfolded time range is reported as absent, not as `[0, 0]`.
    #[test]
    fn a_recovered_partial_keeps_the_identity_it_was_opened_with() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = writer_bound_to(tmp.path(), "ing", Some(DROPPED));
        w.append_events(&[Event::now("row".to_string())]).unwrap();
        std::mem::forget(w); // SIGKILL: no seal.

        assert_eq!(recover_orphaned_partials(tmp.path(), "ing").unwrap(), 1);
        let sealed = list_sealed(tmp.path()).unwrap();
        assert_eq!(sealed.len(), 1);
        assert_eq!(
            classify_segment_owner(&sealed[0], LIVE),
            WalOwner::Stale(DROPPED.to_string())
        );
        assert_eq!(read_segment(&sealed[0]).unwrap().len(), 1);
        assert_eq!(
            read_segment_meta(&sealed[0]).unwrap(),
            None,
            "an open segment's bounds are unfolded; reporting [0,0] would prune its rows away"
        );
    }

    /// Rebinding seals first: rows accepted for the previous table keep that
    /// table's identity, and only the next segment gets the new one.
    #[test]
    fn a_rebind_seals_the_open_segment_under_the_previous_table() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = writer_bound_to(tmp.path(), "ing", Some(DROPPED));
        w.append_events(&[Event::now("old".to_string())]).unwrap();
        let sealed = w
            .bind_table_uuid(Some(Uuid::parse_str(LIVE).unwrap()))
            .unwrap()
            .expect("the rebind seals the open segment");
        assert_eq!(
            segment_owner(&sealed.path).map(|u| u.to_string()),
            Some(DROPPED.to_string())
        );
        let fresh = seal_one(&mut w, "new");
        assert_eq!(classify_segment_owner(&fresh, LIVE), WalOwner::Owned);
        // Idempotent: rebinding to the same identity is not a roll.
        assert!(w
            .bind_table_uuid(Some(Uuid::parse_str(LIVE).unwrap()))
            .unwrap()
            .is_none());
    }

    /// A segment refused by the live table is HELD under `stale/<its uuid>/`,
    /// with its sidecar, exactly as a whole quarantined directory would be.
    #[test]
    fn quarantining_one_segment_holds_it_under_its_own_uuid() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = writer_bound_to(tmp.path(), "ing", Some(DROPPED));
        let path = seal_one(&mut w, "row");
        let name = path.file_name().unwrap().to_owned();

        let held = quarantine_stale_segment(&path, DROPPED).unwrap();
        assert_eq!(held, tmp.path().join(STALE_DIR).join(DROPPED).join(&name));
        assert!(held.exists() && !path.exists());
        assert!(list_sealed(tmp.path()).unwrap().is_empty());
        assert_eq!(read_segment(&held).unwrap().len(), 1, "held, not mangled");

        // A re-quarantine of the same name converges instead of failing.
        let again = seal_one(&mut w, "row");
        fs::rename(&again, path.parent().unwrap().join(&name)).unwrap();
        quarantine_stale_segment(&path, DROPPED).unwrap();
        assert!(list_sealed(tmp.path()).unwrap().is_empty());
    }
}

#[cfg(test)]
mod ipc_framing_tests {
    use super::*;

    /// A genuine legacy (pre-WS-8) raw-IPC segment: schema message, one
    /// record-batch message, EOS marker.
    fn legacy_bytes(rows: usize) -> Vec<u8> {
        let evs: Vec<Event> = (0..rows).map(|i| Event::now(format!("row {i}"))).collect();
        let batch = events_to_record_batch(&evs).unwrap();
        let mut buf = Vec::new();
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
        drop(w);
        buf
    }

    /// One message of a raw-IPC stream, located by the same framing rules
    /// [`ipc_stream_extent`] walks: where its metadata starts, how long it is,
    /// and the body length it declares.
    struct Message {
        meta_start: usize,
        meta_len: usize,
        body_len: usize,
    }

    /// Locate every message in a well-formed raw-IPC stream. Test-side only —
    /// it panics rather than classifying, so a fixture that stops being
    /// well-formed fails loudly.
    fn messages(bytes: &[u8]) -> Vec<Message> {
        let mut out = Vec::new();
        let mut pos = 0usize;
        while bytes.len() - pos >= 4 {
            let mut prefix: [u8; 4] = bytes[pos..pos + 4].try_into().unwrap();
            pos += 4;
            if prefix == IPC_CONTINUATION_MARKER {
                prefix = bytes[pos..pos + 4].try_into().unwrap();
                pos += 4;
            }
            let meta_len = i32::from_le_bytes(prefix) as usize;
            if meta_len == 0 {
                break; // end-of-stream marker
            }
            let message = arrow::ipc::root_as_message(&bytes[pos..pos + meta_len]).unwrap();
            let body_len = message.bodyLength() as usize;
            out.push(Message {
                meta_start: pos,
                meta_len,
                body_len,
            });
            pos += meta_len + body_len;
        }
        out
    }

    /// `Message.bodyLength`'s slot in the flatbuffer vtable: `version`,
    /// `header_type`, `header` (a union spends two slots), then `bodyLength`.
    /// Matches `arrow_ipc::Message::VT_BODYLENGTH`, which is not public.
    const VT_BODYLENGTH: usize = 10;

    /// Rewrite a message's declared `bodyLength` in place. Flatbuffers store
    /// the field as an inline little-endian `i64`, reached through the root
    /// offset and the table's vtable, so the edit is a byte substitution that
    /// leaves the metadata verifiable.
    fn set_declared_body_len(bytes: &mut [u8], message: &Message, to: i64) {
        let meta = message.meta_start;
        let u16_at = |at: usize| u16::from_le_bytes(bytes[at..at + 2].try_into().unwrap()) as usize;
        let table = meta + u32::from_le_bytes(bytes[meta..meta + 4].try_into().unwrap()) as usize;
        let vtable =
            table - i32::from_le_bytes(bytes[table..table + 4].try_into().unwrap()) as usize;
        assert!(
            u16_at(vtable) > VT_BODYLENGTH,
            "the vtable must carry a bodyLength slot"
        );
        let field = u16_at(vtable + VT_BODYLENGTH);
        assert_ne!(field, 0, "bodyLength must be stored, not defaulted");
        let at = table + field;
        assert_eq!(
            i64::from_le_bytes(bytes[at..at + 8].try_into().unwrap()),
            message.body_len as i64,
            "the located field must hold the declared body length"
        );
        bytes[at..at + 8].copy_from_slice(&to.to_le_bytes());
        assert_eq!(
            arrow::ipc::root_as_message(
                &bytes[message.meta_start..message.meta_start + message.meta_len]
            )
            .unwrap()
            .bodyLength(),
            to,
            "the patch must land on bodyLength"
        );
    }

    /// The adversarial segments: bytes whose IPC length prefixes declare far
    /// more than the segment holds, each with the byte count a pre-#4650
    /// reader would have allocated and zero-filled.
    fn adversarial() -> Vec<(&'static str, Vec<u8>, u64)> {
        let mut cases: Vec<(&'static str, Vec<u8>, u64)> = Vec::new();

        // The measured fixture: nine ASCII bytes whose first four read as a
        // 1.85 GB metadata length. Cost 9.2 s per drain attempt (#4650).
        cases.push(("ascii `truncated`", b"truncated".to_vec(), 0x6e75_7274));

        // The largest length the prefix can express, with nothing behind it.
        let mut max = i32::MAX.to_le_bytes().to_vec();
        max.extend_from_slice(b"xx");
        cases.push(("i32::MAX metadata length", max, i32::MAX as u64));

        // Same, behind a continuation marker — the modern framing.
        let mut marked = IPC_CONTINUATION_MARKER.to_vec();
        marked.extend_from_slice(&i32::MAX.to_le_bytes());
        marked.extend_from_slice(b"xxxx");
        cases.push(("continuation marker then i32::MAX", marked, i32::MAX as u64));

        // A continuation marker with a torn length behind it.
        let mut short = IPC_CONTINUATION_MARKER.to_vec();
        short.extend_from_slice(b"ab");
        cases.push(("continuation marker, no length", short, 0));

        // A length that reads as negative: `usize::try_from` in the walk, and
        // an unchecked `as usize` widening in the decoder.
        let mut negative = i32::MIN.to_le_bytes().to_vec();
        negative.extend_from_slice(b"garbage");
        cases.push(("negative metadata length", negative, 0));

        // Corruption in a *later* message: a genuine schema and batch read
        // clean, then the appended prefix declares 1.85 GB. Checking only the
        // first prefix would leave this reachable.
        let mut later = legacy_bytes(4);
        later.truncate(later.len() - 8); // drop the EOS marker
        later.extend_from_slice(b"truncated");
        cases.push(("second message declares 1.85 GB", later, 0x6e75_7274));

        // A body length nobody checked: the record batch's metadata is genuine
        // and verifies, and declares a 1 TiB body.
        let mut huge_body = legacy_bytes(4);
        let batch_message = messages(&huge_body).pop().expect("a record-batch message");
        set_declared_body_len(&mut huge_body, &batch_message, 1 << 40);
        cases.push(("record batch declares a 1 TiB body", huge_body, 1 << 40));

        // The same body check against a real truncation rather than a patch.
        let whole = legacy_bytes(4);
        let batch_message = messages(&whole).pop().expect("a record-batch message");
        cases.push((
            "record batch metadata whole, body absent",
            whole[..batch_message.meta_start + batch_message.meta_len].to_vec(),
            0,
        ));

        cases
    }

    /// Every adversarial segment is refused by the pure framing walk, which
    /// allocates nothing: the verdict names the declared length and what was
    /// left of the segment, so the refusal is provably ahead of the decoder's
    /// `resize`/`from_len_zeroed` rather than merely fast.
    #[test]
    fn the_framing_walk_refuses_every_adversarial_length() {
        for (label, bytes, declared) in adversarial() {
            let verdict = ipc_stream_extent(&bytes);
            let detail = match &verdict {
                IpcStreamExtent::Complete => panic!("{label}: accepted as a complete stream"),
                IpcStreamExtent::Truncated { detail, .. }
                | IpcStreamExtent::Malformed { detail, .. } => detail.clone(),
            };
            if declared != 0 {
                assert!(
                    detail.contains(&declared.to_string()),
                    "{label}: the verdict must name the declared length {declared}: {detail}"
                );
            }
        }
    }

    /// Both public readers refuse the adversarial segments, and the whole
    /// table costs a fraction of the 9.2 s one of these bytes used to cost on
    /// its own. The bound is deliberately loose — the assertion that the
    /// refusal precedes the allocation is
    /// [`the_framing_walk_refuses_every_adversarial_length`]; this one only
    /// rules out a multi-gigabyte zero-fill still happening somewhere.
    #[test]
    fn the_readers_refuse_adversarial_segments_without_allocating() {
        let tmp = tempfile::tempdir().unwrap();
        let started = Instant::now();
        for (label, bytes, _) in adversarial() {
            let err = read_segment_bytes(&bytes)
                .map(|b| b.len())
                .expect_err(&format!("{label}: read_segment_bytes must refuse"));
            assert!(
                format!("{err:#}").contains("IPC framing check"),
                "{label}: refused by the framing check, not the decoder: {err:#}"
            );

            // The same bytes as a file: `read_segment` must route an unframed
            // segment through the checked path, not straight at the decoder.
            let path = tmp.path().join("legacy.arrow");
            fs::write(&path, &bytes).unwrap();
            assert!(!is_framed(&bytes), "{label}: the fixture is unframed");
            let err = read_segment(&path)
                .map(|b| b.len())
                .expect_err(&format!("{label}: read_segment must refuse"));
            assert!(
                format!("{err:#}").contains("IPC framing check"),
                "{label}: read_segment refused by the framing check: {err:#}"
            );

            // And over the object-store path the catalog-claim drain uses.
            assert!(
                read_segment_from_bytes(&bytes).is_err(),
                "{label}: read_segment_from_bytes must refuse"
            );
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "refusing {} adversarial segments took {elapsed:?}; one of these bytes cost 9.2 s \
             before the framing walk",
            adversarial().len()
        );
    }

    /// The negative control, kept runnable rather than described: the decoder
    /// the framing walk now guards is unchanged, so calling it directly on the
    /// #4650 fixture reproduces the cost the walk removes. Ignored because it
    /// spends seconds and gigabytes on purpose.
    ///
    /// `cargo test -p siglake-wal --lib unguarded_decode -- --ignored --nocapture`
    #[test]
    #[ignore = "allocates ~1.85 GB and takes seconds, by design"]
    fn the_unguarded_decode_still_pays_for_the_declared_length() {
        let started = Instant::now();
        assert!(decode_ipc_stream(b"truncated").is_err());
        let unguarded = started.elapsed();

        let started = Instant::now();
        assert!(read_segment_bytes(b"truncated").is_err());
        let guarded = started.elapsed();

        println!("unguarded decode {unguarded:?}, guarded read {guarded:?}");
        assert!(
            unguarded > guarded * 100,
            "the walk must be orders cheaper than the allocation it avoids: \
             unguarded {unguarded:?} vs guarded {guarded:?}"
        );
    }

    /// The check does not cost the honest cases anything: a genuine legacy
    /// raw-IPC segment and a framed one both still read, through every reader.
    #[test]
    fn genuine_segments_still_read() {
        let legacy = legacy_bytes(4);
        assert!(matches!(
            ipc_stream_extent(&legacy),
            IpcStreamExtent::Complete
        ));
        let rows = |batches: Vec<RecordBatch>| batches.iter().map(|b| b.num_rows()).sum::<usize>();
        assert_eq!(rows(read_segment_bytes(&legacy).unwrap()), 4);
        assert_eq!(rows(read_segment_from_bytes(&legacy).unwrap()), 4);

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("legacy.arrow");
        fs::write(&path, &legacy).unwrap();
        assert_eq!(rows(read_segment(&path).unwrap()), 4);

        // A stream whose EOS marker never made it to disk is still complete —
        // that is what every recovered partial looks like.
        let no_eos = &legacy[..legacy.len() - 8];
        assert!(matches!(
            ipc_stream_extent(no_eos),
            IpcStreamExtent::Complete
        ));
        assert_eq!(rows(read_segment_bytes(no_eos).unwrap()), 4);

        // Trailing bytes too few to hold a length prefix: `StreamReader`
        // treats EOF there as a clean end of stream, and so does the walk.
        for extra in 1..=3 {
            let mut torn = no_eos.to_vec();
            torn.extend_from_slice(&IPC_CONTINUATION_MARKER[..extra]);
            assert!(
                matches!(ipc_stream_extent(&torn), IpcStreamExtent::Complete),
                "{extra} byte(s) of a continuation marker is a clean end of stream"
            );
            assert_eq!(rows(read_segment_bytes(&torn).unwrap()), 4);
        }

        // The framed path this build writes, for completeness.
        let mut w = WalWriter::new(tmp.path(), "ing").unwrap();
        w.append_events(&[Event::now("framed")]).unwrap();
        let sealed = w.seal().unwrap().expect("a sealed segment");
        assert_eq!(rows(read_segment(&sealed.path).unwrap()), 1);
    }
}
