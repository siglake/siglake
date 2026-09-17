//! Best-effort WAL → object-store mirror.
//!
//! When a WAL segment is sealed (atomic rename `active/<id>.partial`
//! → `sealed/<id>`), the [`WalWriter`] sends a [`WalSegment`] onto an
//! unbounded mpsc channel and creates a durable `mirror-pending/` hard link.
//! [`WalMirror::run`] pulls those off the channel and writes the segment file's
//! bytes to S3 (or any other [`opendal::Operator`] backend) at
//! `<prefix>/<filename>` for the legacy flat layout or
//! `<prefix>/<tenant>/<filename>` for per-tenant writers. The link survives
//! local compaction and retention and is removed after the remote object is
//! confirmed.
//!
//! ## What this gives us
//!
//! Disaster recovery: the PVC backing `sealed/` can be lost (node
//! failure, accidental delete) and we can rebuild it from S3 via
//! `siglake wal-recover`, which rebuilds the `<tenant>[/<index>]/sealed/`
//! layout on the WAL root so the ordinary drain routes each segment to the
//! namespace and table it came from. Sealed segments are recoverable; the
//! *active* (not-yet-sealed) segment is uploaded periodically via
//! [`active_mirror_loop`] so the loss window is bounded by the mirror interval
//! rather than the segment-roll interval, and recovery pulls those back too
//! (preferring a sealed copy where a segment appears as both).
//!
//! ## What this does NOT do
//!
//! - Block ingest. Upload happens on a background tokio task off the
//!   hot path; the channel is unbounded so seal never waits for remote I/O.
//! - Replace local-FS reads. The compactor + detector keep reading
//!   from the local PVC. S3 is write-through only.
//! - Encrypt or deduplicate. The object store's server-side
//!   encryption (S3 SSE-S3 by default) is what protects the data.
//!
//! ## opendal not object_store
//!
//! Earlier versions used `object_store` 0.12 for the S3 backend.
//! Round-5 of the AWS smoke surfaced that `object_store` doesn't
//! auto-resolve EKS IRSA web-identity tokens
//! (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`); only static
//! credentials work. opendal's S3 service does handle IRSA natively
//! via `reqsign`, matching what `iceberg-rust` already uses for
//! warehouse access.
//!
//! Failures emit `siglake_wal_mirror_failures_total` and a tracing
//! warn; they never propagate back to the ingest path.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Buf;
use opendal::Operator;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::WalSegment;

/// A successfully mirrored segment, for catalog registration by the
/// ingest server (fleet mode): the ingester registering its own uploads
/// keeps the drain-side `sync_mirror_to_catalog` a rare RECOVERY sweep
/// instead of a per-cycle full-prefix re-list + re-INSERT of every
/// object (which collapsed the claim path once the mirror backlog grew —
/// the 2026-07-14 round-2 finding).
#[derive(Debug, Clone)]
pub struct MirroredSegment {
    /// Segment id (file basename without `.arrow`).
    pub id: String,
    pub tenant: String,
    pub index_id: String,
    /// Object key including the mirror prefix.
    pub url: String,
    pub bytes: u64,
    pub rows: usize,
}

/// Derive `(tenant, index_id)` from a mirror key suffix — the same
/// layout contract `sync_mirror_to_catalog` parses: `<file>` (legacy
/// flat ⇒ default tenant), `<tenant>/<file>`, `<tenant>/<index>/<file>`.
pub fn parse_mirror_key_suffix(suffix: &str) -> (String, String) {
    let parts: Vec<&str> = suffix.trim_matches('/').split('/').collect();
    match parts.len() {
        2 => (parts[0].to_string(), String::new()),
        3 => (parts[0].to_string(), parts[1].to_string()),
        _ => ("default".to_string(), String::new()),
    }
}

/// Mirror-side counterpart of [`crate::OWNER_FILE`]: the key holding the
/// Iceberg table UUID the segments under `<prefix>/<tenant>/<index>/` were
/// written for (#2661).
///
/// The catalog-claim drain never reads the ingester's filesystem — it claims
/// rows and fetches bytes from this store — so the filesystem marker cannot
/// reach it. The key ends in `owner`, not `.arrow`, so neither
/// `catch_up_sweep` nor the compactor's mirror sync sees it as a segment.
pub fn mirror_owner_key(prefix: &str, tenant: &str, index: &str) -> String {
    format!("{}/{tenant}/{index}/owner", prefix.trim_matches('/'))
}

/// What the mirror-side owner marker says about a key prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorOwner {
    /// The current stamp, compared against the caller's expected table.
    pub state: crate::WalOwner,
    /// Tables this prefix was stamped for BEFORE the current one, most
    /// recently superseded first (#2729).
    ///
    /// Non-empty means the prefix has hosted more than one incarnation of the
    /// index name, so an object here with NO identity of its own cannot be
    /// attributed to the current table — it may be a legacy segment of a
    /// dropped one. This outlives the drain cycle that re-stamped the marker,
    /// which is what keeps #2661's guarantee once the stamp has moved on.
    pub superseded: Vec<String>,
}

/// How many superseded owners a marker keeps. The list exists to answer one
/// yes/no question ("has this prefix ever named another table?"), so it is
/// capped rather than grown without bound by an index that is recreated daily.
const MIRROR_OWNER_HISTORY: usize = 15;

/// Compare the mirror-side owner marker for `(tenant, index)` against
/// `expected`. A marker that is absent is [`crate::WalOwner::Unmarked`]; any
/// OTHER read failure is an error, deliberately — the caller's answer to
/// "unmarked" is to write the marker, and doing that over a transient GET
/// failure would silently adopt a dropped incarnation's segments.
///
/// The body is one UUID per line: the first is the current owner, the rest are
/// [`MirrorOwner::superseded`]. A single-line marker (every marker written
/// before #2729) parses as "current owner, no history".
pub async fn classify_mirror_owner(
    op: &Operator,
    prefix: &str,
    tenant: &str,
    index: &str,
    expected: &str,
) -> Result<MirrorOwner> {
    let key = mirror_owner_key(prefix, tenant, index);
    let raw = match op.read(&key).await {
        Ok(bytes) => String::from_utf8_lossy(&bytes.to_vec()).into_owned(),
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(anyhow::Error::from(e).context(format!("read mirror owner {key}"))),
    };
    let mut lines = raw
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string);
    let Some(current) = lines.next() else {
        return Ok(MirrorOwner {
            state: crate::WalOwner::Unmarked,
            superseded: Vec::new(),
        });
    };
    let state = if current == expected {
        crate::WalOwner::Owned
    } else {
        crate::WalOwner::Stale(current)
    };
    Ok(MirrorOwner {
        state,
        superseded: lines.collect(),
    })
}

/// Write the mirror-side owner marker for `(tenant, index)`, with no history:
/// the prefix has never named another table as far as this drain can tell.
pub async fn stamp_mirror_owner(
    op: &Operator,
    prefix: &str,
    tenant: &str,
    index: &str,
    owner: &str,
) -> Result<()> {
    write_mirror_owner(op, prefix, tenant, index, owner, &[]).await
}

/// Move the marker to `owner`, recording `superseded` as the tables the prefix
/// named before it (#2729).
///
/// The re-stamp is what lets a recreated index drain at all: leaving the marker
/// on the dropped table refuses every object under the prefix forever,
/// including the replacement's own. The history is what keeps the re-stamp from
/// vouching for objects it has no business vouching for.
pub async fn restamp_mirror_owner(
    op: &Operator,
    prefix: &str,
    tenant: &str,
    index: &str,
    owner: &str,
    superseded: &[String],
) -> Result<()> {
    write_mirror_owner(op, prefix, tenant, index, owner, superseded).await
}

async fn write_mirror_owner(
    op: &Operator,
    prefix: &str,
    tenant: &str,
    index: &str,
    owner: &str,
    superseded: &[String],
) -> Result<()> {
    let key = mirror_owner_key(prefix, tenant, index);
    let mut body = format!("{owner}\n");
    for previous in superseded.iter().take(MIRROR_OWNER_HISTORY) {
        body.push_str(previous);
        body.push('\n');
    }
    op.write(&key, body.into_bytes())
        .await
        .with_context(|| format!("stamp mirror owner {key}"))?;
    Ok(())
}

/// Upload attempts before a segment is declared permanently failed.
const MIRROR_UPLOAD_ATTEMPTS: u32 = 5;

/// Durable hard links held while a sealed segment waits for its remote upload.
/// The local drain may reap its own link without removing this one.
const MIRROR_PENDING_DIR: &str = "mirror-pending";

/// Per-segment retry jitter, so a fleet whose object store just recovered does
/// not retry in lockstep. Derived from the key rather than a clock: every node
/// that failed at the same instant would otherwise pick the same delay.
fn upload_retry_jitter(segment: &WalSegment, attempt: u32) -> std::time::Duration {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    segment.mirror_key_suffix.hash(&mut h);
    attempt.hash(&mut h);
    std::time::Duration::from_millis(h.finish() % 250)
}

/// Handle returned by [`WalMirror::new`]. Clone-able; one clone per
/// writer.
#[derive(Clone)]
pub struct WalMirrorHandle {
    tx: UnboundedSender<QueuedSegment>,
    queued: Arc<AtomicUsize>,
}

impl WalMirrorHandle {
    /// Non-blocking send. On send failure (receiver gone) the durable pin stays
    /// for catch-up and the failure counter records the dead worker.
    pub fn enqueue(&self, segment: WalSegment) {
        if let Err(e) = pin_segment(&segment) {
            metrics::counter!("siglake_wal_mirror_failures_total",
                "reason" => "pin")
            .increment(1);
            tracing::error!(path = %segment.path.display(), error = ?e,
                "WAL mirror could not pin sealed segment; local retention may expire it before upload");
        }

        let queued = self.queued.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("siglake_wal_mirror_queue_depth").set(queued as f64);
        if self
            .tx
            .send(QueuedSegment {
                segment,
                enqueued_at: std::time::Instant::now(),
            })
            .is_err()
        {
            let queued = self.queued.fetch_sub(1, Ordering::Relaxed) - 1;
            metrics::gauge!("siglake_wal_mirror_queue_depth").set(queued as f64);
            metrics::counter!("siglake_wal_mirror_failures_total",
                "reason" => "channel_closed")
            .increment(1);
        }
    }
}

struct QueuedSegment {
    segment: WalSegment,
    enqueued_at: std::time::Instant,
}

/// Background worker — drain segments off the channel and write them
/// to the object store.
pub struct WalMirror {
    rx: UnboundedReceiver<QueuedSegment>,
    queued: Arc<AtomicUsize>,
    op: Operator,
    prefix: String,
    uploaded_tx: Option<UnboundedSender<MirroredSegment>>,
}

impl WalMirror {
    /// Build a mirror + the sender handle. The handle is what the
    /// [`crate::WalWriter`] gets, the [`WalMirror`] gets spawned onto
    /// a tokio task.
    pub fn new(op: Operator, prefix: impl Into<String>) -> (Self, WalMirrorHandle) {
        let (tx, rx) = mpsc::unbounded_channel();
        let queued = Arc::new(AtomicUsize::new(0));
        let prefix = prefix.into().trim_matches('/').to_string();
        metrics::gauge!("siglake_wal_mirror_queue_depth").set(0.0);
        (
            Self {
                rx,
                queued: queued.clone(),
                op,
                prefix,
                uploaded_tx: None,
            },
            WalMirrorHandle { tx, queued },
        )
    }

    /// Notify `tx` after each successful upload (see [`MirroredSegment`]).
    pub fn with_uploaded_tx(mut self, tx: UnboundedSender<MirroredSegment>) -> Self {
        self.uploaded_tx = Some(tx);
        self
    }

    /// Run the upload loop forever. Returns only when the sender
    /// half of the channel is dropped (i.e. the writer has shut down).
    pub async fn run(mut self) {
        while let Some(queued_segment) = self.rx.recv().await {
            let queued = self.queued.fetch_sub(1, Ordering::Relaxed) - 1;
            metrics::gauge!("siglake_wal_mirror_queue_depth").set(queued as f64);
            metrics::histogram!("siglake_wal_mirror_queue_wait_seconds")
                .record(queued_segment.enqueued_at.elapsed().as_secs_f64());
            self.upload_and_notify(&queued_segment.segment).await;
        }
    }

    /// Upload one segment, retrying, and make sure the registrar hears about it
    /// if the object ends up in the mirror by ANY route.
    ///
    /// The bug this closes (measured 2026-08-15, 36,122 rows): a failed upload
    /// simply logged and dropped the segment. Nothing told the registrar, so no
    /// catalog row was written. Recovery then depended on `catch_up_sweep`, which
    /// compares LOCAL SEALED FILES against the MIRROR and re-uploads what is
    /// missing -- so it repairs "not uploaded", never "not registered".
    ///
    /// An S3 write can fail from the client's point of view (timeout, reset
    /// mid-multipart) while having succeeded server-side. That produces an object
    /// present in the mirror with no catalog row: the sweep sees it present and
    /// skips it forever, and the rows are accepted, durable, and permanently
    /// unqueryable with zero errors anywhere. Four segments landed in exactly
    /// that state.
    ///
    /// So on exhaustion we STAT the key. If the object is there, the segment is
    /// mirrored regardless of what the error said, and it gets announced. Only a
    /// genuinely absent object is a permanent failure -- and that one the sweep
    /// can and will repair.
    async fn upload_and_notify(&self, segment: &WalSegment) {
        let mut delay = std::time::Duration::from_millis(200);
        let mut last_err = None;
        for attempt in 1..=MIRROR_UPLOAD_ATTEMPTS {
            match self.upload(segment).await {
                Ok(bytes) => {
                    metrics::counter!("siglake_wal_mirror_segments_total",
                        "outcome" => "ok")
                    .increment(1);
                    metrics::counter!("siglake_wal_mirror_bytes_uploaded_total").increment(bytes);
                    if attempt > 1 {
                        tracing::info!(path = %segment.path.display(), attempt,
                            "WAL mirror upload recovered");
                    }
                    self.notify_uploaded(segment, bytes);
                    self.remove_pin(segment);
                    return;
                }
                Err(e) => {
                    let local_gone = matches!(e, MirrorUploadError::LocalNotFound);
                    tracing::warn!(path = %segment.path.display(), attempt, error = ?e,
                        retrying = !local_gone && attempt < MIRROR_UPLOAD_ATTEMPTS,
                        "WAL mirror upload failed");
                    last_err = Some(e);
                    // A vanished local source cannot reappear during this
                    // worker's backoff. Check the remote result immediately
                    // instead of blocking every later segment for ~3.3s.
                    if local_gone {
                        break;
                    }
                    if attempt < MIRROR_UPLOAD_ATTEMPTS {
                        tokio::time::sleep(delay + upload_retry_jitter(segment, attempt)).await;
                        delay = (delay * 2).min(std::time::Duration::from_secs(10));
                    }
                }
            }
        }

        metrics::counter!("siglake_wal_mirror_segments_total", "outcome" => "err").increment(1);
        metrics::counter!("siglake_wal_mirror_failures_total", "reason" => "upload").increment(1);

        // Did it actually land despite the error?
        let key = format!("{}/{}", self.prefix, segment.mirror_key_suffix);
        match self.op.stat(&key).await {
            Ok(meta) => {
                metrics::counter!("siglake_wal_mirror_upload_present_after_error_total")
                    .increment(1);
                tracing::warn!(path = %segment.path.display(), key = %key,
                    "WAL mirror upload reported an error but the object IS present; \
                     registering it so it cannot strand unqueryable");
                self.notify_uploaded(segment, meta.content_length());
                self.remove_pin(segment);
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                metrics::counter!("siglake_wal_mirror_upload_abandoned_total").increment(1);
                tracing::error!(path = %segment.path.display(), key = %key,
                    attempts = MIRROR_UPLOAD_ATTEMPTS, error = ?last_err,
                    "WAL mirror upload PERMANENTLY FAILED and the object is absent; \
                     retaining the pinned local segment for catch-up");
            }
            Err(e) => {
                metrics::counter!("siglake_wal_mirror_failures_total",
                    "reason" => "post_upload_stat")
                .increment(1);
                tracing::error!(path = %segment.path.display(), key = %key,
                    attempts = MIRROR_UPLOAD_ATTEMPTS, error = ?e, upload_error = ?last_err,
                    "WAL mirror upload failed and remote presence could not be determined; \
                     retaining the pinned local segment for catch-up");
            }
        }
    }

    fn remove_pin(&self, segment: &WalSegment) {
        if let Err(e) = remove_pin(segment) {
            metrics::counter!("siglake_wal_mirror_failures_total",
                "reason" => "unpin")
            .increment(1);
            tracing::warn!(path = %segment.path.display(), error = ?e,
                "WAL mirror upload completed but its local pin could not be removed");
        }
    }

    fn notify_uploaded(&self, segment: &WalSegment, bytes: u64) {
        let Some(tx) = &self.uploaded_tx else { return };
        let suffix = segment.mirror_key_suffix.as_str();
        let (tenant, index_id) = parse_mirror_key_suffix(suffix);
        let id = suffix
            .rsplit('/')
            .next()
            .unwrap_or(suffix)
            .trim_end_matches(".arrow")
            .to_string();
        let _ = tx.send(MirroredSegment {
            id,
            tenant,
            index_id,
            url: format!("{}/{}", self.prefix, suffix),
            bytes,
            rows: segment.rows,
        });
    }

    async fn upload(&self, segment: &WalSegment) -> std::result::Result<u64, MirrorUploadError> {
        // Try the segment at the path we were given first. If the
        // compactor already claimed it (sealed/ → processing/) or
        // moved it post-commit (processing/ → committed/), fall back.
        let mut path = segment.path.clone();
        if !path.exists() {
            if let Some(found) = find_segment(segment) {
                path = found;
            } else {
                return Err(MirrorUploadError::LocalNotFound);
            }
        }
        let bytes = tokio::fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                MirrorUploadError::LocalNotFound
            } else {
                MirrorUploadError::Other(
                    anyhow::Error::new(e).context(format!("read {}", path.display())),
                )
            }
        })?;
        let n = bytes.len() as u64;
        let key = format!("{}/{}", self.prefix, segment.mirror_key_suffix);
        self.op
            .write(&key, bytes)
            .await
            .with_context(|| format!("PUT {key}"))
            .map_err(MirrorUploadError::Other)?;
        Ok(n)
    }
}

#[derive(Debug, thiserror::Error)]
enum MirrorUploadError {
    #[error("segment file not found in sealed/, processing/, committed/, or mirror-pending/")]
    LocalNotFound,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

fn wal_dir(segment: &WalSegment) -> Option<&Path> {
    segment.path.parent()?.parent()
}

fn pending_path(segment: &WalSegment) -> Option<PathBuf> {
    Some(
        wal_dir(segment)?
            .join(MIRROR_PENDING_DIR)
            .join(segment.path.file_name()?),
    )
}

/// Give the async uploader a durable name that compactor retention does not
/// own. A hard link costs no second copy and remains readable after the drain
/// renames and removes its sealed/processing/committed names.
fn pin_segment(segment: &WalSegment) -> Result<()> {
    let pending = pending_path(segment).context("segment is not under a WAL directory")?;
    let dir = pending.parent().context("mirror pin has no parent")?;
    crate::durability::create_dir_all(dir)?;
    // Resolve again after a NotFound: the compactor may rename sealed →
    // processing → committed while seal is publishing the queue entry.
    for _ in 0..3 {
        let source = find_segment(segment).context(
            "segment file not found in sealed/, processing/, committed/, or mirror-pending/",
        )?;
        match std::fs::hard_link(&source, &pending) {
            Ok(()) => return crate::durability::sync_dir(dir).context("persist mirror pin"),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!(
                    "hard-link {} -> {}",
                    source.display(),
                    pending.display()
                )))
            }
        }
    }
    anyhow::bail!("segment moved through every local mirror source while pinning")
}

fn remove_pin(segment: &WalSegment) -> Result<()> {
    let Some(pending) = pending_path(segment) else {
        return Ok(());
    };
    match std::fs::remove_file(&pending) {
        Ok(()) => {
            let dir = pending.parent().context("mirror pin has no parent")?;
            crate::durability::sync_dir(dir).context("persist mirror unpin")
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow::Error::new(e).context(format!("remove {}", pending.display()))),
    }
}

/// Catch-up sweep (#68): reconcile locally-sealed and mirror-pinned segments
/// against the mirror prefix, uploading any the mirror is missing. Pins cover
/// the case where the local drain consumed and reaped a segment before the
/// asynchronous uploader reached it, and survive a process crash for this
/// sweep to resume.
///
/// Safe against re-processing while a committed segment keeps both its mirror
/// object and its `wal_segments` row (`status = 'committed'`; re-registration
/// is insert-ignore). Configured committed-mirror retention is therefore held
/// past the ingester's local sealed-file cleanup window: the local copy must be
/// gone before both remote records can disappear. `sealed/` and
/// `mirror-pending/` are swept; `processing/` and `committed/` are local-drain
/// states already consumed by a co-located compactor. Key layout matches the
/// per-writer `mirror_key_suffix`:
/// `<wal>/[tenant[/index]]/sealed/<file>` → `<prefix>/[tenant[/index]/]<file>`.
///
/// Upload sealed segments that never reached the mirror, and RETURN them so the
/// caller can register each one.
///
/// Returning only a count was a correctness hole. This sweep exists for exactly
/// the crash window where a segment was sealed while the object store was
/// unreachable -- it put those segments in the mirror and then left them
/// unregistered, so nothing ever claimed them and the rows stayed unqueryable.
/// The recovery path silently recreated the condition it was written to fix.
pub async fn catch_up_sweep(
    op: &Operator,
    prefix: &str,
    wal_root: &std::path::Path,
) -> Result<Vec<MirroredSegment>> {
    use futures::{stream, StreamExt};

    let prefix = prefix.trim_matches('/');
    // Every local dir that can hold sealed segments, with its mirror key
    // subdir: the legacy root, per-tenant dirs, and per-tenant index dirs.
    let mut roots: Vec<(std::path::PathBuf, String)> =
        vec![(wal_root.to_path_buf(), String::new())];
    if let Ok(tenants) = crate::list_tenant_dirs(wal_root) {
        for (tenant, dir) in tenants {
            roots.push((dir.clone(), tenant.clone()));
            if let Ok(indexes) = crate::list_index_dirs(&dir) {
                for (index, index_dir) in indexes {
                    roots.push((index_dir, format!("{tenant}/{index}")));
                }
            }
        }
    }

    struct Candidate {
        path: std::path::PathBuf,
        name: String,
        subdir: String,
        key: String,
        pinned: bool,
    }

    // Build work from the local backlog. In particular, do not touch
    // the remote store when there is no local sealed work, and never retain
    // remote history in this process.
    let mut candidates = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (dir, subdir) in roots {
        for (paths, pinned) in [
            (list_pending(&dir).unwrap_or_default(), true),
            (crate::list_sealed(&dir).unwrap_or_default(), false),
        ] {
            for path in paths {
                let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                    continue;
                };
                let name = name.to_string();
                let key = if subdir.is_empty() {
                    format!("{prefix}/{name}")
                } else {
                    format!("{prefix}/{subdir}/{name}")
                };
                if seen.insert(key.clone()) {
                    candidates.push(Candidate {
                        path,
                        name,
                        subdir: subdir.clone(),
                        key,
                        pinned,
                    });
                }
            }
        }
    }

    // `buffered`, rather than `buffer_unordered`, preserves the sorted local
    // candidate order. If a later check fails, all registrations returned by
    // earlier candidates are therefore still delivered to the caller.
    const MAX_CONCURRENT_EXISTENCE_CHECKS: usize = 16;
    let checks = stream::iter(candidates)
        .map(|candidate| async move {
            let result = op.stat(&candidate.key).await;
            (candidate, result)
        })
        .buffered(MAX_CONCURRENT_EXISTENCE_CHECKS);
    futures::pin_mut!(checks);

    let mut uploaded: Vec<MirroredSegment> = Vec::new();
    while let Some((candidate, stat)) = checks.next().await {
        match stat {
            Ok(metadata) if metadata.is_file() => {
                if candidate.pinned {
                    let suffix = if candidate.subdir.is_empty() {
                        candidate.name.clone()
                    } else {
                        format!("{}/{name}", candidate.subdir, name = candidate.name)
                    };
                    let (tenant, index_id) = parse_mirror_key_suffix(&suffix);
                    uploaded.push(MirroredSegment {
                        id: candidate.name.trim_end_matches(".arrow").to_string(),
                        tenant,
                        index_id,
                        url: candidate.key.trim_start_matches('/').to_string(),
                        bytes: metadata.content_length(),
                        rows: 0,
                    });
                    remove_candidate_pin(&candidate.path);
                }
                continue;
            }
            Ok(_) => {}
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {}
            Err(e) => {
                metrics::counter!("siglake_wal_mirror_failures_total",
                    "reason" => "sweep_stat")
                .increment(1);
                tracing::warn!(
                    key = %candidate.key,
                    error = ?e,
                    already_uploaded = uploaded.len(),
                    "catch-up sweep: mirror existence check failed; returning the registrations \
                     earned so far and retrying the rest next pass"
                );
                return Ok(uploaded);
            }
        }

        // The local drain may move the file after candidate discovery. Read
        // the same segment from processing/ before treating it as gone.
        let bytes = match read_sweep_candidate(&candidate.path).await {
            Ok(SweepCandidateRead::Bytes(b)) => b,
            Ok(SweepCandidateRead::LocallyCommitted) => {
                // Discovery is not proof that the upload is still owed. This
                // candidate was in `sealed/` when the pass started and is now
                // only in `committed/`: the local drain's Iceberg append
                // returned, so its rows are durable without the mirror, and
                // mirror reclamation may already have collected the key from
                // the ledger mark that same rename drives (#4913). Uploading
                // it here would recreate a reclaimed object that no later pass
                // revisits. A segment whose upload IS still owed keeps its
                // `mirror-pending/` pin, which is a candidate in its own right
                // and reads from its own hard link.
                metrics::counter!("siglake_wal_mirror_sweep_committed_skipped_total").increment(1);
                tracing::debug!(path = %candidate.path.display(), key = %candidate.key,
                    "catch-up sweep: segment was committed locally mid-sweep; not uploading");
                continue;
            }
            Err(e) => {
                tracing::debug!(path = %candidate.path.display(), error = ?e,
                    "catch-up sweep: local segment vanished mid-sweep; skipping");
                continue;
            }
        };
        let n = bytes.len() as u64;
        // STOP the sweep on an upload failure, but do NOT discard what it
        // has already uploaded.
        //
        // This used to be `?`, so the first failing PUT propagated Err and
        // dropped `uploaded` entirely — every segment successfully uploaded
        // earlier in the pass was then in the mirror with NO catalog row.
        // The next pass sees those keys present and skips them, permanently.
        // That is exactly the condition this function's own doc comment says
        // it was rewritten to prevent, reintroduced through the error path
        // instead of the success path. The caller only logs a warn, so it was
        // silent.
        //
        // Returning what succeeded lets the caller register those rows; the
        // segments that did not upload are still in `sealed/` and will be
        // retried next pass, which is the resumable half of the contract.
        if let Err(e) = op.write(&candidate.key, bytes).await {
            metrics::counter!("siglake_wal_mirror_failures_total",
                    "reason" => "sweep_upload")
            .increment(1);
            tracing::warn!(
                key = %candidate.key,
                error = ?e,
                already_uploaded = uploaded.len(),
                "catch-up sweep: upload failed; returning the registrations earned so far \
                 rather than dropping them, and retrying the rest next pass"
            );
            return Ok(uploaded);
        }
        metrics::counter!("siglake_wal_mirror_sweep_uploads_total").increment(1);
        metrics::counter!("siglake_wal_mirror_bytes_uploaded_total").increment(n);
        if candidate.pinned {
            remove_candidate_pin(&candidate.path);
        }
        let suffix = if candidate.subdir.is_empty() {
            candidate.name.clone()
        } else {
            format!("{}/{name}", candidate.subdir, name = candidate.name)
        };
        // Reuse the one parser that defines the layout contract, rather than
        // re-deriving it here and letting the two drift. It takes the suffix
        // WITHOUT the mirror prefix: `<file>`, `<tenant>/<file>`, or
        // `<tenant>/<index>/<file>`.
        let (tenant, index_id) = parse_mirror_key_suffix(&suffix);
        uploaded.push(MirroredSegment {
            id: candidate.name.trim_end_matches(".arrow").to_string(),
            tenant,
            index_id,
            url: candidate.key.trim_start_matches('/').to_string(),
            bytes: n,
            // Row count is not known without decoding the segment, and the
            // claim row only uses it for reporting. The drain reads the real
            // batch anyway.
            rows: 0,
        });
    }
    if !uploaded.is_empty() {
        tracing::info!(
            recovered = uploaded.len(),
            prefix,
            "WAL mirror catch-up sweep recovered segments"
        );
    }
    Ok(uploaded)
}

/// File names under `<dir>/mirror-pending/`: the segments in one WAL directory
/// whose upload is still owed. A pin is created before the queue send and
/// removed only once the object is confirmed present, so a name here means the
/// mirror object may not exist yet — and a name that has left can never come
/// back, because pins are minted at seal time only.
///
/// The mirror-reclamation mark reads this to skip pinned segments (#4913): a
/// mark plus retention would delete a key the uploader is about to write.
pub fn mirror_pending_names(dir: &Path) -> std::collections::BTreeSet<String> {
    list_pending(dir)
        .unwrap_or_default()
        .iter()
        .filter_map(|p| p.file_name().and_then(|s| s.to_str()).map(str::to_string))
        .collect()
}

fn list_pending(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let pending = dir.join(MIRROR_PENDING_DIR);
    if !pending.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(pending)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry.path().extension().and_then(|ext| ext.to_str()) == Some("arrow")
        {
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}

fn remove_candidate_pin(path: &Path) {
    let result = std::fs::remove_file(path).and_then(|()| {
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "pin has no parent")
        })?;
        crate::durability::sync_dir(parent).map_err(std::io::Error::other)
    });
    if let Err(e) = result {
        if e.kind() == std::io::ErrorKind::NotFound {
            return;
        }
        metrics::counter!("siglake_wal_mirror_failures_total",
            "reason" => "sweep_unpin")
        .increment(1);
        tracing::warn!(path = %path.display(), error = ?e,
            "catch-up sweep: mirrored pin could not be removed");
    }
}

/// What a sweep candidate's bytes turned out to be.
enum SweepCandidateRead {
    Bytes(Vec<u8>),
    /// The segment's only remaining local name is `committed/`: the local
    /// drain's Iceberg append returned while this pass was in flight.
    LocallyCommitted,
}

async fn read_sweep_candidate(path: &Path) -> std::io::Result<SweepCandidateRead> {
    match tokio::fs::read(path).await {
        Ok(bytes) => return Ok(SweepCandidateRead::Bytes(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }

    let Some(filename) = path.file_name() else {
        return tokio::fs::read(path).await.map(SweepCandidateRead::Bytes);
    };
    let Some(wal_dir) = path.parent().and_then(Path::parent) else {
        return tokio::fs::read(path).await.map(SweepCandidateRead::Bytes);
    };
    // `processing/` is a claim in flight: the commit has not returned, so the
    // mirror copy is still owed. `committed/` is the opposite answer and is
    // reported, not read — see the caller.
    match tokio::fs::read(wal_dir.join(crate::PROCESSING_DIR).join(filename)).await {
        Ok(bytes) => return Ok(SweepCandidateRead::Bytes(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    if tokio::fs::try_exists(wal_dir.join(crate::COMMITTED_DIR).join(filename))
        .await
        .unwrap_or(false)
    {
        return Ok(SweepCandidateRead::LocallyCommitted);
    }
    tokio::fs::read(path).await.map(SweepCandidateRead::Bytes)
}

/// Search every local name that may retain a segment for the uploader.
fn find_segment(segment: &WalSegment) -> Option<std::path::PathBuf> {
    let filename = segment.path.file_name()?;
    let wal_dir = segment.path.parent()?.parent()?;
    for sub in [
        crate::SEALED_DIR,
        crate::PROCESSING_DIR,
        crate::COMMITTED_DIR,
        MIRROR_PENDING_DIR,
    ] {
        let p = wal_dir.join(sub).join(filename);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

/// One writer's flushed active segment: the local path of the unsealed
/// `.partial` file, its byte count at flush time, and the
/// `<tenant>[/<index>]` its mirror key must carry.
#[derive(Debug, Clone)]
pub struct ActiveSnapshot {
    pub path: PathBuf,
    pub bytes: u64,
    /// `None` for the legacy flat layout, whose key has no tenant component.
    pub subdir: Option<String>,
}

/// Flush a writer's active segment and describe it for the mirror.
///
/// `None` when there is nothing to upload: no open segment, a rows-empty one,
/// or a flush that failed (charged to
/// `siglake_wal_mirror_failures_total{reason="active_flush"}`).
pub fn snapshot_active(writer: &mut crate::WalWriter) -> Option<ActiveSnapshot> {
    let flushed = match writer.flush_active_for_mirror() {
        Ok(s) => s,
        Err(e) => {
            metrics::counter!("siglake_wal_mirror_failures_total",
                "reason" => "active_flush")
            .increment(1);
            tracing::warn!(error = %e, "WAL active mirror flush failed");
            return None;
        }
    };
    let (path, bytes) = flushed?;
    // The active key must carry the same `<tenant>[/<index>]` path the SEALED
    // keys carry. Without it recovery cannot tell whose rows these are, so the
    // whole active mirror — the thing that bounds the PVC-loss window — is
    // unrecoverable on any multi-tenant install.
    let subdir = writer.mirror_subdir().map(str::to_string);
    Some(ActiveSnapshot {
        path,
        bytes,
        subdir,
    })
}

/// The writers one tick of [`active_mirror_loop`] snapshots.
///
/// The loop used to take a single `Arc<Mutex<WalWriter>>`, and on the ingest
/// server that was the ROOT writer — the one writer ingest never appends to.
/// Every row goes to a per-tenant writer (`TenantWalRouter`) or to a
/// backpressure lane's task-owned writer, so the loop flushed an empty writer
/// every tick and uploaded nothing at all: the flag logged itself as enabled
/// and the N-second bound it advertises did not exist (#5055). A source is
/// whatever owns writers, and it is asked on every tick because tenants,
/// indexes and shards come into being as traffic arrives.
///
/// Implementors flush under their own lock (or, for a task-owned writer, inside
/// the task) and return paths. The object-store PUT is the loop's, so no upload
/// ever waits on a writer lock and no writer lock ever waits on an upload.
#[async_trait::async_trait]
pub trait ActiveMirrorSource: Send + Sync {
    /// Flush every writer this source owns, skipping the ones with nothing to
    /// mirror. Best-effort: a writer that cannot be reached this tick is left
    /// for the next one.
    async fn flush_active(&self) -> Vec<ActiveSnapshot>;
}

/// A single writer behind its own lock: the legacy flat path, and the shape
/// `AppState` still uses when no tenant router is installed.
#[async_trait::async_trait]
impl ActiveMirrorSource for tokio::sync::Mutex<crate::WalWriter> {
    async fn flush_active(&self) -> Vec<ActiveSnapshot> {
        let mut writer = self.lock().await;
        snapshot_active(&mut writer).into_iter().collect()
    }
}

/// The interval `--wal-active-mirror-interval-secs` (or
/// `wal.mirror.activeIntervalSecs`) asks for, or `None` for off.
///
/// `0` is the documented disable and the default; it is also the value
/// `tokio::time::interval` panics on, so the two answers are one function.
pub fn active_mirror_interval(secs: u64) -> Option<std::time::Duration> {
    (secs > 0).then(|| std::time::Duration::from_secs(secs))
}

/// Periodically snapshot every in-flight (active) WAL segment and write each
/// partial Arrow IPC stream to `<prefix>/_active/<tenant>[/<index>]/<filename>`.
/// Replaces the previous tick's blob each cycle, so the object store holds at
/// most one active blob per (ingester, segment-uuid).
///
/// On every tick: ask each [`ActiveMirrorSource`] to flush its writers, then
/// read each flushed file and PUT it. A segment whose byte count has not moved
/// since this loop last uploaded it is skipped — the object already holds those
/// bytes, and an idle tenant would otherwise cost a PUT per tick for as long as
/// its segment stays open. Failures emit
/// `siglake_wal_mirror_failures_total{reason="active_upload"}` and a tracing
/// warn; the loop continues.
pub async fn active_mirror_loop(
    sources: Vec<Arc<dyn ActiveMirrorSource>>,
    op: Operator,
    prefix: String,
    interval: std::time::Duration,
) {
    let prefix = prefix.trim_matches('/').to_string();
    let mut ticker = tokio::time::interval(interval);
    // Skip the immediate first fire: we want the first upload to happen
    // *after* one interval, not at startup before any events have arrived.
    ticker.tick().await;
    // Bytes each open segment held when this loop last uploaded it. Keyed by
    // the local path, which is unique per (ingester, segment-uuid), and pruned
    // to the segments still open at the end of every tick — a sealed segment's
    // path never comes back.
    let mut uploaded: std::collections::HashMap<PathBuf, u64> = std::collections::HashMap::new();
    loop {
        ticker.tick().await;
        let mut snapshots = Vec::new();
        for source in &sources {
            snapshots.extend(source.flush_active().await);
        }
        let mut still_open = std::collections::HashSet::with_capacity(snapshots.len());
        for snapshot in snapshots {
            let ActiveSnapshot {
                path,
                bytes,
                subdir,
            } = snapshot;
            still_open.insert(path.clone());
            if uploaded.get(&path) == Some(&bytes) {
                continue;
            }
            let Some(filename) = path
                .file_name()
                .and_then(|s| s.to_str())
                .map(str::to_string)
            else {
                continue;
            };
            let body = match tokio::fs::read(&path).await {
                Ok(b) => b,
                // Sealed between the flush and this read: the segment is the
                // sealed uploader's now, and it carries every byte this tick
                // would have sent.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(path = %path.display(),
                        "WAL active mirror: segment sealed before its snapshot was read");
                    continue;
                }
                Err(e) => {
                    metrics::counter!("siglake_wal_mirror_failures_total",
                        "reason" => "active_read")
                    .increment(1);
                    tracing::warn!(error = %e, "WAL active mirror read failed");
                    continue;
                }
            };
            let n = body.len() as u64;
            let key = match subdir.as_deref() {
                Some(sub) if !sub.is_empty() => format!("{prefix}/_active/{sub}/{filename}"),
                _ => format!("{prefix}/_active/{filename}"),
            };
            match op.write(&key, body).await {
                Ok(_) => {
                    metrics::counter!("siglake_wal_mirror_active_uploads_total").increment(1);
                    metrics::counter!("siglake_wal_mirror_bytes_uploaded_total").increment(n);
                    tracing::debug!(%key, bytes = n, "WAL active mirror upload ok");
                    // The bytes that reached the object store, not the count
                    // the flush reported: appends land between the two, and
                    // the next tick must re-upload what this one did not send.
                    uploaded.insert(path, n);
                }
                Err(e) => {
                    metrics::counter!("siglake_wal_mirror_failures_total",
                        "reason" => "active_upload")
                    .increment(1);
                    tracing::warn!(%key, error = ?e, "WAL active mirror upload failed");
                }
            }
        }
        uploaded.retain(|path, _| still_open.contains(path));
    }
}

/// Where a mirrored object belongs on a reconstructed WAL root, and whether it
/// is a complete sealed segment or an active-mirror prefix of one.
#[derive(Debug, Clone)]
struct RecoveryTarget {
    /// Tenant the key routes to; `default` for the legacy flat layout.
    tenant: String,
    /// Index the key routes to, or `None` for an events segment.
    index: Option<String>,
    /// Path relative to the WAL root, e.g. `acme/sealed/x.arrow` or
    /// `acme/orders/sealed/x.arrow`.
    rel: std::path::PathBuf,
    /// For an INDEX segment, the tenant's own `sealed/` relative to the WAL
    /// root — the tenant discovery dir. `None` for an events segment, whose
    /// destination directory IS that path.
    discovery: Option<std::path::PathBuf>,
    /// Segment stem, used to prefer a sealed copy over an active one.
    stem: String,
    /// True for `_active/` objects: a flushed but unsealed prefix of the
    /// segment, readable (Arrow `StreamReader` tolerates a missing EOS) but
    /// strictly a subset of the sealed copy if one exists.
    partial: bool,
}

/// Map a mirror key suffix (the part after `<prefix>/`) onto its place in the
/// WAL layout.
///
/// Recovery used to keep only the BASENAME and drop the intermediate path, so
/// every tenant's and every index's segments landed in one directory. The FS
/// drain then committed all of them to the default tenant's `events` table —
/// one tenant's logs queryable in another tenant's namespace, index rows
/// null-filled into events, silently. The layout is the routing information;
/// discarding it is the bug.
fn recovery_target(suffix: &str) -> Option<RecoveryTarget> {
    let suffix = suffix.trim_matches('/');
    if suffix.is_empty() {
        return None;
    }
    let (partial, body) = match suffix.strip_prefix("_active/") {
        Some(rest) => (true, rest),
        None => (false, suffix),
    };
    let parts: Vec<&str> = body.split('/').filter(|p| !p.is_empty()).collect();
    let (filename, dirs) = parts.split_last()?;
    // `.arrow.partial` in the active mirror recovers AS a sealed segment: the
    // stream is readable without its EOS marker, which is exactly the basis on
    // which `recover_orphaned_partials` promotes a local partial.
    let stem = filename
        .strip_suffix(".arrow.partial")
        .or_else(|| filename.strip_suffix(".arrow"))?
        .to_string();
    // `<file>` alone is the legacy flat layout, which predates tenancy and
    // therefore means the default tenant.
    let (tenant, index) = match dirs {
        [] => ("default", None),
        [tenant] => (*tenant, None),
        [tenant, index] => (*tenant, Some(*index)),
        // Deeper than the layout allows: refuse rather than guess where it goes.
        _ => return None,
    };
    let mut rel = std::path::PathBuf::from(tenant);
    // An index segment's own directory says nothing about the tenant: the
    // drain enumerates a tenant only by its own `sealed/`, so the segment
    // carries the discovery dir it needs alongside it (#4972).
    let discovery = index.map(|_| rel.join(crate::SEALED_DIR));
    if let Some(index) = index {
        rel.push(index);
    }
    rel.push(crate::SEALED_DIR);
    rel.push(format!("{stem}.arrow"));
    Some(RecoveryTarget {
        tenant: tenant.to_string(),
        index: index.map(str::to_string),
        rel,
        discovery,
        stem,
        partial,
    })
}

/// What the LISTING under `--from` says about whether `--from` is the mirror
/// root, from the two markers that sit at a known depth under it
/// (`docs/DESIGN_wal_recovery_root_identity.md`, "Identity evidence that
/// already exists").
///
/// Neither marker is universal: a mirror with no managed index and no active
/// mirroring — the default install — has neither, and that population gets
/// [`RootVerdict::Unverified`] and the plan.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RootVerdict {
    /// A marker sits at exactly the depth the mirror layout puts it: `--from`
    /// IS the mirror root.
    Confirmed {
        /// The key that pinned it, verbatim.
        evidence: String,
    },
    /// A marker sits exactly one component deeper than the layout allows, so
    /// `--from` is one component above the mirror root — the near miss #4928
    /// cannot see.
    Contradicted {
        /// The misplaced marker key, verbatim.
        evidence: String,
        /// The first component of that key: the directory to pass instead.
        directory: String,
        /// A marker that DID sit at root depth, when the listing holds both.
        /// A confirming marker does not erase the contradiction: a listing
        /// with markers at two depths is not a mirror root either way.
        also_confirmed: Option<String>,
    },
    /// No marker at either depth. The plan is the only checkpoint.
    #[default]
    Unverified,
}

impl RootVerdict {
    /// One line for the operator, whatever the verdict.
    pub fn line(&self) -> String {
        match self {
            Self::Confirmed { evidence } => {
                format!("root confirmed by `{evidence}`, a mirror marker at its own depth")
            }
            Self::Contradicted {
                evidence,
                directory,
                also_confirmed,
            } => {
                let both = also_confirmed
                    .as_deref()
                    .map(|k| format!(" (`{k}` sits at root depth: the listing has markers at two depths, which no mirror root has)"))
                    .unwrap_or_default();
                format!(
                    "root CONTRADICTED: `{evidence}` is a mirror marker one component deeper \
                     than the layout puts it, so --from is one component above the mirror root. \
                     Pass the `{directory}` directory under it instead{both}"
                )
            }
            Self::Unverified => "root unverified: this mirror carries no `_active/` object and \
                                 no `<tenant>/<index>/owner` marker, so nothing in the listing \
                                 pins the root. Read the plan"
                .to_string(),
        }
    }
}

/// The identity evidence one listed key carries, if any.
enum RootEvidence {
    /// A marker at the depth the mirror layout puts it.
    AtRoot,
    /// The same marker one component deeper: the first component is the
    /// directory `--from` should have named.
    OneDeeper(String),
}

/// Read one key suffix for root evidence.
///
/// Both markers are written by siglake itself at a fixed depth under the
/// mirror root: the active loop writes a first component `_active` with a
/// `.arrow.partial` tail (`mirror_active_loop`), and the catalog-claim drain
/// writes `<tenant>/<index>/owner` (`mirror_owner_key`). A sealed segment key
/// never ends in `.partial` and never ends in `owner`, so neither test can
/// fire on ordinary traffic.
fn root_evidence(suffix: &str) -> Option<RootEvidence> {
    let parts: Vec<&str> = suffix
        .trim_matches('/')
        .split('/')
        .filter(|p| !p.is_empty())
        .collect();
    let last = *parts.last()?;
    if last.ends_with(".arrow.partial") {
        if parts[0] == "_active" && parts.len() >= 2 {
            return Some(RootEvidence::AtRoot);
        }
        if parts.len() >= 3 && parts[1] == "_active" {
            return Some(RootEvidence::OneDeeper(parts[0].to_string()));
        }
        return None;
    }
    if last == crate::OWNER_FILE {
        // `<tenant>/<index>/owner` is the only depth the drain writes it at.
        return match parts.len() {
            3 => Some(RootEvidence::AtRoot),
            4 => Some(RootEvidence::OneDeeper(parts[0].to_string())),
            _ => None,
        };
    }
    None
}

/// One `(tenant, index)` destination in a [`RecoveryPlan`]: what would be
/// written there, and where "there" is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanGroup {
    /// Tenant the segments route to; `default` for the legacy flat layout.
    pub tenant: String,
    /// Index they route to, or `None` for a tenant's events lane.
    pub index: Option<String>,
    /// Distinct segments (a sealed key and its active prefix are one).
    pub segments: usize,
    /// Bytes the listing reported for them, or `None` when the store does not
    /// report sizes in a listing (opendal's in-memory service does not).
    pub bytes: Option<u64>,
    /// One key, verbatim, so the operator can see what the layout looked like.
    pub sample_key: String,
    /// Absolute destination directory under the WAL root.
    pub dest: std::path::PathBuf,
    /// Segments of this group already on the WAL root, which an apply would
    /// leave alone.
    pub already_present: usize,
}

/// A restore that has not happened: the whole listing under `--from`,
/// reconstructed into the layout the drain routes on, plus the verdict on the
/// root itself. Nothing on the WAL root is created or modified to build one.
///
/// This is the LIST the restore was going to pay for anyway
/// (`recover_from_object_store` collects every key before it reads one body),
/// and no segment GET. A separate `--apply` invocation lists again, because it
/// must decide on the listing that is current when it writes.
#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    /// Destinations, ordered by tenant then index.
    pub groups: Vec<PlanGroup>,
    /// Keys under the prefix whose layout recovery refuses to guess at.
    pub skipped: usize,
    /// One refused key, verbatim.
    pub sample_skipped_key: Option<String>,
    /// What the listing says about `--from` being the mirror root.
    pub verdict: RootVerdict,
    /// The objects to fetch, keyed by store key — an apply's work list.
    candidates: Vec<(String, RecoveryTarget)>,
}

impl RecoveryPlan {
    /// Distinct segments the plan would restore, already-present ones
    /// included.
    pub fn segments(&self) -> usize {
        self.groups.iter().map(|g| g.segments).sum()
    }

    /// Segments already on the WAL root, which an apply would leave alone.
    pub fn already_present(&self) -> usize {
        self.groups.iter().map(|g| g.already_present).sum()
    }

    /// Bytes across every group, or `None` when the store reported no sizes.
    pub fn bytes(&self) -> Option<u64> {
        let total: u64 = self.groups.iter().filter_map(|g| g.bytes).sum();
        (total > 0).then_some(total)
    }

    /// The error an apply owes the operator when the listing contradicts the
    /// root. Separate from [`RootVerdict::line`] because it has to name the
    /// destination it did NOT touch.
    fn refusal(&self, wal_root: &Path) -> Option<anyhow::Error> {
        let RootVerdict::Contradicted { .. } = self.verdict else {
            return None;
        };
        Some(anyhow::anyhow!(
            "refusing to restore: {}. Nothing under {} was created or changed.",
            self.verdict.line(),
            wal_root.display()
        ))
    }
}

/// List `<prefix>/` and reconstruct what a restore onto `wal_root` would
/// write, without writing it — the plan `siglake wal-recover` prints when it
/// is not given `--apply`.
///
/// `wal_root` is READ (each reconstructed destination is tested for a segment
/// that is already there) and never created or modified, including the root
/// itself: creating it is a visible change on a volume the operator may be
/// inspecting, and a plan that changes the thing it is describing is not a
/// checkpoint.
pub async fn plan_recovery(op: &Operator, prefix: &str, wal_root: &Path) -> Result<RecoveryPlan> {
    use futures::stream::StreamExt;

    // An EMPTY prefix means the caller is already ROOTED at the mirror —
    // which is the only shape `siglake wal-recover` can pass, because
    // `build_opendal_operator` roots the store at the `--from` URL. Formatting
    // `"{prefix}/"` unconditionally turned that into listing `"/"` and
    // stripping `"/"` off relative keys, which fails for every entry, so the
    // whole restore was dropped and the command still exited 0 (#4912).
    let prefix = prefix.trim_matches('/').to_string();
    let listing_prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    };

    // Lister yields `Entry` (dirs + files); filter to files only.
    let lister = op
        .lister_with(&listing_prefix)
        .recursive(true)
        .await
        .context("list WAL mirror")?;
    let mut listing = lister.fuse();
    // Collect first, then decide: a segment can appear both sealed and active,
    // and the sealed copy must win.
    let mut candidates: std::collections::HashMap<String, (String, u64, RecoveryTarget)> =
        std::collections::HashMap::new();
    let mut skipped = 0usize;
    let mut sample_skipped_key: Option<String> = None;
    let mut at_root: Option<String> = None;
    let mut one_deeper: Option<(String, String)> = None;
    while let Some(entry) = listing.next().await {
        let entry = entry.context("list entry")?;
        let path = entry.path().to_string();
        if !entry.metadata().is_file() {
            continue;
        }
        // With an empty prefix the listed key is already relative to the
        // mirror root, so it is its own suffix.
        let Some(suffix) = path.strip_prefix(listing_prefix.as_str()) else {
            continue;
        };
        // The verdict reads keys recovery refuses as well as keys it takes:
        // both markers are refused keys, and the misplaced one is the whole
        // point.
        match root_evidence(suffix) {
            Some(RootEvidence::AtRoot) => {
                at_root.get_or_insert_with(|| path.clone());
            }
            Some(RootEvidence::OneDeeper(dir)) => {
                one_deeper.get_or_insert_with(|| (path.clone(), dir));
            }
            None => {}
        }
        let bytes = entry.metadata().content_length();
        let Some(target) = recovery_target(suffix) else {
            skipped += 1;
            sample_skipped_key.get_or_insert_with(|| path.clone());
            tracing::warn!(key = %path, "wal-recover: unrecognised key, skipped");
            continue;
        };
        match candidates.get(&target.stem) {
            // Already have a sealed copy of this segment; an active prefix of
            // it adds nothing and would duplicate rows.
            Some((_, _, existing)) if !existing.partial => continue,
            _ => {
                candidates.insert(target.stem.clone(), (path, bytes, target));
            }
        }
    }
    if skipped > 0 {
        metrics::counter!("siglake_wal_recover_skipped_total").increment(skipped as u64);
    }

    // Contradiction wins over confirmation: a listing holding markers at two
    // depths is not a mirror root under either reading, and the misplaced one
    // is the evidence that `--from` is too high.
    let verdict = match (one_deeper, at_root) {
        (Some((evidence, directory)), also_confirmed) => RootVerdict::Contradicted {
            evidence,
            directory,
            also_confirmed,
        },
        (None, Some(evidence)) => RootVerdict::Confirmed { evidence },
        (None, None) => RootVerdict::Unverified,
    };

    let mut by_dest: std::collections::BTreeMap<(String, Option<String>), PlanGroup> =
        std::collections::BTreeMap::new();
    let mut work: Vec<(String, RecoveryTarget)> = Vec::with_capacity(candidates.len());
    for (key, bytes, target) in candidates.into_values() {
        let dest = wal_root.join(&target.rel);
        let group = by_dest
            .entry((target.tenant.clone(), target.index.clone()))
            .or_insert_with(|| PlanGroup {
                tenant: target.tenant.clone(),
                index: target.index.clone(),
                segments: 0,
                bytes: None,
                sample_key: key.clone(),
                dest: dest.parent().map(Path::to_path_buf).unwrap_or_default(),
                already_present: 0,
            });
        group.segments += 1;
        if bytes > 0 {
            *group.bytes.get_or_insert(0) += bytes;
        }
        if key < group.sample_key {
            group.sample_key = key.clone();
        }
        if dest.exists() {
            group.already_present += 1;
        }
        work.push((key, target));
    }
    // Stable order for the apply too, so two runs of the same plan write in
    // the same sequence.
    work.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(RecoveryPlan {
        groups: by_dest.into_values().collect(),
        skipped,
        sample_skipped_key,
        verdict,
        candidates: work,
    })
}

/// What one [`recover_from_object_store`] pass did, in the counts an operator
/// needs to tell a finished restore from one that understood nothing.
///
/// A restore that recognises no key and a re-run with nothing left to do both
/// pull zero segments, so reporting only that number turns "nothing
/// understood" into "nothing to do" — which is what pointing `--from`
/// one component above the mirror root produces (#4928). `skipped` is the
/// keys [`recovery_target`] refuses. Neither an already-present destination
/// nor the active copy of a segment also held sealed is a skip: both mean the
/// segment is on the WAL root.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoverySummary {
    /// Segments written onto the WAL root by this pass, each one fsynced
    /// under its final name before it was counted.
    pub pulled: usize,
    /// Keys under the prefix whose layout recovery refuses to guess at.
    pub skipped: usize,
    /// Recognised segments whose destination already existed, so this pass
    /// left them alone.
    pub already_present: usize,
    /// One refused key, verbatim, so a caller can show the operator what the
    /// layout under `--from` actually looked like.
    pub sample_skipped_key: Option<String>,
}

/// Disaster-recovery helper: pull every WAL segment under `<prefix>/` in the
/// object store back onto a local WAL root, RECONSTRUCTING the
/// `<tenant>[/<index>]/sealed/` layout the drain routes on. Returns a
/// [`RecoverySummary`].
///
/// `prefix` is RELATIVE to the operator's root, and may be empty when the
/// operator is already rooted at the mirror (the `siglake wal-recover` shape:
/// the store is built from the whole `--from` URL, path included). Pass the
/// mirror prefix only when the operator is rooted above it, as the uploader's
/// own operator is.
///
/// `wal_root` is the WAL ROOT — the directory the ingester and compactor are
/// pointed at — not a `sealed/` directory. Segments are placed at
/// `<wal_root>/<tenant>/sealed/` and `<wal_root>/<tenant>/<index>/sealed/`, so
/// the ordinary FS drain commits each one to the namespace and table it came
/// from.
///
/// Restoring an index segment also creates the tenant's own `sealed/` — the
/// ingester's "tenant discovery dir" (`siglake-ingest`), which exists because
/// `list_tenant_dirs` enumerates a child of the WAL root only if it has one.
/// Without it a mirror holding only index segments for a tenant — an
/// Elasticsearch-bulk-only tenant whose events lane never sealed — restored
/// from the RIGHT prefix, with a clean report, into a layout the drain never
/// walks: no commit, no `siglake_compactor_index_unresolved_total`, no backlog
/// gauge, nothing in `orphans/` (#4972). It is created before the
/// already-present skip below, so a re-run repairs a restore that predates
/// this.
///
/// Active-mirror objects (`_active/`) are recovered too — they are the whole
/// point of `wal.mirror.activeIntervalSecs`, which bounds the window a PVC loss
/// can lose, and previously had no consumer at all: they were downloaded and
/// then ignored, because `list_sealed` filters on the `.arrow` extension and
/// they end in `.arrow.partial`. A sealed copy always wins over an active one:
/// the active object is a flushed prefix of the same segment, so taking both
/// would duplicate its rows.
///
/// Every segment it counts is durable (#3149): the body goes to a `.tmp`
/// sibling, is fsynced, is renamed onto its final name and the `sealed/`
/// directory is fsynced, all before the count moves. Without that, a restore
/// reported as complete is page-cache-durable only, and the
/// already-present skip below turns a power loss into a permanent hole — a
/// truncated file the operator is told they have and a re-run never re-pulls.
///
/// [`plan_recovery`] and [`apply_plan`] in one call: the listing this apply
/// writes from is its own, taken here. `siglake wal-recover --apply` calls the
/// two halves separately so it can print the plan it is about to apply, and
/// pays the same single LIST.
pub async fn recover_from_object_store(
    op: Operator,
    prefix: &str,
    wal_root: &Path,
) -> Result<RecoverySummary> {
    let plan = plan_recovery(&op, prefix, wal_root).await?;
    apply_plan(&op, plan, wal_root).await
}

/// Write `plan` onto the WAL root: the only path in this module that creates
/// anything under `wal_root`.
///
/// A [`RootVerdict::Contradicted`] plan is refused before anything is created,
/// `wal_root` itself included — the listing says `--from` is one component
/// above the mirror root, and restoring it would invent a tenant named after
/// the mirror prefix (#4964).
///
/// `plan` must come from the listing THIS apply is deciding on
/// (`recover_from_object_store` is the one-call form): the already-present
/// counts in a plan printed by an earlier invocation are a report, not a
/// work list.
pub async fn apply_plan(
    op: &Operator,
    plan: RecoveryPlan,
    wal_root: &Path,
) -> Result<RecoverySummary> {
    if let Some(refusal) = plan.refusal(wal_root) {
        return Err(refusal);
    }
    let mut summary = RecoverySummary {
        skipped: plan.skipped,
        sample_skipped_key: plan.sample_skipped_key.clone(),
        ..RecoverySummary::default()
    };

    crate::create_wal_dir(wal_root).with_context(|| format!("create {}", wal_root.display()))?;
    for (key, target) in plan.candidates {
        let dest = wal_root.join(&target.rel);
        // BEFORE the already-present skip, because the skip is the path a
        // re-run takes over a restore that landed the segments and not this
        // directory — and that restore is the invisible one. Repairing it
        // costs one `is_dir` per candidate on the common path.
        if let Some(discovery) = &target.discovery {
            let discovery = wal_root.join(discovery);
            if !discovery.is_dir() {
                crate::create_wal_dir(&discovery)
                    .with_context(|| format!("create {}", discovery.display()))?;
                tracing::info!(
                    dir = %discovery.display(),
                    "wal-recover: created the tenant discovery dir the drain enumerates on"
                );
            }
        }
        if dest.exists() {
            summary.already_present += 1;
            tracing::debug!(dest = %dest.display(), "wal-recover: already present, skipping");
            continue;
        }
        let Some(parent) = dest.parent() else {
            anyhow::bail!(
                "restored segment {} has no parent directory",
                dest.display()
            );
        };
        crate::create_wal_dir(parent).with_context(|| format!("create {}", parent.display()))?;
        let bs = op.read(&key).await.with_context(|| format!("GET {key}"))?;
        let body = bs.to_bytes();
        // Temp sibling, fsync, rename, fsync the directory — the same ordering
        // the seal uses for the same reason. A `.tmp` left by an interrupted
        // run is invisible to the drain (it scans for `.arrow`) and is
        // truncated by the next attempt at the same key.
        let mut tmp = dest.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = std::path::PathBuf::from(tmp);
        crate::durability::publish_file(&dest, &tmp, &body)
            .with_context(|| format!("write {}", dest.display()))?;
        summary.pulled += 1;
        tracing::info!(
            key = %key,
            dest = %target.rel.display(),
            bytes = body.len(),
            from_active_mirror = target.partial,
            "wal-recover: pulled"
        );
    }
    Ok(summary)
}

/// Tiny adapter so the lister works the same way it did with
/// `object_store::ObjectMeta`. opendal returns a list of entries; we
/// can convert via `meta()` and check `mode()`.
trait _UnusedBufHint {}
impl<T: Buf> _UnusedBufHint for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SEALED_DIR;

    use opendal::layers::observe::{MetricLabels, MetricValue, MetricsIntercept, MetricsLayer};
    use opendal::services::Memory;

    use siglake_core::Event;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    fn memory_op() -> Operator {
        Operator::new(Memory::default()).unwrap().finish()
    }

    #[derive(Clone, Debug, Default)]
    struct RequestCounts {
        lists: Arc<AtomicUsize>,
        stats: Arc<AtomicUsize>,
        writes: Arc<AtomicUsize>,
        move_on_stat: Arc<Mutex<Option<(std::path::PathBuf, std::path::PathBuf)>>>,
    }

    impl MetricsIntercept for RequestCounts {
        fn observe(&self, labels: MetricLabels, value: MetricValue) {
            let MetricValue::OperationExecuting(1) = value else {
                return;
            };
            match labels.operation {
                "list" => {
                    self.lists.fetch_add(1, Ordering::Relaxed);
                }
                "stat" => {
                    self.stats.fetch_add(1, Ordering::Relaxed);
                    if let Some((from, to)) = self.move_on_stat.lock().unwrap().take() {
                        std::fs::rename(from, to).unwrap();
                    }
                }
                "write" => {
                    self.writes.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
    }

    fn counting_fs_op(root: &Path, counts: RequestCounts) -> Operator {
        Operator::new(opendal::services::Fs::default().root(root.to_str().unwrap()))
            .unwrap()
            .layer(MetricsLayer::new(counts))
            .finish()
    }

    fn synth_event(i: usize) -> Event {
        Event {
            timestamp: chrono::Utc::now(),
            host: format!("h{i}"),
            source: "test".into(),
            sourcetype: "t".into(),
            index: "main".into(),
            raw: format!("e{i}"),
            attributes: None,
        }
    }

    #[tokio::test]
    async fn mirror_uploads_sealed_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = crate::WalWriter::with_thresholds(
            tmp.path(),
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();

        let op = memory_op();
        let (mirror, handle) = WalMirror::new(op.clone(), "wal-mirror");
        let mirror_task = tokio::spawn(mirror.run());

        writer.set_mirror_handle(Some(handle));
        let sealed = writer
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");
        drop(writer);
        mirror_task.await.unwrap();

        let key = format!(
            "wal-mirror/{}",
            sealed.path.file_name().unwrap().to_str().unwrap()
        );
        let body = op.read(&key).await.unwrap().to_bytes();
        assert!(!body.is_empty(), "mirrored segment is empty?");
    }

    /// Fleet mode: a successful upload notifies the registrar channel with
    /// the parsed identity so the ingest server can self-register the
    /// segment in the shared catalog (drains then claim it without a
    /// full-prefix recovery sync).
    #[tokio::test]
    async fn uploaded_tx_reports_each_mirrored_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer = crate::WalWriter::with_thresholds(
            tmp.path(),
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        writer.set_mirror_subdir(Some("acme/logs-bench".into()));

        let op = memory_op();
        let (mirror, handle) = WalMirror::new(op.clone(), "wal-mirror");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mirror_task = tokio::spawn(mirror.with_uploaded_tx(tx).run());

        writer.set_mirror_handle(Some(handle));
        let sealed = writer
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");
        drop(writer);
        mirror_task.await.unwrap();

        let seg = rx.recv().await.expect("uploaded notification");
        let file = sealed.path.file_name().unwrap().to_str().unwrap();
        assert_eq!(seg.id, file.trim_end_matches(".arrow"));
        assert_eq!(seg.tenant, "acme");
        assert_eq!(seg.index_id, "logs-bench");
        assert_eq!(seg.url, format!("wal-mirror/acme/logs-bench/{file}"));
        assert_eq!(seg.rows, 2);
        assert!(seg.bytes > 0);
        assert!(rx.recv().await.is_none(), "one seal, one notification");
    }

    /// Run-65 regression: the single uploader fell more than 60 seconds behind,
    /// while the embedded compactor consumed and reaped committed/ at 60s. The
    /// queued path then had no local source and catch-up could not see it.
    #[tokio::test]
    async fn queued_segment_survives_consumption_and_retention_until_upload() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let mut writer = crate::WalWriter::with_thresholds(
            &root,
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();

        let op = memory_op();
        // Deliberately do not run the worker yet: this deterministically models
        // a segment waiting behind a slow/retrying PUT.
        let (mirror, handle) = WalMirror::new(op.clone(), "wal-mirror");
        writer.set_mirror_handle(Some(handle));
        let segment = writer
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");

        let pinned = pending_path(&segment).expect("pin path");
        assert!(pinned.exists(), "seal must durably pin queued bytes");

        // Consume the segment and force the retention floor and hard ceiling to
        // expire before the uploader receives it, exactly as in run 65.
        let processing = crate::claim_segment(&segment.path).unwrap();
        let committed = crate::finish_segment(&processing).unwrap();
        assert_eq!(
            crate::sweep_committed(&root, std::time::Duration::ZERO).unwrap(),
            1
        );
        assert!(!segment.path.exists());
        assert!(!processing.exists());
        assert!(!committed.exists());
        assert!(pinned.exists(), "retention must not own the mirror pin");

        drop(writer);
        mirror.run().await;

        let key = format!("wal-mirror/{}", segment.mirror_key_suffix);
        let mirrored = op.read(&key).await.unwrap().to_bytes();
        assert!(
            !mirrored.is_empty(),
            "queued bytes did not reach the mirror"
        );
        assert!(!pinned.exists(), "a completed upload must release its pin");
    }

    #[tokio::test]
    async fn catch_up_resumes_a_pin_left_after_the_uploader_exits() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let mut writer = crate::WalWriter::with_thresholds(
            &root,
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let op = memory_op();
        let (mirror, handle) = WalMirror::new(op.clone(), "wal-mirror");
        writer.set_mirror_handle(Some(handle));
        let segment = writer
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");
        let pinned = pending_path(&segment).unwrap();

        drop(writer);
        drop(mirror); // process exits before receiving its queued segment
        let processing = crate::claim_segment(&segment.path).unwrap();
        crate::finish_segment(&processing).unwrap();
        crate::sweep_committed(&root, std::time::Duration::ZERO).unwrap();

        let recovered = catch_up_sweep(&op, "wal-mirror", &root).await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].id,
            segment
                .path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .trim_end_matches(".arrow")
        );
        assert!(op
            .stat(&format!("wal-mirror/{}", segment.mirror_key_suffix))
            .await
            .is_ok());
        assert!(!pinned.exists(), "catch-up must release a recovered pin");
    }

    #[tokio::test]
    async fn a_missing_local_source_skips_the_retry_backoff() {
        let tmp = tempfile::tempdir().unwrap();
        let (mirror, _handle) = WalMirror::new(memory_op(), "wal-mirror");
        let segment = WalSegment {
            path: tmp.path().join("sealed/gone.arrow"),
            rows: 1,
            bytes: 1,
            mirror_key_suffix: "gone.arrow".to_string(),
        };

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            mirror.upload_and_notify(&segment),
        )
        .await
        .expect("local NotFound must go directly to remote stat");
    }

    #[test]
    fn parse_mirror_key_suffix_layouts() {
        assert_eq!(
            parse_mirror_key_suffix("seg.arrow"),
            ("default".into(), String::new())
        );
        assert_eq!(
            parse_mirror_key_suffix("acme/seg.arrow"),
            ("acme".into(), String::new())
        );
        assert_eq!(
            parse_mirror_key_suffix("acme/logs/seg.arrow"),
            ("acme".into(), "logs".into())
        );
    }

    /// An upload failure must not discard the registrations the sweep has
    /// already EARNED.
    ///
    /// This used to be `?`, so the first failing PUT propagated Err and dropped
    /// the accumulated `Vec` — every segment uploaded earlier in the pass was
    /// then in the mirror with no catalog row. The next pass sees those keys
    /// present and skips them, permanently. The caller only logs a warn, so it
    /// was silent. That is precisely the condition this function's doc comment
    /// says it was rewritten to prevent, reintroduced via the error path.
    ///
    /// Blocking one key with a DIRECTORY makes its PUT fail while its
    /// predecessor's succeeds; segment names are `<id>-<uuid7>`, and
    /// `list_sealed` sorts, so uuid7's time ordering makes "second" deterministic.
    /// A directory is not a mirrored file, so the existence check proceeds to
    /// the deliberately failing write.
    ///
    /// Against the old code this test FAILS: the sweep returns Err and the
    /// first segment's registration is lost.
    #[tokio::test]
    async fn a_failed_upload_keeps_the_registrations_already_earned() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let store = tmp.path().join("store");
        std::fs::create_dir_all(&store).unwrap();

        let mut w = crate::WalWriter::with_thresholds(
            &root,
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let first = w
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");
        let second = w
            .append_events(&[synth_event(3), synth_event(4)])
            .unwrap()
            .expect("seal");
        drop(w);
        let first_name = first
            .path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let second_name = second
            .path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(first_name < second_name, "uuid7 must order these");

        let op = Operator::new(opendal::services::Fs::default().root(store.to_str().unwrap()))
            .unwrap()
            .finish();
        // Block the SECOND key: a directory cannot be overwritten by a file.
        std::fs::create_dir_all(store.join("wal-mirror").join(&second_name)).unwrap();

        let uploaded = catch_up_sweep(&op, "wal-mirror", &root)
            .await
            .expect("a failed upload must not fail the whole sweep");
        let ids: Vec<&str> = uploaded.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(
            ids.len(),
            1,
            "the sweep must return the registration it earned before failing, got {ids:?}"
        );
        assert_eq!(
            ids[0],
            first_name.trim_end_matches(".arrow"),
            "and it must be the segment that actually uploaded"
        );
        // The blocked segment is still in sealed/, so the next pass retries it.
        assert!(
            crate::list_sealed(&root)
                .unwrap()
                .iter()
                .any(|p| p.file_name().unwrap() == second_name.as_str()),
            "the un-uploaded segment must remain for the next pass"
        );
    }

    #[tokio::test]
    async fn catch_up_checks_only_local_candidates_and_never_lists_remote_history() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let store = tmp.path().join("store");
        let history = store.join("wal-mirror/unrelated-tenant/history");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&history).unwrap();
        for i in 0..2_048 {
            std::fs::write(history.join(format!("old-{i}.arrow")), b"old").unwrap();
        }

        let counts = RequestCounts::default();
        let op = counting_fs_op(&store, counts.clone());

        let empty = catch_up_sweep(&op, "wal-mirror", &root).await.unwrap();
        assert!(empty.is_empty());
        assert_eq!(counts.lists.load(Ordering::Relaxed), 0);
        assert_eq!(counts.stats.load(Ordering::Relaxed), 0);
        assert_eq!(counts.writes.load(Ordering::Relaxed), 0);

        let mut writer = crate::WalWriter::with_thresholds(
            &root,
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let present = writer
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");
        let missing = writer
            .append_events(&[synth_event(3), synth_event(4)])
            .unwrap()
            .expect("seal");
        drop(writer);
        let present_name = present.path.file_name().unwrap();
        std::fs::write(store.join("wal-mirror").join(present_name), b"present").unwrap();

        let recovered = catch_up_sweep(&op, "wal-mirror", &root).await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].id,
            missing
                .path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .trim_end_matches(".arrow")
        );
        assert_eq!(counts.lists.load(Ordering::Relaxed), 0);
        assert_eq!(
            counts.stats.load(Ordering::Relaxed),
            2,
            "one existence check per local candidate, independent of remote history"
        );
        assert_eq!(counts.writes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn catch_up_reads_a_candidate_moved_by_the_local_drain() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let store = tmp.path().join("store");
        std::fs::create_dir_all(&store).unwrap();

        let mut writer = crate::WalWriter::with_thresholds(
            &root,
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let segment = writer
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");
        drop(writer);

        let processing = root
            .join(crate::PROCESSING_DIR)
            .join(segment.path.file_name().unwrap());
        std::fs::create_dir_all(processing.parent().unwrap()).unwrap();
        let counts = RequestCounts::default();
        *counts.move_on_stat.lock().unwrap() = Some((segment.path.clone(), processing.clone()));
        let op = counting_fs_op(&store, counts.clone());

        let recovered = catch_up_sweep(&op, "wal-mirror", &root).await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert!(
            processing.exists(),
            "the stat hook must exercise the move race"
        );
        assert_eq!(counts.lists.load(Ordering::Relaxed), 0);
        assert_eq!(counts.stats.load(Ordering::Relaxed), 1);
        assert_eq!(counts.writes.load(Ordering::Relaxed), 1);
    }

    /// #4913: the same race, one rename further on. A candidate the local
    /// drain has COMMITTED is not uploaded: its rows are in Iceberg without
    /// the mirror, and ledger-driven reclamation may already have deleted the
    /// key that rename marks. Re-uploading it here would recreate an object no
    /// later pass revisits, which is the leak the mark gate exists to close.
    #[tokio::test]
    async fn catch_up_does_not_upload_a_candidate_the_drain_committed() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let store = tmp.path().join("store");
        std::fs::create_dir_all(&store).unwrap();

        let mut writer = crate::WalWriter::with_thresholds(
            &root,
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let segment = writer
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");
        drop(writer);

        let committed = root
            .join(crate::COMMITTED_DIR)
            .join(segment.path.file_name().unwrap());
        std::fs::create_dir_all(committed.parent().unwrap()).unwrap();
        let counts = RequestCounts::default();
        *counts.move_on_stat.lock().unwrap() = Some((segment.path.clone(), committed.clone()));
        let op = counting_fs_op(&store, counts.clone());

        let recovered = catch_up_sweep(&op, "wal-mirror", &root).await.unwrap();
        assert!(
            recovered.is_empty(),
            "a locally-committed segment must not be registered by the sweep"
        );
        assert!(committed.exists(), "the stat hook must exercise the race");
        assert_eq!(
            counts.writes.load(Ordering::Relaxed),
            0,
            "no PUT may recreate the reclaimed key"
        );
        assert_eq!(counts.lists.load(Ordering::Relaxed), 0);
        assert_eq!(counts.stats.load(Ordering::Relaxed), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_permission_denied_stat_keeps_prior_registrations() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let store = tmp.path().join("store");
        std::fs::create_dir_all(&store).unwrap();

        let mut legacy = crate::WalWriter::with_thresholds(
            &root,
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let first = legacy
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");
        drop(legacy);
        let tenant_root = root.join("acme");
        let mut tenant = crate::WalWriter::with_thresholds(
            &tenant_root,
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let second = tenant
            .append_events(&[synth_event(3), synth_event(4)])
            .unwrap()
            .expect("seal");
        drop(tenant);

        let denied = store.join("wal-mirror/acme");
        std::fs::create_dir_all(&denied).unwrap();
        std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o000)).unwrap();
        let counts = RequestCounts::default();
        let op = counting_fs_op(&store, counts.clone());
        let recovered = catch_up_sweep(&op, "wal-mirror", &root).await.unwrap();
        std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].id,
            first
                .path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .trim_end_matches(".arrow")
        );
        assert!(
            second.path.exists(),
            "permission errors must stop before uploading the denied candidate"
        );
        assert_eq!(counts.lists.load(Ordering::Relaxed), 0);
        assert_eq!(counts.stats.load(Ordering::Relaxed), 2);
        assert_eq!(counts.writes.load(Ordering::Relaxed), 1);
    }

    /// #68: segments sealed with NO mirror attached (the object store was
    /// unreachable / unconfigured) are recovered by the catch-up sweep across
    /// all three layouts — legacy root, tenant, tenant/index — while segments
    /// the mirror already holds are not re-uploaded, and a second sweep is a
    /// no-op.
    #[tokio::test]
    async fn catch_up_sweep_recovers_stranded_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // Legacy-root writer, NO mirror handle: two sealed segments strand.
        let mut w = crate::WalWriter::with_thresholds(
            root,
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let s1 = w
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");
        let s2 = w
            .append_events(&[synth_event(3), synth_event(4)])
            .unwrap()
            .expect("seal");
        drop(w);

        // Tenant + tenant/index writers, also stranded.
        let mut wt = crate::WalWriter::with_thresholds(
            root.join("acme"),
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let st = wt
            .append_events(&[synth_event(5), synth_event(6)])
            .unwrap()
            .expect("seal");
        drop(wt);
        let mut wi = crate::WalWriter::with_thresholds(
            root.join("acme").join("app-logs"),
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let si = wi
            .append_events(&[synth_event(7), synth_event(8)])
            .unwrap()
            .expect("seal");
        drop(wi);

        let name =
            |s: &crate::WalSegment| s.path.file_name().unwrap().to_str().unwrap().to_string();
        let op = memory_op();
        // Pre-seed one segment as already-mirrored: it must NOT be re-uploaded.
        op.write(
            &format!("wal-mirror/{}", name(&s1)),
            b"already-mirrored".to_vec(),
        )
        .await
        .unwrap();

        let recovered = catch_up_sweep(&op, "wal-mirror", root).await.unwrap();
        let uploaded = recovered.len();
        assert_eq!(
            uploaded, 3,
            "s2 + tenant + index segments recovered; s1 skipped"
        );

        // The sweep must REPORT what it recovered, not just count it. Returning a
        // bare count meant these segments reached the mirror and were never
        // registered -- so nothing claimed them and their rows stayed unqueryable,
        // which is the exact failure this sweep exists to repair.
        for seg in &recovered {
            assert!(
                !seg.id.is_empty(),
                "a segment must carry an id to be registered"
            );
            assert!(
                !seg.id.ends_with(".arrow"),
                "id is the claim key, not a filename: {}",
                seg.id
            );
            assert!(
                seg.url.starts_with("wal-mirror/"),
                "url must be the mirror key: {}",
                seg.url
            );
            assert!(
                seg.bytes > 0,
                "recovered segment reported as empty: {}",
                seg.id
            );
        }
        // Tenant/index routing survives the round trip, so a recovered segment
        // commits to the same table it would have on the normal path.
        let tenants: std::collections::BTreeSet<_> =
            recovered.iter().map(|s| s.tenant.as_str()).collect();
        assert!(
            tenants.contains("acme"),
            "tenant-scoped segment lost its tenant on recovery: {tenants:?}"
        );

        // The pre-seeded object is untouched (not overwritten by the sweep).
        let body = op
            .read(&format!("wal-mirror/{}", name(&s1)))
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(&body[..], b"already-mirrored");
        // Recovered objects land at the per-layout keys with real bytes.
        for key in [
            format!("wal-mirror/{}", name(&s2)),
            format!("wal-mirror/acme/{}", name(&st)),
            format!("wal-mirror/acme/app-logs/{}", name(&si)),
        ] {
            let body = op.read(&key).await.unwrap().to_bytes();
            assert!(!body.is_empty(), "swept segment {key} is empty");
        }

        // Idempotent: everything present ⇒ second sweep uploads nothing.
        let again = catch_up_sweep(&op, "wal-mirror", root).await.unwrap().len();
        assert_eq!(again, 0, "second sweep must be a no-op");
    }

    #[tokio::test]
    async fn mirror_uploads_tenant_segment_under_tenant_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let tenant_dir = tmp.path().join("acme");
        let mut writer = crate::WalWriter::with_thresholds(
            &tenant_dir,
            "ing-test",
            2,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        writer.set_mirror_subdir(Some("acme".to_string()));

        let op = memory_op();
        let (mirror, handle) = WalMirror::new(op.clone(), "wal-mirror");
        let mirror_task = tokio::spawn(mirror.run());

        writer.set_mirror_handle(Some(handle));
        let sealed = writer
            .append_events(&[synth_event(1), synth_event(2)])
            .unwrap()
            .expect("seal");
        drop(writer);
        mirror_task.await.unwrap();

        let filename = sealed.path.file_name().unwrap().to_str().unwrap();
        let key = format!("wal-mirror/acme/{filename}");
        let body = op.read(&key).await.unwrap().to_bytes();
        assert!(!body.is_empty(), "mirrored tenant segment is empty?");
    }

    #[tokio::test]
    async fn active_mirror_uploads_partial_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let writer = crate::WalWriter::with_thresholds(
            tmp.path(),
            "ing-test",
            1_000_000,
            std::time::Duration::from_secs(3600),
        )
        .unwrap();
        let writer = std::sync::Arc::new(tokio::sync::Mutex::new(writer));

        {
            let mut w = writer.lock().await;
            w.append_events(&[synth_event(1), synth_event(2)]).unwrap();
        }

        let op = memory_op();
        let task_op = op.clone();
        let task_writer: Vec<Arc<dyn ActiveMirrorSource>> = vec![writer.clone()];
        let task = tokio::spawn(async move {
            active_mirror_loop(
                task_writer,
                task_op,
                "wal-mirror".to_string(),
                std::time::Duration::from_millis(50),
            )
            .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        task.abort();

        use futures::stream::StreamExt;
        let lister = op.lister_with("wal-mirror/_active/").await.unwrap();
        let mut listing = lister.fuse();
        let mut found = 0;
        let mut total_bytes = 0u64;
        while let Some(entry) = listing.next().await {
            let entry = entry.unwrap();
            if !entry.metadata().is_file() {
                continue;
            }
            found += 1;
            // opendal's Memory service doesn't populate content_length
            // in list metadata; read the body to confirm non-empty.
            let body = op.read(entry.path()).await.unwrap();
            total_bytes += body.len() as u64;
        }
        assert_eq!(found, 1, "expected exactly one active-mirror blob");
        assert!(total_bytes > 0, "active-mirror blob is empty");
    }

    /// One tick covers every source, each writer keyed by its own subdir.
    ///
    /// The ingest server has one source per router and one writer per
    /// (tenant, index, shard); a loop that stopped at the first source with
    /// something to send would bound one tenant's loss window and no other.
    /// `active_mirror_wiring.rs` in `siglake-ingest` is the same claim through
    /// the HTTP handlers.
    #[tokio::test]
    async fn active_mirror_covers_every_source_per_tick() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sources: Vec<Arc<dyn ActiveMirrorSource>> = Vec::new();
        for tenant in ["acme", "widgets"] {
            let dir = tmp.path().join(tenant);
            let mut writer = crate::WalWriter::with_thresholds(
                &dir,
                "ing-test",
                1_000_000,
                std::time::Duration::from_secs(3600),
            )
            .unwrap();
            writer.set_mirror_subdir(Some(tenant.to_string()));
            writer
                .append_events(&[synth_event(1), synth_event(2)])
                .unwrap();
            sources.push(Arc::new(tokio::sync::Mutex::new(writer)));
        }

        let op = memory_op();
        let task_op = op.clone();
        let task = tokio::spawn(async move {
            active_mirror_loop(
                sources,
                task_op,
                "wal-mirror".to_string(),
                std::time::Duration::from_millis(20),
            )
            .await;
        });

        use futures::stream::StreamExt;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let keys = loop {
            let mut listing = op
                .lister_with("wal-mirror/_active/")
                .recursive(true)
                .await
                .unwrap()
                .fuse();
            let mut keys = Vec::new();
            while let Some(entry) = listing.next().await {
                let entry = entry.unwrap();
                if entry.metadata().is_file() {
                    keys.push(entry.path().to_string());
                }
            }
            if keys.len() >= 2 || std::time::Instant::now() >= deadline {
                keys.sort();
                break keys;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        task.abort();

        assert_eq!(keys.len(), 2, "one blob per source: {keys:?}");
        assert!(
            keys[0].starts_with("wal-mirror/_active/acme/")
                && keys[1].starts_with("wal-mirror/_active/widgets/"),
            "each blob is keyed by its writer's subdir: {keys:?}"
        );
    }

    /// The plan groups by the destination the drain routes on, and touches
    /// the WAL root not at all — not the root, not a `sealed/`, not the
    /// tenant discovery dir (#4973).
    #[tokio::test]
    async fn plan_groups_by_destination_and_writes_nothing() {
        let op = memory_op();
        for key in [
            "wal-mirror/acme/a.arrow",
            "wal-mirror/acme/b.arrow",
            "wal-mirror/acme/orders/c.arrow",
            "wal-mirror/flat.arrow",
            "wal-mirror/README.md",
        ] {
            op.write(key, bytes::Bytes::from_static(b"BODY"))
                .await
                .unwrap();
        }

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let plan = plan_recovery(&op, "wal-mirror", &root).await.unwrap();

        assert!(!root.exists(), "the plan created {}", root.display());
        assert_eq!(plan.segments(), 4);
        assert_eq!(plan.skipped, 1);
        assert_eq!(
            plan.sample_skipped_key.as_deref(),
            Some("wal-mirror/README.md")
        );
        assert_eq!(plan.already_present(), 0);
        assert_eq!(
            plan.groups
                .iter()
                .map(|g| (
                    g.tenant.as_str(),
                    g.index.as_deref(),
                    g.segments,
                    g.dest
                        .strip_prefix(&root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("acme", None, 2, format!("acme/{SEALED_DIR}")),
                (
                    "acme",
                    Some("orders"),
                    1,
                    format!("acme/orders/{SEALED_DIR}")
                ),
                ("default", None, 1, format!("default/{SEALED_DIR}")),
            ]
        );
        // opendal's in-memory service reports no size in a listing, and a
        // plan pays one LIST and no per-object request, so it says so rather
        // than inventing a total.
        assert_eq!(plan.bytes(), None);

        // After the apply, a second plan counts what is there instead of
        // proposing it again.
        let summary = recover_from_object_store(op.clone(), "wal-mirror", &root)
            .await
            .unwrap();
        assert_eq!(summary.pulled, 4);
        let plan = plan_recovery(&op, "wal-mirror", &root).await.unwrap();
        assert_eq!(plan.already_present(), 4);
    }

    /// Both markers pin the root at their own depth, and refuse one component
    /// above it. A confirming marker does not cancel a contradicting one.
    #[tokio::test]
    async fn the_root_verdict_reads_both_markers_at_their_own_depth() {
        async fn verdict_for(keys: &[&str], prefix: &str) -> RootVerdict {
            let op = memory_op();
            for key in keys {
                op.write(key, bytes::Bytes::from_static(b"BODY"))
                    .await
                    .unwrap();
            }
            let tmp = tempfile::tempdir().unwrap();
            plan_recovery(&op, prefix, &tmp.path().join("wal"))
                .await
                .unwrap()
                .verdict
        }

        // At their own depth, from the mirror root.
        for key in [
            "m/_active/x.arrow.partial",
            "m/_active/acme/x.arrow.partial",
            "m/_active/acme/orders/x.arrow.partial",
            "m/acme/orders/owner",
        ] {
            assert!(
                matches!(
                    verdict_for(&[key], "m").await,
                    RootVerdict::Confirmed { .. }
                ),
                "{key} pins the root"
            );
        }

        // One component deeper: the near miss, naming the directory to pass.
        for (key, dir) in [
            ("m/wal-mirror/_active/acme/x.arrow.partial", "wal-mirror"),
            ("m/wal-mirror/acme/orders/owner", "wal-mirror"),
        ] {
            match verdict_for(&[key], "m").await {
                RootVerdict::Contradicted {
                    directory,
                    also_confirmed,
                    ..
                } => {
                    assert_eq!(directory, dir);
                    assert_eq!(also_confirmed, None);
                }
                other => panic!("{key} should contradict, got {other:?}"),
            }
        }

        // Neither: the default install.
        assert_eq!(
            verdict_for(&["m/acme/x.arrow", "m/README.md"], "m").await,
            RootVerdict::Unverified
        );

        // Both depths at once: the contradiction wins and the confirming key
        // is reported alongside it.
        match verdict_for(
            &[
                "m/_active/acme/a.arrow.partial",
                "m/wal-mirror/_active/acme/b.arrow.partial",
            ],
            "m",
        )
        .await
        {
            RootVerdict::Contradicted {
                evidence,
                directory,
                also_confirmed,
            } => {
                assert_eq!(evidence, "m/wal-mirror/_active/acme/b.arrow.partial");
                assert_eq!(directory, "wal-mirror");
                assert_eq!(
                    also_confirmed.as_deref(),
                    Some("m/_active/acme/a.arrow.partial")
                );
            }
            other => panic!("the contradiction must win, got {other:?}"),
        }
    }

    /// An apply over a contradicted root creates NOTHING — not the segment,
    /// not its directories, and not the WAL root itself.
    #[tokio::test]
    async fn an_apply_refuses_a_contradicted_root_before_it_creates_anything() {
        let op = memory_op();
        for key in [
            "warehouse/wal-mirror/acme/a.arrow",
            "warehouse/wal-mirror/_active/acme/b.arrow.partial",
        ] {
            op.write(key, bytes::Bytes::from_static(b"BODY"))
                .await
                .unwrap();
        }

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let err = recover_from_object_store(op, "warehouse", &root)
            .await
            .expect_err("a contradicted root must refuse");
        let msg = format!("{err:#}");
        assert!(msg.contains("wal-mirror"), "{msg}");
        assert!(msg.contains("one component above the mirror root"), "{msg}");
        assert!(!root.exists(), "{} was created", root.display());
    }

    /// Legacy flat keys predate tenancy, so they mean the default tenant.
    #[tokio::test]
    async fn mirror_recovers_flat_legacy_keys_as_the_default_tenant() {
        let op = memory_op();
        op.write("wal-mirror/a.arrow", bytes::Bytes::from_static(b"AAA"))
            .await
            .unwrap();
        op.write("wal-mirror/b.arrow", bytes::Bytes::from_static(b"BBB"))
            .await
            .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let summary = recover_from_object_store(op, "wal-mirror", &root)
            .await
            .unwrap();
        assert_eq!(summary.pulled, 2);
        assert!(root
            .join("default")
            .join(SEALED_DIR)
            .join("a.arrow")
            .exists());
        assert!(root
            .join("default")
            .join(SEALED_DIR)
            .join("b.arrow")
            .exists());
    }

    /// The layout IS the routing information, and recovery used to throw it
    /// away: it kept only the basename, so every tenant's and every index's
    /// segments landed in one directory and the FS drain committed all of them
    /// to the default tenant's `events` table — one tenant's logs queryable in
    /// another tenant's namespace, index rows null-filled into events, no error
    /// anywhere. The only test covered the flat legacy layout the product had
    /// already stopped emitting, so the multi-tenant case never ran.
    ///
    /// Against that code this test FAILS.
    #[tokio::test]
    async fn recovery_rebuilds_the_tenant_and_index_layout() {
        let op = memory_op();
        op.write("wal-mirror/acme/s1.arrow", bytes::Bytes::from_static(b"A1"))
            .await
            .unwrap();
        op.write(
            "wal-mirror/widgets/s2.arrow",
            bytes::Bytes::from_static(b"W2"),
        )
        .await
        .unwrap();
        op.write(
            "wal-mirror/acme/orders/s3.arrow",
            bytes::Bytes::from_static(b"A3"),
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let summary = recover_from_object_store(op, "wal-mirror", &root)
            .await
            .unwrap();
        assert_eq!(summary.pulled, 3);
        assert!(
            root.join("acme").join(SEALED_DIR).join("s1.arrow").exists(),
            "acme's events segment must land in acme's WAL, not the default tenant's"
        );
        assert!(
            root.join("widgets")
                .join(SEALED_DIR)
                .join("s2.arrow")
                .exists(),
            "widgets' segment must land in widgets' WAL"
        );
        assert!(
            root.join("acme")
                .join("orders")
                .join(SEALED_DIR)
                .join("s3.arrow")
                .exists(),
            "an index segment must land in that index's WAL, not merged into events"
        );
        // And nothing may be flattened into a shared directory.
        assert!(
            !root.join(SEALED_DIR).exists(),
            "no segment may be written to a tenant-less sealed/ directory"
        );
    }

    /// #4972: a tenant is enumerated by its OWN `sealed/`
    /// (`list_layout_dirs`), which the ingester creates before it opens any
    /// per-index lane and calls the tenant discovery dir. A mirror holding
    /// only index segments for a tenant — Elasticsearch-bulk-only traffic
    /// whose events lane never sealed — restored to the right layout from the
    /// right prefix and was still never walked: the tenant did not exist as
    /// far as the drain was concerned.
    ///
    /// Against the pre-fix code this test FAILS on `list_tenant_dirs`.
    #[tokio::test]
    async fn recovery_rebuilds_the_tenant_discovery_dir_for_an_index_only_tenant() {
        use crate::durability::probe;

        let op = memory_op();
        op.write(
            "wal-mirror/acme/orders/s1.arrow",
            bytes::Bytes::from_static(b"A1"),
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        probe::record();
        let summary = recover_from_object_store(op, "wal-mirror", &root)
            .await
            .unwrap();
        let ops = probe::taken();
        assert_eq!(summary.pulled, 1);
        assert!(root
            .join("acme")
            .join("orders")
            .join(SEALED_DIR)
            .join("s1.arrow")
            .exists());

        assert_eq!(
            crate::list_tenant_dirs(&root)
                .unwrap()
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            vec!["acme".to_string()],
            "the drain enumerates the tenant it restored"
        );
        assert_eq!(
            crate::list_index_dirs(&root.join("acme"))
                .unwrap()
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            vec!["orders".to_string()],
            "and reaches the index directory beneath it"
        );

        // Same durability as the rest of the restore: the discovery dir is
        // created BEFORE the segment it makes reachable, and each new
        // component's parent is fsynced, so a power loss cannot leave the
        // segment under a directory whose own entry never reached the device.
        let tenant = ops
            .iter()
            .position(|op| op == "create_dir acme")
            .unwrap_or_else(|| panic!("{ops:?}"));
        assert_eq!(
            &ops[tenant..tenant + 4],
            &[
                "create_dir acme".to_string(),
                "sync_dir wal".to_string(),
                "create_dir sealed".to_string(),
                "sync_dir acme".to_string(),
            ],
            "the discovery dir is the FIRST thing the restore creates under the \
             tenant, and each component's parent is synced: {ops:?}"
        );
        assert!(
            ops[tenant + 4..].contains(&"rename s1.arrow".to_string()),
            "and the segment is published after it: {ops:?}"
        );
    }

    /// A restore that predates #4972 left the segments and not the discovery
    /// dir, and the only command an operator has is this one again — which
    /// takes the already-present skip on every segment. The repair therefore
    /// runs BEFORE that skip. It claims nothing it did not do: the re-run
    /// still reports zero pulled and one already present.
    #[tokio::test]
    async fn a_rerun_repairs_a_discovery_dir_an_earlier_restore_omitted() {
        let op = memory_op();
        op.write(
            "wal-mirror/acme/orders/s1.arrow",
            bytes::Bytes::from_static(b"A1"),
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        recover_from_object_store(op.clone(), "wal-mirror", &root)
            .await
            .unwrap();
        // The layout the old code produced: segments present, tenant invisible.
        std::fs::remove_dir(root.join("acme").join(SEALED_DIR)).unwrap();
        assert!(crate::list_tenant_dirs(&root).unwrap().is_empty());

        let rerun = recover_from_object_store(op, "wal-mirror", &root)
            .await
            .unwrap();
        assert_eq!(rerun.pulled, 0, "nothing is re-pulled or re-counted");
        assert_eq!(rerun.already_present, 1);
        assert_eq!(rerun.skipped, 0);
        assert_eq!(
            crate::list_tenant_dirs(&root)
                .unwrap()
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            vec!["acme".to_string()],
            "and the stranded segments become reachable"
        );
        assert_eq!(
            std::fs::read(
                root.join("acme")
                    .join("orders")
                    .join(SEALED_DIR)
                    .join("s1.arrow")
            )
            .unwrap(),
            b"A1",
            "the segment already on the volume is left exactly as it was"
        );
    }

    /// A discovery dir that cannot be made durable fails the restore rather
    /// than reporting segments whose reachability did not reach the device.
    /// The failure is before the download, so nothing is counted and the
    /// re-run — the only thing that can finish the restore — has everything
    /// left to do.
    #[tokio::test]
    async fn a_discovery_dir_that_cannot_be_made_durable_fails_the_restore() {
        use crate::durability::probe;

        let op = memory_op();
        op.write(
            "wal-mirror/acme/orders/s1.arrow",
            bytes::Bytes::from_static(b"A1"),
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        // The fsync of `acme/` that publishes the `sealed/` entry inside it.
        probe::fail(&["sync_dir acme"]);
        let err = recover_from_object_store(op.clone(), "wal-mirror", &root)
            .await
            .unwrap_err();
        probe::disarm();
        assert!(
            format!("{err:#}").contains("injected durability failure at `sync_dir acme`"),
            "{err:#}"
        );
        assert!(
            crate::list_sealed(&root.join("acme").join("orders"))
                .unwrap()
                .is_empty(),
            "no segment is published under a directory the drain may not reach"
        );

        let summary = recover_from_object_store(op, "wal-mirror", &root)
            .await
            .unwrap();
        assert_eq!(summary.pulled, 1);
        assert_eq!(summary.already_present, 0);
        assert_eq!(
            crate::list_tenant_dirs(&root)
                .unwrap()
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            vec!["acme".to_string()]
        );
    }

    /// An events segment's destination IS the tenant discovery dir, so the
    /// repair adds no directory of its own — a restore of `<tenant>/<seg>`
    /// must not invent an index directory or a second `sealed/`.
    #[tokio::test]
    async fn an_events_only_restore_grows_no_extra_directories() {
        let op = memory_op();
        op.write("wal-mirror/acme/s1.arrow", bytes::Bytes::from_static(b"A1"))
            .await
            .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        assert_eq!(
            recover_from_object_store(op, "wal-mirror", &root)
                .await
                .unwrap()
                .pulled,
            1
        );
        let mut children: Vec<String> = std::fs::read_dir(root.join("acme"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        children.sort();
        assert_eq!(children, vec![SEALED_DIR.to_string()]);
    }

    /// #4912: an operator ROOTED at the mirror passes an empty relative
    /// prefix, which is the only thing `siglake wal-recover` can pass — it
    /// builds the store from the whole `--from` URL. `format!("{prefix}/")`
    /// turned that into listing `"/"` and stripping `"/"` off relative keys,
    /// which fails for every entry, so the restore silently pulled nothing.
    ///
    /// Against that code this test FAILS (0 pulled).
    #[tokio::test]
    async fn recovery_accepts_an_empty_prefix_from_an_operator_rooted_at_the_mirror() {
        for prefix in ["", "/"] {
            let op = memory_op();
            // Keys as they appear relative to the mirror root: a tenant's
            // events segment, an index segment, and an active-mirror prefix.
            op.write("acme/s1.arrow", bytes::Bytes::from_static(b"A1"))
                .await
                .unwrap();
            op.write("acme/orders/s2.arrow", bytes::Bytes::from_static(b"A2"))
                .await
                .unwrap();
            op.write(
                "_active/widgets/s3.arrow.partial",
                bytes::Bytes::from_static(b"W3"),
            )
            .await
            .unwrap();

            let tmp = tempfile::tempdir().unwrap();
            let root = tmp.path().join("wal");
            let summary = recover_from_object_store(op.clone(), prefix, &root)
                .await
                .unwrap();
            assert_eq!(summary.pulled, 3, "prefix {prefix:?} restored nothing");
            assert!(root.join("acme").join(SEALED_DIR).join("s1.arrow").exists());
            assert!(root
                .join("acme")
                .join("orders")
                .join(SEALED_DIR)
                .join("s2.arrow")
                .exists());
            assert!(
                root.join("widgets")
                    .join(SEALED_DIR)
                    .join("s3.arrow")
                    .exists(),
                "an active-mirror object recovers as a sealed segment"
            );

            // Idempotent: a second pass re-pulls nothing it already has.
            let rerun = recover_from_object_store(op, prefix, &root).await.unwrap();
            assert_eq!(rerun.pulled, 0);
            assert_eq!(rerun.already_present, 3, "and says why it pulled nothing");
            assert_eq!(rerun.skipped, 0);
        }
    }

    /// Rooting the operator ABOVE the mirror — `--from …/store` when the
    /// segments are at `…/store/warehouse/wal-mirror/<tenant>/` — does not
    /// guess. The keys are deeper than the layout allows, so each one is
    /// refused and counted as skipped rather than filed under a tenant named
    /// `warehouse`. The empty-prefix listing is what makes those keys visible
    /// to the refusal at all; before #4912 they were dropped by the failed
    /// `strip_prefix` without a warning or a count.
    #[tokio::test]
    async fn recovery_refuses_keys_deeper_than_the_layout_instead_of_guessing() {
        let op = memory_op();
        op.write(
            "warehouse/wal-mirror/acme/s1.arrow",
            bytes::Bytes::from_static(b"A1"),
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let summary = recover_from_object_store(op, "", &root).await.unwrap();
        assert_eq!(
            summary.pulled, 0,
            "an ancestor of the mirror root restores nothing"
        );
        assert_eq!(
            summary.skipped, 1,
            "and the refusal is counted, not silent (#4928)"
        );
        assert_eq!(
            summary.sample_skipped_key.as_deref(),
            Some("warehouse/wal-mirror/acme/s1.arrow"),
            "one refused key is carried out verbatim for the operator's diagnostic"
        );
        assert_eq!(summary.already_present, 0);
        assert!(
            !root.join("warehouse").exists(),
            "a prefix component must not be taken for a tenant"
        );
    }

    /// The other half of #4912's convention: a NONEMPTY prefix still selects
    /// only what sits under it, so the uploader's unrooted operator and the
    /// library's callers keep working.
    #[tokio::test]
    async fn recovery_with_a_nonempty_prefix_ignores_keys_outside_it() {
        let op = memory_op();
        op.write("wal-mirror/acme/s1.arrow", bytes::Bytes::from_static(b"A1"))
            .await
            .unwrap();
        op.write(
            "other-prefix/acme/s2.arrow",
            bytes::Bytes::from_static(b"X"),
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let summary = recover_from_object_store(op, "wal-mirror", &root)
            .await
            .unwrap();
        assert_eq!(summary.pulled, 1);
        assert!(root.join("acme").join(SEALED_DIR).join("s1.arrow").exists());
        assert!(
            !root.join("acme").join(SEALED_DIR).join("s2.arrow").exists(),
            "a key outside the prefix must not be restored"
        );
    }

    /// The active mirror is what bounds the window a PVC loss can lose, and it
    /// had no consumer at all: recovery downloaded `_active/*.arrow.partial`
    /// objects and then nothing read them, because `list_sealed` filters on the
    /// `.arrow` extension. They recover AS sealed segments — an Arrow stream is
    /// readable without its EOS marker, which is the same basis on which a
    /// local partial is promoted.
    ///
    /// A sealed copy always wins: the active object is a flushed PREFIX of the
    /// same segment, so recovering both would duplicate its rows.
    #[tokio::test]
    async fn recovery_takes_active_partials_but_prefers_a_sealed_copy() {
        let op = memory_op();
        // Only ever mirrored while active — its writer died before sealing.
        op.write(
            "wal-mirror/_active/acme/lost.arrow.partial",
            bytes::Bytes::from_static(b"PARTIAL"),
        )
        .await
        .unwrap();
        // Present as BOTH: the sealed copy is the complete one.
        op.write(
            "wal-mirror/_active/acme/both.arrow.partial",
            bytes::Bytes::from_static(b"PREFIX"),
        )
        .await
        .unwrap();
        op.write(
            "wal-mirror/acme/both.arrow",
            bytes::Bytes::from_static(b"COMPLETE-SEALED"),
        )
        .await
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        let summary = recover_from_object_store(op, "wal-mirror", &root)
            .await
            .unwrap();
        assert_eq!(
            summary.pulled, 2,
            "two distinct segments, not three objects"
        );

        let sealed = root.join("acme").join(SEALED_DIR);
        assert!(
            sealed.join("lost.arrow").exists(),
            "an active-only segment must be recovered, or the active mirror is pointless"
        );
        assert_eq!(
            std::fs::read(sealed.join("both.arrow")).unwrap(),
            b"COMPLETE-SEALED",
            "the sealed copy must win over its active prefix"
        );
    }

    /// #3149: a restore reports segments an operator then plans around, so
    /// each one is whole on the device before it is counted. `std::fs::write`
    /// plus the already-present skip made that report page-cache-durable: a
    /// power loss could leave a truncated file under a name a re-run refuses
    /// to re-pull.
    #[tokio::test]
    async fn a_restored_segment_is_fsynced_under_its_final_name_before_it_is_counted() {
        use crate::durability::probe;

        let op = memory_op();
        op.write("wal-mirror/acme/s1.arrow", bytes::Bytes::from_static(b"A1"))
            .await
            .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        probe::record();
        let summary = recover_from_object_store(op, "wal-mirror", &root)
            .await
            .unwrap();
        let ops = probe::taken();
        assert_eq!(summary.pulled, 1);

        // The tail is the publish: temp, fsync, rename, directory fsync. The
        // head is the durable creation of the reconstructed layout.
        assert_eq!(
            &ops[ops.len() - 4..],
            &[
                "write s1.arrow.tmp".to_string(),
                "sync_file s1.arrow.tmp".to_string(),
                "rename s1.arrow".to_string(),
                "sync_dir sealed".to_string(),
            ],
            "{ops:?}"
        );
        assert!(
            ops.contains(&"create_dir sealed".to_string())
                && ops.contains(&"create_dir acme".to_string()),
            "the rebuilt layout is created durably too: {ops:?}"
        );
        assert_eq!(
            std::fs::read(root.join("acme").join(SEALED_DIR).join("s1.arrow")).unwrap(),
            b"A1"
        );
    }

    /// The failure is reported and leaves no final name, so the next run — the
    /// only thing that can finish the restore — re-pulls the segment instead
    /// of skipping a file it has no reason to trust.
    #[tokio::test]
    async fn a_restore_that_cannot_be_made_durable_fails_and_publishes_no_name() {
        use crate::durability::probe;

        let op = memory_op();
        op.write("wal-mirror/acme/s1.arrow", bytes::Bytes::from_static(b"A1"))
            .await
            .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("wal");
        probe::fail(&["sync_file s1.arrow.tmp"]);
        let err = recover_from_object_store(op.clone(), "wal-mirror", &root)
            .await
            .unwrap_err();
        probe::disarm();
        assert!(
            format!("{err:#}").contains("injected durability failure at `sync_file s1.arrow.tmp`"),
            "{err:#}"
        );
        let dest = root.join("acme").join(SEALED_DIR).join("s1.arrow");
        assert!(!dest.exists(), "no name is published for an unsynced body");

        // A leftover `.tmp` is invisible to the drain and does not block the
        // retry, which pulls the segment again and counts it this time.
        assert!(crate::list_sealed(&root.join("acme")).unwrap().is_empty());
        let summary = recover_from_object_store(op, "wal-mirror", &root)
            .await
            .unwrap();
        assert_eq!(summary.pulled, 1);
        assert_eq!(std::fs::read(&dest).unwrap(), b"A1");
    }

    const LIVE: &str = "11111111-1111-4111-8111-111111111111";
    const DROPPED: &str = "22222222-2222-4222-8222-222222222222";
    const OLDER: &str = "33333333-3333-4333-8333-333333333333";

    /// #2661/#2729: absent is no opinion, a single-line marker is the pre-#2729
    /// format and carries no history, and a re-stamp records what it displaced.
    #[tokio::test]
    async fn the_mirror_owner_marker_records_the_owners_it_displaced() {
        let op = memory_op();
        let classify = |expected: &'static str| {
            let op = op.clone();
            async move {
                classify_mirror_owner(&op, "wal-mirror", "default", "idx", expected)
                    .await
                    .unwrap()
            }
        };

        assert_eq!(
            classify(LIVE).await,
            MirrorOwner {
                state: crate::WalOwner::Unmarked,
                superseded: Vec::new()
            }
        );

        // The pre-#2729 body: one line, no history.
        op.write(
            &mirror_owner_key("wal-mirror", "default", "idx"),
            format!("{DROPPED}\n").into_bytes(),
        )
        .await
        .unwrap();
        assert_eq!(
            classify(DROPPED).await,
            MirrorOwner {
                state: crate::WalOwner::Owned,
                superseded: Vec::new()
            }
        );
        assert_eq!(
            classify(LIVE).await,
            MirrorOwner {
                state: crate::WalOwner::Stale(DROPPED.to_string()),
                superseded: Vec::new()
            }
        );

        restamp_mirror_owner(
            &op,
            "wal-mirror",
            "default",
            "idx",
            LIVE,
            &[DROPPED.to_string(), OLDER.to_string()],
        )
        .await
        .unwrap();
        assert_eq!(
            classify(LIVE).await,
            MirrorOwner {
                state: crate::WalOwner::Owned,
                superseded: vec![DROPPED.to_string(), OLDER.to_string()]
            },
            "the prefix names the live table AND remembers that it has named others"
        );

        // A plain stamp is for a prefix with no history to record.
        stamp_mirror_owner(&op, "wal-mirror", "default", "idx", LIVE)
            .await
            .unwrap();
        assert!(classify(LIVE).await.superseded.is_empty());
    }

    /// The history answers one yes/no question, so it is capped rather than
    /// grown without bound by an index that is recreated every day.
    #[tokio::test]
    async fn the_mirror_owner_history_is_bounded() {
        let op = memory_op();
        let long: Vec<String> = (0..40).map(|i| format!("dropped-{i}")).collect();
        restamp_mirror_owner(&op, "wal-mirror", "default", "idx", LIVE, &long)
            .await
            .unwrap();
        let marker = classify_mirror_owner(&op, "wal-mirror", "default", "idx", LIVE)
            .await
            .unwrap();
        assert_eq!(marker.state, crate::WalOwner::Owned);
        assert_eq!(marker.superseded.len(), MIRROR_OWNER_HISTORY);
        assert_eq!(marker.superseded[0], "dropped-0", "newest kept first");
    }
}

#[cfg(test)]
mod upload_retry_tests {
    use super::*;

    fn seg(suffix: &str) -> WalSegment {
        WalSegment {
            path: std::path::PathBuf::from(format!("/tmp/{suffix}")),
            mirror_key_suffix: suffix.to_string(),
            rows: 1,
            bytes: 1,
        }
    }

    #[test]
    fn jitter_is_bounded_and_decorrelated() {
        for a in 1..=MIRROR_UPLOAD_ATTEMPTS {
            assert!(
                upload_retry_jitter(&seg("a.arrow"), a) < std::time::Duration::from_millis(250)
            );
        }
        let a: Vec<_> = (1..=4)
            .map(|n| upload_retry_jitter(&seg("a.arrow"), n))
            .collect();
        let b: Vec<_> = (1..=4)
            .map(|n| upload_retry_jitter(&seg("b.arrow"), n))
            .collect();
        assert_ne!(
            a, b,
            "two nodes failing at the same instant must not share a retry schedule"
        );
    }

    #[test]
    fn jitter_is_deterministic_per_segment() {
        assert_eq!(
            upload_retry_jitter(&seg("x.arrow"), 2),
            upload_retry_jitter(&seg("x.arrow"), 2)
        );
        assert_ne!(
            upload_retry_jitter(&seg("x.arrow"), 2),
            upload_retry_jitter(&seg("x.arrow"), 3),
            "successive attempts must not reuse one delay"
        );
    }
}

/// Task #3787's measurement of what `pin_segment` costs a seal, part by part.
#[cfg(test)]
mod pin_cost_tests {
    use super::*;
    use crate::SEALED_DIR;

    /// #3787: where does the seal path's pin cost go, and what would batching
    /// the directory fsync recover?
    ///
    /// `pin_segment` does three things — resolve the segment's current local
    /// name, hard-link it into `mirror-pending/`, and fsync that directory so
    /// the new name survives a power loss. #3758 measured the three together
    /// (0.474 ms per seal at saturation, `docs/PERF_WAL_MIRROR_2026-09-11.md`
    /// "Re-measurement 2026-09-13") and could not say which part it was. This
    /// prices each one, and prices the alternative: `batch` links under one
    /// `sync_dir`, amortized per pin. The last column is what a barrier costs
    /// when the directory has nothing pending — the drain-side design, where
    /// the process about to recycle a sealed name syncs the pins first.
    ///
    /// Each iteration seals a segment the way `WalWriter::seal` does (body
    /// fsynced, renamed into `sealed/`, `sealed/` fsynced) before timing the
    /// pin, so the journal is in the state the pin actually meets.
    ///
    /// Run it on the filesystem the WAL lives on. On tmpfs every fsync here
    /// returns without reaching a device and the whole report reads zero.
    ///
    ///     TMPDIR=/var/tmp cargo test -p siglake-wal --lib \
    ///         report_pin_cost_breakdown -- --ignored --nocapture
    #[test]
    #[ignore = "measurement, not a gate"]
    fn report_pin_cost_breakdown() {
        use std::time::Instant;

        fn knob(name: &str, default: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }

        /// Mean / p50 / p90, in microseconds. Consumes the order.
        fn stats(samples: &mut [f64]) -> (f64, f64, f64) {
            samples.sort_by(|a, b| a.partial_cmp(b).expect("no NaN durations"));
            let mean = samples.iter().sum::<f64>() / samples.len() as f64;
            let at = |p: f64| samples[(((samples.len() - 1) as f64) * p).round() as usize];
            (mean, at(0.5), at(0.9))
        }

        fn since(t: Instant) -> f64 {
            t.elapsed().as_secs_f64() * 1e6
        }

        /// Lay a sealed segment down exactly as the seal path leaves it.
        fn seal_one(sealed: &Path, name: &str, body: &[u8]) -> PathBuf {
            let final_path = sealed.join(name);
            let tmp_path = sealed.join(format!("{name}.tmp"));
            {
                let mut f = std::fs::File::create(&tmp_path).unwrap();
                std::io::Write::write_all(&mut f, body).unwrap();
                crate::durability::sync_file(&f, &tmp_path).unwrap();
            }
            crate::durability::rename(&tmp_path, &final_path).unwrap();
            crate::durability::sync_dir(sealed).unwrap();
            final_path
        }

        let iters = knob("PIN_COST_ITERS", 200);
        let seg_bytes = knob("PIN_COST_SEGMENT_BYTES", 90 * 1024);
        // 164.8 MB over 1,819 segments in the saturation arm: ~90 KiB each.
        let body = vec![0x5au8; seg_bytes];

        println!(
            "\n#3787 pin cost: {iters} seals x {seg_bytes} B, tmpdir {} (fsync must reach a device)",
            std::env::temp_dir().display()
        );
        println!("all figures microseconds per pin\n");

        // Arm 0: the whole `pin_segment`, for a total to check the parts against.
        {
            let tmp = tempfile::tempdir().unwrap();
            let sealed = tmp.path().join(SEALED_DIR);
            std::fs::create_dir_all(&sealed).unwrap();
            let mut whole = Vec::with_capacity(iters);
            for i in 0..iters {
                let name = format!("ing-{i:06}.arrow");
                let path = seal_one(&sealed, &name, &body);
                let segment = WalSegment {
                    path,
                    rows: 1,
                    bytes: seg_bytes as u64,
                    mirror_key_suffix: name,
                };
                let t = Instant::now();
                pin_segment(&segment).unwrap();
                whole.push(since(t));
            }
            let (mean, p50, p90) = stats(&mut whole);
            println!("pin_segment, whole:  mean {mean:8.1}  p50 {p50:8.1}  p90 {p90:8.1}");
        }

        println!(
            "\n{:>5}  {:>10}  {:>10}  {:>12}  {:>12}  {:>12}",
            "batch", "lookup", "hard_link", "sync_dir/pin", "pin total", "idle barrier"
        );
        for batch in [1usize, 2, 4, 8, 16, 64] {
            let tmp = tempfile::tempdir().unwrap();
            let sealed = tmp.path().join(SEALED_DIR);
            std::fs::create_dir_all(&sealed).unwrap();
            let pending = tmp.path().join(MIRROR_PENDING_DIR);
            std::fs::create_dir_all(&pending).unwrap();
            crate::durability::sync_dir(tmp.path()).unwrap();

            let mut lookup = Vec::with_capacity(iters);
            let mut link = Vec::with_capacity(iters);
            let mut sync = Vec::with_capacity(iters / batch + 1);
            let mut idle = Vec::with_capacity(iters / batch + 1);
            let mut unsynced = 0usize;
            for i in 0..iters {
                let name = format!("ing-{i:06}.arrow");
                let path = seal_one(&sealed, &name, &body);
                let segment = WalSegment {
                    path,
                    rows: 1,
                    bytes: seg_bytes as u64,
                    mirror_key_suffix: name.clone(),
                };

                let t = Instant::now();
                let source = find_segment(&segment).expect("just sealed");
                lookup.push(since(t));

                let t = Instant::now();
                std::fs::hard_link(&source, pending.join(&name)).unwrap();
                link.push(since(t));

                unsynced += 1;
                if unsynced == batch {
                    let t = Instant::now();
                    crate::durability::sync_dir(&pending).unwrap();
                    sync.push(since(t) / batch as f64);
                    // Same directory, nothing pending: the floor under any
                    // design that syncs on someone else's schedule.
                    let t = Instant::now();
                    crate::durability::sync_dir(&pending).unwrap();
                    idle.push(since(t));
                    unsynced = 0;
                }
            }
            let (lookup_mean, ..) = stats(&mut lookup);
            let (link_mean, ..) = stats(&mut link);
            let (sync_mean, ..) = stats(&mut sync);
            let (idle_mean, ..) = stats(&mut idle);
            println!(
                "{batch:>5}  {lookup_mean:>10.1}  {link_mean:>10.1}  {sync_mean:>12.1}  {:>12.1}  {idle_mean:>12.1}",
                lookup_mean + link_mean + sync_mean
            );
        }
        println!();
    }
}
