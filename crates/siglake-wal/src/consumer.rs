//! Consuming WAL segments from outside siglake.
//!
//! siglake's ingest path writes every accepted event to a write-ahead log
//! before anything else touches it, and the compactor drains that log into
//! Iceberg. This module is the supported way for a SEPARATE process to read the
//! same stream — a detector, a router, a mirror into another system — without
//! reimplementing the parts that are easy to get wrong.
//!
//! It exists because siglake used to ship its own detection pipeline, which
//! consumed the WAL through these primitives directly. The pipeline moved out;
//! the interface it needed stayed, generalised, so that an arbitrary consumer
//! gets the same guarantees the in-tree one had.
//!
//! # What a consumer gets
//!
//! - **A durable position.** [`SegmentConsumer`] persists a cursor, so a
//!   restart resumes rather than re-reading the whole retention window. The
//!   cursor is published through an fsynced temp + rename with a sync of the
//!   state directory, so a power loss resumes from the last committed position
//!   rather than from whichever earlier one happened to reach the device.
//! - **Retention that waits.** Committing a segment publishes a watermark the
//!   compactor's sweep reads, so a segment is not deleted until every live
//!   consumer has passed it. Bounded: see "A slow consumer" below.
//! - **Visibility across the compactor's renames.** A segment moves
//!   `sealed/` → `processing/` → `committed/` while you read it. The listing
//!   de-dupes across all three, so a segment is never missed or seen twice
//!   because of a rename that happened mid-poll.
//! - **Integrity.** Reads verify the segment's CRC frame, so a truncated or
//!   corrupted segment is an error rather than silently short data.
//!
//! # Delivery semantics
//!
//! **At-least-once.** [`SegmentConsumer::commit`] advances the position AFTER
//! you have processed a segment. A crash between processing and commit
//! re-delivers that segment on restart. A consumer that must not double-count
//! needs its own idempotence — keyed on the segment name, which is stable. The
//! cursor's own durability narrows that replay window to the segments since the
//! last commit; it does not remove the idempotence duty.
//!
//! Segments are delivered in order. Names embed a UUIDv7, so lexicographic
//! order is time order, and the cursor is simply the last name processed.
//!
//! # A slow consumer
//!
//! The watermark holds retention open, so a consumer that stops committing
//! would grow the WAL without bound. It cannot: the sweep applies a hard
//! ceiling, and treats a watermark that has not moved for long enough as
//! absent. A dead consumer therefore stops holding data after the stale
//! window, and the guarantee degrades to "you missed some" rather than "the
//! ingester ran out of disk". Consumers that must not miss data should alert on
//! their own lag rather than rely on the WAL to wait forever.
//!
//! # Scope
//!
//! One `SegmentConsumer` reads ONE WAL directory. siglake's layout is
//! `<wal>/<tenant>/` and, for user indexes, `<wal>/<tenant>/<index>/`; use
//! [`crate::list_tenant_dirs`] and [`crate::list_index_dirs`] to enumerate them
//! and run a consumer per directory. Sharding across consumer replicas is the
//! consumer's business: give each replica a distinct `consumer_id` and have it
//! filter the records it does not own.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::{
    list_visible, publish_consumer_watermark, read_segment, ACTIVE_DIR, COMMITTED_DIR,
    CONSUMERS_DIR, ORPHANS_DIR, PROCESSING_DIR, SEALED_DIR,
};

/// WAL directories that actually hold segments, at or below `root`.
///
/// siglake's layout is `<wal>/<tenant>/` for the events index and
/// `<wal>/<tenant>/<index>/` for a user index, and the root ALSO contains empty
/// `active/ sealed/ processing/ committed/` stubs created at startup. So there
/// are several plausible paths to point a consumer at and only one is right for
/// any given stream — and choosing wrong is SILENT: [`crate::list_visible`] on
/// a directory whose `sealed/` is empty returns no segments and no error, so
/// the consumer runs, reports healthy, and processes nothing forever.
///
/// Measured on the 2026-08-30 200G round: a detector pointed at the WAL root
/// ran 489 clean cycles with `segments_pending 0` while 9,376 segments were
/// sealed one directory below it. Nothing in any metric distinguished that from
/// an idle system.
///
/// Use this to enumerate what is actually there and run one
/// [`SegmentConsumer`] per returned directory.
pub fn consumable_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    fn has_segments(dir: &Path) -> bool {
        list_visible(dir).map(|v| !v.is_empty()).unwrap_or(false)
    }
    fn walk(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
        if has_segments(dir) {
            out.push(dir.to_path_buf());
        }
        // Two levels is the whole layout: <root>/<tenant>/<index>.
        if depth == 0 {
            return;
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if !p.is_dir() {
                continue;
            }
            // The layout dirs are a segment directory's own contents, not
            // children to descend into.
            if p.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                matches!(
                    n,
                    ACTIVE_DIR
                        | SEALED_DIR
                        | PROCESSING_DIR
                        | COMMITTED_DIR
                        | ORPHANS_DIR
                        | CONSUMERS_DIR
                )
            }) {
                continue;
            }
            walk(&p, depth - 1, out);
        }
    }
    let mut out = Vec::new();
    walk(root, 2, &mut out);
    out.sort();
    Ok(out)
}

/// One segment available to a consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRef {
    /// File name, e.g. `01920f....arrow`. Stable, unique, and time-ordered —
    /// the right idempotence key for a consumer that needs one.
    pub name: String,
    /// Where it is right now. May already have been renamed by the compactor
    /// by the time you read it; [`SegmentConsumer::read`] handles that.
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CursorState {
    /// Name of the last segment fully processed. `None` = never run, so the
    /// first poll returns everything currently visible.
    last_segment_name: Option<String>,
    /// Diagnostic only.
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// A durable, retention-coordinated reader over one WAL directory.
///
/// ```no_run
/// # use siglake_wal::consumer::SegmentConsumer;
/// # fn main() -> anyhow::Result<()> {
/// let mut c = SegmentConsumer::open("my-detector", "/siglake/wal/default", "/var/lib/mine")?;
/// for seg in c.poll()? {
///     for batch in c.read(&seg)? {
///         // ... process the batch ...
///         let _ = batch;
///     }
///     c.commit(&seg)?; // AFTER processing: this is the at-least-once point
/// }
/// # Ok(())
/// # }
/// ```
pub struct SegmentConsumer {
    consumer_id: String,
    wal_dir: PathBuf,
    cursor_path: PathBuf,
    state: CursorState,
    misdirected: bool,
}

impl SegmentConsumer {
    /// Open (or create) a consumer.
    ///
    /// `consumer_id` identifies this consumer to the retention sweep and must
    /// be STABLE across restarts — a changing id looks like a new consumer that
    /// has never acknowledged anything, and leaves the old one's watermark to
    /// go stale. Distinct replicas need distinct ids.
    ///
    /// `state_dir` holds the cursor and must survive restarts; losing it means
    /// re-reading whatever is still in the retention window.
    pub fn open(
        consumer_id: impl Into<String>,
        wal_dir: impl AsRef<Path>,
        state_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let consumer_id = consumer_id.into();
        anyhow::ensure!(
            !consumer_id.is_empty()
                && consumer_id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "consumer_id must be non-empty [A-Za-z0-9_-] (it becomes a file name): {consumer_id:?}"
        );
        let state_dir = state_dir.as_ref();
        // Durable creation: a cursor is no more durable than the directory
        // entry that names its directory, and this one is new on first run.
        crate::durability::create_dir_all(state_dir)
            .with_context(|| format!("creating state dir {}", state_dir.display()))?;
        let cursor_path = state_dir.join("cursor.json");
        let state = if cursor_path.exists() {
            let bytes = std::fs::read(&cursor_path)
                .with_context(|| format!("reading {}", cursor_path.display()))?;
            serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing {}", cursor_path.display()))?
        } else {
            CursorState::default()
        };
        let wal_dir = wal_dir.as_ref().to_path_buf();
        // POINTED AT THE WRONG LEVEL is the failure this catches. It is silent
        // otherwise: an empty `sealed/` yields no segments and no error, so the
        // consumer looks perfectly healthy while doing nothing. Warn loudly and
        // publish a gauge rather than refusing, because a legitimately-empty
        // directory with busy children exists too (a tenant whose events index
        // is idle while a user index is not) — the caller decides.
        let misdirected = list_visible(&wal_dir).map(|v| v.is_empty()).unwrap_or(true)
            && consumable_dirs(&wal_dir)
                .map(|d| !d.is_empty())
                .unwrap_or(false);
        if misdirected {
            let found = consumable_dirs(&wal_dir).unwrap_or_default();
            tracing::error!(
                consumer = %consumer_id,
                wal_dir = %wal_dir.display(),
                candidates = ?found,
                "WAL directory holds NO segments, but directories below it do — this consumer \
                 will process nothing. Point it at one of the listed directories (one consumer \
                 each), or it will look healthy forever while ignoring the stream."
            );
        }
        metrics::gauge!(
            "siglake_wal_consumer_misdirected",
            "consumer" => consumer_id.clone()
        )
        .set(u8::from(misdirected) as f64);

        Ok(Self {
            consumer_id,
            wal_dir,
            cursor_path,
            state,
            misdirected,
        })
    }

    /// True if [`Self::open`] found no segments here but did find some below.
    /// A caller that would rather fail fast than run a no-op consumer can check
    /// this and exit.
    pub fn looks_misdirected(&self) -> bool {
        self.misdirected
    }

    /// Segments not yet committed by this consumer, oldest first.
    ///
    /// Cheap to call in a loop; it is a directory listing, not a read. Returns
    /// empty when there is nothing new.
    pub fn poll(&self) -> Result<Vec<SegmentRef>> {
        let mut out = Vec::new();
        for path in list_visible(&self.wal_dir)? {
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            // Names are UUIDv7-based, so lexicographic order IS time order and
            // "after the cursor" is a string comparison.
            if let Some(last) = self.state.last_segment_name.as_deref() {
                if name <= last {
                    continue;
                }
            }
            out.push(SegmentRef {
                name: name.to_string(),
                path: path.clone(),
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Decode a segment into Arrow batches, verifying its CRC.
    ///
    /// Tolerates the compactor renaming the file between [`Self::poll`] and
    /// here: it retries by name across `sealed/`, `processing/` and
    /// `committed/`. A segment that has been swept entirely (the consumer fell
    /// outside the retention window) is a clear error, not silent empty data.
    pub fn read(&self, segment: &SegmentRef) -> Result<Vec<RecordBatch>> {
        match read_segment(&segment.path) {
            Ok(batches) => Ok(batches),
            Err(first) => {
                for candidate in list_visible(&self.wal_dir)? {
                    if candidate.file_name().and_then(|s| s.to_str()) == Some(&segment.name) {
                        return read_segment(&candidate).with_context(|| {
                            format!("reading segment {} after rename", segment.name)
                        });
                    }
                }
                Err(first).with_context(|| {
                    format!(
                        "segment {} is no longer present in {} — it was swept before this \
                         consumer read it, which means the consumer fell outside the retention \
                         window",
                        segment.name,
                        self.wal_dir.display()
                    )
                })
            }
        }
    }

    /// Record that `segment` is fully processed.
    ///
    /// Call AFTER processing, never before: this is the at-least-once point.
    /// Advances the durable cursor and publishes the retention watermark, so
    /// the compactor will not reap anything up to and including this segment
    /// while this consumer is live.
    pub fn commit(&mut self, segment: &SegmentRef) -> Result<()> {
        self.commit_name(&segment.name)
    }

    /// [`Self::commit`] by name, for a consumer that processed a batch of
    /// segments and wants to acknowledge only the highest.
    pub fn commit_name(&mut self, name: &str) -> Result<()> {
        if self
            .state
            .last_segment_name
            .as_deref()
            .is_some_and(|last| name <= last)
        {
            // Never move backwards: a consumer that processes out of order
            // would otherwise re-open a window it had already closed.
            return Ok(());
        }
        let mut next_state = self.state.clone();
        next_state.last_segment_name = Some(name.to_string());
        next_state.updated_at = Some(chrono::Utc::now());
        self.persist(&next_state)?;
        self.state = next_state;
        // Best-effort: the cursor is what makes progress durable, and a missed
        // watermark only means retention is less patient than it could be.
        // Failing the commit here would re-deliver an already-processed
        // segment, which is the worse trade.
        if let Err(e) = publish_consumer_watermark(&self.wal_dir, &self.consumer_id, name) {
            tracing::warn!(
                consumer = %self.consumer_id,
                error = %e,
                "failed to publish WAL consumer watermark; retention will not wait for this \
                 consumer until the next successful commit"
            );
        }
        Ok(())
    }

    /// The last committed segment name, if any.
    pub fn position(&self) -> Option<&str> {
        self.state.last_segment_name.as_deref()
    }

    /// This consumer's id, as the retention sweep sees it.
    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }

    fn persist(&self, state: &CursorState) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(state)?;
        // Atomic AND durable (#3169): temp + rename means a crash mid-write
        // cannot leave a cursor that parses as an earlier position (or fails
        // to parse at all), and the fsyncs — the temp before the rename, the
        // state directory after it — mean a power loss cannot revert the
        // position to one an earlier commit wrote. Without them a reverted
        // cursor re-reads every segment since whichever write reached the
        // device, which after a busy interval is many segments, not one.
        //
        // The state directory can be on a different filesystem from the WAL,
        // so this syncs its own directory rather than relying on anything the
        // watermark's publish did.
        let tmp = self.cursor_path.with_extension("json.tmp");
        crate::durability::publish_file(&self.cursor_path, &tmp, &bytes)
            .with_context(|| format!("publishing {}", self.cursor_path.display()))
    }
}

#[cfg(test)]
mod cursor_durability_tests {
    use std::time::Duration;

    // The op log is thread-local and test-only, so the order these assert on
    // is only visible from inside the crate.
    use crate::durability::probe;

    use super::*;

    fn consumer(tmp: &Path) -> SegmentConsumer {
        let wal = tmp.join("wal");
        crate::create_wal_dir(&wal).unwrap();
        SegmentConsumer::open("detector", &wal, tmp.join("state")).unwrap()
    }

    #[test]
    fn opening_creates_the_state_directory_durably() {
        let tmp = tempfile::tempdir().unwrap();
        let dir_name = tmp
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        probe::record();
        let c = consumer(tmp.path());
        let ops = probe::taken();
        assert!(
            ops.windows(2).any(|w| w
                == [
                    "create_dir state".to_string(),
                    format!("sync_dir {dir_name}")
                ]),
            "the state directory's own entry is synced when it is created: {ops:?}"
        );
        assert_eq!(c.position(), None);
    }

    #[test]
    fn committing_publishes_the_cursor_before_the_watermark() {
        let tmp = tempfile::tempdir().unwrap();
        let mut c = consumer(tmp.path());

        probe::record();
        c.commit_name("01920f00.arrow").unwrap();
        assert_eq!(
            probe::taken(),
            vec![
                // The cursor: fsynced temp, rename, sync of `state/`.
                "write cursor.json.tmp".to_string(),
                "sync_file cursor.json.tmp".to_string(),
                "rename cursor.json".to_string(),
                "sync_dir state".to_string(),
                // Then the watermark, in its own directory.
                "create_dir consumers".to_string(),
                "sync_dir wal".to_string(),
                "write .detector.tmp".to_string(),
                "sync_file .detector.tmp".to_string(),
                "rename detector".to_string(),
                "sync_dir consumers".to_string(),
            ],
            "a committed position is durable before retention is allowed to act on it"
        );
        assert_eq!(c.position(), Some("01920f00.arrow"));
    }

    #[test]
    fn a_failed_cursor_publication_leaves_the_commit_retryable() {
        for failed_step in [
            "write cursor.json.tmp",
            "sync_file cursor.json.tmp",
            "rename cursor.json",
            "sync_dir state",
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let mut c = consumer(tmp.path());
            c.commit_name("01920f00.arrow").unwrap();

            probe::fail(&[failed_step]);
            let err = c.commit_name("01920f01.arrow").unwrap_err();
            probe::disarm();
            assert!(
                format!("{err:#}")
                    .contains(&format!("injected durability failure at `{failed_step}`")),
                "{err:#}"
            );

            // Keep the last successful in-memory position. In particular, a
            // retry of the same name must not take commit_name's no-op path.
            // A directory-sync failure occurs after rename, so the file may
            // already contain the staged position; no disk rollback is
            // promised for that case.
            assert_eq!(
                c.position(),
                Some("01920f00.arrow"),
                "failed at {failed_step}"
            );
            assert_eq!(
                crate::min_consumer_watermark(&tmp.path().join("wal"), Duration::from_secs(3600))
                    .unwrap()
                    .as_deref(),
                Some("01920f00.arrow"),
                "failed at {failed_step}"
            );

            probe::record();
            c.commit_name("01920f01.arrow").unwrap();
            let retry_ops = probe::taken();
            assert_eq!(
                &retry_ops[..4],
                [
                    "write cursor.json.tmp",
                    "sync_file cursor.json.tmp",
                    "rename cursor.json",
                    "sync_dir state",
                ],
                "the retry must publish the cursor again after {failed_step}: {retry_ops:?}"
            );
            assert_eq!(c.position(), Some("01920f01.arrow"));
            assert_eq!(
                consumer(tmp.path()).position(),
                Some("01920f01.arrow"),
                "failed at {failed_step}"
            );
        }
    }

    #[test]
    fn a_committed_position_survives_a_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let mut c = consumer(tmp.path());
        c.commit_name("01920f00.arrow").unwrap();
        c.commit_name("01920f01.arrow").unwrap();

        assert_eq!(consumer(tmp.path()).position(), Some("01920f01.arrow"));
    }
}
