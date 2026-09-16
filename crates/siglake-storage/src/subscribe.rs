//! Iceberg-incremental subscription primitive.
//!
//! Tail any siglake Iceberg table snapshot-by-snapshot. On each
//! `poll()`, returns rows that were added in commits the subscription
//! hasn't seen yet, advances the cursor, and stops. Long-running
//! consumers wrap this in their own polling loop with whatever
//! interval makes sense (a detector: 1s; UI dashboards: 5s; batch
//! ETL: minutes).
//!
//! Used by:
//!
//! - The `siglake subscribe` CLI subcommand for ad-hoc tailing.
//! - Any separate consumer process (a detector, a router, a mirror
//!   into another system) that wants committed rows from an Iceberg
//!   table rather than the earlier, pre-commit WAL stream. Any index
//!   works, including output tables a consumer declares for itself.
//!   `docs/CONSUMING_SEGMENTS.md` describes both paths and when to
//!   pick each.
//!
//! # Two delivery modes
//!
//! - **Snapshot-incremental** (Phase 4.12.11): after the first poll
//!   against an available snapshot, every subsequent poll walks the
//!   manifest list of any new snapshots and delivers *exactly* the
//!   rows added by those commits. Truly late-arriving rows
//!   (`time_column` < cursor_time but committed after cursor's snapshot)
//!   are now delivered — they were silently dropped in the
//!   time-filter-only mode that preceded this.
//! - **Time-filter bootstrap**: on the first poll against an available
//!   snapshot, the subscription has no snapshot anchor yet, so it
//!   falls back to a full scan + in-process `time_column > cursor`
//!   filter. This honors the `since` constructor's "skip backlog"
//!   semantics; snapshot-incremental mode starts with the *next* poll.
//!
//! # Snapshot-id fast path
//!
//! Each cursor tracks the table's `current_snapshot_id` from the last
//! poll. If the snapshot hasn't changed, `poll()` returns `[]`
//! without touching manifests or data files — the high-frequency
//! dispatcher polling loop no longer pays a full table scan per
//! cycle when the writer hasn't committed. The snapshot_id is also
//! serializable (just an `i64`), so a consumer that persists it can
//! resume exactly where it left off across restarts.
//!
//! # At-least-once semantics
//!
//! Each committed row is delivered to one `poll()` call. Re-runs of
//! `poll()` after an error return the same rows again, so consumers
//! must be idempotent (or track their own dedup via row keys). The
//! snapshot-incremental mode reads only files that were added in the
//! snapshot range; rows that already existed (`EXISTING` status) or
//! were rewritten by a compaction commit are not re-delivered.
//!
//! # Rewrite commits are advanced across, not delivered
//!
//! Compaction is continuous, so a poll interval routinely contains
//! commits that add data files holding rows the table *already had*:
//! the tier-2 re-clustering compactor merges files, retention drops
//! them, a delete task rewrites one without the deleted rows. All
//! three commit through iceberg's `rewrite_files`, which records
//! `Operation::Overwrite` and writes its replacements with `ADDED`
//! status — indistinguishable, entry by entry, from an append.
//! Delivering them would re-deliver already-delivered rows on every
//! compaction cycle and keep inflating downstream aggregates.
//!
//! So each snapshot in range is classified by its summary before any
//! of its files are read: `Operation::Append` delivers its added
//! files, anything else is skipped and the cursor advances past it.
//! An interval containing both kinds delivers exactly the appends' new
//! rows, including late-arriving ones. Skips are counted by
//! `siglake_subscription_rewrite_commits_skipped_total{table,origin}`.
//!
//! The classification reads only the operation and the marker property,
//! not the file counts. `rewrite_files` also accepts an add-only shape
//! (added files, no removals), and it commits `Operation::Overwrite`
//! like the other two shapes, so a marked add-only rewrite is skipped
//! as well, whatever its `deleted-data-files` says. Rows a subscriber
//! has to see therefore belong in a `fast_append`; this is the contract
//! stated in the fork's `iceberg::transaction::rewrite` module docs,
//! and every siglake `rewrite_files` call site passes removals.
//!
//! **External overwrite semantics are not supported.** siglake's own
//! rewrites carry the `siglake.rewrite` snapshot-summary property
//! (`origin="siglake"`), and none of them adds a row the subscription
//! has not already been offered. A non-append commit written by
//! *another* engine — Spark `INSERT OVERWRITE`, a `MERGE`, a
//! row-level delete — can add genuinely new rows, and those rows are
//! **not delivered**: it is skipped like any other rewrite, counted
//! with `origin="foreign"`, and logged at WARN. There is no way to
//! tell a replacement file from a new-row file inside such a commit,
//! and delivering it would reintroduce exactly the re-delivery this
//! guards against. If you need those rows, query the interval
//! (`POST /api/v1/sql`) — the same recovery a [`HistoryGap`] takes.
//! Rewrites siglake committed before the marker property existed also
//! classify as foreign; the treatment is identical, only the counter
//! label and the WARN differ.
//!
//! # History gaps
//!
//! Incremental delivery depends on the parent-link chain from the
//! table's current snapshot back to the cursor's snapshot. Snapshot
//! expiry (`SIGLAKE_SNAPSHOT_RETAIN_LAST`, default 100, swept every
//! 60s) removes old snapshots from table metadata, so a subscription
//! that was down for more than `retain_last` commits can find an
//! *intermediate* ancestor gone. Those commits can no longer be
//! enumerated, so `poll()` refuses: it returns [`HistoryGap`] and
//! advances neither cursor, rather than delivering the reachable
//! suffix and acknowledging the rest. The cursor's *own* snapshot may
//! expire safely — a retained child whose `parent_snapshot_id` names
//! the cursor proves continuity by itself.
//! `docs/CONSUMING_SEGMENTS.md` describes recovery.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    DataContentType, ManifestContentType, ManifestStatus, Operation, SnapshotRef, Summary,
};
use iceberg::table::Table;

use crate::iceberg::{IcebergContext, REWRITE_COMMIT_PROP};
use crate::query_provider::SiglakeStaticTableProvider;

/// A subscription cannot prove it has seen every commit between its
/// cursor and the table's current snapshot: some intermediate
/// ancestor's metadata is no longer retained (snapshot expiry), so the
/// commits in between cannot be enumerated.
///
/// Returned by [`IcebergSubscription::poll`] *before* any data is
/// read and *without* advancing either cursor, so an unseen interval
/// is never acknowledged. Consumers can recognize it with
/// `err.downcast_ref::<HistoryGap>()`; recovery is described in
/// `docs/CONSUMING_SEGMENTS.md`.
#[derive(Debug, Clone, thiserror::Error)]
#[error(
    "subscription history gap on table `{table}`: snapshot {missing_snapshot_id} \
     (parent of retained snapshot {child_snapshot_id}) is no longer retained, so the \
     commits between cursor snapshot {cursor_snapshot_id} and current snapshot \
     {current_snapshot_id} cannot be enumerated; the subscription cursor is unchanged \
     — see docs/CONSUMING_SEGMENTS.md for recovery"
)]
pub struct HistoryGap {
    pub table: String,
    /// The snapshot the subscription last acknowledged.
    pub cursor_snapshot_id: i64,
    /// The table's current snapshot at the time of the refused poll.
    pub current_snapshot_id: i64,
    /// The expired ancestor that broke the chain.
    pub missing_snapshot_id: i64,
    /// The retained snapshot whose `parent_snapshot_id` named it.
    pub child_snapshot_id: i64,
}

/// One node of the parent-link chain. Implemented for iceberg's
/// [`SnapshotRef`]; the test module supplies its own so the walk's
/// classification is testable without a warehouse.
trait SnapshotNode: Clone {
    fn id(&self) -> i64;
    fn parent(&self) -> Option<i64>;
}

impl SnapshotNode for SnapshotRef {
    fn id(&self) -> i64 {
        self.snapshot_id()
    }
    fn parent(&self) -> Option<i64> {
        self.parent_snapshot_id()
    }
}

/// What walking parent links backwards from the current snapshot found.
#[derive(Debug, PartialEq, Eq)]
enum Ancestry<S> {
    /// The cursor is reachable: these are the snapshots committed in
    /// `(cursor, current]`, newest first. Safe to deliver and ack.
    Reached(Vec<S>),
    /// A parent link named a snapshot that is no longer in the table
    /// metadata, and that snapshot is not the cursor. The interval
    /// between the cursor and `missing` is unknowable.
    Gap { missing: i64, child: i64 },
    /// The walk ran to a snapshot with no parent without meeting the
    /// cursor: the cursor is not an ancestor of current at all (e.g.
    /// the table was rolled back to a sibling branch, or the cursor
    /// belongs to another table). Delivering the whole retained
    /// history is an over-delivery, which at-least-once permits.
    CursorNotAncestor(Vec<S>),
}

/// Walk `current`'s parent links backwards looking for `cursor_snap`,
/// resolving each parent id through `lookup` (the table metadata).
///
/// Reaching the cursor id *through a parent link* is enough — a
/// retained child pointing at the cursor proves the chain is intact
/// even when the cursor's own snapshot metadata has been expired.
fn walk_ancestry<S: SnapshotNode>(
    current: S,
    cursor_snap: i64,
    lookup: impl Fn(i64) -> Option<S>,
) -> Ancestry<S> {
    let mut acc: Vec<S> = Vec::new();
    let mut seen: HashSet<i64> = HashSet::new();
    let mut node = current;
    loop {
        let id = node.id();
        if id == cursor_snap {
            return Ancestry::Reached(acc);
        }
        let parent = node.parent();
        acc.push(node);
        let Some(pid) = parent else {
            return Ancestry::CursorNotAncestor(acc);
        };
        if pid == cursor_snap {
            return Ancestry::Reached(acc);
        }
        // A repeated id means the parent links cycle, so the cursor is
        // unreachable however far we walk. Treat it like a break in the
        // chain rather than looping forever.
        if !seen.insert(pid) {
            return Ancestry::Gap {
                missing: pid,
                child: id,
            };
        }
        match lookup(pid) {
            Some(parent_snap) => node = parent_snap,
            None => {
                return Ancestry::Gap {
                    missing: pid,
                    child: id,
                }
            }
        }
    }
}

/// What an intervening commit did, as far as a subscription is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitKind {
    /// `Operation::Append`: the files it added hold rows the table did not
    /// hold before. Deliver them.
    Append,
    /// One of siglake's own row-rewrite commits — the tier-2 re-clustering
    /// compactor, retention's file drop, or a delete task — recognized by
    /// [`REWRITE_COMMIT_PROP`]. Its added files replace rows the table
    /// already held (and the subscription has already been offered), so they
    /// are skipped and the cursor advances across the commit.
    ///
    /// A marked rewrite that removes nothing (`rewrite_files`' add-only
    /// shape, `deleted-data-files = 0`) classifies here too and is skipped
    /// like the rest: new rows for a subscriber go through `fast_append`,
    /// per the fork's `iceberg::transaction::rewrite` contract.
    SiglakeRewrite,
    /// A non-append commit that siglake did not write: an external engine's
    /// `INSERT OVERWRITE` / `MERGE` / row-level delete, or a siglake rewrite
    /// committed before [`REWRITE_COMMIT_PROP`] existed. Treated exactly like
    /// [`Self::SiglakeRewrite`] — see the module docs for why, and for what
    /// that costs an external writer.
    ForeignOverwrite,
}

impl CommitKind {
    /// Whether the files this commit added are rows to deliver.
    fn delivers_added_files(self) -> bool {
        matches!(self, CommitKind::Append)
    }
}

/// Classify one snapshot from its summary.
///
/// Pure over the summary so the classification is testable without a
/// warehouse; the warehouse tests then prove siglake's writers produce the
/// summaries this expects.
fn classify_commit(summary: &Summary) -> CommitKind {
    match summary.operation {
        Operation::Append => CommitKind::Append,
        // Overwrite / Replace / Delete. Every one siglake produces comes from
        // `rewrite_files` (compaction re-cluster, retention, delete task) and
        // carries the marker property.
        Operation::Overwrite | Operation::Replace | Operation::Delete => {
            if summary
                .additional_properties
                .contains_key(REWRITE_COMMIT_PROP)
            {
                CommitKind::SiglakeRewrite
            } else {
                CommitKind::ForeignOverwrite
            }
        }
    }
}

/// Stateful subscription to one Iceberg table. See module docs.
pub struct IcebergSubscription {
    ice: Arc<IcebergContext>,
    table_name: String,
    time_column: String,
    cursor: DateTime<Utc>,
    /// `current_snapshot_id` observed on the last `poll()` that
    /// actually saw committed data. `None` until we've polled at
    /// least once against a non-empty table. Drives the fast-path
    /// short-circuit when nothing has been committed since AND the
    /// snapshot-incremental delivery mode for follow-up polls.
    cursor_snapshot_id: Option<i64>,
}

impl IcebergSubscription {
    /// Start a subscription at `since` (exclusive).
    pub fn new(
        ice: Arc<IcebergContext>,
        table_name: impl Into<String>,
        time_column: impl Into<String>,
        since: DateTime<Utc>,
    ) -> Self {
        Self {
            ice,
            table_name: table_name.into(),
            time_column: time_column.into(),
            cursor: since,
            cursor_snapshot_id: None,
        }
    }

    /// Resume from a previously persisted `(time_cursor, snapshot_id)`
    /// pair. The dispatcher (and any other long-running consumer)
    /// should call this with the values it last persisted so the
    /// snapshot-id fast path stays warm across restarts.
    pub fn resume(
        ice: Arc<IcebergContext>,
        table_name: impl Into<String>,
        time_column: impl Into<String>,
        cursor: DateTime<Utc>,
        cursor_snapshot_id: Option<i64>,
    ) -> Self {
        Self {
            ice,
            table_name: table_name.into(),
            time_column: time_column.into(),
            cursor,
            cursor_snapshot_id,
        }
    }

    pub fn cursor(&self) -> DateTime<Utc> {
        self.cursor
    }

    /// The Iceberg `current_snapshot_id` last observed on a non-empty
    /// poll. Persist this alongside [`Self::cursor`] for resume.
    pub fn cursor_snapshot_id(&self) -> Option<i64> {
        self.cursor_snapshot_id
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// One poll cycle. See module docs for the two delivery modes
    /// and the snapshot-id fast path.
    ///
    /// Fails with [`HistoryGap`] — leaving both cursors where they
    /// were — when snapshot expiry has removed an ancestor between
    /// the cursor's snapshot and the current one.
    pub async fn poll(&mut self) -> Result<Vec<RecordBatch>> {
        let table_ident = table_ident_for(&self.ice, &self.table_name)?;
        let table = self
            .ice
            .catalog()
            .load_table(&table_ident)
            .await
            .with_context(|| format!("load_table {table_ident} for poll"))?;
        self.poll_captured(table).await
    }

    /// Poll one already-captured table generation. Keeping capture separate
    /// from registration makes the bootstrap's read/ack generation binding
    /// explicit: commits after `table` was loaded belong to the next poll.
    async fn poll_captured(&mut self, table: Table) -> Result<Vec<RecordBatch>> {
        let current_snapshot_id = table.metadata().current_snapshot_id();
        metrics::counter!(
            "siglake_subscription_polls_total",
            "table" => self.table_name.clone()
        )
        .increment(1);
        if current_snapshot_id.is_some() && current_snapshot_id == self.cursor_snapshot_id {
            metrics::counter!(
                "siglake_subscription_fast_path_total",
                "table" => self.table_name.clone()
            )
            .increment(1);
            return Ok(vec![]);
        }
        let Some(current_snapshot_id) = current_snapshot_id else {
            // Table has no snapshots yet.
            return Ok(vec![]);
        };

        // Snapshot-incremental mode: a non-`None` cursor_snapshot_id
        // means we've seen at least one commit before, so we can
        // ask the manifests for *only* the rows added in commits
        // we haven't seen yet (true late-data delivery).
        if let Some(cursor_snap) = self.cursor_snapshot_id {
            metrics::counter!(
                "siglake_subscription_incremental_total",
                "table" => self.table_name.clone()
            )
            .increment(1);
            let batches = self.poll_incremental(&table, cursor_snap).await?;
            // Advance both cursors. The time cursor moves only forward
            // (to the largest timestamp we saw); the snapshot cursor
            // jumps to whatever's current.
            self.advance_cursors_from_batches(&batches, current_snapshot_id);
            return Ok(batches);
        }

        // Time-filter bootstrap mode: first poll, no prior snapshot
        // anchor. Full-scan + in-process `time_column > cursor`. The
        // snapshot cursor advances afterwards, so subsequent polls
        // use the incremental path above.
        metrics::counter!(
            "siglake_subscription_bootstrap_total",
            "table" => self.table_name.clone()
        )
        .increment(1);
        let batches = self.poll_full_scan_filtered(&table).await?;
        self.advance_cursors_from_batches(&batches, current_snapshot_id);
        Ok(batches)
    }

    /// Read every parquet file that was added by an *appending*
    /// snapshot in `(cursor_snap, current]` via the manifest list, and
    /// return its rows verbatim (no time filter — these rows are *by
    /// definition* new since the last poll). Rewrite commits in the
    /// range are advanced across without reading their replacement
    /// files. See module docs.
    async fn poll_incremental(
        &self,
        table: &iceberg::table::Table,
        cursor_snap: i64,
    ) -> Result<Vec<RecordBatch>> {
        let metadata = table.metadata();
        let file_io = table.file_io();

        let Some(current_snapshot) = metadata.current_snapshot() else {
            return Ok(vec![]);
        };

        // Establish continuity BEFORE reading anything: walk
        // parent_snapshot_id pointers backwards from current looking
        // for cursor_snap. The result is the set of snapshots
        // committed in (cursor_snap, current].
        let snapshots = match walk_ancestry(current_snapshot.clone(), cursor_snap, |pid| {
            metadata.snapshot_by_id(pid).cloned()
        }) {
            Ancestry::Reached(snaps) => snaps,
            // An expired intermediate ancestor: the commits between the
            // cursor and it are gone from metadata and cannot be
            // enumerated. Delivering the reachable suffix would let the
            // cursor advance past them, turning routine snapshot expiry
            // into silent data loss for the consumer. Refuse instead —
            // neither cursor moves, so a later poll (or a deliberate
            // re-bootstrap) still has the same choice to make.
            Ancestry::Gap { missing, child } => {
                metrics::counter!(
                    "siglake_subscription_history_gap_total",
                    "table" => self.table_name.clone()
                )
                .increment(1);
                return Err(HistoryGap {
                    table: self.table_name.clone(),
                    cursor_snapshot_id: cursor_snap,
                    current_snapshot_id: current_snapshot.snapshot_id(),
                    missing_snapshot_id: missing,
                    child_snapshot_id: child,
                }
                .into());
            }
            // The cursor isn't in this table's ancestry at all (e.g.
            // the table was rolled back to a sibling branch — siglake
            // doesn't do this today but iceberg allows it). Every
            // retained snapshot is delivered: an over-delivery rather
            // than a silent skip, which matches at-least-once.
            Ancestry::CursorNotAncestor(snaps) => snaps,
        };

        if snapshots.is_empty() {
            return Ok(vec![]);
        }

        let schema = metadata.current_schema().clone();
        let project_field_ids: Vec<i32> =
            schema.as_struct().fields().iter().map(|f| f.id).collect();

        // Walk each in-range snapshot's manifest list; for each
        // manifest *added by that snapshot*, load it and collect
        // ADDED data-file entries.
        let mut tasks: Vec<FileScanTask> = Vec::new();
        for snap in &snapshots {
            // A rewrite commit's added files are replacements for rows the
            // table already held, so reading them would re-deliver rows an
            // earlier poll already returned — steady-state behavior in a
            // continuously compacted table, and enough to keep inflating a
            // downstream aggregate for as long as compaction runs. Skip the
            // commit's files; the cursor still advances across it.
            let kind = classify_commit(snap.summary());
            if !kind.delivers_added_files() {
                let origin = match kind {
                    CommitKind::SiglakeRewrite => "siglake",
                    _ => "foreign",
                };
                metrics::counter!(
                    "siglake_subscription_rewrite_commits_skipped_total",
                    "table" => self.table_name.clone(),
                    "origin" => origin
                )
                .increment(1);
                if kind == CommitKind::ForeignOverwrite {
                    tracing::warn!(
                        table = %self.table_name,
                        snapshot_id = snap.snapshot_id(),
                        operation = snap.summary().operation.as_str(),
                        "subscription skipped a non-append commit it did not write: \
                         any rows it added are not delivered. External overwrite \
                         semantics are unsupported (see the subscribe module docs); \
                         siglake rewrites committed before the siglake.rewrite \
                         summary property existed also land here."
                    );
                }
                continue;
            }
            let manifest_list = snap
                .load_manifest_list(file_io, metadata)
                .await
                .with_context(|| format!("load_manifest_list snapshot={}", snap.snapshot_id()))?;
            for mf in manifest_list.entries() {
                if mf.added_snapshot_id != snap.snapshot_id() {
                    // This manifest was carried forward unchanged
                    // from an earlier snapshot — its rows aren't new.
                    continue;
                }
                if mf.content != ManifestContentType::Data {
                    // Skip delete manifests — siglake doesn't write
                    // deletes today, but if a future feature does,
                    // this guard keeps the subscription strictly
                    // append-flavored.
                    continue;
                }
                let manifest = mf
                    .load_manifest(file_io)
                    .await
                    .with_context(|| format!("load_manifest {}", mf.manifest_path))?;
                for entry in manifest.entries() {
                    if entry.status() != ManifestStatus::Added {
                        continue;
                    }
                    let data_file = entry.data_file();
                    if data_file.content_type() != DataContentType::Data {
                        continue;
                    }
                    tasks.push(FileScanTask {
                        file_size_in_bytes: data_file.file_size_in_bytes(),
                        start: 0,
                        length: data_file.file_size_in_bytes(),
                        record_count: Some(data_file.record_count()),
                        data_file_path: data_file.file_path().to_string(),
                        data_file_format: data_file.file_format(),
                        schema: schema.clone(),
                        project_field_ids: project_field_ids.clone(),
                        predicate: None,
                        deletes: vec![],
                        partition: None,
                        partition_spec: None,
                        name_mapping: None,
                        case_sensitive: true,
                        statistics_blobs: vec![],
                    });
                }
            }
        }

        if tasks.is_empty() {
            return Ok(vec![]);
        }

        let task_stream =
            futures::stream::iter(tasks.into_iter().map(Ok::<_, iceberg::Error>)).boxed();
        let reader = ArrowReaderBuilder::new(file_io.clone()).build();
        let mut arrow_stream = reader.read(task_stream).context("ArrowReader::read")?;

        let mut out = Vec::new();
        while let Some(batch) = arrow_stream.next().await {
            out.push(batch.context("incremental scan batch")?);
        }
        Ok(out)
    }

    /// First-poll bootstrap: register the whole table with DataFusion,
    /// scan it, filter in-process by `time_column > cursor`. Used
    /// once per subscription to honor the `since` argument's "skip
    /// backlog" semantics; subsequent polls take the incremental
    /// path.
    async fn poll_full_scan_filtered(&self, table: &Table) -> Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        register_table_for_subscription(&ctx, &self.table_name, table).await?;
        let df = ctx
            .table(self.table_name.as_str())
            .await
            .with_context(|| format!("loading table {} for poll", self.table_name))?;
        let batches = df.collect().await.context("collect subscription batches")?;

        let cursor_ns = self
            .cursor
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("cursor out of nanosecond range: {}", self.cursor))?;

        let mut out: Vec<RecordBatch> = Vec::new();
        for batch in &batches {
            // Nanosecond-exact: a subscription cursor that advanced to a
            // microsecond-truncated instant would re-deliver every row inside
            // that microsecond on the next poll.
            let time_column =
                siglake_core::nanos_source_column(batch.schema().as_ref(), &self.time_column);
            let ts = batch
                .column_by_name(time_column)
                .with_context(|| {
                    format!(
                        "table {} missing time column `{}`",
                        self.table_name, self.time_column
                    )
                })
                .and_then(|c| {
                    siglake_core::column_nanos(c).with_context(|| {
                        format!(
                            "table {} column `{}` is not a timestamp or an ns long",
                            self.table_name, time_column
                        )
                    })
                })?;
            let mask_vals: Vec<bool> = (0..ts.len()).map(|i| ts.value(i) > cursor_ns).collect();
            let mask = arrow_array::BooleanArray::from(mask_vals);
            let filtered =
                arrow::compute::filter_record_batch(batch, &mask).context("filter_record_batch")?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
        Ok(out)
    }

    /// Move both cursors forward after a successful poll. The time
    /// cursor advances to the largest `time_column` value present in
    /// the returned batches (if any); the snapshot cursor advances to
    /// whatever the table's current snapshot is, even when zero rows
    /// passed our filter — that way the next poll short-circuits
    /// against the same snapshot.
    fn advance_cursors_from_batches(&mut self, batches: &[RecordBatch], current_snapshot_id: i64) {
        let cursor_ns = self.cursor.timestamp_nanos_opt().unwrap_or(0);
        let mut max_seen = cursor_ns;
        for batch in batches {
            let time_column =
                siglake_core::nanos_source_column(batch.schema().as_ref(), &self.time_column);
            let Some(col) = batch.column_by_name(time_column) else {
                continue;
            };
            let Some(ts) = siglake_core::column_nanos(col) else {
                continue;
            };
            for i in 0..ts.len() {
                if ts.value(i) > max_seen {
                    max_seen = ts.value(i);
                }
            }
        }
        if max_seen > cursor_ns {
            self.cursor = chrono::DateTime::from_timestamp_nanos(max_seen);
        }
        self.cursor_snapshot_id = Some(current_snapshot_id);
    }
}

fn table_ident_for(ice: &IcebergContext, table_name: &str) -> Result<iceberg::TableIdent> {
    Ok(match table_name {
        "events" => ice.table_ident().clone(),
        // Any index, by id. An index table's identifier IS its id in the
        // namespace, which is what the removed detection-table accessors
        // returned too.
        other => ice.index_table_ident(other),
    })
}

/// Register the subscribed table with DataFusion.
///
/// `events` is the built-in; ANY user index works by name. It used to be a
/// closed list with four detection tables hardcoded into it, which meant
/// subscriptions were a privilege of siglake's own consumer. They are now a
/// property of being an index, so a consumer that declares its own output
/// tables can tail them too.
async fn register_table_for_subscription(
    ctx: &SessionContext,
    table_name: &str,
    table: &Table,
) -> Result<()> {
    if crate::index_manager::index_config_from_table(table_name, table)?.is_none() {
        anyhow::bail!(
            "unknown subscription table `{table_name}`; expected `events` or an index id"
        );
    }
    let provider = SiglakeStaticTableProvider::try_new_from_table(table.clone())
        .await
        .with_context(|| format!("build subscription provider for {table_name}"))?;
    ctx.register_table(table_name, Arc::new(provider))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use siglake_core::index_config::IndexConfig;
    use siglake_core::Event;

    async fn fresh_ice(tmp: &tempfile::TempDir) -> Arc<IcebergContext> {
        let warehouse = tmp.path().join("warehouse");
        Arc::new(IcebergContext::open(&warehouse).await.unwrap())
    }

    fn mk_event(secs: i64, host: &str) -> Event {
        Event {
            timestamp: chrono::Utc.timestamp_opt(secs, 0).single().unwrap(),
            host: host.into(),
            source: "src".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: format!("e@{secs} {host}"),
            attributes: None,
        }
    }

    async fn append_index_events(ice: &IcebergContext, index_id: &str, events: &[Event]) {
        let batch = siglake_core::events_to_record_batch(events).unwrap();
        ice.append_to_table(&ice.index_table_ident(index_id), batch, &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn bootstrap_events_reads_the_fresh_catalog_generation_not_a_warm_provider() {
        let tmp = tempfile::tempdir().unwrap();
        let subscriber = fresh_ice(&tmp).await;
        let writer = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

        writer
            .append_events(&[mk_event(t0.timestamp() - 1, "s0")])
            .await
            .unwrap();
        let warm_ctx = SessionContext::new();
        subscriber
            .register_with_datafusion(&warm_ctx)
            .await
            .unwrap();
        let s0 = subscriber
            .table_snapshot_id("events")
            .await
            .unwrap()
            .unwrap();

        writer
            .append_events(&[mk_event(t0.timestamp() + 1, "s1")])
            .await
            .unwrap();
        assert_eq!(
            subscriber.table_snapshot_id("events").await.unwrap(),
            Some(s0),
            "the subscriber fixture must still hold its S0 provider"
        );
        let s1 = subscriber
            .catalog()
            .load_table(subscriber.table_ident())
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id()
            .unwrap();
        assert_ne!(s1, s0);

        let mut sub = IcebergSubscription::new(subscriber.clone(), "events", "timestamp", t0);
        let batches = sub.poll().await.unwrap();
        assert_eq!(hosts_in(&batches), vec!["s1"]);
        assert_eq!(sub.cursor_snapshot_id(), Some(s1));

        // The bootstrap's since filter still skips S0, while a row committed
        // later than S1 is incremental even when its event time is older.
        writer
            .append_events(&[mk_event(t0.timestamp() - 2, "late")])
            .await
            .unwrap();
        assert_eq!(hosts_in(&sub.poll().await.unwrap()), vec!["late"]);
    }

    #[tokio::test]
    async fn bootstrap_managed_index_reads_the_fresh_catalog_generation() {
        let tmp = tempfile::tempdir().unwrap();
        let subscriber = fresh_ice(&tmp).await;
        let index_id = "subscription_index";
        let mut config = IndexConfig::builtin_events();
        config.index_id = index_id.to_string();
        subscriber.create_index(&config).await.unwrap();
        let writer = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

        append_index_events(&writer, index_id, &[mk_event(t0.timestamp() - 1, "s0")]).await;
        let warm_ctx = SessionContext::new();
        assert!(subscriber
            .register_index_with_datafusion(&warm_ctx, index_id)
            .await
            .unwrap());
        let s0 = subscriber
            .table_snapshot_id(index_id)
            .await
            .unwrap()
            .unwrap();

        append_index_events(&writer, index_id, &[mk_event(t0.timestamp() + 1, "s1")]).await;
        assert_eq!(
            subscriber.table_snapshot_id(index_id).await.unwrap(),
            Some(s0),
            "the subscriber fixture must still hold its S0 index provider"
        );
        let s1 = subscriber
            .catalog()
            .load_table(&subscriber.index_table_ident(index_id))
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id()
            .unwrap();

        let mut sub = IcebergSubscription::new(subscriber, index_id, "timestamp", t0);
        let batches = sub.poll().await.unwrap();
        assert_eq!(hosts_in(&batches), vec!["s1"]);
        assert_eq!(sub.cursor_snapshot_id(), Some(s1));
    }

    #[tokio::test]
    async fn commit_between_capture_and_registration_is_delivered_next_poll() {
        let tmp = tempfile::tempdir().unwrap();
        let subscriber = fresh_ice(&tmp).await;
        let writer = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        writer
            .append_events(&[mk_event(t0.timestamp(), "captured")])
            .await
            .unwrap();

        let captured = subscriber
            .catalog()
            .load_table(subscriber.table_ident())
            .await
            .unwrap();
        let captured_snapshot = captured.metadata().current_snapshot_id().unwrap();
        writer
            .append_events(&[mk_event(t0.timestamp() - 3600, "late_commit")])
            .await
            .unwrap();

        let mut sub = IcebergSubscription::new(
            subscriber,
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        let first = sub.poll_captured(captured).await.unwrap();
        assert_eq!(hosts_in(&first), vec!["captured"]);
        assert_eq!(sub.cursor_snapshot_id(), Some(captured_snapshot));

        let second = sub.poll().await.unwrap();
        assert_eq!(
            hosts_in(&second),
            vec!["late_commit"],
            "the post-capture commit must remain incremental despite its older event time"
        );
    }

    #[tokio::test]
    async fn bootstrap_read_failure_leaves_both_cursors_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        ice.append_events(&[mk_event(t0.timestamp(), "unreadable")])
            .await
            .unwrap();
        let captured = ice.catalog().load_table(ice.table_ident()).await.unwrap();
        let data_file = ice
            .live_data_files(ice.table_ident())
            .await
            .unwrap()
            .remove(0);
        let path = data_file
            .file_path()
            .split_once("://")
            .map(|(_, path)| path)
            .unwrap_or_else(|| data_file.file_path());
        std::fs::remove_file(path).unwrap();

        let since = t0 - chrono::Duration::seconds(1);
        let mut sub = IcebergSubscription::new(ice, "events", "timestamp", since);
        let err = sub.poll_captured(captured).await.unwrap_err();
        assert!(format!("{err:#}").contains("collect subscription batches"));
        assert_eq!(sub.cursor(), since);
        assert_eq!(sub.cursor_snapshot_id(), None);
    }

    #[tokio::test]
    async fn polls_only_new_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;

        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        // Seed 5 events at t0..t0+4s.
        let batch_a: Vec<Event> = (0..5).map(|i| mk_event(t0.timestamp() + i, "h1")).collect();
        ice.append_events(&batch_a).await.unwrap();

        let mut sub = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        let batches = sub.poll().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5, "should see all 5 seeded rows on first poll");

        // Second poll: nothing new.
        let batches2 = sub.poll().await.unwrap();
        let total2: usize = batches2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total2, 0);

        // Append 3 more events at t0+10s..t0+12s; only those should
        // come back on the next poll.
        let batch_b: Vec<Event> = (10..13)
            .map(|i| mk_event(t0.timestamp() + i, "h2"))
            .collect();
        ice.append_events(&batch_b).await.unwrap();

        let batches3 = sub.poll().await.unwrap();
        let total3: usize = batches3.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total3, 3);
    }

    #[tokio::test]
    async fn cursor_advances_to_max_seen() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

        ice.append_events(&[
            mk_event(t0.timestamp(), "h1"),
            mk_event(t0.timestamp() + 5, "h1"),
            mk_event(t0.timestamp() + 10, "h1"),
        ])
        .await
        .unwrap();

        let mut sub = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        let _ = sub.poll().await.unwrap();
        assert_eq!(sub.cursor(), t0 + chrono::Duration::seconds(10));
    }

    #[tokio::test]
    async fn snapshot_id_advances_after_poll() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        ice.append_events(&[mk_event(t0.timestamp(), "h1")])
            .await
            .unwrap();

        let mut sub = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        assert!(sub.cursor_snapshot_id().is_none(), "starts at None");
        let _ = sub.poll().await.unwrap();
        let snap_after = sub.cursor_snapshot_id();
        assert!(snap_after.is_some(), "advances to the table's snapshot id");

        // A second poll against the same snapshot must short-circuit
        // and not change the snapshot cursor.
        let _ = sub.poll().await.unwrap();
        assert_eq!(sub.cursor_snapshot_id(), snap_after);

        // A new commit must advance the snapshot cursor.
        ice.append_events(&[mk_event(t0.timestamp() + 10, "h1")])
            .await
            .unwrap();
        let _ = sub.poll().await.unwrap();
        let snap_after2 = sub.cursor_snapshot_id();
        assert!(snap_after2.is_some());
        assert_ne!(snap_after2, snap_after);
    }

    #[tokio::test]
    async fn resume_from_persisted_snapshot_id() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        ice.append_events(&[
            mk_event(t0.timestamp(), "h1"),
            mk_event(t0.timestamp() + 5, "h2"),
        ])
        .await
        .unwrap();

        // Sub 1: poll once, capture both cursors.
        let mut sub1 = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        let _ = sub1.poll().await.unwrap();
        let snap_at_pause = sub1.cursor_snapshot_id();
        let time_at_pause = sub1.cursor();

        // Sub 2: resumed from the persisted state. A poll against the
        // unchanged table must short-circuit and return empty.
        let mut sub2 = IcebergSubscription::resume(
            ice.clone(),
            "events",
            "timestamp",
            time_at_pause,
            snap_at_pause,
        );
        let batches = sub2.poll().await.unwrap();
        assert!(batches.is_empty(), "resume short-circuits when nothing new");
    }

    #[tokio::test]
    async fn rejects_unknown_table() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let mut sub = IcebergSubscription::new(
            ice,
            "no_such_table",
            "timestamp",
            chrono::Utc::now() - chrono::Duration::days(1),
        );
        let err = sub.poll().await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("no_such_table"), "got {msg}");
    }

    /// The headline Phase 4.12.11 case: data that lands in a snapshot
    /// committed *after* the cursor's snapshot but with a `time_column`
    /// value *earlier* than the cursor's time was silently dropped by
    /// the time-filter-only path. With manifest-walk delivery, it
    /// shows up on the next poll.
    #[tokio::test]
    async fn late_arriving_rows_are_delivered() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t_recent = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 12, 0, 0).unwrap();

        // First commit: a few recent events. Subscription bootstraps
        // and observes them.
        ice.append_events(&[
            mk_event(t_recent.timestamp(), "h1"),
            mk_event(t_recent.timestamp() + 10, "h1"),
        ])
        .await
        .unwrap();

        let mut sub = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t_recent - chrono::Duration::seconds(1),
        );
        let first = sub.poll().await.unwrap();
        let n_first: usize = first.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n_first, 2);

        // Now a *late-arriving* commit: timestamps from 1 hour earlier
        // than the cursor. Without snapshot-incremental delivery these
        // rows would be dropped (time filter `> cursor` rejects them).
        let t_late = t_recent - chrono::Duration::hours(1);
        ice.append_events(&[
            mk_event(t_late.timestamp(), "h_late"),
            mk_event(t_late.timestamp() + 5, "h_late"),
            mk_event(t_late.timestamp() + 10, "h_late"),
        ])
        .await
        .unwrap();

        let late = sub.poll().await.unwrap();
        let n_late: usize = late.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            n_late, 3,
            "late-arriving rows must be delivered via the manifest walk"
        );

        // And the next poll is empty again.
        let empty = sub.poll().await.unwrap();
        assert!(
            empty.iter().all(|b| b.num_rows() == 0),
            "follow-up poll returns nothing new"
        );
    }

    /// A second incremental delivery — a third commit appends more
    /// rows, and the subscription delivers ONLY those, not the
    /// earlier ones. Guards against "incremental mode re-reads
    /// everything" regressions.
    #[tokio::test]
    async fn incremental_delivery_doesnt_redeliver_prior_commits() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

        ice.append_events(&[mk_event(t0.timestamp(), "h1")])
            .await
            .unwrap();
        let mut sub = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        assert_eq!(
            sub.poll()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            1
        );

        // Three subsequent commits, each one row. Each poll must
        // deliver only the new rows from the most recent commit.
        ice.append_events(&[mk_event(t0.timestamp() + 1, "h2")])
            .await
            .unwrap();
        assert_eq!(
            sub.poll()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            1
        );

        ice.append_events(&[mk_event(t0.timestamp() + 2, "h3")])
            .await
            .unwrap();
        ice.append_events(&[mk_event(t0.timestamp() + 3, "h4")])
            .await
            .unwrap();
        // Two commits since last poll — both should deliver in one go.
        let last = sub.poll().await.unwrap();
        let n_last: usize = last.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n_last, 2, "manifest walk covers all snapshots since cursor");
    }

    /// Snapshot expiry ate an ancestor *between* the cursor and the
    /// current snapshot: the commits in that hole can't be enumerated,
    /// so the poll must refuse rather than deliver the reachable
    /// suffix and quietly ack the rest.
    #[tokio::test]
    async fn history_gap_refuses_and_leaves_both_cursors_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

        // Commit 1, then bootstrap the subscription against it.
        ice.append_events(&[mk_event(t0.timestamp(), "h1")])
            .await
            .unwrap();
        let mut sub = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        assert_eq!(
            sub.poll()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            1
        );
        let cursor_at_pause = sub.cursor();
        let snap_at_pause = sub.cursor_snapshot_id().expect("bootstrap set a snapshot");

        // Three more commits while the consumer is down.
        for i in 1..4 {
            ice.append_events(&[mk_event(t0.timestamp() + i, "h2")])
                .await
                .unwrap();
        }

        // Retention keeps only the two most recent snapshots, so both
        // the cursor's snapshot AND the commit right after it are gone
        // from metadata — a hole, not just an expired anchor.
        let expired = ice
            .expire_snapshots(&ice.events_table_ident().clone(), 2)
            .await
            .unwrap();
        assert_eq!(expired, 2, "commits 1 and 2 expire, 3 and 4 are retained");

        let err = sub.poll().await.unwrap_err();
        let gap = err
            .downcast_ref::<HistoryGap>()
            .unwrap_or_else(|| panic!("expected a HistoryGap, got {err:#}"));
        assert_eq!(gap.table, "events");
        assert_eq!(gap.cursor_snapshot_id, snap_at_pause);
        assert_ne!(
            gap.missing_snapshot_id, snap_at_pause,
            "the hole is an intermediate ancestor, not the cursor itself"
        );

        // Nothing was acknowledged: both cursors sit exactly where the
        // last successful poll left them, so the missing interval is
        // still visibly missing on every subsequent poll.
        assert_eq!(sub.cursor(), cursor_at_pause);
        assert_eq!(sub.cursor_snapshot_id(), Some(snap_at_pause));
        let again = sub.poll().await.unwrap_err();
        assert!(again.downcast_ref::<HistoryGap>().is_some());
        assert_eq!(sub.cursor_snapshot_id(), Some(snap_at_pause));
    }

    /// The cursor's own snapshot expiring is NOT a gap: a retained
    /// child whose `parent_snapshot_id` names the cursor proves the
    /// chain is intact, and delivery continues from there.
    #[tokio::test]
    async fn expired_cursor_snapshot_still_delivers_via_parent_link() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

        ice.append_events(&[mk_event(t0.timestamp(), "h1")])
            .await
            .unwrap();
        ice.append_events(&[mk_event(t0.timestamp() + 1, "h2")])
            .await
            .unwrap();
        let mut sub = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        assert_eq!(
            sub.poll()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            2
        );
        let snap_at_pause = sub.cursor_snapshot_id().unwrap();

        // One more commit, then expire everything but it — the cursor's
        // snapshot is gone, but the new snapshot's parent link names it.
        ice.append_events(&[mk_event(t0.timestamp() + 2, "h3")])
            .await
            .unwrap();
        let expired = ice
            .expire_snapshots(&ice.events_table_ident().clone(), 1)
            .await
            .unwrap();
        assert_eq!(expired, 2, "only the current snapshot is retained");

        let batches = sub.poll().await.unwrap();
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 1, "only the one commit after the cursor is delivered");
        assert_ne!(
            sub.cursor_snapshot_id(),
            Some(snap_at_pause),
            "a proven-contiguous poll still advances the snapshot cursor"
        );
    }

    /// The headline case for this module's no-compaction-replay promise:
    /// re-clustering rewrites the table's files with no ingest at all, and
    /// the subscription must return nothing — the replacement files hold
    /// rows it already delivered. A later append is still delivered, and
    /// only that append.
    #[tokio::test]
    async fn compaction_rewrite_delivers_no_rows() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let ident = ice.events_table_ident().clone();

        // Two appends ⇒ two data files for the re-cluster to merge.
        ice.append_events(&[mk_event(t0.timestamp(), "h1")])
            .await
            .unwrap();
        ice.append_events(&[mk_event(t0.timestamp() + 1, "h2")])
            .await
            .unwrap();

        let mut sub = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        assert_eq!(row_count(&sub.poll().await.unwrap()), 2);
        let snap_before = sub.cursor_snapshot_id().unwrap();
        let cursor_before = sub.cursor();

        // Compact with no ingest in between.
        let files = ice.live_data_files(&ident).await.unwrap();
        assert_eq!(files.len(), 2, "two appends, two files");
        let stats = ice.recluster_files(&ident, files, &[]).await.unwrap();
        assert_eq!(stats.files_added, 1, "merged into one replacement file");

        let after_compaction = sub.poll().await.unwrap();
        assert_eq!(
            row_count(&after_compaction),
            0,
            "a rewrite commit re-delivers nothing"
        );
        assert_eq!(sub.cursor(), cursor_before, "time cursor does not move");
        assert_ne!(
            sub.cursor_snapshot_id(),
            Some(snap_before),
            "but the subscription advances across the rewrite"
        );

        // The next real append is delivered, and only it.
        ice.append_events(&[mk_event(t0.timestamp() + 2, "h3")])
            .await
            .unwrap();
        assert_eq!(row_count(&sub.poll().await.unwrap()), 1);
        assert_eq!(row_count(&sub.poll().await.unwrap()), 0);
    }

    /// One poll interval spanning both kinds of commit: the appended rows
    /// are delivered exactly once and the rewrite's replacement file — which
    /// contains those same rows plus the earlier ones — is not.
    #[tokio::test]
    async fn poll_interval_with_both_append_and_rewrite() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 12, 0, 0).unwrap();
        let ident = ice.events_table_ident().clone();

        ice.append_events(&[mk_event(t0.timestamp(), "h1")])
            .await
            .unwrap();
        let mut sub = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        assert_eq!(row_count(&sub.poll().await.unwrap()), 1);

        // Between polls: an append (two rows, one of them late-arriving)
        // and then a re-cluster that folds every live file into one.
        let t_late = t0 - chrono::Duration::hours(1);
        ice.append_events(&[
            mk_event(t0.timestamp() + 5, "h2"),
            mk_event(t_late.timestamp(), "h_late"),
        ])
        .await
        .unwrap();
        let files = ice.live_data_files(&ident).await.unwrap();
        let stats = ice.recluster_files(&ident, files, &[]).await.unwrap();
        assert_eq!(stats.rows, 3, "the rewrite carries all three rows");

        let batches = sub.poll().await.unwrap();
        assert_eq!(
            row_count(&batches),
            2,
            "only the appended rows, not the rewrite's copy of all three"
        );
        // The late-arriving row is among them (that is the property the
        // manifest walk exists for) and survives the rewrite in the same
        // interval.
        let hosts = hosts_in(&batches);
        assert!(hosts.contains(&"h_late".to_string()), "got {hosts:?}");
        assert!(hosts.contains(&"h2".to_string()), "got {hosts:?}");

        assert_eq!(row_count(&sub.poll().await.unwrap()), 0);
    }

    /// Retention's delete-only rewrite is a non-append commit too, and so is
    /// the same shape committed by a writer that is *not* siglake (no marker
    /// property). Both must deliver nothing and both must let the cursor
    /// advance — a foreign overwrite that stalled the subscription would be
    /// worse than the unsupported-semantics gap it documents.
    #[tokio::test]
    async fn delete_only_rewrites_advance_without_delivering() {
        for marker in [Some("retention"), None] {
            let tmp = tempfile::tempdir().unwrap();
            let ice = fresh_ice(&tmp).await;
            let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
            let ident = ice.events_table_ident().clone();

            ice.append_events(&[mk_event(t0.timestamp(), "old")])
                .await
                .unwrap();
            ice.append_events(&[mk_event(t0.timestamp() + 3600, "new")])
                .await
                .unwrap();
            let mut sub = IcebergSubscription::new(
                ice.clone(),
                "events",
                "timestamp",
                t0 - chrono::Duration::seconds(1),
            );
            assert_eq!(row_count(&sub.poll().await.unwrap()), 2);
            let snap_before = sub.cursor_snapshot_id().unwrap();

            // Drop the first file: the delete-only `rewrite_files` shape
            // retention commits, with and without siglake's marker.
            let dropped = ice.live_data_files(&ident).await.unwrap().remove(0);
            commit_delete_only_rewrite(&ice, &ident, dropped, marker).await;

            let table = ice.catalog().load_table(&ident).await.unwrap();
            let expected = if marker.is_some() {
                CommitKind::SiglakeRewrite
            } else {
                CommitKind::ForeignOverwrite
            };
            assert_eq!(
                classify_commit(table.metadata().current_snapshot().unwrap().summary()),
                expected
            );

            assert_eq!(
                row_count(&sub.poll().await.unwrap()),
                0,
                "marker={marker:?}"
            );
            assert_ne!(sub.cursor_snapshot_id(), Some(snap_before));
        }
    }

    /// #1658's documented gap, end to end. Another engine commits an *unmarked*
    /// `rewrite_files` that both drops a live file and adds a file of genuinely
    /// new rows: the rows land in the table and are queryable, yet the
    /// subscription delivers none of them and still advances its snapshot
    /// cursor. Both halves matter — stalling on the commit would be worse than
    /// the gap, and silently delivering the added files would re-deliver a
    /// siglake rewrite's copies of already-seen rows. The delete-only sibling
    /// above can't show the loss because it adds no rows.
    #[tokio::test]
    async fn foreign_overwrite_adding_new_rows_is_skipped_but_advances() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap().with_tuning(
            crate::iceberg::IcebergTuning {
                // Above the inline ceiling, so this regression exercises
                // both the inline object and the folded wide aggregate.
                table_group_count_cardinality: Some(4097),
                ..Default::default()
            },
        ));
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let ident = ice.events_table_ident().clone();

        // Two appends ⇒ two live files, so the overwrite has one to drop and
        // one to leave alone.
        ice.append_events(&[mk_event(t0.timestamp(), "seed_a")])
            .await
            .unwrap();
        ice.append_events(&[mk_event(t0.timestamp() + 1, "seed_b")])
            .await
            .unwrap();
        let mut sub = IcebergSubscription::new(
            ice.clone(),
            "events",
            "timestamp",
            t0 - chrono::Duration::seconds(1),
        );
        assert_eq!(row_count(&sub.poll().await.unwrap()), 2);
        let snap_before = sub.cursor_snapshot_id().unwrap();
        let cursor_before = sub.cursor();

        // Write the added file outside any commit — what a foreign writer
        // does — with event times *later* than the cursor, so nothing about
        // the time filter can explain the rows going undelivered.
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let new_rows =
            siglake_core::events_to_record_batch(&[mk_event(t0.timestamp() + 10, "foreign_a")])
                .unwrap();
        let added = ice
            .write_uncommitted_data_files_for_test(&table, new_rows)
            .await
            .unwrap();
        assert_eq!(added.len(), 1, "one unpartitioned output file");
        let dropped = ice.live_data_files(&ident).await.unwrap().remove(0);
        commit_foreign_overwrite(&ice, &ident, added, vec![dropped]).await;

        let table = ice.catalog().load_table(&ident).await.unwrap();
        assert_eq!(
            classify_commit(table.metadata().current_snapshot().unwrap().summary()),
            CommitKind::ForeignOverwrite
        );

        // The added row really is in the table: one seed file was replaced by
        // one different row, preserving the table's row total. This is the
        // case a total-only aggregate guard cannot distinguish.
        let live = hosts_in(&all_rows(&ice, &ident).await);
        assert_eq!(
            live.len(),
            2,
            "one seed file dropped, one row added: {live:?}"
        );
        for host in ["foreign_a", "seed_b"] {
            assert!(
                live.contains(&host.to_string()),
                "{host} must be queryable: {live:?}"
            );
        }

        // Cold then warm metadata/result-cache reads must both take an exact
        // fallback. The inline and wide artifacts still total two rows, but
        // their snapshot provenance ends before the foreign overwrite.
        let expected = vec![
            (Some("foreign_a".to_string()), 1),
            (Some("seed_b".to_string()), 1),
        ];
        for cache_state in ["cold", "warm"] {
            let grouped = ice
                .grouped_counts_with_summary("events", "host", None, None)
                .await
                .unwrap()
                .unwrap();
            let mut grouped_rows = grouped.to_rows();
            grouped_rows.sort();
            assert_eq!(grouped_rows, expected, "{cache_state} GROUP BY");
            assert_eq!(
                grouped.source_label(),
                "materialized",
                "{cache_state} GROUP BY admitted a stale inline or wide aggregate"
            );
            let window = crate::iceberg::TimeBounds {
                start: Some(t0 - chrono::Duration::seconds(1)),
                end: Some(t0 + chrono::Duration::seconds(20)),
            };
            let windowed = ice
                .grouped_counts_with_summary("events", "host", None, Some(window))
                .await
                .unwrap()
                .unwrap();
            let mut windowed_rows = windowed.to_rows();
            windowed_rows.sort();
            assert_eq!(windowed_rows, expected, "{cache_state} windowed GROUP BY");
            assert_eq!(windowed.source_label(), "materialized");
            assert_eq!(
                ice.windowed_count("events", window, None).await.unwrap(),
                None,
                "{cache_state} windowed count admitted stale time buckets"
            );
            let exact_window_count: i64 = ice
                .date_histogram_counts("events", 1_000_000_000, 0, None, Some(window))
                .await
                .unwrap()
                .unwrap()
                .into_iter()
                .map(|(_, count)| count)
                .sum();
            assert_eq!(
                exact_window_count,
                live.len() as i64,
                "{cache_state} exact windowed-count fallback disagrees with the direct scan"
            );
        }

        // …and the subscription delivers none of them, without stalling.
        let after = sub.poll().await.unwrap();
        assert_eq!(
            row_count(&after),
            0,
            "an unmarked overwrite's added rows are not delivered: {:?}",
            hosts_in(&after)
        );
        assert_eq!(sub.cursor(), cursor_before, "time cursor does not move");
        assert_ne!(
            sub.cursor_snapshot_id(),
            Some(snap_before),
            "the subscription advances across the foreign commit"
        );

        // A following append is delivered normally, and only its own rows —
        // the skipped commit does not poison the incremental walk.
        ice.append_events(&[mk_event(t0.timestamp() + 20, "after")])
            .await
            .unwrap();
        assert_eq!(hosts_in(&sub.poll().await.unwrap()), vec!["after"]);
        assert_eq!(row_count(&sub.poll().await.unwrap()), 0);
    }

    /// Every row currently live in `ident`, read through the same provider a
    /// subscription bootstrap uses (host column only).
    async fn all_rows(ice: &IcebergContext, ident: &iceberg::TableIdent) -> Vec<RecordBatch> {
        let table = ice.catalog().load_table(ident).await.unwrap();
        let ctx = SessionContext::new();
        register_table_for_subscription(&ctx, "events", &table)
            .await
            .unwrap();
        ctx.sql("SELECT host FROM events")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    }

    /// Commit an unmarked `rewrite_files` — the shape an external engine's
    /// overwrite takes — that deletes `deleted` and adds `added`.
    async fn commit_foreign_overwrite(
        ice: &IcebergContext,
        ident: &iceberg::TableIdent,
        added: Vec<iceberg::spec::DataFile>,
        deleted: Vec<iceberg::spec::DataFile>,
    ) {
        use iceberg::transaction::{ApplyTransactionAction, Transaction};

        let table = ice.catalog().load_table(ident).await.unwrap();
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .with_check_duplicate(false)
            .add_data_files(added)
            .delete_files(deleted)
            // No REWRITE_COMMIT_PROP: a foreign writer stamps its own.
            .set_snapshot_properties(std::collections::HashMap::from([(
                "other-engine.job".to_string(),
                "43".to_string(),
            )]));
        let tx = action.apply(tx).unwrap();
        tx.commit(ice.catalog().as_ref()).await.unwrap();
        ice.invalidate_cached_table(ident).await;
    }

    /// Commit a `rewrite_files` transaction that only removes `file`,
    /// optionally stamped with siglake's rewrite marker.
    async fn commit_delete_only_rewrite(
        ice: &IcebergContext,
        ident: &iceberg::TableIdent,
        file: iceberg::spec::DataFile,
        marker: Option<&str>,
    ) {
        use iceberg::transaction::{ApplyTransactionAction, Transaction};

        let table = ice.catalog().load_table(ident).await.unwrap();
        let tx = Transaction::new(&table);
        let mut action = tx
            .rewrite_files()
            .with_check_duplicate(false)
            .delete_files([file]);
        if let Some(marker) = marker {
            action = action.set_snapshot_properties(std::collections::HashMap::from([(
                REWRITE_COMMIT_PROP.to_string(),
                marker.to_string(),
            )]));
        } else {
            // `rewrite_files` needs either an added file or a snapshot
            // property; an external engine's overwrite would carry its own.
            action = action.set_snapshot_properties(std::collections::HashMap::from([(
                "other-engine.job".to_string(),
                "42".to_string(),
            )]));
        }
        let tx = action.apply(tx).unwrap();
        tx.commit(ice.catalog().as_ref()).await.unwrap();
        ice.invalidate_cached_table(ident).await;
    }

    fn row_count(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn hosts_in(batches: &[RecordBatch]) -> Vec<String> {
        use arrow_array::Array;
        let mut out = Vec::new();
        for batch in batches {
            let col = batch.column_by_name("host").expect("host column");
            let hosts = col
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("host is a string column");
            for i in 0..hosts.len() {
                out.push(hosts.value(i).to_string());
            }
        }
        out
    }

    fn summary_of(operation: &Operation, props: &[(&str, &str)]) -> Summary {
        Summary {
            operation: operation.clone(),
            additional_properties: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn classify_delivers_only_appends() {
        assert_eq!(
            classify_commit(&summary_of(&Operation::Append, &[("added-records", "7")])),
            CommitKind::Append
        );
        assert!(CommitKind::Append.delivers_added_files());
        for op in [Operation::Overwrite, Operation::Replace, Operation::Delete] {
            let marked = summary_of(&op, &[(REWRITE_COMMIT_PROP, "recluster")]);
            assert_eq!(
                classify_commit(&marked),
                CommitKind::SiglakeRewrite,
                "{op:?}"
            );
            assert!(!classify_commit(&marked).delivers_added_files());
            let unmarked = summary_of(&op, &[]);
            assert_eq!(
                classify_commit(&unmarked),
                CommitKind::ForeignOverwrite,
                "{op:?}"
            );
            assert!(!classify_commit(&unmarked).delivers_added_files());
        }
    }

    /// `rewrite_files`' add-only shape commits an `Overwrite` that adds files
    /// and removes none. It is skipped like every other marked rewrite —
    /// subscriber-visible rows go through `fast_append` (decision #2387).
    #[test]
    fn classify_skips_a_marked_add_only_rewrite() {
        let add_only = summary_of(
            &Operation::Overwrite,
            &[
                (REWRITE_COMMIT_PROP, "recluster"),
                ("deleted-data-files", "0"),
                ("added-data-files", "3"),
                ("added-records", "9000"),
            ],
        );
        assert_eq!(classify_commit(&add_only), CommitKind::SiglakeRewrite);
        assert!(!classify_commit(&add_only).delivers_added_files());
    }

    /// The marker is what separates siglake's rewrites from an external
    /// engine's overwrite. Both are skipped, so this pins the *labelling*
    /// contract: every rewrite siglake commits must carry the property.
    #[tokio::test]
    async fn siglake_rewrites_carry_the_marker_property() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = fresh_ice(&tmp).await;
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let ident = ice.events_table_ident().clone();

        ice.append_events(&[mk_event(t0.timestamp(), "h1")])
            .await
            .unwrap();
        ice.append_events(&[mk_event(t0.timestamp() + 1, "h2")])
            .await
            .unwrap();
        let files = ice.live_data_files(&ident).await.unwrap();
        ice.recluster_files(&ident, files, &[]).await.unwrap();

        let table = ice.catalog().load_table(&ident).await.unwrap();
        let current = table.metadata().current_snapshot().unwrap();
        assert_eq!(
            classify_commit(current.summary()),
            CommitKind::SiglakeRewrite,
            "recluster summary was {:?}",
            current.summary()
        );
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Node(i64, Option<i64>);

    impl SnapshotNode for Node {
        fn id(&self) -> i64 {
            self.0
        }
        fn parent(&self) -> Option<i64> {
            self.1
        }
    }

    fn lookup_in(chain: &[Node]) -> impl Fn(i64) -> Option<Node> + '_ {
        move |id| chain.iter().find(|n| n.0 == id).cloned()
    }

    #[test]
    fn walk_classifies_contiguous_history() {
        let chain = vec![Node(1, None), Node(2, Some(1)), Node(3, Some(2))];
        assert_eq!(
            walk_ancestry(Node(3, Some(2)), 1, lookup_in(&chain)),
            Ancestry::Reached(vec![Node(3, Some(2)), Node(2, Some(1))])
        );
        // Cursor == current: nothing in range.
        assert_eq!(
            walk_ancestry(Node(3, Some(2)), 3, lookup_in(&chain)),
            Ancestry::Reached(vec![])
        );
    }

    #[test]
    fn walk_accepts_an_expired_cursor_named_by_a_parent_link() {
        // Snapshot 1's metadata is gone; 2 still points at it.
        let retained = vec![Node(2, Some(1)), Node(3, Some(2))];
        assert_eq!(
            walk_ancestry(Node(3, Some(2)), 1, lookup_in(&retained)),
            Ancestry::Reached(vec![Node(3, Some(2)), Node(2, Some(1))])
        );
    }

    #[test]
    fn walk_reports_a_gap_for_an_expired_intermediate() {
        // Cursor is 1; 2 expired, so what happened in (1, 3] is unknowable.
        let retained = vec![Node(3, Some(2)), Node(4, Some(3))];
        assert_eq!(
            walk_ancestry(Node(4, Some(3)), 1, lookup_in(&retained)),
            Ancestry::Gap {
                missing: 2,
                child: 3
            }
        );
    }

    #[test]
    fn walk_reports_cursor_not_ancestor_at_the_root() {
        let chain = vec![Node(1, None), Node(2, Some(1))];
        assert_eq!(
            walk_ancestry(Node(2, Some(1)), 99, lookup_in(&chain)),
            Ancestry::CursorNotAncestor(vec![Node(2, Some(1)), Node(1, None)])
        );
    }

    #[test]
    fn walk_terminates_on_a_cyclic_parent_chain() {
        let cyclic = vec![Node(1, Some(2)), Node(2, Some(1))];
        assert_eq!(
            walk_ancestry(Node(2, Some(1)), 99, lookup_in(&cyclic)),
            Ancestry::Gap {
                missing: 1,
                child: 2
            }
        );
    }
}
