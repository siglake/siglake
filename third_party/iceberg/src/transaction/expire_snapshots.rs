// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Snapshot-metadata expiry.
//!
//! iceberg-rust 0.9 has no public `expire_snapshots` action, but its
//! [`TableMetadataBuilder::remove_snapshots`] already drops snapshots
//! from the metadata and `build()` trims the snapshot- and metadata-log
//! accordingly. This action wires that into the transaction layer so a
//! caller can bound the unbounded `snapshots` array that otherwise grows
//! one entry per commit and is read + rewritten on every subsequent
//! commit (the dominant per-commit catalog cost — see siglake BIG-4).
//!
//! **Non-destructive.** This only removes snapshots from *metadata*; the
//! manifest-list / manifest / data files of expired snapshots are left in
//! object storage as orphans. Physically deleting unreachable files
//! (true `expire_snapshots`) needs reachability analysis across retained
//! snapshots and is deferred to the upstream 0.10 API. The current
//! snapshot and every branch/tag ref target are always retained, so the
//! live read path is unaffected.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Result, TableRequirement, TableUpdate};

/// Default number of most-recent snapshots to retain.
pub const DEFAULT_RETAIN_LAST: usize = 100;

/// A transactional action that expires old snapshots from table metadata,
/// retaining the most-recent `retain_last` plus the current snapshot and
/// every ref target. See the module docs for the non-destructive caveat.
pub struct ExpireSnapshotsAction {
    retain_last: usize,
    /// When set, only snapshots with `timestamp_ms` strictly older than this
    /// cutoff are eligible to expire (a snapshot-*age* sweep). `None` = pure
    /// count-based (`retain_last`) expiry.
    older_than_ms: Option<i64>,
}

impl ExpireSnapshotsAction {
    /// New action retaining [`DEFAULT_RETAIN_LAST`] snapshots.
    pub fn new() -> Self {
        Self {
            retain_last: DEFAULT_RETAIN_LAST,
            older_than_ms: None,
        }
    }

    /// Retain the `n` most-recent snapshots (by timestamp). Clamped to at
    /// least 1 so the current snapshot is never the only thing standing
    /// between the table and an empty snapshot set.
    pub fn retain_last(mut self, n: usize) -> Self {
        self.retain_last = n.max(1);
        self
    }

    /// Only expire snapshots older than `cutoff_ms` (epoch millis). Combines
    /// with [`Self::retain_last`]: the current snapshot, every ref target, and
    /// the most-recent `retain_last` are always kept, and among the rest only
    /// those older than the cutoff expire. The age sweep that lets a caller
    /// bound snapshot history by time without ever dropping recent snapshots.
    pub fn older_than(mut self, cutoff_ms: i64) -> Self {
        self.older_than_ms = Some(cutoff_ms);
        self
    }

    /// The snapshot ids this action would expire for `table` given
    /// `retain_last`. Never includes the current snapshot or any ref
    /// target. Exposed so callers can skip a no-op commit when empty.
    pub fn expired_ids(table: &Table, retain_last: usize) -> Vec<i64> {
        Self::expired_ids_aged(table, retain_last, None)
    }

    /// As [`Self::expired_ids`], but when `older_than_ms` is `Some(cutoff)`
    /// only snapshots strictly older than `cutoff` are expired (still always
    /// retaining current + refs + the most-recent `retain_last`).
    pub fn expired_ids_aged(
        table: &Table,
        retain_last: usize,
        older_than_ms: Option<i64>,
    ) -> Vec<i64> {
        let metadata = table.metadata();
        let retain_last = retain_last.max(1);

        // Always-retained: the main-branch current snapshot and every
        // branch/tag ref target. (`refs` is crate-internal.)
        let mut protected: HashSet<i64> =
            metadata.refs.values().map(|r| r.snapshot_id).collect();
        if let Some(current) = metadata.current_snapshot_id() {
            protected.insert(current);
        }

        // Newest `retain_last` snapshots by timestamp are kept.
        let mut by_recency: Vec<(i64, i64)> = metadata
            .snapshots()
            .map(|s| (s.snapshot_id(), s.timestamp_ms()))
            .collect();
        // Sort newest-first; tie-break on id for determinism.
        by_recency.sort_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
        let keep_recent: HashSet<i64> = by_recency
            .iter()
            .take(retain_last)
            .map(|(id, _)| *id)
            .collect();

        by_recency
            .iter()
            .filter(|(id, ts)| {
                !protected.contains(id)
                    && !keep_recent.contains(id)
                    && older_than_ms.is_none_or(|cut| *ts < cut)
            })
            .map(|(id, _)| *id)
            .collect()
    }
}

impl Default for ExpireSnapshotsAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for ExpireSnapshotsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let expired = Self::expired_ids_aged(table, self.retain_last, self.older_than_ms);
        if expired.is_empty() {
            // Nothing to expire — emit no updates so the transaction is a
            // no-op (callers should also pre-check to avoid an empty
            // commit entirely).
            return Ok(ActionCommit::new(vec![], vec![]));
        }
        Ok(ActionCommit::new(
            vec![TableUpdate::RemoveSnapshots {
                snapshot_ids: expired,
            }],
            vec![TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            }],
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::transaction::expire_snapshots::ExpireSnapshotsAction;
    use crate::transaction::tests::make_v2_table;

    #[test]
    fn expired_ids_keeps_current_and_recent() {
        // TableMetadataV2Valid has two snapshots; current is the newer.
        let table = make_v2_table();
        let total = table.metadata().snapshots().count();
        assert!(total >= 2, "fixture should have >=2 snapshots");

        // Retain 1: everything but the single most-recent should expire,
        // and the current snapshot must never be in the expired set.
        let expired = ExpireSnapshotsAction::expired_ids(&table, 1);
        let current = table.metadata().current_snapshot_id().unwrap();
        assert!(
            !expired.contains(&current),
            "current snapshot must never expire"
        );
        assert_eq!(
            expired.len(),
            total - 1,
            "retain_last=1 keeps exactly one snapshot"
        );
    }

    #[test]
    fn expired_ids_noop_when_under_retention() {
        let table = make_v2_table();
        let total = table.metadata().snapshots().count();
        // Retaining at least as many as exist expires nothing.
        assert!(ExpireSnapshotsAction::expired_ids(&table, total).is_empty());
        assert!(ExpireSnapshotsAction::expired_ids(&table, total + 5).is_empty());
    }

    #[tokio::test]
    async fn commit_emits_remove_snapshots_update() {
        use crate::transaction::action::TransactionAction;
        let table = make_v2_table();
        let action = Arc::new(ExpireSnapshotsAction::new().retain_last(1));
        let mut commit = action.commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(updates.len(), 1);
        assert!(matches!(
            &updates[0],
            crate::TableUpdate::RemoveSnapshots { snapshot_ids } if !snapshot_ids.is_empty()
        ));
        assert_eq!(commit.take_requirements().len(), 1);
    }
}
