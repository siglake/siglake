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

//! Rewrite (overwrite) action: atomically remove a set of existing data files and
//! add a set of new data files in a single snapshot. This is the primitive used by
//! the siglake tier-2 re-clustering compactor to replace a partition's time-overlapping
//! files with time-contiguous ones without a window of duplicated or missing rows.
//!
//! Contract, for all three accepted shapes. The new snapshot's live file set is
//! `(previous live files − removed) ∪ added`, and its running totals follow the same
//! arithmetic, so a scan of the new snapshot and its `total-records` always agree:
//!
//! - **Both sides non-empty** (a compaction rewrite): the existing manifests are
//!   rewritten, removed entries becoming `Deleted` tombstones and the rest `Existing`.
//! - **Delete-only** (`delete_files` with no `add_data_files`, e.g. retention): same
//!   manifest rewrite, no added manifest. Snapshot properties are optional.
//! - **Add-only** (`add_data_files` with no `delete_files`): every live manifest is
//!   carried forward and the new files land in one added manifest, as for an append.
//!
//! Both sides empty is refused. Removed files are matched by path against the live
//! entries of the snapshot the commit lands on — which is re-resolved on every commit
//! attempt, so a rewrite that rebases over an intervening append keeps that append's
//! files.
//!
//! The snapshot's operation is `Overwrite` for all three shapes, and siglake's
//! incremental subscription (`siglake-storage`'s `classify_commit`) delivers rows only
//! from `Append` snapshots. New rows that a subscriber has to see therefore belong in a
//! `fast_append`, not in an add-only rewrite.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result};

/// RewriteFilesAction replaces `removed_data_files` with `added_data_files` in a single
/// `Overwrite` snapshot. The removed files are matched by path against the current
/// snapshot's live manifest entries; the commit fails if any of them is not currently
/// live, so a re-clustering rewrite can never silently duplicate or drop rows.
pub struct RewriteFilesAction {
    check_duplicate: bool,
    snapshot_id: Option<i64>,
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            snapshot_id: None,
            commit_uuid: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            removed_data_files: vec![],
        }
    }

    /// Set whether to check that added files are not already referenced by the table.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot (the re-clustered replacements).
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Mark data files for removal in the snapshot (the original time-overlapping files).
    pub fn delete_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_data_files.extend(data_files);
        self
    }

    /// FORK ADDITION (siglake #4377). Commit under a snapshot id the caller
    /// reserved with [`reserve_snapshot_id`](crate::transaction::reserve_snapshot_id),
    /// so a Puffin statistics file written for that id can be registered in
    /// the same transaction as the rewrite.
    ///
    /// Unset, the id is generated inside the commit as before. Set, the commit
    /// fails — without retrying — if the id is present on the base the attempt
    /// re-applies against, because the sidecar already names it and a second
    /// snapshot cannot take it.
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.added_data_files.is_empty() && self.removed_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "RewriteFilesAction requires at least one added or removed data file",
            ));
        }

        let commit_uuid = self.commit_uuid.unwrap_or_else(Uuid::now_v7);
        let mut snapshot_producer = match self.snapshot_id {
            Some(snapshot_id) => SnapshotProducer::new_with_snapshot_id(
                table,
                snapshot_id,
                commit_uuid,
                self.snapshot_properties.clone(),
                self.added_data_files.clone(),
            )?,
            None => SnapshotProducer::new(
                table,
                commit_uuid,
                self.snapshot_properties.clone(),
                self.added_data_files.clone(),
            ),
        };
        snapshot_producer.set_removed_data_files(self.removed_data_files.clone());

        // Validate the new files (partition spec, content type).
        snapshot_producer.validate_added_data_files()?;

        // Added files must not already be referenced by the table; the manifest rewrite
        // additionally guarantees every removed file was live.
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        snapshot_producer
            .commit(RewriteFilesOperation, DefaultManifestProcess)
            .await
    }
}

struct RewriteFilesOperation;

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // Removed entries are handled directly by the snapshot producer's manifest
        // rewrite (it has the &mut access needed to write replacement manifests), so
        // this hook is unused for the rewrite path.
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        // Reached only for an add-only rewrite: with removals the producer rewrites the
        // existing manifests itself and never calls this. Add-only carries every live
        // manifest forward, exactly as an append does — returning empty here would drop
        // the whole prior file set from the new snapshot while its totals still counted
        // those rows.
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot_produce
            .table
            .manifest_list_reader(snapshot)
            .load()
            .await?;

        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures::TryStreamExt;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Operation, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, Result};

    fn data_file(name: &str, record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("test/{name}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(record_count)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    /// The file paths the current snapshot actually scans, sorted, plus the records
    /// those files claim.
    async fn scan(table: &Table) -> Result<(Vec<String>, u64)> {
        let tasks: Vec<_> = table
            .scan()
            .build()?
            .plan_files()
            .await?
            .try_collect()
            .await?;
        let mut paths: Vec<String> = tasks.iter().map(|t| t.data_file_path.clone()).collect();
        paths.sort();
        let records = tasks.iter().map(|t| t.record_count.unwrap_or(0)).sum();
        Ok((paths, records))
    }

    /// The running row total the current snapshot's summary claims.
    fn total_records(table: &Table) -> u64 {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get("total-records")
            .unwrap()
            .parse()
            .unwrap()
    }

    async fn append(table: &Table, catalog: &impl Catalog, files: Vec<DataFile>) -> Table {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        action.apply(tx).unwrap().commit(catalog).await.unwrap()
    }

    #[tokio::test]
    async fn test_rewrite_with_neither_added_nor_removed_files_is_rejected() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append(&table, &catalog, vec![data_file("a", 10)]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files();
        let err = action
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap_err();
        assert!(
            err.message()
                .contains("requires at least one added or removed data file"),
            "unexpected error: {err}"
        );
    }

    /// An add-only rewrite must carry the prior live files into the new snapshot. It
    /// used to return no existing manifests, so the snapshot scanned only the new file
    /// while its totals still counted every prior row.
    #[tokio::test]
    async fn test_add_only_rewrite_preserves_existing_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append(
            &table,
            &catalog,
            vec![data_file("a", 10), data_file("b", 20)],
        )
        .await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().add_data_files(vec![data_file("c", 5)]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let (paths, records) = scan(&table).await.unwrap();
        assert_eq!(
            paths,
            vec![
                "test/a.parquet".to_string(),
                "test/b.parquet".to_string(),
                "test/c.parquet".to_string(),
            ]
        );
        assert_eq!(records, 35);
        assert_eq!(total_records(&table), 35);
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Overwrite
        );
    }

    /// A delete-only rewrite carrying no snapshot properties used to be refused by
    /// `SnapshotProducer::manifest_file`; the removals are the snapshot's content.
    #[tokio::test]
    async fn test_delete_only_rewrite_without_snapshot_properties() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append(
            &table,
            &catalog,
            vec![data_file("a", 10), data_file("b", 20), data_file("c", 5)],
        )
        .await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_files(vec![data_file("b", 20)]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let (paths, records) = scan(&table).await.unwrap();
        assert_eq!(
            paths,
            vec!["test/a.parquet".to_string(), "test/c.parquet".to_string()]
        );
        assert_eq!(records, 15);
        assert_eq!(total_records(&table), 15);
        assert!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .additional_properties
                .contains_key("total-records")
        );
    }

    /// The rewrite is re-applied against the base the commit lands on, so an add-only
    /// rewrite built from a stale handle must keep the intervening append's file.
    #[tokio::test]
    async fn test_add_only_rewrite_rebases_over_intervening_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append(&table, &catalog, vec![data_file("a", 10)]).await;

        // Build the rewrite against this base, then let another writer commit first.
        let tx = Transaction::new(&table);
        let rewrite = tx
            .rewrite_files()
            .add_data_files(vec![data_file("c", 5)])
            .apply(tx)
            .unwrap();

        let table = append(&table, &catalog, vec![data_file("b", 20)]).await;
        assert_eq!(total_records(&table), 30);

        let table = rewrite.commit(&catalog).await.unwrap();

        let (paths, records) = scan(&table).await.unwrap();
        assert_eq!(
            paths,
            vec![
                "test/a.parquet".to_string(),
                "test/b.parquet".to_string(),
                "test/c.parquet".to_string(),
            ]
        );
        assert_eq!(records, 35);
        assert_eq!(total_records(&table), 35);
    }

    /// A rewrite with both sides populated still replaces exactly its own files.
    #[tokio::test]
    async fn test_rewrite_replaces_only_its_own_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append(
            &table,
            &catalog,
            vec![data_file("a", 10), data_file("b", 20)],
        )
        .await;

        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .add_data_files(vec![data_file("c", 20)])
            .delete_files(vec![data_file("b", 20)]);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let (paths, records) = scan(&table).await.unwrap();
        assert_eq!(
            paths,
            vec!["test/a.parquet".to_string(), "test/c.parquet".to_string()]
        );
        assert_eq!(records, 30);
        assert_eq!(total_records(&table), 30);
    }

    #[tokio::test]
    async fn test_rewrite_preserves_caller_markers_beside_computed_totals() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append(&table, &catalog, vec![data_file("a", 10)]).await;

        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .add_data_files(vec![data_file("b", 10)])
            .delete_files(vec![data_file("a", 10)])
            .set_snapshot_properties(HashMap::from([
                ("siglake.rewrite".to_string(), "recluster".to_string()),
                (
                    "siglake.consumed_segments".to_string(),
                    "segment-a".to_string(),
                ),
            ]));
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();
        let properties = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;

        assert_eq!(
            properties.get("siglake.rewrite").map(String::as_str),
            Some("recluster")
        );
        assert_eq!(
            properties
                .get("siglake.consumed_segments")
                .map(String::as_str),
            Some("segment-a")
        );
        assert_eq!(
            properties.get("total-records").map(String::as_str),
            Some("10")
        );
    }
}
