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

//! This module contains transaction api.
//!
//! The transaction API enables changes to be made to an existing table.
//!
//! Note that this may also have side effects, such as producing new manifest
//! files.
//!
//! Below is a basic example using the "fast-append" action:
//!
//! ```ignore
//! use iceberg::transaction::{ApplyTransactionAction, Transaction};
//! use iceberg::Catalog;
//!
//! // Create a transaction.
//! let tx = Transaction::new(my_table);
//!
//! // Create a `FastAppendAction` which will not rewrite or append
//! // to existing metadata. This will create a new manifest.
//! let action = tx.fast_append().add_data_files(my_data_files);
//!
//! // Apply the fast-append action to the given transaction, returning
//! // the newly updated `Transaction`.
//! let tx = action.apply(tx).unwrap();
//!
//!
//! // End the transaction by committing to an `iceberg::Catalog`
//! // implementation. This will cause a table update to occur.
//! let table = tx
//!     .commit(&some_catalog_impl)
//!     .await
//!     .unwrap();
//! ```

/// The `ApplyTransactionAction` trait provides an `apply` method
/// that allows users to apply a transaction action to a `Transaction`.
mod action;

pub use action::*;
mod append;
mod expire_snapshots;
pub use expire_snapshots::ExpireSnapshotsAction;
mod rewrite;
mod snapshot;
mod sort_order;
mod update_location;
mod update_properties;
mod update_schema;
pub use update_schema::UpdateSchemaAction;
mod update_statistics;
mod upgrade_format_version;

use std::sync::Arc;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder, RetryableWithContext};
use uuid::Uuid;

use crate::error::Result;
use crate::spec::TableProperties;
use crate::table::Table;
use crate::transaction::action::BoxedTransactionAction;
use crate::transaction::append::FastAppendAction;
use crate::transaction::rewrite::RewriteFilesAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::transaction::update_location::UpdateLocationAction;
use crate::transaction::update_properties::UpdatePropertiesAction;
use crate::transaction::update_statistics::UpdateStatisticsAction;
use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::{Catalog, Error, ErrorKind, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
#[derive(Clone)]
pub struct Transaction {
    table: Table,
    table_uuid: Uuid,
    actions: Vec<BoxedTransactionAction>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
            table_uuid: table.metadata().uuid(),
            actions: vec![],
        }
    }

    fn update_table_metadata(table: Table, updates: &[TableUpdate]) -> Result<Table> {
        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        Ok(table.with_metadata(Arc::new(metadata_builder.build()?.metadata)))
    }

    /// Applies an [`ActionCommit`] to the given [`Table`], returning a new [`Table`] with updated metadata.
    /// Also appends any derived [`TableUpdate`]s and [`TableRequirement`]s to the provided vectors.
    fn apply(
        table: Table,
        mut action_commit: ActionCommit,
        existing_updates: &mut Vec<TableUpdate>,
        existing_requirements: &mut Vec<TableRequirement>,
    ) -> Result<Table> {
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        for requirement in &requirements {
            requirement.check(Some(table.metadata()))?;
        }

        let updated_table = Self::update_table_metadata(table, &updates)?;

        existing_updates.extend(updates);
        existing_requirements.extend(requirements);

        Ok(updated_table)
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(&self) -> UpgradeFormatVersionAction {
        UpgradeFormatVersionAction::new()
    }

    /// Update table's property.
    pub fn update_table_properties(&self) -> UpdatePropertiesAction {
        UpdatePropertiesAction::new()
    }

    /// Creates a fast append action.
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    /// Creates a rewrite-files (overwrite) action that atomically removes a set of
    /// existing data files and adds a set of new ones in a single snapshot.
    pub fn rewrite_files(&self) -> RewriteFilesAction {
        RewriteFilesAction::new()
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(&self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction::new()
    }

    /// Creates an additive, idempotent schema-evolution action that
    /// appends new optional columns to the current schema. See
    /// [`UpdateSchemaAction`].
    pub fn update_schema(&self) -> UpdateSchemaAction {
        UpdateSchemaAction::new()
    }

    /// Creates an expire-snapshots action that removes old snapshots from
    /// table metadata (retaining the most-recent N plus the current
    /// snapshot and every ref target). Non-destructive: it drops snapshot
    /// metadata only and leaves expired-snapshot files in object storage
    /// as orphans. See [`ExpireSnapshotsAction`].
    pub fn expire_snapshots(&self) -> ExpireSnapshotsAction {
        ExpireSnapshotsAction::new()
    }

    /// Set the location of table
    pub fn update_location(&self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Update the statistics of table
    pub fn update_statistics(&self) -> UpdateStatisticsAction {
        UpdateStatisticsAction::new()
    }

    /// Commit transaction.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.actions.is_empty() {
            // nothing to commit
            return Ok(self.table);
        }

        let table_props = self.table.metadata().table_properties()?;

        let backoff = Self::build_backoff(table_props)?;
        let tx = self;

        (|mut tx: Transaction| async {
            let result = tx.do_commit(catalog).await;
            (tx, result)
        })
        .retry(backoff)
        .sleep(tokio::time::sleep)
        .context(tx)
        .when(|e| e.retryable())
        .await
        .1
    }

    fn build_backoff(props: TableProperties) -> Result<ExponentialBackoff> {
        Ok(ExponentialBuilder::new()
            .with_min_delay(Duration::from_millis(props.commit_min_retry_wait_ms))
            .with_max_delay(Duration::from_millis(props.commit_max_retry_wait_ms))
            .with_total_delay(Some(Duration::from_millis(
                props.commit_total_retry_timeout_ms,
            )))
            .with_max_times(props.commit_num_retries)
            .with_factor(2.0)
            .build())
    }

    async fn do_commit(&mut self, catalog: &dyn Catalog) -> Result<Table> {
        // Attempts-per-success (this counter / the caller's commit count) is the
        // direct measure of optimistic-concurrency churn: every attempt pays a
        // catalog load_table + full action re-apply, so a ratio well above 1
        // under concurrent writers means the commit path is CAS-bound.
        metrics::counter!("siglake_iceberg_commit_attempts_total").increment(1);
        let refreshed = catalog.load_table(self.table.identifier()).await?;
        let refreshed_uuid = refreshed.metadata().uuid();

        if refreshed_uuid != self.table_uuid {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "Cannot commit transaction: table UUID changed",
            )
            .with_context("identifier", self.table.identifier().to_string())
            .with_context("expected", self.table_uuid)
            .with_context("found", refreshed_uuid)
            .with_retryable(false));
        }

        if self.table.metadata() != refreshed.metadata()
            || self.table.metadata_location() != refreshed.metadata_location()
        {
            // current base is stale, use refreshed as base and re-apply transaction
            // actions (another writer committed since this transaction's base load).
            metrics::counter!("siglake_iceberg_commit_stale_base_total").increment(1);
            self.table = refreshed.clone();
        }

        let mut current_table = self.table.clone();
        let mut existing_updates: Vec<TableUpdate> = vec![];
        let mut existing_requirements: Vec<TableRequirement> = vec![];

        for action in &self.actions {
            let action_commit = Arc::clone(action).commit(&current_table).await?;
            // apply action commit to current_table
            current_table = Self::apply(
                current_table,
                action_commit,
                &mut existing_updates,
                &mut existing_requirements,
            )?;
        }

        // Retried idempotent actions can disappear after they are re-applied
        // to the refreshed base. Do not turn a genuinely empty transaction
        // into a metadata version and catalog update. Requirements still need
        // catalog validation even when an action produced no updates.
        if existing_updates.is_empty() && existing_requirements.is_empty() {
            return Ok(current_table);
        }

        let table_commit = TableCommit::builder()
            .ident(self.table.identifier().to_owned())
            .updates(existing_updates)
            .requirements(existing_requirements)
            .build();

        // `self.table` is the base this commit's diff (updates + requirements)
        // was computed against — freshly loaded at the top of `do_commit` and
        // refreshed above if it was stale. Hand it to the catalog so a
        // re-loading implementation (e.g. the SQL catalog) can apply the commit
        // without a redundant `metadata.json` read. On a concurrent-commit
        // conflict the catalog returns a retryable error and the `commit`
        // backoff loop re-runs `do_commit`, picking up the new base.
        catalog
            .update_table_with_base(table_commit, self.table.clone())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::catalog::MockCatalog;
    use crate::io::FileIO;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Literal, Struct,
        TableMetadata,
    };
    use crate::table::Table;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, Error, ErrorKind, TableCreation, TableIdent};
    use uuid::Uuid;

    pub fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .build()
            .unwrap()
    }

    pub fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .build()
            .unwrap()
    }

    pub fn make_v2_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .build()
            .unwrap()
    }

    /// A committable v2 table: the minimal v2 metadata's schema, partition spec and
    /// sort order created through `catalog`, so a transaction can be committed against
    /// it and the resulting snapshot loaded back and scanned.
    pub(crate) async fn make_v2_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        make_minimal_table_in_catalog(
            catalog,
            "TableMetadataV2ValidMinimal.json",
            FormatVersion::V2,
        )
        .await
    }

    pub(crate) async fn make_v3_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        make_minimal_table_in_catalog(
            catalog,
            "TableMetadataV3ValidMinimal.json",
            FormatVersion::V3,
        )
        .await
    }

    async fn make_minimal_table_in_catalog(
        catalog: &impl Catalog,
        metadata_file: &str,
        format_version: FormatVersion,
    ) -> Table {
        let table_ident =
            TableIdent::from_strs([format!("ns1-{}", uuid::Uuid::new_v4()), "test1".to_string()])
                .unwrap();

        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .await
            .unwrap();

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let base_metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .partition_spec((**base_metadata.default_partition_spec()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_string())
            .format_version(format_version)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// Helper function to create a test table with retry properties
    pub(super) fn setup_test_table(num_retries: &str) -> Table {
        let table = make_v2_table();

        // Set retry properties
        let mut props = HashMap::new();
        props.insert("commit.retry.min-wait-ms".to_string(), "10".to_string());
        props.insert("commit.retry.max-wait-ms".to_string(), "100".to_string());
        props.insert(
            "commit.retry.total-timeout-ms".to_string(),
            "1000".to_string(),
        );
        props.insert(
            "commit.retry.num-retries".to_string(),
            num_retries.to_string(),
        );

        // Update table properties
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(props)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        table.with_metadata(Arc::new(metadata))
    }

    /// Helper function to create a transaction with a simple update action
    fn create_test_transaction(table: &Table) -> Transaction {
        let tx = Transaction::new(table);
        tx.update_table_properties()
            .set("test.key".to_string(), "test.value".to_string())
            .apply(tx)
            .unwrap()
    }

    fn with_table_uuid(table: &Table, table_uuid: Uuid) -> Table {
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .assign_uuid(table_uuid)
            .build()
            .unwrap()
            .metadata;
        table.clone().with_metadata(Arc::new(metadata))
    }

    /// Helper function to set up a mock catalog with retryable errors
    fn setup_mock_catalog_with_retryable_errors(
        success_after_attempts: Option<u32>,
        expected_calls: usize,
    ) -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        let attempts = AtomicU32::new(0);
        mock_catalog
            .expect_update_table_with_base()
            .times(expected_calls)
            .returning_st(move |_, _| {
                if let Some(success_after_attempts) = success_after_attempts {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if attempts.load(Ordering::SeqCst) <= success_after_attempts {
                        Box::pin(async move {
                            Err(
                                Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                    .with_retryable(true),
                            )
                        })
                    } else {
                        Box::pin(async move { Ok(make_v2_table()) })
                    }
                } else {
                    // Always fail with retryable error
                    Box::pin(async move {
                        Err(
                            Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                                .with_retryable(true),
                        )
                    })
                }
            });

        mock_catalog
    }

    /// Helper function to set up a mock catalog with non-retryable error
    fn setup_mock_catalog_with_non_retryable_error() -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();

        mock_catalog
            .expect_load_table()
            .returning_st(|_| Box::pin(async move { Ok(make_v2_table()) }));

        mock_catalog
            .expect_update_table_with_base()
            .times(1) // Should only be called once since error is not retryable
            .returning_st(move |_, _| {
                Box::pin(async move {
                    Err(Error::new(ErrorKind::Unexpected, "Non-retryable error")
                        .with_retryable(false))
                })
            });

        mock_catalog
    }

    #[tokio::test]
    async fn test_commit_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails twice then succeeds
        let mock_catalog = setup_mock_catalog_with_retryable_errors(Some(2), 3);

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_ok(), "Transaction should eventually succeed");
    }

    #[tokio::test]
    async fn test_commit_non_retryable_error() {
        // Create a test table with retry properties
        let table = setup_test_table("3");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that fails with non-retryable error
        let mock_catalog = setup_mock_catalog_with_non_retryable_error();

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail immediately");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), "Non-retryable error");
            assert!(!err.retryable(), "Error should not be retryable");
        }
    }

    #[tokio::test]
    async fn test_commit_max_retries_exceeded() {
        // Create a test table with retry properties (only allow 2 retries)
        let table = setup_test_table("2");

        // Create a transaction with a simple update action
        let tx = create_test_transaction(&table);

        // Create a mock catalog that always fails with retryable error
        let mock_catalog = setup_mock_catalog_with_retryable_errors(None, 3); // Initial attempt + 2 retries = 3 total attempts

        // Commit the transaction
        let result = tx.commit(&mock_catalog).await;

        // Verify the result
        assert!(result.is_err(), "Transaction should fail after max retries");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
            assert_eq!(err.message(), "Commit conflict");
            assert!(err.retryable(), "Error should be retryable");
        }
    }

    #[tokio::test]
    async fn test_stale_append_refuses_recreated_table_before_writing_manifests() {
        let table = make_v2_minimal_table();
        let replacement = with_table_uuid(&table, Uuid::from_u128(2));
        assert_eq!(
            table.metadata().current_schema(),
            replacement.metadata().current_schema()
        );
        assert_ne!(table.metadata().uuid(), replacement.metadata().uuid());

        let commit_uuid = Uuid::from_u128(3);
        let manifest_path = format!(
            "{}/metadata/{commit_uuid}-m0.avro",
            replacement.metadata().location()
        );
        assert!(!replacement.file_io().exists(&manifest_path).await.unwrap());

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/replacement-compatible.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .set_commit_uuid(commit_uuid)
            .add_data_files([data_file])
            .apply(tx)
            .unwrap();

        let expected_metadata = replacement.metadata_ref();
        let loaded_replacement = replacement.clone();
        let mut catalog = MockCatalog::new();
        catalog.expect_load_table().times(2).returning_st(move |_| {
            let replacement = loaded_replacement.clone();
            Box::pin(async move { Ok(replacement) })
        });
        catalog.expect_update_table().times(0);

        let err = tx.commit(&catalog).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
        assert_eq!(
            err.message(),
            "Cannot commit transaction: table UUID changed"
        );
        assert!(!err.retryable());

        let after = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(after.metadata(), expected_metadata.as_ref());
        assert_eq!(after.metadata().snapshots().len(), 0);
        assert!(!after.file_io().exists(&manifest_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_commit_refuses_recreated_table_between_cas_attempts() {
        let table = setup_test_table("3");
        let refreshed_metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([(
                "concurrent.key".to_string(),
                "concurrent.value".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let refreshed = table.clone().with_metadata(Arc::new(refreshed_metadata));
        let replacement = with_table_uuid(&refreshed, Uuid::from_u128(4));
        let expected_metadata = replacement.metadata_ref();
        let tx = create_test_transaction(&table);

        let load_attempt = AtomicU32::new(0);
        let loaded_refreshed = refreshed.clone();
        let loaded_replacement = replacement.clone();
        let mut catalog = MockCatalog::new();
        catalog.expect_load_table().times(3).returning_st(move |_| {
            let table = if load_attempt.fetch_add(1, Ordering::SeqCst) == 0 {
                loaded_refreshed.clone()
            } else {
                loaded_replacement.clone()
            };
            Box::pin(async move { Ok(table) })
        });
        catalog
            .expect_update_table_with_base()
            .times(1)
            .returning_st(|_, _| {
                Box::pin(async {
                    Err(
                        Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict")
                            .with_retryable(true),
                    )
                })
            });

        let err = tx.commit(&catalog).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CatalogCommitConflicts);
        assert_eq!(
            err.message(),
            "Cannot commit transaction: table UUID changed"
        );
        assert!(!err.retryable());

        let after = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(after.metadata(), expected_metadata.as_ref());
        assert_eq!(
            after.metadata().snapshots().collect::<Vec<_>>(),
            replacement.metadata().snapshots().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_commit_same_uuid_rebase_skips_redundant_property_update() {
        let table = make_v2_table();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([(
                "test.key".to_string(),
                "test.value".to_string(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let refreshed = table.clone().with_metadata(Arc::new(metadata));
        assert_eq!(table.metadata().uuid(), refreshed.metadata().uuid());

        let tx = create_test_transaction(&table);
        let expected = refreshed.clone();
        let mut catalog = MockCatalog::new();
        catalog.expect_load_table().times(1).returning_st(move |_| {
            let refreshed = refreshed.clone();
            Box::pin(async move { Ok(refreshed) })
        });
        catalog.expect_update_table().times(0);

        let committed = tx.commit(&catalog).await.unwrap();

        // Dropping the redundant property update still rebuilds the metadata,
        // and `build()` restamps `last_updated_ms` from the wall clock — so the
        // two stamps differ whenever the two builds land in different
        // milliseconds. Assert the stamp for monotonicity and everything else
        // for equality.
        assert!(
            committed.metadata().last_updated_ms() >= expected.metadata().last_updated_ms(),
            "{} >= {}",
            committed.metadata().last_updated_ms(),
            expected.metadata().last_updated_ms()
        );
        let mut committed_metadata = committed.metadata().clone();
        committed_metadata.last_updated_ms = expected.metadata().last_updated_ms();
        assert_eq!(&committed_metadata, expected.metadata());
        assert_eq!(
            committed
                .metadata()
                .properties()
                .get("test.key")
                .map(String::as_str),
            Some("test.value")
        );
    }
}

#[cfg(test)]
mod test_row_lineage {
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    #[tokio::test]
    async fn test_fast_append_with_row_lineage() {
        // Helper function to create a data file with specified number of rows
        fn file_with_rows(record_count: u64) -> DataFile {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/{record_count}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(record_count)
                .partition(Struct::from_iter([Some(Literal::long(0))]))
                .partition_spec_id(0)
                .build()
                .unwrap()
        }
        let catalog = new_memory_catalog().await;

        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Check initial state - next_row_id should be 0
        assert_eq!(table.metadata().next_row_id(), 0);

        // First fast append with 30 rows
        let tx = Transaction::new(&table);
        let data_file_30 = file_with_rows(30);
        let action = tx.fast_append().add_data_files(vec![data_file_30]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after first append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(0));
        assert_eq!(table.metadata().next_row_id(), 30);

        // Check written manifest for first_row_id
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        assert_eq!(manifest_list.entries().len(), 1);
        let manifest_file = &manifest_list.entries()[0];
        assert_eq!(manifest_file.first_row_id, Some(0));

        // Second fast append with 17 and 11 rows
        let tx = Transaction::new(&table);
        let data_file_17 = file_with_rows(17);
        let data_file_11 = file_with_rows(11);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file_17, data_file_11]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after second append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(30));
        assert_eq!(table.metadata().next_row_id(), 30 + 17 + 11);

        // Check written manifest for first_row_id
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 2);
        let manifest_file = &manifest_list.entries()[1];
        assert_eq!(manifest_file.first_row_id, Some(30));
    }
}
