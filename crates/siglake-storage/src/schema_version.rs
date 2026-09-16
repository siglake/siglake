//! Monotone publication of `siglake.schema_version.v1`.
//!
//! The property is what `migrate-schema --dry-run` answers "what shape is this
//! table at?" with, and README's rollback contract promises it never goes down.
//! Two migration jobs can overlap — a Helm hook and the operator's Job, or the
//! same Job retried while the first attempt still runs — so the maximum has to
//! be taken against the table state the commit actually lands on, not against a
//! version read before the transaction started.

use std::sync::Arc;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::TableUpdate;

use crate::iceberg::IcebergContext;

/// Transaction action that publishes `max(base, at_least)` for the schema
/// version, recomputed against the base handed to `commit`.
///
/// A precomputed `SetProperties` is wrong twice over: the value was derived
/// from a table read before the transaction opened, and `Transaction::do_commit`
/// replays the actions unchanged against a refreshed base after a lost CAS. In
/// both windows a concurrent migrator's higher version is already published,
/// and replaying the lower one takes it away.
#[derive(Clone, Debug)]
pub struct StampSchemaVersionAtLeastAction {
    at_least: u32,
}

impl StampSchemaVersionAtLeastAction {
    pub fn new(at_least: u32) -> Self {
        Self { at_least }
    }

    /// The version this action would publish onto `table`, or `None` when the
    /// base already records `at_least` or more and the action emits nothing.
    ///
    /// A base whose property is absent or unparseable is not a version, so it
    /// cannot bound anything: [`IcebergContext::schema_version_of`]'s shape
    /// inference is the floor, and the stamp is written. That keeps the stamp
    /// evidence of the columns rather than something a missing property can
    /// silently skip.
    fn resolve(&self, table: &Table) -> Option<u32> {
        let recorded = table
            .metadata()
            .properties()
            .get(siglake_core::SCHEMA_VERSION_PROPERTY_KEY)
            .and_then(|raw| raw.parse::<u32>().ok());
        match recorded {
            Some(at) if at >= self.at_least => None,
            _ => Some(self.at_least.max(IcebergContext::schema_version_of(table))),
        }
    }
}

#[async_trait]
impl TransactionAction for StampSchemaVersionAtLeastAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let Some(version) = self.resolve(table) else {
            // No updates and no requirements: `do_commit` returns the base
            // without a catalog update, so a table already at or above this
            // version does not gain a metadata version per migration run.
            return Ok(ActionCommit::new(vec![], vec![]));
        };
        Ok(ActionCommit::new(
            vec![TableUpdate::SetProperties {
                updates: [(
                    siglake_core::SCHEMA_VERSION_PROPERTY_KEY.to_string(),
                    version.to_string(),
                )]
                .into_iter()
                .collect(),
            }],
            vec![],
        ))
    }
}

/// The transaction [`IcebergContext::stamp_schema_version_at_least`] commits.
///
/// Separate from the context method so a test can commit the production
/// transaction against a catalog decorator of its own.
pub fn stamp_at_least_transaction(table: &Table, at_least: u32) -> iceberg::Result<Transaction> {
    StampSchemaVersionAtLeastAction::new(at_least).apply(Transaction::new(table))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::test_catalog::TestCatalog;
    use iceberg::TableIdent;

    async fn events_table_at(version: u32) -> (tempfile::TempDir, IcebergContext, TableIdent) {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.ensure_events_table().await.unwrap();
        let ident = ice.events_table_ident().clone();
        // Fixture setup: put the table at an exact version, in either
        // direction, which is what the explicit writer is kept for.
        #[allow(clippy::disallowed_methods)]
        ice.stamp_schema_version(&ident, version).await.unwrap();
        (tmp, ice, ident)
    }

    async fn recorded(ice: &IcebergContext, ident: &TableIdent) -> Option<u32> {
        let table = ice.catalog().load_table(ident).await.unwrap();
        table
            .metadata()
            .properties()
            .get(siglake_core::SCHEMA_VERSION_PROPERTY_KEY)
            .and_then(|raw| raw.parse::<u32>().ok())
    }

    /// The pre-fix stamp, written here rather than kept in production: a fixed
    /// property value staged before the commit. Both concurrency regressions
    /// run it in the same interleaving as the monotone action, so the
    /// interleaving is proven to be one that actually loses a version.
    fn fixed_value_transaction(table: &Table, version: u32) -> Transaction {
        let tx = Transaction::new(table);
        tx.update_table_properties()
            .set(
                siglake_core::SCHEMA_VERSION_PROPERTY_KEY.to_string(),
                version.to_string(),
            )
            .apply(tx)
            .unwrap()
    }

    /// A version already recorded above the request is left alone, and the
    /// action emits nothing at all — no metadata version per migration run on a
    /// table that is already ahead.
    #[tokio::test]
    async fn a_base_that_is_ahead_produces_no_update() {
        let (_tmp, ice, ident) = events_table_at(siglake_core::EVENTS_SCHEMA_VERSION + 2).await;
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let before = table.metadata_location().map(str::to_string);

        let action = Arc::new(StampSchemaVersionAtLeastAction::new(
            siglake_core::EVENTS_SCHEMA_VERSION,
        ));
        let mut commit = action.commit(&table).await.unwrap();
        assert!(commit.take_updates().is_empty());
        assert!(commit.take_requirements().is_empty());

        let recorded_after = ice
            .stamp_schema_version_at_least(&ident, siglake_core::EVENTS_SCHEMA_VERSION)
            .await
            .unwrap();
        assert_eq!(recorded_after, siglake_core::EVENTS_SCHEMA_VERSION + 2);
        assert_eq!(
            ice.catalog()
                .load_table(&ident)
                .await
                .unwrap()
                .metadata_location()
                .map(str::to_string),
            before,
            "a no-op stamp must not write a metadata version"
        );
    }

    /// Behind, equal-and-absent, and unparseable: the three bases that must
    /// still get a stamp. `schema_version_of` infers 2 for a table carrying
    /// `attributes`, so the floor never drops below the shape.
    #[tokio::test]
    async fn a_base_that_is_behind_or_unreadable_is_stamped() {
        let (_tmp, ice, ident) = events_table_at(siglake_core::EVENTS_SCHEMA_VERSION - 1).await;
        let want = siglake_core::EVENTS_SCHEMA_VERSION;

        let table = ice.catalog().load_table(&ident).await.unwrap();
        assert_eq!(
            StampSchemaVersionAtLeastAction::new(want).resolve(&table),
            Some(want)
        );
        assert_eq!(
            ice.stamp_schema_version_at_least(&ident, want)
                .await
                .unwrap(),
            want
        );
        assert_eq!(recorded(&ice, &ident).await, Some(want));

        // An unparseable property: not a version, so it bounds nothing.
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let tx = Transaction::new(&table);
        tx.update_table_properties()
            .set(
                siglake_core::SCHEMA_VERSION_PROPERTY_KEY.to_string(),
                "v2-rc1".to_string(),
            )
            .apply(tx)
            .unwrap()
            .commit(ice.catalog().as_ref())
            .await
            .unwrap();
        assert_eq!(
            ice.stamp_schema_version_at_least(&ident, 1).await.unwrap(),
            2,
            "an unreadable property falls back to the inferred shape, not below it"
        );
    }

    /// Stamp a table recording `start` through `stamp`, with a competing
    /// migrator publishing `competing` inside the CAS window of the first
    /// attempt. Returns the version the table records afterwards.
    ///
    /// The interleaving is a [`TestCatalog`] hook on the first
    /// `update_table_with_base`: it releases the competitor, waits for its
    /// commit, and only then runs the conditional UPDATE — which the competitor
    /// has by then invalidated. What follows is `do_commit` replaying the
    /// actions against a base the caller never saw.
    ///
    /// `start` must be below what `stamp` asks for, or the transaction commits
    /// nothing, never reaches the catalog, and the interleaving does not happen
    /// — the timeout below says so rather than hanging.
    async fn under_conflicting_commit<F>(start: u32, competing: u32, stamp: F) -> u32
    where
        F: for<'a> FnOnce(&'a Table) -> Transaction,
    {
        let (_tmp, ice, ident) = events_table_at(start).await;
        let base = ice.catalog().load_table(&ident).await.unwrap();
        // Both halves of the rendezvous: the competitor waits on the barrier,
        // commits, then waits again, so the CAS cannot run until the competing
        // version is published.
        let gate = Arc::new(tokio::sync::Barrier::new(2));
        let catalog = TestCatalog::new(ice.catalog().clone())
            .before_first_update_with_base({
                let gate = gate.clone();
                move || {
                    let gate = gate.clone();
                    async move {
                        gate.wait().await;
                        gate.wait().await;
                    }
                }
            })
            .shared();

        let competitor = {
            let ice = ice.clone();
            let ident = ident.clone();
            let gate = gate.clone();
            async move {
                gate.wait().await;
                // The competing migrator publishes an exact version.
                #[allow(clippy::disallowed_methods)]
                ice.stamp_schema_version(&ident, competing).await.unwrap();
                gate.wait().await;
            }
        };
        let attempt = async {
            stamp(&base).commit(catalog.as_ref()).await.unwrap();
        };
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            tokio::join!(competitor, attempt)
        })
        .await
        .expect("the stamp never reached the CAS window the competitor waits on");

        assert!(
            catalog.fired(),
            "no competing commit landed in the CAS window"
        );
        recorded(&ice, &ident).await.unwrap()
    }

    /// #2553: the version recomputed on a retried attempt is the one the retry's
    /// base carries, so a competing migrator that won the CAS keeps its higher
    /// version.
    #[tokio::test]
    async fn a_retried_attempt_does_not_lower_a_version_it_lost_to() {
        // A table one version behind this binary, so the stamp is a real
        // commit, met by a migrator two versions ahead.
        let want = siglake_core::EVENTS_SCHEMA_VERSION;
        let start = want - 1;
        let competing = want + 2;

        assert_eq!(
            under_conflicting_commit(start, competing, |base| {
                stamp_at_least_transaction(base, want).unwrap()
            })
            .await,
            competing,
            "the retried attempt published its own lower version"
        );

        // The same interleaving with the pre-fix shape: the replay writes the
        // value staged before the conflict, and the higher version is gone.
        assert_eq!(
            under_conflicting_commit(start, competing, |base| fixed_value_transaction(base, want))
                .await,
            want,
            "a fixed property value survived the rebase — this interleaving no \
             longer reproduces the bug and proves nothing"
        );
    }

    /// #2553: a higher version published after the caller read the table, but
    /// before the stamp transaction opens. This is the CLI's window between
    /// `observed_schema_version` and the stamp; `migrate_schema_tests` covers it
    /// through `migrate_one_namespace` itself.
    #[tokio::test]
    async fn a_version_published_after_the_observation_is_not_lowered() {
        let (_tmp, ice, ident) = events_table_at(siglake_core::EVENTS_SCHEMA_VERSION).await;
        let observed = ice.observed_schema_version(&ident).await.unwrap();
        let competing = observed + 2;
        let gate = Arc::new(tokio::sync::Barrier::new(2));

        let competitor = {
            let ice = ice.clone();
            let ident = ident.clone();
            let gate = gate.clone();
            async move {
                // The competing migrator publishes an exact version.
                #[allow(clippy::disallowed_methods)]
                ice.stamp_schema_version(&ident, competing).await.unwrap();
                gate.wait().await;
            }
        };
        let migrator = async {
            // Everything the caller knew is `observed`, read above.
            gate.wait().await;
            ice.stamp_schema_version_at_least(&ident, observed)
                .await
                .unwrap()
        };
        let (_, after) = tokio::join!(competitor, migrator);

        assert_eq!(after, competing, "the stamp reported a stale version");
        assert_eq!(recorded(&ice, &ident).await, Some(competing));

        // The explicit writer in the same position is what the CLI used to
        // call, and it still writes what it is given: the fixtures that claim a
        // version below the table's shape depend on that, and it is why the
        // apply path routes through the monotone operation instead.
        #[allow(clippy::disallowed_methods)]
        ice.stamp_schema_version(&ident, observed).await.unwrap();
        assert_eq!(recorded(&ice, &ident).await, Some(observed));
    }
}
