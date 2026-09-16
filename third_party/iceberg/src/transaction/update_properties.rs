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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::TableProperties;
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableUpdate};

/// A transactional action that updates or removes table properties
///
/// This action is used to modify key-value pairs in a table's metadata
/// properties during a transaction. It supports setting new values for existing keys
/// or adding new keys, as well as removing existing keys. Each key can only be updated
/// or removed in a single action, not both.
pub struct UpdatePropertiesAction {
    updates: HashMap<String, String>,
    removals: HashSet<String>,
}

impl UpdatePropertiesAction {
    /// Creates a new [`UpdatePropertiesAction`] with no updates or removals.
    pub fn new() -> Self {
        UpdatePropertiesAction {
            updates: HashMap::default(),
            removals: HashSet::default(),
        }
    }

    /// Adds a key-value pair to the update set of this action.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to update.
    /// * `value` - The new value to associate with the key.
    ///
    /// # Returns
    ///
    /// The updated [`UpdatePropertiesAction`] with the key-value pair added to the update set.
    pub fn set(mut self, key: String, value: String) -> Self {
        self.updates.insert(key, value);
        self
    }

    /// Adds a key to the removal set of this action.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to remove.
    ///
    /// # Returns
    ///
    /// The updated [`UpdatePropertiesAction`] with the key added to the removal set.
    pub fn remove(mut self, key: String) -> Self {
        self.removals.insert(key);
        self
    }
}

impl Default for UpdatePropertiesAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpdatePropertiesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if let Some(overlapping_key) = self.removals.iter().find(|k| self.updates.contains_key(*k))
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Key {overlapping_key} is present in both removal set and update set"),
            ));
        }

        // Actions are re-applied against a freshly loaded table after an
        // optimistic-concurrency conflict. Drop changes that another writer
        // has already made so an idempotent retry can become a true no-op.
        // Keep reserved-property operations so the metadata builder still
        // rejects them exactly as it did before this filtering.
        let effective_updates = self
            .updates
            .iter()
            .filter(|(key, value)| {
                TableProperties::RESERVED_PROPERTIES.contains(&key.as_str())
                    || table.metadata().properties().get(*key) != Some(*value)
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        let effective_removals = self
            .removals
            .iter()
            .filter(|key| {
                TableProperties::RESERVED_PROPERTIES.contains(&key.as_str())
                    || table.metadata().properties().contains_key(*key)
            })
            .cloned()
            .collect::<Vec<_>>();

        let mut updates = Vec::with_capacity(2);
        if !effective_updates.is_empty() {
            updates.push(TableUpdate::SetProperties {
                updates: effective_updates,
            });
        }
        if !effective_removals.is_empty() {
            updates.push(TableUpdate::RemoveProperties {
                removals: effective_removals,
            });
        }

        Ok(ActionCommit::new(updates, vec![]))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use as_any::Downcast;

    use crate::transaction::Transaction;
    use crate::transaction::action::ApplyTransactionAction;
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::update_properties::UpdatePropertiesAction;
    use crate::transaction::TransactionAction;
    use crate::TableUpdate;

    #[test]
    fn test_update_table_property() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set("a".to_string(), "b".to_string())
            .remove("b".to_string())
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<UpdatePropertiesAction>()
            .unwrap();
        assert_eq!(
            action.updates,
            HashMap::from([("a".to_string(), "b".to_string())])
        );

        assert_eq!(action.removals, HashSet::from(["b".to_string()]));
    }

    #[tokio::test]
    async fn test_matching_updates_and_missing_removals_are_noop() {
        let table = make_v2_table();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([("a".to_string(), "b".to_string())]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));

        let mut commit = Arc::new(
            UpdatePropertiesAction::new()
                .set("a".to_string(), "b".to_string())
                .remove("missing".to_string()),
        )
        .commit(&table)
        .await
        .unwrap();

        assert!(commit.take_updates().is_empty());
        assert!(commit.take_requirements().is_empty());
    }

    #[tokio::test]
    async fn test_changed_updates_and_existing_removals_are_preserved() {
        let table = make_v2_table();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([
                ("change".to_string(), "old".to_string()),
                ("same".to_string(), "value".to_string()),
                ("remove".to_string(), "value".to_string()),
            ]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));

        let mut commit = Arc::new(
            UpdatePropertiesAction::new()
                .set("change".to_string(), "new".to_string())
                .set("same".to_string(), "value".to_string())
                .remove("remove".to_string()),
        )
        .commit(&table)
        .await
        .unwrap();

        assert_eq!(
            commit.take_updates(),
            vec![
                TableUpdate::SetProperties {
                    updates: HashMap::from([("change".to_string(), "new".to_string())]),
                },
                TableUpdate::RemoveProperties {
                    removals: vec!["remove".to_string()],
                },
            ]
        );
    }
}
