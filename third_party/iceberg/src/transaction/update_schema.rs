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

//! Additive, idempotent schema evolution.
//!
//! [`UpdateSchemaAction`] appends new top-level columns to a table's
//! current schema. It is intentionally restricted to the safe subset
//! of Iceberg schema evolution:
//!
//! * **Additive only** — columns can be added, never dropped, renamed,
//!   reordered, or retyped. Existing data files remain readable; the
//!   new columns read back as null for rows written before the change.
//! * **Always optional** — added columns are nullable, so no backfill
//!   or default is required.
//! * **Idempotent** — adding a column whose name already exists is a
//!   no-op, so the same migration can be replayed safely. When every
//!   requested column already exists the action emits no updates at
//!   all (an empty commit).
//!
//! New columns are assigned field-ids starting at `last_column_id + 1`,
//! which the metadata builder reconciles against `last_column_id` on
//! commit. The action emits [`TableUpdate::AddSchema`] plus
//! [`TableUpdate::SetCurrentSchema`] with `schema_id: -1` ("the
//! last-added schema"), guarded by a [`TableRequirement::
//! CurrentSchemaIdMatch`] so concurrent schema changes are rejected by
//! the optimistic lock rather than silently lost.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{NestedField, Schema, Type};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// A column to append, resolved against the live schema at commit time.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingColumn {
    name: String,
    field_type: Type,
    doc: Option<String>,
}

/// Transaction action that additively evolves a table's schema.
///
/// See the [module docs](self) for the guarantees and restrictions.
pub struct UpdateSchemaAction {
    add_columns: Vec<PendingColumn>,
}

impl UpdateSchemaAction {
    /// Creates an empty schema-update action.
    pub fn new() -> Self {
        UpdateSchemaAction {
            add_columns: vec![],
        }
    }

    /// Append a new optional (nullable) top-level column.
    ///
    /// If a column of this name already exists at commit time the
    /// request is skipped, making the action idempotent.
    pub fn add_optional_column(mut self, name: &str, field_type: Type) -> Self {
        self.add_columns.push(PendingColumn {
            name: name.to_string(),
            field_type,
            doc: None,
        });
        self
    }

    /// Like [`add_optional_column`](Self::add_optional_column) but with
    /// a column doc string.
    pub fn add_optional_column_with_doc(
        mut self,
        name: &str,
        field_type: Type,
        doc: &str,
    ) -> Self {
        self.add_columns.push(PendingColumn {
            name: name.to_string(),
            field_type,
            doc: Some(doc.to_string()),
        });
        self
    }
}

impl Default for UpdateSchemaAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let current_schema = table.metadata().current_schema();

        // Field-ids for appended columns start just past the highest id
        // the table has ever assigned. Using `last_column_id` (not the
        // current schema's highest id) guards against reusing an id that
        // an earlier, since-replaced schema already burned.
        let mut next_field_id = table.metadata().last_column_id() + 1;

        let mut fields = current_schema.as_struct().fields().to_vec();
        let mut added = 0usize;
        for col in &self.add_columns {
            // Idempotent: a column that already exists is left untouched.
            // We deliberately do not check that the existing type matches
            // — additive evolution never retypes, and surfacing a type
            // mismatch here would make replay fail rather than no-op.
            if current_schema.field_id_by_name(col.name.as_str()).is_some() {
                continue;
            }
            // Guard against duplicate names within a single request.
            if fields.iter().any(|f| f.name == col.name) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("duplicate column {} in schema migration", col.name),
                ));
            }
            let mut field =
                NestedField::optional(next_field_id, &col.name, col.field_type.clone());
            if let Some(doc) = &col.doc {
                field = field.with_doc(doc);
            }
            fields.push(Arc::new(field));
            next_field_id += 1;
            added += 1;
        }

        if added == 0 {
            // Nothing to do — every requested column already exists.
            // An empty commit is a clean no-op for the catalog.
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        // schema_id here is a placeholder; the metadata builder reassigns
        // it via `reuse_or_create_new_schema_id` when the update applies.
        let new_schema = Schema::builder()
            .with_schema_id(current_schema.schema_id() + 1)
            .with_fields(fields)
            .build()?;

        let updates = vec![
            TableUpdate::AddSchema { schema: new_schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];

        let requirements = vec![TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: current_schema.schema_id(),
        }];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use as_any::Downcast;

    use crate::spec::{PrimitiveType, Type};
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::update_schema::UpdateSchemaAction;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    #[test]
    fn test_update_schema_queues_pending_columns() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let update_schema = tx.update_schema();

        let tx = update_schema
            .add_optional_column("severity", Type::Primitive(PrimitiveType::String))
            .add_optional_column("score", Type::Primitive(PrimitiveType::Long))
            .apply(tx)
            .unwrap();

        let action = (*tx.actions[0])
            .downcast_ref::<UpdateSchemaAction>()
            .unwrap();

        assert_eq!(action.add_columns.len(), 2);
        assert_eq!(action.add_columns[0].name, "severity");
        assert_eq!(action.add_columns[1].name, "score");
    }
}
