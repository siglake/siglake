use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::io::FileIO;
use iceberg::spec::Type as IcebergType;
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::Error as IcebergError;
use iceberg::ErrorKind as IcebergErrorKind;
use iceberg::{TableIdent, TableUpdate};
use serde::{Deserialize, Serialize};
use siglake_bloom::Tokenizer;
use siglake_core::index_config::{
    DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode, RetentionPolicy,
    DOC_MAPPING_PROPERTY_KEY,
};
use thiserror::Error;

use crate::iceberg::{
    ascending_identity_sort_order_with_tiebreak, day_partition_spec, minimum_format_version,
    retry_object_write, IcebergContext, QUERY_AUDIT_TABLE, TABLE_NAME,
};

/// The v1 document: every template of the warehouse in one JSON array. Read
/// forever, never written again — see [`IndexTemplateRecord`].
const LEGACY_INDEX_TEMPLATES_PATH: &str = "_siglake/config/index_templates.json";
/// Directory (relative to the warehouse root) holding namespace-scoped
/// template records. Records written before namespace scoping sit directly
/// below this directory and remain readable by the configured default
/// namespace only.
const INDEX_TEMPLATE_RECORDS_DIR: &str = "_siglake/config/index_templates";
const TEMPLATE_VALIDATION_INDEX_ID: &str = "template-validation";
/// Attempts a template-record write gets before it is reported failed. Same
/// linear policy as the delete-task records: a credential refresh recovers in
/// seconds, and this runs behind an API call the caller will treat as durable.
const INDEX_TEMPLATE_WRITE_ATTEMPTS: u32 = 4;

/// Index-template metadata persisted in the warehouse config area.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct IndexTemplate {
    /// Stable template identifier.
    pub template_id: String,
    /// Glob patterns matched against candidate index ids.
    pub index_id_patterns: Vec<String>,
    /// Higher values win when multiple templates match.
    pub priority: i32,
    /// Document mapping to materialize onto the created index.
    pub doc_mapping: DocMapping,
    /// Optional retention policy copied into the created index.
    pub retention: Option<RetentionPolicy>,
}

/// One template id's stored record, at
/// `_siglake/config/index_templates/{namespace}/{template_id}.json`.
///
/// STORAGE LAYOUT. v1 kept every template of the warehouse in one JSON array
/// document and replaced it on every PUT and DELETE. v2 moved each id to a
/// warehouse-root record. Both layouts made templates visible across tenant
/// namespaces. Namespace-scoped records isolate template mappings and
/// retention while preserving the one-writer-owned-key property.
///
/// A delete writes a TOMBSTONE (`template: None`) rather than removing the
/// object, because older compatibility records are never rewritten: without a
/// tombstone, deleting a template that still exists in an older layout would
/// let it reappear on the next read.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexTemplateRecord {
    template_id: String,
    /// `None` is a tombstone: the id is deleted, including any legacy entry.
    #[serde(default)]
    template: Option<IndexTemplate>,
}

/// Typed failures surfaced by the index manager.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum IndexManagerError {
    #[error("index_id `{0}` is system-reserved and may not be used for a user index")]
    ReservedSystemIndexId(String),
    #[error("index `{0}` already exists")]
    IndexAlreadyExists(String),
    #[error("index `{0}` does not exist")]
    IndexNotFound(String),
    #[error("table `{0}` is not a managed index")]
    NotAnIndex(String),
    #[error(
        "field_mappings must preserve the stored prefix at position {position}: expected `{expected}`, got `{actual}`"
    )]
    FieldMappingsPrefixMismatch {
        position: usize,
        expected: String,
        actual: String,
    },
    #[error("field `{field}` changed type in an additive-only update")]
    FieldTypeChanged { field: String },
    #[error(
        "field `{field}` changed required from {stored} to {proposed} in an additive-only update"
    )]
    FieldRequiredChanged {
        field: String,
        stored: bool,
        proposed: bool,
    },
    #[error("new field `{field}` must be nullable when added to an existing index")]
    AppendedFieldMustBeNullable { field: String },
    #[error("timestamp_field changed from `{stored}` to `{proposed}`")]
    TimestampFieldChanged { stored: String, proposed: String },
    #[error("stored table metadata for `{table_name}` points at index_id `{stored_index_id}`")]
    StoredConfigIndexIdMismatch {
        table_name: String,
        stored_index_id: String,
    },
    #[error("template `{0}` must contain at least one index_id pattern")]
    EmptyTemplatePatterns(String),
    #[error(
        "field `{field}` is declared `{declared}` but the live schema of index `{index_id}` holds `{actual}`: re-read the index and retry"
    )]
    IndexSchemaFieldConflict {
        index_id: String,
        field: String,
        declared: String,
        actual: String,
    },
    #[error(
        "index template record `{path}` is keyed by `{record_id}` but holds template `{template_id}`"
    )]
    TemplateRecordIdMismatch {
        path: String,
        record_id: String,
        template_id: String,
    },
}

impl IndexTemplate {
    /// Validate the template's identifier, pattern list, and materialized mapping.
    pub fn validate(&self) -> Result<()> {
        IndexConfig {
            index_id: self.template_id.clone(),
            doc_mapping: IndexConfig::builtin_events().doc_mapping,
            retention: None,
            index_at_flush: None,
        }
        .validate()
        .with_context(|| format!("validate template_id `{}`", self.template_id))?;

        if self.index_id_patterns.is_empty() {
            return Err(IndexManagerError::EmptyTemplatePatterns(self.template_id.clone()).into());
        }

        IndexConfig {
            index_id: TEMPLATE_VALIDATION_INDEX_ID.to_string(),
            doc_mapping: self.doc_mapping.clone(),
            retention: self.retention.clone(),
            index_at_flush: None,
        }
        .validate()
        .with_context(|| format!("validate template `{}` doc_mapping", self.template_id))?;

        Ok(())
    }
}

/// Builtin events-shaped logs template used for auto-create.
pub fn builtin_logs_template() -> IndexTemplate {
    IndexTemplate {
        template_id: "siglake-logs".to_string(),
        index_id_patterns: vec!["siglake-logs-*".to_string()],
        priority: 0,
        doc_mapping: IndexConfig::builtin_events().doc_mapping,
        retention: None,
    }
}

/// Builtin OTLP traces template used for auto-create.
pub fn builtin_traces_template() -> IndexTemplate {
    IndexTemplate {
        template_id: "siglake-traces".to_string(),
        index_id_patterns: vec!["siglake-traces-*".to_string()],
        priority: 0,
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                FieldMapping {
                    name: "timestamp".to_string(),
                    field_type: FieldType::Datetime,
                    required: true,
                },
                FieldMapping {
                    name: "trace_id".to_string(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    required: false,
                },
                FieldMapping {
                    name: "span_id".to_string(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    required: false,
                },
                FieldMapping {
                    name: "parent_span_id".to_string(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    required: false,
                },
                FieldMapping {
                    name: "service".to_string(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    required: false,
                },
                FieldMapping {
                    name: "name".to_string(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    required: false,
                },
                FieldMapping {
                    name: "kind".to_string(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    required: false,
                },
                FieldMapping {
                    name: "status_code".to_string(),
                    field_type: FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                    required: false,
                },
                FieldMapping {
                    name: "duration_nanos".to_string(),
                    field_type: FieldType::Long,
                    required: false,
                },
                // Exact unix nanoseconds for `timestamp`, which is microsecond
                // precise (2026-09-06 timestamp contract). Declared last so the
                // existing columns keep their field ids.
                FieldMapping {
                    name: siglake_core::TIMESTAMP_NS_COLUMN.to_string(),
                    field_type: FieldType::Long,
                    required: true,
                },
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec![
                "service".to_string(),
                "name".to_string(),
                "kind".to_string(),
                "status_code".to_string(),
                // Intentionally high-cardinality: row-group blooms on `trace_id`
                // sharply prune Jaeger point lookups for one trace.
                "trace_id".to_string(),
            ],
            default_search_fields: vec!["name".to_string()],
        },
        retention: None,
    }
}

impl IcebergContext {
    /// Identifier for an index table in this context's namespace.
    pub fn index_table_ident(&self, index_id: &str) -> TableIdent {
        TableIdent::new(self.namespace().clone(), index_id.to_string())
    }

    /// Create a new managed index table from an [`IndexConfig`].
    pub async fn create_index(&self, config: &IndexConfig) -> Result<TableIdent> {
        config.validate().context("validate index config")?;
        reject_reserved_index_id(config.index_id.as_str())?;

        let table_ident = self.index_table_ident(config.index_id.as_str());
        if self.catalog().table_exists(&table_ident).await? {
            return Err(IndexManagerError::IndexAlreadyExists(config.index_id.clone()).into());
        }

        let arrow_schema = config.to_arrow_schema();
        let iceberg_schema = arrow_schema_to_schema(arrow_schema.as_ref())
            .with_context(|| format!("arrow -> iceberg schema conversion ({})", config.index_id))?;
        // The `timestamp_ns` tiebreak only means anything as the exact twin of
        // the canonical `timestamp` column (the builtin events and traces
        // templates). An index that declares some other event-time field may
        // also carry an unrelated column of that name, and sorting by it would
        // stamp an order that is not a time order at all.
        let timestamp_field = config.doc_mapping.timestamp_field.as_str();
        let tiebreak =
            (timestamp_field == "timestamp").then_some(siglake_core::TIMESTAMP_NS_COLUMN);
        let sort_order = ascending_identity_sort_order_with_tiebreak(
            &iceberg_schema,
            timestamp_field,
            tiebreak,
            "index sort order",
        )?;
        let iceberg_schema_ref: iceberg::spec::SchemaRef = iceberg_schema.into();
        let partition_name = format!("day_{}", config.doc_mapping.timestamp_field);
        let partition_spec = day_partition_spec(
            &iceberg_schema_ref,
            config.doc_mapping.timestamp_field.as_str(),
            partition_name.as_str(),
        )?
        .into_unbound();
        let config_json = serde_json::to_string(config)
            .with_context(|| format!("serialize index config `{}`", config.index_id))?;
        let creation = iceberg::TableCreation::builder()
            .name(config.index_id.clone())
            .schema((*iceberg_schema_ref).clone())
            .format_version(minimum_format_version(&iceberg_schema_ref))
            .sort_order(sort_order)
            .partition_spec(partition_spec)
            .properties([(DOC_MAPPING_PROPERTY_KEY.to_string(), config_json)])
            .build();

        self.catalog()
            .create_table(self.namespace(), creation)
            .await
            .with_context(|| format!("create index table {table_ident}"))?;

        Ok(table_ident)
    }

    /// Load one managed index configuration by table name.
    pub async fn get_index(&self, index_id: &str) -> Result<Option<IndexConfig>> {
        let table_ident = self.index_table_ident(index_id);
        if !self.catalog().table_exists(&table_ident).await? {
            return Ok(None);
        }
        let table = self
            .catalog()
            .load_table(&table_ident)
            .await
            .with_context(|| format!("load_table {table_ident}"))?;
        index_config_from_table(index_id, &table)
    }

    /// The Iceberg table UUID behind one managed index, or `None` when the
    /// name resolves to no table.
    ///
    /// #2661: the identity a per-index WAL directory's
    /// [`siglake_wal::OWNER_FILE`] is compared against. Read through
    /// `load_table`, deliberately NOT through the bounded-staleness table
    /// cache: right after a `DELETE`+`POST` of the same index id, a cache
    /// entry within its TTL still holds the DROPPED table, and comparing a
    /// marker against that uuid is the exact confusion the marker exists to
    /// resolve. One extra metadata read per index per drain cycle.
    pub async fn index_table_uuid(&self, index_id: &str) -> Result<Option<String>> {
        let table_ident = self.index_table_ident(index_id);
        if !self.catalog().table_exists(&table_ident).await? {
            return Ok(None);
        }
        let table = self
            .catalog()
            .load_table(&table_ident)
            .await
            .with_context(|| format!("load_table {table_ident}"))?;
        Ok(Some(table.metadata().uuid().to_string()))
    }

    /// One managed index's stored config together with the UUID of the table
    /// it was read from, resolved from a SINGLE `load_table`.
    ///
    /// #2837: a delete task records the incarnation it was accepted against.
    /// Resolving the config and the uuid separately (`get_index` then
    /// [`Self::index_table_uuid`]) can straddle a `DELETE` + `POST` of the same
    /// index id and bind the task to an incarnation whose config was never the
    /// one validated. One load answers both, so the pair is consistent by
    /// construction. Deliberately NOT through the bounded-staleness table
    /// cache, for the reason [`Self::index_table_uuid`] gives.
    pub(crate) async fn index_config_with_table_uuid(
        &self,
        index_id: &str,
    ) -> Result<Option<(IndexConfig, String)>> {
        let table_ident = self.index_table_ident(index_id);
        if !self.catalog().table_exists(&table_ident).await? {
            return Ok(None);
        }
        let table = self
            .catalog()
            .load_table(&table_ident)
            .await
            .with_context(|| format!("load_table {table_ident}"))?;
        let uuid = table.metadata().uuid().to_string();
        Ok(index_config_from_table(index_id, &table)?.map(|config| (config, uuid)))
    }

    /// List every managed index in the current namespace.
    pub async fn list_indexes(&self) -> Result<Vec<IndexConfig>> {
        let mut indexes = Vec::new();
        for table_ident in self.catalog().list_tables(self.namespace()).await? {
            let index_id = table_ident.name().to_string();
            let table = self
                .catalog()
                .load_table(&table_ident)
                .await
                .with_context(|| format!("load_table {table_ident}"))?;
            if let Some(config) = index_config_from_table(index_id.as_str(), &table)? {
                indexes.push(config);
            }
        }
        indexes.sort_by(|left, right| left.index_id.cmp(&right.index_id));
        Ok(indexes)
    }

    /// Additively update a managed index's stored config and live Iceberg schema.
    pub async fn update_index(&self, config: &IndexConfig) -> Result<()> {
        let prepared = self.prepare_index_update(config).await?;
        self.commit_index_update(prepared).await
    }

    /// Validate `config` against the stored mapping and build the transaction
    /// that would store it.
    ///
    /// Split from [`Self::commit_index_update`] only so the two-writer
    /// regressions can land a competing commit in the window between this
    /// validation and the CAS — the window a transaction rebase used to hide.
    /// [`SetIndexMappingAction`] re-derives every refusal this validation can
    /// produce against the base the commit actually uses; what this adds is a
    /// typed error for the uncontended case without paying for a commit
    /// attempt.
    async fn prepare_index_update(&self, config: &IndexConfig) -> Result<PreparedIndexUpdate> {
        config.validate().context("validate index config")?;

        let stored = self
            .get_index(config.index_id.as_str())
            .await?
            .ok_or_else(|| IndexManagerError::IndexNotFound(config.index_id.clone()))?;
        validate_additive_update(&stored, config)?;

        let table_ident = self.index_table_ident(config.index_id.as_str());
        let table = self
            .catalog()
            .load_table(&table_ident)
            .await
            .with_context(|| format!("load_table {table_ident}"))?;

        let desired_arrow = config.to_arrow_schema();
        let desired_iceberg = arrow_schema_to_schema(desired_arrow.as_ref())
            .with_context(|| format!("convert desired schema for `{}`", config.index_id))?;
        let current_schema = table.metadata().current_schema().clone();

        let desired_columns: Vec<(String, IcebergType)> = desired_iceberg
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.clone(), field.field_type.as_ref().clone()))
            .collect();
        let mut missing_columns = Vec::new();
        for (name, field_type) in &desired_columns {
            if current_schema.field_id_by_name(name.as_str()).is_none() {
                missing_columns.push((name.clone(), field_type.clone()));
            }
        }

        let config_json = serde_json::to_string(config)
            .with_context(|| format!("serialize index config `{}`", config.index_id))?;
        let existing_json = table
            .metadata()
            .properties()
            .get(DOC_MAPPING_PROPERTY_KEY)
            .cloned();
        if missing_columns.is_empty() && existing_json.as_deref() == Some(config_json.as_str()) {
            return Ok(PreparedIndexUpdate {
                table_ident,
                transaction: None,
                refusal: MappingRefusal::default(),
            });
        }

        // The mapping action goes in FIRST so it validates against the pristine
        // commit base rather than the base plus this transaction's own schema
        // additions.
        let mapping = SetIndexMappingAction::new(
            config.clone(),
            config_json,
            desired_columns,
            missing_columns
                .iter()
                .map(|(name, _)| name.clone())
                .collect(),
        );
        let refusal = mapping.refusal();
        let mut tx = Transaction::new(&table);
        tx = mapping.apply(tx).context("SetIndexMappingAction::apply")?;
        if !missing_columns.is_empty() {
            // Iceberg appends additive columns to the live schema's tail with
            // fresh field ids. That means newly added index fields land after the
            // residual `attributes` column, which is correct: the live table
            // schema is authoritative after creation; `to_arrow_schema()` is only
            // the creation-time declaration.
            let mut action = tx.update_schema();
            for (name, field_type) in missing_columns {
                action = action.add_optional_column(name.as_str(), field_type);
            }
            tx = action.apply(tx).context("UpdateSchemaAction::apply")?;
        }

        Ok(PreparedIndexUpdate {
            table_ident,
            transaction: Some(tx),
            refusal,
        })
    }

    /// Commit a transaction built by [`Self::prepare_index_update`].
    async fn commit_index_update(&self, prepared: PreparedIndexUpdate) -> Result<()> {
        let PreparedIndexUpdate {
            table_ident,
            transaction,
            refusal,
        } = prepared;
        let Some(tx) = transaction else {
            return Ok(());
        };

        if let Err(err) = tx.commit(self.catalog().as_ref()).await {
            // The mapping action refuses against the base the commit actually
            // used, which is not the base this update was prepared against
            // whenever another writer got there first. Report its typed reason,
            // not the Iceberg wrapper, so the API answers the same way it does
            // when the conflict is visible before the transaction.
            if let Some(refused) = refusal.take() {
                return Err(anyhow::Error::new(refused).context(format!(
                    "index `{}` changed while this update was in flight",
                    table_ident.name()
                )));
            }
            return Err(anyhow::Error::new(err).context("update_index commit"));
        }
        self.invalidate_cached_table(&table_ident).await;

        Ok(())
    }

    /// Drop a managed index table's catalog entry.
    ///
    /// Committed files remain at the table location. The current orphan-GC
    /// entry point first reloads the table from the catalog, so it cannot
    /// reclaim those files after this removes the entry.
    pub async fn delete_index(&self, index_id: &str) -> Result<bool> {
        let table_ident = self.index_table_ident(index_id);
        if !self.catalog().table_exists(&table_ident).await? {
            return Ok(false);
        }
        let table = self
            .catalog()
            .load_table(&table_ident)
            .await
            .with_context(|| format!("load_table {table_ident}"))?;
        if index_config_from_table(index_id, &table)?.is_none() {
            return Err(IndexManagerError::NotAnIndex(index_id.to_string()).into());
        }

        self.catalog()
            .drop_table(&table_ident)
            .await
            .with_context(|| format!("drop table {table_ident}"))?;
        self.invalidate_cached_table(&table_ident).await;
        Ok(true)
    }

    /// Upsert one stored index template.
    ///
    /// Writes only this template's own record, so it cannot lose — or be lost
    /// by — a concurrent edit of a different template from another process. Two
    /// writers racing on the SAME template id are last-write-wins; there is no
    /// CAS (see `docs/LIMITATIONS.md`).
    pub async fn put_index_template(&self, template: &IndexTemplate) -> Result<()> {
        template.validate()?;
        self.write_index_template_record(&IndexTemplateRecord {
            template_id: template.template_id.clone(),
            template: Some(template.clone()),
        })
        .await
    }

    /// Delete one stored index template. Returns whether it existed.
    ///
    /// Records a tombstone for the id rather than replacing the template set,
    /// which is what keeps a concurrent PUT of a different template — and a
    /// compatibility entry of this one — from being resurrected or erased.
    pub async fn delete_index_template(&self, template_id: &str) -> Result<bool> {
        if !self
            .read_index_templates()
            .await?
            .iter()
            .any(|template| template.template_id == template_id)
        {
            return Ok(false);
        }
        self.write_index_template_record(&IndexTemplateRecord {
            template_id: template_id.to_string(),
            template: None,
        })
        .await?;
        Ok(true)
    }

    /// List stored index templates for this context's namespace.
    pub async fn list_index_templates(&self) -> Result<Vec<IndexTemplate>> {
        let mut templates = self.read_index_templates().await?;
        templates.sort_by(|left, right| left.template_id.cmp(&right.template_id));
        Ok(templates)
    }

    /// Resolve the winning template for a candidate index id.
    pub async fn resolve_index_template(&self, index_id: &str) -> Result<Option<IndexConfig>> {
        let mut by_id = HashMap::new();
        let builtin_logs = builtin_logs_template();
        by_id.insert(builtin_logs.template_id.clone(), builtin_logs);
        let builtin_traces = builtin_traces_template();
        by_id.insert(builtin_traces.template_id.clone(), builtin_traces);
        for template in self.read_index_templates().await? {
            by_id.insert(template.template_id.clone(), template);
        }

        let mut matches: Vec<IndexTemplate> = by_id
            .into_values()
            .filter(|template| {
                template
                    .index_id_patterns
                    .iter()
                    .any(|pattern| glob_match(pattern.as_str(), index_id))
            })
            .collect();
        matches.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| left.template_id.cmp(&right.template_id))
        });
        let Some(template) = matches.into_iter().next() else {
            return Ok(None);
        };

        let config = IndexConfig {
            index_id: index_id.to_string(),
            doc_mapping: template.doc_mapping,
            retention: template.retention,
            // Template-resolved indexes inherit the deployment default.
            index_at_flush: None,
        };
        config
            .validate()
            .with_context(|| format!("validate resolved index config `{index_id}`"))?;
        Ok(Some(config))
    }

    /// Ensure a matching managed index exists, auto-creating via templates.
    pub async fn ensure_index(&self, index_id: &str) -> Result<Option<TableIdent>> {
        if self.get_index(index_id).await?.is_some() {
            return Ok(Some(self.index_table_ident(index_id)));
        }

        let Some(config) = self.resolve_index_template(index_id).await? else {
            return Ok(None);
        };
        match self.create_index(&config).await {
            Ok(table_ident) => Ok(Some(table_ident)),
            Err(err) if is_table_already_exists_error(&err) => {
                Ok(Some(self.index_table_ident(index_id)))
            }
            Err(err) => Err(err),
        }
    }

    /// Every stored template of this namespace. The configured default
    /// namespace retains compatibility reads of the v1 document and the v2
    /// warehouse-root records; other namespaces start empty. A
    /// namespace-scoped live record or tombstone shadows either older layout.
    async fn read_index_templates(&self) -> Result<Vec<IndexTemplate>> {
        let mut by_id = BTreeMap::new();
        if self.is_default_namespace() {
            by_id.extend(
                self.read_legacy_index_templates()
                    .await?
                    .into_iter()
                    .map(|template| (template.template_id.clone(), template)),
            );
            let root_dir = format!("{INDEX_TEMPLATE_RECORDS_DIR}/");
            apply_index_template_records(
                &mut by_id,
                self.read_index_template_records(&root_dir).await?,
            );
        }
        let dir = self.index_template_records_dir();
        apply_index_template_records(&mut by_id, self.read_index_template_records(&dir).await?);
        Ok(by_id.into_values().collect())
    }

    /// The v1 whole-warehouse document, if this warehouse ever wrote one.
    /// Read-only forever: nothing rewrites or deletes it, so a downgrade keeps
    /// working for the templates it already holds. Nothing migrates itself.
    async fn read_legacy_index_templates(&self) -> Result<Vec<IndexTemplate>> {
        let path = self.legacy_index_templates_location();
        let file_io = self.warehouse_file_io();
        if !file_exists(file_io, path.as_str()).await? {
            return Ok(Vec::new());
        }

        let bytes = file_io
            .new_input(path.as_str())
            .with_context(|| format!("new_input {path}"))?
            .read()
            .await
            .with_context(|| format!("read {path}"))?;
        let templates: Vec<IndexTemplate> =
            serde_json::from_slice(bytes.as_ref()).with_context(|| format!("parse {path}"))?;
        for template in &templates {
            template.validate()?;
        }
        Ok(templates)
    }

    /// One LIST plus one GET per record. A record that exists but cannot be
    /// parsed is an error, never a silently skipped template: an acknowledged
    /// PUT that quietly stops applying is the defect this layout exists to
    /// prevent, and a template that vanishes changes what auto-create builds.
    async fn read_index_template_records(&self, dir: &str) -> Result<Vec<IndexTemplateRecord>> {
        let op = self.warehouse_object_store()?;
        let entries = match op.list(dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == opendal::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(err).with_context(|| format!("list index templates {dir}")),
        };
        let mut records = Vec::new();
        for entry in entries {
            let rel = entry.path();
            let Some(filename) = rel.strip_prefix(dir) else {
                continue;
            };
            // A compatibility read of the warehouse-root directory must not
            // descend into another namespace's segment, even on an object
            // store whose LIST implementation returns recursive results.
            if filename.contains('/') {
                continue;
            }
            // `*.staged` siblings orphaned by a crashed writer are inert.
            if !filename.ends_with(".json") {
                continue;
            }
            let bytes = op
                .read(rel)
                .await
                .with_context(|| format!("read index template record {rel}"))?;
            let record: IndexTemplateRecord = serde_json::from_slice(&bytes.to_bytes())
                .with_context(|| format!("parse index template record {rel}"))?;
            if let Some(template) = &record.template {
                if template.template_id != record.template_id {
                    return Err(IndexManagerError::TemplateRecordIdMismatch {
                        path: rel.to_string(),
                        record_id: record.template_id.clone(),
                        template_id: template.template_id.clone(),
                    }
                    .into());
                }
                template.validate()?;
            }
            records.push(record);
        }
        Ok(records)
    }

    /// Persist one template id's record. The key is the template id, so this
    /// replaces only that template — an edit of another id cannot be erased by
    /// it, and cannot erase it.
    ///
    /// The replacement must also be ATOMIC for a concurrent reader. An S3 PUT
    /// is; the local-filesystem backend truncates and then writes, and a reader
    /// listing at that moment gets a zero-byte record, which
    /// `read_index_template_records` reports as a parse error rather than
    /// skipping. Where the backend supports rename (fs does; S3 does not and
    /// does not need it) the record is staged under a unique sibling key and
    /// renamed into place.
    async fn write_index_template_record(&self, record: &IndexTemplateRecord) -> Result<()> {
        let op = self.warehouse_object_store()?;
        // `template_id` validates as an index id — `[a-z0-9][a-z0-9_-]{0,127}`,
        // no leading `_` — so it is already a safe single-segment object key.
        let rel = format!(
            "{}{}.json",
            self.index_template_records_dir(),
            record.template_id
        );
        let body = serde_json::to_vec(record)
            .with_context(|| format!("serialize index template {}", record.template_id))?;
        let staged = op
            .info()
            .full_capability()
            .rename
            .then(|| format!("{rel}.{}.staged", uuid::Uuid::now_v7()));
        let written = staged.as_deref().unwrap_or(rel.as_str());
        let retries = retry_object_write(
            "index template",
            written,
            INDEX_TEMPLATE_WRITE_ATTEMPTS,
            || {
                let op = op.clone();
                let written = written.to_string();
                let body = body.clone();
                async move {
                    op.write(&written, body)
                        .await
                        .map(|_| ())
                        .map_err(Into::into)
                }
            },
        )
        .await?;
        if retries > 0 {
            tracing::info!(
                rel,
                attempt = retries + 1,
                "index template record write succeeded on retry"
            );
        }
        if let Some(staged) = staged {
            op.rename(staged.as_str(), rel.as_str())
                .await
                .with_context(|| format!("rename index template record {staged} -> {rel}"))?;
        }
        Ok(())
    }

    fn legacy_index_templates_location(&self) -> String {
        format!(
            "{}/{}",
            self.warehouse_url().trim_end_matches('/'),
            LEGACY_INDEX_TEMPLATES_PATH
        )
    }

    fn index_template_records_dir(&self) -> String {
        format!("{INDEX_TEMPLATE_RECORDS_DIR}/{}/", self.namespace())
    }
}

fn apply_index_template_records(
    by_id: &mut BTreeMap<String, IndexTemplate>,
    records: Vec<IndexTemplateRecord>,
) {
    for record in records {
        match record.template {
            Some(template) => {
                by_id.insert(record.template_id, template);
            }
            None => {
                by_id.remove(&record.template_id);
            }
        }
    }
}

fn reject_reserved_index_id(index_id: &str) -> Result<()> {
    if is_reserved_index_id(index_id) {
        return Err(IndexManagerError::ReservedSystemIndexId(index_id.to_string()).into());
    }
    Ok(())
}

/// Index ids siglake owns and a caller may not claim.
///
/// This used to also reserve `candidates`, `episodes`, `episode_events`,
/// `detector_runs` and `webhook_dlq` — the detection pipeline's tables, back
/// when siglake built them in. They are ordinary indexes now, declared by the
/// consumer that writes them, so reserving their names would prevent that
/// consumer from creating its own tables. Only siglake's own remain.
fn is_reserved_index_id(index_id: &str) -> bool {
    matches!(index_id, QUERY_AUDIT_TABLE)
}

pub(crate) fn index_config_from_table(
    index_id: &str,
    table: &Table,
) -> Result<Option<IndexConfig>> {
    if let Some(json) = table.metadata().properties().get(DOC_MAPPING_PROPERTY_KEY) {
        let config: IndexConfig = serde_json::from_str(json)
            .with_context(|| format!("parse {DOC_MAPPING_PROPERTY_KEY} for `{index_id}`"))?;
        config
            .validate()
            .with_context(|| format!("validate stored index config `{index_id}`"))?;
        if config.index_id != index_id {
            return Err(IndexManagerError::StoredConfigIndexIdMismatch {
                table_name: index_id.to_string(),
                stored_index_id: config.index_id,
            }
            .into());
        }
        return Ok(Some(config));
    }

    if index_id == TABLE_NAME {
        return Ok(Some(IndexConfig::builtin_events()));
    }

    Ok(None)
}

pub(crate) fn loaded_table_index_config(table: &Table) -> Result<Option<IndexConfig>> {
    index_config_from_table(table.identifier().name(), table)
}

pub(crate) fn text_field_tokenizers(config: &IndexConfig) -> HashMap<String, Tokenizer> {
    config
        .doc_mapping
        .field_mappings
        .iter()
        .filter_map(|field| match &field.field_type {
            siglake_core::index_config::FieldType::Text { tokenizer } => Some((
                field.name.clone(),
                tokenizer
                    .as_deref()
                    .and_then(Tokenizer::parse)
                    .unwrap_or(Tokenizer::Default),
            )),
            _ => None,
        })
        .collect()
}

/// The reason [`SetIndexMappingAction`] refused an update, shared with the
/// caller that built the transaction.
///
/// An action can only fail with an [`iceberg::Error`], and the retry loop hands
/// that back through layers that erase the type. The slot carries the typed
/// refusal out so `update_index` answers a conflict caught at the commit base
/// exactly as it answers one caught before the transaction.
#[derive(Clone, Debug, Default)]
struct MappingRefusal(Arc<Mutex<Option<IndexManagerError>>>);

impl MappingRefusal {
    fn record(&self, err: IndexManagerError) {
        *self.0.lock().unwrap() = Some(err);
    }

    fn take(&self) -> Option<IndexManagerError> {
        self.0.lock().unwrap().take()
    }
}

/// A validated managed-index update, waiting to be committed.
struct PreparedIndexUpdate {
    table_ident: TableIdent,
    /// `None` when the stored mapping and schema already match the request.
    transaction: Option<Transaction>,
    refusal: MappingRefusal,
}

/// Transaction action that re-validates a managed index's additive-only
/// contract against the base handed to `commit`, then stores the mapping.
///
/// The validation `update_index` runs before building the transaction is
/// against the base it loaded; `Transaction::do_commit` refreshes the base on
/// every attempt and replays the actions without repeating it. A precomputed
/// `SetProperties` would then store a mapping validated against a table that no
/// longer exists — dropping a concurrent writer's column from the stored
/// mapping while the column stays in the Iceberg schema. Deriving the update
/// here means every attempt, first or retried, is validated against the base it
/// commits onto.
///
/// Conflicts are refused, not merged: an update whose `field_mappings` no
/// longer extend the stored ones was computed from a state that is gone, and
/// only the caller can say what it meant to append.
struct SetIndexMappingAction {
    proposed: IndexConfig,
    proposed_json: String,
    /// Every column the proposed mapping declares, with the Iceberg type it
    /// declares it as.
    desired_columns: Vec<(String, IcebergType)>,
    /// Columns this transaction's [`iceberg::transaction::UpdateSchemaAction`]
    /// will add. A declared column absent from the base and from this list
    /// would be stored as a mapping over a column no schema has.
    pending_columns: Vec<String>,
    refusal: MappingRefusal,
}

impl SetIndexMappingAction {
    fn new(
        proposed: IndexConfig,
        proposed_json: String,
        desired_columns: Vec<(String, IcebergType)>,
        pending_columns: Vec<String>,
    ) -> Self {
        Self {
            proposed,
            proposed_json,
            desired_columns,
            pending_columns,
            refusal: MappingRefusal::default(),
        }
    }

    fn refusal(&self) -> MappingRefusal {
        self.refusal.clone()
    }

    /// Record `err` and turn it into a non-retryable Iceberg error: a stale
    /// mapping does not become valid by being retried.
    fn refuse(&self, err: IndexManagerError) -> IcebergError {
        let message = err.to_string();
        self.refusal.record(err);
        IcebergError::new(IcebergErrorKind::DataInvalid, message)
    }
}

#[async_trait::async_trait]
impl TransactionAction for SetIndexMappingAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let index_id = self.proposed.index_id.as_str();
        let stored = index_config_from_table(index_id, table)
            .map_err(|err| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("read stored mapping for `{index_id}`"),
                )
                .with_source(err)
            })?
            .ok_or_else(|| self.refuse(IndexManagerError::NotAnIndex(index_id.to_string())))?;

        if let Err(err) = validate_additive_update(&stored, &self.proposed) {
            return Err(match err.downcast::<IndexManagerError>() {
                Ok(typed) => self.refuse(typed),
                // `validate_additive_update` only ever fails with a typed
                // refusal; keep the error rather than assert the shape.
                Err(other) => IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("validate additive update for `{index_id}`"),
                )
                .with_source(other),
            });
        }

        // A name the base schema already carries is left alone by
        // `UpdateSchemaAction` whatever type it holds, so a mapping that
        // declares a different type for it would be stored over a column that
        // contradicts it.
        let base_schema = table.metadata().current_schema();
        for (name, declared) in &self.desired_columns {
            match base_schema.field_by_name(name.as_str()) {
                Some(field) if field.field_type.as_ref() != declared => {
                    return Err(self.refuse(IndexManagerError::IndexSchemaFieldConflict {
                        index_id: index_id.to_string(),
                        field: name.clone(),
                        declared: declared.to_string(),
                        actual: field.field_type.to_string(),
                    }));
                }
                Some(_) => {}
                None if self.pending_columns.contains(name) => {}
                None => {
                    return Err(self.refuse(IndexManagerError::IndexSchemaFieldConflict {
                        index_id: index_id.to_string(),
                        field: name.clone(),
                        declared: declared.to_string(),
                        actual: "<missing>".to_string(),
                    }));
                }
            }
        }

        if table.metadata().properties().get(DOC_MAPPING_PROPERTY_KEY) == Some(&self.proposed_json)
        {
            // Another writer stored exactly this mapping. Emitting the property
            // again would turn an idempotent update into a metadata version.
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        Ok(ActionCommit::new(
            vec![TableUpdate::SetProperties {
                updates: [(
                    DOC_MAPPING_PROPERTY_KEY.to_string(),
                    self.proposed_json.clone(),
                )]
                .into_iter()
                .collect(),
            }],
            vec![],
        ))
    }
}

fn validate_additive_update(stored: &IndexConfig, proposed: &IndexConfig) -> Result<()> {
    if stored.doc_mapping.timestamp_field != proposed.doc_mapping.timestamp_field {
        return Err(IndexManagerError::TimestampFieldChanged {
            stored: stored.doc_mapping.timestamp_field.clone(),
            proposed: proposed.doc_mapping.timestamp_field.clone(),
        }
        .into());
    }

    for (position, stored_field) in stored.doc_mapping.field_mappings.iter().enumerate() {
        let Some(proposed_field) = proposed.doc_mapping.field_mappings.get(position) else {
            return Err(IndexManagerError::FieldMappingsPrefixMismatch {
                position,
                expected: stored_field.name.clone(),
                actual: "<missing>".to_string(),
            }
            .into());
        };
        if stored_field.name != proposed_field.name {
            return Err(IndexManagerError::FieldMappingsPrefixMismatch {
                position,
                expected: stored_field.name.clone(),
                actual: proposed_field.name.clone(),
            }
            .into());
        }
        if stored_field.field_type != proposed_field.field_type {
            return Err(IndexManagerError::FieldTypeChanged {
                field: stored_field.name.clone(),
            }
            .into());
        }
        if stored_field.required != proposed_field.required {
            return Err(IndexManagerError::FieldRequiredChanged {
                field: stored_field.name.clone(),
                stored: stored_field.required,
                proposed: proposed_field.required,
            }
            .into());
        }
    }

    for appended in &proposed.doc_mapping.field_mappings[stored.doc_mapping.field_mappings.len()..]
    {
        if appended.required {
            return Err(IndexManagerError::AppendedFieldMustBeNullable {
                field: appended.name.clone(),
            }
            .into());
        }
    }

    Ok(())
}

fn is_table_already_exists_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<iceberg::Error>()
        .is_some_and(|iceberg_err| iceberg_err.kind() == IcebergErrorKind::TableAlreadyExists)
}

async fn file_exists(file_io: &FileIO, path: &str) -> Result<bool> {
    file_io
        .exists(path)
        .await
        .with_context(|| format!("exists {path}"))
}

fn glob_match(pattern: &str, value: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == value;
    }

    let parts: Vec<&str> = pattern.split('*').collect();
    let anchored_start = !pattern.starts_with('*');
    let anchored_end = !pattern.ends_with('*');
    let mut cursor = 0usize;
    let mut part_index = 0usize;

    if anchored_start {
        let prefix = parts[0];
        if !value.starts_with(prefix) {
            return false;
        }
        cursor = prefix.len();
        part_index = 1;
    }

    let end_exclusive = if anchored_end {
        parts.len().saturating_sub(1)
    } else {
        parts.len()
    };
    for part in &parts[part_index..end_exclusive] {
        if part.is_empty() {
            continue;
        }
        let Some(found) = value[cursor..].find(part) else {
            return false;
        };
        cursor += found + part.len();
    }

    if anchored_end {
        let suffix = parts[parts.len() - 1];
        value[cursor..].ends_with(suffix)
    } else {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::test_catalog::TestCatalog;
    use arrow_array::{RecordBatch, StringArray, TimestampMicrosecondArray};
    use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, MappingMode};

    fn parquet_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(parquet_files(&path));
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
        files
    }

    fn mapping_field(name: &str, field_type: FieldType, required: bool) -> FieldMapping {
        FieldMapping {
            name: name.to_string(),
            field_type,
            required,
        }
    }

    fn logs_config(index_id: &str) -> IndexConfig {
        IndexConfig {
            index_id: index_id.to_string(),
            doc_mapping: DocMapping {
                mode: MappingMode::Dynamic,
                field_mappings: vec![
                    mapping_field("ts", FieldType::Datetime, true),
                    mapping_field(
                        "message",
                        FieldType::Text {
                            tokenizer: Some("default".to_string()),
                        },
                        false,
                    ),
                    mapping_field(
                        "service",
                        FieldType::Text {
                            tokenizer: Some("raw".to_string()),
                        },
                        false,
                    ),
                ],
                timestamp_field: "ts".to_string(),
                tag_fields: vec!["service".to_string()],
                default_search_fields: vec!["message".to_string()],
            },
            retention: Some(RetentionPolicy {
                period_secs: 3600,
                schedule: None,
            }),
            index_at_flush: None,
        }
    }

    fn logs_batch(config: &IndexConfig, messages: &[&str], start_micros: i64) -> RecordBatch {
        RecordBatch::try_new(
            config.to_arrow_schema(),
            vec![
                std::sync::Arc::new(
                    TimestampMicrosecondArray::from(
                        (0..messages.len())
                            .map(|offset| Some(start_micros + offset as i64))
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone("+00:00"),
                ),
                std::sync::Arc::new(StringArray::from(
                    messages.iter().copied().map(Some).collect::<Vec<_>>(),
                )),
                std::sync::Arc::new(StringArray::from(vec![Some("api"); messages.len()])),
                // Dynamic mappings append the residual attributes column.
                std::sync::Arc::new(StringArray::from(vec![None::<&str>; messages.len()])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn create_get_list_and_delete_index_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = IcebergContext::open(&warehouse).await.unwrap();
        let config = logs_config("logs");

        let table_ident = ice.create_index(&config).await.unwrap();
        assert_eq!(table_ident, ice.index_table_ident("logs"));

        let table = ice.catalog().load_table(&table_ident).await.unwrap();
        let stored_json = table
            .metadata()
            .properties()
            .get(DOC_MAPPING_PROPERTY_KEY)
            .expect("doc-mapping property");
        assert_eq!(
            serde_json::from_str::<IndexConfig>(stored_json).unwrap(),
            config
        );

        assert_eq!(ice.get_index("logs").await.unwrap(), Some(config.clone()));
        assert_eq!(
            ice.list_indexes().await.unwrap(),
            vec![IndexConfig::builtin_events(), config.clone()]
        );

        assert!(ice.delete_index("logs").await.unwrap());
        assert_eq!(ice.get_index("logs").await.unwrap(), None);
        assert!(!ice.delete_index("logs").await.unwrap());

        // `query_audit` is siglake's own and stays reserved.
        let err = ice
            .create_index(&logs_config("query_audit"))
            .await
            .unwrap_err();
        assert_eq!(
            err.downcast_ref::<IndexManagerError>(),
            Some(&IndexManagerError::ReservedSystemIndexId(
                "query_audit".to_string()
            ))
        );

        // `candidates` is NOT. It used to be, because siglake built the
        // detection tables in; they are declared by the consumer that writes
        // them now, so reserving the name would stop that consumer creating
        // its own output table.
        ice.create_index(&logs_config("candidates"))
            .await
            .expect("a consumer must be able to declare `candidates` as its own index");
    }

    #[tokio::test]
    async fn dropping_committed_index_retains_files_and_recreation_is_a_new_table() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = IcebergContext::open(&warehouse).await.unwrap();
        let config = logs_config("logs");
        let table_ident = ice.create_index(&config).await.unwrap();

        ice.append_to_table(
            &table_ident,
            logs_batch(&config, &["row from dropped table"], 1_700_000_000_000_000),
            &["service"],
        )
        .await
        .unwrap();
        let dropped = ice.catalog().load_table(&table_ident).await.unwrap();
        let dropped_uuid = dropped.metadata().uuid();
        let dropped_location = dropped.metadata().location().to_string();
        let table_dir = url::Url::parse(&dropped_location)
            .unwrap()
            .to_file_path()
            .unwrap();
        let dropped_data_files = parquet_files(&table_dir);
        assert_eq!(dropped_data_files.len(), 1, "one committed data file");

        assert!(ice.delete_index("logs").await.unwrap());
        assert!(
            dropped_data_files.iter().all(|path| path.exists()),
            "dropping the catalog entry must leave committed files in place"
        );
        let gc_error = ice
            .gc_orphans(
                &table_ident,
                crate::iceberg::GcOptions {
                    min_age: std::time::Duration::ZERO,
                    apply: false,
                },
            )
            .await
            .unwrap_err();
        assert!(
            gc_error.to_string().contains("load_table"),
            "orphan GC should fail while the table is absent from the catalog: {gc_error:#}"
        );

        ice.create_index(&config).await.unwrap();
        let replacement = ice.catalog().load_table(&table_ident).await.unwrap();
        assert_ne!(replacement.metadata().uuid(), dropped_uuid);
        assert_eq!(replacement.metadata().location(), dropped_location);
        drop(replacement);

        ice.append_to_table(
            &table_ident,
            logs_batch(
                &config,
                &["replacement row one", "replacement row two"],
                1_800_000_000_000_000,
            ),
            &["service"],
        )
        .await
        .unwrap();
        assert!(
            dropped_data_files.iter().all(|path| path.exists()),
            "recreation must not remove the dropped table's files"
        );

        let session = datafusion::prelude::SessionContext::new();
        assert!(ice
            .register_index_with_datafusion(&session, "logs")
            .await
            .unwrap());
        let batches = session
            .sql("SELECT message FROM logs ORDER BY ts")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let messages: Vec<&str> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(Option::unwrap)
            })
            .collect();
        assert_eq!(messages, ["replacement row one", "replacement row two"]);
    }

    /// A fresh warehouse holds siglake's own tables and NOTHING else.
    ///
    /// `IcebergContext::open` used to also provision five detection tables --
    /// a storage engine creating one particular consumer's output. It does not
    /// any more, so a warehouse nobody has written to contains `events` and
    /// the audit table, and every other table is something a caller declared.
    #[tokio::test]
    async fn a_fresh_warehouse_provisions_only_siglakes_own_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = IcebergContext::open(&warehouse).await.unwrap();

        assert_eq!(
            ice.get_index("events").await.unwrap(),
            Some(IndexConfig::builtin_events())
        );
        assert_eq!(
            ice.list_indexes().await.unwrap(),
            vec![IndexConfig::builtin_events()],
            "a fresh warehouse provisioned a table siglake does not own"
        );
    }

    #[tokio::test]
    async fn update_index_adds_nullable_columns_and_rejects_non_additive_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = IcebergContext::open(&warehouse).await.unwrap();
        let original = logs_config("logs");
        ice.create_index(&original).await.unwrap();

        let mut updated = original.clone();
        updated.doc_mapping.field_mappings.push(mapping_field(
            "severity",
            FieldType::Text {
                tokenizer: Some("raw".to_string()),
            },
            false,
        ));
        updated.doc_mapping.tag_fields.push("severity".to_string());
        updated.retention = None;
        ice.update_index(&updated).await.unwrap();

        let table = ice
            .catalog()
            .load_table(&ice.index_table_ident("logs"))
            .await
            .unwrap();
        let schema = table.metadata().current_schema();
        assert!(schema.field_id_by_name("severity").is_some());
        let stored_json = table
            .metadata()
            .properties()
            .get(DOC_MAPPING_PROPERTY_KEY)
            .unwrap();
        assert_eq!(
            serde_json::from_str::<IndexConfig>(stored_json).unwrap(),
            updated
        );

        let mut changed_type = updated.clone();
        changed_type.doc_mapping.field_mappings[2].field_type = FieldType::Long;
        let err = ice.update_index(&changed_type).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<IndexManagerError>(),
            Some(&IndexManagerError::FieldTypeChanged {
                field: "service".to_string()
            })
        );

        let mut removed = updated.clone();
        removed.doc_mapping.field_mappings.remove(2);
        removed.doc_mapping.tag_fields.clear();
        let err = ice.update_index(&removed).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<IndexManagerError>(),
            Some(&IndexManagerError::FieldMappingsPrefixMismatch {
                position: 2,
                expected: "service".to_string(),
                actual: "severity".to_string(),
            })
        );

        let mut reordered = updated.clone();
        reordered.doc_mapping.field_mappings.swap(1, 2);
        let err = ice.update_index(&reordered).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<IndexManagerError>(),
            Some(&IndexManagerError::FieldMappingsPrefixMismatch {
                position: 1,
                expected: "message".to_string(),
                actual: "service".to_string(),
            })
        );

        let mut new_required = updated.clone();
        new_required.doc_mapping.field_mappings.push(mapping_field(
            "status_code",
            FieldType::Long,
            true,
        ));
        let err = ice.update_index(&new_required).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<IndexManagerError>(),
            Some(&IndexManagerError::AppendedFieldMustBeNullable {
                field: "status_code".to_string()
            })
        );

        let mut changed_timestamp = updated.clone();
        changed_timestamp
            .doc_mapping
            .field_mappings
            .push(mapping_field("ingested_at", FieldType::Datetime, true));
        changed_timestamp.doc_mapping.timestamp_field = "ingested_at".to_string();
        let err = ice.update_index(&changed_timestamp).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<IndexManagerError>(),
            Some(&IndexManagerError::TimestampFieldChanged {
                stored: "ts".to_string(),
                proposed: "ingested_at".to_string(),
            })
        );
    }

    /// One managed index and the two writers of the #2552 regressions.
    async fn index_with_two_writers(warehouse: &std::path::Path) -> (IcebergContext, IndexConfig) {
        let ice = IcebergContext::open(warehouse).await.unwrap();
        let original = logs_config("logs");
        ice.create_index(&original).await.unwrap();
        (ice, original)
    }

    async fn stored_mapping_of(ice: &IcebergContext, index_id: &str) -> IndexConfig {
        ice.get_index(index_id).await.unwrap().unwrap()
    }

    /// A stale retention edit must not erase a concurrently added column from
    /// the stored mapping (#2552).
    ///
    /// Writer B prepares an edit of the mapping it read; writer A appends
    /// `severity` and commits; B's transaction then rebases onto A's commit.
    /// `Transaction::do_commit` replays the actions against the refreshed base
    /// without repeating the caller's validation, so a precomputed
    /// `SetProperties` stored B's three-field mapping over A's four-field one —
    /// leaving `severity` in the Iceberg schema with nothing in the mapping to
    /// populate it.
    #[tokio::test]
    async fn a_stale_retention_edit_is_refused_after_a_concurrent_column_addition() {
        let tmp = tempfile::tempdir().unwrap();
        let (ice, original) = index_with_two_writers(&tmp.path().join("warehouse")).await;

        let mut retention_only = original.clone();
        retention_only.retention = Some(RetentionPolicy {
            period_secs: 7200,
            schedule: None,
        });
        let prepared = ice.prepare_index_update(&retention_only).await.unwrap();

        let mut with_column = original.clone();
        with_column.doc_mapping.field_mappings.push(mapping_field(
            "severity",
            FieldType::Long,
            false,
        ));
        ice.update_index(&with_column).await.unwrap();

        let err = ice.commit_index_update(prepared).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<IndexManagerError>(),
            Some(&IndexManagerError::FieldMappingsPrefixMismatch {
                position: 3,
                expected: "severity".to_string(),
                actual: "<missing>".to_string(),
            }),
            "a stale update was not refused against the actual commit base: {err:#}"
        );

        assert_eq!(
            stored_mapping_of(&ice, "logs").await,
            with_column,
            "the stale update dropped a committed column from the stored mapping"
        );
        let table = ice
            .catalog()
            .load_table(&ice.index_table_ident("logs"))
            .await
            .unwrap();
        assert!(table
            .metadata()
            .current_schema()
            .field_id_by_name("severity")
            .is_some());
    }

    /// Two writers appending the SAME column name with different types: the
    /// loser must be refused, not stored (#2552).
    ///
    /// `UpdateSchemaAction` skips a name the schema already carries without
    /// comparing types, so the losing writer used to keep the winner's `string`
    /// column and store a mapping declaring it `long` — every subsequent write
    /// to the index would then be built against a mapping the schema rejects.
    #[tokio::test]
    async fn incompatible_same_name_additions_refuse_the_losing_writer() {
        let tmp = tempfile::tempdir().unwrap();
        let (ice, original) = index_with_two_writers(&tmp.path().join("warehouse")).await;

        let mut as_long = original.clone();
        as_long
            .doc_mapping
            .field_mappings
            .push(mapping_field("level", FieldType::Long, false));
        let prepared = ice.prepare_index_update(&as_long).await.unwrap();

        let mut as_text = original.clone();
        as_text.doc_mapping.field_mappings.push(mapping_field(
            "level",
            FieldType::Text {
                tokenizer: Some("raw".to_string()),
            },
            false,
        ));
        ice.update_index(&as_text).await.unwrap();

        let err = ice.commit_index_update(prepared).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<IndexManagerError>(),
            Some(&IndexManagerError::FieldTypeChanged {
                field: "level".to_string()
            }),
            "the losing writer was not refused: {err:#}"
        );

        assert_eq!(stored_mapping_of(&ice, "logs").await, as_text);
        let table = ice
            .catalog()
            .load_table(&ice.index_table_ident("logs"))
            .await
            .unwrap();
        let level = table
            .metadata()
            .current_schema()
            .field_by_name("level")
            .expect("the winning writer's column")
            .clone();
        assert_eq!(
            level.field_type.as_ref(),
            &IcebergType::Primitive(iceberg::spec::PrimitiveType::String),
            "the stored mapping and the live schema disagree on `level`"
        );
    }

    /// Two writers committing the SAME additive update: the loser is a no-op,
    /// not a refusal and not a second metadata version (#2552).
    #[tokio::test]
    async fn identical_concurrent_additions_stay_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let (ice, original) = index_with_two_writers(&tmp.path().join("warehouse")).await;

        let mut updated = original.clone();
        updated
            .doc_mapping
            .field_mappings
            .push(mapping_field("severity", FieldType::Long, false));
        let prepared = ice.prepare_index_update(&updated).await.unwrap();
        ice.update_index(&updated).await.unwrap();

        let ident = ice.index_table_ident("logs");
        let after_winner = ice.catalog().load_table(&ident).await.unwrap();
        let winner_location = after_winner.metadata_location().unwrap().to_string();

        ice.commit_index_update(prepared)
            .await
            .expect("an identical additive update must stay idempotent");

        let after_loser = ice.catalog().load_table(&ident).await.unwrap();
        assert_eq!(
            after_loser.metadata_location().unwrap(),
            winner_location,
            "a replayed identical update produced a new metadata version"
        );
        assert_eq!(stored_mapping_of(&ice, "logs").await, updated);
        let severity_columns = after_loser
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .filter(|field| field.name == "severity")
            .count();
        assert_eq!(severity_columns, 1);
    }

    /// A retried attempt is re-validated against the base it retries onto
    /// (#2552).
    ///
    /// The competing addition lands after this transaction validated its base
    /// and before its conditional UPDATE, so the first attempt loses the CAS and
    /// `do_commit` replays the actions on the winner's base. That replay must
    /// refuse, not store the mapping the first attempt validated.
    ///
    /// The prepare/commit interleaving above covers the base refresh at the top
    /// of `do_commit`. This covers the other half — the attempt that validated
    /// its base, lost the CAS, and is replayed against a base the caller never
    /// saw — by running the competing writer's update inside the first
    /// `update_table_with_base`, from a [`TestCatalog`] hook.
    #[tokio::test]
    async fn a_retried_attempt_is_revalidated_against_the_base_it_lost_to() {
        let tmp = tempfile::tempdir().unwrap();
        let (ice, original) = index_with_two_writers(&tmp.path().join("warehouse")).await;

        let mut as_long = original.clone();
        as_long
            .doc_mapping
            .field_mappings
            .push(mapping_field("level", FieldType::Long, false));
        let prepared = ice.prepare_index_update(&as_long).await.unwrap();

        let mut as_text = original.clone();
        as_text.doc_mapping.field_mappings.push(mapping_field(
            "level",
            FieldType::Text {
                tokenizer: Some("raw".to_string()),
            },
            false,
        ));
        let catalog = TestCatalog::new(ice.catalog().clone())
            .before_first_update_with_base({
                let ice = ice.clone();
                let competing = as_text.clone();
                move || {
                    let ice = ice.clone();
                    let competing = competing.clone();
                    async move {
                        ice.update_index(&competing)
                            .await
                            .expect("the competing writer must commit");
                    }
                }
            })
            .shared();

        let outcome = prepared
            .transaction
            .expect("the update is not a no-op")
            .commit(catalog.as_ref())
            .await;
        assert!(
            catalog.fired(),
            "no competing commit landed in the CAS window"
        );
        let Err(err) = outcome else {
            panic!("the replayed attempt stored a mapping its base contradicts");
        };
        assert_eq!(
            prepared.refusal.take(),
            Some(IndexManagerError::FieldTypeChanged {
                field: "level".to_string()
            }),
            "the retried attempt was refused for another reason: {err}"
        );
        assert_eq!(stored_mapping_of(&ice, "logs").await, as_text);
    }

    /// The mapping action refuses a declared column the base schema holds under
    /// a different type, or does not hold at all (#2552).
    ///
    /// Neither shape is reachable through `update_index` today — the mapping
    /// prefix check catches the concurrent cases first — but the action is what
    /// stands between `UpdateSchemaAction`'s name-only idempotency and a stored
    /// mapping the schema contradicts, so it is tested directly.
    #[tokio::test]
    async fn the_mapping_action_refuses_a_column_the_base_schema_contradicts() {
        let tmp = tempfile::tempdir().unwrap();
        let (ice, original) = index_with_two_writers(&tmp.path().join("warehouse")).await;
        let table = ice
            .catalog()
            .load_table(&ice.index_table_ident("logs"))
            .await
            .unwrap();
        let json = serde_json::to_string(&original).unwrap();
        let long = IcebergType::Primitive(iceberg::spec::PrimitiveType::Long);

        let retyped = SetIndexMappingAction::new(
            original.clone(),
            json.clone(),
            vec![("ts".to_string(), long.clone())],
            vec![],
        );
        let refusal = retyped.refusal();
        let Err(err) = Arc::new(retyped).commit(&table).await else {
            panic!("a mapping the schema contradicts was accepted");
        };
        assert!(!err.retryable(), "a stale mapping is not worth retrying");
        assert_eq!(
            refusal.take(),
            Some(IndexManagerError::IndexSchemaFieldConflict {
                index_id: "logs".to_string(),
                field: "ts".to_string(),
                declared: "long".to_string(),
                actual: "timestamptz".to_string(),
            })
        );

        let absent = SetIndexMappingAction::new(
            original.clone(),
            json,
            vec![("nowhere".to_string(), long)],
            vec![],
        );
        let refusal = absent.refusal();
        assert!(Arc::new(absent).commit(&table).await.is_err());
        assert_eq!(
            refusal.take(),
            Some(IndexManagerError::IndexSchemaFieldConflict {
                index_id: "logs".to_string(),
                field: "nowhere".to_string(),
                declared: "long".to_string(),
                actual: "<missing>".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn templates_round_trip_and_resolution_rules_work() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = IcebergContext::open(&warehouse).await.unwrap();

        let low = IndexTemplate {
            template_id: "alpha".to_string(),
            index_id_patterns: vec!["logs-*".to_string()],
            priority: 1,
            doc_mapping: logs_config("placeholder").doc_mapping,
            retention: None,
        };
        let mut high = low.clone();
        high.template_id = "zeta".to_string();
        high.priority = 2;
        ice.put_index_template(&low).await.unwrap();
        ice.put_index_template(&high).await.unwrap();

        let listed = ice.list_index_templates().await.unwrap();
        assert_eq!(listed, vec![low.clone(), high.clone()]);

        let resolved = ice
            .resolve_index_template("logs-prod")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resolved.index_id, "logs-prod");
        assert_eq!(resolved.doc_mapping, high.doc_mapping);

        high.priority = 1;
        ice.put_index_template(&high).await.unwrap();
        let resolved = ice
            .resolve_index_template("logs-prod")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resolved.doc_mapping, low.doc_mapping);

        let builtin = ice
            .resolve_index_template("siglake-logs-app")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            builtin.doc_mapping,
            IndexConfig::builtin_events().doc_mapping
        );

        let builtin = ice
            .resolve_index_template("siglake-traces-default")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(builtin.doc_mapping, builtin_traces_template().doc_mapping);

        let shadow = IndexTemplate {
            template_id: "siglake-logs".to_string(),
            index_id_patterns: vec!["siglake-logs-*".to_string()],
            priority: 0,
            doc_mapping: low.doc_mapping.clone(),
            retention: Some(RetentionPolicy {
                period_secs: 60,
                schedule: None,
            }),
        };
        ice.put_index_template(&shadow).await.unwrap();
        let resolved = ice
            .resolve_index_template("siglake-logs-shadowed")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resolved.doc_mapping, shadow.doc_mapping);
        assert_eq!(resolved.retention, shadow.retention);

        assert!(ice.delete_index_template("alpha").await.unwrap());
        assert!(!ice.delete_index_template("missing").await.unwrap());
    }

    #[tokio::test]
    async fn ensure_index_auto_creates_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = IcebergContext::open(&warehouse).await.unwrap();
        let template = IndexTemplate {
            template_id: "logs".to_string(),
            index_id_patterns: vec!["tenant-*".to_string()],
            priority: 0,
            doc_mapping: logs_config("placeholder").doc_mapping,
            retention: None,
        };
        ice.put_index_template(&template).await.unwrap();

        let first = ice.ensure_index("tenant-a").await.unwrap();
        let second = ice.ensure_index("tenant-a").await.unwrap();
        assert_eq!(first, Some(ice.index_table_ident("tenant-a")));
        assert_eq!(second, Some(ice.index_table_ident("tenant-a")));
        assert_eq!(
            ice.get_index("tenant-a").await.unwrap().unwrap().index_id,
            "tenant-a"
        );
        assert_eq!(ice.ensure_index("unmatched").await.unwrap(), None);
    }

    #[test]
    fn builtin_traces_template_compiles_expected_schema() {
        let template = builtin_traces_template();
        let schema = IndexConfig {
            index_id: "siglake-traces-default".to_string(),
            doc_mapping: template.doc_mapping.clone(),
            retention: None,
            index_at_flush: None,
        }
        .to_arrow_schema();
        let fields = schema.fields();

        assert_eq!(template.template_id, "siglake-traces");
        assert_eq!(
            template.index_id_patterns,
            vec!["siglake-traces-*".to_string()]
        );
        assert_eq!(
            template.doc_mapping.tag_fields,
            vec![
                "service".to_string(),
                "name".to_string(),
                "kind".to_string(),
                "status_code".to_string(),
                "trace_id".to_string(),
            ]
        );
        assert_eq!(
            template.doc_mapping.default_search_fields,
            vec!["name".to_string()]
        );

        assert_eq!(fields.len(), 11);
        assert_eq!(fields[0].name(), "timestamp");
        assert_eq!(fields[0].data_type(), &siglake_core::timestamp_data_type());
        assert!(!fields[0].is_nullable());

        for name in [
            "trace_id",
            "span_id",
            "parent_span_id",
            "service",
            "name",
            "kind",
            "status_code",
        ] {
            let field = schema.field_with_name(name).unwrap();
            assert_eq!(field.data_type(), &arrow_schema::DataType::Utf8);
            assert!(field.is_nullable(), "{name} should be nullable");
        }

        let duration = schema.field_with_name("duration_nanos").unwrap();
        assert_eq!(duration.data_type(), &arrow_schema::DataType::Int64);
        assert!(duration.is_nullable());

        let residual = schema.field_with_name("attributes").unwrap();
        assert_eq!(residual.data_type(), &arrow_schema::DataType::Utf8);
        assert!(residual.is_nullable());
    }

    #[test]
    fn glob_match_covers_supported_patterns() {
        assert!(glob_match("prefix*", "prefix-value"));
        assert!(glob_match("*suffix", "value-suffix"));
        assert!(glob_match("mid*dle", "middle"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exactly"));
        assert!(glob_match("a**c", "abbbc"));
        assert!(!glob_match("foo*bar", "foo-baz"));
    }
}
