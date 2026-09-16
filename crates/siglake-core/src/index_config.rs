//! Search-engine-style index configuration and Arrow schema compilation.
//!
//! An index config declares the typed columns that belong to one logical index.
//! The generated Arrow schema is the Iceberg-facing contract: declared fields in
//! order, followed by the residual `attributes` JSON string column.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::de::{self, MapAccess, Visitor};
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::with_field_id;

/// Text tokenizer names accepted by validation. These are name-coupled to the
/// canonical `siglake-bloom::Tokenizer` registry; `siglake-core` deliberately
/// avoids depending on that crate directly.
pub const ALLOWED_TEXT_TOKENIZERS: &[&str] = &["default", "raw", "stem"];

/// Iceberg table-property key under which the doc-mapping JSON is stored.
pub const DOC_MAPPING_PROPERTY_KEY: &str = "siglake.doc_mapping.v1";

const MAX_FIELD_MAPPINGS: usize = 256;
const RESIDUAL_ATTRIBUTES_FIELD: &str = "attributes";
// Keep this list in sync with the directory constants in siglake-wal
// (`ACTIVE_DIR`, `SEALED_DIR`, `PROCESSING_DIR`, `COMMITTED_DIR`,
// `CONSUMERS_DIR`) without depending on that crate from siglake-core.
/// Directory names the WAL layout owns, and which therefore cannot be a tenant
/// id or an index id.
///
/// `list_layout_dirs` deliberately skips child directories with these names so
/// the legacy flat layout is not mistaken for a set of tenants. A tenant or
/// index that IS one of them is written to disk and then never enumerated —
/// accepted, durable, permanently unqueryable, no error anywhere.
///
/// Public because both validators must read the same list. It was already
/// enforced for index ids and not for tenant ids, which is the drift this
/// export exists to prevent.
pub const RESERVED_WAL_LAYOUT_DIRS: &[&str] =
    &["active", "sealed", "processing", "committed", "consumers"];

/// One declared field type in a document mapping.
///
/// Deliberately no unsigned integer variants: the compiled schema must survive
/// the strict `iceberg::arrow::arrow_schema_to_schema` conversion, and Iceberg
/// only supports signed integer and floating-point primitives here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum FieldType {
    /// Full-text/string field.
    Text {
        /// Named tokenizer/analyzer to use for this field.
        #[serde(default)]
        tokenizer: Option<String>,
    },
    /// Signed 64-bit integer.
    Long,
    /// IEEE-754 64-bit floating-point.
    Double,
    /// Boolean field.
    Bool,
    /// Event-time timestamp with timezone.
    Datetime,
    /// Opaque binary payload.
    Bytes,
    /// JSON stored as a string for Arrow/Iceberg compatibility.
    Json,
}

/// One named field declaration inside a document mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldMapping {
    /// Column name.
    pub name: String,
    /// Declared field type.
    #[serde(flatten)]
    pub field_type: FieldType,
    /// Whether the column is required on ingest.
    #[serde(default)]
    pub required: bool,
}

impl<'de> Deserialize<'de> for FieldMapping {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["name", "type", "required", "tokenizer"];
        const FIELD_TYPES: &[&str] = &[
            "text", "long", "double", "bool", "datetime", "bytes", "json",
        ];

        struct FieldMappingVisitor;

        impl<'de> Visitor<'de> for FieldMappingVisitor {
            type Value = FieldMapping;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a field mapping object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut name = None;
                let mut required = None;
                let mut field_type = None;
                let mut tokenizer = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "name" => {
                            if name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value()?);
                        }
                        "required" => {
                            if required.is_some() {
                                return Err(de::Error::duplicate_field("required"));
                            }
                            required = Some(map.next_value()?);
                        }
                        "type" => {
                            if field_type.is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                            field_type = Some(map.next_value::<String>()?);
                        }
                        "tokenizer" => {
                            if tokenizer.is_some() {
                                return Err(de::Error::duplicate_field("tokenizer"));
                            }
                            tokenizer = Some(map.next_value::<Option<String>>()?);
                        }
                        other => return Err(de::Error::unknown_field(other, FIELDS)),
                    }
                }

                let name = name.ok_or_else(|| de::Error::missing_field("name"))?;
                let required = required.unwrap_or(false);
                let field_type = match field_type
                    .ok_or_else(|| de::Error::missing_field("type"))?
                    .as_str()
                {
                    "text" => FieldType::Text {
                        tokenizer: tokenizer.unwrap_or(None),
                    },
                    "long" => {
                        reject_tokenizer::<A::Error>(&tokenizer)?;
                        FieldType::Long
                    }
                    "double" => {
                        reject_tokenizer::<A::Error>(&tokenizer)?;
                        FieldType::Double
                    }
                    "bool" => {
                        reject_tokenizer::<A::Error>(&tokenizer)?;
                        FieldType::Bool
                    }
                    "datetime" => {
                        reject_tokenizer::<A::Error>(&tokenizer)?;
                        FieldType::Datetime
                    }
                    "bytes" => {
                        reject_tokenizer::<A::Error>(&tokenizer)?;
                        FieldType::Bytes
                    }
                    "json" => {
                        reject_tokenizer::<A::Error>(&tokenizer)?;
                        FieldType::Json
                    }
                    other => return Err(de::Error::unknown_variant(other, FIELD_TYPES)),
                };

                Ok(FieldMapping {
                    name,
                    field_type,
                    required,
                })
            }
        }

        deserializer.deserialize_map(FieldMappingVisitor)
    }
}

// `FieldMapping` cannot derive `ToSchema` correctly because it flattens the
// internally tagged, deny-unknown-fields `FieldType`. Keep this real serde type
// so `field_mapping_schema_matches_wire_format` can detect wire-format drift.
/// OpenAPI wire schema for an index field mapping.
///
/// The `type` field selects one of seven mapping shapes. `tokenizer` is valid
/// only for `text`, and every shape rejects unknown fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum FieldMappingSchema {
    /// Full-text/string field. The only variant that accepts `tokenizer`.
    Text {
        /// Column name.
        name: String,
        /// Whether the column is required on ingest.
        #[serde(default)]
        required: bool,
        /// Named tokenizer/analyzer to use for this field. Serialized even when
        /// absent (as `null`) to match the index mapping wire format.
        #[serde(default)]
        tokenizer: Option<String>,
    },
    /// Signed 64-bit integer.
    Long {
        name: String,
        #[serde(default)]
        required: bool,
    },
    /// IEEE-754 64-bit floating-point.
    Double {
        name: String,
        #[serde(default)]
        required: bool,
    },
    /// Boolean field.
    Bool {
        name: String,
        #[serde(default)]
        required: bool,
    },
    /// Event-time timestamp with timezone.
    Datetime {
        name: String,
        #[serde(default)]
        required: bool,
    },
    /// Opaque binary payload.
    Bytes {
        name: String,
        #[serde(default)]
        required: bool,
    },
    /// JSON stored as a string for Arrow/Iceberg compatibility.
    Json {
        name: String,
        #[serde(default)]
        required: bool,
    },
}

/// Behavior for attributes that are not declared in `field_mappings`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum MappingMode {
    /// Preserve unmapped attributes in the residual `attributes` column.
    #[default]
    Dynamic,
    /// Drop unmapped attributes silently.
    Lenient,
    /// Reject events that carry unmapped attributes.
    Strict,
}

/// Retention configuration for one index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RetentionPolicy {
    /// Retention horizon in seconds.
    pub period_secs: u64,
    /// Optional evaluator schedule string.
    pub schedule: Option<String>,
}

/// Document mapping for one index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DocMapping {
    /// Handling mode for unmapped attributes.
    #[serde(default)]
    pub mode: MappingMode,
    /// Declared typed fields in stable order.
    #[schema(value_type = Vec<FieldMappingSchema>)]
    pub field_mappings: Vec<FieldMapping>,
    /// Required event-time field.
    pub timestamp_field: String,
    /// Fields treated as low-cardinality tags for pruning.
    #[serde(default)]
    pub tag_fields: Vec<String>,
    /// Fields searched by default for bare text queries.
    #[serde(default)]
    pub default_search_fields: Vec<String>,
}

/// Complete index configuration for one logical index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct IndexConfig {
    /// Human-facing index identifier.
    pub index_id: String,
    /// Declared document mapping.
    pub doc_mapping: DocMapping,
    /// Optional retention policy.
    pub retention: Option<RetentionPolicy>,
    /// Whether the ingest drain builds the raw-text search indexes (inverted
    /// index, trigram + row-group token blooms, Puffin sidecar) inline with
    /// every flush, or defers them to compaction (files gain indexes when they
    /// consolidate at L1). Inline makes the freshest data fully searchable at
    /// ~30 % append-time cost (measured); deferring buys drain throughput and
    /// is the right default for firehose streams — the query funnel tolerates
    /// mixed indexed/unindexed files by falling back to a scan. `None` inherits
    /// the deployment default (`SIGLAKE_INDEX_AT_FLUSH`, inline unless `0`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_at_flush: Option<bool>,
}

/// Validation failures for [`IndexConfig`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum IndexConfigError {
    #[error("invalid index_id `{0}`: must match [a-z0-9][a-z0-9_-]{{0,127}}")]
    InvalidIndexId(String),
    #[error("reserved index_id `{0}`: names starting with `_` are system-reserved")]
    ReservedIndexId(String),
    #[error("reserved index_id `{0}`: collides with a WAL layout directory name")]
    ReservedWalLayoutDir(String),
    #[error("invalid field name `{0}`: must match [a-zA-Z_][a-zA-Z0-9_]*")]
    InvalidFieldName(String),
    #[error("duplicate field name `{0}`")]
    DuplicateFieldName(String),
    #[error("reserved field name `{0}`: this column is appended automatically")]
    ReservedFieldName(String),
    #[error("too many field mappings: {0} > {MAX_FIELD_MAPPINGS}")]
    TooManyFieldMappings(usize),
    #[error("timestamp_field `{0}` does not name a declared field")]
    UnknownTimestampField(String),
    #[error("timestamp_field `{0}` must be a datetime field")]
    TimestampFieldMustBeDatetime(String),
    #[error("timestamp_field `{0}` must be required")]
    TimestampFieldMustBeRequired(String),
    #[error("tag field `{0}` does not name a declared field")]
    UnknownTagField(String),
    #[error("default search field `{0}` does not name a declared field")]
    UnknownDefaultSearchField(String),
    #[error("default search field `{0}` must be a text field")]
    DefaultSearchFieldMustBeText(String),
    #[error(
        "unknown tokenizer `{tokenizer}` for field `{field}`: allowed values are {}",
        ALLOWED_TEXT_TOKENIZERS.join(", ")
    )]
    UnknownTokenizer { field: String, tokenizer: String },
    #[error("retention period_secs must be greater than zero")]
    InvalidRetentionPeriod,
}

/// Result type for index-config validation.
pub type Result<T> = std::result::Result<T, IndexConfigError>;

impl IndexConfig {
    /// Validate that this config is internally consistent and compile-safe.
    pub fn validate(&self) -> Result<()> {
        validate_index_id(&self.index_id)?;

        let field_mappings = &self.doc_mapping.field_mappings;
        if field_mappings.len() > MAX_FIELD_MAPPINGS {
            return Err(IndexConfigError::TooManyFieldMappings(field_mappings.len()));
        }

        let mut declared = HashMap::with_capacity(field_mappings.len());
        let mut names = HashSet::with_capacity(field_mappings.len());
        for field in field_mappings {
            validate_field_name(&field.name)?;
            if field.name == RESIDUAL_ATTRIBUTES_FIELD {
                return Err(IndexConfigError::ReservedFieldName(field.name.clone()));
            }
            if !names.insert(field.name.as_str()) {
                return Err(IndexConfigError::DuplicateFieldName(field.name.clone()));
            }
            if let FieldType::Text {
                tokenizer: Some(tokenizer),
            } = &field.field_type
            {
                if !ALLOWED_TEXT_TOKENIZERS.contains(&tokenizer.as_str()) {
                    return Err(IndexConfigError::UnknownTokenizer {
                        field: field.name.clone(),
                        tokenizer: tokenizer.clone(),
                    });
                }
            }
            declared.insert(field.name.as_str(), field);
        }

        let timestamp = declared
            .get(self.doc_mapping.timestamp_field.as_str())
            .ok_or_else(|| {
                IndexConfigError::UnknownTimestampField(self.doc_mapping.timestamp_field.clone())
            })?;
        if !matches!(timestamp.field_type, FieldType::Datetime) {
            return Err(IndexConfigError::TimestampFieldMustBeDatetime(
                self.doc_mapping.timestamp_field.clone(),
            ));
        }
        if !timestamp.required {
            return Err(IndexConfigError::TimestampFieldMustBeRequired(
                self.doc_mapping.timestamp_field.clone(),
            ));
        }

        for tag_field in &self.doc_mapping.tag_fields {
            if !declared.contains_key(tag_field.as_str()) {
                return Err(IndexConfigError::UnknownTagField(tag_field.clone()));
            }
        }

        for field_name in &self.doc_mapping.default_search_fields {
            let field = declared
                .get(field_name.as_str())
                .ok_or_else(|| IndexConfigError::UnknownDefaultSearchField(field_name.clone()))?;
            if !matches!(field.field_type, FieldType::Text { .. }) {
                return Err(IndexConfigError::DefaultSearchFieldMustBeText(
                    field_name.clone(),
                ));
            }
        }

        if self
            .retention
            .as_ref()
            .is_some_and(|retention| retention.period_secs == 0)
        {
            return Err(IndexConfigError::InvalidRetentionPeriod);
        }

        Ok(())
    }

    /// Compile this mapping into the Arrow schema stored in Iceberg.
    pub fn to_arrow_schema(&self) -> SchemaRef {
        let mut fields = Vec::with_capacity(self.doc_mapping.field_mappings.len() + 1);
        for (index, field) in self.doc_mapping.field_mappings.iter().enumerate() {
            fields.push(with_field_id(
                Field::new(
                    &field.name,
                    field.field_type.arrow_data_type(),
                    !field.required,
                ),
                (index + 1) as i32,
            ));
        }
        fields.push(with_field_id(
            Field::new(RESIDUAL_ATTRIBUTES_FIELD, DataType::Utf8, true),
            (self.doc_mapping.field_mappings.len() + 1) as i32,
        ));
        // A probe build declares one extra column wherever it declares a table
        // schema, not only for `events`: the write path builds ONE batch and
        // refuses to write it to a table that lacks a column the rows populate,
        // so an index table without the probe column would make every managed
        // index and every delete task unwritable under that build. See
        // `ROLLBACK_PROBE_COLUMN`.
        #[cfg(feature = "experimental-schema-rollback-probe")]
        fields.push(with_field_id(
            Field::new(crate::ROLLBACK_PROBE_COLUMN, DataType::Int64, true),
            (self.doc_mapping.field_mappings.len() + 2) as i32,
        ));
        Arc::new(Schema::new(fields))
    }

    /// Generated mapping for the legacy fixed `events` table.
    pub fn builtin_events() -> Self {
        Self {
            index_id: "events".into(),
            doc_mapping: DocMapping {
                mode: MappingMode::Dynamic,
                field_mappings: vec![
                    FieldMapping {
                        name: "timestamp".into(),
                        field_type: FieldType::Datetime,
                        required: true,
                    },
                    FieldMapping {
                        name: "host".into(),
                        field_type: FieldType::Text {
                            tokenizer: Some("raw".into()),
                        },
                        required: true,
                    },
                    FieldMapping {
                        name: "source".into(),
                        field_type: FieldType::Text {
                            tokenizer: Some("raw".into()),
                        },
                        required: true,
                    },
                    FieldMapping {
                        name: "sourcetype".into(),
                        field_type: FieldType::Text {
                            tokenizer: Some("raw".into()),
                        },
                        required: true,
                    },
                    FieldMapping {
                        name: "index".into(),
                        field_type: FieldType::Text {
                            tokenizer: Some("raw".into()),
                        },
                        required: true,
                    },
                    FieldMapping {
                        name: "raw".into(),
                        field_type: FieldType::Text {
                            tokenizer: Some("default".into()),
                        },
                        required: true,
                    },
                    // Exact unix nanoseconds; `timestamp` is only microsecond
                    // precise. Declared after `raw` so the residual
                    // `attributes` column keeps its trailing position and the
                    // core columns keep the field ids the readers expect.
                    FieldMapping {
                        name: crate::TIMESTAMP_NS_COLUMN.into(),
                        field_type: FieldType::Long,
                        required: true,
                    },
                ],
                timestamp_field: "timestamp".into(),
                tag_fields: vec![
                    "host".into(),
                    "source".into(),
                    "sourcetype".into(),
                    "index".into(),
                ],
                default_search_fields: vec!["raw".into()],
            },
            retention: None,
            index_at_flush: None,
        }
    }
}

impl FieldType {
    fn arrow_data_type(&self) -> DataType {
        match self {
            FieldType::Text { .. } | FieldType::Json => DataType::Utf8,
            FieldType::Long => DataType::Int64,
            FieldType::Double => DataType::Float64,
            FieldType::Bool => DataType::Boolean,
            FieldType::Datetime => crate::timestamp_data_type(),
            FieldType::Bytes => DataType::Binary,
        }
    }
}

pub fn validate_index_id(index_id: &str) -> Result<()> {
    if index_id.starts_with('_') {
        return Err(IndexConfigError::ReservedIndexId(index_id.to_string()));
    }
    let mut chars = index_id.chars();
    let Some(first) = chars.next() else {
        return Err(IndexConfigError::InvalidIndexId(index_id.to_string()));
    };
    if index_id.len() > 128 || !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(IndexConfigError::InvalidIndexId(index_id.to_string()));
    }
    if chars.any(|c| !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '_' && c != '-') {
        return Err(IndexConfigError::InvalidIndexId(index_id.to_string()));
    }
    if RESERVED_WAL_LAYOUT_DIRS.contains(&index_id) {
        return Err(IndexConfigError::ReservedWalLayoutDir(index_id.to_string()));
    }
    Ok(())
}

fn validate_field_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(IndexConfigError::InvalidFieldName(name.to_string()));
    };
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(IndexConfigError::InvalidFieldName(name.to_string()));
    }
    if chars.any(|c| !c.is_ascii_alphanumeric() && c != '_') {
        return Err(IndexConfigError::InvalidFieldName(name.to_string()));
    }
    Ok(())
}

fn reject_tokenizer<E>(tokenizer: &Option<Option<String>>) -> std::result::Result<(), E>
where
    E: de::Error,
{
    if tokenizer.is_some() {
        return Err(de::Error::unknown_field(
            "tokenizer",
            &["name", "type", "required"],
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use serde_json::json;

    use crate::{events_schema, with_field_id};

    fn field(name: &str, field_type: FieldType, required: bool) -> FieldMapping {
        FieldMapping {
            name: name.into(),
            field_type,
            required,
        }
    }

    fn valid_config() -> IndexConfig {
        IndexConfig {
            index_id: "logs".into(),
            doc_mapping: DocMapping {
                mode: MappingMode::Dynamic,
                field_mappings: vec![
                    field("timestamp", FieldType::Datetime, true),
                    field(
                        "message",
                        FieldType::Text {
                            tokenizer: Some("default".into()),
                        },
                        false,
                    ),
                    field("service", FieldType::Text { tokenizer: None }, false),
                ],
                timestamp_field: "timestamp".into(),
                tag_fields: vec!["service".into()],
                default_search_fields: vec!["message".into()],
            },
            retention: None,
            index_at_flush: None,
        }
    }

    #[test]
    fn builtin_events_schema_matches_legacy_schema_exactly() {
        assert_eq!(
            IndexConfig::builtin_events().to_arrow_schema(),
            events_schema()
        );
    }

    #[test]
    fn validate_rejects_bad_index_ids() {
        let mut uppercase = valid_config();
        uppercase.index_id = "Bad".into();
        assert_eq!(
            uppercase.validate(),
            Err(IndexConfigError::InvalidIndexId("Bad".into()))
        );

        let mut reserved = valid_config();
        reserved.index_id = "_system".into();
        assert_eq!(
            reserved.validate(),
            Err(IndexConfigError::ReservedIndexId("_system".into()))
        );

        let mut too_long = valid_config();
        too_long.index_id = "a".repeat(129);
        assert_eq!(
            too_long.validate(),
            Err(IndexConfigError::InvalidIndexId("a".repeat(129)))
        );

        for reserved in ["active", "sealed", "processing", "committed", "consumers"] {
            let mut config = valid_config();
            config.index_id = reserved.to_string();
            assert_eq!(
                config.validate(),
                Err(IndexConfigError::ReservedWalLayoutDir(reserved.to_string()))
            );
        }
    }

    #[test]
    fn validate_rejects_duplicate_field_names() {
        let mut config = valid_config();
        config
            .doc_mapping
            .field_mappings
            .push(field("service", FieldType::Long, false));
        assert_eq!(
            config.validate(),
            Err(IndexConfigError::DuplicateFieldName("service".into()))
        );
    }

    #[test]
    fn validate_rejects_reserved_attributes_field_name() {
        let mut config = valid_config();
        config.doc_mapping.field_mappings[1].name = "attributes".into();
        assert_eq!(
            config.validate(),
            Err(IndexConfigError::ReservedFieldName("attributes".into()))
        );
    }

    #[test]
    fn validate_rejects_missing_timestamp_field() {
        let mut config = valid_config();
        config.doc_mapping.timestamp_field = "ts".into();
        assert_eq!(
            config.validate(),
            Err(IndexConfigError::UnknownTimestampField("ts".into()))
        );
    }

    #[test]
    fn validate_rejects_non_datetime_timestamp_field() {
        let mut config = valid_config();
        config.doc_mapping.field_mappings[0].field_type = FieldType::Long;
        assert_eq!(
            config.validate(),
            Err(IndexConfigError::TimestampFieldMustBeDatetime(
                "timestamp".into()
            ))
        );
    }

    #[test]
    fn validate_rejects_non_required_timestamp_field() {
        let mut config = valid_config();
        config.doc_mapping.field_mappings[0].required = false;
        assert_eq!(
            config.validate(),
            Err(IndexConfigError::TimestampFieldMustBeRequired(
                "timestamp".into()
            ))
        );
    }

    #[test]
    fn validate_rejects_unknown_tag_field() {
        let mut config = valid_config();
        config.doc_mapping.tag_fields.push("missing".into());
        assert_eq!(
            config.validate(),
            Err(IndexConfigError::UnknownTagField("missing".into()))
        );
    }

    #[test]
    fn validate_rejects_non_text_default_search_field() {
        let mut config = valid_config();
        config.doc_mapping.default_search_fields = vec!["timestamp".into()];
        assert_eq!(
            config.validate(),
            Err(IndexConfigError::DefaultSearchFieldMustBeText(
                "timestamp".into()
            ))
        );
    }

    #[test]
    fn validate_rejects_unknown_tokenizer() {
        let mut config = valid_config();
        config.doc_mapping.field_mappings[1].field_type = FieldType::Text {
            tokenizer: Some("ngram".into()),
        };
        assert_eq!(
            config.validate(),
            Err(IndexConfigError::UnknownTokenizer {
                field: "message".into(),
                tokenizer: "ngram".into()
            })
        );
    }

    #[test]
    fn validate_rejects_too_many_fields() {
        let field_mappings = (0..257)
            .map(|i| {
                field(
                    &format!("f{i}"),
                    if i == 0 {
                        FieldType::Datetime
                    } else {
                        FieldType::Long
                    },
                    i == 0,
                )
            })
            .collect();
        let config = IndexConfig {
            index_id: "logs".into(),
            doc_mapping: DocMapping {
                mode: MappingMode::Dynamic,
                field_mappings,
                timestamp_field: "f0".into(),
                tag_fields: Vec::new(),
                default_search_fields: Vec::new(),
            },
            retention: None,
            index_at_flush: None,
        };
        assert_eq!(
            config.validate(),
            Err(IndexConfigError::TooManyFieldMappings(257))
        );
    }

    #[test]
    fn validate_rejects_zero_retention_period() {
        let mut config = valid_config();
        config.retention = Some(RetentionPolicy {
            period_secs: 0,
            schedule: Some("0 0 * * *".into()),
        });
        assert_eq!(
            config.validate(),
            Err(IndexConfigError::InvalidRetentionPeriod)
        );
    }

    #[test]
    fn serde_round_trip_exercises_every_field_type() {
        let config = IndexConfig {
            index_id: "typed-logs".into(),
            doc_mapping: DocMapping {
                mode: MappingMode::Strict,
                field_mappings: vec![
                    field("timestamp", FieldType::Datetime, true),
                    field(
                        "message",
                        FieldType::Text {
                            tokenizer: Some("default".into()),
                        },
                        false,
                    ),
                    field("count", FieldType::Long, false),
                    field("ratio", FieldType::Double, false),
                    field("ok", FieldType::Bool, false),
                    field("blob", FieldType::Bytes, false),
                    field("payload", FieldType::Json, false),
                ],
                timestamp_field: "timestamp".into(),
                tag_fields: vec!["ok".into()],
                default_search_fields: vec!["message".into()],
            },
            retention: Some(RetentionPolicy {
                period_secs: 3600,
                schedule: Some("0 * * * *".into()),
            }),
            index_at_flush: None,
        };

        let encoded = serde_json::to_string(&config).unwrap();
        let decoded: IndexConfig = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, config);
    }

    /// [`FieldMappingSchema`] exists only to generate a correct OpenAPI schema
    /// for [`FieldMapping`] (see its doc comment). It is therefore only useful
    /// if the two describe the identical wire format — this pins that in three
    /// directions for every field type, so a change to either one fails here
    /// rather than silently shipping a spec that misdescribes the API.
    #[test]
    fn field_mapping_schema_matches_wire_format() {
        let types = [
            FieldType::Text {
                tokenizer: Some("default".into()),
            },
            FieldType::Text { tokenizer: None },
            FieldType::Long,
            FieldType::Double,
            FieldType::Bool,
            FieldType::Datetime,
            FieldType::Bytes,
            FieldType::Json,
        ];

        for field_type in types {
            for required in [true, false] {
                let real = field("f", field_type.clone(), required);
                let wire = serde_json::to_value(&real).unwrap();

                // 1. The mirror accepts exactly what the real type emits.
                let mirror: FieldMappingSchema = serde_json::from_value(wire.clone())
                    .unwrap_or_else(|e| panic!("mirror rejected {wire}: {e}"));

                // 2. ... and re-emits it byte-identically.
                assert_eq!(
                    wire,
                    serde_json::to_value(&mirror).unwrap(),
                    "mirror round-trip changed the wire form of {wire}"
                );

                // 3. ... and the real hand-written `Deserialize` accepts the
                //    mirror's output, so a client coded against the spec
                //    produces bodies this server parses.
                let back: FieldMapping = serde_json::from_value(wire).unwrap();
                assert_eq!(back, real);
            }
        }
    }

    #[test]
    fn index_at_flush_parses_and_defaults_to_none() {
        // Absent → None (inherit the deployment default).
        let cfg: IndexConfig = serde_json::from_value(json!({
            "index_id": "logs",
            "doc_mapping": {
                "mode": "dynamic",
                "field_mappings": [
                    {"name": "timestamp", "type": "datetime", "required": true}
                ],
                "timestamp_field": "timestamp"
            },
            "retention": null
        }))
        .unwrap();
        assert_eq!(cfg.index_at_flush, None);
        // Explicit false → defer the raw-index build to compaction.
        let cfg: IndexConfig = serde_json::from_value(json!({
            "index_id": "logs",
            "doc_mapping": {
                "mode": "dynamic",
                "field_mappings": [
                    {"name": "timestamp", "type": "datetime", "required": true}
                ],
                "timestamp_field": "timestamp"
            },
            "retention": null,
            "index_at_flush": false
        }))
        .unwrap();
        assert_eq!(cfg.index_at_flush, Some(false));
    }

    #[test]
    fn serde_rejects_unknown_keys() {
        let err = serde_json::from_value::<IndexConfig>(json!({
            "index_id": "logs",
            "doc_mapping": {
                "mode": "dynamic",
                "field_mappings": [
                    {
                        "name": "timestamp",
                        "type": "datetime",
                        "required": true
                    }
                ],
                "timestamp_field": "timestamp",
                "tag_fields": [],
                "default_search_fields": []
            },
            "retention": null,
            "extra": true
        }))
        .unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn custom_mapping_compiles_to_expected_schema() {
        let config = IndexConfig {
            index_id: "typed-logs".into(),
            doc_mapping: DocMapping {
                mode: MappingMode::Lenient,
                field_mappings: vec![
                    field("timestamp", FieldType::Datetime, true),
                    field(
                        "message",
                        FieldType::Text {
                            tokenizer: Some("default".into()),
                        },
                        false,
                    ),
                    field("size", FieldType::Long, true),
                    field("ratio", FieldType::Double, false),
                    field("enabled", FieldType::Bool, false),
                    field("blob", FieldType::Bytes, false),
                    field("payload", FieldType::Json, false),
                ],
                timestamp_field: "timestamp".into(),
                tag_fields: vec!["enabled".into()],
                default_search_fields: vec!["message".into()],
            },
            retention: None,
            index_at_flush: None,
        };

        #[allow(unused_mut)]
        let mut expected_fields = vec![
            with_field_id(
                Field::new("timestamp", crate::timestamp_data_type(), false),
                1,
            ),
            with_field_id(Field::new("message", DataType::Utf8, true), 2),
            with_field_id(Field::new("size", DataType::Int64, false), 3),
            with_field_id(Field::new("ratio", DataType::Float64, true), 4),
            with_field_id(Field::new("enabled", DataType::Boolean, true), 5),
            with_field_id(Field::new("blob", DataType::Binary, true), 6),
            with_field_id(Field::new("payload", DataType::Utf8, true), 7),
            with_field_id(Field::new("attributes", DataType::Utf8, true), 8),
        ];
        #[cfg(feature = "experimental-schema-rollback-probe")]
        expected_fields.push(with_field_id(
            Field::new(crate::ROLLBACK_PROBE_COLUMN, DataType::Int64, true),
            9,
        ));
        let expected = Schema::new(expected_fields);

        assert_eq!(*config.to_arrow_schema(), expected);
    }
}
