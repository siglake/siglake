//! WS-7 dense extraction: promote declared OTLP attributes from the residual
//! `attributes` JSON column into their own typed Parquet columns.
//!
//! Declared: an operator lists `(attr_key, column, type)` triples; the storage
//! write path calls [`promote_attributes`] to widen
//! each events batch with the typed columns, extracted from each row's
//! `attributes` JSON. Typed columns get Parquet statistics + (for strings)
//! blooms, so queries on them prune files/row-groups instead of scanning +
//! `attr_get`-ing the JSON. The long tail stays in `attributes`.
//!
//! Opt-in: an empty promotion list reproduces the fixed 7-column schema exactly.
//!
//! The list can also be chosen for the operator, by the compactor's
//! frequency-driven sampling pass — which widens the schema on its own and so
//! ships off. Its bounds, cost and evidence:
//! `docs/DESIGN_auto_promotion_qualification.md`.

use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use crate::{with_field_id, Result};

/// Table property recording the declared promotions as JSON
/// (`[{"attr_key","name","ty"}]`). Written by the storage layer when it
/// widens the schema, read by the query provider to map
/// `attr_get(attributes, key)` predicates onto promoted columns for
/// stats-based pruning — the provider only holds a `Table`, not CLI state.
pub const PROMOTED_PROPERTY_KEY: &str = "siglake.promoted.v1";

/// Promotion property carrying expression-aware sources. Version 1 remains a
/// direct-attribute-only compatibility view for older writers: they may safely
/// null-pad a composed column, but must never mistake its first key for the
/// whole expression and materialize the wrong value.
pub const PROMOTED_PROPERTY_V2_KEY: &str = "siglake.promoted.v2";

/// Arrow/Parquet type a promoted attribute lands in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PromotedType {
    Utf8,
    Int64,
    Float64,
    Boolean,
}

impl PromotedType {
    pub fn arrow(self) -> DataType {
        match self {
            PromotedType::Utf8 => DataType::Utf8,
            PromotedType::Int64 => DataType::Int64,
            PromotedType::Float64 => DataType::Float64,
            PromotedType::Boolean => DataType::Boolean,
        }
    }

    /// Parse the spec token used on the CLI (`string|int|float|bool`).
    pub fn parse(token: &str) -> Option<Self> {
        match token.to_ascii_lowercase().as_str() {
            "string" | "utf8" | "str" => Some(PromotedType::Utf8),
            "int" | "int64" | "long" | "integer" => Some(PromotedType::Int64),
            "float" | "float64" | "double" => Some(PromotedType::Float64),
            "bool" | "boolean" => Some(PromotedType::Boolean),
            _ => None,
        }
    }
}

/// One declared promotion: pull `attr_key` out of the `attributes` JSON into a
/// `name` column of `ty`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PromotedColumn {
    pub attr_key: String,
    /// Keys applied by outer `attr_get` calls after `attr_key`. Empty is the
    /// original `attr_get(attributes, attr_key)` source. For example,
    /// `attr_key = "aws.cloudwatch"`, `attr_get_path = ["log_stream"]`
    /// identifies `attr_get(attr_get(attributes, 'aws.cloudwatch'),
    /// 'log_stream')` without conflating it with the literal flat key
    /// `aws.cloudwatch.log_stream`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attr_get_path: Vec<String>,
    pub name: String,
    pub ty: PromotedType,
}

/// Serialize declared promotions for [`PROMOTED_PROPERTY_KEY`].
pub fn promoted_property_json(promoted: &[PromotedColumn]) -> Result<String> {
    serde_json::to_string(promoted)
        .map_err(|e| crate::CoreError::InvalidEvent(format!("serialize promoted columns: {e}")))
}

/// Parse [`PROMOTED_PROPERTY_KEY`] from table properties. Absent or
/// unparseable ⇒ empty (no pruning; conservative).
pub fn promoted_columns_from_property(json: Option<&str>) -> Vec<PromotedColumn> {
    json.and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default()
}

/// Read the expression-aware property when present, otherwise the legacy
/// direct-only property. An invalid v2 value is not allowed to fall back to v1:
/// doing so could silently discard a composed source and qualify the wrong map.
pub fn promoted_columns_from_properties(
    expression_aware: Option<&str>,
    legacy: Option<&str>,
) -> Vec<PromotedColumn> {
    match expression_aware {
        Some(json) => promoted_columns_from_property(Some(json)),
        None => promoted_columns_from_property(legacy),
    }
}

impl PromotedColumn {
    /// Parse a CLI spec `attr_key:type[:column]`, or an exact composed lookup
    /// as `["inner key","outer key",...]:type[:column]`. The column name
    /// defaults to all source keys with `.`/`-` replaced by `_`.
    pub fn parse_spec(spec: &str) -> Option<Self> {
        if spec.starts_with('[') {
            let mut values = serde_json::Deserializer::from_str(spec).into_iter::<Vec<String>>();
            let path = values.next()?.ok()?;
            let offset = values.byte_offset();
            let suffix = spec.get(offset..)?.strip_prefix(':')?;
            if path.is_empty() || path.iter().any(|key| key.is_empty()) {
                return None;
            }
            let mut parts = suffix.splitn(2, ':');
            let ty = PromotedType::parse(parts.next()?.trim())?;
            let name = match parts.next() {
                Some(n) if !n.trim().is_empty() => n.trim().to_string(),
                _ => path
                    .iter()
                    .map(|key| key.replace(['.', '-'], "_"))
                    .collect::<Vec<_>>()
                    .join("_"),
            };
            return Some(PromotedColumn {
                attr_key: path[0].clone(),
                attr_get_path: path[1..].to_vec(),
                name,
                ty,
            });
        }
        let mut parts = spec.splitn(3, ':');
        let attr_key = parts.next()?.trim();
        if attr_key.is_empty() {
            return None;
        }
        let ty = PromotedType::parse(parts.next()?.trim())?;
        let name = match parts.next() {
            Some(n) if !n.trim().is_empty() => n.trim().to_string(),
            _ => attr_key.replace(['.', '-'], "_"),
        };
        Some(PromotedColumn {
            attr_key: attr_key.to_string(),
            attr_get_path: Vec::new(),
            name,
            ty,
        })
    }

    pub fn is_composed(&self) -> bool {
        !self.attr_get_path.is_empty()
    }

    /// Exact sequence of literal keys in the represented `attr_get` tree.
    pub fn expression_keys(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.attr_key.as_str()).chain(self.attr_get_path.iter().map(String::as_str))
    }
}

/// File/table provenance version required by this promotion set. Direct
/// lookups retain v2. A composed lookup requires v3 so a concurrent or rolled
/// back v2 writer cannot qualify its null-padded or wrongly materialized file.
pub fn promotion_extractor_version(promoted: &[PromotedColumn]) -> u32 {
    if promoted.iter().any(PromotedColumn::is_composed) {
        COMPOSED_ATTR_EXTRACTOR_VERSION
    } else {
        ATTR_EXTRACTOR_VERSION
    }
}

/// Direct-only compatibility view stored under [`PROMOTED_PROPERTY_KEY`].
/// Older binaries cannot represent composed expression identity.
pub fn direct_promotions(promoted: &[PromotedColumn]) -> Vec<PromotedColumn> {
    promoted
        .iter()
        .filter(|column| !column.is_composed())
        .cloned()
        .collect()
}

/// Version of the dotted-key extraction rule shared by [`attr_lookup`] here and
/// `find_value_path` in the query server's `attr_get` UDF. The two must agree
/// on every input: the query layer substitutes a promoted column for
/// `attr_get(attributes, key)` and prunes row groups from its statistics, and
/// both are wrong the moment the column holds a value the UDF would not have
/// produced.
///
/// Bump it when the rule changes. Values materialized by an older rule are not
/// interchangeable with the current `attr_get`, so the storage layer stamps
/// this version on the table (`siglake.promotion_extraction.v1`) and withholds
/// the rewrite and the prune hints from data written under an older one.
///
/// - v1 (≤ 0.2.1): literal key at the top level, then a strict segment walk.
///   A key `a.b.c` whose object under `a` holds a literal `"b.c"` key extracted
///   to NULL while `attr_get` returned the value.
/// - v2: literal key retried at EVERY level, matching the UDF (#6769).
pub const ATTR_EXTRACTOR_VERSION: u32 = 2;

/// Provenance version for promotion sets containing a composed `attr_get`.
/// Direct-only writers stamp v2; expression-aware writers stamp v3.
pub const COMPOSED_ATTR_EXTRACTOR_VERSION: u32 = 3;

/// Look `key` up in a parsed attributes map: a literal (possibly dotted) key
/// wins at every level; otherwise a dotted key walks into nested objects one
/// segment at a time (OTLP residuals hold nested objects).
///
/// Byte-for-byte the rule of the query server's `find_value_path`, including
/// its absence of backtracking: `a.b.c` against `{"a":{"x":1},"a.b":{"c":9}}`
/// resolves `a`, fails inside it, and does NOT retry `a.b` at the top level.
/// Both extractors must answer identically for every input — see
/// [`ATTR_EXTRACTOR_VERSION`].
fn attr_lookup<'a>(
    map: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<&'a serde_json::Value> {
    if let Some(v) = map.get(key) {
        return Some(v);
    }
    let (head, tail) = key.split_once('.')?;
    attr_lookup(map.get(head)?.as_object()?, tail)
}

/// Evaluate the exact nested `attr_get` source represented by `column`.
/// Every outer step consumes the previous call's STRING result and reparses
/// it, including the case where the inner value was itself a JSON string.
fn promoted_value(
    map: &serde_json::Map<String, serde_json::Value>,
    column: &PromotedColumn,
) -> Option<serde_json::Value> {
    let mut value = attr_lookup(map, &column.attr_key)?.clone();
    for key in &column.attr_get_path {
        let text = match value {
            serde_json::Value::Null => return None,
            serde_json::Value::String(text) => text,
            other => other.to_string(),
        };
        let inner: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&text).ok()?;
        value = attr_lookup(&inner, key)?.clone();
    }
    Some(value)
}

/// Field id of the `index`-th promoted column. The 8 core columns own ids 1..=8.
///
/// A `experimental-schema-rollback-probe` build declares one more column ahead
/// of the promoted ones and shifts these by one, because the catalog assigns
/// ids in FIELD ORDER when it creates the table and the Iceberg writer then
/// matches batch columns to table fields by id: a declared id that disagrees
/// with the assigned one fails the write with "Field id N not found in struct
/// array" rather than anything about schemas.
pub(crate) fn promoted_field_id(index: usize) -> i32 {
    #[cfg(feature = "experimental-schema-rollback-probe")]
    let first = crate::ROLLBACK_PROBE_FIELD_ID + 1;
    #[cfg(not(feature = "experimental-schema-rollback-probe"))]
    let first = 9;
    first + index as i32
}

/// Build the nullable Arrow [`Field`] for a promoted column (with its field id).
pub(crate) fn promoted_field(col: &PromotedColumn, index: usize) -> Field {
    with_field_id(
        Field::new(&col.name, col.ty.arrow(), true),
        promoted_field_id(index),
    )
}

/// Widen an events `RecordBatch` with the declared promoted columns, extracted
/// from its `attributes` JSON column. The output schema is
/// [`events_schema_with`]`(promoted)`. Empty `promoted` ⇒ the batch unchanged.
///
/// A promoted cell is null when the source row's `attributes` is null, the key
/// is absent, the JSON value is null, or it can't be coerced to the column type
/// (e.g. a non-numeric string into an `Int64`) — lossless: the original value is
/// still in `attributes`.
///
/// A `Utf8` promotion of an object- or array-valued key stores that value's
/// compact JSON — the same text `attr_get(attributes, key)` returns, because
/// both parse the value and serialize it again rather than copying the stored
/// slice. That parity is what lets the query layer read an `attr_get` off the
/// promoted column; it is pinned in
/// `siglake-query-server/tests/promoted_object_parity.rs`. The declared-mapping
/// `Text` builder deliberately differs: see [`json_scalar_as_string`].
pub fn promote_attributes(batch: &RecordBatch, promoted: &[PromotedColumn]) -> Result<RecordBatch> {
    if promoted.is_empty() {
        return Ok(batch.clone());
    }
    let n = batch.num_rows();
    let attrs: Option<&StringArray> = batch
        .column_by_name("attributes")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());

    // One typed builder per promoted column.
    enum B {
        Utf8(StringBuilder),
        Int64(Int64Builder),
        Float64(Float64Builder),
        Boolean(BooleanBuilder),
    }
    let mut builders: Vec<B> = promoted
        .iter()
        .map(|c| match c.ty {
            PromotedType::Utf8 => B::Utf8(StringBuilder::with_capacity(n, n * 16)),
            PromotedType::Int64 => B::Int64(Int64Builder::with_capacity(n)),
            PromotedType::Float64 => B::Float64(Float64Builder::with_capacity(n)),
            PromotedType::Boolean => B::Boolean(BooleanBuilder::with_capacity(n)),
        })
        .collect();

    for row in 0..n {
        // Parse this row's attributes JSON once; extract every promoted key from it.
        let parsed: Option<serde_json::Map<String, serde_json::Value>> = attrs
            .filter(|a| !a.is_null(row))
            .and_then(|a| serde_json::from_str(a.value(row)).ok());
        for (col, b) in promoted.iter().zip(builders.iter_mut()) {
            let v = parsed.as_ref().and_then(|m| promoted_value(m, col));
            match b {
                B::Utf8(bld) => match v.as_ref() {
                    Some(serde_json::Value::String(s)) => bld.append_value(s),
                    Some(serde_json::Value::Null) | None => bld.append_null(),
                    Some(other) => bld.append_value(other.to_string()),
                },
                B::Int64(bld) => bld.append_option(v.as_ref().and_then(json_as_i64)),
                B::Float64(bld) => bld.append_option(v.as_ref().and_then(json_as_f64)),
                B::Boolean(bld) => bld.append_option(v.as_ref().and_then(json_as_bool)),
            }
        }
    }

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    for (index, (col, b)) in promoted.iter().zip(builders).enumerate() {
        fields.push(promoted_field(col, index));
        columns.push(match b {
            B::Utf8(mut bld) => Arc::new(bld.finish()),
            B::Int64(mut bld) => Arc::new(bld.finish()),
            B::Float64(mut bld) => Arc::new(bld.finish()),
            B::Boolean(mut bld) => Arc::new(bld.finish()),
        });
    }
    // Output = input schema + the promoted fields. When the input is the
    // canonical 8-column events batch (the compactor's case) this is exactly
    // events_schema_with(promoted).
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

pub(crate) fn json_as_i64(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

pub(crate) fn json_as_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

pub(crate) fn json_as_bool(v: &serde_json::Value) -> Option<bool> {
    match v {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Scalar-only text extraction for a declared `Text` field: a string, number
/// or bool becomes its literal, an object or array yields `None` so the field
/// stays null and its key stays in the residual `attributes`. Unlike a `Utf8`
/// promotion, which stringifies the whole value, this never puts JSON into a
/// field an index mapping declared as text — and the same `None` is what
/// required-field validation sees. A declared `Json` field is the mapping-side
/// way to keep the structure (`mapping.rs`'s `Builder::Json`).
pub(crate) fn json_scalar_as_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => Some(v.to_string()),
        _ => None,
    }
}

/// Re-extract every promoted column from the residual `attributes` JSON,
/// REPLACING any existing promoted columns in `batch`. The rewrite/backfill
/// path needs this: a pre-promotion file reads with the promoted columns
/// all-NULL (schema alignment) while its JSON still carries the keys, and
/// extraction is deterministic, so post-promotion rows re-extract to the
/// identical values (idempotent). `promote_attributes` alone would append
/// duplicates when the columns are already present.
pub fn repromote_attributes(
    batch: &RecordBatch,
    promoted: &[PromotedColumn],
) -> Result<RecordBatch> {
    if promoted.is_empty() {
        return Ok(batch.clone());
    }
    let names: std::collections::HashSet<&str> = promoted.iter().map(|c| c.name.as_str()).collect();
    let keep: Vec<usize> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !names.contains(f.name().as_str()))
        .map(|(i, _)| i)
        .collect();
    let stripped = if keep.len() == batch.num_columns() {
        batch.clone()
    } else {
        batch.project(&keep)?
    };
    promote_attributes(&stripped, promoted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{events_schema, events_to_record_batch, Event};
    use arrow_array::{BooleanArray, Float64Array, Int64Array};

    fn ev(attributes: Option<&str>) -> Event {
        Event::now("raw").with_attributes(attributes.map(str::to_string))
    }

    fn cols() -> Vec<PromotedColumn> {
        vec![
            PromotedColumn {
                attr_key: "http.status_code".into(),
                attr_get_path: Vec::new(),
                name: "status".into(),
                ty: PromotedType::Int64,
            },
            PromotedColumn {
                attr_key: "k8s.namespace".into(),
                attr_get_path: Vec::new(),
                name: "ns".into(),
                ty: PromotedType::Utf8,
            },
            PromotedColumn {
                attr_key: "ratio".into(),
                attr_get_path: Vec::new(),
                name: "ratio".into(),
                ty: PromotedType::Float64,
            },
            PromotedColumn {
                attr_key: "sampled".into(),
                attr_get_path: Vec::new(),
                name: "sampled".into(),
                ty: PromotedType::Boolean,
            },
        ]
    }

    #[test]
    fn parse_spec_forms() {
        assert_eq!(
            PromotedColumn::parse_spec("http.status_code:int"),
            Some(PromotedColumn {
                attr_key: "http.status_code".into(),
                attr_get_path: Vec::new(),
                name: "http_status_code".into(),
                ty: PromotedType::Int64
            })
        );
        assert_eq!(
            PromotedColumn::parse_spec("k8s.namespace:string:ns"),
            Some(PromotedColumn {
                attr_key: "k8s.namespace".into(),
                attr_get_path: Vec::new(),
                name: "ns".into(),
                ty: PromotedType::Utf8
            })
        );
        assert_eq!(PromotedColumn::parse_spec("bad"), None);
        assert_eq!(PromotedColumn::parse_spec("k:nosuchtype"), None);
        assert_eq!(
            PromotedColumn::parse_spec(
                r#"["aws.cloudwatch","log_stream"]:string:cloudwatch_stream"#
            ),
            Some(PromotedColumn {
                attr_key: "aws.cloudwatch".into(),
                attr_get_path: vec!["log_stream".into()],
                name: "cloudwatch_stream".into(),
                ty: PromotedType::Utf8,
            })
        );
        assert_eq!(
            PromotedColumn::parse_spec(r#"["aws.cloudwatch","log-stream"]:string"#)
                .unwrap()
                .name,
            "aws_cloudwatch_log_stream"
        );
        assert!(PromotedColumn::parse_spec("[]:string").is_none());
        assert!(PromotedColumn::parse_spec(r#"["only"]:nosuchtype"#).is_none());
    }

    #[test]
    fn composed_lookup_materialization_preserves_expression_identity() {
        let cases = [
            (
                r#"{"aws.cloudwatch":{"log_stream":"prefix-child"},"aws.cloudwatch.log_stream":"full-literal"}"#,
                Some("prefix-child"),
            ),
            (
                r#"{"aws.cloudwatch":{"log_stream":"prefix-child"}}"#,
                Some("prefix-child"),
            ),
            (
                r#"{"aws":{"cloudwatch":{"log_stream":"nested"}}}"#,
                Some("nested"),
            ),
            (
                r#"{"aws.cloudwatch":{"log_stream":"prefix-child"},"aws":{"cloudwatch":{"log_stream":"nested"}}}"#,
                Some("prefix-child"),
            ),
            (
                r#"{"aws.cloudwatch":null,"aws":{"cloudwatch":{"log_stream":"nested"}}}"#,
                None,
            ),
            (
                r#"{"aws.cloudwatch":{"log_stream":"prefix-child"},"aws.cloudwatch.log_stream":null}"#,
                Some("prefix-child"),
            ),
            (
                r#"{"aws.cloudwatch":"{\"log_stream\":\"json-string-child\"}"}"#,
                Some("json-string-child"),
            ),
            (r#"{"aws.cloudwatch":7}"#, None),
            (r#"{}"#, None),
            (
                r#"{"aws":{"cloudwatch":{"log_stream":9007199254740993}}}"#,
                Some("9007199254740993"),
            ),
            (
                r#"{"aws":{"cloudwatch":{"log_stream":false}}}"#,
                Some("false"),
            ),
        ];
        let promoted = [PromotedColumn {
            attr_key: "aws.cloudwatch".into(),
            attr_get_path: vec!["log_stream".into()],
            name: "cloudwatch_stream".into(),
            ty: PromotedType::Utf8,
        }];
        let events = cases
            .iter()
            .map(|(attributes, _)| ev(Some(attributes)))
            .collect::<Vec<_>>();
        let batch = events_to_record_batch(&events).unwrap();
        let out = promote_attributes(&batch, &promoted).unwrap();
        let values = out
            .column_by_name("cloudwatch_stream")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (row, (_, expected)) in cases.iter().enumerate() {
            let actual = (!values.is_null(row)).then(|| values.value(row));
            assert_eq!(actual, *expected, "row {row}");
        }
        assert_eq!(promotion_extractor_version(&promoted), 3);
        assert!(direct_promotions(&promoted).is_empty());
    }

    #[test]
    fn empty_promotion_is_identity() {
        let batch = events_to_record_batch(&[ev(Some(r#"{"a":1}"#))]).unwrap();
        let out = promote_attributes(&batch, &[]).unwrap();
        assert_eq!(out.schema(), events_schema());
        assert_eq!(out.num_columns(), events_schema().fields().len());
    }

    #[test]
    fn promotes_typed_columns_from_attributes() {
        let events = vec![
            ev(Some(
                r#"{"http.status_code":500,"k8s.namespace":"prod","ratio":0.25,"sampled":true}"#,
            )),
            ev(Some(r#"{"http.status_code":"200","k8s.namespace":"dev"}"#)), // int as string; others absent
            ev(None),                                                        // null attributes
            ev(Some(r#"{"http.status_code":"oops"}"#)),                      // uncoercible int
        ];
        let batch = events_to_record_batch(&events).unwrap();
        let out = promote_attributes(&batch, &cols()).unwrap();

        // schema = 8 core + 4 promoted, matching events_schema_with.
        assert_eq!(out.schema(), crate::events_schema_with(&cols()));
        assert_eq!(
            out.num_columns(),
            crate::events_schema_with(&cols()).fields().len()
        );

        let status = out
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(status.value(0), 500);
        assert_eq!(status.value(1), 200); // "200" string → 200
        assert!(status.is_null(2)); // null attributes
        assert!(status.is_null(3)); // "oops" not an int → null (still in attributes)

        let ns = out
            .column_by_name("ns")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ns.value(0), "prod");
        assert_eq!(ns.value(1), "dev");
        assert!(ns.is_null(2));

        let ratio = out
            .column_by_name("ratio")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((ratio.value(0) - 0.25).abs() < 1e-9);
        assert!(ratio.is_null(1));

        let sampled = out
            .column_by_name("sampled")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(sampled.value(0));
        assert!(sampled.is_null(1));
    }

    /// #6769: the inner literal dotted key. `attr_get`'s walk retries the
    /// literal key at every level, so `a.b.c` resolves against
    /// `{"a":{"b.c":1}}`; promotion's strict segment walk used to miss it and
    /// write NULL, and the rewrite then substituted that NULL for the UDF.
    #[test]
    fn inner_literal_dotted_key_matches_attr_get() {
        let json = serde_json::from_str::<serde_json::Map<_, _>>(r#"{"a":{"b.c":1}}"#).unwrap();
        assert_eq!(attr_lookup(&json, "a.b.c"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn literal_key_wins_at_every_level() {
        // Top level: the literal dotted key beats the nested object (the Big5
        // `log.file.path` shape depends on this precedence).
        let top =
            serde_json::from_str::<serde_json::Map<_, _>>(r#"{"a.b":1,"a":{"b":2}}"#).unwrap();
        assert_eq!(attr_lookup(&top, "a.b"), Some(&serde_json::json!(1)));
        // Inner level: same precedence one step down.
        let inner = serde_json::from_str::<serde_json::Map<_, _>>(r#"{"a":{"b.c":1,"b":{"c":2}}}"#)
            .unwrap();
        assert_eq!(attr_lookup(&inner, "a.b.c"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn nested_walk_and_misses() {
        let m = |s: &str| serde_json::from_str::<serde_json::Map<_, _>>(s).unwrap();
        // Plain nested walk, unchanged.
        assert_eq!(
            attr_lookup(&m(r#"{"a":{"b":{"c":3}}}"#), "a.b.c"),
            Some(&serde_json::json!(3))
        );
        // A JSON null resolves — and promotes to a NULL cell, see below.
        assert_eq!(
            attr_lookup(&m(r#"{"a":{"b.c":null}}"#), "a.b.c"),
            Some(&serde_json::Value::Null)
        );
        // No backtracking: `a` resolves, the rest fails inside it, and the
        // top-level `a.b` is NOT retried. The UDF answers None here too.
        assert_eq!(
            attr_lookup(&m(r#"{"a":{"x":1},"a.b":{"c":9}}"#), "a.b.c"),
            None
        );
        // A scalar in the middle of a path is a miss, not a panic.
        assert_eq!(attr_lookup(&m(r#"{"a":"scalar"}"#), "a.b"), None);
        assert_eq!(attr_lookup(&m(r#"{"a":{"b":1}}"#), "a.b.c"), None);
    }

    #[test]
    fn promotes_inner_literal_dotted_key_and_keeps_null_precedence() {
        let cols = vec![
            PromotedColumn {
                attr_key: "a.b.c".into(),
                attr_get_path: Vec::new(),
                name: "abc".into(),
                ty: PromotedType::Int64,
            },
            PromotedColumn {
                attr_key: "log.file.path".into(),
                attr_get_path: Vec::new(),
                name: "log_file_path".into(),
                ty: PromotedType::Utf8,
            },
        ];
        let events = vec![
            ev(Some(r#"{"a":{"b.c":7},"log.file.path":"/var/log/a"}"#)),
            ev(Some(
                r#"{"a":{"b":{"c":8}},"log":{"file":{"path":"/var/log/b"}}}"#,
            )),
            // Literal dotted key wins over the nested object at both levels.
            ev(Some(
                r#"{"a":{"b.c":9,"b":{"c":1}},"log.file.path":"/lit","log":{"file":{"path":"/nested"}}}"#,
            )),
            // An explicit JSON null promotes to NULL, as an absent key does.
            ev(Some(r#"{"a":{"b.c":null},"log.file.path":null}"#)),
        ];
        let batch = events_to_record_batch(&events).unwrap();
        let out = promote_attributes(&batch, &cols).unwrap();
        let abc = out
            .column_by_name("abc")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(abc.value(0), 7, "inner literal dotted key");
        assert_eq!(abc.value(1), 8, "nested walk still resolves");
        assert_eq!(abc.value(2), 9, "literal beats nested at the inner level");
        assert!(abc.is_null(3), "JSON null promotes to NULL");

        let path = out
            .column_by_name("log_file_path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path.value(0), "/var/log/a");
        assert_eq!(path.value(1), "/var/log/b");
        assert_eq!(path.value(2), "/lit", "top-level literal precedence held");
        assert!(path.is_null(3));
    }

    #[test]
    fn no_attributes_column_yields_all_null_promoted() {
        // A batch without an `attributes` column (e.g. a pre-WS-7 read) still
        // widens, with all-null promoted columns — no panic.
        use arrow_array::builder::StringBuilder;
        let mut h = StringBuilder::new();
        h.append_value("host-1");
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "host",
                arrow_schema::DataType::Utf8,
                false,
            )]));
        let batch = RecordBatch::try_new(schema, vec![std::sync::Arc::new(h.finish())]).unwrap();
        let out = promote_attributes(&batch, &cols()).unwrap();
        assert_eq!(out.num_columns(), 5); // host + 4 promoted
        let status = out
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(status.is_null(0));
    }
}
