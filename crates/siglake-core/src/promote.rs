//! WS-7 dense extraction: promote declared OTLP attributes from the residual
//! `attributes` JSON column into their own typed Parquet columns.
//!
//! Declared (not frequency-driven yet): an operator lists `(attr_key, column,
//! type)` triples; the storage write path calls [`promote_attributes`] to widen
//! each events batch with the typed columns, extracted from each row's
//! `attributes` JSON. Typed columns get Parquet statistics + (for strings)
//! blooms, so queries on them prune files/row-groups instead of scanning +
//! `attr_get`-ing the JSON. The long tail stays in `attributes`.
//!
//! Opt-in: an empty promotion list reproduces the fixed 7-column schema exactly.

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

impl PromotedColumn {
    /// Parse a CLI spec `attr_key:type[:column]`. The column name defaults to
    /// the attr key with `.`/`-` replaced by `_` (a SQL-friendly identifier).
    pub fn parse_spec(spec: &str) -> Option<Self> {
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
            name,
            ty,
        })
    }
}

/// Look `key` up in a parsed attributes map: a literal (possibly dotted)
/// top-level key wins; otherwise a dotted key walks into nested objects one
/// segment at a time (OTLP residuals hold nested objects).
fn attr_lookup<'a>(
    map: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<&'a serde_json::Value> {
    if let Some(v) = map.get(key) {
        return Some(v);
    }
    let (head, tail) = key.split_once('.')?;
    let mut cur = map.get(head)?;
    for seg in tail.split('.') {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
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
            let v = parsed.as_ref().and_then(|m| attr_lookup(m, &col.attr_key));
            match b {
                B::Utf8(bld) => match v {
                    Some(serde_json::Value::String(s)) => bld.append_value(s),
                    Some(serde_json::Value::Null) | None => bld.append_null(),
                    Some(other) => bld.append_value(other.to_string()),
                },
                B::Int64(bld) => bld.append_option(v.and_then(json_as_i64)),
                B::Float64(bld) => bld.append_option(v.and_then(json_as_f64)),
                B::Boolean(bld) => bld.append_option(v.and_then(json_as_bool)),
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
                name: "status".into(),
                ty: PromotedType::Int64,
            },
            PromotedColumn {
                attr_key: "k8s.namespace".into(),
                name: "ns".into(),
                ty: PromotedType::Utf8,
            },
            PromotedColumn {
                attr_key: "ratio".into(),
                name: "ratio".into(),
                ty: PromotedType::Float64,
            },
            PromotedColumn {
                attr_key: "sampled".into(),
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
                name: "http_status_code".into(),
                ty: PromotedType::Int64
            })
        );
        assert_eq!(
            PromotedColumn::parse_spec("k8s.namespace:string:ns"),
            Some(PromotedColumn {
                attr_key: "k8s.namespace".into(),
                name: "ns".into(),
                ty: PromotedType::Utf8
            })
        );
        assert_eq!(PromotedColumn::parse_spec("bad"), None);
        assert_eq!(PromotedColumn::parse_spec("k:nosuchtype"), None);
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
