//! Schema for siglake's `query_audit` table.
//!
//! This module used to hold the detection pipeline's table schemas too. Those
//! moved out with the pipeline: they are declared as ordinary user indexes by
//! the consumer that writes them, so siglake no longer carries them.
//!
//! Carries `PARQUET:field_id` metadata on every column so it round-trips
//! cleanly through `iceberg::arrow::arrow_schema_to_schema`, same as the
//! events table.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::PARQUET_FIELD_ID_KEY;

fn with_field_id(field: Field, id: i32) -> Field {
    let mut md = std::collections::HashMap::new();
    md.insert(PARQUET_FIELD_ID_KEY.to_string(), id.to_string());
    field.with_metadata(md)
}

/// Microsecond `timestamptz`, same as every other siglake table. The audit
/// table has no nanosecond sibling: rows are one-per-query and microsecond ties
/// carry no meaning here (decided 2026-09-06).
fn ts() -> DataType {
    crate::timestamp_data_type()
}

/// `query_audit` table schema. One row per query executed against the
/// query-server (both interactive and batch). Captures the caller
/// identity (when OIDC is on), the query string, the resolved cost
/// estimate, and the final outcome. Used by ops dashboards + audit
/// reviews.
///
/// Field IDs: 1: timestamp, 2: subject, 3: email, 4: endpoint,
/// 5: query, 6: format, 7: priority, 8: duration_ms, 9: status,
/// 10: complexity, 11: estimated_bytes_scanned,
/// 12: estimated_rows_processed, 13: truncated, 14: error.
pub fn query_audit_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        with_field_id(Field::new("timestamp", ts(), false), 1),
        with_field_id(Field::new("subject", DataType::Utf8, false), 2),
        with_field_id(Field::new("email", DataType::Utf8, true), 3),
        with_field_id(Field::new("endpoint", DataType::Utf8, false), 4),
        with_field_id(Field::new("query", DataType::Utf8, false), 5),
        with_field_id(Field::new("format", DataType::Utf8, true), 6),
        with_field_id(Field::new("priority", DataType::Utf8, false), 7),
        with_field_id(Field::new("duration_ms", DataType::Int64, false), 8),
        with_field_id(Field::new("status", DataType::Utf8, false), 9),
        with_field_id(Field::new("complexity", DataType::Utf8, true), 10),
        with_field_id(
            Field::new("estimated_bytes_scanned", DataType::Int64, true),
            11,
        ),
        with_field_id(
            Field::new("estimated_rows_processed", DataType::Int64, true),
            12,
        ),
        with_field_id(Field::new("truncated", DataType::Boolean, false), 13),
        with_field_id(Field::new("error", DataType::Utf8, true), 14),
    ]))
}

pub const QUERY_AUDIT_BLOOM_COLUMNS: &[&str] =
    &["subject", "endpoint", "status", "priority", "complexity"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemas_have_field_ids() {
        for sch in [query_audit_schema()] {
            for (i, f) in sch.fields().iter().enumerate() {
                let got = f.metadata().get(PARQUET_FIELD_ID_KEY).unwrap_or_else(|| {
                    panic!("field {} `{}` missing PARQUET:field_id", i, f.name())
                });
                assert_eq!(
                    got,
                    &(i as i32 + 1).to_string(),
                    "field {} `{}`",
                    i,
                    f.name()
                );
            }
        }
    }
}
