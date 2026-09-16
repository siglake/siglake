use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};

use crate::index_config::{FieldMapping, FieldType, IndexConfig, MappingMode};
use crate::promote::{json_as_bool, json_as_f64, json_as_i64, json_scalar_as_string};
use crate::{CoreError, Result, TIMESTAMP_NS_COLUMN, TIMESTAMP_TZ};

#[derive(Debug, Clone)]
pub struct CarrierBatchMapping {
    pub batch: RecordBatch,
    pub strict_residual_rows: usize,
}

enum Plan<'a> {
    Carrier(ArrayRef),
    Attribute {
        field: &'a FieldMapping,
        builder_index: usize,
    },
}

enum Builder {
    Text(StringBuilder),
    Long(Int64Builder),
    Double(Float64Builder),
    Bool(BooleanBuilder),
    Datetime(TimestampMicrosecondBuilder),
    Bytes(BinaryBuilder),
    Json(StringBuilder),
}

struct AppendStatus {
    extracted: bool,
    is_null: bool,
}

enum ParsedAttributes<'a> {
    Missing,
    Raw(&'a str),
    Object(serde_json::Map<String, serde_json::Value>),
}

pub fn map_carrier_batch(batch: &RecordBatch, config: &IndexConfig) -> Result<RecordBatch> {
    Ok(map_carrier_batch_with_stats(batch, config)?.batch)
}

pub fn map_carrier_batch_with_stats(
    batch: &RecordBatch,
    config: &IndexConfig,
) -> Result<CarrierBatchMapping> {
    let row_count = batch.num_rows();
    let timestamp = batch
        .column_by_name("timestamp")
        .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
        .ok_or_else(|| {
            CoreError::InvalidEvent(
                "carrier batch is missing a Timestamp(Microsecond) `timestamp` column".into(),
            )
        })?;
    // The exact-nanosecond sibling, when the carrier has one. A config that
    // declares it (the builtin `events` mapping does) gets the carrier column
    // verbatim rather than a null from the attributes extractor. Only taken as
    // the twin of the canonical `timestamp` column — see `nanos_source_column`.
    let timestamp_ns = batch
        .column_by_name(TIMESTAMP_NS_COLUMN)
        .map(|c| {
            c.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                CoreError::InvalidEvent(format!(
                    "carrier batch `{TIMESTAMP_NS_COLUMN}` column must be Int64"
                ))
            })
        })
        .transpose()?;
    let host = carrier_text_column(batch, "host")?;
    let source = carrier_text_column(batch, "source")?;
    let sourcetype = carrier_text_column(batch, "sourcetype")?;
    let index = carrier_text_column(batch, "index")?;
    let raw = carrier_text_column(batch, "raw")?;
    let attributes = batch
        .column_by_name("attributes")
        .map(|c| {
            c.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                CoreError::InvalidEvent("carrier batch `attributes` column must be Utf8".into())
            })
        })
        .transpose()?;

    let canonical_event_time = config.doc_mapping.timestamp_field == "timestamp";

    let mut plans = Vec::with_capacity(config.doc_mapping.field_mappings.len());
    let mut builders = Vec::new();
    let mut required_null_counts = Vec::new();

    for field in &config.doc_mapping.field_mappings {
        if field.name == config.doc_mapping.timestamp_field {
            plans.push(Plan::Carrier(Arc::new(timestamp.clone())));
            continue;
        }
        // Substituted only for the sibling of the canonical `timestamp` column,
        // matching `nanos_source_column` and the index sort order: an index that
        // declares another event-time field is free to carry an unrelated `long`
        // it happens to call `timestamp_ns`, and that value is user data.
        if canonical_event_time
            && field.name == TIMESTAMP_NS_COLUMN
            && matches!(field.field_type, FieldType::Long)
        {
            if let Some(ns) = timestamp_ns {
                plans.push(Plan::Carrier(Arc::new(ns.clone())));
                continue;
            }
        }
        if let Some(array) = carrier_text_array(field, host, source, sourcetype, index, raw) {
            plans.push(Plan::Carrier(array));
            continue;
        }
        let builder_index = builders.len();
        builders.push(new_builder(&field.field_type, row_count));
        required_null_counts.push(0usize);
        plans.push(Plan::Attribute {
            field,
            builder_index,
        });
    }

    let mut residual = StringBuilder::with_capacity(row_count, row_count * 64);
    let mut strict_residual_rows = 0usize;

    for row in 0..row_count {
        let mut parsed = parse_attributes(attributes, row)?;
        for plan in &plans {
            let Plan::Attribute {
                field,
                builder_index,
            } = plan
            else {
                continue;
            };
            let value = match &parsed {
                ParsedAttributes::Object(map) => map.get(field.name.as_str()),
                ParsedAttributes::Missing | ParsedAttributes::Raw(_) => None,
            };
            let status = builders[*builder_index].append(value)?;
            if status.is_null && field.required {
                required_null_counts[*builder_index] += 1;
            }
            if status.extracted {
                if let ParsedAttributes::Object(map) = &mut parsed {
                    map.remove(field.name.as_str());
                }
            }
        }

        let residual_value = match config.doc_mapping.mode {
            MappingMode::Lenient => None,
            MappingMode::Dynamic | MappingMode::Strict => residual_json(parsed)?,
        };
        if config.doc_mapping.mode == MappingMode::Strict && residual_value.is_some() {
            strict_residual_rows += 1;
        }
        residual.append_option(residual_value.as_deref());
    }

    for plan in &plans {
        let Plan::Attribute {
            field,
            builder_index,
        } = plan
        else {
            continue;
        };
        let nulls = required_null_counts[*builder_index];
        if field.required && nulls > 0 {
            return Err(CoreError::InvalidEvent(format!(
                "mapped required field `{}` produced null for {nulls} rows",
                field.name
            )));
        }
    }

    let mut columns = Vec::with_capacity(config.doc_mapping.field_mappings.len() + 1);
    for plan in plans {
        match plan {
            Plan::Carrier(array) => columns.push(array),
            Plan::Attribute { builder_index, .. } => columns.push(builders[builder_index].finish()),
        }
    }
    columns.push(Arc::new(residual.finish()));
    // A probe build declares one extra column on every table schema, index
    // tables included (see `ROLLBACK_PROBE_COLUMN`), so the mapped batch has to
    // carry it. The carrier's own values are passed through: an index table
    // then records the same value the events table does.
    #[cfg(feature = "experimental-schema-rollback-probe")]
    columns.push(match batch.column_by_name(crate::ROLLBACK_PROBE_COLUMN) {
        Some(column) => Arc::clone(column),
        None => arrow_array::new_null_array(&arrow_schema::DataType::Int64, row_count),
    });

    let schema = config.to_arrow_schema();
    let batch = RecordBatch::try_new(schema, columns).map_err(CoreError::from)?;
    Ok(CarrierBatchMapping {
        batch,
        strict_residual_rows,
    })
}

fn carrier_text_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            CoreError::InvalidEvent(format!("carrier batch is missing a Utf8 `{name}` column"))
        })
}

fn carrier_text_array(
    field: &FieldMapping,
    host: &StringArray,
    source: &StringArray,
    sourcetype: &StringArray,
    index: &StringArray,
    raw: &StringArray,
) -> Option<ArrayRef> {
    if !matches!(field.field_type, FieldType::Text { .. }) {
        return None;
    }
    let array: &StringArray = match field.name.as_str() {
        "host" => host,
        "source" => source,
        "sourcetype" => sourcetype,
        "index" => index,
        "raw" => raw,
        _ => return None,
    };
    Some(Arc::new(array.clone()))
}

fn new_builder(field_type: &FieldType, row_count: usize) -> Builder {
    match field_type {
        FieldType::Text { .. } => {
            Builder::Text(StringBuilder::with_capacity(row_count, row_count * 16))
        }
        FieldType::Long => Builder::Long(Int64Builder::with_capacity(row_count)),
        FieldType::Double => Builder::Double(Float64Builder::with_capacity(row_count)),
        FieldType::Bool => Builder::Bool(BooleanBuilder::with_capacity(row_count)),
        FieldType::Datetime => Builder::Datetime(
            TimestampMicrosecondBuilder::with_capacity(row_count).with_timezone(TIMESTAMP_TZ),
        ),
        FieldType::Bytes => Builder::Bytes(BinaryBuilder::new()),
        FieldType::Json => Builder::Json(StringBuilder::with_capacity(row_count, row_count * 32)),
    }
}

impl Builder {
    fn append(&mut self, value: Option<&serde_json::Value>) -> Result<AppendStatus> {
        let status = match self {
            Builder::Text(builder) => match value.and_then(json_scalar_as_string) {
                Some(text) => {
                    builder.append_value(text);
                    AppendStatus {
                        extracted: true,
                        is_null: false,
                    }
                }
                None => {
                    builder.append_null();
                    AppendStatus {
                        extracted: false,
                        is_null: true,
                    }
                }
            },
            Builder::Long(builder) => match value.and_then(json_as_i64) {
                Some(v) => {
                    builder.append_value(v);
                    AppendStatus {
                        extracted: true,
                        is_null: false,
                    }
                }
                None => {
                    builder.append_null();
                    AppendStatus {
                        extracted: false,
                        is_null: true,
                    }
                }
            },
            Builder::Double(builder) => match value.and_then(json_as_f64) {
                Some(v) => {
                    builder.append_value(v);
                    AppendStatus {
                        extracted: true,
                        is_null: false,
                    }
                }
                None => {
                    builder.append_null();
                    AppendStatus {
                        extracted: false,
                        is_null: true,
                    }
                }
            },
            Builder::Bool(builder) => match value.and_then(json_as_bool) {
                Some(v) => {
                    builder.append_value(v);
                    AppendStatus {
                        extracted: true,
                        is_null: false,
                    }
                }
                None => {
                    builder.append_null();
                    AppendStatus {
                        extracted: false,
                        is_null: true,
                    }
                }
            },
            Builder::Datetime(builder) => {
                builder.append_null();
                AppendStatus {
                    extracted: false,
                    is_null: true,
                }
            }
            Builder::Bytes(builder) => {
                builder.append_null();
                AppendStatus {
                    extracted: false,
                    is_null: true,
                }
            }
            Builder::Json(builder) => match value {
                Some(v) => {
                    let json = serde_json::to_string(v).map_err(|e| {
                        CoreError::InvalidEvent(format!("serialize residual JSON value: {e}"))
                    })?;
                    builder.append_value(json);
                    AppendStatus {
                        extracted: true,
                        is_null: false,
                    }
                }
                None => {
                    builder.append_null();
                    AppendStatus {
                        extracted: false,
                        is_null: true,
                    }
                }
            },
        };
        Ok(status)
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Builder::Text(builder) => Arc::new(builder.finish()),
            Builder::Long(builder) => Arc::new(builder.finish()),
            Builder::Double(builder) => Arc::new(builder.finish()),
            Builder::Bool(builder) => Arc::new(builder.finish()),
            Builder::Datetime(builder) => Arc::new(builder.finish()),
            Builder::Bytes(builder) => Arc::new(builder.finish()),
            Builder::Json(builder) => Arc::new(builder.finish()),
        }
    }
}

fn parse_attributes(attributes: Option<&StringArray>, row: usize) -> Result<ParsedAttributes<'_>> {
    let Some(attributes) = attributes else {
        return Ok(ParsedAttributes::Missing);
    };
    if attributes.is_null(row) {
        return Ok(ParsedAttributes::Missing);
    }
    let raw = attributes.value(row);
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(serde_json::Value::Object(map)) => Ok(ParsedAttributes::Object(map)),
        Ok(_) | Err(_) => Ok(ParsedAttributes::Raw(raw)),
    }
}

fn residual_json(parsed: ParsedAttributes<'_>) -> Result<Option<String>> {
    match parsed {
        ParsedAttributes::Missing => Ok(None),
        ParsedAttributes::Raw("") => Ok(None),
        ParsedAttributes::Raw(raw) => Ok(Some(raw.to_string())),
        ParsedAttributes::Object(map) if map.is_empty() => Ok(None),
        ParsedAttributes::Object(map) => serde_json::to_string(&map)
            .map(Some)
            .map_err(|e| CoreError::InvalidEvent(format!("serialize residual attributes: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray};
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::{events_to_record_batch, Event};

    fn carrier_event(attributes: Option<&str>) -> Event {
        let mut event = Event::now("raw-body").with_attributes(attributes.map(str::to_string));
        event.timestamp = Utc.timestamp_opt(1_700_000_000, 123).single().unwrap();
        event.host = "host-a".into();
        event.source = "source-a".into();
        event.sourcetype = "app".into();
        event.index = "carrier-index".into();
        event
    }

    fn config(mode: MappingMode) -> IndexConfig {
        IndexConfig {
            index_id: "typed-logs".into(),
            doc_mapping: crate::index_config::DocMapping {
                mode,
                field_mappings: vec![
                    FieldMapping {
                        name: "event_time".into(),
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
                        name: "raw".into(),
                        field_type: FieldType::Text {
                            tokenizer: Some("default".into()),
                        },
                        required: true,
                    },
                    FieldMapping {
                        name: "status".into(),
                        field_type: FieldType::Long,
                        required: false,
                    },
                    FieldMapping {
                        name: "ratio".into(),
                        field_type: FieldType::Double,
                        required: false,
                    },
                    FieldMapping {
                        name: "sampled".into(),
                        field_type: FieldType::Bool,
                        required: false,
                    },
                    FieldMapping {
                        name: "meta".into(),
                        field_type: FieldType::Json,
                        required: false,
                    },
                    FieldMapping {
                        name: "seen_at".into(),
                        field_type: FieldType::Datetime,
                        required: false,
                    },
                    FieldMapping {
                        name: "blob".into(),
                        field_type: FieldType::Bytes,
                        required: false,
                    },
                ],
                timestamp_field: "event_time".into(),
                tag_fields: vec!["host".into()],
                default_search_fields: vec!["raw".into()],
            },
            retention: None,
            index_at_flush: None,
        }
    }

    #[test]
    fn maps_carrier_timestamp_and_text_columns() {
        let batch = events_to_record_batch(&[carrier_event(Some(r#"{"status":200}"#))]).unwrap();
        let mapped = map_carrier_batch_with_stats(&batch, &config(MappingMode::Dynamic)).unwrap();
        let out = mapped.batch;

        let ts = out
            .column_by_name("event_time")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            ts.value(0),
            batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0)
        );

        let host = out
            .column_by_name("host")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(host.value(0), "host-a");

        let raw = out
            .column_by_name("raw")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(raw.value(0), "raw-body");
    }

    #[test]
    fn coerces_attribute_values_and_removes_successful_keys() {
        let batch = events_to_record_batch(&[carrier_event(Some(
            r#"{"status":"200","ratio":"0.5","sampled":"true","meta":{"env":"prod"},"left":"keep"}"#,
        ))])
        .unwrap();
        let mapped = map_carrier_batch_with_stats(&batch, &config(MappingMode::Dynamic)).unwrap();
        let out = mapped.batch;

        assert_eq!(
            out.column_by_name("status")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            200
        );
        assert!(
            (out.column_by_name("ratio")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                - 0.5)
                .abs()
                < 1e-9
        );
        assert!(out
            .column_by_name("sampled")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0));
        assert_eq!(
            out.column_by_name("meta")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            r#"{"env":"prod"}"#
        );
        assert_eq!(
            out.column_by_name("attributes")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            r#"{"left":"keep"}"#
        );
    }

    #[test]
    fn failed_parse_yields_null_and_retains_residual() {
        let batch = events_to_record_batch(&[carrier_event(Some(r#"{"status":"oops","left":1}"#))])
            .unwrap();
        let mapped = map_carrier_batch_with_stats(&batch, &config(MappingMode::Dynamic)).unwrap();
        let out = mapped.batch;

        let status = out
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(status.is_null(0));
        assert_eq!(
            out.column_by_name("attributes")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            r#"{"left":1,"status":"oops"}"#
        );
    }

    #[test]
    fn absent_key_yields_null_and_residual_stays_null() {
        let batch = events_to_record_batch(&[carrier_event(None)]).unwrap();
        let mapped = map_carrier_batch_with_stats(&batch, &config(MappingMode::Dynamic)).unwrap();
        let out = mapped.batch;

        assert!(out
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .is_null(0));
        assert!(out
            .column_by_name("attributes")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn lenient_mode_drops_residual_attributes() {
        let batch =
            events_to_record_batch(&[carrier_event(Some(r#"{"status":200,"left":"drop"}"#))])
                .unwrap();
        let mapped = map_carrier_batch_with_stats(&batch, &config(MappingMode::Lenient)).unwrap();
        let out = mapped.batch;
        assert!(out
            .column_by_name("attributes")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn strict_mode_counts_rows_with_residual_keys() {
        let batch = events_to_record_batch(&[
            carrier_event(Some(r#"{"status":200}"#)),
            carrier_event(Some(r#"{"status":200,"left":"keep"}"#)),
        ])
        .unwrap();
        let mapped = map_carrier_batch_with_stats(&batch, &config(MappingMode::Strict)).unwrap();
        assert_eq!(mapped.strict_residual_rows, 1);
    }

    #[test]
    fn non_text_shadowed_carrier_name_extracts_from_attributes() {
        let mut cfg = config(MappingMode::Dynamic);
        cfg.doc_mapping.field_mappings.push(FieldMapping {
            name: "source".into(),
            field_type: FieldType::Long,
            required: false,
        });
        let batch = events_to_record_batch(&[carrier_event(Some(r#"{"source":"42"}"#))]).unwrap();
        let mapped = map_carrier_batch_with_stats(&batch, &cfg).unwrap();
        let out = mapped.batch;
        assert_eq!(
            out.column_by_name("source")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
    }

    #[test]
    fn required_attribute_field_errors_with_row_count() {
        let mut cfg = config(MappingMode::Dynamic);
        cfg.doc_mapping.field_mappings.push(FieldMapping {
            name: "service".into(),
            field_type: FieldType::Text {
                tokenizer: Some("raw".into()),
            },
            required: true,
        });
        let batch = events_to_record_batch(&[
            carrier_event(Some(r#"{"service":"api"}"#)),
            carrier_event(Some(r#"{"left":"missing"}"#)),
        ])
        .unwrap();
        let err = map_carrier_batch_with_stats(&batch, &cfg).unwrap_err();
        assert!(err
            .to_string()
            .contains("mapped required field `service` produced null for 1 rows"));
    }

    fn timestamp_ns_field(required: bool) -> FieldMapping {
        FieldMapping {
            name: TIMESTAMP_NS_COLUMN.into(),
            field_type: FieldType::Long,
            required,
        }
    }

    fn carrier_nanos(batch: &RecordBatch) -> i64 {
        batch
            .column_by_name(TIMESTAMP_NS_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    /// An index that declares its own event-time field may also carry an
    /// unrelated `long` it happens to call `timestamp_ns`; the carrier's
    /// nanoseconds must not overwrite it (see `nanos_source_column`).
    #[test]
    fn independent_timestamp_ns_attribute_keeps_its_supplied_value() {
        let mut cfg = config(MappingMode::Dynamic);
        cfg.doc_mapping
            .field_mappings
            .push(timestamp_ns_field(true));
        let batch =
            events_to_record_batch(&[carrier_event(Some(r#"{"timestamp_ns":42}"#))]).unwrap();
        assert_ne!(carrier_nanos(&batch), 42);

        let out = map_carrier_batch_with_stats(&batch, &cfg).unwrap().batch;
        assert_eq!(
            out.column_by_name(TIMESTAMP_NS_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
        // Extracted from the attributes like any other mapped field, so the key
        // does not linger in the residual.
        assert!(out
            .column_by_name("attributes")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .is_null(0));
    }

    /// ...and it gets ordinary required/null validation rather than a value
    /// silently supplied by the carrier.
    #[test]
    fn independent_timestamp_ns_attribute_gets_required_validation() {
        let mut cfg = config(MappingMode::Dynamic);
        cfg.doc_mapping
            .field_mappings
            .push(timestamp_ns_field(true));
        let batch = events_to_record_batch(&[carrier_event(Some(r#"{"status":200}"#))]).unwrap();
        let err = map_carrier_batch_with_stats(&batch, &cfg).unwrap_err();
        assert!(err
            .to_string()
            .contains("mapped required field `timestamp_ns` produced null for 1 rows"));

        cfg.doc_mapping.field_mappings.pop();
        cfg.doc_mapping
            .field_mappings
            .push(timestamp_ns_field(false));
        let out = map_carrier_batch_with_stats(&batch, &cfg).unwrap().batch;
        assert!(out
            .column_by_name(TIMESTAMP_NS_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .is_null(0));
    }

    /// The builtin `events` mapping declares the sibling of the canonical
    /// `timestamp` column, so it still gets the exact carrier nanoseconds even
    /// when an attribute of that name disagrees.
    #[test]
    fn builtin_events_timestamp_ns_takes_the_carrier_nanoseconds() {
        let batch =
            events_to_record_batch(&[carrier_event(Some(r#"{"timestamp_ns":42}"#))]).unwrap();
        let expected = carrier_nanos(&batch);
        assert_ne!(expected, 42);

        let out = map_carrier_batch_with_stats(&batch, &IndexConfig::builtin_events())
            .unwrap()
            .batch;
        assert_eq!(
            out.column_by_name(TIMESTAMP_NS_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected
        );
        // The carrier column wins, so the attribute is never extracted and
        // stays in the residual.
        assert_eq!(
            out.column_by_name("attributes")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            r#"{"timestamp_ns":42}"#
        );
    }

    #[test]
    fn datetime_and_bytes_extraction_are_null_and_residual_retained() {
        let batch = events_to_record_batch(&[carrier_event(Some(
            r#"{"seen_at":"2024-01-01T00:00:00Z","blob":"abc","left":1}"#,
        ))])
        .unwrap();
        let mapped = map_carrier_batch_with_stats(&batch, &config(MappingMode::Dynamic)).unwrap();
        let out = mapped.batch;

        assert!(out
            .column_by_name("seen_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .is_null(0));
        assert!(out
            .column_by_name("blob")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .is_null(0));
        assert_eq!(
            out.column_by_name("attributes")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            r#"{"blob":"abc","left":1,"seen_at":"2024-01-01T00:00:00Z"}"#
        );
    }
}
