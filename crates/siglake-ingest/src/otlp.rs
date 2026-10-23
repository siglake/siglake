use arrow_array::RecordBatch;
use base64::Engine as _;
use chrono::{DateTime, TimeZone, Utc};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess as ProtoExportLogsPartialSuccess,
    ExportLogsServiceRequest as ProtoExportLogsServiceRequest,
    ExportLogsServiceResponse as ProtoExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{
    any_value::Value as ProtoValue, AnyValue as ProtoAnyValue, ArrayValue as ProtoArrayValue,
    KeyValue as ProtoKeyValue, KeyValueList as ProtoKeyValueList,
};
use opentelemetry_proto::tonic::logs::v1::{
    LogRecord as ProtoLogRecord, ResourceLogs as ProtoResourceLogs, ScopeLogs as ProtoScopeLogs,
    SeverityNumber,
};
use opentelemetry_proto::tonic::resource::v1::Resource as ProtoResource;
use prost::Message;
use serde::{Deserialize, Deserializer, Serialize};
use siglake_core::{Event, EventBatchBuilder};
use std::borrow::Cow;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportLogsServiceRequest<'a> {
    #[serde(default, borrow)]
    pub resource_logs: Vec<ResourceLogs<'a>>,
}

pub type ExportLogsServiceResponse = ProtoExportLogsServiceResponse;
pub type ExportLogsPartialSuccess = ProtoExportLogsPartialSuccess;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceLogs<'a> {
    #[serde(default, borrow)]
    pub resource: Option<Resource<'a>>,
    #[serde(default, borrow)]
    pub scope_logs: Vec<ScopeLogs<'a>>,
    #[serde(default, borrow)]
    pub schema_url: Cow<'a, str>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource<'a> {
    #[serde(default, borrow)]
    pub attributes: Vec<KeyValue<'a>>,
    #[serde(default)]
    pub dropped_attributes_count: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeLogs<'a> {
    #[serde(default, borrow)]
    pub scope: Option<InstrumentationScope<'a>>,
    #[serde(default, borrow)]
    pub log_records: Vec<LogRecord<'a>>,
    #[serde(default, borrow)]
    pub schema_url: Cow<'a, str>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentationScope<'a> {
    #[serde(default, borrow)]
    pub name: Cow<'a, str>,
    #[serde(default, borrow)]
    pub version: Cow<'a, str>,
    #[serde(default, borrow)]
    pub attributes: Vec<KeyValue<'a>>,
    #[serde(default)]
    pub dropped_attributes_count: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogRecord<'a> {
    #[serde(default, deserialize_with = "deserialize_u64")]
    pub time_unix_nano: u64,
    #[serde(default, deserialize_with = "deserialize_u64")]
    pub observed_time_unix_nano: u64,
    #[serde(default, deserialize_with = "deserialize_severity_number")]
    pub severity_number: i32,
    #[serde(default, borrow)]
    pub severity_text: Cow<'a, str>,
    #[serde(default, borrow)]
    pub body: Option<AnyValue<'a>>,
    #[serde(default, borrow)]
    pub attributes: Vec<KeyValue<'a>>,
    #[serde(default)]
    pub dropped_attributes_count: u32,
    #[serde(default)]
    pub flags: u32,
    #[serde(default, borrow)]
    pub trace_id: Cow<'a, str>,
    #[serde(default, borrow)]
    pub span_id: Cow<'a, str>,
    #[serde(default, borrow)]
    pub event_name: Cow<'a, str>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValue<'a> {
    #[serde(borrow)]
    pub key: Cow<'a, str>,
    #[serde(default, borrow)]
    pub value: Option<AnyValue<'a>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnyValue<'a> {
    #[serde(default, borrow)]
    pub string_value: Option<Cow<'a, str>>,
    #[serde(default)]
    pub bool_value: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_option_i64")]
    pub int_value: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_option_f64")]
    pub double_value: Option<f64>,
    #[serde(default, borrow)]
    pub array_value: Option<ArrayValue<'a>>,
    #[serde(default, borrow)]
    pub kvlist_value: Option<KeyValueList<'a>>,
    #[serde(default, borrow)]
    pub bytes_value: Option<Cow<'a, str>>,
    #[serde(skip)]
    #[doc(hidden)]
    pub bytes_value_encoding: BytesValueEncoding,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[doc(hidden)]
pub enum BytesValueEncoding {
    #[default]
    Base64,
    Hex,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArrayValue<'a> {
    #[serde(default, borrow)]
    pub values: Vec<AnyValue<'a>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValueList<'a> {
    #[serde(default, borrow)]
    pub values: Vec<KeyValue<'a>>,
}

/// Parse an OTLP/HTTP logs JSON body into [`ExportLogsServiceRequest`].
///
/// Uses `simd-json` (SIMD-accelerated, in-situ) instead of `serde_json` — JSON
/// deserialization is the dominant pre-WAL ingest CPU. simd-json unescapes
/// strings in place in the buffer, so the result borrows attribute keys + string
/// values straight from `buf` (zero-copy via the `Cow` fields) — the caller must
/// keep `buf` alive across the subsequent `otlp_logs_to_events` mapping.
pub fn parse_otlp_json(buf: &mut [u8]) -> Result<ExportLogsServiceRequest<'_>, String> {
    simd_json::serde::from_slice(buf).map_err(|e| e.to_string())
}

/// Decode an OTLP/protobuf `ExportLogsServiceRequest` via the canonical
/// `opentelemetry-proto` (prost) types, then convert into the local structs so
/// the shared `otlp_logs_to_events` mapping and JSON path stay unchanged.
pub fn decode_export_logs_service_request_protobuf(
    bytes: &[u8],
) -> Result<ExportLogsServiceRequest<'static>, String> {
    let proto = ProtoExportLogsServiceRequest::decode(bytes)
        .map_err(|e| format!("protobuf decode error: {e}"))?;
    Ok(convert_request(proto))
}

pub fn otlp_proto_logs_to_events(proto: ProtoExportLogsServiceRequest) -> Vec<Event> {
    otlp_logs_to_events(convert_request(proto))
}

/// gRPC-path equivalent of [`otlp_logs_to_record_batch`].
pub fn otlp_proto_logs_to_record_batch(
    proto: ProtoExportLogsServiceRequest,
) -> siglake_core::Result<RecordBatch> {
    otlp_logs_to_record_batch(convert_request(proto))
}

pub fn export_logs_service_success_response(accepted_events: usize) -> ExportLogsServiceResponse {
    let partial_success = (accepted_events == 0).then(|| ExportLogsPartialSuccess {
        rejected_log_records: 0,
        error_message: "request contained no log records".to_string(),
    });
    ExportLogsServiceResponse { partial_success }
}

pub fn encode_export_logs_service_response_protobuf(
    response: &ExportLogsServiceResponse,
) -> Vec<u8> {
    response.encode_to_vec()
}

// The protobuf path produces owned data, so its local structs are `'static`
// (every string is `Cow::Owned`). Only the JSON path borrows from the buffer.
fn convert_request(proto: ProtoExportLogsServiceRequest) -> ExportLogsServiceRequest<'static> {
    ExportLogsServiceRequest {
        resource_logs: proto
            .resource_logs
            .into_iter()
            .map(convert_resource_logs)
            .collect(),
    }
}

fn convert_resource_logs(proto: ProtoResourceLogs) -> ResourceLogs<'static> {
    ResourceLogs {
        resource: proto.resource.map(convert_resource),
        scope_logs: proto
            .scope_logs
            .into_iter()
            .map(convert_scope_logs)
            .collect(),
        schema_url: Cow::Owned(proto.schema_url),
    }
}

fn convert_resource(proto: ProtoResource) -> Resource<'static> {
    Resource {
        attributes: proto
            .attributes
            .into_iter()
            .map(convert_key_value)
            .collect(),
        dropped_attributes_count: proto.dropped_attributes_count,
    }
}

fn convert_scope_logs(proto: ProtoScopeLogs) -> ScopeLogs<'static> {
    ScopeLogs {
        scope: proto.scope.map(|scope| InstrumentationScope {
            name: Cow::Owned(scope.name),
            version: Cow::Owned(scope.version),
            attributes: scope
                .attributes
                .into_iter()
                .map(convert_key_value)
                .collect(),
            dropped_attributes_count: scope.dropped_attributes_count,
        }),
        log_records: proto
            .log_records
            .into_iter()
            .map(convert_log_record)
            .collect(),
        schema_url: Cow::Owned(proto.schema_url),
    }
}

fn convert_log_record(proto: ProtoLogRecord) -> LogRecord<'static> {
    LogRecord {
        time_unix_nano: proto.time_unix_nano,
        observed_time_unix_nano: proto.observed_time_unix_nano,
        severity_number: proto.severity_number,
        severity_text: Cow::Owned(proto.severity_text),
        body: proto.body.map(convert_any_value),
        attributes: proto
            .attributes
            .into_iter()
            .map(convert_key_value)
            .collect(),
        dropped_attributes_count: proto.dropped_attributes_count,
        flags: proto.flags,
        trace_id: Cow::Owned(hex_encode(&proto.trace_id)),
        span_id: Cow::Owned(hex_encode(&proto.span_id)),
        event_name: Cow::Owned(proto.event_name),
    }
}

fn convert_key_value(proto: ProtoKeyValue) -> KeyValue<'static> {
    KeyValue {
        key: Cow::Owned(proto.key),
        value: proto.value.map(convert_any_value),
    }
}

fn convert_any_value(proto: ProtoAnyValue) -> AnyValue<'static> {
    let mut out = AnyValue::default();
    match proto.value {
        Some(ProtoValue::StringValue(value)) => out.string_value = Some(Cow::Owned(value)),
        Some(ProtoValue::BoolValue(value)) => out.bool_value = Some(value),
        Some(ProtoValue::IntValue(value)) => out.int_value = Some(value),
        Some(ProtoValue::DoubleValue(value)) => out.double_value = Some(value),
        Some(ProtoValue::ArrayValue(value)) => out.array_value = Some(convert_array_value(value)),
        Some(ProtoValue::KvlistValue(value)) => out.kvlist_value = Some(convert_kvlist(value)),
        Some(ProtoValue::BytesValue(value)) => {
            out.bytes_value = Some(Cow::Owned(hex_encode(&value)));
            out.bytes_value_encoding = BytesValueEncoding::Hex;
        }
        // Other AnyValue variants (e.g. string_value_strindex from the
        // profiles signal) are not used by the logs mapping; ignore them.
        _ => {}
    }
    out
}

fn convert_array_value(proto: ProtoArrayValue) -> ArrayValue<'static> {
    ArrayValue {
        values: proto.values.into_iter().map(convert_any_value).collect(),
    }
}

fn convert_kvlist(proto: ProtoKeyValueList) -> KeyValueList<'static> {
    KeyValueList {
        values: proto.values.into_iter().map(convert_key_value).collect(),
    }
}

/// One mapped log record with every core field still borrowing from the parsed
/// request (and, on the JSON path, from the request buffer itself). `raw` is
/// borrowed for the common string-body case and owned only when the body needs
/// JSON serialization; `attributes` is the residual JSON built per record.
struct MappedRecord<'a> {
    timestamp: DateTime<Utc>,
    host: &'a str,
    source: &'a str,
    sourcetype: &'a str,
    index: &'a str,
    raw: Cow<'a, str>,
    attributes: Option<String>,
}

fn total_records(req: &ExportLogsServiceRequest<'_>) -> usize {
    req.resource_logs
        .iter()
        .flat_map(|rl| &rl.scope_logs)
        .map(|sl| sl.log_records.len())
        .sum()
}

/// Walk the request and hand each mapped record to `emit` with borrowed
/// fields. Shared core of [`otlp_logs_to_events`] (owned `Event`s, kept for
/// replay/tee/test callers) and [`otlp_logs_to_record_batch`] (the ingest hot
/// path — appends straight into Arrow builders, no per-event `String`s).
fn map_logs<'a>(req: &'a ExportLogsServiceRequest<'a>, mut emit: impl FnMut(MappedRecord<'a>)) {
    for resource_logs in &req.resource_logs {
        let resource_attrs = resource_logs
            .resource
            .as_ref()
            .map(|resource| resource.attributes.as_slice())
            .unwrap_or(&[]);
        let host = string_attr(resource_attrs, "host.name").unwrap_or("unknown");
        let service_name = string_attr(resource_attrs, "service.name");
        // WS-7: the residual resource attributes are identical for every record
        // in this resourceLogs — build that JSON fragment once, not per record.
        let resource_fragment = build_residual_fragment(resource_attrs, CORE_RESOURCE_KEYS);
        for scope_logs in &resource_logs.scope_logs {
            let scope_name = scope_logs
                .scope
                .as_ref()
                .and_then(|scope| (!scope.name.is_empty()).then_some(scope.name.as_ref()));
            let source = service_name.or(scope_name).unwrap_or("otel");
            for record in &scope_logs.log_records {
                // Single pass over the record's attributes: pull `sourcetype` +
                // `index` and build the residual JSON in one scan (was 3 scans).
                let (sourcetype, index, attributes) = map_record_attributes(
                    record,
                    resource_attrs,
                    &resource_fragment,
                    resource_logs,
                    scope_logs,
                );
                emit(MappedRecord {
                    timestamp: timestamp_for_record(record),
                    host,
                    source,
                    sourcetype: sourcetype.unwrap_or("otel:logs"),
                    index: index.unwrap_or("main"),
                    raw: body_to_raw(record.body.as_ref()),
                    attributes,
                });
            }
        }
    }
}

pub fn otlp_logs_to_events(req: ExportLogsServiceRequest<'_>) -> Vec<Event> {
    let mut events = Vec::with_capacity(total_records(&req));
    map_logs(&req, |rec| {
        events.push(Event {
            timestamp: rec.timestamp,
            host: rec.host.to_string(),
            source: rec.source.to_string(),
            sourcetype: rec.sourcetype.to_string(),
            index: rec.index.to_string(),
            raw: rec.raw.into_owned(),
            attributes: rec.attributes,
        });
    });
    events
}

/// Map an OTLP logs request straight into the canonical `events` [`RecordBatch`]
/// (the WAL currency), skipping the owned [`Event`] entirely. On the JSON path
/// the core string fields still borrow from the request buffer, so the only
/// per-event copy is the append into the Arrow builders — this removes the
/// 5–6 per-event heap allocations the `Vec<Event>` detour used to cost.
///
/// Errors only on a timestamp outside Arrow's nanosecond range (same contract
/// as `events_to_record_batch`).
pub fn otlp_logs_to_record_batch(
    req: ExportLogsServiceRequest<'_>,
) -> siglake_core::Result<RecordBatch> {
    let mut builder = EventBatchBuilder::with_capacity(total_records(&req));
    let mut first_err: Option<siglake_core::CoreError> = None;
    map_logs(&req, |rec| {
        if first_err.is_some() {
            return;
        }
        if let Err(e) = builder.append(
            rec.timestamp,
            rec.host,
            rec.source,
            rec.sourcetype,
            rec.index,
            &rec.raw,
            rec.attributes.as_deref(),
        ) {
            first_err = Some(e);
        }
    });
    if let Some(e) = first_err {
        return Err(e);
    }
    builder.finish()
}

/// Resource attribute keys that already map to a core column (`host`/`source`);
/// excluded from the residual `attributes` map so we don't duplicate them. (The
/// record-level core keys `sourcetype`/`index` are matched directly in
/// [`map_record_attributes`].)
const CORE_RESOURCE_KEYS: &[&str] = &["host.name", "service.name"];

/// Build the inner `"k":v,"k2":v2` JSON fragment (no braces) of the non-core
/// attributes in `attrs`, excluding `core_keys`. Empty string if none. Built in
/// one pass with inline escaping — no `serde_json::Value` tree or per-value
/// allocations (this is the WS-7 ingest hot path; round-3 perf work).
fn build_residual_fragment(attrs: &[KeyValue], core_keys: &[&str]) -> String {
    let mut out = String::with_capacity(attrs.len() * 32);
    for kv in attrs {
        if core_keys.contains(&kv.key.as_ref()) {
            continue;
        }
        let Some(v) = kv.value.as_ref() else { continue };
        if !out.is_empty() {
            out.push(',');
        }
        write_json_str(&mut out, &kv.key);
        out.push(':');
        write_any_value_json(&mut out, v);
    }
    out
}

/// Single pass over a record's attributes: extract the core `sourcetype` +
/// `index` (borrowed) and build the residual-attribute JSON object — combining
/// the precomputed resource `fragment` with the record's non-core attributes —
/// in one scan instead of three. Returns `(sourcetype, index, attributes_json)`;
/// `attributes` is `None` when there are no residuals. Record attributes follow
/// the resource ones (a later duplicate key reads as last-wins). Lossless WS-7
/// capture, LIKE-able.
///
/// The `String` is allocated lazily — only when the first residual attr is seen
/// — so records with only core attrs (`sourcetype` / `index`) pay zero alloc.
#[allow(clippy::type_complexity)]
fn map_record_attributes<'a>(
    record: &'a LogRecord<'a>,
    resource_attrs: &'a [KeyValue<'a>],
    resource_fragment: &str,
    resource_logs: &'a ResourceLogs<'a>,
    scope_logs: &'a ScopeLogs<'a>,
) -> (Option<&'a str>, Option<&'a str>, Option<String>) {
    let record_attrs = &record.attributes;
    let mut sourcetype = None;
    let mut index = None;
    // Defer the String alloc: initialize with the resource fragment when it
    // is non-empty (always has content), otherwise leave it None until the
    // first residual record attr triggers the allocation.
    let has_context = has_otel_context(record, resource_attrs, resource_logs, scope_logs);
    let record_residual_cap =
        1 + resource_fragment.len() + record_attrs.len() * 32 + if has_context { 256 } else { 0 };
    let mut out = if has_context {
        let mut s = String::with_capacity(record_residual_cap);
        s.push('{');
        write_json_str(&mut s, "otel");
        s.push(':');
        write_otel_context(&mut s, record, resource_attrs, resource_logs, scope_logs);
        if !resource_fragment.is_empty() {
            s.push(',');
            s.push_str(resource_fragment);
        }
        Some(s)
    } else if resource_fragment.is_empty() {
        None
    } else {
        let mut s = String::with_capacity(record_residual_cap);
        s.push('{');
        s.push_str(resource_fragment);
        Some(s)
    };
    for kv in record_attrs {
        match kv.key.as_ref() {
            "sourcetype" => {
                sourcetype = kv.value.as_ref().and_then(|v| v.string_value.as_deref());
            }
            "index" => {
                index = kv.value.as_ref().and_then(|v| v.string_value.as_deref());
            }
            _ => {
                let Some(v) = kv.value.as_ref() else { continue };
                let o = out.get_or_insert_with(|| {
                    let mut s = String::with_capacity(record_residual_cap);
                    s.push('{');
                    s
                });
                // `o.len() > 1` means there's already content after the opening `{`.
                if o.len() > 1 {
                    o.push(',');
                }
                write_json_str(o, &kv.key);
                o.push(':');
                write_any_value_json(o, v);
            }
        }
    }
    let attributes = out.map(|mut s| {
        s.push('}');
        s
    });
    (sourcetype, index, attributes)
}

fn has_otel_context(
    record: &LogRecord<'_>,
    resource_attrs: &[KeyValue<'_>],
    resource_logs: &ResourceLogs<'_>,
    scope_logs: &ScopeLogs<'_>,
) -> bool {
    record.observed_time_unix_nano != 0
        || record.severity_number != 0
        || !record.severity_text.is_empty()
        || record.dropped_attributes_count != 0
        || record.flags != 0
        || !record.trace_id.is_empty()
        || !record.span_id.is_empty()
        || !record.event_name.is_empty()
        || !resource_logs.schema_url.is_empty()
        || resource_logs
            .resource
            .as_ref()
            .is_some_and(|resource| resource.dropped_attributes_count != 0)
        || !scope_logs.schema_url.is_empty()
        || any_value_sidecar_needed(record, resource_attrs, scope_logs)
        || scope_logs.scope.as_ref().is_some_and(|scope| {
            !scope.version.is_empty()
                || !scope.attributes.is_empty()
                || scope.dropped_attributes_count != 0
        })
        || resource_attrs.iter().any(|resource| {
            !CORE_RESOURCE_KEYS.contains(&resource.key.as_ref())
                && resource.value.is_some()
                && record.attributes.iter().any(|record| {
                    record.key == resource.key
                        && record.value.is_some()
                        && !matches!(record.key.as_ref(), "sourcetype" | "index")
                })
        })
}

fn write_otel_context(
    out: &mut String,
    record: &LogRecord<'_>,
    resource_attrs: &[KeyValue<'_>],
    resource_logs: &ResourceLogs<'_>,
    scope_logs: &ScopeLogs<'_>,
) {
    use std::fmt::Write as _;

    out.push('{');
    let mut first = true;
    let mut field = |out: &mut String, name: &str| {
        if !first {
            out.push(',');
        }
        first = false;
        write_json_str(out, name);
        out.push(':');
    };

    if record.severity_number != 0 {
        field(out, "severity_number");
        let _ = write!(out, "{}", record.severity_number);
    }
    if !record.severity_text.is_empty() {
        field(out, "severity_text");
        write_json_str(out, &record.severity_text);
    }
    if !record.trace_id.is_empty() {
        field(out, "trace_id");
        write_ascii_lowercase_json_str(out, &record.trace_id);
    }
    if !record.span_id.is_empty() {
        field(out, "span_id");
        write_ascii_lowercase_json_str(out, &record.span_id);
    }
    if record.flags != 0 {
        field(out, "flags");
        let _ = write!(out, "{}", record.flags);
    }
    if record.observed_time_unix_nano != 0 {
        field(out, "observed_time_unix_nano");
        let _ = write!(out, "{}", record.observed_time_unix_nano);
    }
    if !record.event_name.is_empty() {
        field(out, "event_name");
        write_json_str(out, &record.event_name);
    }
    if record.dropped_attributes_count != 0 {
        field(out, "dropped_attributes_count");
        let _ = write!(out, "{}", record.dropped_attributes_count);
    }

    field(out, "timestamp_source");
    write_json_str(out, timestamp_source(record));

    if any_value_sidecar_needed(record, resource_attrs, scope_logs) {
        field(out, "any_values");
        write_any_value_sidecar(out, record, resource_attrs, scope_logs);
    }

    let resource = resource_logs.resource.as_ref();
    let has_resource_context = !resource_logs.schema_url.is_empty()
        || resource.is_some_and(|resource| resource.dropped_attributes_count != 0)
        || resource_attrs.iter().any(|resource| {
            !CORE_RESOURCE_KEYS.contains(&resource.key.as_ref())
                && resource.value.is_some()
                && record.attributes.iter().any(|record| {
                    record.key == resource.key
                        && record.value.is_some()
                        && !matches!(record.key.as_ref(), "sourcetype" | "index")
                })
        });
    if has_resource_context {
        field(out, "resource");
        out.push('{');
        let mut resource_first = true;
        if !resource_logs.schema_url.is_empty() {
            write_object_field(out, &mut resource_first, "schema_url");
            write_json_str(out, &resource_logs.schema_url);
        }
        if let Some(dropped) = resource
            .map(|resource| resource.dropped_attributes_count)
            .filter(|dropped| *dropped != 0)
        {
            write_object_field(out, &mut resource_first, "dropped_attributes_count");
            let _ = write!(out, "{dropped}");
        }
        let mut wrote_attributes = false;
        for resource in resource_attrs {
            let shadowed = !CORE_RESOURCE_KEYS.contains(&resource.key.as_ref())
                && resource.value.is_some()
                && record.attributes.iter().any(|record| {
                    record.key == resource.key
                        && record.value.is_some()
                        && !matches!(record.key.as_ref(), "sourcetype" | "index")
                });
            if !shadowed {
                continue;
            }
            if !wrote_attributes {
                write_object_field(out, &mut resource_first, "attributes");
                out.push('{');
                wrote_attributes = true;
            } else {
                out.push(',');
            }
            write_json_str(out, &resource.key);
            out.push(':');
            write_any_value_json(out, resource.value.as_ref().expect("checked above"));
        }
        if wrote_attributes {
            out.push('}');
        }
        out.push('}');
    }

    let scope = scope_logs.scope.as_ref();
    if scope.is_some() || !scope_logs.schema_url.is_empty() {
        field(out, "scope");
        out.push('{');
        let mut scope_first = true;
        if let Some(scope) = scope {
            if !scope.name.is_empty() {
                write_object_field(out, &mut scope_first, "name");
                write_json_str(out, &scope.name);
            }
            if !scope.version.is_empty() {
                write_object_field(out, &mut scope_first, "version");
                write_json_str(out, &scope.version);
            }
            if !scope.attributes.is_empty() {
                write_object_field(out, &mut scope_first, "attributes");
                write_key_values_object(out, &scope.attributes);
            }
            if scope.dropped_attributes_count != 0 {
                write_object_field(out, &mut scope_first, "dropped_attributes_count");
                let _ = write!(out, "{}", scope.dropped_attributes_count);
            }
        }
        if !scope_logs.schema_url.is_empty() {
            write_object_field(out, &mut scope_first, "schema_url");
            write_json_str(out, &scope_logs.schema_url);
        }
        out.push('}');
    }
    out.push('}');
}

fn write_object_field(out: &mut String, first: &mut bool, name: &str) {
    if !*first {
        out.push(',');
    }
    *first = false;
    write_json_str(out, name);
    out.push(':');
}

fn write_key_values_object(out: &mut String, values: &[KeyValue<'_>]) {
    out.push('{');
    let mut first = true;
    for kv in values {
        let Some(value) = kv.value.as_ref() else {
            continue;
        };
        write_object_field(out, &mut first, &kv.key);
        write_any_value_json(out, value);
    }
    out.push('}');
}

fn any_value_sidecar_needed(
    record: &LogRecord<'_>,
    resource_attrs: &[KeyValue<'_>],
    scope_logs: &ScopeLogs<'_>,
) -> bool {
    record.body.as_ref().is_some_and(any_value_needs_sidecar)
        || resource_attrs.iter().any(key_value_needs_sidecar)
        || scope_logs
            .scope
            .as_ref()
            .is_some_and(|scope| scope.attributes.iter().any(key_value_needs_sidecar))
        || record.attributes.iter().any(key_value_needs_sidecar)
}

fn key_value_needs_sidecar(kv: &KeyValue<'_>) -> bool {
    kv.value.as_ref().is_none_or(any_value_needs_sidecar)
}

fn any_value_needs_sidecar(value: &AnyValue<'_>) -> bool {
    if value.string_value.is_some() || value.bool_value.is_some() || value.int_value.is_some() {
        false
    } else if let Some(value) = value.double_value {
        !value.is_finite()
    } else if value.bytes_value.is_some() {
        true
    } else if let Some(array) = &value.array_value {
        array.values.iter().any(any_value_needs_sidecar)
    } else if let Some(kvlist) = &value.kvlist_value {
        kvlist.values.iter().any(key_value_needs_sidecar)
            || kvlist.values.iter().enumerate().any(|(index, value)| {
                kvlist.values[..index]
                    .iter()
                    .any(|previous| previous.key == value.key)
            })
    } else {
        true
    }
}

/// Write only the values whose ordinary JSON compatibility form loses OTLP
/// type or presence. Attribute lists stay arrays so duplicate keys and a
/// present key without a value remain distinguishable.
fn write_any_value_sidecar(
    out: &mut String,
    record: &LogRecord<'_>,
    resource_attrs: &[KeyValue<'_>],
    scope_logs: &ScopeLogs<'_>,
) {
    out.push('{');
    let mut first = true;
    if let Some(body) = record
        .body
        .as_ref()
        .filter(|body| any_value_needs_sidecar(body))
    {
        write_object_field(out, &mut first, "body");
        write_typed_any_value(out, body);
    }
    write_key_value_sidecar_field(out, &mut first, "resource_attributes", resource_attrs);
    if let Some(scope) = &scope_logs.scope {
        write_key_value_sidecar_field(out, &mut first, "scope_attributes", &scope.attributes);
    }
    write_key_value_sidecar_field(out, &mut first, "record_attributes", &record.attributes);
    out.push('}');
}

fn write_key_value_sidecar_field(
    out: &mut String,
    first: &mut bool,
    name: &str,
    values: &[KeyValue<'_>],
) {
    if !values.iter().any(key_value_needs_sidecar) {
        return;
    }
    write_object_field(out, first, name);
    out.push('[');
    let mut entry_first = true;
    for kv in values.iter().filter(|kv| key_value_needs_sidecar(kv)) {
        if !entry_first {
            out.push(',');
        }
        entry_first = false;
        out.push('{');
        write_json_str(out, "key");
        out.push(':');
        write_json_str(out, &kv.key);
        out.push(',');
        write_json_str(out, "value");
        out.push(':');
        match &kv.value {
            Some(value) => write_typed_any_value(out, value),
            None => write_typed_value_kind(out, "missing"),
        }
        out.push('}');
    }
    out.push(']');
}

fn write_typed_any_value(out: &mut String, value: &AnyValue<'_>) {
    use std::fmt::Write as _;

    out.push('{');
    if let Some(value) = &value.string_value {
        write_typed_value_prefix(out, "string");
        write_json_str(out, value);
    } else if let Some(value) = value.bool_value {
        write_typed_value_prefix(out, "bool");
        out.push_str(if value { "true" } else { "false" });
    } else if let Some(value) = value.int_value {
        write_typed_value_prefix(out, "int");
        let _ = write!(out, "{value}");
    } else if let Some(value) = value.double_value {
        write_typed_value_prefix(out, "double");
        if value.is_finite() {
            let _ = write!(out, "{value}");
        } else {
            write_json_str(
                out,
                if value.is_nan() {
                    "NaN"
                } else if value.is_sign_positive() {
                    "Infinity"
                } else {
                    "-Infinity"
                },
            );
        }
    } else if let Some(bytes) = &value.bytes_value {
        write_json_str(out, "type");
        out.push(':');
        write_json_str(out, "bytes");
        match value.bytes_value_encoding {
            BytesValueEncoding::Hex => {
                out.push(',');
                write_json_str(out, "hex");
                out.push(':');
                write_json_str(out, bytes);
            }
            BytesValueEncoding::Base64 => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(bytes.as_bytes())
                    .or_else(|_| {
                        base64::engine::general_purpose::STANDARD_NO_PAD.decode(bytes.as_bytes())
                    });
                out.push(',');
                if let Ok(decoded) = decoded {
                    write_json_str(out, "hex");
                    out.push(':');
                    write_json_str(out, &hex_encode(&decoded));
                } else {
                    write_json_str(out, "base64");
                    out.push(':');
                    write_json_str(out, bytes);
                }
            }
        }
    } else if let Some(array) = &value.array_value {
        write_typed_value_prefix(out, "array");
        out.push('[');
        for (index, value) in array.values.iter().enumerate() {
            if index != 0 {
                out.push(',');
            }
            write_typed_any_value(out, value);
        }
        out.push(']');
    } else if let Some(kvlist) = &value.kvlist_value {
        write_typed_value_prefix(out, "kvlist");
        out.push('[');
        for (index, kv) in kvlist.values.iter().enumerate() {
            if index != 0 {
                out.push(',');
            }
            out.push('{');
            write_json_str(out, "key");
            out.push(':');
            write_json_str(out, &kv.key);
            out.push(',');
            write_json_str(out, "value");
            out.push(':');
            match &kv.value {
                Some(value) => write_typed_any_value(out, value),
                None => write_typed_value_kind(out, "missing"),
            }
            out.push('}');
        }
        out.push(']');
    } else {
        write_json_str(out, "type");
        out.push(':');
        write_json_str(out, "empty");
    }
    out.push('}');
}

fn write_typed_value_prefix(out: &mut String, kind: &str) {
    write_json_str(out, "type");
    out.push(':');
    write_json_str(out, kind);
    out.push(',');
    write_json_str(out, "value");
    out.push(':');
}

fn write_typed_value_kind(out: &mut String, kind: &str) {
    out.push('{');
    write_json_str(out, "type");
    out.push(':');
    write_json_str(out, kind);
    out.push('}');
}

fn write_ascii_lowercase_json_str(out: &mut String, value: &str) {
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        write_json_str(out, &value.to_ascii_lowercase());
    } else {
        write_json_str(out, value);
    }
}

/// Append a JSON-escaped, quoted string to `out` (RFC 8259 string escaping).
///
/// ASCII fast path: attribute keys and most string values are plain ASCII with
/// no characters that need escaping. Scan the bytes first; if none need quoting,
/// copy the whole string in one `push_str` (LLVM auto-vectorizes the scan).
fn write_json_str(out: &mut String, s: &str) {
    out.push('"');
    // Check whether any byte requires escaping: control chars (< 0x20), `"`, `\`.
    if s.bytes().all(|b| b >= 0x20 && b != b'"' && b != b'\\') {
        out.push_str(s);
    } else {
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '\u{08}' => out.push_str("\\b"),
                '\u{0c}' => out.push_str("\\f"),
                c if (c as u32) < 0x20 => {
                    use std::fmt::Write as _;
                    let _ = write!(out, "\\u{:04x}", c as u32);
                }
                c => out.push(c),
            }
        }
    }
    out.push('"');
}

/// Append one OTLP `AnyValue` to `out` as a JSON value, preserving native type.
fn write_any_value_json(out: &mut String, v: &AnyValue) {
    use std::fmt::Write as _;
    if let Some(s) = &v.string_value {
        write_json_str(out, s);
    } else if let Some(b) = v.bool_value {
        out.push_str(if b { "true" } else { "false" });
    } else if let Some(i) = v.int_value {
        let _ = write!(out, "{i}");
    } else if let Some(d) = v.double_value {
        if d.is_finite() {
            let _ = write!(out, "{d}");
        } else {
            out.push_str("null");
        }
    } else if let Some(s) = &v.bytes_value {
        write_json_str(out, s);
    } else if let Some(arr) = &v.array_value {
        out.push('[');
        for (i, item) in arr.values.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_any_value_json(out, item);
        }
        out.push(']');
    } else if let Some(kvlist) = &v.kvlist_value {
        out.push('{');
        let mut first = true;
        for kv in &kvlist.values {
            let Some(val) = kv.value.as_ref() else {
                continue;
            };
            if !first {
                out.push(',');
            }
            first = false;
            write_json_str(out, &kv.key);
            out.push(':');
            write_any_value_json(out, val);
        }
        out.push('}');
    } else {
        out.push_str("null");
    }
}

fn timestamp_for_record(record: &LogRecord) -> DateTime<Utc> {
    timestamp_from_unix_nanos(record.time_unix_nano)
        .or_else(|| timestamp_from_unix_nanos(record.observed_time_unix_nano))
        .unwrap_or_else(Utc::now)
}

fn timestamp_source(record: &LogRecord<'_>) -> &'static str {
    if timestamp_from_unix_nanos(record.time_unix_nano).is_some() {
        "time_unix_nano"
    } else if timestamp_from_unix_nanos(record.observed_time_unix_nano).is_some() {
        "observed_time_unix_nano"
    } else {
        "ingest_time"
    }
}

fn timestamp_from_unix_nanos(nanos: u64) -> Option<DateTime<Utc>> {
    if nanos == 0 {
        return None;
    }
    let secs = i64::try_from(nanos / 1_000_000_000).ok()?;
    let subsec_nanos = u32::try_from(nanos % 1_000_000_000).ok()?;
    Utc.timestamp_opt(secs, subsec_nanos).single()
}

fn body_to_raw<'a>(body: Option<&'a AnyValue<'a>>) -> Cow<'a, str> {
    match body.and_then(|value| value.string_value.as_deref()) {
        Some(value) => Cow::Borrowed(value),
        None => Cow::Owned(
            body.map(|value| serde_json::to_string(value).unwrap_or_default())
                .unwrap_or_default(),
        ),
    }
}

fn string_attr<'a>(attrs: &'a [KeyValue<'a>], key: &str) -> Option<&'a str> {
    attrs.iter().find_map(|attr| {
        if attr.key.as_ref() != key {
            return None;
        }
        attr.value.as_ref()?.string_value.as_deref()
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    out
}

// OTLP/JSON encodes u64/i64 as either a JSON number or a quoted string. A
// `#[serde(untagged)]` enum handles both but makes serde *buffer* the value
// (deserialize into an intermediate `Content`, then retry each variant) — once
// per `timeUnixNano` / `intValue`. A direct `Visitor` dispatches on the actual
// token with no buffering. (round-8 perf work)
fn deserialize_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    struct U64Visitor;
    impl serde::de::Visitor<'_> for U64Visitor {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a u64 or a numeric string")
        }
        fn visit_u64<E>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<u64, E> {
            u64::try_from(v).map_err(E::custom)
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<u64, E> {
            v.parse().map_err(E::custom)
        }
        fn visit_unit<E>(self) -> Result<u64, E> {
            Ok(0)
        }
    }
    deserializer.deserialize_any(U64Visitor)
}

fn deserialize_option_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct I64Visitor;
    impl serde::de::Visitor<'_> for I64Visitor {
        type Value = Option<i64>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an i64 or a numeric string")
        }
        fn visit_i64<E>(self, v: i64) -> Result<Option<i64>, E> {
            Ok(Some(v))
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Option<i64>, E> {
            i64::try_from(v).map(Some).map_err(E::custom)
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Option<i64>, E> {
            v.parse().map(Some).map_err(E::custom)
        }
        fn visit_unit<E>(self) -> Result<Option<i64>, E> {
            Ok(None)
        }
    }
    deserializer.deserialize_any(I64Visitor)
}

fn deserialize_option_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct F64Visitor;
    impl serde::de::Visitor<'_> for F64Visitor {
        type Value = Option<f64>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a number or one of NaN, Infinity and -Infinity")
        }
        fn visit_f64<E>(self, value: f64) -> Result<Option<f64>, E> {
            Ok(Some(value))
        }
        fn visit_i64<E>(self, value: i64) -> Result<Option<f64>, E> {
            Ok(Some(value as f64))
        }
        fn visit_u64<E>(self, value: u64) -> Result<Option<f64>, E> {
            Ok(Some(value as f64))
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Option<f64>, E> {
            match value {
                "NaN" => Ok(Some(f64::NAN)),
                "Infinity" => Ok(Some(f64::INFINITY)),
                "-Infinity" => Ok(Some(f64::NEG_INFINITY)),
                _ => Err(E::custom("invalid non-finite double")),
            }
        }
        fn visit_unit<E>(self) -> Result<Option<f64>, E> {
            Ok(None)
        }
    }
    deserializer.deserialize_any(F64Visitor)
}

fn deserialize_severity_number<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    struct SeverityVisitor;
    impl serde::de::Visitor<'_> for SeverityVisitor {
        type Value = i32;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an OTLP severity number or enum name")
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<i32, E> {
            i32::try_from(value).map_err(E::custom)
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<i32, E> {
            i32::try_from(value).map_err(E::custom)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<i32, E> {
            Ok(SeverityNumber::from_str_name(value)
                .map(|severity| severity as i32)
                .unwrap_or(SeverityNumber::Unspecified as i32))
        }
    }
    deserializer.deserialize_any(SeverityVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_resource_and_log_record_fields() {
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        string_kv("host.name", "web-01"),
                        string_kv("service.name", "checkout"),
                    ],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope-name".into(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_123_456_789,
                        body: Some(string_value("hello otlp")),
                        attributes: vec![
                            string_kv("sourcetype", "otel:app"),
                            string_kv("index", "prod"),
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let events = otlp_logs_to_events(req);

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.host, "web-01");
        assert_eq!(event.source, "checkout");
        assert_eq!(event.sourcetype, "otel:app");
        assert_eq!(event.index, "prod");
        assert_eq!(event.raw, "hello otlp");
        assert_eq!(event.timestamp.timestamp(), 1_700_000_000);
        assert_eq!(event.timestamp.timestamp_subsec_nanos(), 123_456_789);
        // Only core attributes present → no residual map.
        assert_eq!(event.attributes, None);
    }

    #[test]
    fn ws7_captures_residual_attributes_as_json() {
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![
                        string_kv("host.name", "web-01"),   // core → host
                        string_kv("k8s.namespace", "prod"), // residual
                    ],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        body: Some(string_value("b")),
                        attributes: vec![
                            string_kv("sourcetype", "otel:app"), // core
                            string_kv("http.method", "GET"),     // residual string
                            KeyValue {
                                key: "http.status_code".into(),
                                value: Some(AnyValue {
                                    int_value: Some(500),
                                    ..Default::default()
                                }),
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let events = otlp_logs_to_events(req);
        let attrs = events[0]
            .attributes
            .as_deref()
            .expect("residual attributes captured");
        let v: serde_json::Value = serde_json::from_str(attrs).unwrap();
        // Non-core attrs preserved with native types; core keys excluded.
        assert_eq!(v["k8s.namespace"], "prod");
        assert_eq!(v["http.method"], "GET");
        assert_eq!(v["http.status_code"], 500); // int, not string
        assert!(
            v.get("host.name").is_none(),
            "core key must not duplicate into residual"
        );
        assert!(v.get("sourcetype").is_none());
    }

    #[test]
    fn ws7_residual_json_is_valid_with_escaping_and_nested_types() {
        // Keys/values needing escaping + array + nested kvlist must round-trip
        // as valid JSON through the one-pass builder.
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![string_kv("host.name", "h")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        body: Some(string_value("b")),
                        attributes: vec![
                            string_kv("quote\"key", "tab\tand\nnewline"),
                            KeyValue {
                                key: "arr".into(),
                                value: Some(AnyValue {
                                    array_value: Some(ArrayValue {
                                        values: vec![
                                            AnyValue {
                                                int_value: Some(1),
                                                ..Default::default()
                                            },
                                            AnyValue {
                                                string_value: Some("x".into()),
                                                ..Default::default()
                                            },
                                        ],
                                    }),
                                    ..Default::default()
                                }),
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let events = otlp_logs_to_events(req);
        let attrs = events[0].attributes.as_deref().unwrap();
        // Must parse back as valid JSON with the escaped key + nested array intact.
        let v: serde_json::Value = serde_json::from_str(attrs).expect("valid JSON");
        assert_eq!(v["quote\"key"], "tab\tand\nnewline");
        assert_eq!(v["arr"][0], 1);
        assert_eq!(v["arr"][1], "x");
    }

    #[test]
    fn old_client_attributes_remain_byte_identical() {
        let json = r#"{"resourceLogs":[{"resource":{"attributes":[
          {"key":"host.name","value":{"stringValue":"h"}},
          {"key":"resource.only","value":{"stringValue":"resource"}}]},
          "scopeLogs":[{"scope":{"name":"old-scope"},"logRecords":[{
            "timeUnixNano":"1700000000000000000",
            "attributes":[{"key":"record.only","value":{"intValue":"7"}}]
          }]}]}]}"#;
        let mut buf = json.as_bytes().to_vec();
        let events = otlp_logs_to_events(parse_otlp_json(&mut buf).unwrap());

        assert_eq!(
            events[0].attributes.as_deref(),
            Some(r#"{"resource.only":"resource","record.only":7}"#)
        );
    }

    #[test]
    fn json_preserves_log_and_context_fidelity_under_otel() {
        let json = r#"{"resourceLogs":[{
          "schemaUrl":"https://opentelemetry.io/schemas/1.30.0",
          "resource":{"droppedAttributesCount":2,"attributes":[
            {"key":"host.name","value":{"stringValue":"json-host"}},
            {"key":"shared","value":{"intValue":"41"}}]},
          "scopeLogs":[{
            "schemaUrl":"https://example.test/scope/1.0",
            "scope":{"name":"json-scope","version":"2.1.0",
              "droppedAttributesCount":3,"attributes":[
                {"key":"scope.max","value":{"intValue":"9223372036854775807"}},
                {"key":"scope.array","value":{"arrayValue":{"values":[
                  {"boolValue":true},{"bytesValue":"AQI="}
                ]}}}]},
            "logRecords":[{
              "observedTimeUnixNano":"1700000000123456789",
              "severityNumber":"SEVERITY_NUMBER_INFO","severityText":"INFO",
              "flags":257,
              "traceId":"00112233445566778899AABBCCDDEEFF",
              "spanId":"8899AABBCCDDEEFF","eventName":"checkout.completed",
              "droppedAttributesCount":4,
              "attributes":[
                {"key":"shared","value":{"intValue":"42"}},
                {"key":"otel.trace_id","value":{"stringValue":"literal-id"}},
                {"key":"record.min","value":{"intValue":"-9223372036854775808"}}
              ]
            },{
              "timeUnixNano":"1700000001000000000","severityText":"WARN",
              "attributes":[{"key":"otel","value":{"kvlistValue":{"values":[
                {"key":"trace_id","value":{"stringValue":"user-wins"}}
              ]}}}]
            },{
              "timeUnixNano":"1700000002000000000",
              "observedTimeUnixNano":"18446744073709551615"
            }]
          }]
        }]}"#;
        let mut buf = json.as_bytes().to_vec();
        let events = otlp_logs_to_events(parse_otlp_json(&mut buf).unwrap());

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].timestamp.timestamp(), 1_700_000_000);
        assert_eq!(events[0].timestamp.timestamp_subsec_nanos(), 123_456_789);
        let first: serde_json::Value =
            serde_json::from_str(events[0].attributes.as_deref().unwrap()).unwrap();
        assert_eq!(first["shared"], 42);
        assert_eq!(first["otel.trace_id"], "literal-id");
        assert_eq!(first["record.min"], i64::MIN);
        assert_eq!(first["otel"]["severity_number"], 9);
        assert_eq!(first["otel"]["severity_text"], "INFO");
        assert_eq!(
            first["otel"]["trace_id"],
            "00112233445566778899aabbccddeeff"
        );
        assert_eq!(first["otel"]["span_id"], "8899aabbccddeeff");
        assert_eq!(first["otel"]["flags"], 257);
        assert_eq!(
            first["otel"]["observed_time_unix_nano"].as_u64(),
            Some(1_700_000_000_123_456_789)
        );
        assert_eq!(first["otel"]["timestamp_source"], "observed_time_unix_nano");
        assert_eq!(first["otel"]["resource"]["attributes"]["shared"], 41);
        assert_eq!(first["otel"]["scope"]["name"], "json-scope");
        assert_eq!(first["otel"]["scope"]["version"], "2.1.0");
        assert_eq!(first["otel"]["scope"]["attributes"]["scope.max"], i64::MAX);
        assert_eq!(
            first["otel"]["scope"]["attributes"]["scope.array"],
            serde_json::json!([true, "AQI="])
        );

        let second: serde_json::Value =
            serde_json::from_str(events[1].attributes.as_deref().unwrap()).unwrap();
        assert_eq!(second["otel"]["trace_id"], "user-wins");
        assert!(second["otel"].get("severity_text").is_none());

        let third: serde_json::Value =
            serde_json::from_str(events[2].attributes.as_deref().unwrap()).unwrap();
        assert_eq!(
            third["otel"]["observed_time_unix_nano"].as_u64(),
            Some(u64::MAX)
        );
        assert_eq!(third["otel"]["timestamp_source"], "time_unix_nano");
    }

    #[test]
    fn json_keeps_existing_integer_and_timestamp_rejections() {
        let above_i64 = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{
          "attributes":[{"key":"too-large","value":{"intValue":"9223372036854775808"}}]
        }]}]}]}"#;
        let mut above_i64 = above_i64.as_bytes().to_vec();
        assert!(parse_otlp_json(&mut above_i64).is_err());

        let negative_time = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{
          "timeUnixNano":"-1"
        }]}]}]}"#;
        let mut negative_time = negative_time.as_bytes().to_vec();
        assert!(parse_otlp_json(&mut negative_time).is_err());

        let beyond_arrow_ns = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{
          "timeUnixNano":"10000000000000000000"
        }]}]}]}"#;
        let mut beyond_arrow_ns = beyond_arrow_ns.as_bytes().to_vec();
        let request = parse_otlp_json(&mut beyond_arrow_ns).unwrap();
        assert!(otlp_logs_to_record_batch(request).is_err());
    }

    #[test]
    fn any_value_sidecar_preserves_lossy_json_and_protobuf_values() {
        use opentelemetry_proto::tonic::common::v1::{
            any_value::Value as PValue, AnyValue as PAnyValue, ArrayValue as PArrayValue,
            InstrumentationScope as PInstrumentationScope, KeyValue as PKeyValue,
            KeyValueList as PKeyValueList,
        };
        use opentelemetry_proto::tonic::logs::v1::{
            LogRecord as PLogRecord, ResourceLogs as PResourceLogs, ScopeLogs as PScopeLogs,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource as PResource;

        fn pvalue(value: PValue) -> Option<PAnyValue> {
            Some(PAnyValue { value: Some(value) })
        }

        let json = r#"{"resourceLogs":[{"resource":{"attributes":[
          {"key":"missing-value"},
          {"key":"empty","value":{}},
          {"key":"absent-control","value":{"stringValue":"present"}}
        ]},"scopeLogs":[{"scope":{"attributes":[
          {"key":"nested","value":{"kvlistValue":{"values":[
            {"key":"items","value":{"arrayValue":{"values":[
              {"bytesValue":"AQI="},{},{"doubleValue":"NaN"}
            ]}}},
            {"key":"missing-nested"}
          ]}}}
        ]},"logRecords":[{"attributes":[
          {"key":"string","value":{"stringValue":"dead"}},
          {"key":"bytes","value":{"bytesValue":"3q0="}},
          {"key":"positive-infinity","value":{"doubleValue":"Infinity"}},
          {"key":"negative-infinity","value":{"doubleValue":"-Infinity"}}
        ]}]}]}]}"#;
        let mut json_buf = json.as_bytes().to_vec();
        let json_events = otlp_logs_to_events(parse_otlp_json(&mut json_buf).unwrap());
        let json_attrs: serde_json::Value =
            serde_json::from_str(json_events[0].attributes.as_deref().unwrap()).unwrap();

        let proto = ProtoExportLogsServiceRequest {
            resource_logs: vec![PResourceLogs {
                resource: Some(PResource {
                    attributes: vec![
                        PKeyValue {
                            key: "missing-value".into(),
                            value: None,
                            ..Default::default()
                        },
                        PKeyValue {
                            key: "empty".into(),
                            value: Some(PAnyValue { value: None }),
                            ..Default::default()
                        },
                        PKeyValue {
                            key: "absent-control".into(),
                            value: pvalue(PValue::StringValue("present".into())),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }),
                scope_logs: vec![PScopeLogs {
                    scope: Some(PInstrumentationScope {
                        attributes: vec![PKeyValue {
                            key: "nested".into(),
                            value: pvalue(PValue::KvlistValue(PKeyValueList {
                                values: vec![
                                    PKeyValue {
                                        key: "items".into(),
                                        value: pvalue(PValue::ArrayValue(PArrayValue {
                                            values: vec![
                                                PAnyValue {
                                                    value: Some(PValue::BytesValue(vec![1, 2])),
                                                },
                                                PAnyValue { value: None },
                                                PAnyValue {
                                                    value: Some(PValue::DoubleValue(f64::NAN)),
                                                },
                                            ],
                                        })),
                                        ..Default::default()
                                    },
                                    PKeyValue {
                                        key: "missing-nested".into(),
                                        value: None,
                                        ..Default::default()
                                    },
                                ],
                            })),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    log_records: vec![PLogRecord {
                        attributes: vec![
                            PKeyValue {
                                key: "string".into(),
                                value: pvalue(PValue::StringValue("dead".into())),
                                ..Default::default()
                            },
                            PKeyValue {
                                key: "bytes".into(),
                                value: pvalue(PValue::BytesValue(vec![0xde, 0xad])),
                                ..Default::default()
                            },
                            PKeyValue {
                                key: "positive-infinity".into(),
                                value: pvalue(PValue::DoubleValue(f64::INFINITY)),
                                ..Default::default()
                            },
                            PKeyValue {
                                key: "negative-infinity".into(),
                                value: pvalue(PValue::DoubleValue(f64::NEG_INFINITY)),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let proto_events = otlp_logs_to_events(
            decode_export_logs_service_request_protobuf(&proto.encode_to_vec()).unwrap(),
        );
        let proto_attrs: serde_json::Value =
            serde_json::from_str(proto_events[0].attributes.as_deref().unwrap()).unwrap();

        assert_eq!(proto_attrs["string"], "dead");
        assert_eq!(proto_attrs["bytes"], "dead");
        assert_eq!(proto_attrs["positive-infinity"], serde_json::Value::Null);
        assert!(proto_attrs.get("missing-value").is_none());
        assert_eq!(proto_attrs["empty"], serde_json::Value::Null);
        assert!(proto_attrs.get("absent").is_none());

        let json_sidecar = &json_attrs["otel"]["any_values"];
        let proto_sidecar = &proto_attrs["otel"]["any_values"];
        assert_eq!(json_sidecar, proto_sidecar);
        assert_eq!(
            proto_sidecar["resource_attributes"],
            serde_json::json!([
                {"key":"missing-value","value":{"type":"missing"}},
                {"key":"empty","value":{"type":"empty"}}
            ])
        );
        assert_eq!(
            proto_sidecar["record_attributes"],
            serde_json::json!([
                {"key":"bytes","value":{"type":"bytes","hex":"dead"}},
                {"key":"positive-infinity","value":{"type":"double","value":"Infinity"}},
                {"key":"negative-infinity","value":{"type":"double","value":"-Infinity"}}
            ])
        );
        assert_eq!(
            proto_sidecar["scope_attributes"][0]["value"]["value"][0]["value"]["value"][0],
            serde_json::json!({"type":"bytes","hex":"0102"})
        );
        assert_eq!(
            proto_sidecar["scope_attributes"][0]["value"]["value"][0]["value"]["value"][1],
            serde_json::json!({"type":"empty"})
        );
        assert_eq!(
            proto_sidecar["scope_attributes"][0]["value"]["value"][0]["value"]["value"][2],
            serde_json::json!({"type":"double","value":"NaN"})
        );
        assert_eq!(
            proto_sidecar["scope_attributes"][0]["value"]["value"][1],
            serde_json::json!({"key":"missing-nested","value":{"type":"missing"}})
        );
    }

    #[test]
    fn uses_fallbacks_when_attrs_and_time_are_missing() {
        let before = Utc::now();
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let events = otlp_logs_to_events(req);
        let after = Utc::now();

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.host, "unknown");
        assert_eq!(event.source, "otel");
        assert_eq!(event.sourcetype, "otel:logs");
        assert_eq!(event.index, "main");
        assert!(event.timestamp >= before);
        assert!(event.timestamp <= after);
    }

    #[test]
    fn decodes_protobuf_via_opentelemetry_proto() {
        use opentelemetry_proto::tonic::common::v1::{
            any_value::Value as PValue, AnyValue as PAnyValue,
            InstrumentationScope as PInstrumentationScope, KeyValue as PKeyValue,
        };
        use opentelemetry_proto::tonic::logs::v1::{
            LogRecord as PLogRecord, ResourceLogs as PResourceLogs, ScopeLogs as PScopeLogs,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource as PResource;

        fn pstr(key: &str, value: &str) -> PKeyValue {
            PKeyValue {
                key: key.into(),
                value: Some(PAnyValue {
                    value: Some(PValue::StringValue(value.into())),
                }),
                ..Default::default()
            }
        }

        let proto = ProtoExportLogsServiceRequest {
            resource_logs: vec![PResourceLogs {
                resource: Some(PResource {
                    attributes: vec![
                        pstr("host.name", "pb-host"),
                        pstr("service.name", "billing"),
                    ],
                    dropped_attributes_count: 2,
                    ..Default::default()
                }),
                scope_logs: vec![PScopeLogs {
                    scope: Some(PInstrumentationScope {
                        name: "protobuf-scope".into(),
                        version: "3.0.0".into(),
                        attributes: vec![PKeyValue {
                            key: "scope.bytes".into(),
                            value: Some(PAnyValue {
                                value: Some(PValue::BytesValue(vec![0xab, 0xcd])),
                            }),
                            ..Default::default()
                        }],
                        dropped_attributes_count: 3,
                    }),
                    log_records: vec![PLogRecord {
                        time_unix_nano: 1_700_000_000_123_456_789,
                        observed_time_unix_nano: 1_700_000_000_123_456_999,
                        severity_number: SeverityNumber::Error as i32,
                        severity_text: "ERROR".into(),
                        body: Some(PAnyValue {
                            value: Some(PValue::StringValue("pb body".into())),
                        }),
                        attributes: vec![pstr("sourcetype", "otel:pb")],
                        dropped_attributes_count: 4,
                        flags: 1,
                        trace_id: vec![0x11; 16],
                        span_id: vec![0x22; 8],
                        event_name: "payment.failed".into(),
                    }],
                    schema_url: "https://example.test/scope/3.0".into(),
                }],
                schema_url: "https://opentelemetry.io/schemas/1.30.0".into(),
            }],
        };

        let bytes = proto.encode_to_vec();
        let decoded = decode_export_logs_service_request_protobuf(&bytes).unwrap();
        let events = otlp_logs_to_events(decoded);

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.host, "pb-host");
        assert_eq!(event.source, "billing");
        assert_eq!(event.sourcetype, "otel:pb");
        assert_eq!(event.raw, "pb body");
        assert_eq!(event.timestamp.timestamp(), 1_700_000_000);
        let attrs: serde_json::Value =
            serde_json::from_str(event.attributes.as_deref().unwrap()).unwrap();
        assert_eq!(attrs["otel"]["severity_number"], 17);
        assert_eq!(attrs["otel"]["severity_text"], "ERROR");
        assert_eq!(
            attrs["otel"]["trace_id"],
            "11111111111111111111111111111111"
        );
        assert_eq!(attrs["otel"]["span_id"], "2222222222222222");
        assert_eq!(attrs["otel"]["flags"], 1);
        assert_eq!(attrs["otel"]["event_name"], "payment.failed");
        assert_eq!(attrs["otel"]["dropped_attributes_count"], 4);
        assert_eq!(attrs["otel"]["resource"]["dropped_attributes_count"], 2);
        assert_eq!(attrs["otel"]["scope"]["version"], "3.0.0");
        assert_eq!(attrs["otel"]["scope"]["attributes"]["scope.bytes"], "abcd");
    }

    #[test]
    fn batch_path_matches_event_path() {
        // The direct-to-RecordBatch hot path must produce byte-identical
        // output to the owned-Event detour it replaced, across string/int
        // bodies, residual attrs, escaping, and core-only records.
        let json = r#"{"resourceLogs":[
          {"resource":{"attributes":[
             {"key":"host.name","value":{"stringValue":"web-01"}},
             {"key":"service.name","value":{"stringValue":"checkout"}},
             {"key":"k8s.namespace","value":{"stringValue":"prod"}}]},
           "scopeLogs":[{"scope":{"name":"scope-a"},"logRecords":[
             {"timeUnixNano":"1700000000000000001",
              "body":{"stringValue":"plain \"escaped\" body"},
              "attributes":[
                {"key":"sourcetype","value":{"stringValue":"nginx"}},
                {"key":"http.status_code","value":{"intValue":500}}]},
             {"timeUnixNano":"1700000000000000002",
              "body":{"intValue":42},
              "attributes":[{"key":"index","value":{"stringValue":"prod"}}]}]}]},
          {"resource":null,
           "scopeLogs":[{"logRecords":[
             {"timeUnixNano":"1700000000000000003"}]}]}
        ]}"#;
        let mut buf_a = json.as_bytes().to_vec();
        let mut buf_b = buf_a.clone();
        let via_events = siglake_core::events_to_record_batch(&otlp_logs_to_events(
            parse_otlp_json(&mut buf_a).unwrap(),
        ))
        .unwrap();
        let direct = otlp_logs_to_record_batch(parse_otlp_json(&mut buf_b).unwrap()).unwrap();
        assert_eq!(via_events, direct);
        assert_eq!(direct.num_rows(), 3);
    }

    #[test]
    fn json_parse_unescapes_borrowed_strings() {
        // Exercises the real JSON path: simd-json unescapes in place + the Cow
        // fields borrow the unescaped slices. Escaped quote/newline/tab in both
        // the body and a residual attribute must come through verbatim.
        let json = r#"{"resourceLogs":[{"resource":{"attributes":[
            {"key":"host.name","value":{"stringValue":"h"}}]},
          "scopeLogs":[{"logRecords":[{
            "body":{"stringValue":"line with \"quote\" and \n newline"},
            "attributes":[{"key":"path","value":{"stringValue":"/a/b\tc"}}]}]}]}]}"#;
        let mut buf = json.as_bytes().to_vec();
        let req = parse_otlp_json(&mut buf).unwrap();
        let events = otlp_logs_to_events(req);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].raw, "line with \"quote\" and \n newline");
        let attrs: serde_json::Value =
            serde_json::from_str(events[0].attributes.as_deref().unwrap()).unwrap();
        assert_eq!(attrs["path"], "/a/b\tc");
    }

    fn string_kv<'a>(key: &'a str, value: &'a str) -> KeyValue<'a> {
        KeyValue {
            key: key.into(),
            value: Some(string_value(value)),
        }
    }

    fn string_value(value: &str) -> AnyValue<'_> {
        AnyValue {
            string_value: Some(value.into()),
            ..Default::default()
        }
    }
}
