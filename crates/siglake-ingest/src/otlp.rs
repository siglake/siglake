use arrow_array::RecordBatch;
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
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource<'a> {
    #[serde(default, borrow)]
    pub attributes: Vec<KeyValue<'a>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeLogs<'a> {
    #[serde(default, borrow)]
    pub scope: Option<InstrumentationScope<'a>>,
    #[serde(default, borrow)]
    pub log_records: Vec<LogRecord<'a>>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentationScope<'a> {
    #[serde(default, borrow)]
    pub name: Cow<'a, str>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogRecord<'a> {
    #[serde(default, deserialize_with = "deserialize_u64")]
    pub time_unix_nano: u64,
    #[serde(default, deserialize_with = "deserialize_u64")]
    pub observed_time_unix_nano: u64,
    #[serde(default, borrow)]
    pub body: Option<AnyValue<'a>>,
    #[serde(default, borrow)]
    pub attributes: Vec<KeyValue<'a>>,
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
    #[serde(default)]
    pub double_value: Option<f64>,
    #[serde(default, borrow)]
    pub array_value: Option<ArrayValue<'a>>,
    #[serde(default, borrow)]
    pub kvlist_value: Option<KeyValueList<'a>>,
    #[serde(default, borrow)]
    pub bytes_value: Option<Cow<'a, str>>,
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
    }
}

fn convert_resource(proto: ProtoResource) -> Resource<'static> {
    Resource {
        attributes: proto
            .attributes
            .into_iter()
            .map(convert_key_value)
            .collect(),
    }
}

fn convert_scope_logs(proto: ProtoScopeLogs) -> ScopeLogs<'static> {
    ScopeLogs {
        scope: proto.scope.map(|scope| InstrumentationScope {
            name: Cow::Owned(scope.name),
        }),
        log_records: proto
            .log_records
            .into_iter()
            .map(convert_log_record)
            .collect(),
    }
}

fn convert_log_record(proto: ProtoLogRecord) -> LogRecord<'static> {
    LogRecord {
        time_unix_nano: proto.time_unix_nano,
        observed_time_unix_nano: proto.observed_time_unix_nano,
        body: proto.body.map(convert_any_value),
        attributes: proto
            .attributes
            .into_iter()
            .map(convert_key_value)
            .collect(),
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
            out.bytes_value = Some(Cow::Owned(hex_encode(&value)))
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
                let (sourcetype, index, attributes) =
                    map_record_attributes(&record.attributes, &resource_fragment);
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
    record_attrs: &'a [KeyValue<'a>],
    resource_fragment: &str,
) -> (Option<&'a str>, Option<&'a str>, Option<String>) {
    let mut sourcetype = None;
    let mut index = None;
    // Defer the String alloc: initialize with the resource fragment when it
    // is non-empty (always has content), otherwise leave it None until the
    // first residual record attr triggers the allocation.
    let record_residual_cap = 1 + resource_fragment.len() + record_attrs.len() * 32;
    let mut out: Option<String> = if resource_fragment.is_empty() {
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
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope-name".into(),
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
                }],
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
                }],
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
                }],
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
            any_value::Value as PValue, AnyValue as PAnyValue, KeyValue as PKeyValue,
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
                    ..Default::default()
                }),
                scope_logs: vec![PScopeLogs {
                    log_records: vec![PLogRecord {
                        time_unix_nano: 1_700_000_000_123_456_789,
                        body: Some(PAnyValue {
                            value: Some(PValue::StringValue("pb body".into())),
                        }),
                        attributes: vec![pstr("sourcetype", "otel:pb")],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
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
