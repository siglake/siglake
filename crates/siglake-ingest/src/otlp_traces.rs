use chrono::{DateTime, TimeZone, Utc};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess as ProtoExportTracePartialSuccess,
    ExportTraceServiceRequest as ProtoExportTraceServiceRequest,
    ExportTraceServiceResponse as ProtoExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{
    any_value::Value as ProtoValue, AnyValue as ProtoAnyValue, KeyValue as ProtoKeyValue,
};
use opentelemetry_proto::tonic::trace::v1::{
    span::{Event as ProtoSpanEvent, Link as ProtoSpanLink, SpanKind},
    status::StatusCode,
    Span as ProtoSpan,
};
use prost::Message;
use serde_json::{Map, Value};
use siglake_core::Event;

pub type ExportTraceServiceRequest = ProtoExportTraceServiceRequest;
pub type ExportTraceServiceResponse = ProtoExportTraceServiceResponse;
pub type ExportTracePartialSuccess = ProtoExportTracePartialSuccess;

pub fn parse_otlp_json(buf: &mut [u8]) -> Result<ExportTraceServiceRequest, String> {
    let mut value: Value = simd_json::serde::from_slice(buf).map_err(|e| e.to_string())?;
    normalize_trace_json_enums(&mut value);
    serde_json::from_value(value).map_err(|e| e.to_string())
}

pub fn decode_export_trace_service_request_protobuf(
    bytes: &[u8],
) -> Result<ExportTraceServiceRequest, String> {
    ProtoExportTraceServiceRequest::decode(bytes).map_err(|e| format!("protobuf decode error: {e}"))
}

pub fn otlp_proto_traces_to_events(proto: ProtoExportTraceServiceRequest) -> Vec<Event> {
    otlp_traces_to_events(proto)
}

pub fn export_trace_service_success_response(accepted_events: usize) -> ExportTraceServiceResponse {
    let partial_success = (accepted_events == 0).then(|| ExportTracePartialSuccess {
        rejected_spans: 0,
        error_message: "request contained no spans".to_string(),
    });
    ExportTraceServiceResponse { partial_success }
}

pub fn encode_export_trace_service_response_protobuf(
    response: &ExportTraceServiceResponse,
) -> Vec<u8> {
    response.encode_to_vec()
}

pub fn otlp_traces_to_events(req: ExportTraceServiceRequest) -> Vec<Event> {
    let mut events = Vec::new();
    for resource_spans in req.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .as_ref()
            .map(|resource| resource.attributes.as_slice())
            .unwrap_or(&[]);
        let host = string_attr(resource_attrs, "host.name").unwrap_or("unknown");
        let service = string_attr(resource_attrs, "service.name").unwrap_or("unknown");
        for scope_spans in resource_spans.scope_spans {
            for span in scope_spans.spans {
                events.push(span_to_event(&span, resource_attrs, host, service));
            }
        }
    }
    events
}

fn span_to_event(
    span: &ProtoSpan,
    resource_attrs: &[ProtoKeyValue],
    host: &str,
    service: &str,
) -> Event {
    let mut attrs = Map::new();
    for attr in &span.attributes {
        if let Some(value) = attr.value.as_ref() {
            attrs.insert(attr.key.clone(), proto_any_value_to_json(value));
        }
    }
    for attr in resource_attrs {
        if let Some(value) = attr.value.as_ref() {
            attrs.insert(
                format!("resource.{}", attr.key),
                proto_any_value_to_json(value),
            );
        }
    }
    if !span.events.is_empty() {
        attrs.insert(
            "events".to_string(),
            Value::Array(span.events.iter().map(span_event_to_json).collect()),
        );
    }
    if !span.links.is_empty() {
        attrs.insert(
            "links".to_string(),
            Value::Array(span.links.iter().map(span_link_to_json).collect()),
        );
    }

    let duration_nanos = span
        .end_time_unix_nano
        .saturating_sub(span.start_time_unix_nano)
        .min(i64::MAX as u64) as i64;
    attrs.insert(
        "trace_id".to_string(),
        Value::String(hex_encode(&span.trace_id)),
    );
    attrs.insert(
        "span_id".to_string(),
        Value::String(hex_encode(&span.span_id)),
    );
    attrs.insert(
        "parent_span_id".to_string(),
        Value::String(hex_encode(&span.parent_span_id)),
    );
    attrs.insert(
        "kind".to_string(),
        Value::String(span_kind_name(span.kind).to_string()),
    );
    attrs.insert(
        "status_code".to_string(),
        Value::String(status_code_name(span.status.as_ref().map(|status| status.code)).to_string()),
    );
    attrs.insert(
        "duration_nanos".to_string(),
        Value::Number(duration_nanos.into()),
    );
    attrs.insert("service".to_string(), Value::String(service.to_string()));
    attrs.insert("name".to_string(), Value::String(span.name.clone()));

    Event {
        timestamp: timestamp_from_unix_nanos(span.start_time_unix_nano).unwrap_or_else(Utc::now),
        host: host.to_string(),
        source: service.to_string(),
        sourcetype: "otel:span".to_string(),
        index: "main".to_string(),
        raw: span.name.clone(),
        attributes: Some(Value::Object(attrs).to_string()),
    }
}

fn proto_any_value_to_json(value: &ProtoAnyValue) -> Value {
    match value.value.as_ref() {
        Some(ProtoValue::StringValue(value)) => Value::String(value.clone()),
        Some(ProtoValue::BoolValue(value)) => Value::Bool(*value),
        Some(ProtoValue::IntValue(value)) => Value::Number((*value).into()),
        Some(ProtoValue::DoubleValue(value)) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Some(ProtoValue::ArrayValue(value)) => {
            Value::Array(value.values.iter().map(proto_any_value_to_json).collect())
        }
        Some(ProtoValue::KvlistValue(value)) => {
            let mut map = Map::new();
            for kv in &value.values {
                if let Some(value) = kv.value.as_ref() {
                    map.insert(kv.key.clone(), proto_any_value_to_json(value));
                }
            }
            Value::Object(map)
        }
        Some(ProtoValue::BytesValue(value)) => Value::String(hex_encode(value)),
        Some(ProtoValue::StringValueStrindex(_)) | None => Value::Null,
    }
}

fn span_event_to_json(event: &ProtoSpanEvent) -> Value {
    serde_json::to_value(event).unwrap_or(Value::Null)
}

fn span_link_to_json(link: &ProtoSpanLink) -> Value {
    serde_json::to_value(link).unwrap_or(Value::Null)
}

fn normalize_trace_json_enums(value: &mut Value) {
    let Some(resource_spans) = value.get_mut("resourceSpans").and_then(Value::as_array_mut) else {
        return;
    };
    for resource_spans in resource_spans {
        let Some(scope_spans) = resource_spans
            .get_mut("scopeSpans")
            .and_then(Value::as_array_mut)
        else {
            continue;
        };
        for scope_spans in scope_spans {
            let Some(spans) = scope_spans.get_mut("spans").and_then(Value::as_array_mut) else {
                continue;
            };
            for span in spans {
                let Some(span_obj) = span.as_object_mut() else {
                    continue;
                };
                if let Some(kind) = span_obj.get_mut("kind") {
                    normalize_span_kind(kind);
                }
                if let Some(status) = span_obj.get_mut("status").and_then(Value::as_object_mut) {
                    if let Some(code) = status.get_mut("code") {
                        normalize_status_code(code);
                    }
                }
            }
        }
    }
}

fn normalize_span_kind(value: &mut Value) {
    let Some(kind) = value.as_str() else {
        return;
    };
    let kind = SpanKind::from_str_name(kind)
        .map(|kind| kind as i32)
        .unwrap_or(SpanKind::Unspecified as i32);
    *value = Value::Number(kind.into());
}

fn normalize_status_code(value: &mut Value) {
    let Some(code) = value.as_str() else {
        return;
    };
    let code = StatusCode::from_str_name(code)
        .map(|code| code as i32)
        .unwrap_or(StatusCode::Unset as i32);
    *value = Value::Number(code.into());
}

fn string_attr<'a>(attrs: &'a [ProtoKeyValue], key: &str) -> Option<&'a str> {
    attrs.iter().find_map(|attr| {
        if attr.key != key {
            return None;
        }
        match attr.value.as_ref()?.value.as_ref()? {
            ProtoValue::StringValue(value) => Some(value.as_str()),
            _ => None,
        }
    })
}

fn span_kind_name(kind: i32) -> &'static str {
    SpanKind::try_from(kind)
        .ok()
        .map(|kind| kind.as_str_name())
        .unwrap_or(SpanKind::Unspecified.as_str_name())
}

fn status_code_name(code: Option<i32>) -> &'static str {
    code.and_then(|code| StatusCode::try_from(code).ok())
        .map(|code| code.as_str_name())
        .unwrap_or(StatusCode::Unset.as_str_name())
}

fn timestamp_from_unix_nanos(nanos: u64) -> Option<DateTime<Utc>> {
    if nanos == 0 {
        return None;
    }
    let secs = i64::try_from(nanos / 1_000_000_000).ok()?;
    let subsec_nanos = u32::try_from(nanos % 1_000_000_000).ok()?;
    Utc.timestamp_opt(secs, subsec_nanos).single()
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

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{
        any_value::Value as ProtoValue, AnyValue as ProtoAnyValue, KeyValue as ProtoKeyValue,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource as ProtoResource;
    use opentelemetry_proto::tonic::trace::v1::{
        span, ResourceSpans as ProtoResourceSpans, ScopeSpans as ProtoScopeSpans, Status,
    };

    #[test]
    fn converts_trace_ids_duration_and_resource_prefixes() {
        let events = otlp_traces_to_events(ProtoExportTraceServiceRequest {
            resource_spans: vec![ProtoResourceSpans {
                resource: Some(ProtoResource {
                    attributes: vec![
                        proto_str("host.name", "api-1"),
                        proto_str("service.name", "checkout"),
                        proto_str("cloud.region", "us-east-1"),
                    ],
                    ..Default::default()
                }),
                scope_spans: vec![ProtoScopeSpans {
                    spans: vec![ProtoSpan {
                        trace_id: vec![0xab; 16],
                        span_id: vec![0xcd; 8],
                        parent_span_id: vec![0xef; 8],
                        name: "GET /cart".to_string(),
                        kind: SpanKind::Server as i32,
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_000_123_456,
                        attributes: vec![proto_str("http.method", "GET")],
                        status: Some(Status {
                            code: StatusCode::Ok as i32,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.host, "api-1");
        assert_eq!(event.source, "checkout");
        assert_eq!(event.sourcetype, "otel:span");
        assert_eq!(event.raw, "GET /cart");
        let attrs: Value = serde_json::from_str(event.attributes.as_deref().unwrap()).unwrap();
        assert_eq!(attrs["trace_id"], "abababababababababababababababab");
        assert_eq!(attrs["span_id"], "cdcdcdcdcdcdcdcd");
        assert_eq!(attrs["parent_span_id"], "efefefefefefefef");
        assert_eq!(attrs["kind"], "SPAN_KIND_SERVER");
        assert_eq!(attrs["status_code"], "STATUS_CODE_OK");
        assert_eq!(attrs["duration_nanos"], 123456);
        assert_eq!(attrs["service"], "checkout");
        assert_eq!(attrs["name"], "GET /cart");
        assert_eq!(attrs["http.method"], "GET");
        assert_eq!(attrs["resource.cloud.region"], "us-east-1");
        assert_eq!(attrs["resource.service.name"], "checkout");
    }

    #[test]
    fn clamps_negative_duration_and_preserves_events_and_links() {
        let events = otlp_traces_to_events(ProtoExportTraceServiceRequest {
            resource_spans: vec![ProtoResourceSpans {
                scope_spans: vec![ProtoScopeSpans {
                    spans: vec![ProtoSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "span".to_string(),
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 9,
                        events: vec![span::Event {
                            time_unix_nano: 11,
                            name: "exception".to_string(),
                            attributes: vec![proto_str("exception.type", "IOError")],
                            ..Default::default()
                        }],
                        links: vec![span::Link {
                            trace_id: vec![3; 16],
                            span_id: vec![4; 8],
                            attributes: vec![proto_str("link.kind", "follows")],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let attrs: Value = serde_json::from_str(events[0].attributes.as_deref().unwrap()).unwrap();
        assert_eq!(attrs["duration_nanos"], 0);
        assert_eq!(attrs["events"][0]["name"], "exception");
        assert_eq!(attrs["events"][0]["attributes"][0]["key"], "exception.type");
        assert_eq!(
            attrs["links"][0]["traceId"],
            "03030303030303030303030303030303"
        );
        assert_eq!(attrs["links"][0]["attributes"][0]["key"], "link.kind");
    }

    #[test]
    fn json_and_protobuf_decode_round_trip() {
        let json = serde_json::json!({
            "resourceSpans": [{
                "resource": { "attributes": [
                    { "key": "host.name", "value": { "stringValue": "json-host" } },
                    { "key": "service.name", "value": { "stringValue": "json-svc" } }
                ]},
                "scopeSpans": [{
                    "spans": [{
                        "traceId": "00112233445566778899aabbccddeeff",
                        "spanId": "8899aabbccddeeff",
                        "parentSpanId": "0011223344556677",
                        "name": "json-span",
                        "kind": "SPAN_KIND_CLIENT",
                        "startTimeUnixNano": "1700000000000000000",
                        "endTimeUnixNano": "1700000000000001000",
                        "status": { "code": "STATUS_CODE_ERROR" }
                    }]
                }]
            }]
        });

        let mut buf = serde_json::to_vec(&json).unwrap();
        let request = parse_otlp_json(&mut buf).unwrap();
        let events = otlp_traces_to_events(request.clone());
        assert_eq!(events[0].source, "json-svc");

        let bytes = request.encode_to_vec();
        let decoded = decode_export_trace_service_request_protobuf(&bytes).unwrap();
        let events = otlp_traces_to_events(decoded);
        let attrs: Value = serde_json::from_str(events[0].attributes.as_deref().unwrap()).unwrap();
        assert_eq!(attrs["trace_id"], "00112233445566778899aabbccddeeff");
        assert_eq!(attrs["kind"], "SPAN_KIND_CLIENT");
        assert_eq!(attrs["status_code"], "STATUS_CODE_ERROR");
    }

    fn proto_str(key: &str, value: &str) -> ProtoKeyValue {
        ProtoKeyValue {
            key: key.to_string(),
            value: Some(ProtoAnyValue {
                value: Some(ProtoValue::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }
}
