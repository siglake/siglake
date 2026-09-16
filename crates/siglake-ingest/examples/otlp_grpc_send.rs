//! Send a small OTLP logs batch over **gRPC** to a running ingester —
//! the live-socket validation tool for the gRPC transport (the unit
//! tests drive the service trait; this exercises a real tonic channel).
//!
//! Usage:
//!   cargo run -p siglake-ingest --example otlp_grpc_send -- \
//!     http://127.0.0.1:4317 [tenant] [index] [n]
//!
//! Prints the Export response. Exits non-zero on transport or RPC error.

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let endpoint = args
        .next()
        .unwrap_or_else(|| "http://127.0.0.1:4317".to_string());
    let tenant = args.next().unwrap_or_else(|| "default".to_string());
    let index = args.next().unwrap_or_else(|| "events".to_string());
    let n: usize = args.next().map(|s| s.parse()).transpose()?.unwrap_or(10);

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos() as u64;

    let str_attr = |key: &str, value: &str| KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
        ..Default::default()
    };

    let records: Vec<LogRecord> = (0..n)
        .map(|i| LogRecord {
            time_unix_nano: now_ns + i as u64,
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(format!(
                    "grpc smoke event {i} round76"
                ))),
            }),
            attributes: vec![str_attr("sourcetype", "grpc:smoke")],
            ..Default::default()
        })
        .collect();

    let request_payload = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    str_attr("host.name", "grpc-smoke-host"),
                    str_attr("service.name", "otlp-grpc-send"),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let mut client = LogsServiceClient::connect(endpoint.clone()).await?;
    let mut request = tonic::Request::new(request_payload);
    request
        .metadata_mut()
        .insert("x-scope-orgid", tenant.parse()?);
    request
        .metadata_mut()
        .insert("x-siglake-index", index.parse()?);

    let response = client.export(request).await?;
    println!("sent {n} records to {endpoint} (tenant={tenant}, index={index})");
    println!("response: {:?}", response.into_inner());
    Ok(())
}
