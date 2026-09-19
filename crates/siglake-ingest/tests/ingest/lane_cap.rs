use std::time::Duration;

use axum::body::Body;
use axum::http::{header::RETRY_AFTER, Request, StatusCode};
use axum::response::Response;
use axum::Router;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tonic::metadata::MetadataValue;
use tonic::{Code, Request as TonicRequest, Status};
use tower::util::ServiceExt;

use siglake_ingest::backpressure::BackpressureRouter;
use siglake_ingest::{router, AppState, OtlpGrpcLogsService, OtlpGrpcTracesService, INDEX_HEADER};
use siglake_wal::WalWriter;

fn capped_state(root: &std::path::Path) -> AppState {
    let direct = WalWriter::with_thresholds(
        root.join("direct"),
        "unused-direct",
        100,
        Duration::from_secs(60),
    )
    .unwrap();
    let lanes = BackpressureRouter::new(
        root.join("lanes"),
        "lane-cap",
        100,
        Duration::from_secs(60),
        16,
    )
    .with_max_lanes(1);
    AppState::with_streaming(direct).with_backpressure(lanes)
}

async fn send(app: &Router, uri: &str, content_type: &str, body: &'static str) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

fn assert_http_lane_refusal(surface: &str, response: &Response) {
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "unexpected {surface} status"
    );
    assert!(
        response.headers().get(RETRY_AFTER).is_none(),
        "a persistent lane-cap refusal advertised a retry delay"
    );
}

fn assert_grpc_lane_refusal(status: &Status) {
    assert_eq!(status.code(), Code::Unavailable);
    assert!(
        status.metadata().get("retry-after").is_none(),
        "a persistent lane-cap refusal advertised plain retry-after metadata"
    );
    assert!(
        status.details().is_empty(),
        "a persistent lane-cap refusal carried RetryInfo or other status details"
    );
}

#[tokio::test]
async fn lane_cap_is_hintless_503_or_unavailable_on_every_ingest_surface() {
    let tmp = tempfile::tempdir().unwrap();
    let state = capped_state(tmp.path());
    let app = router(state.clone());
    let grpc_logs = OtlpGrpcLogsService::new(state.clone());
    let grpc_traces = OtlpGrpcTracesService::new(state);

    // Fill the sole slot with the built-in events lane, then prove that the
    // already-admitted key remains writable at the cap.
    let admitted = send(&app, "/v1/logs", "application/json", "{}").await;
    assert_eq!(admitted.status(), StatusCode::OK);
    let existing = send(&app, "/v1/logs", "application/json", "{}").await;
    assert_eq!(
        existing.status(),
        StatusCode::OK,
        "an existing lane stopped accepting when the lane map reached its cap"
    );

    // Select a novel index for the logs refusal without spending a separate
    // test server.
    let logs = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("content-type", "application/json")
                .header(INDEX_HEADER, "logs-over-cap")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_http_lane_refusal("HTTP logs", &logs);

    assert_http_lane_refusal(
        "HTTP traces",
        &send(
            &app,
            "/v1/traces",
            "application/json",
            "{\"resourceSpans\":[]}",
        )
        .await,
    );
    assert_http_lane_refusal(
        "bulk body index",
        &send(
            &app,
            "/api/v1/_elastic/_bulk",
            "application/x-ndjson",
            "{\"index\":{\"_index\":\"bulk-over-cap\"}}\n{\"message\":\"x\"}\n",
        )
        .await,
    );
    assert_http_lane_refusal(
        "bulk path index",
        &send(
            &app,
            "/api/v1/_elastic/path-over-cap/_bulk",
            "application/x-ndjson",
            "{\"create\":{}}\n{\"message\":\"x\"}\n",
        )
        .await,
    );

    let mut logs_request = TonicRequest::new(ExportLogsServiceRequest::default());
    logs_request.metadata_mut().insert(
        INDEX_HEADER,
        MetadataValue::try_from("grpc-logs-over-cap").unwrap(),
    );
    let logs_status = grpc_logs.export(logs_request).await.unwrap_err();
    assert_grpc_lane_refusal(&logs_status);

    let traces_status = grpc_traces
        .export(TonicRequest::new(ExportTraceServiceRequest::default()))
        .await
        .unwrap_err();
    assert_grpc_lane_refusal(&traces_status);
}

#[tokio::test]
async fn genuine_lane_creation_failure_stays_500_or_internal() {
    let tmp = tempfile::tempdir().unwrap();
    let blocked_root = tmp.path().join("not-a-directory");
    std::fs::write(&blocked_root, b"file").unwrap();
    let direct = WalWriter::with_thresholds(
        tmp.path().join("direct"),
        "unused-direct",
        100,
        Duration::from_secs(60),
    )
    .unwrap();
    let lanes = BackpressureRouter::new(
        &blocked_root,
        "broken-lane",
        100,
        Duration::from_secs(60),
        16,
    )
    .with_max_lanes(1);
    let state = AppState::with_streaming(direct).with_backpressure(lanes);
    let app = router(state.clone());
    let grpc = OtlpGrpcLogsService::new(state);

    let response = send(&app, "/v1/logs", "application/json", "{}").await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response.headers().get(RETRY_AFTER).is_none());

    let status = grpc
        .export(TonicRequest::new(ExportLogsServiceRequest::default()))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Internal);
    assert!(status.metadata().get("retry-after").is_none());
    assert!(status.details().is_empty());
}
