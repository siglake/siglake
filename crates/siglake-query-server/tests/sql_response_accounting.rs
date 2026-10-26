use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use siglake_core::Event;
use siglake_query_server::{
    router, AppState, AuthConfig, HANDLER_MICROS_HEADER, RESULT_CACHE_HEADER,
};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

async fn post(
    app: &axum::Router,
    request: serde_json::Value,
) -> (axum::response::Response, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql/local")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let bytes = to_bytes(body, 1024 * 1024).await.unwrap().to_vec();
    (
        axum::response::Response::from_parts(parts, Body::empty()),
        bytes,
    )
}

#[tokio::test]
async fn unmeasured_response_paths_omit_execution_totals() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    ice.append_events(&[Event::now("first"), Event::now("second")])
        .await
        .unwrap();
    let app = router(AppState::new(ice, AuthConfig::open()));

    let (fast_response, fast_bytes) = post(
        &app,
        serde_json::json!({"query": "SELECT count(*) AS n FROM events"}),
    )
    .await;
    assert_eq!(fast_response.status(), StatusCode::OK);
    assert!(fast_response.headers().contains_key(HANDLER_MICROS_HEADER));
    assert!(fast_response.headers().contains_key(RESULT_CACHE_HEADER));
    let fast: serde_json::Value = serde_json::from_slice(&fast_bytes).unwrap();
    assert!(fast["stats"]["phases"]["execution_micros"].is_null());
    assert!(fast["stats"]["phases"]["other_micros"].is_null());

    let (truncated_response, truncated_bytes) = post(
        &app,
        serde_json::json!({
            "query": "SELECT raw FROM events ORDER BY timestamp DESC",
            "limits": {"max_rows_returned": 1}
        }),
    )
    .await;
    assert_eq!(truncated_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(truncated_response
        .headers()
        .contains_key(HANDLER_MICROS_HEADER));
    let truncated: serde_json::Value = serde_json::from_slice(&truncated_bytes).unwrap();
    assert_eq!(truncated["truncated"], true);
    assert!(truncated["stats"]["phases"]["execution_micros"].is_null());
    assert!(truncated["stats"]["phases"]["other_micros"].is_null());

    let (ndjson_response, ndjson_bytes) = post(
        &app,
        serde_json::json!({
            "query": "SELECT raw FROM events ORDER BY timestamp DESC LIMIT 1",
            "format": "ndjson"
        }),
    )
    .await;
    assert_eq!(ndjson_response.status(), StatusCode::OK);
    assert!(!ndjson_response
        .headers()
        .contains_key(HANDLER_MICROS_HEADER));
    assert!(!ndjson_response.headers().contains_key(RESULT_CACHE_HEADER));
    assert!(!ndjson_bytes.is_empty());
}
