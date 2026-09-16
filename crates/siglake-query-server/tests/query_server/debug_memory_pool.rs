use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Request, StatusCode};
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

async fn app() -> (axum::Router, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let app = router(AppState::new(ice, AuthConfig::from_tokens(["debug-token"])));
    (app, tmp)
}

#[tokio::test]
async fn memory_pool_debug_endpoint_requires_api_auth() {
    let (app, _tmp) = app().await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/debug/memory-pool")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn memory_pool_debug_endpoint_returns_current_snapshot() {
    let (app, _tmp) = app().await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/debug/memory-pool")
                .header(header::AUTHORIZATION, "Bearer debug-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(body.get("reserved_bytes").is_some());
    assert!(body.get("limit_bytes").is_some());
    assert!(body.get("top_consumers").is_some());
    assert_eq!(
        body["reserved_bytes"].is_null(),
        body["limit_bytes"].is_null()
    );
}
