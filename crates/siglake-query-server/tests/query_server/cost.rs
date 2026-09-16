//! Cost-estimator tests against fixture warehouses with known sizes.

use crate::support;

use std::sync::Arc;

use chrono::Utc;
use serde_json::Value;

use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;

macro_rules! require_loopback {
    () => {
        if !support::loopback_available() {
            return;
        }
    };
}

struct Server {
    base: String,
    _tmp: tempfile::TempDir,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn spawn(n: usize) -> Server {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    if n > 0 {
        let events: Vec<Event> = (0..n)
            .map(|i| Event {
                timestamp: Utc::now(),
                host: format!("host-{}", i % 4),
                source: "smoke".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: format!("event {i}"),
                attributes: None,
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    let state = AppState::new(Arc::new(ice), AuthConfig::open());
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        base: format!("http://{addr}"),
        _tmp: tmp,
        handle,
    }
}

#[tokio::test]
async fn sql_explain_returns_cost_without_execution() {
    require_loopback!();
    let srv = spawn(50).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql/explain", srv.base))
        .json(&serde_json::json!({ "query": "SELECT count(*) FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert!(body["complexity_class"].is_string());
    // 50 small events fits comfortably in the small bucket.
    assert_eq!(body["complexity_class"], "small");
    // No rows[] / row_count[] in /explain responses.
    assert!(body.get("rows").is_none());
    // Manifest walk surfaces non-zero stats now (chunk: Iceberg
    // manifest cost walk).
    assert!(body["estimated_bytes_scanned"].as_u64().unwrap() > 0);
    assert_eq!(body["estimated_rows_processed"], 50);
    assert!(body["files_to_scan"].as_u64().unwrap() >= 1);
    assert_eq!(body["exact"], true);
}

#[tokio::test]
async fn sql_dry_run_skips_execution() {
    require_loopback!();
    let srv = spawn(50).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":   "SELECT * FROM events",
            "dry_run": true,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    // Dry-run response is the bare CostReport, not the records envelope.
    assert!(body.get("rows").is_none());
    assert!(body["complexity_class"].is_string());
}

#[tokio::test]
async fn sql_records_response_includes_cost() {
    require_loopback!();
    let srv = spawn(5).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT host FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert!(body["rows"].is_array());
    assert!(body["cost"].is_object());
    assert_eq!(body["cost"]["complexity_class"], "small");
}

#[tokio::test]
async fn warning_when_no_time_predicate() {
    require_loopback!();
    let srv = spawn(5).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql/explain", srv.base))
        .json(&serde_json::json!({ "query": "SELECT * FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let warnings = body["warnings"].as_array().unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().unwrap().contains("timestamp")),
        "expected time-predicate warning, got {warnings:?}"
    );
}

#[tokio::test]
async fn no_time_warning_when_predicate_present() {
    require_loopback!();
    let srv = spawn(5).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql/explain", srv.base))
        .json(&serde_json::json!({
            "query": "SELECT * FROM events WHERE timestamp > to_timestamp('2020-01-01')"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let warnings = body["warnings"].as_array().unwrap();
    assert!(
        !warnings
            .iter()
            .any(|w| w.as_str().unwrap().contains("timestamp")),
        "did not expect time-predicate warning, got {warnings:?}"
    );
}
