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

// #7404: `SELECT FROM events` — an empty projection list — is syntax the
// locked parser accepts (sqlparser 0.61 `GenericDialect`/`PostgreSqlDialect`
// both opt into it), so it reaches planning like any other query and the
// server answers 200. The tests below pin what it resolves to: rows with no
// column data, NOT `SELECT *`. Row count, filters and LIMIT behave exactly as
// they do for a projecting query over the same table.

#[tokio::test]
async fn empty_projection_selects_rows_without_columns() {
    require_loopback!();
    let srv = spawn(50).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    // No columns, but one (empty) row per table row: not `SELECT *`.
    assert_eq!(body["columns"], serde_json::json!([]));
    assert_eq!(body["row_count"], 50);
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 50);
    assert!(
        rows.iter().all(|r| r.as_object().unwrap().is_empty()),
        "expected every row to carry no fields, got {:?}",
        &rows[..rows.len().min(3)]
    );

    // The `SELECT *` control over the same fixture does project the table's
    // columns, which is what the empty projection is not doing.
    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT * FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let columns = body["columns"].as_array().unwrap();
    assert!(
        columns.iter().any(|c| c == "host"),
        "expected `SELECT *` to project table columns, got {columns:?}"
    );
    assert_eq!(body["row_count"], 50);
}

#[tokio::test]
async fn empty_projection_honours_filter_and_limit() {
    require_loopback!();
    let srv = spawn(50).await;
    let client = reqwest::Client::new();

    // hosts cycle host-0..host-3, so 13 of the 50 events land on host-1.
    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query": "SELECT FROM events WHERE host = 'host-1'"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["columns"], serde_json::json!([]));
    assert_eq!(body["row_count"], 13);

    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT FROM events LIMIT 3" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["row_count"], 3);
}

#[tokio::test]
async fn empty_projection_over_empty_table_returns_no_rows() {
    require_loopback!();
    let srv = spawn(0).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["columns"], serde_json::json!([]));
    assert_eq!(body["row_count"], 0);
    assert_eq!(body["rows"], serde_json::json!([]));
}

#[tokio::test]
async fn empty_projection_ndjson_renders_empty_objects() {
    require_loopback!();
    let srv = spawn(5).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":  "SELECT FROM events",
            "format": "ndjson",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "{}\n{}\n{}\n{}\n{}\n");
}

#[tokio::test]
async fn empty_projection_dry_run_returns_cost_only() {
    require_loopback!();
    let srv = spawn(50).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":   "SELECT FROM events",
            "dry_run": true,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    // Bare CostReport, same shape as any other dry run.
    assert!(body.get("rows").is_none());
    assert!(body.get("row_count").is_none());
    assert_eq!(body["complexity_class"], "small");
    assert_eq!(body["estimated_rows_processed"], 50);
    assert_eq!(body["exact"], true);
}

#[tokio::test]
async fn invalid_queries_are_rejected_at_plan_time() {
    require_loopback!();
    let srv = spawn(5).await;
    let client = reqwest::Client::new();
    // The sibling controls for #7404: each of these is rejected before
    // execution, which is what makes the empty projection's 200 a statement
    // about the syntax being valid rather than about missing validation.
    for (query, needle) in [
        ("SELECT no_such_fn(host) FROM events", "no_such_fn"),
        ("SELECT no_such_col FROM events", "no_such_col"),
        ("SELECT lower(host, raw) FROM events", "lower"),
        ("SELECT FROM no_such_table", "no_such_table"),
    ] {
        let resp = client
            .post(format!("{}/api/v1/sql", srv.base))
            .json(&serde_json::json!({ "query": query, "dry_run": true }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "expected 400 for {query}");
        let body: Value = resp.json().await.unwrap();
        let message = body["error"].as_str().unwrap();
        assert!(
            message.contains(needle),
            "expected `{needle}` in error for {query}, got {message}"
        );
    }
}

#[tokio::test]
async fn empty_projection_matches_datafusion() {
    // The server's behaviour is DataFusion's, not a siglake rewrite: planning
    // the same syntax on a bare SessionContext yields the same empty schema
    // and the same row count.
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let df = ctx.sql("SELECT FROM t").await.unwrap();
    assert_eq!(df.schema().fields().len(), 0);
    let rows: usize = df
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 2);
}
