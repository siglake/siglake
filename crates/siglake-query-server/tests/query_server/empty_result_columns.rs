//! #7420: a zero-row result still declares the columns the query projected.
//!
//! `columns` is the served column SET, not a summary of the rows that came
//! back (`result_cache_schema_generation.rs` pins the same contract for a
//! column that is null in every row). A collect that produces no batches has
//! no batch schema to read the names from, so every path that renders records
//! passes its final projected output schema for that case.

use crate::support;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;

use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, JobStore};
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

async fn warehouse(n: usize) -> (Arc<IcebergContext>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
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
    (Arc::new(ice), tmp)
}

async fn spawn(n: usize) -> Server {
    let (ice, tmp) = warehouse(n).await;
    let state =
        AppState::new(ice, AuthConfig::open()).with_jobs(JobStore::new(2, Duration::from_secs(10)));
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

async fn records(base: &str, path: &str, query: &str) -> Value {
    let resp = reqwest::Client::new()
        .post(format!("{base}{path}"))
        .json(&serde_json::json!({ "query": query }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{path} `{query}`");
    resp.json().await.unwrap()
}

fn columns(body: &Value) -> Vec<String> {
    body["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("no columns in {body}"))
        .iter()
        .map(|c| c.as_str().expect("a column name").to_string())
        .collect()
}

fn assert_empty_with_columns(body: &Value, expected: &[&str]) {
    assert_eq!(body["row_count"], 0, "{body}");
    assert_eq!(body["rows"], serde_json::json!([]), "{body}");
    assert_eq!(columns(body), expected, "{body}");
}

#[tokio::test]
async fn filtered_to_empty_projection_keeps_its_column_names() {
    require_loopback!();
    let srv = spawn(50).await;
    let body = records(
        &srv.base,
        "/api/v1/sql",
        "SELECT host FROM events WHERE host = 'nope'",
    )
    .await;
    assert_empty_with_columns(&body, &["host"]);
}

#[tokio::test]
async fn filtered_to_empty_result_keeps_aliases_and_projection_order() {
    require_loopback!();
    let srv = spawn(50).await;
    let body = records(
        &srv.base,
        "/api/v1/sql",
        "SELECT source, host AS h, upper(raw) AS shout \
         FROM events WHERE host = 'nope'",
    )
    .await;
    assert_empty_with_columns(&body, &["source", "h", "shout"]);

    // Same columns, different projection order: `columns` follows the query,
    // not the table.
    let body = records(
        &srv.base,
        "/api/v1/sql",
        "SELECT host AS h, source FROM events WHERE host = 'nope'",
    )
    .await;
    assert_empty_with_columns(&body, &["h", "source"]);
}

#[tokio::test]
async fn select_star_over_an_empty_table_declares_the_table_columns() {
    require_loopback!();
    let srv = spawn(0).await;
    let empty = records(&srv.base, "/api/v1/sql", "SELECT * FROM events").await;
    assert_eq!(empty["row_count"], 0, "{empty}");

    // The populated control fixes what `SELECT *` projects; the empty table
    // must declare the same list.
    let populated = spawn(5).await;
    let rows = records(&populated.base, "/api/v1/sql", "SELECT * FROM events").await;
    assert_eq!(rows["row_count"], 5, "{rows}");
    let expected = columns(&rows);
    assert!(expected.contains(&"host".to_string()), "{rows}");
    assert_eq!(columns(&empty), expected, "{empty}");
}

#[tokio::test]
async fn an_empty_projection_still_declares_no_columns() {
    require_loopback!();
    // #7404's semantics: `SELECT FROM events` projects nothing, so `columns`
    // is empty because the query asked for nothing — the one case that must
    // stay distinguishable from "projected columns, matched no rows".
    let srv = spawn(50).await;
    let body = records(&srv.base, "/api/v1/sql", "SELECT FROM events").await;
    assert_eq!(body["columns"], serde_json::json!([]), "{body}");
    assert_eq!(body["row_count"], 50, "{body}");

    let empty = spawn(0).await;
    let body = records(&empty.base, "/api/v1/sql", "SELECT FROM events").await;
    assert_empty_with_columns(&body, &[]);
}

#[tokio::test]
async fn the_shard_endpoint_keeps_columns_on_an_empty_result() {
    require_loopback!();
    let srv = spawn(50).await;
    let body = records(
        &srv.base,
        "/api/v1/sql/local",
        "SELECT host AS h FROM events WHERE host = 'nope'",
    )
    .await;
    assert_empty_with_columns(&body, &["h"]);
}

#[tokio::test]
async fn a_batch_job_result_keeps_columns_on_an_empty_result() {
    require_loopback!();
    let srv = spawn(50).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query": "SELECT host AS h, source FROM events WHERE host = 'nope'",
            "priority": "batch",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let job: Value = resp.json().await.unwrap();
    let id = job["job_id"].as_str().unwrap().to_string();

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let status = loop {
        let body: Value = client
            .get(format!("{}/api/v1/jobs/{id}", srv.base))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let status = body["status"].as_str().unwrap().to_string();
        if matches!(
            status.as_str(),
            "succeeded" | "failed" | "cancelled" | "timeout"
        ) {
            break status;
        }
        assert!(std::time::Instant::now() < deadline, "job stalled: {body}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(status, "succeeded");

    let body: Value = client
        .get(format!("{}/api/v1/jobs/{id}/result", srv.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_empty_with_columns(&body, &["h", "source"]);
}

#[tokio::test]
async fn a_distributed_empty_result_keeps_columns() {
    require_loopback!();
    let (ice, _tmp) = warehouse(50).await;

    // Worker peer, then the coordinator over the same warehouse.
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url_b = format!("http://{}", listener_b.local_addr().unwrap());
    let app_b = router(AppState::new(ice.clone(), AuthConfig::open()));
    let _hb = tokio::spawn(async move { axum::serve(listener_b, app_b).await.unwrap() });

    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url_a = format!("http://{}", listener_a.local_addr().unwrap());
    let state_a = AppState::new(ice.clone(), AuthConfig::open())
        .with_coordinator(vec![url_a.clone(), url_b.clone()], None);
    let app_a = router(state_a);
    let _ha = tokio::spawn(async move { axum::serve(listener_a, app_a).await.unwrap() });

    // Scan fan-out and an aggregate fan-out: both merge to no rows, and both
    // must still declare the projection.
    let body = records(
        &url_a,
        "/api/v1/sql/distributed",
        "SELECT host AS h, raw FROM events WHERE host = 'nope'",
    )
    .await;
    assert_empty_with_columns(&body, &["h", "raw"]);

    let body = records(
        &url_a,
        "/api/v1/sql/distributed",
        "SELECT host, count(*) AS n FROM events WHERE host = 'nope' GROUP BY host",
    )
    .await;
    assert_empty_with_columns(&body, &["host", "n"]);
}
