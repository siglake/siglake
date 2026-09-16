//! Client SQL is READ-ONLY on every entry point.
//!
//! THE DEFECT THIS PINS (task #1523, measured 2026-09-06). Every SQL handler
//! planned request text with `SessionContext::sql`, which is
//! `sql_with_options(sql, SQLOptions::new())` — DDL, DML and session statements
//! all permitted. Three things followed, all reachable by an ordinary
//! authenticated caller with no special privilege:
//!
//!   1. **Arbitrary filesystem writes.** `COPY (SELECT raw FROM events) TO
//!      '/abs/path.csv'` returned 200 and left the file on the query pod, with
//!      tenant rows in it. `.parquet`, `.json`, `COPY <table> TO`, and
//!      `EXPLAIN ANALYZE COPY` all worked the same way, on `/api/v1/sql`,
//!      `/api/v1/sql/local`, `/api/v1/sql/shard` and `priority: "batch"`.
//!   2. **Reads outside the warehouse.** `CREATE EXTERNAL TABLE t STORED AS
//!      PARQUET LOCATION '<any dir>'` returned 200 having listed and
//!      schema-read a path the tenant has no claim to.
//!   3. **Side effects before admission.** DataFusion runs `LogicalPlan::Ddl`
//!      eagerly inside `sql()`, so (2) happened *during planning* — before the
//!      cost estimate, before the admission slot, and before the `dry_run`
//!      early return. `dry_run: true` was not a preview.
//!
//! The only thing that stopped the rest of the write surface was an accident:
//! `rewrite_search_if_needed` pre-parses with plain sqlparser, which rejects
//! the DataFusion-only `STORED AS CSV` / `COPY ... STORED AS <fmt>` spellings.
//! Everything sqlparser happened to accept went through, which is why the
//! cases below deliberately use only spellings BOTH parsers accept — a test
//! built on the rejected spellings would pass against the broken code.
//!
//! Two assertions per case, not one: the status must be a 4xx refusal AND the
//! temp destination must still be empty. A refusal that arrives after the file
//! is written is not a fix.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, JobStore};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

async fn post(app: &Router, path: &str, body: serde_json::Value) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

async fn get_json(app: &Router, path: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, value)
}

/// Every regular file under `dir`, recursively.
fn files_under(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(files_under(&path));
        } else {
            out.push(path.display().to_string());
        }
    }
    out.sort();
    out
}

/// Poll a batch job to a terminal state.
async fn await_job(app: &Router, job_id: &str) -> serde_json::Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let (status, info) = get_json(app, &format!("/api/v1/jobs/{job_id}")).await;
        assert_eq!(status, StatusCode::OK, "job status: {info}");
        match info["status"].as_str() {
            Some("pending") | Some("running") => {}
            Some(_) => return info,
            None => panic!("job status has no `status` field: {info}"),
        }
        assert!(
            std::time::Instant::now() < deadline,
            "batch job {job_id} never reached a terminal state"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_sql_entry_point_writes_the_filesystem_or_the_catalog() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    ice.append_events(&[Event::now("hello".to_string())])
        .await
        .unwrap();

    // The destination every hostile statement below aims at: a temporary
    // directory OUTSIDE the warehouse, which must stay empty for the whole
    // test. `readable/` exists so the CREATE EXTERNAL TABLE case names a path
    // that WOULD have worked — the refusal has to come from the policy, not
    // from the path being unusable.
    let target = tmp.path().join("outside");
    let readable = target.join("readable");
    std::fs::create_dir_all(&readable).unwrap();

    let jobs = JobStore::new(2, Duration::from_secs(30));
    let state = AppState::new(ice.clone(), AuthConfig::open()).with_jobs(jobs);
    let app = router(state);

    let dest = |name: &str| target.join(name).display().to_string();
    let hostile: Vec<(&str, String)> = vec![
        // --- writes: reproduced against the unfixed code -------------------
        (
            "copy_literal_to_parquet",
            format!("COPY (SELECT 1 AS a) TO '{}'", dest("literal.parquet")),
        ),
        (
            "copy_events_to_csv",
            format!("COPY (SELECT raw FROM events) TO '{}'", dest("events.csv")),
        ),
        (
            "copy_table_to_json",
            format!("COPY events TO '{}'", dest("events.json")),
        ),
        (
            "explain_analyze_copy",
            format!(
                "EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '{}'",
                dest("analyze.parquet")
            ),
        ),
        // --- reads outside the warehouse, executed at PLANNING time --------
        (
            "create_external_table",
            format!(
                "CREATE EXTERNAL TABLE leak STORED AS PARQUET LOCATION '{}'",
                readable.display()
            ),
        ),
        // --- catalog / session mutation ------------------------------------
        (
            "insert_into_events",
            "INSERT INTO events SELECT * FROM events".to_string(),
        ),
        (
            "create_table_as",
            "CREATE TABLE t AS SELECT 1 AS a".to_string(),
        ),
        ("create_view", "CREATE VIEW v AS SELECT 1 AS a".to_string()),
        ("drop_table", "DROP TABLE events".to_string()),
        (
            "set_variable",
            "SET datafusion.execution.batch_size = 2".to_string(),
        ),
    ];

    // Every synchronous entry point that plans client SQL.
    let endpoints = [
        ("local", "/api/v1/sql/local", serde_json::json!({})),
        (
            // The `dry_run` preview must refuse BEFORE it can be a preview of
            // something that already happened.
            "local_dry_run",
            "/api/v1/sql/local",
            serde_json::json!({ "dry_run": true }),
        ),
        ("transparent", "/api/v1/sql", serde_json::json!({})),
        ("shard", "/api/v1/sql/shard", serde_json::json!({})),
        ("explain", "/api/v1/sql/explain", serde_json::json!({})),
    ];

    for (name, sql) in &hostile {
        for (label, path, extra) in &endpoints {
            let mut body = serde_json::json!({ "query": sql });
            for (key, value) in extra.as_object().unwrap() {
                body[key] = value.clone();
            }
            let (status, text) = post(&app, path, body).await;
            assert!(
                status.is_client_error(),
                "{name} via {label} was not refused: {status} {text}"
            );
            assert_eq!(
                files_under(&target),
                Vec::<String>::new(),
                "{name} via {label} touched the filesystem"
            );
        }

        // The batch tier plans on its own runtime, after a 202. The refusal has
        // to land there too, as a failed job.
        let (status, text) = post(
            &app,
            "/api/v1/sql",
            serde_json::json!({ "query": sql, "priority": "batch" }),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{name} via batch: {text}");
        let submitted: serde_json::Value = serde_json::from_str(&text).unwrap();
        let info = await_job(&app, submitted["job_id"].as_str().unwrap()).await;
        assert_eq!(info["status"], "failed", "{name} via batch: {info}");
        assert!(
            files_under(&target).is_empty(),
            "{name} via batch touched the filesystem: {:?}",
            files_under(&target)
        );
    }

    // The refusal is the planner's, and it names the statement class — enough
    // for a caller to see this is policy, not a parse failure.
    let (status, text) = post(
        &app,
        "/api/v1/sql/local",
        serde_json::json!({ "query": format!("COPY (SELECT 1 AS a) TO '{}'", dest("x.parquet")) }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        text.contains("DML not supported"),
        "unexpected refusal message: {text}"
    );

    // A green run must not be one where everything is refused: ordinary reads
    // still answer on every endpoint.
    for (label, path, _) in &endpoints {
        let (status, text) = post(
            &app,
            path,
            serde_json::json!({ "query": "SELECT count(*) AS n FROM events" }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "a plain SELECT was refused on {label}: {text}"
        );
    }
    let (status, text) = post(
        &app,
        "/api/v1/sql",
        serde_json::json!({ "query": "SELECT count(*) AS n FROM events", "priority": "batch" }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "batch SELECT: {text}");
    let submitted: serde_json::Value = serde_json::from_str(&text).unwrap();
    let info = await_job(&app, submitted["job_id"].as_str().unwrap()).await;
    assert_eq!(info["status"], "succeeded", "batch SELECT: {info}");
}
