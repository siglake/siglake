//! A query the engine answers from METADATA must not be pre-flight rejected for
//! the bytes a naive full scan would have read.
//!
//! THE ORDERING BUG. Byte-based pre-flight rejection ran BEFORE the Tier-1
//! fast-path battery, so `SELECT count(*)` -- answered from manifest metadata
//! having read no data at all -- was refused because a full scan of the same
//! table would exceed the interactive byte limit.
//!
//! It was invisible until 2026-08-24 because the cost estimator did not
//! recognise managed indexes and priced every user-index query at ZERO, so the
//! gate never fired on the tables anyone actually queries. Making the estimate
//! honest turned on a refusal that had never run, and it refused free queries.
//! A 1TB validation round caught it; no test did.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use serde_json::Value;
use tower::util::ServiceExt;

use siglake_core::index_config::IndexConfig;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;

/// Byte ceiling of 1: any query that genuinely scans is over it.
fn tiny_byte_ceiling() -> siglake_query_server::ServerLimits {
    let mut l = siglake_query_server::ServerLimits::default();
    l.interactive.ceiling_bytes_scanned = 1;
    l.batch.ceiling_bytes_scanned = 1;
    l
}

async fn post_sql(app: &Router, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, body)
}

#[tokio::test]
async fn a_metadata_answered_count_survives_a_tiny_byte_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let config = IndexConfig {
        index_id: "logs-bench".into(),
        ..IndexConfig::builtin_events()
    };
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident("logs-bench");
    let events: Vec<Event> = (0..2_000).map(|i| Event::now(format!("row {i}"))).collect();
    let batch = siglake_core::events_to_record_batch(&events).unwrap();
    let mapped = siglake_core::mapping::map_carrier_batch(&batch, &config).unwrap();
    ice.append_to_table(&ident, mapped, &[]).await.unwrap();

    let mut state = AppState::new(ice, AuthConfig::open());
    // A byte ceiling so small that ANY real scan is over it. The count must
    // still be answered, because it does not scan.
    state.limits = Arc::new(tiny_byte_ceiling());
    let app = router(state);

    let (status, body) = post_sql(
        &app,
        serde_json::json!({ "query": "SELECT count(*) AS n FROM \"logs-bench\"" }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "a metadata-answered count was pre-flight rejected for bytes it never \
         reads -- the byte gate is running before the fast-path battery: {body}"
    );
    assert_eq!(body["rows"][0]["n"].as_u64(), Some(2_000), "{body}");
}

/// THE REGRESSION THE 2026-08-25 ROUND FOUND. A row-limited browse must not be
/// refused for the bytes a full scan would read. `WHERE region=... LIMIT 100`
/// answered 200 in 88ms over 1.0B rows and was turned into a 400 by the byte
/// gate — 6,413 times in under three hours — because the estimator prices an
/// early-stopping LIMIT as a full 122 GB scan.
///
/// A LIMIT cannot be priced pre-flight in EITHER direction (2026-08-19:
/// `region='probe' LIMIT 100` matched 130 rows in 2B while sifting the whole
/// table), so these are bounded at runtime by the mid-flight row breaker.
#[tokio::test]
async fn a_row_limited_browse_is_not_refused_by_the_byte_gate() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let events: Vec<Event> = (0..2_000).map(|i| Event::now(format!("row {i}"))).collect();
    ice.append_events(&events).await.unwrap();

    let mut state = AppState::new(ice, AuthConfig::open());
    state.limits = Arc::new(tiny_byte_ceiling());
    let app = router(state);

    let (status, body) = post_sql(
        &app,
        serde_json::json!({ "query": "SELECT timestamp, raw FROM events LIMIT 10" }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "a LIMIT browse was refused for bytes a full scan would read — this is \
         the shape that answers in 88ms at 1.0B rows: {body}"
    );
    assert_eq!(body["row_count"].as_u64(), Some(10), "{body}");
    // The early-stop, in miniature: it stops short of the table rather than
    // reading it whole. Granularity is a batch, not the LIMIT, so the margin is
    // small on a 2,000-row fixture — the number that carries the argument is
    // the live one, where this shape answered in 88ms over 1.0B rows while
    // being priced at 122 GB in the 2026-08-25 live validation. This asserts
    // the PROPERTY (it stops early), not that environment-specific magnitude.
    let scanned = body["stats"]["rows_scanned"].as_u64().unwrap_or(u64::MAX);
    assert!(
        scanned < 2_000,
        "the LIMIT browse scanned {scanned} of 2,000 rows — it no longer stops \
         early, so the premise of exempting it from the byte gate is gone: {body}"
    );
}

/// The gate must still refuse an UNBOUNDED scan. Moving LIMITed queries to the
/// runtime breaker must not disable pre-flight refusal for the work it is
/// actually for: aggregations over everything, exports, whole-table sorts.
#[tokio::test]
async fn an_unbounded_scan_is_still_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let events: Vec<Event> = (0..2_000)
        .map(|i| Event::now(format!("row {i} needle")))
        .collect();
    ice.append_events(&events).await.unwrap();

    let mut state = AppState::new(ice, AuthConfig::open());
    state.limits = Arc::new(tiny_byte_ceiling());
    let app = router(state);

    // No LIMIT, and not answerable from metadata: it must scan, so refuse it.
    let (status, body) = post_sql(
        &app,
        serde_json::json!({ "query": "SELECT raw FROM events WHERE raw LIKE '%needle%'" }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an unbounded scan slipped past the byte gate — exempting LIMITed \
         queries must not exempt everything: {body}"
    );
}
