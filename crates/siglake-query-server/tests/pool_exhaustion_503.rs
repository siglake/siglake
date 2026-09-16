//! A query the memory pool refuses is a capacity answer, not a server fault.
//!
//! THE CONTRACT THIS PINS. The process-wide pool is the design: sorts,
//! aggregates and joins are bounded and spill when they can. When an operator
//! that cannot spill (a `TopK`, an ordered merge) is refused, DataFusion raises
//! `ResourcesExhausted` — and until this test existed that surfaced as HTTP 500
//! with no `Retry-After` and no metric. A 500 tells the client the server broke
//! and pages someone; the truth is "wait, then retry", which is 503 +
//! `Retry-After`, the answer the ingester already gives for a full lane. It is
//! not 429: the client did nothing wrong, the pool refused on everyone's usage.
//!
//! Every route that executes a plan must give that answer — the transparent
//! `/api/v1/sql`, the explicit `/api/v1/sql/local`, and the worker's
//! `/api/v1/sql/shard` (whose 503 the coordinator forwards rather than re-running
//! the shard on itself) — and each refusal must be counted under
//! `siglake_query_breaker_trips_total{breaker="pool_exhausted"}` so an operator
//! can tell memory pressure from a defect.
//!
//! Its own binary, on purpose: the pool is a process-wide `OnceLock`, pinned
//! here to a size no real query pod would run at, through
//! `preset_query_memory_pool_bytes` rather than `set_var`. Nothing here reads
//! or writes the environment.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::Router;
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

/// Small enough that a `TopK` over the table below cannot fit, large enough
/// that a scan's decode reservation (which degrades to unreserved rather than
/// failing) and a Tier-1 answer still work — the test proves both.
///
/// Was 2 MiB until the 2026-09-06 timestamp contract added the `timestamp_ns`
/// column. The scan-decode reservation is derived from planned FILE bytes, not
/// from the projection, so a wider file raises it for every query: at 2 MiB the
/// decode reservation alone took 1913 KB and the positive control (a 10-row
/// browse) no longer had room for its `SortPreservingMergeExec`. This is a
/// property of a deliberately absurd pool, not of a real 4Gi query pod.
const POOL_BYTES: u64 = 3 * 1024 * 1024;

/// A query whose `TopK` must hold every row: `raw` is not the scan order, so
/// nothing early-stops, and the heap alone is several times the pool.
const REFUSED_SQL: &str = "SELECT \"timestamp\", host, raw FROM events ORDER BY raw LIMIT 50000";

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str, labels: &[(&str, &str)]) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && labels.iter().all(|(lk, lv)| {
                    key.key()
                        .labels()
                        .any(|l| l.key() == *lk && l.value() == *lv)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

/// Several files whose time ranges overlap, with `raw` long enough that
/// 50,000 rows of it are unambiguously larger than the pool.
async fn wide_table(ice: &IcebergContext) {
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let hosts = ["web-01", "web-02", "db-01", "cache-01"];
    for file in 0..3i64 {
        let mut events = Vec::with_capacity(20_000);
        for i in 0..20_000i64 {
            let k = i * 3 + file;
            let mut e = Event::now(String::new());
            e.timestamp = base + ChronoDuration::milliseconds(k * 700);
            e.host = hosts[(k % hosts.len() as i64) as usize].to_string();
            // ~130 bytes each, unique per row so the sort has real work.
            e.raw = format!(
                "row {k:08} request served in {}ms for /api/v1/items/{} from 10.0.{}.{} \
                 user-agent=siglake-bench/1.0 trace={k:016x}",
                (k * 37) % 1000,
                (k * 7919) % 100_000,
                (k / 256) % 256,
                k % 256
            );
            events.push(e);
        }
        ice.append_events(&events).await.unwrap();
    }
}

async fn post_json(
    app: &Router,
    path: &str,
    body: serde_json::Value,
) -> (StatusCode, HeaderMap, serde_json::Value) {
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
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or_else(
        |_| serde_json::json!({ "raw_body": String::from_utf8_lossy(&bytes).to_string() }),
    );
    (status, headers, body)
}

async fn post_ndjson(
    app: &Router,
    path: &str,
    body: serde_json::Value,
) -> (StatusCode, HeaderMap, Vec<serde_json::Value>) {
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
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let lines = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("every NDJSON line must be valid JSON"))
        .collect();
    (status, headers, lines)
}

fn retry_after_secs(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

fn assert_pool_refusal(
    route: &str,
    status: StatusCode,
    headers: &HeaderMap,
    body: &serde_json::Value,
) {
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{route}: a pool refusal must be 503, got {status} with body {body}"
    );
    let retry_after = retry_after_secs(headers);
    assert!(
        matches!(retry_after, Some(secs) if secs >= 1),
        "{route}: a 503 for a pool refusal must carry Retry-After >= 1, got {:?}",
        headers.get(header::RETRY_AFTER)
    );
    assert_eq!(
        body["code"], 503,
        "{route}: body code must match the status: {body}"
    );
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("query memory pool exhausted"),
        "{route}: the body must say what refused, got: {msg}"
    );
    assert!(
        msg.contains("remain available for the total pool") || msg.contains("Failed to allocate"),
        "{route}: DataFusion's own account (consumer and bytes) must survive to the client: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_allocation_is_a_503_with_retry_after_on_every_execution_route() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    // Explicit config, not `set_var`: pin the process-wide pool before anything
    // in this binary touches it.
    assert!(
        siglake_storage::preset_query_memory_pool_bytes(POOL_BYTES),
        "the query memory pool was already built at another size before this binary \
         could bound it at {POOL_BYTES} bytes — nothing may touch the pool before this"
    );

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    wide_table(&ice).await;
    let app = router(AppState::new(ice, AuthConfig::open()));

    // The bound must not have made the pod useless: a Tier-1 answer and a
    // small browse both fit. Trading an OOM for universal ResourcesExhausted
    // would be a different outage, and a 503 from a broken server would pass
    // the assertions below for the wrong reason.
    let (status, _, body) = post_json(
        &app,
        "/api/v1/sql",
        serde_json::json!({ "query": "SELECT count(*) AS n FROM events" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "count(*) must fit a {POOL_BYTES}-byte pool: {body}"
    );
    assert_eq!(body["rows"][0]["n"], 60_000);
    let (status, _, body) = post_json(
        &app,
        "/api/v1/sql",
        serde_json::json!({ "query": "SELECT host, raw FROM events LIMIT 10" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "a 10-row browse must fit: {body}");

    // The transparent entry point (no peers configured, so it runs the
    // single-pod path) and the explicit single-pod route. A refusal is never
    // inserted into the result cache, so the same SQL executes on both routes.
    for route in ["/api/v1/sql", "/api/v1/sql/local"] {
        let (status, headers, body) =
            post_json(&app, route, serde_json::json!({ "query": REFUSED_SQL })).await;
        assert_pool_refusal(route, status, &headers, &body);
    }

    // The worker route: the coordinator forwards this verdict (see
    // `ApiError::from_query`) instead of collapsing it to 500 or re-running the
    // shard on itself.
    let (status, headers, body) = post_json(
        &app,
        "/api/v1/sql/shard",
        serde_json::json!({
            "query": REFUSED_SQL,
            "shard": { "index": 0, "count": 1 },
        }),
    )
    .await;
    assert_pool_refusal("/api/v1/sql/shard", status, &headers, &body);

    // Once an NDJSON response has started its HTTP status is already 200. A
    // refusal while polling the plan therefore travels as the final parseable
    // line, including the retry contract that can no longer be put in headers.
    let (status, headers, lines) = post_ndjson(
        &app,
        "/api/v1/sql/local",
        serde_json::json!({
            "query": REFUSED_SQL,
            "format": "ndjson",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get(header::RETRY_AFTER).is_none());
    let trailer = lines.last().expect("NDJSON error trailer");
    assert_eq!(trailer["_meta"], "error", "lines: {lines:?}");
    assert_eq!(trailer["code"], 503, "trailer: {trailer}");
    assert_eq!(trailer["retry_after_secs"], 5, "trailer: {trailer}");
    let error = trailer["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("query memory pool exhausted"),
        "trailer must identify the pool refusal: {trailer}"
    );
    assert!(
        error.contains("remain available for the total pool")
            || error.contains("Failed to allocate"),
        "DataFusion's allocation account must survive in the trailer: {trailer}"
    );

    // Each refusal counted once, under the breaker scheme the other refusals
    // use, with the priority that ran. Three pre-response routes plus one
    // streamed refusal, four trips.
    let snapshot = snapshotter.snapshot().into_vec();
    let trips = counter_sum(
        &snapshot,
        "siglake_query_breaker_trips_total",
        &[("breaker", "pool_exhausted"), ("priority", "interactive")],
    );
    assert_eq!(
        trips, 4,
        "four pool refusals must be four pool_exhausted trips, got {trips}"
    );
    // And they were 503s in the request counter, not 500s.
    let served_503 = counter_sum(
        &snapshot,
        "siglake_query_requests_total",
        &[("endpoint", "sql"), ("status", "503")],
    );
    assert!(
        served_503 >= 2,
        "the two /sql refusals must be counted as 503 requests, got {served_503}"
    );
    let served_500 = counter_sum(
        &snapshot,
        "siglake_query_requests_total",
        &[("endpoint", "sql"), ("status", "500")],
    );
    assert_eq!(served_500, 0, "no refusal may be counted as a server fault");

    // The pool gives everything back once the refused queries are gone, so the
    // next query is not refused by a leak from this one.
    let (status, _, body) = post_json(
        &app,
        "/api/v1/sql",
        serde_json::json!({ "query": "SELECT count(*) AS n FROM events" }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the pod must still serve after refusing: {body}"
    );
}
