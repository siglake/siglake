//! The query memory pool must read ZERO once every query has finished.
//!
//! THE READING THIS PINS. After the 2026-09-01 fast-path regression suite,
//! `siglake_query_memory_pool_reserved_bytes` read 3,657,844,321 (3.66 GiB) on a
//! pod with admission at 0 and no request in flight; the 2026-08-29 1TB
//! validation had established 0 as the healthy idle value after ~12,000
//! queries. A reservation that does not return inflates the ratio behind
//! `SiglakeQueryPoolNearLimit` and the KEDA signal, and every memory outage this
//! project has had lived in the query pod.
//!
//! So: every shape in the benchmark workload matrix -- the suite that produced
//! the reading -- runs several times in both response formats through the real
//! router, and after each shape the pool must settle back to 0. The matrix is
//! copied here because this test remains in the public tree while the benchmark
//! harness does not. Then the two
//! ways a query goes away WITHOUT finishing: a client that hangs up mid-NDJSON
//! stream, and one that never reads its records response. Those are the paths
//! whose abandoned scans historically produced the "only a restart fixed it"
//! degradations, and they are the paths a per-request Drop can miss.
//!
//! When it fails, the message carries `query_memory_pool_top_consumers`, so a
//! residual names the operator or stream holding it rather than just a size.
//!
//! ONE test function, and its own binary, on purpose: the pool is process-wide,
//! so a sibling test's live reservation would read as this test's leak.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{to_bytes, Body, Bytes};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use futures::StreamExt;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

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

/// `(name, sql)` for every Siglake arm in the benchmark workload matrix.
///
/// Keep this copy in sync when that matrix changes. The benchmark harness is
/// deliberately absent from the public tree, where this regression test must
/// remain self-contained and runnable. The full-window bucket uses 24 hours;
/// any positive value exercises the same plan.
const BENCH_SHAPES: &[(&str, &str)] = &[
    ("count_all", "SELECT count(*) AS n FROM events"),
    (
        "rare_needle_10",
        "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang0')",
    ),
    (
        "rare_needle_1000",
        "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang2')",
    ),
    (
        "rare_needle_100000",
        "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang4')",
    ),
    (
        "common_term",
        "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'error')",
    ),
    (
        "phrase",
        "SELECT count(*) AS n FROM events WHERE match_phrase(raw, 'quantum entanglement cascade')",
    ),
    (
        "like_substring",
        "SELECT count(*) AS n FROM events WHERE raw LIKE '%xqzfrag%'",
    ),
    (
        "date_histogram_1h",
        "SELECT date_bin(INTERVAL '1 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket",
    ),
    (
        "date_histogram_24h",
        "SELECT date_bin(INTERVAL '24 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket",
    ),
    (
        "date_histogram_full",
        "SELECT date_bin(INTERVAL '24 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket",
    ),
    (
        "terms_top10_hosts",
        "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC, host ASC LIMIT 10",
    ),
    (
        "newest_first_100",
        "SELECT timestamp, host, raw FROM events LIMIT 100",
    ),
];

/// Several APPENDS whose time ranges all overlap, so a timestamp-ordered scan
/// takes the per-cluster merge path (a `SiglakeOrderedMerge` reservation per
/// partition) and every partition holds a `siglake-scan-decode` reservation.
/// The rows carry the bench corpus's needles so the FTS shapes match something.
async fn overlapping_table(ice: &IcebergContext) {
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let hosts = ["web-01", "web-02", "db-01", "cache-01"];
    for file in 0..6i64 {
        let mut events = Vec::with_capacity(20_000);
        for i in 0..20_000i64 {
            let k = i * 6 + file;
            let mut e = Event::now(String::new());
            e.timestamp = base + ChronoDuration::milliseconds(k * 700);
            e.host = hosts[(k % hosts.len() as i64) as usize].to_string();
            e.raw = match k % 997 {
                0 => format!("row {k} zugzwang0 error"),
                1 => format!("row {k} zugzwang2 warn"),
                2 => format!("row {k} zugzwang4 info"),
                3 => format!("row {k} quantum entanglement cascade observed"),
                4 => format!("row {k} prefix-xqzfrag-suffix"),
                r if r % 5 == 0 => format!("row {k} error connecting upstream"),
                _ => format!("row {k} request served"),
            };
            events.push(e);
        }
        ice.append_events(&events).await.unwrap();
    }
}

async fn post_sql(
    app: &Router,
    sql: &str,
    format: &str,
    max_rows_returned: usize,
) -> (StatusCode, Bytes) {
    let body = serde_json::json!({
        "query": sql,
        "format": format,
        "limits": { "max_rows_returned": max_rows_returned },
    });
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
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, bytes)
}

/// Wait for the pool to read 0. Pumps DataFusion spawned above a finished
/// stream release their reservations when they observe end-of-input, which is
/// a scheduling hop after the response is built, so this polls rather than
/// asserting instantly -- but it does not wait long: a live query on this
/// table finishes in well under a second, so anything still held after the
/// deadline is not a query in flight.
async fn settle_to_zero(label: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last = None;
    while Instant::now() < deadline {
        let (reserved, _) = siglake_storage::query_memory_pool_usage()
            .expect("the pool is bounded on any Linux host: /proc/meminfo sizes it");
        if reserved == 0 {
            return;
        }
        last = Some(reserved);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "{label}: the query memory pool still holds {} bytes 15s after the last response; \
         consumers holding them:\n{}",
        last.unwrap_or(0),
        siglake_storage::query_memory_pool_top_consumers(10)
            .unwrap_or_else(|| "(consumer tracking disabled)".to_string())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pool_returns_to_zero_after_every_bench_shape() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    overlapping_table(&ice).await;
    let app = router(AppState::new(ice, AuthConfig::open()));

    assert!(
        siglake_storage::query_memory_pool_top_consumers(1).is_some(),
        "consumer tracking is on by default; without it a failure here could not \
         name what it found"
    );
    settle_to_zero("before any query").await;

    // Shapes whose response proves a scan ran (`stats.rows_scanned > 0`). The
    // aggregate shapes are answered from manifests and footers without touching
    // the pool, which is correct and proves nothing here; the test is only
    // meaningful if the shapes that DO reserve -- the FTS needles, the LIKE,
    // the browse -- actually executed their scans.
    let _ = snapshotter.snapshot();
    let mut scanning_shapes = 0usize;
    for (shape_index, &(name, sql)) in BENCH_SHAPES.iter().enumerate() {
        for format in ["records", "ndjson"] {
            for iteration in 1..=3 {
                // Request limits are part of the result-cache key; SQL comments
                // are not. Keep the limits above every shape's result size and
                // unique across the matrix (which contains duplicate SQL) so
                // every records request executes.
                let max_rows_returned = 1_000 + shape_index * 3 + iteration;
                let (status, body) = post_sql(&app, sql, format, max_rows_returned).await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "{name}/{format} iteration {iteration}: {}",
                    String::from_utf8_lossy(&body)
                );
                if format == "records" && iteration == 1 {
                    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    if body["stats"]["rows_scanned"].as_u64().unwrap_or(0) > 0 {
                        scanning_shapes += 1;
                    }
                }
            }
            settle_to_zero(&format!("{name}/{format}")).await;
        }
    }
    let snapshot = snapshotter.snapshot().into_vec();
    let result_cache_hits = counter_sum(
        &snapshot,
        "siglake_query_sql_result_cache_requests_total",
        &[("outcome", "hit")],
    );
    assert_eq!(
        result_cache_hits, 0,
        "the bench-shape loop must execute every records request, not replay cached results"
    );
    assert!(
        scanning_shapes >= 3,
        "only {scanning_shapes} bench shapes scanned rows; the shapes that reserve pool \
         bytes did not execute, so a leak in them could not have been observed"
    );

    // A client that hangs up mid-stream: take the first NDJSON chunk of a
    // whole-table browse and drop the body. `GuardedStream` fires the query's
    // cancel flag on that drop; the scan must end and give its bytes back.
    for _ in 0..3 {
        let body = serde_json::json!({
            "query": "SELECT \"timestamp\", host, raw FROM events",
            "format": "ndjson",
        });
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
        assert_eq!(response.status(), StatusCode::OK);
        let mut chunks = response.into_body().into_data_stream();
        let first = chunks.next().await;
        assert!(
            matches!(first, Some(Ok(ref b)) if !b.is_empty()),
            "the stream must have started before the client hangs up"
        );
        drop(chunks);
    }
    settle_to_zero("abandoned ndjson stream").await;

    // A client that goes away before its records response exists: the handler
    // future is dropped mid-collect. The exec-pool slot's Drop aborts the
    // spawned collect, and the scan's cancel guard ends the source stream.
    for _ in 0..3 {
        let body = serde_json::json!({
            "query": "SELECT \"timestamp\", host, raw FROM events ORDER BY raw LIMIT 50000",
            "format": "records",
        });
        let request = app.clone().oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        );
        // Long enough for planning to finish and the scan to start reserving,
        // short enough that the sort has not completed.
        let _ = tokio::time::timeout(Duration::from_millis(30), request).await;
    }
    settle_to_zero("abandoned records request").await;
}
