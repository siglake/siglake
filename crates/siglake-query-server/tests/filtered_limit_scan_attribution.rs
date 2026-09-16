//! A filtered `LIMIT` over a multi-partition scan, through the real router:
//! the response's `stats.scan` must be complete when it is returned, no
//! request-attributable scan work may land afterwards, and a response served
//! from the decoded file-batch cache must say so.
//!
//! THE READING THIS PINS (task #274). The 2026-09-03 50G benchmark's
//! `label_filter` responses carried `rows_scanned = 106,648`, `files_planned =
//! 15` and `files_read = row_groups_read = object_store_reads = fetched_bytes =
//! 0`, while the process-level delta across the same ten runs averaged 28 GETs
//! and 124 MB. Two mechanisms produce that shape and the response could not
//! tell them apart:
//!
//!  1. Partition counters fold when the partition stream ends. An early LIMIT
//!     aborts the other partitions' pumps, and `render_records` snapshotted
//!     the plan before those aborts landed. Fixed by settling the plan in the
//!     collect path; this test checks the response against the process
//!     histograms every partition records at the same instant it folds.
//!  2. Decoded file-batch cache hits legitimately emit leaf rows with no
//!     reads. Fixed by `stats.scan.file_cache_hits` / `file_cache_misses`.
//!
//! One test function in its own binary: the metrics recorder and the scan
//! tuning it flips are process-global.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, QueryScanConfig};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

const PARTITIONS: usize = 8;
const FILES: usize = 64;
const ROWS_PER_FILE: usize = 5_000;
/// The only `host` inside the range predicate: statistics prune nothing (every
/// file spans `host-a` .. `host-z`) and bloom filters cannot serve a range, so
/// every partition has real reads to attribute.
const NEEDLE: &str = "host-mm";
const PREDICATE: &str = "host > 'host-ml' AND host < 'host-mn'";

/// File 0 is the largest and the only one carrying the needle (first rows), so
/// the unordered-limit re-split puts it first in partition 0 and the LIMIT is
/// satisfied while the other partitions are still reading.
async fn needle_table(ice: &IcebergContext) {
    for file in 0..FILES {
        let rows = if file == 0 {
            ROWS_PER_FILE + 1_000
        } else {
            ROWS_PER_FILE
        };
        let events: Vec<Event> = (0..rows)
            .map(|i| {
                let mut e = Event::now(format!("row {file}-{i} payload"));
                e.host = if file == 0 && i < 4 {
                    NEEDLE.to_string()
                } else {
                    format!("host-{}", (b'a' + (i % 26) as u8) as char)
                };
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
}

async fn post_sql(app: &Router, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or_else(
        |_| serde_json::json!({ "raw_body": String::from_utf8_lossy(&bytes).to_string() }),
    );
    (status, body)
}

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

/// One materialized snapshot. `Snapshotter::snapshot()` DRAINS everything it
/// reads — it clears histogram samples and `swap(0)`s counters and gauges — so
/// each call reports what was recorded since the previous call, which is
/// exactly what the checkpoints below want. A gauge therefore reads as the
/// last value SET since the previous snapshot, and 0 if nothing set it.
fn snapshot(snap: &Snapshotter) -> SnapshotVec {
    snap.snapshot().into_vec()
}

/// `(count, sum)` of the per-partition histogram `name` in `snapshot`: one
/// sample per partition, recorded in the same `finish` that folds the
/// partition's counters into the plan.
fn partition_histogram(snapshot: &SnapshotVec, name: &str) -> (usize, u64) {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == name)
        .map(|(_, _, _, value)| match value {
            DebugValue::Histogram(samples) => (
                samples.len(),
                samples.iter().map(|s| s.into_inner() as u64).sum::<u64>(),
            ),
            _ => (0, 0),
        })
        .fold((0, 0), |(c, s), (dc, ds)| (c + dc, s + ds))
}

fn counter(snapshot: &SnapshotVec, name: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == name)
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

fn counter_outcome(snapshot: &SnapshotVec, name: &str, outcome: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == outcome)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

fn gauge(snapshot: &SnapshotVec, name: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == name)
        .map(|(_, _, _, value)| match value {
            DebugValue::Gauge(g) => g.into_inner() as u64,
            _ => 0,
        })
        .sum()
}

const FETCHED: &str = "siglake_query_scan_partition_fetched_bytes";
const FILE_CACHE_REQUESTS: &str = "siglake_query_scan_file_cache_requests_total";

/// How many full scans the cache may take to cover every file before the
/// best-effort populate path counts as losing ground. See the warm loop.
const WARM_PASSES_MAX: usize = 8;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_limit_response_carries_complete_and_self_describing_scan_stats() {
    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    needle_table(&ice).await;
    let app = router(
        AppState::new(ice.clone(), AuthConfig::open()).with_query_scan(QueryScanConfig {
            target_partitions: Some(PARTITIONS),
            ..Default::default()
        }),
    );
    // `default_order: false`: the interactive default injects `ORDER BY
    // timestamp DESC`, which turns a filtered LIMIT into a TopK over a full
    // scan — correct, but it never early-stops, so it cannot show the window.
    // `max_rows_returned` is part of the result-cache key (SQL comments are
    // normalised away), so a distinct value per request keeps every request
    // a real execution.
    let request = |max_rows_returned: usize| {
        serde_json::json!({
            "query": format!("SELECT host, raw FROM events WHERE {PREDICATE} LIMIT 1"),
            "default_order": false,
            "limits": { "max_rows_returned": max_rows_returned },
        })
    };

    // --- 1. Cold: every read is a real read, and all of them are in the
    // response before it is returned.
    let _ = snapshot(&snap); // drain whatever the fixture recorded
    let (status, body) = post_sql(&app, request(1_000)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let at_response = snapshot(&snap);
    let (partitions_folded, bytes_folded) = partition_histogram(&at_response, FETCHED);
    assert_eq!(body["row_count"], 1, "{body}");
    assert_eq!(body["rows"][0]["host"], NEEDLE, "{body}");
    let stats = &body["stats"];
    let scan = &stats["scan"];
    eprintln!("COLD stats={stats}");
    assert_eq!(scan["files_planned"], FILES as u64, "{scan}");
    assert!(
        stats["rows_scanned"].as_u64().unwrap() >= 1,
        "the scan emitted the matching row: {stats}"
    );
    assert!(
        scan["files_read"].as_u64().unwrap() >= 2,
        "a multi-partition early-stopped scan opens more than the matching file, \
         and the response must carry those opens: {scan}"
    );
    assert!(scan["object_store_reads"].as_u64().unwrap() >= 2, "{scan}");
    assert!(
        scan.get("unsettled_partitions").is_none(),
        "the response was rendered with partitions still unwinding: {scan}"
    );
    assert!(
        scan.get("file_cache_hits").is_none() && scan.get("file_cache_misses").is_none(),
        "the decoded cache is off; nothing may claim a hit or miss: {scan}"
    );
    // Every partition that ran has folded: the response's fetched bytes equal
    // the sum of what the partitions recorded, and the partitions of the
    // multi-partition scan did so before the response existed.
    eprintln!("COLD partitions_folded={partitions_folded} bytes_folded={bytes_folded}");
    assert!(
        partitions_folded >= 2,
        "a multi-partition scan must have folded several partitions before the \
         response returned, got {partitions_folded}"
    );
    assert_eq!(
        scan["fetched_bytes"].as_u64().unwrap(),
        bytes_folded,
        "stats.scan.fetched_bytes must equal the bytes every finished partition \
         recorded: {scan}"
    );
    assert_eq!(scan["fetched_bytes"], stats["bytes_scanned"], "{stats}");

    // --- 2. Nothing request-attributable lands after the response.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let later = snapshot(&snap);
    assert_eq!(
        partition_histogram(&later, FETCHED),
        (0, 0),
        "a scan partition finished (and recorded reads) after the response was returned"
    );
    assert_eq!(
        counter(&later, "siglake_query_scan_attribution_incomplete_total"),
        0,
        "the settle wait hit its deadline"
    );

    // --- 3. Served from the decoded file-batch cache: zero reads, and the
    // response says why.
    siglake_storage::configure_query_scan_tuning(siglake_storage::QueryScanTuning {
        file_cache_max_bytes: Some(256 << 20),
        file_cache_max_entries: Some(4096),
        ..Default::default()
    });
    // Warm: a scan that consumes every file (the cache is populated when a
    // task drains to its end) with the SAME projection as the browse — the
    // cache key carries the projected field ids, so a `raw`-only warm-up
    // would fill entries the `host, raw` browse never looks up. Through the
    // same eight-partition router; a distinct limit per pass keeps every pass
    // out of the result cache and a real execution.
    //
    // WHY A LOOP AND NOT A FIXED TWO PASSES. The populate is best-effort:
    // `CachePopulateStream` takes the cache mutex with `try_lock` and skips
    // the insert when it is contended ("DEGRADE, DO NOT SPIN" in
    // `query_provider.rs`), because the entry is rebuildable by the next
    // reader. Eight partitions of one scan finish within microseconds of each
    // other, so *which* of them collide is pure scheduling — a single pass can
    // leave anywhere from zero to a couple of dozen files uncached, and a
    // fixed skipped/inserts ratio measures the runtime's luck, not the code:
    // it failed CI on 2026-09-11 at skipped=17 inserts=61, and over 25 idle
    // local runs the skip count ranged 1..17 against a threshold of 16, so one
    // of the 25 failed. The property that holds is that the
    // skips are transient: each one costs one future miss, and the reader that
    // takes that miss re-inserts. So scan until the cache covers every file,
    // and bound the number of scans — a populate path that lost ground instead
    // of converging would never reach coverage.
    let mut passes = 0usize;
    let mut cached_files = 0u64;
    let (mut inserts, mut skipped) = (0, 0);
    while cached_files < FILES as u64 && passes < WARM_PASSES_MAX {
        let (status, body) = post_sql(
            &app,
            serde_json::json!({
                "query": "SELECT sum(length(host) + length(raw)) AS n FROM events",
                "limits": { "max_rows_returned": 2_000 + passes },
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(
            body["stats"]["rows_scanned"].as_u64().unwrap() > 0,
            "warm pass {passes} must scan: {body}"
        );
        passes += 1;
        // Every snapshot is a delta (see `snapshot`), so the counters accumulate
        // by hand. The entries gauge is SET on each insert, so the pass's read
        // is the cache size as of its last insert — 0 only if it inserted
        // nothing, in which case the loop is not done anyway.
        let warm_metrics = snapshot(&snap);
        inserts += counter_outcome(&warm_metrics, FILE_CACHE_REQUESTS, "insert");
        skipped += counter_outcome(
            &warm_metrics,
            FILE_CACHE_REQUESTS,
            "insert_skipped_contended",
        );
        cached_files = gauge(&warm_metrics, "siglake_query_scan_file_cache_entries");
        eprintln!(
            "CACHE WARM pass={passes} entries={cached_files} inserts={inserts} skipped={skipped}"
        );
    }
    assert!(inserts > 0, "parallel warming must populate the cache");
    assert_eq!(
        cached_files, FILES as u64,
        "repeated full scans must converge on a cache entry per file: after {passes} passes \
         the cache holds {cached_files} of {FILES} (inserts={inserts}, \
         contended skips={skipped})"
    );
    let (status, body) = post_sql(&app, request(1_001)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["row_count"], 1, "{body}");
    assert_eq!(body["rows"][0]["host"], NEEDLE, "{body}");
    let stats = &body["stats"];
    let scan = &stats["scan"];
    eprintln!("WARM stats={stats}");
    let hits = scan["file_cache_hits"].as_u64().unwrap_or(0);
    let misses = scan["file_cache_misses"].as_u64().unwrap_or(0);
    assert!(
        stats["rows_scanned"].as_u64().unwrap() >= 1,
        "cached batches still count as leaf rows: {stats}"
    );
    let cache_outcomes: Vec<String> = snapshot(&snap)
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == "siglake_query_scan_file_cache_requests_total")
        .map(|(key, _, _, value)| format!("{:?}={value:?}", key.key().labels().collect::<Vec<_>>()))
        .collect();
    assert!(
        hits >= 1,
        "the warmed scan must be served from the decoded cache: {scan}; process cache \
         outcomes so far: {cache_outcomes:?}"
    );
    // The self-describing invariant: every file the request opened was a
    // cache miss, so `files_read = 0` alongside hits is a cache answer, not a
    // partial fold.
    assert_eq!(
        scan["files_read"].as_u64().unwrap(),
        misses,
        "files_read must equal the tasks that missed the cache: {scan}"
    );
    if misses == 0 {
        assert_eq!(scan["fetched_bytes"], 0, "{scan}");
        assert_eq!(scan["object_store_reads"], 0, "{scan}");
    }
    assert!(scan.get("unsettled_partitions").is_none(), "{scan}");
}
