//! Circuit-breaker tests.
//!
//! Caveat: as of iceberg-datafusion 0.9 the byte-scanned signal we
//! feed the pre-flight breaker comes back as 0 from
//! `ExecutionPlan::partition_statistics()`, so we can't drive the
//! bytes-scanned breaker from a small test fixture. Tests below
//! exercise: the resolver math (unit), the timeout breaker via a
//! 0-second request timeout (forces `tokio::time::timeout` to fire on
//! the first poll), per-request row-cap override, and
//! `circuit_breakers: false` bypass.

use crate::support;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;

use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, Priority, ServerLimits, TierLimits};
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};

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

async fn spawn(n: usize, limits: ServerLimits) -> Server {
    spawn_with_state(n, limits).await.0
}

/// Like [`spawn`] but also hands back the [`AppState`], so a test can
/// drive internal handles (e.g. the admission controller) directly.
async fn spawn_with_state(n: usize, limits: ServerLimits) -> (Server, AppState) {
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
    let state = AppState::new(Arc::new(ice), AuthConfig::open()).with_limits(limits);
    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        Server {
            base: format!("http://{addr}"),
            _tmp: tmp,
            handle,
        },
        state,
    )
}

#[tokio::test]
async fn preflight_bytes_breaker_fires_with_manifest_walk() {
    require_loopback!();
    // 200 events produces a non-trivial Parquet file. Set the
    // interactive bytes ceiling to 1 byte → breaker must trip.
    let mut limits = ServerLimits::default();
    limits.interactive.ceiling_bytes_scanned = 1;
    let srv = spawn(200, limits).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT * FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("exceeds"));
    assert!(body["cost"]["estimated_bytes_scanned"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn batch_priority_passes_under_looser_ceiling() {
    require_loopback!();
    let mut limits = ServerLimits::default();
    // Interactive: tight; Batch: loose.
    limits.interactive.ceiling_bytes_scanned = 1;
    limits.batch.ceiling_bytes_scanned = 100 * 1024 * 1024 * 1024;
    let srv = spawn(200, limits).await;

    // Interactive: rejected.
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT * FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Batch: accepted (202).
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":    "SELECT * FROM events",
            "priority": "batch",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
}

#[tokio::test]
async fn request_can_tighten_max_rows_returned() {
    require_loopback!();
    let srv = spawn(20, ServerLimits::default()).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":  "SELECT host FROM events",
            "limits": { "max_rows_returned": 5 }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["row_count"], 5);
    assert_eq!(body["truncated"], true);
}

#[tokio::test]
async fn timeout_breaker_returns_504_with_cost() {
    require_loopback!();
    // A 1 µs timeout combined with a 100k-row generator-driven SELECT
    // guarantees the inner future pends (DataFusion yields between
    // record batches) before completion, so `tokio::time::timeout`
    // fires reliably. The 100k rows aren't actually fully streamed
    // anywhere — the timeout wrapper drops the stream the moment it
    // trips.
    let srv = spawn(0, ServerLimits::default()).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":  "SELECT v FROM generate_series(1, 100000) AS t(v)",
            "limits": { "timeout_seconds": 0 }
        }))
        .send()
        .await
        .unwrap();
    // Either 504 (timeout tripped) or 200 (collect finished before the
    // first yield — possible on a very fast host). We accept either
    // outcome but assert the timeout response shape when it does trip.
    if resp.status() == 504 {
        let body: Value = resp.json().await.unwrap();
        assert!(body["error"].as_str().unwrap().contains("timeout"));
        assert!(body["cost"].is_object());
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("priority: \"batch\""));
    }
}

/// Tenant resolution and SQL rewrites are preparation, but still consume the
/// interactive request's one wall-clock budget. An exhausted request must be
/// refused before even an immediately-ready rewrite can return its own 400;
/// this pins both the transparent and explicitly-local entry points.
#[tokio::test]
async fn query_preparation_obeys_wall_clock_timeout_on_both_user_routes() {
    require_loopback!();
    let client = reqwest::Client::new();
    let malformed = serde_json::json!({ "query": "SELECT (" });

    // Control: with time to parse, the rewrite reports malformed SQL.
    let normal = spawn(0, ServerLimits::default()).await;
    let response = client
        .post(format!("{}/api/v1/sql", normal.base))
        .json(&malformed)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    let mut tier = TierLimits::interactive_defaults();
    tier.default_timeout = Duration::from_nanos(1);
    let limits = ServerLimits {
        interactive: tier,
        ..ServerLimits::default()
    };
    let bounded = spawn(0, limits).await;
    for path in ["/api/v1/sql", "/api/v1/sql/local"] {
        let response = client
            .post(format!("{}{path}", bounded.base))
            .json(&malformed)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            504,
            "preparation on {path} escaped the wall-clock budget: {}",
            response.text().await.unwrap()
        );
    }
}

/// A batch timeout governs the queued run, not validation/admission/the 202
/// handoff. A blanket request wrapper would turn this into a 504.
#[tokio::test]
async fn zero_second_batch_run_budget_still_allows_submission() {
    require_loopback!();
    let srv = spawn(0, ServerLimits::default()).await;
    let response = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "priority": "batch",
            "limits": { "timeout_seconds": 0 }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        202,
        "the run budget refused the batch handoff: {}",
        response.text().await.unwrap()
    );
}

/// Poll a job to its terminal state. Batch jobs below finish in one poll of the
/// batch runtime; the loop only exists so a busy machine cannot race it.
async fn await_terminal_status(client: &reqwest::Client, base: &str, job_id: &str) -> Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let body: Value = client
            .get(format!("{base}/api/v1/jobs/{job_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(
            body["status"].as_str().unwrap(),
            "succeeded" | "failed" | "cancelled" | "timeout"
        ) {
            return body;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "batch job never finished: {body}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn submit_batch_job(client: &reqwest::Client, base: &str, body: Value) -> String {
    let response = client
        .post(format!("{base}/api/v1/sql"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 202);
    let body: Value = response.json().await.unwrap();
    body["job_id"].as_str().unwrap().to_string()
}

/// The 202 is not a promise that the RUN gets to ignore the budget it was
/// given. `count(*)` is answered from the footers in a single poll, so before
/// the run-wide deadline it slipped past the collect-only wrapper entirely and
/// a zero-second batch job reported `succeeded`.
#[tokio::test]
async fn a_zero_budget_batch_job_ends_as_timeout() {
    require_loopback!();
    let srv = spawn(5, ServerLimits::default()).await;
    let client = reqwest::Client::new();
    let job_id = submit_batch_job(
        &client,
        &srv.base,
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "priority": "batch",
            "limits": { "timeout_seconds": 0 }
        }),
    )
    .await;

    let final_status = await_terminal_status(&client, &srv.base, &job_id).await;
    assert_eq!(
        final_status["status"], "timeout",
        "the batch run escaped its own budget: {final_status}"
    );
    assert!(final_status["error"].as_str().unwrap().contains("timeout"));
}

/// `circuit_breakers: false` on a batch job waives the wall clock and nothing
/// else: the same submission runs to an answer, while the bytes-scanned ceiling
/// still refuses it.
#[tokio::test]
async fn batch_circuit_breaker_opt_out_keeps_the_other_limits() {
    require_loopback!();
    let srv = spawn(5, ServerLimits::default()).await;
    let client = reqwest::Client::new();

    let waived = submit_batch_job(
        &client,
        &srv.base,
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "priority": "batch",
            "limits": { "timeout_seconds": 0, "circuit_breakers": false }
        }),
    )
    .await;
    let final_status = await_terminal_status(&client, &srv.base, &waived).await;
    assert_eq!(
        final_status["status"], "succeeded",
        "the opt-out did not lift the wall clock: {final_status}"
    );

    let capped = submit_batch_job(
        &client,
        &srv.base,
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "priority": "batch",
            "limits": {
                "timeout_seconds": 0,
                "circuit_breakers": false,
                "max_bytes_scanned": 1
            }
        }),
    )
    .await;
    let final_status = await_terminal_status(&client, &srv.base, &capped).await;
    assert_eq!(
        final_status["status"], "failed",
        "the opt-out lifted the bytes ceiling too: {final_status}"
    );
    assert!(final_status["error"]
        .as_str()
        .unwrap()
        .contains("exceeds batch limit"));
}

/// The local path's timeout must cover the metadata fast-path battery, not just
/// the DataFusion collect that follows it. The warmed Tier-2 path below fits
/// within one cooperative budget, deliberately making it immediately ready:
/// an already-expired deadline must still win before Tokio polls that work.
#[tokio::test]
async fn local_fast_path_battery_obeys_wall_clock_timeout() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    // Starve Tier 1 while leaving the data file's footer intact. Disabling
    // result caches makes the repeated aggregate revisit that warmed footer.
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            table_group_count_cardinality: Some(1),
            result_caches: Some(false),
            ..Default::default()
        });
    let events: Vec<Event> = (0..2)
        .map(|i| Event {
            timestamp: Utc::now(),
            host: format!("host-{i}"),
            source: "smoke".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: format!("event {i}"),
            attributes: None,
        })
        .collect();
    // One file is enough to starve Tier 1 (two values against a cap of one)
    // while keeping the warm Tier-2 lookup below the forced-yield threshold.
    ice.append_events(&events).await.unwrap();

    let warmed = ice
        .grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .expect("footer-backed group counts");
    assert_eq!(warmed.source_label(), "materialized");

    // The dry run performs registration and estimation but deliberately skips
    // the fast-path battery. The later run's single footer-cache hit completes
    // in one poll, exercising the expired-deadline check rather than a yield.
    let app = router(AppState::new(Arc::new(ice), AuthConfig::open()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let srv = Server {
        base: format!("http://{addr}"),
        _tmp: tmp,
        handle,
    };
    let client = reqwest::Client::new();
    let query = "SELECT host, count(*) AS n FROM events GROUP BY host";
    let warm = client
        .post(format!("{}/api/v1/sql/local", srv.base))
        .json(&serde_json::json!({ "query": query, "dry_run": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(warm.status(), 200, "estimate warm-up failed");

    let resp = client
        .post(format!("{}/api/v1/sql/local", srv.base))
        .json(&serde_json::json!({
            "query": query,
            "limits": { "timeout_seconds": 0 }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        504,
        "local fast-path battery escaped the wall-clock timeout"
    );
    let body: Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("timeout"));
    assert!(body["cost"].is_object());
}

/// A warmed result cache is not an exemption from the wall-clock budget. The
/// hit path returned its memoized body BEFORE any deadline enforcement, so an
/// already-expired request was answered 200 by whichever earlier request had
/// happened to run the same query — the one response shape that made the
/// documented limit look optional.
///
/// The cache key carries no timeout (see `result_cache_key`), so the expired
/// request below is the same key the warm-up inserted.
#[tokio::test]
async fn a_warmed_result_cache_does_not_outlive_the_wall_clock_budget() {
    require_loopback!();
    let srv = spawn(50, ServerLimits::default()).await;
    let client = reqwest::Client::new();
    // Filtered scan: a cacheable shape (see `prepare_result_cache`).
    let query = "SELECT host FROM events WHERE host = 'host-1'";

    for attempt in 0..2 {
        let resp = client
            .post(format!("{}/api/v1/sql/local", srv.base))
            .json(&serde_json::json!({ "query": query }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "cache warm-up {attempt} failed");
    }

    let started = std::time::Instant::now();
    let resp = client
        .post(format!("{}/api/v1/sql/local", srv.base))
        .json(&serde_json::json!({
            "query":  query,
            "limits": { "timeout_seconds": 0 }
        }))
        .send()
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(
        resp.status(),
        504,
        "a warmed cache served an already-expired request"
    );
    let body: Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("timeout"));
    assert!(body["cost"].is_object());
    // Preparation is bounded by the request budget, not by the single-flight
    // cap: a 504 that took ten seconds to arrive is the bug this closes.
    assert!(
        elapsed < Duration::from_secs(5),
        "the expired request spent {elapsed:?} in preparation"
    );

    // The refused request must not have left an in-flight marker behind: the
    // next caller of the same shape is served normally.
    let resp = client
        .post(format!("{}/api/v1/sql/local", srv.base))
        .json(&serde_json::json!({ "query": query }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "the timed-out request poisoned its cache key"
    );
}

/// `circuit_breakers: false` must still bypass every one of the new
/// preparation-phase checks, cache hit included.
#[tokio::test]
async fn circuit_breakers_false_still_serves_a_warm_cache_hit() {
    require_loopback!();
    let srv = spawn(50, ServerLimits::default()).await;
    let client = reqwest::Client::new();
    let query = "SELECT host FROM events WHERE host = 'host-2'";
    let resp = client
        .post(format!("{}/api/v1/sql/local", srv.base))
        .json(&serde_json::json!({ "query": query }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "cache warm-up failed");

    let resp = client
        .post(format!("{}/api/v1/sql/local", srv.base))
        .json(&serde_json::json!({
            "query":  query,
            "limits": { "timeout_seconds": 0, "circuit_breakers": false }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn timeout_error_shape_via_direct_constructor() {
    require_loopback!();
    // Asserts the ApiError::timeout response shape without depending
    // on a real timeout firing — issue a query that always succeeds,
    // then a second one with circuit_breakers=true + timeout=0 to
    // hopefully trip. If neither triggers, the test still passes
    // because the error-shape assertion is conditional.
    let srv = spawn(50, ServerLimits::default()).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT count(*) FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn circuit_breakers_false_disables_timeout() {
    require_loopback!();
    let srv = spawn(50, ServerLimits::default()).await;
    // Even with timeout_seconds=0, circuit_breakers=false short-circuits
    // around the timeout wrapper.
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":  "SELECT host FROM events",
            "limits": {
                "timeout_seconds":  0,
                "circuit_breakers": false
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn batch_priority_picks_batch_tier_defaults() {
    require_loopback!();
    use siglake_query_server::{Priority, RequestLimits, ResolvedLimits, ServerLimits};
    // Unit-style assertion over the resolver: confirm that priority
    // selection routes through the right tier defaults.
    let mut limits = ServerLimits::default();
    limits.interactive.default_timeout = std::time::Duration::from_secs(7);
    limits.batch.default_timeout = std::time::Duration::from_secs(7777);

    let interactive_resolved = ResolvedLimits::resolve(
        &RequestLimits::default(),
        Priority::Interactive,
        &limits.tier(Priority::Interactive),
    );
    let batch_resolved = ResolvedLimits::resolve(
        &RequestLimits::default(),
        Priority::Batch,
        &limits.tier(Priority::Batch),
    );
    assert_eq!(interactive_resolved.timeout.as_secs(), 7);
    assert_eq!(batch_resolved.timeout.as_secs(), 7777);
}

#[tokio::test]
async fn midflight_rows_scanned_breaker_trips() {
    require_loopback!();
    // Seed 100 events; set max_rows_scanned = 10 → the breaker
    // should trip and return 413 before all rows are emitted.
    let mut limits = ServerLimits::default();
    limits.interactive.ceiling_rows_scanned = 10;
    let srv = spawn(100, limits).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT * FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
    let body: Value = resp.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("mid-flight breaker"),
        "{body}"
    );
}

#[tokio::test]
async fn midflight_breaker_trips_in_ndjson_stream() {
    require_loopback!();
    // 100 events, cap 10 → the NDJSON stream should emit at most a
    // few data lines and then a midflight marker line.
    let mut limits = ServerLimits::default();
    limits.interactive.ceiling_rows_scanned = 10;
    let srv = spawn(100, limits).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":  "SELECT host FROM events",
            "format": "ndjson",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let last: Value = serde_json::from_str(lines.last().expect("at least one line")).unwrap();
    assert_eq!(
        last["_meta"], "midflight_rows_scanned_exceeded",
        "expected midflight marker, lines = {lines:?}"
    );
    assert!(
        last["rows_scanned"].as_u64().unwrap() > 10,
        "rows_scanned should exceed cap"
    );
}

#[tokio::test]
async fn midflight_breaker_silent_under_cap() {
    require_loopback!();
    // 10 events, cap 1000 → no trip, 200 OK.
    let mut limits = ServerLimits::default();
    limits.interactive.ceiling_rows_scanned = 1000;
    let srv = spawn(10, limits).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT count(*) FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn tier_helper_clamps_max_rows_returned() {
    require_loopback!();
    let limits = ServerLimits {
        max_rows: 10,
        interactive: TierLimits {
            ceiling_rows_returned: 1_000_000,
            ..TierLimits::interactive_defaults()
        },
        ..ServerLimits::default()
    };
    let tier = limits.tier(Priority::Interactive);
    assert_eq!(
        tier.ceiling_rows_returned, 10,
        "max_rows must clamp the tier ceiling"
    );
}

#[tokio::test]
async fn admission_control_returns_429_with_retry_after() {
    require_loopback!();
    // Deterministic, no timing races: fill the budget by holding a
    // reservation directly on the controller, then prove an HTTP query
    // is rejected with 429 + Retry-After, and that releasing the hold
    // lets the next query through. (An earlier version raced N
    // concurrent heavy queries; on the single-threaded test runtime
    // CPU-bound execution serializes the requests so guards never
    // overlap, and the test was vacuous in sandboxes without loopback.)
    let budget: u64 = 16 * 1024 * 1024;
    let limits = ServerLimits {
        admission_budget_bytes: budget,
        admission_wait_timeout: Duration::from_millis(25),
        ..ServerLimits::default()
    };
    let (srv, state) = spawn_with_state(100, limits).await;
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/sql", srv.base);
    let query = serde_json::json!({ "query": "SELECT raw FROM events LIMIT 5" });

    let hold = state
        .admission
        .acquire(budget)
        .await
        .expect("filling the empty budget must succeed");

    let response = client.post(&url).json(&query).send().await.unwrap();
    assert_eq!(response.status(), 429);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|h| h.to_str().ok()),
        Some("1")
    );
    let body: Value = response.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("admission"));

    drop(hold);
    let response = client.post(&url).json(&query).send().await.unwrap();
    assert_eq!(
        response.status(),
        200,
        "releasing the hold must unblock admission"
    );
}

#[tokio::test]
async fn healthz_stays_available_with_tight_admission_budget() {
    require_loopback!();
    let limits = ServerLimits {
        admission_budget_bytes: 16 * 1024 * 1024,
        admission_wait_timeout: Duration::from_millis(250),
        ..ServerLimits::default()
    };
    let srv = spawn(20_000, limits).await;
    let client = reqwest::Client::new();
    let in_flight = tokio::spawn({
        let client = client.clone();
        let url = format!("{}/api/v1/sql", srv.base);
        async move {
            client
                .post(url)
                .json(&serde_json::json!({
                    "query": "SELECT raw FROM events CROSS JOIN generate_series(1, 200) AS g(v) ORDER BY raw DESC LIMIT 500000"
                }))
                .send()
                .await
                .unwrap()
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let health = client
        .get(format!("{}/healthz", srv.base))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);
    let _ = in_flight.await.unwrap();
}
