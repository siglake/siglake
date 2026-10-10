//! End-to-end tests: spin up an Iceberg-backed [`AppState`], wrap it
//! with the production [`router`], drive over HTTP via reqwest, assert
//! the response shape.
//!
//! Each test gets its own tempdir-backed warehouse so they run in
//! parallel without colliding.

use crate::support;

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, TimeZone, Utc};

use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, ServerLimits};
use siglake_storage::iceberg::IcebergContext;

macro_rules! require_loopback {
    () => {
        if !support::loopback_available() {
            return;
        }
    };
}

/// Boilerplate: create a fresh warehouse, seed it with `n` events,
/// build an [`AppState`] with the supplied auth, mount the router on
/// a random port, and return the base URL + a handle to abort on drop.
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

async fn spawn_with(n: usize, auth: AuthConfig) -> Server {
    spawn_full(n, auth, ServerLimits::default()).await
}

async fn spawn_full(n: usize, auth: AuthConfig, limits: ServerLimits) -> Server {
    let events: Vec<Event> = (0..n).map(synth_event).collect();
    spawn_with_events(events, auth, limits).await
}

async fn spawn_with_events(events: Vec<Event>, auth: AuthConfig, limits: ServerLimits) -> Server {
    spawn_with_event_batches(vec![events], auth, limits).await
}

async fn spawn_with_event_batches(
    batches: Vec<Vec<Event>>,
    auth: AuthConfig,
    limits: ServerLimits,
) -> Server {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    for events in batches {
        if !events.is_empty() {
            ice.append_events(&events).await.unwrap();
        }
    }
    let state = AppState::new(Arc::new(ice), auth).with_limits(limits);
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

async fn spawn(n: usize) -> Server {
    spawn_with(n, AuthConfig::open()).await
}

fn synth_event(i: usize) -> Event {
    Event {
        timestamp: Utc::now(),
        host: format!("host-{}", i % 4),
        source: "/var/log/app.log".into(),
        sourcetype: "app:json".into(),
        index: "main".into(),
        raw: format!("event {i} status={}", i % 5),
        attributes: None,
    }
}

#[tokio::test]
async fn healthz_returns_ok() {
    require_loopback!();
    let srv = spawn(0).await;
    let resp = reqwest::get(format!("{}/healthz", srv.base)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn readyz_returns_ok_when_catalog_is_up() {
    require_loopback!();
    let srv = spawn(0).await;
    let resp = reqwest::get(format!("{}/readyz", srv.base)).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ready");
}

#[tokio::test]
async fn sql_count_via_records() {
    require_loopback!();
    let srv = spawn(10).await;
    let body = serde_json::json!({ "query": "SELECT count(*) AS n FROM events" });
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["row_count"], 1);
    assert_eq!(body["columns"], serde_json::json!(["n"]));
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["n"], 10);
}

/// The fast paths apply their extracted half-open window to `timestamp_ns`,
/// while SQL evaluates the public `timestamp` at microsecond precision. Compare
/// every accelerated shape with the same predicate plus a non-null dimensional
/// clause, which deliberately forces ordinary DataFusion execution. The mix of
/// committed and WAL rows proves both sides of the fast-path merge.
#[tokio::test]
async fn windowed_fast_paths_preserve_microsecond_sql_predicates() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let nanos = [
        -2_500, -1_877, -1_001, -1_000, -999, -1, 0, 123, 999, 1_000, 1_123, 1_999,
    ];
    let events: Vec<Event> = nanos
        .iter()
        .enumerate()
        .map(|(i, nanos)| Event {
            timestamp: Utc.timestamp_nanos(*nanos),
            host: format!("h{}", i % 2),
            source: "precision-test".into(),
            sourcetype: "test".into(),
            index: "main".into(),
            raw: format!("row {nanos}"),
            attributes: None,
        })
        .collect();
    let committed: Vec<_> = events.iter().step_by(2).cloned().collect();
    let buffered: Vec<_> = events.iter().skip(1).step_by(2).cloned().collect();
    ice.append_events(&committed).await.unwrap();

    let wal_root = tmp.path().join("wal");
    let mut writer = siglake_wal::WalWriter::with_thresholds(
        &wal_root,
        "precision-test",
        10_000,
        std::time::Duration::from_secs(60),
    )
    .unwrap();
    writer.append_events(&buffered).unwrap();
    writer.seal().unwrap().expect("sealed precision segment");
    drop(writer);

    let state = AppState::new(ice, AuthConfig::open()).with_wal_buffer_dir(Some(wal_root));
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let predicates = [
        "timestamp > to_timestamp_micros(-2) AND timestamp < to_timestamp_micros(2)",
        "timestamp >= to_timestamp_micros(-1) AND timestamp < to_timestamp_micros(2)",
        "timestamp >= to_timestamp_micros(-2) AND timestamp < to_timestamp_micros(1)",
        "timestamp >= to_timestamp_micros(-2) AND timestamp <= to_timestamp_micros(0)",
        "timestamp BETWEEN to_timestamp_micros(-1) AND to_timestamp_micros(0)",
        "timestamp >= to_timestamp_nanos(123) AND timestamp <= to_timestamp_nanos(999)",
        "timestamp >= to_timestamp_nanos(-1877) AND timestamp <= to_timestamp_nanos(-1)",
    ];
    for predicate in predicates {
        for select in [
            "count(*) AS n".to_string(),
            "host, count(*) AS n".to_string(),
        ] {
            let group_by = if select.starts_with("host") {
                " GROUP BY host ORDER BY host"
            } else {
                ""
            };
            let accelerated = format!("SELECT {select} FROM events WHERE {predicate}{group_by}");
            let ordinary = format!(
                "SELECT {select} FROM events WHERE ({predicate}) AND host IS NOT NULL{group_by}"
            );
            let run = |query: String| {
                let client = client.clone();
                let url = format!("{base}/api/v1/sql");
                async move {
                    let response = client
                        .post(url)
                        .json(&serde_json::json!({ "query": query, "default_order": false }))
                        .send()
                        .await
                        .unwrap();
                    let status = response.status();
                    let text = response.text().await.unwrap();
                    assert_eq!(status, 200, "{text}");
                    serde_json::from_str::<serde_json::Value>(&text).unwrap()
                }
            };
            let fast = run(accelerated.clone()).await;
            let baseline = run(ordinary).await;
            assert_eq!(fast["rows"], baseline["rows"], "{accelerated}: {fast}");
            assert_eq!(
                fast["stats"]["rows_scanned"].as_u64(),
                Some(0),
                "query did not exercise a metadata fast path: {accelerated}: {fast}"
            );
        }
    }
    handle.abort();
}

#[tokio::test]
async fn sql_cte_query_returns_rows() {
    require_loopback!();
    let srv = spawn(12).await;
    let body = serde_json::json!({
        "query": "WITH matched AS (\
            SELECT * FROM events WHERE raw LIKE '%status=3%'\
        ) SELECT host, count(*) AS n FROM matched GROUP BY host ORDER BY host"
    });
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["columns"], serde_json::json!(["host", "n"]));
    assert_eq!(
        body["rows"],
        serde_json::json!([
            { "host": "host-0", "n": 1 },
            { "host": "host-3", "n": 1 }
        ])
    );
}

#[tokio::test]
async fn search_convenience_matches_match_terms_over_events_raw() {
    require_loopback!();
    let srv = spawn(12).await;
    let client = reqwest::Client::new();

    let search = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE search('status 3')"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(search.status(), 200);
    let search_body: serde_json::Value = search.json().await.unwrap();

    let explicit = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'status 3')"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(explicit.status(), 200);
    let explicit_body: serde_json::Value = explicit.json().await.unwrap();

    assert_eq!(search_body["rows"], explicit_body["rows"]);
}

#[tokio::test]
async fn sql_count_via_ndjson() {
    require_loopback!();
    let srv = spawn(10).await;
    let body = serde_json::json!({
        "query": "SELECT count(*) AS n FROM events",
        "format": "ndjson",
    });
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 1);
    let row: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(row["n"], 10);
}

async fn post_count(
    server: &Server,
    route: &str,
    format: Option<&str>,
    shard: Option<(usize, usize)>,
    query: &str,
) -> (u64, serde_json::Value) {
    let mut request = serde_json::json!({ "query": query });
    if let Some(format) = format {
        request["format"] = serde_json::json!(format);
    }
    if let Some((index, count)) = shard {
        request["shard"] = serde_json::json!({ "index": index, "count": count });
    }
    let response = reqwest::Client::new()
        .post(format!("{}{route}", server.base))
        .json(&request)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status, 200, "{route} {request}: {text}");

    if format == Some("ndjson") {
        let lines: Vec<_> = text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();
        assert_eq!(lines.len(), 1, "{route} {request}: {text}");
        let row: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let count = row["n"]
            .as_u64()
            .unwrap_or_else(|| panic!("no count in {row}"));
        (count, row)
    } else {
        let body: serde_json::Value = serde_json::from_str(&text).unwrap();
        let count = body["rows"][0]["n"]
            .as_u64()
            .unwrap_or_else(|| panic!("no count in {body}"));
        (count, body)
    }
}

/// A shard selector partitions files, while the whole-count Tier-1 answer is
/// the manifest total across every file. Both public single-pod entry points
/// and both response renderers must decline that metadata answer for a shard.
#[tokio::test]
async fn sharded_whole_table_counts_match_scans_on_public_routes_and_formats() {
    require_loopback!();
    let mut next = 0;
    let mut batches = Vec::new();
    for rows in 1..=6 {
        batches.push((next..next + rows).map(synth_event).collect());
        next += rows;
    }
    let total_rows = next as u64;
    let server =
        spawn_with_event_batches(batches, AuthConfig::open(), ServerLimits::default()).await;
    let shard_count = 2;

    for route in ["/api/v1/sql", "/api/v1/sql/local"] {
        for format in [None, Some("ndjson")] {
            let mut sum = 0;
            for index in 0..shard_count {
                // count(raw) is deliberately outside the whole-count fast
                // path, so it supplies the scanned answer for this file slice.
                let (scanned, scanned_body) = post_count(
                    &server,
                    route,
                    format,
                    Some((index, shard_count)),
                    "SELECT count(raw) AS n FROM events",
                )
                .await;
                let (actual, actual_body) = post_count(
                    &server,
                    route,
                    format,
                    Some((index, shard_count)),
                    "SELECT count(*) AS n FROM events",
                )
                .await;
                assert_eq!(
                    actual, scanned,
                    "{route} {format:?} shard {index}/{shard_count}: {actual_body}"
                );
                if format.is_none() {
                    assert_eq!(scanned_body["stats"]["served_by"], "scan");
                    assert_eq!(actual_body["stats"]["served_by"], "scan");
                }
                sum += actual;
            }
            assert_eq!(
                sum, total_rows,
                "{route} {format:?}: shards must partition rows"
            );

            let (whole, body) = post_count(
                &server,
                route,
                format,
                None,
                "SELECT count(*) AS n FROM events",
            )
            .await;
            assert_eq!(whole, total_rows, "{route} {format:?}: {body}");
            if format.is_none() {
                assert_eq!(body["stats"]["rows_scanned"], 0);
                assert!(body["stats"]["scan"].is_null());
            }
        }
    }
}

#[tokio::test]
async fn sql_timestamp_predicate_via_records() {
    require_loopback!();
    let now = Utc::now();
    let events = vec![
        Event {
            timestamp: now - ChronoDuration::minutes(10),
            host: "old-host".into(),
            source: "/var/log/app.log".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: "old event".into(),
            attributes: None,
        },
        Event {
            timestamp: now - ChronoDuration::seconds(30),
            host: "recent-1".into(),
            source: "/var/log/app.log".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: "recent event 1".into(),
            attributes: None,
        },
        Event {
            timestamp: now - ChronoDuration::seconds(5),
            host: "recent-2".into(),
            source: "/var/log/app.log".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: "recent event 2".into(),
            attributes: None,
        },
    ];
    let srv = spawn_with_events(events, AuthConfig::open(), ServerLimits::default()).await;
    let body = serde_json::json!({
        "query": "SELECT count(host) AS n FROM events WHERE timestamp >= now() - interval '1 minute'"
    });
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["row_count"], 1);
    assert_eq!(body["rows"][0]["n"], 2);
}

/// A windowed `count(*)` must count ROWS IN THE WINDOW, not rows in the files
/// the window touches.
///
/// THE DEFECT THIS GUARDS. `try_count_fast_path_records` returned
/// `cost.estimated_rows_processed` as the answer, gated on `cost.exact`. But
/// `exact` describes the FILE LIST -- whether the estimator could resolve which
/// files a scan must open -- and the row total it accompanies is the sum of the
/// FULL `record_count()` of every file whose manifest bounds merely OVERLAP the
/// window. That is an upper bound on the matching rows, and it equals the
/// answer only when no time predicate narrows anything. The two meanings of
/// "exact" were never the same property.
///
/// Here all four events sit in ONE file, so the file overlaps the window and
/// contributes all four rows; the window contains two. Pre-fix this returned 4.
///
/// Measured at 200G on 2026-08-29 before the fix, where it is not a rounding
/// error: on a uniform 4,000 rows/s corpus a one-hour window whose true count is
/// 14,400,000 returned 64,177,907 -- 4.46x over, `"exact": true`,
/// `rows_scanned: 0`, from the 12 files the hour touched. The sibling
/// `GROUP BY date_trunc('hour', ...)` over the same range returned 14,400,000
/// for every one of its 24 buckets, summing to the exactly-verified day total,
/// so the two answers to the same question disagreed by 4.46x and only the
/// cheap one was wrong.
///
/// `count(host)` on this shape was already tested above and always passed --
/// it is not `count(*)`, so `is_exact_count_star` rejects it and it took the
/// planner. The one shape nobody asserted a VALUE for was the one that broke.
#[tokio::test]
async fn windowed_count_star_counts_rows_not_whole_files() {
    require_loopback!();
    let now = Utc::now();
    let mk = |secs: i64, host: &str| Event {
        timestamp: now - ChronoDuration::seconds(secs),
        host: host.into(),
        source: "/var/log/app.log".into(),
        sourcetype: "app:json".into(),
        index: "main".into(),
        raw: "event".into(),
        attributes: None,
    };
    // Two outside the window, two inside, all in a single file.
    let events = vec![
        mk(600, "old-1"),
        mk(300, "old-2"),
        mk(30, "recent-1"),
        mk(5, "recent-2"),
    ];
    let srv = spawn_with_events(events, AuthConfig::open(), ServerLimits::default()).await;

    let ask = |sql: &'static str| {
        let base = srv.base.clone();
        async move {
            let resp = reqwest::Client::new()
                .post(format!("{base}/api/v1/sql"))
                .json(&serde_json::json!({ "query": sql }))
                .send()
                .await
                .unwrap();
            let status = resp.status();
            let text = resp.text().await.unwrap();
            assert_eq!(status, 200, "{sql}: {text}");
            serde_json::from_str::<serde_json::Value>(&text).unwrap()
        }
    };

    let windowed =
        ask("SELECT count(*) AS n FROM events WHERE timestamp >= now() - interval '1 minute'")
            .await;
    assert_eq!(
        windowed["rows"][0]["n"], 2,
        "windowed count(*) returned the whole file instead of the window: {windowed}"
    );

    // The unwindowed count still takes the cheap whole-table path, which is
    // where `estimated_rows_processed` IS the answer -- the fix must not have
    // turned the fast path off wholesale.
    let total = ask("SELECT count(*) AS n FROM events").await;
    assert_eq!(total["rows"][0]["n"], 4, "unwindowed count(*): {total}");
    assert_eq!(
        total["stats"]["rows_scanned"], 0,
        "unwindowed count(*) stopped being served from the manifest: {total}"
    );
}

#[tokio::test]
async fn sql_ndjson_format() {
    require_loopback!();
    let srv = spawn(3).await;
    let body = serde_json::json!({
        "query": "SELECT host FROM events ORDER BY host",
        "format": "ndjson",
    });
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/x-ndjson"),
    );
    let text = resp.text().await.unwrap();
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 3);
    for line in &lines {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(v["host"].is_string());
    }
}

#[tokio::test]
async fn sql_malformed_returns_400() {
    require_loopback!();
    let srv = spawn(0).await;
    let body = serde_json::json!({ "query": "SELECT bogus FROM nowhere" });
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].is_string());
}

#[tokio::test]
async fn empty_query_returns_400() {
    require_loopback!();
    let srv = spawn(0).await;
    let body = serde_json::json!({ "query": "   " });
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "empty sql should be 400");
}

#[tokio::test]
async fn auth_open_allows_anonymous() {
    require_loopback!();
    let srv = spawn_with(1, AuthConfig::open()).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT 1 AS n" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn auth_required_rejects_missing_header() {
    require_loopback!();
    let srv = spawn_with(0, AuthConfig::from_tokens(["sekret"])).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT 1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn auth_required_rejects_wrong_token() {
    require_loopback!();
    let srv = spawn_with(0, AuthConfig::from_tokens(["sekret"])).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .header(reqwest::header::AUTHORIZATION, "Bearer nope")
        .json(&serde_json::json!({ "query": "SELECT 1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn auth_required_accepts_valid_token() {
    require_loopback!();
    let srv = spawn_with(1, AuthConfig::from_tokens(["sekret"])).await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .header(reqwest::header::AUTHORIZATION, "Bearer sekret")
        .json(&serde_json::json!({ "query": "SELECT count(*) AS n FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"].as_array().unwrap()[0]["n"], 1);
}

/// A float leaves the server with the bits the engine computed. The records
/// renderer writes rows with arrow-json and parses them back, so a parser that
/// rounds a decimal token to the nearest-but-one binary64 value silently
/// rewrites results; `31.458800533996442` reached a client as
/// `31.45880053399644` before #6659. Both response formats are checked against
/// the raw body text, which no parser on this side can repair.
#[tokio::test]
async fn float_results_keep_their_bits_through_the_http_response() {
    require_loopback!();
    let srv = spawn(1).await;
    // The 2026-10-10 status-304 quotient, a repeating fraction, and the sum
    // whose shortest round-trip token needs all 17 significant digits — all
    // computed in binary64 by DataFusion.
    let query = "SELECT CAST(1168295731 AS DOUBLE) / CAST(37137326 AS DOUBLE) AS q, \
                 CAST(1 AS DOUBLE) / CAST(3 AS DOUBLE) AS third, \
                 CAST(0.1 AS DOUBLE) + CAST(0.2 AS DOUBLE) AS sum \
                 FROM events LIMIT 1";
    let expected = [
        ("q", 1_168_295_731_f64 / 37_137_326_f64),
        ("third", 1.0 / 3.0),
        ("sum", 0.1_f64 + 0.2_f64),
    ];

    for format in ["records", "ndjson"] {
        let resp = reqwest::Client::new()
            .post(format!("{}/api/v1/sql", srv.base))
            .json(&serde_json::json!({ "query": query, "format": format }))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let text = resp.text().await.unwrap();
        assert_eq!(status, 200, "{text}");
        assert!(
            text.contains("31.458800533996442"),
            "{format} response lost the quotient's bits: {text}"
        );

        let row: serde_json::Value = match format {
            "records" => {
                let body: serde_json::Value = serde_json::from_str(&text).unwrap();
                body["rows"][0].clone()
            }
            _ => serde_json::from_str(text.lines().next().unwrap()).unwrap(),
        };
        for (column, value) in expected {
            assert_eq!(
                row[column].as_f64().unwrap().to_bits(),
                value.to_bits(),
                "{format} response changed {column}: {text}"
            );
        }
    }
}

#[tokio::test]
async fn auth_required_still_allows_healthz() {
    require_loopback!();
    let srv = spawn_with(0, AuthConfig::from_tokens(["sekret"])).await;
    let resp = reqwest::get(format!("{}/healthz", srv.base)).await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn records_truncated_when_over_max_rows() {
    require_loopback!();
    // 10 events, cap at 4 → expect truncated=true, 413 status, 4 rows.
    let srv = spawn_full(
        10,
        AuthConfig::open(),
        ServerLimits {
            max_rows: 4,
            ..ServerLimits::default()
        },
    )
    .await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({ "query": "SELECT host FROM events ORDER BY host" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 413);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["truncated"], true);
    assert_eq!(body["row_count"], 4);
    assert_eq!(body["max_rows"], 4);
    assert_eq!(body["rows"].as_array().unwrap().len(), 4);
}

#[tokio::test]
async fn ndjson_streams_with_truncation_marker() {
    require_loopback!();
    let srv = spawn_full(
        10,
        AuthConfig::open(),
        ServerLimits {
            max_rows: 3,
            ..ServerLimits::default()
        },
    )
    .await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":  "SELECT host FROM events ORDER BY host",
            "format": "ndjson",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    // 3 data lines + 1 truncation marker.
    assert_eq!(lines.len(), 4, "lines = {lines:?}");

    let last: serde_json::Value = serde_json::from_str(lines[3]).unwrap();
    assert_eq!(last["_meta"], "truncated");
    assert_eq!(last["max_rows"], 3);
    assert_eq!(last["row_count"], 3);

    // First three lines are real rows with `host`.
    for line in &lines[..3] {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(v["host"].is_string());
    }
}

#[tokio::test]
async fn ndjson_under_cap_has_no_marker() {
    require_loopback!();
    let srv = spawn_full(
        3,
        AuthConfig::open(),
        ServerLimits {
            max_rows: 1000,
            ..ServerLimits::default()
        },
    )
    .await;
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":  "SELECT host FROM events ORDER BY host",
            "format": "ndjson",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 3);
    for line in &lines {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        // No _meta on data rows.
        assert!(v.get("_meta").is_none());
    }
}

/// An EMPTY declared index must still register, or a dashboard that joins
/// against it breaks.
///
/// This used to be written against `episodes`, one of five detection tables
/// `IcebergContext::open` auto-created. siglake no longer provisions another
/// consumer's tables, so the same guarantee is now asserted where it actually
/// lives: any index a caller declares, empty, queryable.
#[tokio::test]
async fn sql_query_against_an_empty_declared_index() {
    require_loopback!();
    let srv = spawn(0).await;
    let client = reqwest::Client::new();

    let config = serde_json::json!({
        "index_id": "episodes",
        "doc_mapping": {
            "mode": "strict",
            "timestamp_field": "started_at",
            "field_mappings": [
                { "name": "started_at", "type": "datetime", "required": true },
                { "name": "episode_id", "type": "text", "required": true },
            ],
        },
    });
    let resp = client
        .post(format!("{}/api/v1/indexes", srv.base))
        .json(&config)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{}", resp.text().await.unwrap());

    let body = serde_json::json!({ "query": "SELECT count(*) AS n FROM episodes" });
    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["rows"].as_array().unwrap()[0]["n"], 0);
}
