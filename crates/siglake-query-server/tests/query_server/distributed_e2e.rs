//! End-to-end distributed query (#7 part 2b) over real HTTP: a coordinator
//! server fans a query out to two worker peers (itself + a sibling) sharing one
//! warehouse, and the merged result equals the single-pod answer.

use crate::support;

use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use siglake_core::Event;
use siglake_query_server::discovery::{PeerDirectory, PeerSource};
use siglake_query_server::format::batches_to_records;
use siglake_query_server::{router, AppState, AuthConfig, ServerLimits, TierLimits};
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::session_context_with;

macro_rules! require_loopback {
    () => {
        if !support::loopback_available() {
            return;
        }
    };
}

async fn serve(state: AppState) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state);
    let h = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), h)
}

/// Normalize records-JSON rows into a sorted Vec<String> for order-insensitive
/// comparison.
fn norm(rows: &serde_json::Value) -> Vec<String> {
    let mut v: Vec<String> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| serde_json::to_string(r).unwrap())
        .collect();
    v.sort();
    v
}

async fn unsharded_rows(ice: &Arc<IcebergContext>, sql: &str) -> serde_json::Value {
    let ctx = session_context_with(None, None);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches_to_records(&batches, None).unwrap().rows
}

#[tokio::test]
async fn distributed_query_over_http_matches_single_pod() {
    require_loopback!();
    // One shared warehouse, several files.
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    for i in 0..5 {
        let batch: Vec<Event> = (0..((i + 1) * 5))
            .map(|j| {
                let mut e = Event::now(format!("row {j} status={}", (i + j) % 3 * 100 + 200));
                e.host = format!("host-{}", j % 4);
                e.timestamp = base + Duration::seconds((i * 100 + j) as i64);
                e
            })
            .collect();
        ice.append_events(&batch).await.unwrap();
    }

    // Spawn the worker peer (B) and the coordinator (A), both over `ice`.
    let (url_b, _hb) = serve(AppState::new(ice.clone(), AuthConfig::open())).await;
    // A needs its own URL in the peer list; bind it first to learn the addr.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener.local_addr().unwrap();
    let url_a = format!("http://{addr_a}");
    let state_a = AppState::new(ice.clone(), AuthConfig::open())
        .with_coordinator(vec![url_a.clone(), url_b.clone()], None);
    let app_a = router(state_a);
    let _ha = tokio::spawn(async move { axum::serve(listener, app_a).await.unwrap() });

    let client = reqwest::Client::new();
    for sql in [
        "SELECT count(*) AS n FROM events",
        // Unfiltered whole-table GROUP BY: the coordinator answers this from the
        // manifest aggregate (Tier-1) WITHOUT fanning out — must still equal the
        // single-pod / fan-out answer.
        "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 10",
        "SELECT host, count(*) AS n FROM events WHERE raw LIKE '%status=500%' GROUP BY host",
        "SELECT host FROM events WHERE host = 'host-2'",
    ] {
        let resp = client
            .post(format!("{url_a}/api/v1/sql/distributed"))
            .json(&serde_json::json!({ "query": sql }))
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_success(),
            "distributed `{sql}` -> {}",
            resp.status()
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        let got = norm(&body["rows"]);
        let want = norm(&unsharded_rows(&ice, sql).await);
        assert_eq!(got, want, "distributed != single-pod for `{sql}`");
        // #85: a coordinated query carries coordinator attribution — the
        // merge mode plus one wall per shard for a genuine fan-out, a single
        // wall for the whole-table `local` fallback, and zero walls for a
        // Tier-1 answer served on the coordinator (its phases carry the
        // plan/battery split instead).
        if let Some(dist) = body["stats"]["phases"]["distributed"].as_object() {
            let mode = dist["mode"].as_str().expect("mode present");
            let walls = dist["shard_wall_micros"].as_array().map(Vec::len);
            match mode {
                "local" => assert_eq!(walls, Some(1), "local fallback runs once: {body}"),
                "tier1_local" => {
                    assert_eq!(walls, Some(0), "Tier-1 serves without fan-out: {body}")
                }
                _ => {
                    assert_eq!(walls, Some(2), "one wall per shard: {body}");
                    // Item 6: worker scan attribution survives the Arrow-IPC
                    // transport (x-siglake-scan header) and sums on the
                    // coordinator.
                    let scan = body["stats"]["scan"].as_object().unwrap_or_else(|| {
                        panic!("fanned-out query must carry summed stats.scan: {body}")
                    });
                    assert!(
                        scan["files_planned"].as_u64().unwrap() >= 1,
                        "shards planned files: {body}"
                    );
                }
            }
        }

        // Transparent `/api/v1/sql` on a peer-configured node must coordinate
        // and return the same answer; `/api/v1/sql/local` forces single-pod.
        for path in ["/api/v1/sql", "/api/v1/sql/local"] {
            let resp = client
                .post(format!("{url_a}{path}"))
                .json(&serde_json::json!({ "query": sql }))
                .send()
                .await
                .unwrap();
            assert!(
                resp.status().is_success(),
                "{path} `{sql}` -> {}",
                resp.status()
            );
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(
                norm(&body["rows"]),
                want,
                "{path} != single-pod for `{sql}`"
            );
        }
    }

    for sql in [
        "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'status 200')",
        "SELECT count(*) AS n FROM events WHERE search('status 200')",
        "SELECT timestamp, raw FROM events",
    ] {
        let local = client
            .post(format!("{url_a}/api/v1/sql/local"))
            .json(&serde_json::json!({ "query": sql }))
            .send()
            .await
            .unwrap();
        assert!(
            local.status().is_success(),
            "local `{sql}` -> {}",
            local.status()
        );
        let local_body: serde_json::Value = local.json().await.unwrap();

        for path in ["/api/v1/sql/distributed", "/api/v1/sql"] {
            let resp = client
                .post(format!("{url_a}{path}"))
                .json(&serde_json::json!({ "query": sql }))
                .send()
                .await
                .unwrap();
            assert!(
                resp.status().is_success(),
                "{path} `{sql}` -> {}",
                resp.status()
            );
            let body: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(
                body["rows"], local_body["rows"],
                "{path} != /local for `{sql}`"
            );
        }

        if sql == "SELECT timestamp, raw FROM events" {
            let rows = local_body["rows"].as_array().unwrap();
            let mut prev: Option<chrono::DateTime<Utc>> = None;
            for row in rows {
                let ts = row["timestamp"]
                    .as_str()
                    .unwrap()
                    .parse::<chrono::DateTime<Utc>>()
                    .unwrap();
                if let Some(prev) = prev {
                    assert!(ts <= prev, "bare SELECT must default to newest-first order");
                }
                prev = Some(ts);
            }
        }
    }
}

/// Governance parity (#7): a worker enforces the mid-flight rows-scanned breaker
/// on its own shard. Without this, a distributed query bypasses the per-pod row
/// ceiling that single-pod execution enforces (AWS smoke round 62 finding).
#[tokio::test]
async fn worker_shard_enforces_midflight_rows_breaker() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let batch: Vec<Event> = (0..200)
        .map(|j| Event::now(format!("row {j} status=500")))
        .collect();
    ice.append_events(&batch).await.unwrap();

    // A server whose interactive tier aborts any scan past 10 rows.
    let mut tier = TierLimits::interactive_defaults();
    tier.ceiling_rows_scanned = 10;
    let limits = ServerLimits {
        interactive: tier,
        ..ServerLimits::default()
    };
    let state = AppState::new(ice.clone(), AuthConfig::open()).with_limits(limits);
    let (url, _h) = serve(state).await;

    // A forced full-table scan (200 rows) on the worker endpoint must trip the
    // per-shard breaker — 413, not a silent unbounded scan.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{url}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE raw LIKE '%status=500%'"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        413,
        "worker shard should trip the rows breaker"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("shard mid-flight breaker"),
        "unexpected body: {body}"
    );
}

/// A worker shard must give up on its WALL CLOCK, not only on rows.
///
/// THE DEFECT. `/api/v1/sql/shard` had no timeout at all — the 504 its OpenAPI
/// response list has always documented ("The shard exceeded its wall-clock
/// budget") was aspirational. On the default chart config (`query.replicas: 2`,
/// distributed on) every user query executes here, so a coordinator that gave
/// up at 60s left each worker scanning to completion, holding pool bytes the
/// whole way, with nothing able to stop it.
///
/// The row breaker is NOT the same guard: it bounds rows, and a scan can be slow
/// without being large — a deep unconverged layout, a stalled fetch — in which
/// case the row cap never fires.
///
/// Now more load-bearing, not less: shards no longer take an admission slot, so
/// nothing else bounds how many abandoned scans a pod can accumulate.
#[tokio::test]
async fn worker_shard_gives_up_on_its_wall_clock() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let batch: Vec<Event> = (0..5_000)
        .map(|j| Event::now(format!("row {j} status=500")))
        .collect();
    ice.append_events(&batch).await.unwrap();

    // A tier that allows plenty of ROWS but almost no TIME, so only the
    // wall-clock guard can end this query. With `ceiling_rows_scanned` left
    // generous the row breaker cannot fire, which is what makes the assertion
    // below about the timeout rather than about either guard.
    let mut tier = TierLimits::interactive_defaults();
    tier.default_timeout = std::time::Duration::from_nanos(1);
    tier.ceiling_rows_scanned = 1_000_000_000;
    let limits = ServerLimits {
        interactive: tier,
        ..ServerLimits::default()
    };
    let state = AppState::new(ice.clone(), AuthConfig::open()).with_limits(limits);
    let (url, _h) = serve(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{url}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE raw LIKE '%status=500%'"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        504,
        "worker shard ran past its wall-clock budget: {}",
        resp.text().await.unwrap()
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("wall-clock budget"),
        "unexpected body: {body}"
    );
}

/// #1588: the shard's budget starts when the REQUEST arrives, not at the
/// collect.
///
/// THE DEFECT. The 504 above was armed with `timeout(resolved.timeout, collect)`
/// — a fresh, full budget handed out *after* tenant resolution, `SEARCH`
/// rewriting, table registration, pin resolution and both planning steps had
/// already run unbounded. A shard that spent 40s preparing then got another 60s
/// to collect, outliving the coordinator waiting on it and holding pool bytes
/// the whole way. Same shape as #1526 on the local path, different endpoint.
///
/// Proving it over HTTP without a way to slow preparation by hand: name a table
/// that does not exist. Registration answers that with a 400 before anything is
/// planned or collected, so a 504 on the same request is only reachable if the
/// deadline is already live during registration — which also proves the collect
/// never started, since there is no plan to collect from. The control run with a
/// normal budget pins the other half: the 504 is the clock, not the bad name.
#[tokio::test]
async fn shard_budget_covers_preparation_not_only_the_collect() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    ice.append_events(&[Event::now("row status=500")])
        .await
        .unwrap();

    let unknown_table = serde_json::json!({
        "query": "SELECT count(*) AS n FROM definitely_not_a_table"
    });
    let client = reqwest::Client::new();

    // Control: a normal budget lets registration reach its own verdict.
    let (url, _h) = serve(AppState::new(ice.clone(), AuthConfig::open())).await;
    let resp = client
        .post(format!("{url}/api/v1/sql/shard"))
        .json(&unknown_table)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        400,
        "an unknown table is a 400 when there is budget to discover it: {}",
        resp.text().await.unwrap()
    );

    // Exhausted budget: refused before registration can answer, so before the
    // collect. Rows stay generous so the row breaker cannot be the cause.
    let mut tier = TierLimits::interactive_defaults();
    tier.default_timeout = std::time::Duration::from_nanos(1);
    tier.ceiling_rows_scanned = 1_000_000_000;
    let limits = ServerLimits {
        interactive: tier,
        ..ServerLimits::default()
    };
    let state = AppState::new(ice.clone(), AuthConfig::open()).with_limits(limits);
    let (url, _h) = serve(state).await;
    let resp = client
        .post(format!("{url}/api/v1/sql/shard"))
        .json(&unknown_table)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        504,
        "preparation ran outside the wall-clock budget: {}",
        resp.text().await.unwrap()
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("wall-clock budget"),
        "unexpected body: {body}"
    );
}

/// Phase-4 regression: a USER-INDEX query on a peers-configured node with the
/// WAL buffer enabled (the distributed bench primary's exact shape) must
/// behave like the single-pod path — classify Local (only `events`
/// distributes), then serve aggregates via the fast paths (zero scan) and
/// ordered browses via the WS-3 path, through the transparent `/api/v1/sql`.
#[tokio::test]
async fn user_index_on_coordinator_with_buffer_keeps_fast_paths() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let wal_root = tmp.path().join("wal");
    std::fs::create_dir_all(&wal_root).unwrap();

    // Worker peer (B) + coordinator (A) with buffer dir set, like the bench.
    let (url_b, _hb) = serve(AppState::new(ice.clone(), AuthConfig::open())).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener.local_addr().unwrap();
    let url_a = format!("http://{addr_a}");
    let state_a = AppState::new(ice.clone(), AuthConfig::open())
        .with_coordinator(vec![url_a.clone(), url_b.clone()], None)
        .with_wal_buffer_dir(Some(wal_root));
    let app_a = router(state_a);
    let _ha = tokio::spawn(async move { axum::serve(listener, app_a).await.unwrap() });

    let client = reqwest::Client::new();
    // Create a user index + rows via the API (doc-mapped like logs-bench).
    let config = siglake_core::index_config::IndexConfig {
        index_id: "dist-idx".into(),
        doc_mapping: siglake_core::index_config::IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    let resp = client
        .post(format!("{url_a}/api/v1/indexes"))
        .json(&serde_json::to_value(&config).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let events: Vec<Event> = (0..30)
        .map(|j| {
            let mut e = Event::now(format!("dist row {j}"));
            e.host = format!("h{}", j % 3);
            e.timestamp = base + Duration::seconds(j);
            e
        })
        .collect();
    let batch = siglake_core::events_to_record_batch(&events).unwrap();
    let mapped = siglake_core::map_carrier_batch(&batch, &config).unwrap();
    ice.append_to_table(&ice.index_table_ident("dist-idx"), mapped, &[])
        .await
        .unwrap();

    // Aggregate through the transparent endpoint: fast path, ZERO scan.
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT host, count(*) AS n FROM \"dist-idx\" GROUP BY host ORDER BY n DESC LIMIT 10"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let total: i64 = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["n"].as_i64().unwrap())
        .sum();
    assert_eq!(total, 30, "{body}");
    assert_eq!(
        body["stats"]["rows_scanned"].as_u64(),
        Some(0),
        "group-count fast path must serve on a peers+buffer node: {body}"
    );

    // Ordered browse: newest-first must return 200 with correct order.
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT timestamp, raw FROM \"dist-idx\" ORDER BY timestamp DESC LIMIT 5",
            "default_order": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["row_count"].as_u64(), Some(5), "{body}");
    assert!(
        body["rows"][0]["raw"].as_str().unwrap().contains("row 29"),
        "newest-first: {body}"
    );

    // #4038: the SAME browse without a written-out ORDER BY, through the
    // distributed entrypoint. The implicit newest-first used to be gated on a
    // hard-coded table list, so this returned file order on every managed
    // index while `events` came back newest-first.
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT timestamp, raw FROM \"dist-idx\" LIMIT 5"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["row_count"].as_u64(), Some(5), "{body}");
    let raws: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["raw"].as_str().unwrap())
        .collect();
    assert_eq!(
        raws,
        vec![
            "dist row 29",
            "dist row 28",
            "dist row 27",
            "dist row 26",
            "dist row 25"
        ],
        "bare LIMIT over a managed index must come back newest-first: {body}"
    );
    // The caller's opt-out still leaves it alone: file order, oldest first.
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT timestamp, raw FROM \"dist-idx\" LIMIT 5",
            "default_order": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["rows"][0]["raw"].as_str().unwrap(),
        "dist row 0",
        "default_order:false must not order the scan: {body}"
    );

    // #4090: a projection that aliases another column to `timestamp` used to
    // capture the injected bare identifier through this entrypoint too, so the
    // browse came back ordered by raw text ("dist row 9", "dist row 8", …)
    // rather than newest-first — different rows, not a different rendering.
    // The rewrite declines the shape; this index query is Local, one file, so
    // the remaining order is file order.
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT raw AS timestamp FROM \"dist-idx\" LIMIT 3"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let aliased: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["timestamp"].as_str().unwrap())
        .collect();
    assert_eq!(
        aliased,
        vec!["dist row 0", "dist row 1", "dist row 2"],
        "an aliased `timestamp` must not capture the injected sort: {body}"
    );

    // THE LIVE FAILURE SHAPE (Phase-4): a sealed-but-uncommitted segment in
    // the index's WAL while queries run. Aggregates must fold the buffered
    // rows as a delta (exact + zero scan), and the ordered browse must merge
    // the buffer without losing the base scan's ordering (no full-sort).
    let index_wal = tmp.path().join("wal").join("default").join("dist-idx");
    std::fs::create_dir_all(&index_wal).unwrap();
    let mut w = siglake_wal::WalWriter::with_thresholds(
        &index_wal,
        "ing-test",
        10_000,
        std::time::Duration::from_secs(600),
    )
    .unwrap();
    let newest: Vec<Event> = (0..3)
        .map(|j| {
            let mut e = Event::now(format!("buffered row {j}"));
            e.host = "h9".into();
            e.timestamp = base + Duration::seconds(1000 + j);
            e
        })
        .collect();
    w.append_events(&newest).unwrap();
    w.seal().unwrap().expect("sealed");
    drop(w);

    // Aggregate: exact (30 committed + 3 buffered) AND zero-scan.
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT host, count(*) AS n FROM \"dist-idx\" GROUP BY host ORDER BY n DESC LIMIT 10"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let total: i64 = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["n"].as_i64().unwrap())
        .sum();
    assert_eq!(total, 33, "committed + buffered: {body}");
    assert_eq!(
        body["stats"]["rows_scanned"].as_u64(),
        Some(0),
        "hybrid fast path must stay zero-scan with buffered rows: {body}"
    );

    // Ordered browse: buffered rows are the newest — they must lead, exactly.
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT timestamp, raw FROM \"dist-idx\" ORDER BY timestamp DESC LIMIT 5",
            "default_order": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let raws: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["raw"].as_str().unwrap())
        .collect();
    assert!(
        raws[0].contains("buffered row 2") && raws[2].contains("buffered row 0"),
        "buffered newest rows must lead the ordered browse: {raws:?}"
    );
    assert!(
        raws[3].contains("row 29"),
        "committed rows follow: {raws:?}"
    );
    // #86 policy: a small-LIMIT browse runs on the COORDINATOR (single-node
    // early-stop beats per-worker drains + merge — 769ms vs ~50ms on the
    // round-2 board), so it must NOT carry distributed stats.
    assert!(
        body["stats"]["phases"]["distributed"].is_null(),
        "small-LIMIT browse must run local, not fan out: {body}"
    );

    // Tier-1 battery on the distributed path: windowed count and negation
    // are metadata answers (+ buffered delta) — they must serve zero-scan on
    // the coordinator, never reach a worker's fast-path-less /shard (the six
    // 500s of the first round-2 board).
    for sql in [
        "SELECT count(*) AS n FROM \"dist-idx\" WHERE timestamp >= TIMESTAMP '2026-06-01T00:00:00Z'",
        "SELECT count(*) AS n FROM \"dist-idx\" WHERE host <> 'h9'",
    ] {
        let resp = client
            .post(format!("{url_a}/api/v1/sql"))
            .json(&serde_json::json!({ "query": sql }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "`{sql}`");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["stats"]["rows_scanned"].as_u64(),
            Some(0),
            "Tier-1 must serve `{sql}` zero-scan on the coordinator: {body}"
        );
    }

    // A FILTERED aggregate Tier-1 can't serve (predicate on raw) must
    // genuinely fan out — with the buffered rows folded in exactly.
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM \"dist-idx\" WHERE raw LIKE '%row%'"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["rows"][0]["n"].as_i64(),
        Some(33),
        "filtered count exact: {body}"
    );
    let dist = body["stats"]["phases"]["distributed"]
        .as_object()
        .unwrap_or_else(|| panic!("filtered index aggregate must carry distributed stats: {body}"));
    assert_eq!(dist["mode"].as_str(), Some("aggregate"), "{body}");
    assert_eq!(
        dist["shard_wall_micros"].as_array().map(Vec::len),
        Some(2),
        "{body}"
    );
}

/// #89: a pinned shard request is a TIME-TRAVEL read — it must serve the
/// pinned snapshot's rows even after later commits. This is the mechanism
/// that makes a fan-out consistent cluster-wide (workers read the
/// coordinator's serving snapshot, not their own possibly-newer cache).
#[tokio::test]
async fn shard_pin_serves_the_pinned_snapshot() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let mk = |n: usize, tag: &str| -> Vec<Event> {
        (0..n)
            .map(|j| {
                let mut e = Event::now(format!("{tag} {j}"));
                e.timestamp = base + Duration::seconds(j as i64);
                e
            })
            .collect()
    };
    ice.append_events(&mk(7, "first")).await.unwrap();
    let s1 = ice
        .table_snapshot_id("events")
        .await
        .unwrap()
        .expect("snapshot after commit 1");
    ice.append_events(&mk(5, "second")).await.unwrap();

    let (url, _h) = serve(AppState::new(ice.clone(), AuthConfig::open())).await;
    let client = reqwest::Client::new();
    let shard_request = |pin: Option<serde_json::Value>| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let mut body = serde_json::json!({
                "query": "SELECT count(*) AS n FROM events",
                "shard": { "index": 0, "count": 1 },
            });
            if let Some(pin) = pin {
                body["pin"] = pin;
            }
            client
                .post(format!("{url}/api/v1/sql/shard"))
                .json(&body)
                .send()
                .await
                .unwrap()
        }
    };
    let count = |pin: Option<serde_json::Value>| {
        let shard_request = &shard_request;
        async move {
            let resp = shard_request(pin).await;
            assert!(resp.status().is_success(), "{}", resp.status());
            let bytes = resp.bytes().await.unwrap();
            let batches = siglake_query_server::format::arrow_ipc_to_batches(&bytes).unwrap();
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0)
        }
    };

    // Unpinned: the current snapshot (both commits).
    assert_eq!(count(None).await, 12);
    // Pinned to snapshot 1: only the first commit's rows — time travel.
    assert_eq!(
        count(Some(
            serde_json::json!({ "table": "events", "snapshot_id": s1 })
        ))
        .await,
        7
    );
    // #1525: an unresolvable pin is REFUSED, not silently answered from the
    // current snapshot. The old behavior returned 12 here — a 200 produced from
    // a file generation the coordinator never asked for, which is exactly the
    // mixed-snapshot fan-out the pin exists to prevent.
    let resp = shard_request(Some(
        serde_json::json!({ "table": "events", "snapshot_id": 999999999 }),
    ))
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "an unresolvable pin must refuse, not fall back to current"
    );
    assert_eq!(
        resp.headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
        Some(
            siglake_query_server::error::SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS
                .to_string()
                .as_str()
        ),
        "the refusal is retryable and says when"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["reason"],
        siglake_query_server::error::SHARD_PIN_UNRESOLVED_REASON,
        "a client must tell this 503 from a pool refusal: {body}"
    );
    assert_eq!(body["pin"]["snapshot_id"], 999999999, "{body}");
}

/// #1525: on a real two-peer fan-out where ONE peer cannot resolve the pinned
/// snapshot, the query fails with a retryable 503 instead of returning a count
/// merged across two different file generations.
///
/// The fixture reproduces the field condition — snapshot expiry racing a
/// fan-out — without a race. Coordinator A warms its metadata cache on
/// snapshot 1, then a second writer appends and expires everything but the
/// current snapshot: A still SERVES (and therefore pins to) snapshot 1, which
/// the catalog no longer retains, so peer B cannot resolve it however many
/// times it refreshes. The second commit also RECLUSTERS the live file list —
/// it has more files, in a different order — so a B that fell back to its
/// current snapshot would return a plausible, wrong partial rather than an
/// obviously broken one.
#[tokio::test]
async fn an_unresolvable_pin_on_one_peer_refuses_the_fanout() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let mk = |n: usize, tag: &str| -> Vec<Event> {
        (0..n)
            .map(|j| {
                let mut e = Event::now(format!("{tag} {j} status=500"));
                e.host = format!("host-{}", j % 3);
                e.timestamp = base + Duration::seconds(j as i64);
                e
            })
            .collect()
    };

    // The writer's own context: commits and snapshot expiry happen here, so
    // neither server's metadata cache is refreshed as a side effect.
    let writer = IcebergContext::open(&warehouse).await.unwrap();
    writer.append_events(&mk(7, "first")).await.unwrap();

    let ice_a = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let ice_b = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    // Warm A's cache on snapshot 1 — this is the snapshot it will pin to.
    let s1 = ice_a
        .table_snapshot_id("events")
        .await
        .unwrap()
        .expect("snapshot after commit 1");

    // Recluster: three more commits (more files, renumbered live list), then
    // drop every snapshot but the current one from the catalog.
    for tag in ["second", "third", "fourth"] {
        writer.append_events(&mk(5, tag)).await.unwrap();
    }
    let ident = writer.events_table_ident().clone();
    let expired = writer.expire_snapshots(&ident, 1).await.unwrap();
    assert!(expired > 0, "the fixture must actually expire snapshot 1");
    assert_eq!(
        ice_a.table_snapshot_id("events").await.unwrap(),
        Some(s1),
        "A must still be serving (and pinning to) the expired snapshot; if its \
         metadata cache refreshed, this fixture proves nothing"
    );

    // B is the sibling peer; A coordinates over [A, B].
    let (url_b, _hb) = serve(AppState::new(ice_b.clone(), AuthConfig::open())).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url_a = format!("http://{}", listener.local_addr().unwrap());
    let state_a = AppState::new(ice_a.clone(), AuthConfig::open())
        .with_coordinator(vec![url_a.clone(), url_b.clone()], None);
    let app_a = router(state_a);
    let _ha = tokio::spawn(async move { axum::serve(listener, app_a).await.unwrap() });

    let client = reqwest::Client::new();
    // A filtered count genuinely fans out (an unfiltered whole-table count is
    // answered from the manifest aggregate on the coordinator, without peers).
    let sql = "SELECT count(*) AS n FROM events WHERE raw LIKE '%status=500%'";
    for path in ["/api/v1/sql", "/api/v1/sql/distributed"] {
        let resp = client
            .post(format!("{url_a}{path}"))
            .json(&serde_json::json!({ "query": sql }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "{path} merged across snapshots instead of refusing: {}",
            resp.text().await.unwrap_or_default()
        );
        assert!(
            resp.headers().contains_key(reqwest::header::RETRY_AFTER),
            "{path}: the refusal must tell the client to retry"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains(&format!(
                    "snapshot {s1} of `events` is not available on this worker"
                )),
            "{path}: the worker's own account of the refusal must survive the \
             hop: {body}"
        );
    }

    // Behavior that must NOT change: `/api/v1/sql/local` never fans out, so it
    // never pins, and still answers from this pod's own snapshot.
    let resp = client
        .post(format!("{url_a}/api/v1/sql/local"))
        .json(&serde_json::json!({ "query": sql }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "an unpinned single-pod read is unaffected: {}",
        resp.status()
    );
    // And a direct, correctly-pinned shard request to B still succeeds: only
    // the UNRESOLVABLE pin refuses.
    let live = ice_b
        .table_snapshot_id("events")
        .await
        .unwrap()
        .expect("B has a current snapshot");
    let resp = client
        .post(format!("{url_b}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": sql,
            "shard": { "index": 0, "count": 1 },
            "pin": { "table": "events", "snapshot_id": live },
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "a resolvable pin must still be served: {}",
        resp.status()
    );
}

// ===========================================================================
// #2554: a shard pin carries the SCHEMA generation as well as the snapshot.
//
// THE DEFECT THESE PIN. `migrate_table_schema_additive` commits an
// `UpdateSchemaAction` and no data snapshot, so the schema id moves while the
// snapshot id stands still. A pin that named only the snapshot was therefore
// satisfied by a worker whose metadata cache predated the migration: it
// recognised the snapshot, skipped the refresh, and planned against its narrow
// cached schema — so a fan-out naming the new column failed on that peer alone,
// as invalid SQL, while the coordinator and every converged peer succeeded.
//
// Every fixture below holds the snapshot id FIXED across the migration and
// asserts it, so a green run cannot be one where a data commit did the work.
// Each peer's metadata cache TTL is explicit ([`frozen`] / [`live`]) rather
// than the 5s default, so "this peer is stale" is a property of the fixture and
// not of how fast the machine ran the test.
// ===========================================================================

/// A column the current binary does not declare, standing in for the next
/// additive bump — the same device `result_cache_schema_generation.rs` and
/// `siglake-storage`'s `schema_rollback.rs` use, and the only way to exercise a
/// widen without a second image.
const FUTURE_COLUMN: &str = "severity_number";

/// `events_schema()` plus one nullable column: what a future binary declares
/// and `migrate-schema` additively adds.
fn widened_schema() -> arrow_schema::SchemaRef {
    let base = siglake_core::events_schema();
    let mut fields: Vec<arrow_schema::Field> =
        base.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(arrow_schema::Field::new(
        FUTURE_COLUMN,
        arrow_schema::DataType::Int64,
        true,
    ));
    Arc::new(arrow_schema::Schema::new(fields))
}

/// A context whose table-metadata cache never expires on its own. Only an
/// explicit refresh — which is what pin resolution does — moves it, so a peer
/// built this way is stale for as long as the test needs it to be.
async fn frozen(warehouse: &std::path::Path) -> Arc<IcebergContext> {
    Arc::new(
        IcebergContext::open(warehouse)
            .await
            .unwrap()
            .with_table_cache_ttl(std::time::Duration::from_secs(3600)),
    )
}

/// A context that reloads table metadata on every read — the coordinator in
/// the stale-peer fixtures, and the peer in the ahead-peer one.
async fn live(warehouse: &std::path::Path) -> Arc<IcebergContext> {
    Arc::new(
        IcebergContext::open(warehouse)
            .await
            .unwrap()
            .with_table_cache_ttl(std::time::Duration::ZERO),
    )
}

/// `n` events, all matching `status=500`, spread over one minute.
fn status_500_events(n: usize) -> Vec<Event> {
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    (0..n)
        .map(|j| {
            let mut e = Event::now(format!("row {j} status=500"));
            e.host = format!("host-{}", j % 3);
            e.timestamp = base + Duration::seconds(j as i64);
            e
        })
        .collect()
}

/// Add [`FUTURE_COLUMN`] to `events` and return the (unchanged snapshot, new
/// schema) generation the migration leaves behind, asserting that the snapshot
/// really did stand still.
async fn widen_events(writer: &IcebergContext) {
    let before = writer.current_table_snapshot_id("events").await.unwrap();
    let added = writer
        .migrate_table_schema_additive(writer.events_table_ident(), widened_schema().as_ref())
        .await
        .unwrap();
    assert_eq!(added, 1, "the migration must add exactly {FUTURE_COLUMN}");
    assert_eq!(
        writer.current_table_snapshot_id("events").await.unwrap(),
        before,
        "an additive migration must not move the snapshot; if it does, these \
         tests are no longer about the defect they were written for"
    );
}

/// Coordinator `a` over peers `[a, b]`, with `b` already serving. Returns
/// (coordinator url, worker url) and holds both servers alive through the
/// returned handles.
async fn two_peer_cluster(
    ice_a: Arc<IcebergContext>,
    ice_b: Arc<IcebergContext>,
) -> (
    String,
    String,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    two_peer_cluster_with_buffer(ice_a, ice_b, None).await
}

/// [`two_peer_cluster`] with the coordinator's WAL buffer root set, so its half
/// of a fan-out is committed shards PLUS a buffer partial. Only the coordinator
/// takes the dir: workers scan Iceberg files alone, which is what makes the
/// partial disjoint from their shards.
async fn two_peer_cluster_with_buffer(
    ice_a: Arc<IcebergContext>,
    ice_b: Arc<IcebergContext>,
    buffer_root: Option<std::path::PathBuf>,
) -> (
    String,
    String,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let (url_b, hb) = serve(AppState::new(ice_b, AuthConfig::open())).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url_a = format!("http://{}", listener.local_addr().unwrap());
    let state_a = AppState::new(ice_a, AuthConfig::open())
        .with_coordinator(vec![url_a.clone(), url_b.clone()], None)
        .with_wal_buffer_dir(buffer_root);
    let app_a = router(state_a);
    let ha = tokio::spawn(async move { axum::serve(listener, app_a).await.unwrap() });
    (url_a, url_b, ha, hb)
}

/// The number of shard walls a coordinated answer recorded — `Some(2)` on a
/// genuine two-peer fan-out, `Some(1)` for the whole-table `local` fallback,
/// `Some(0)` for a Tier-1 answer served without peers.
fn shard_walls(body: &serde_json::Value) -> Option<usize> {
    body["stats"]["phases"]["distributed"]["shard_wall_micros"]
        .as_array()
        .map(Vec::len)
}

/// #2554: the coordinator pins the generation it planned against, and a peer
/// whose metadata predates a schema-only migration refreshes onto it instead of
/// answering from its narrow cached schema.
///
/// Before this, the fan-out below returned the peer's `400` ("No field named
/// severity_number") through the coordinator: the pin named a snapshot the
/// stale peer recognised, so nothing made it look at the catalog again.
#[tokio::test]
async fn a_stale_peer_refreshes_onto_the_coordinators_pinned_schema() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    // The writer's own context, so neither server's cache is refreshed as a
    // side effect of committing.
    let writer = IcebergContext::open(&warehouse).await.unwrap();
    writer.append_events(&status_500_events(12)).await.unwrap();

    // B warms on the NARROW generation and stays there.
    let ice_b = frozen(&warehouse).await;
    let narrow = ice_b.current_table_generation("events").await.unwrap();

    widen_events(&writer).await;

    // A reads the catalog fresh: same snapshot, new schema id.
    let ice_a = live(&warehouse).await;
    let wide = ice_a.current_table_generation("events").await.unwrap();
    assert_eq!(
        wide.snapshot_id, narrow.snapshot_id,
        "the fixture must widen WITHOUT a data commit, or a snapshot-only pin \
         would already have caught it"
    );
    assert_ne!(
        wide.schema_id, narrow.schema_id,
        "the migration must move the schema id"
    );
    assert_eq!(
        ice_b.current_table_generation("events").await.unwrap(),
        narrow,
        "B must still be on the narrow generation; if its cache refreshed on \
         its own, this fixture proves nothing"
    );

    let (url_a, url_b, _ha, _hb) = two_peer_cluster(ice_a.clone(), ice_b.clone()).await;
    let client = reqwest::Client::new();

    // A filtered count genuinely fans out (an unfiltered whole-table count is
    // answered from the manifest aggregate on the coordinator), and naming
    // FUTURE_COLUMN is what a stale peer cannot plan.
    let sql = format!(
        "SELECT count(*) AS n FROM events \
         WHERE raw LIKE '%status=500%' AND {FUTURE_COLUMN} IS NULL"
    );
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({ "query": sql }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "a stale peer must refresh onto the pinned schema, not refuse the \
         column: {body}"
    );
    assert_eq!(shard_walls(&body), Some(2), "one wall per peer: {body}");
    assert_eq!(
        body["rows"][0]["n"].as_i64(),
        Some(12),
        "every row predates the widen, so all of them read null: {body}"
    );

    // Behaviour that must NOT change: an ordinary query over a generation both
    // peers agree on still matches the single-pod answer.
    let unchanged = "SELECT host, count(*) AS n FROM events \
                     WHERE raw LIKE '%status=500%' GROUP BY host";
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({ "query": unchanged }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "{}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(shard_walls(&body), Some(2), "{body}");
    assert_eq!(
        norm(&body["rows"]),
        norm(&unsharded_rows(&ice_a, unchanged).await),
        "an unchanged-schema fan-out must still equal the single-pod answer"
    );

    // A schema generation NO metadata retains is the documented retryable
    // refusal, same contract as an unresolvable snapshot: 503, `Retry-After`,
    // `reason: "shard_pin_unresolved"`.
    let resp = client
        .post(format!("{url_b}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "shard": { "index": 0, "count": 1 },
            "pin": {
                "table": "events",
                "snapshot_id": wide.snapshot_id,
                "schema_id": 424_242,
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "an unresolvable SCHEMA pin must refuse, not fall back to current"
    );
    assert_eq!(
        resp.headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()),
        Some(
            siglake_query_server::error::SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS
                .to_string()
                .as_str()
        ),
        "the refusal is retryable and says when"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["reason"],
        siglake_query_server::error::SHARD_PIN_UNRESOLVED_REASON,
        "one reason code covers both halves of a pin: {body}"
    );
    assert_eq!(body["pin"]["schema_id"], 424_242, "{body}");

    // Compatibility: a pin with NO schema id is the pre-#2554 contract and is
    // still served from the snapshot alone.
    let resp = client
        .post(format!("{url_b}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "shard": { "index": 0, "count": 1 },
            "pin": { "table": "events", "snapshot_id": wide.snapshot_id },
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "an older coordinator's snapshot-only pin must still be honoured: {}",
        resp.status()
    );
}

/// #2554: a peer AHEAD of the coordinator serves the pinned older schema out of
/// its retained metadata rather than refusing the shard — and rather than
/// contributing a wider partial than the coordinator planned for.
///
/// `TableMetadata` keeps historical schemas by id, so there is nothing to
/// refuse here: the generation the coordinator asked for is one this peer can
/// still produce. The assertion is a distributed `SELECT *`, whose partials the
/// coordinator concatenates — a peer that answered from its own wider schema
/// would hand back a partial with an extra column.
#[tokio::test]
async fn a_peer_ahead_of_the_coordinator_serves_the_pinned_older_schema() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let writer = IcebergContext::open(&warehouse).await.unwrap();
    writer.append_events(&status_500_events(12)).await.unwrap();

    // This time the COORDINATOR is the frozen one: it plans, and pins, narrow.
    let ice_a = frozen(&warehouse).await;
    let narrow = ice_a.current_table_generation("events").await.unwrap();

    widen_events(&writer).await;

    let ice_b = live(&warehouse).await;
    let wide = ice_b.current_table_generation("events").await.unwrap();
    assert_eq!(wide.snapshot_id, narrow.snapshot_id, "no data commit");
    assert_ne!(wide.schema_id, narrow.schema_id, "the schema id moved");
    assert_eq!(
        ice_a.current_table_generation("events").await.unwrap(),
        narrow,
        "A must still plan against the narrow generation, or it would pin the \
         wide one and there is no ahead-peer left to test"
    );

    let (url_a, _url_b, _ha, _hb) = two_peer_cluster(ice_a.clone(), ice_b.clone()).await;
    let client = reqwest::Client::new();

    let sql = "SELECT * FROM events WHERE host = 'host-1'";
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({ "query": sql }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "a peer that is merely AHEAD must serve the pinned generation, not \
         refuse it: {body}"
    );
    assert_eq!(shard_walls(&body), Some(2), "one wall per peer: {body}");
    assert!(
        !body["columns"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|c| c.as_str() == Some(FUTURE_COLUMN)),
        "the answer must be the generation the coordinator planned: {body}"
    );
    assert_eq!(
        norm(&body["rows"]),
        norm(&unsharded_rows(&ice_a, sql).await),
        "a fan-out pinned to the narrow generation must equal the narrow \
         single-pod answer"
    );
}

/// #2554: an explicit EMPTY-table pin carries the coordinator's schema
/// generation too.
///
/// A table that a migration widened before its first append is the one case
/// with no snapshot to pin at all, and the worker built its `EmptyTable` from
/// whatever schema it happened to hold — so the empty arm had the same gap as
/// the snapshot arm, reachable on any freshly created tenant namespace.
#[tokio::test]
async fn an_empty_table_pin_carries_the_coordinators_schema_generation() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    // No append at all: `events` exists (bootstrap creates it) with no snapshot.
    let writer = IcebergContext::open(&warehouse).await.unwrap();

    let ice_b = frozen(&warehouse).await;
    let narrow = ice_b.current_table_generation("events").await.unwrap();
    assert!(
        narrow.snapshot_id.is_none(),
        "the fixture needs a table with no data: {narrow:?}"
    );

    widen_events(&writer).await;

    let ice_a = live(&warehouse).await;
    let wide = ice_a.current_table_generation("events").await.unwrap();
    assert!(wide.snapshot_id.is_none(), "still no data: {wide:?}");
    assert_ne!(wide.schema_id, narrow.schema_id, "the schema id moved");
    assert_eq!(
        ice_b.current_table_generation("events").await.unwrap(),
        narrow,
        "B must still be on the narrow generation"
    );

    let (url_a, url_b, _ha, _hb) = two_peer_cluster(ice_a.clone(), ice_b.clone()).await;
    let client = reqwest::Client::new();

    // The empty arm, directly: `empty: true` plus the coordinator's schema id.
    // A stale worker used to answer 400 here — its `EmptyTable` had no
    // FUTURE_COLUMN to plan against.
    let resp = client
        .post(format!("{url_b}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": format!("SELECT count(*) AS n FROM events WHERE {FUTURE_COLUMN} IS NULL"),
            "shard": { "index": 0, "count": 1 },
            "pin": { "table": "events", "empty": true, "schema_id": wide.schema_id },
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "an empty pin must be served at the pinned schema: {}",
        resp.text().await.unwrap_or_default()
    );

    // A schema generation nobody retains refuses under the same contract, with
    // no snapshot in the echoed pin.
    let resp = client
        .post(format!("{url_b}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "shard": { "index": 0, "count": 1 },
            "pin": { "table": "events", "empty": true, "schema_id": 424_242 },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "an unresolvable schema on an empty pin must refuse"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["reason"],
        siglake_query_server::error::SHARD_PIN_UNRESOLVED_REASON,
        "{body}"
    );
    assert!(body["pin"]["snapshot_id"].is_null(), "{body}");

    // And through the transparent path, where the coordinator captures the
    // empty pin itself.
    let sql = format!(
        "SELECT count(*) AS n FROM events \
         WHERE raw LIKE '%status=500%' AND {FUTURE_COLUMN} IS NULL"
    );
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({ "query": sql }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"][0]["n"].as_i64(),
        Some(0),
        "an empty table counts zero: {body}"
    );
}

/// #2921: snapshotless incarnations reuse schema id 0, so an empty shard pin
/// needs the Iceberg table UUID as well as the schema id.
///
/// One worker starts with A cached. A B pin must force it to refresh despite
/// the matching schema number; once it holds B, the old A pin must be refused
/// instead of being resolved against B. The final request records the explicit
/// compatibility contract for an older coordinator that omits the UUID.
#[tokio::test]
async fn an_empty_shard_pin_distinguishes_recreated_index_incarnations() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let writer = IcebergContext::open(&warehouse).await.unwrap();

    let first_config = pin_index_config();
    writer.create_index(&first_config).await.unwrap();
    let worker = frozen(&warehouse).await;
    let first = worker.current_table_generation(PIN_INDEX).await.unwrap();
    assert!(
        first.snapshot_id.is_none(),
        "fixture must be empty: {first:?}"
    );

    assert!(writer.delete_index(PIN_INDEX).await.unwrap());
    writer
        .create_index(&widened_pin_index_config())
        .await
        .unwrap();
    let replacement = live(&warehouse)
        .await
        .current_table_generation(PIN_INDEX)
        .await
        .unwrap();
    assert!(
        replacement.snapshot_id.is_none(),
        "replacement must still be empty: {replacement:?}"
    );
    assert_eq!(
        replacement.schema_id, first.schema_id,
        "both initial schemas deliberately use id 0"
    );
    assert_ne!(
        replacement.table_uuid, first.table_uuid,
        "recreation must change the table identity"
    );

    let (url, task) = serve(AppState::new(worker.clone(), AuthConfig::open())).await;
    let client = reqwest::Client::new();

    // The worker still holds A. The UUID mismatch is what makes it refresh to
    // B; schema id 0 alone would accept A and fail planning on FUTURE_COLUMN.
    let resp = client
        .post(format!("{url}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": format!(
                "SELECT count(*) AS n FROM {PIN_INDEX} WHERE {FUTURE_COLUMN} IS NULL"
            ),
            "shard": { "index": 0, "count": 1 },
            "pin": {
                "table": PIN_INDEX,
                "empty": true,
                "schema_id": replacement.schema_id,
                "table_uuid": replacement.table_uuid,
            },
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "a B pin must refresh a worker caching A: {}",
        resp.text().await.unwrap_or_default()
    );
    assert_eq!(
        worker.current_table_generation(PIN_INDEX).await.unwrap(),
        replacement,
        "the successful B pin must leave the worker on B"
    );

    // A is now unavailable on this worker. Its UUID must not be satisfied by
    // B merely because both empty generations use schema id 0.
    let resp = client
        .post(format!("{url}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": format!("SELECT count(*) AS n FROM {PIN_INDEX}"),
            "shard": { "index": 0, "count": 1 },
            "pin": {
                "table": PIN_INDEX,
                "empty": true,
                "schema_id": first.schema_id,
                "table_uuid": first.table_uuid,
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["reason"],
        siglake_query_server::error::SHARD_PIN_UNRESOLVED_REASON,
        "{body}"
    );
    assert_eq!(body["pin"]["table_uuid"], first.table_uuid, "{body}");

    // A coordinator predating #2921 sends no UUID. The newer worker retains
    // that coordinator's schema-only empty-pin behavior.
    let resp = client
        .post(format!("{url}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": format!("SELECT count(*) AS n FROM {PIN_INDEX}"),
            "shard": { "index": 0, "count": 1 },
            "pin": {
                "table": PIN_INDEX,
                "empty": true,
                "schema_id": replacement.schema_id,
            },
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "a UUID-less pin preserves the older coordinator contract"
    );

    task.abort();
}

/// The managed index the pin fixture below widens. Named with an underscore so
/// the SQL needs no quoting and DataFusion's identifier normalization is not
/// part of the test.
const PIN_INDEX: &str = "pin_idx";

/// [`PIN_INDEX`] as created: the builtin events mapping, nothing else.
fn pin_index_config() -> siglake_core::index_config::IndexConfig {
    siglake_core::index_config::IndexConfig {
        index_id: PIN_INDEX.to_string(),
        doc_mapping: siglake_core::index_config::IndexConfig::builtin_events().doc_mapping,
        retention: None,
        index_at_flush: None,
    }
}

/// The same mapping with one appended nullable field — the only widen
/// `validate_additive_update` accepts, and the index's counterpart of
/// [`widened_schema`].
fn widened_pin_index_config() -> siglake_core::index_config::IndexConfig {
    let mut config = pin_index_config();
    config
        .doc_mapping
        .field_mappings
        .push(siglake_core::index_config::FieldMapping {
            name: FUTURE_COLUMN.to_string(),
            field_type: siglake_core::index_config::FieldType::Long,
            required: false,
        });
    config
}

/// #2575: the generation pin over a MANAGED INDEX, whose schema moves through
/// an `update_index` mapping widen rather than through `migrate-schema`.
///
/// The fixtures above all pin `events`. An index reaches the same worker pin by
/// a different coordinator route — `capture_buffered_shard_read` takes
/// `index_provider_with_consumed_snapshot`, not the events twin — and its widen
/// is a `SetIndexMappingAction` plus an `UpdateSchemaAction` in one
/// metadata-only commit rather than `migrate_table_schema_additive`. What comes
/// out is the same shape: snapshot fixed, schema id moved, so a stale peer has
/// the same way to be wrong.
#[tokio::test]
async fn a_managed_index_widened_by_its_mapping_pins_like_events() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    // The writer's own context, so neither server's cache is refreshed as a
    // side effect of creating, appending or widening.
    let writer = IcebergContext::open(&warehouse).await.unwrap();

    let config = pin_index_config();
    writer.create_index(&config).await.unwrap();
    // Committed rows BEFORE the widen: the pin then names a real snapshot (the
    // empty arm is already covered above), and every row reads null through the
    // column the widen adds.
    let batch = siglake_core::events_to_record_batch(&status_500_events(12)).unwrap();
    let mapped = siglake_core::map_carrier_batch(&batch, &config).unwrap();
    writer
        .append_to_table(&writer.index_table_ident(PIN_INDEX), mapped, &[])
        .await
        .unwrap();

    // B warms on the NARROW mapping and stays there.
    let ice_b = frozen(&warehouse).await;
    let narrow = ice_b.current_table_generation(PIN_INDEX).await.unwrap();
    assert!(
        narrow.snapshot_id.is_some(),
        "the fixture needs committed rows: {narrow:?}"
    );

    // The widen: one appended nullable field, no append of data.
    writer
        .update_index(&widened_pin_index_config())
        .await
        .unwrap();

    let ice_a = live(&warehouse).await;
    let wide = ice_a.current_table_generation(PIN_INDEX).await.unwrap();
    assert_eq!(
        wide.snapshot_id, narrow.snapshot_id,
        "a mapping widen must be metadata-only; if `update_index` starts \
         committing data this fixture is no longer about the pin"
    );
    assert_ne!(
        wide.schema_id, narrow.schema_id,
        "the mapping widen must move the schema id"
    );
    assert_eq!(
        ice_b.current_table_generation(PIN_INDEX).await.unwrap(),
        narrow,
        "B must still be on the narrow generation; if its cache refreshed on \
         its own, this fixture proves nothing"
    );

    let (url_a, url_b, _ha, _hb) = two_peer_cluster(ice_a.clone(), ice_b.clone()).await;
    let client = reqwest::Client::new();
    let sql = format!(
        "SELECT count(*) AS n FROM {PIN_INDEX} \
         WHERE raw LIKE '%status=500%' AND {FUTURE_COLUMN} IS NULL"
    );

    // NEGATIVE CONTROL, and it runs FIRST: the successful fan-out below
    // refreshes B onto the wide mapping, after which nothing here is stale.
    //
    // A pre-#2554 coordinator's pin names the snapshot alone. That is still
    // accepted — schema-less pins stay honoured, this adds no mandatory field —
    // and accepting it is precisely what leaves the stale peer planning against
    // its narrow mapping, so the new field does not resolve.
    let resp = client
        .post(format!("{url_b}/api/v1/sql/shard"))
        .json(&serde_json::json!({
            "query": sql,
            "shard": { "index": 0, "count": 1 },
            "pin": { "table": PIN_INDEX, "snapshot_id": wide.snapshot_id },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "a snapshot-only pin is honoured, so the stale peer plans narrow and \
         refuses the new field"
    );
    let refusal = resp.text().await.unwrap();
    assert!(
        refusal.contains(FUTURE_COLUMN),
        "the refusal must be the unresolved column, not some other failure: \
         {refusal}"
    );
    assert_eq!(
        ice_b.current_table_generation(PIN_INDEX).await.unwrap(),
        narrow,
        "a schema-less pin resolves off the snapshot alone, so it must not \
         have refreshed B out of the stale state the fan-out below needs"
    );

    // THE ACCEPTANCE, through the transparent path, where the coordinator
    // captures the index's generation itself via
    // `index_provider_with_consumed_snapshot`. Nothing has refreshed B yet, so
    // this is the same peer that just refused the column — the pin's schema
    // half is the only difference between the two requests.
    //
    // (A direct schema-carrying `/shard` POST would prove the same thing, but
    // only if it ran here, and running it here would refresh B and leave the
    // fan-out below testing a converged cluster.)
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({ "query": sql }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "a stale peer must refresh onto the pinned index mapping: {body}"
    );
    assert_eq!(shard_walls(&body), Some(2), "one wall per peer: {body}");
    assert_eq!(
        body["rows"][0]["n"].as_i64(),
        Some(12),
        "every row predates the widen, so all of them read null: {body}"
    );

    // Nothing in this fixture commits data, so the generation the fan-out ran
    // against is still the one the assertions above named.
    assert_eq!(
        ice_a.current_table_generation(PIN_INDEX).await.unwrap(),
        wide,
        "the index snapshot must not have moved"
    );
}

/// `n` `status=500` rows tagged `<label> row j` on `host`, so the merged answer
/// can tell one population apart from another by either column.
fn labelled_rows(label: &str, n: usize, host: &str) -> Vec<Event> {
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    (0..n)
        .map(|j| {
            let mut e = Event::now(format!("{label} row {j} status=500"));
            e.host = host.to_string();
            e.timestamp = base + Duration::seconds(1_000 + j as i64);
            e
        })
        .collect()
}

/// Carrier-map `rows` through `config` and append them to [`PIN_INDEX`],
/// stamping `props` on the commit (the compactor's `CONSUMED_SEGMENTS_PROP`).
async fn append_pin_rows(
    ice: &IcebergContext,
    config: &siglake_core::index_config::IndexConfig,
    rows: &[Event],
    props: std::collections::HashMap<String, String>,
) {
    let batch = siglake_core::events_to_record_batch(rows).unwrap();
    let mapped = siglake_core::map_carrier_batch(&batch, config).unwrap();
    ice.append_to_table_with_props(&ice.index_table_ident(PIN_INDEX), mapped, &[], props)
        .await
        .unwrap();
}

/// Seal `rows` as one un-committed segment in the index WAL under `wal_root`.
/// `writer_id` separates two segments sealed into the same directory. Returns
/// `(index WAL dir, sealed segment path)`.
fn seal_index_buffer_segment(
    wal_root: &std::path::Path,
    index: &str,
    writer_id: &str,
    rows: &[Event],
) -> (std::path::PathBuf, std::path::PathBuf) {
    let index_wal = wal_root.join("default").join(index);
    std::fs::create_dir_all(&index_wal).unwrap();
    let mut w = siglake_wal::WalWriter::with_thresholds(
        &index_wal,
        writer_id,
        10_000,
        std::time::Duration::from_secs(600),
    )
    .unwrap();
    w.append_events(rows).unwrap();
    let sealed = w.seal().unwrap().expect("sealed");
    drop(w);
    (index_wal, sealed.path)
}

/// #2584: the coordinator's BUFFER half of a managed-index fan-out, across the
/// same `update_index` mapping widen [`a_managed_index_widened_by_its_mapping_pins_like_events`]
/// pins.
///
/// That fixture runs with no `wal_buffer_dir`, so `compute_buffer_partials`
/// contributes nothing and only the workers' pinned shards are exercised. The
/// shape left untested is the one a live pod is always in: a sealed-but-
/// uncommitted index segment in flight while the mapping widens. Its rows were
/// carrier-mapped and written against the NARROW mapping, but the partial is
/// built from `read.snapshot.provider.schema()` — the WIDE schema captured with
/// the pin — and then concatenated with workers' partials that are wide because
/// the pin made them so. Both halves have to agree on the widened column or the
/// merge is a schema mismatch, and the buffered rows have to read null through
/// it rather than being dropped.
///
/// This is preventive coverage; it is not a reproduction of a shipped defect.
#[tokio::test]
async fn a_widened_managed_index_folds_its_buffer_into_the_pinned_fan_out() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let wal_root = tmp.path().join("wal");
    // The writer's own context, so neither server's cache is refreshed as a
    // side effect of creating, appending or widening.
    let writer = IcebergContext::open(&warehouse).await.unwrap();

    let config = pin_index_config();
    writer.create_index(&config).await.unwrap();
    const COMMITTED: usize = 12;
    const BUFFERED: usize = 3;
    let batch = siglake_core::events_to_record_batch(&status_500_events(COMMITTED)).unwrap();
    let mapped = siglake_core::map_carrier_batch(&batch, &config).unwrap();
    writer
        .append_to_table(&writer.index_table_ident(PIN_INDEX), mapped, &[])
        .await
        .unwrap();

    // The in-flight segment, sealed BEFORE the widen: these rows were mapped
    // through the narrow doc mapping and no commit has consumed them.
    let (index_wal, _) = seal_index_buffer_segment(
        &wal_root,
        PIN_INDEX,
        "ing-test",
        &labelled_rows("buffered", BUFFERED, "host-buffered"),
    );
    let sealed = std::fs::read_dir(index_wal.join("sealed")).unwrap().count();
    assert!(
        sealed > 0,
        "the fixture needs an un-committed segment; {} holds none",
        index_wal.display()
    );

    // B warms on the NARROW mapping and stays there.
    let ice_b = frozen(&warehouse).await;
    let narrow = ice_b.current_table_generation(PIN_INDEX).await.unwrap();
    assert!(
        narrow.snapshot_id.is_some(),
        "the fixture needs committed rows: {narrow:?}"
    );

    // The widen: one appended nullable field, no append of data.
    writer
        .update_index(&widened_pin_index_config())
        .await
        .unwrap();

    let ice_a = live(&warehouse).await;
    let wide = ice_a.current_table_generation(PIN_INDEX).await.unwrap();
    assert_eq!(
        wide.snapshot_id, narrow.snapshot_id,
        "a mapping widen must be metadata-only; if `update_index` starts \
         committing data this fixture is no longer about the pin"
    );
    assert_ne!(
        wide.schema_id, narrow.schema_id,
        "the mapping widen must move the schema id"
    );
    assert_eq!(
        ice_b.current_table_generation(PIN_INDEX).await.unwrap(),
        narrow,
        "B must still be on the narrow generation; if its cache refreshed on \
         its own, this fixture proves nothing"
    );

    let (url_a, _url_b, _ha, _hb) =
        two_peer_cluster_with_buffer(ice_a.clone(), ice_b.clone(), Some(wal_root.clone())).await;
    let client = reqwest::Client::new();
    let sql = format!(
        "SELECT count(*) AS n FROM {PIN_INDEX} \
         WHERE raw LIKE '%status=500%' AND {FUTURE_COLUMN} IS NULL"
    );

    // THE ACCEPTANCE. B is still stale here, so this one request covers both
    // halves: the workers refresh onto the pinned wide mapping, and the
    // coordinator's buffer partial — narrow-written rows read through the wide
    // captured schema — merges with them.
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({ "query": sql }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "a stale peer must refresh onto the pinned index mapping: {body}"
    );
    assert_eq!(shard_walls(&body), Some(2), "one wall per peer: {body}");
    assert_eq!(
        body["rows"][0]["n"].as_i64(),
        Some((COMMITTED + BUFFERED) as i64),
        "committed shards plus the buffer partial, every row null through the \
         widened column: {body}"
    );

    // Cross-shard merge by key: the buffered rows must arrive as their own
    // group, not be counted into the committed ones, and the per-key counts
    // must sum to the total above. (B is converged by now — this half is about
    // the merge, not about staleness.)
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": format!(
                "SELECT host, count(*) AS n FROM {PIN_INDEX} \
                 WHERE raw LIKE '%status=500%' AND {FUTURE_COLUMN} IS NULL \
                 GROUP BY host"
            )
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert_eq!(shard_walls(&body), Some(2), "one wall per peer: {body}");
    let rows = body["rows"].as_array().unwrap();
    let total: i64 = rows.iter().map(|r| r["n"].as_i64().unwrap()).sum();
    assert_eq!(
        total,
        (COMMITTED + BUFFERED) as i64,
        "per-key counts must sum to the full row count: {body}"
    );
    let buffered_group = rows
        .iter()
        .find(|r| r["host"].as_str() == Some("host-buffered"))
        .unwrap_or_else(|| panic!("the buffered rows must survive the merge as a group: {body}"));
    assert_eq!(
        buffered_group["n"].as_i64(),
        Some(BUFFERED as i64),
        "{body}"
    );

    // Nothing in this fixture commits data, so the generation the fan-out ran
    // against is still the one the assertions above named, and the segment is
    // still un-committed.
    assert_eq!(
        ice_a.current_table_generation(PIN_INDEX).await.unwrap(),
        wide,
        "the index snapshot must not have moved"
    );
    assert_eq!(
        std::fs::read_dir(index_wal.join("sealed")).unwrap().count(),
        sealed,
        "a query must not drain the buffer it read"
    );
}

/// #2661, the distributed half of
/// `indexes::a_recreated_index_serves_only_its_own_rows`.
///
/// A deleted-and-recreated index keeps its WAL directory, because
/// `resolve_index_wal_dir` keys it by tenant and index NAME. The coordinator
/// builds its buffer partial from that directory and excludes segments using
/// the REPLACEMENT table's consumed set, which is empty — so the fan-out used
/// to fold the dropped table's WAL into an answer over the replacement's
/// shards, and the two populations arrived as their own GROUP BY keys. The
/// directory's owner marker (`siglake_wal::OWNER_FILE`), stamped by the drain
/// that committed population 2 and compared by `compute_buffer_partials`, cuts
/// both off: the merged answer is the replacement's own rows, per key and in
/// total.
///
/// The line between the two ways the dropped table's rows exist on disk is
/// unchanged. Its committed PARQUET FILES stay where they are (that is what
/// `dropping_committed_index_retains_files_and_recreation_is_a_new_table`
/// records) and no shard reads them — `host-0..2` never appears.
#[tokio::test]
async fn a_recreated_index_serves_only_its_own_rows_in_the_fan_out() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let wal_root = tmp.path().join("wal");
    // `list_tenant_dirs` recognizes a tenant by its own `sealed/`; the drain
    // below needs `default` to be found that way.
    std::fs::create_dir_all(wal_root.join("default").join("sealed")).unwrap();
    let writer = IcebergContext::open(&warehouse).await.unwrap();

    let config = pin_index_config();
    writer.create_index(&config).await.unwrap();

    // The dropped table's committed rows: Iceberg files, hosts `host-0..2`.
    const DROPPED: usize = 12;
    append_pin_rows(
        &writer,
        &config,
        &status_500_events(DROPPED),
        std::collections::HashMap::new(),
    )
    .await;

    // Population 2, drained into the DROPPED table by the real compactor: it
    // names the segment in `CONSUMED_SEGMENTS_PROP`, renames the file into
    // `committed/`, and stamps the directory's owner marker with the table it
    // committed to. The marker under test is written by the shipped drain.
    const COMMITTED: usize = 2;
    let committed_rows = labelled_rows("committed", COMMITTED, "host-committed");
    let (index_wal, committed_path) =
        seal_index_buffer_segment(&wal_root, PIN_INDEX, "ing-committed", &committed_rows);
    let committed_name = committed_path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        siglake_compactor::Compactor::new(&wal_root, live(&warehouse).await)
            .run_once()
            .await
            .unwrap(),
        1,
        "the drain must commit the first segment into the ORIGINAL table"
    );
    assert!(index_wal.join("committed").join(&committed_name).exists());
    let dropped_owner =
        siglake_wal::read_wal_owner(&index_wal).expect("the drain stamps the directory's owner");

    // Population 1: sealed after that drain, so never committed anywhere.
    const SEALED: usize = 3;
    let (_, sealed_path) = seal_index_buffer_segment(
        &wal_root,
        PIN_INDEX,
        "ing-sealed",
        &labelled_rows("sealed", SEALED, "host-sealed"),
    );

    // Delete, recreate under the same name, and give the replacement its own
    // committed rows — without them the planner has no shards to split and the
    // fan-out this fixture is about would not happen.
    assert!(writer.delete_index(PIN_INDEX).await.unwrap());
    writer.create_index(&config).await.unwrap();
    const FRESH: usize = 4;
    append_pin_rows(
        &writer,
        &config,
        &labelled_rows("fresh", FRESH, "host-fresh"),
        std::collections::HashMap::new(),
    )
    .await;
    assert!(
        sealed_path.exists(),
        "deleting the index must not touch its WAL"
    );
    assert_eq!(
        siglake_wal::read_wal_owner(&index_wal).as_deref(),
        Some(dropped_owner.as_str()),
        "recreation does not touch the WAL either — the marker still names the \
         dropped table, which is what makes the segments identifiable"
    );

    // Both peers read the replacement; only the coordinator has the WAL root,
    // so the buffer partial is disjoint from the workers' shards.
    let ice_a = live(&warehouse).await;
    let ice_b = live(&warehouse).await;
    let (url_a, _url_b, _ha, _hb) =
        two_peer_cluster_with_buffer(ice_a.clone(), ice_b.clone(), Some(wal_root.clone())).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": format!(
                "SELECT host, count(*) AS n FROM {PIN_INDEX} \
                 WHERE raw LIKE '%status=500%' GROUP BY host"
            )
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert_eq!(shard_walls(&body), Some(2), "one wall per peer: {body}");
    let rows = body["rows"].as_array().unwrap();
    let by_host: std::collections::HashMap<&str, i64> = rows
        .iter()
        .map(|r| (r["host"].as_str().unwrap(), r["n"].as_i64().unwrap()))
        .collect();
    let total: i64 = by_host.values().sum();
    assert_eq!(
        total, FRESH as i64,
        "per-key counts must sum to the replacement's own row count: {body}"
    );
    assert_eq!(by_host.get("host-fresh"), Some(&(FRESH as i64)), "{body}");
    assert_eq!(
        by_host.get("host-sealed"),
        None,
        "the dropped table's un-committed segment must not reach the fan-out: {body}"
    );
    assert_eq!(
        by_host.get("host-committed"),
        None,
        "nor a segment the DROPPED table already consumed, which the \
         replacement's empty consumed set would not exclude: {body}"
    );
    for j in 0..3 {
        assert_eq!(
            by_host.get(format!("host-{j}").as_str()),
            None,
            "the dropped table's committed parquet must NOT come back: {body}"
        );
    }

    // #2835: a lane that re-resolved the index BEFORE the drain arrived has
    // already acknowledged rows for the replacement into the same directory,
    // under the dropped table's marker. The sweep below must tell them apart.
    let live_owner = live(&warehouse)
        .await
        .index_table_uuid(PIN_INDEX)
        .await
        .unwrap()
        .expect("the replacement resolves to a table");
    const REBOUND: usize = 6;
    let mut rebound_writer = siglake_wal::WalWriter::with_thresholds(
        &index_wal,
        "ing-rebound",
        10_000,
        std::time::Duration::from_secs(600),
    )
    .unwrap();
    rebound_writer
        .bind_table_uuid(Some(uuid::Uuid::parse_str(&live_owner).unwrap()))
        .unwrap();
    rebound_writer
        .append_events(&labelled_rows("rebound", REBOUND, "host-rebound"))
        .unwrap();
    let rebound_sealed = rebound_writer.seal().unwrap().expect("sealed").path;

    // The dropped incarnation's population is still on disk, quarantined
    // rather than dropped, once the drain reaches the directory — and the
    // rebound lane's segment is not swept along with it.
    assert_eq!(
        siglake_compactor::Compactor::new(&wal_root, live(&warehouse).await)
            .run_once()
            .await
            .unwrap(),
        0,
        "the drain must not commit the dropped incarnation's sealed segment"
    );
    let held = index_wal.join("stale").join(&dropped_owner);
    assert!(held.join(sealed_path.file_name().unwrap()).exists());
    assert!(held.join(&committed_name).exists());
    assert!(
        rebound_sealed.exists(),
        "the replacement's own acknowledged segment stays in sealed/"
    );

    // #2693, the same fan-out: the directory now names the REPLACEMENT, and an
    // ingester still holding the dropped incarnation's writer open seals into
    // it. The directory marker vouches for that segment; its frame header does
    // not, and the coordinator's buffer partial goes by the header.
    let mut stale_writer = siglake_wal::WalWriter::with_thresholds(
        &index_wal,
        "ing-held",
        10_000,
        std::time::Duration::from_secs(600),
    )
    .unwrap();
    stale_writer
        .bind_table_uuid(Some(uuid::Uuid::parse_str(&dropped_owner).unwrap()))
        .unwrap();
    stale_writer
        .append_events(&labelled_rows("held", 5, "host-held"))
        .unwrap();
    let contaminant = stale_writer.seal().unwrap().expect("sealed").path;
    assert_eq!(
        siglake_wal::read_wal_owner(&index_wal).as_deref(),
        Some(live_owner.as_str()),
        "the directory itself is the replacement's by now"
    );

    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": format!(
                "SELECT host, count(*) AS n FROM {PIN_INDEX} \
                 WHERE raw LIKE '%status=500%' GROUP BY host"
            )
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert_eq!(shard_walls(&body), Some(2), "one wall per peer: {body}");
    let by_host: std::collections::HashMap<&str, i64> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["host"].as_str().unwrap(), r["n"].as_i64().unwrap()))
        .collect();
    assert_eq!(
        by_host.values().sum::<i64>(),
        (FRESH + REBOUND) as i64,
        "the merged total is the replacement's own rows, committed and buffered: {body}"
    );
    assert_eq!(by_host.get("host-held"), None, "{body}");
    assert_eq!(
        by_host.get("host-rebound"),
        Some(&(REBOUND as i64)),
        "the segment the sweep spared merges into the fan-out like any other \
         in-flight data: {body}"
    );

    // And the drain holds the contaminant beside the rest instead of
    // committing it, while the rebound lane's segment goes into the
    // replacement.
    assert_eq!(
        siglake_compactor::Compactor::new(&wal_root, live(&warehouse).await)
            .run_once()
            .await
            .unwrap(),
        1
    );
    assert!(held.join(contaminant.file_name().unwrap()).exists());
    assert!(!rebound_sealed.exists(), "committed out of sealed/");
}

/// #91: EXPLAIN of an ordered browse must plan WITH the scan-order hint —
/// i.e., show the same fetch-limited merge the real query executes, not a
/// misleading blocking TopK.
#[tokio::test]
async fn explain_ordered_browse_shows_the_real_plan() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    for i in 0..3i64 {
        let events: Vec<Event> = (0..10)
            .map(|j| {
                let mut e = Event::now(format!("r{i}-{j}"));
                e.timestamp = base + Duration::seconds(i * 100 + j);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    let (url, _h) = serve(AppState::new(ice.clone(), AuthConfig::open())).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{url}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "EXPLAIN SELECT timestamp, raw FROM events ORDER BY timestamp DESC LIMIT 5",
            "default_order": false
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    let plan = body["rows"].to_string();
    assert!(
        !plan.contains("SortExec"),
        "EXPLAIN must show the early-stop plan, not a blocking TopK:\n{plan}"
    );
}

/// #94: a DEAD worker must not fail the query — the coordinator retries the
/// dead peer's shard on itself (it holds the full file set) and the merged
/// result stays exact.
#[tokio::test]
async fn dead_worker_fails_over_to_the_coordinator() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    for i in 0..4i64 {
        let events: Vec<Event> = (0..10)
            .map(|j| {
                let mut e = Event::now(format!("row {j} tag={}", j % 3));
                e.host = format!("h{}", j % 3);
                e.timestamp = base + Duration::seconds(i * 100 + j);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }

    // Coordinator A with peers = [itself, A DEAD URL] — shard 1 always fails
    // over. Port 1 is never listening.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = listener.local_addr().unwrap();
    let url_a = format!("http://{addr_a}");
    let dead = "http://127.0.0.1:1".to_string();
    let state_a = AppState::new(ice.clone(), AuthConfig::open())
        .with_coordinator(vec![url_a.clone(), dead], None);
    let app_a = router(state_a);
    let _ha = tokio::spawn(async move { axum::serve(listener, app_a).await.unwrap() });

    let client = reqwest::Client::new();
    // A filtered aggregate that genuinely fans out (Tier-1 can't serve it).
    let resp = client
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE raw LIKE '%tag=1%'"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "dead worker must not fail the query");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["rows"][0]["n"].as_i64(),
        Some(12),
        "exact through failover: {body}"
    );
    // Both shard walls present — the failed-over shard's wall includes the
    // failed attempt + the local retry.
    let dist = body["stats"]["phases"]["distributed"].as_object().unwrap();
    assert_eq!(
        dist["shard_wall_micros"].as_array().map(Vec::len),
        Some(2),
        "{body}"
    );
}

/// #967: a departed peer's shard fails over to the CAPTURED COORDINATOR URL,
/// not to `peers[0]`.
///
/// Under the static list every pod rendered the identical peer list, so
/// `peers[0]` was this process only on ordinal zero — a failover on any other
/// pod posted the shard to ordinal zero, and if ordinal zero was the pod that
/// had just left, the retry hit the same dead address the primary attempt did.
/// SRV ordering makes the positional assumption worse still. So the snapshot
/// carries an explicit self URL and failover uses that.
///
/// The fixture puts the dead peer at index 0 and this coordinator at index 1,
/// which is exactly the arrangement the old wiring could not survive: shard
/// `(0, 2)` must retry on the coordinator and the merged count must stay
/// exact. Run against the pre-fix code, the retry goes back to the dead URL
/// and the query fails.
#[tokio::test]
async fn a_departed_peer_fails_over_to_the_captured_coordinator_not_peer_zero() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 9, 7, 0, 0, 0).unwrap();
    for i in 0..4i64 {
        let events: Vec<Event> = (0..10)
            .map(|j| {
                let mut e = Event::now(format!("row {j} tag={}", j % 3));
                e.host = format!("h{}", j % 3);
                e.timestamp = base + Duration::seconds(i * 100 + j);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url_a = format!("http://{}", listener.local_addr().unwrap());
    // Port 1 is never listening: the peer that "left".
    let departed = "http://127.0.0.1:1".to_string();
    let directory = Arc::new(PeerDirectory::new("coordinator"));
    directory.publish(vec![departed, url_a.clone()], url_a.clone());
    let state_a = AppState::new(ice.clone(), AuthConfig::open())
        .with_peer_source(Some(PeerSource::from_directory(directory)), None);
    let app_a = router(state_a);
    let _ha = tokio::spawn(async move { axum::serve(listener, app_a).await.unwrap() });

    let resp = reqwest::Client::new()
        .post(format!("{url_a}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE raw LIKE '%tag=1%'"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "a departed peer at index 0 must not fail the query"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["rows"][0]["n"].as_i64(),
        Some(12),
        "exact through failover — shard (0,2) ran once, on the coordinator: {body}"
    );
    let dist = body["stats"]["phases"]["distributed"].as_object().unwrap();
    assert_eq!(
        dist["shard_wall_micros"].as_array().map(Vec::len),
        Some(2),
        "{body}"
    );
    assert_eq!(dist["peers"].as_u64(), Some(2), "{body}");
}

/// #967: under discovery, a pod with no published membership yet serves
/// `/api/v1/sql` LOCALLY (correct, merely not distributed) while the explicit
/// `/api/v1/sql/distributed` says so instead of silently running single-pod.
#[tokio::test]
async fn before_the_first_membership_transparent_sql_runs_locally() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 9, 7, 0, 0, 0).unwrap();
    let events: Vec<Event> = (0..10)
        .map(|j| {
            let mut e = Event::now(format!("startup row {j} tag={}", j % 3));
            e.host = format!("h{}", j % 3);
            e.timestamp = base + Duration::seconds(j);
            e
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    // A discovery directory that has published nothing — a pod between start
    // and its first usable SRV answer.
    let directory = Arc::new(PeerDirectory::new("coordinator"));
    let (url, _h) = serve(
        AppState::new(ice.clone(), AuthConfig::open())
            .with_peer_source(Some(PeerSource::from_directory(directory)), None),
    )
    .await;
    let client = reqwest::Client::new();
    let query = serde_json::json!({
        "query": "SELECT count(*) AS n FROM events WHERE raw LIKE '%tag=1%'"
    });

    let resp = client
        .post(format!("{url}/api/v1/sql"))
        .json(&query)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    // j in 0..10 with j % 3 == 1 ⇒ rows 1, 4, 7.
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(3), "{body}");
    assert!(
        body["stats"]["phases"]["distributed"].is_null(),
        "no membership ⇒ no fan-out attribution: {body}"
    );

    let explicit = client
        .post(format!("{url}/api/v1/sql/distributed"))
        .json(&query)
        .send()
        .await
        .unwrap();
    assert_eq!(
        explicit.status(),
        400,
        "the explicit endpoint reports the missing membership rather than \
         quietly running single-pod"
    );
}

/// Per-request scan attribution: an executed scan carries `stats.scan` with
/// the planned-vs-read file accounting, while a Tier-1 metadata answer stays
/// visibly scan-free (`scan` absent). Guards the whole chain: reader
/// `ScanCounters` → scan-node metrics → `summarize_plan_runtime` → response.
#[tokio::test]
async fn scan_detail_attributes_the_read() {
    require_loopback!();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    for i in 0..4 {
        let batch: Vec<Event> = (0..25)
            .map(|j| {
                let mut e = Event::now(format!("row {j} marker-{i}"));
                e.host = format!("host-{}", j % 4);
                e.timestamp = base + Duration::seconds((i * 100 + j) as i64);
                e
            })
            .collect();
        ice.append_events(&batch).await.unwrap();
    }

    let (url, _h) = serve(AppState::new(ice.clone(), AuthConfig::open())).await;
    let client = reqwest::Client::new();

    // A raw scan with a residual filter: every file is planned + read.
    let resp = client
        .post(format!("{url}/api/v1/sql"))
        .json(&serde_json::json!({
            "query": "SELECT host FROM events WHERE host = 'host-2'"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let scan = body["stats"]["scan"]
        .as_object()
        .unwrap_or_else(|| panic!("scanning query must carry stats.scan: {body}"));
    assert_eq!(scan["files_planned"].as_u64(), Some(4), "{body}");
    assert_eq!(scan["files_read"].as_u64(), Some(4), "{body}");
    assert!(
        scan["row_groups_considered"].as_u64().unwrap() >= 4,
        "{body}"
    );
    assert_eq!(
        scan["row_groups_read"].as_u64(),
        scan["row_groups_considered"].as_u64(),
        "nothing prunes an equality host filter here: {body}"
    );
    assert!(scan["object_store_reads"].as_u64().unwrap() > 0, "{body}");
    assert!(scan["decoded_bytes"].as_u64().unwrap() > 0, "{body}");
    // Planning-time attribution rides on the cost block.
    assert_eq!(body["cost"]["files_considered"].as_u64(), Some(4), "{body}");

    // A Tier-1 metadata answer executes no data-file scan — `scan` absent.
    let resp = client
        .post(format!("{url}/api/v1/sql"))
        .json(&serde_json::json!({ "query": "SELECT count(*) AS n FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(100), "{body}");
    assert!(
        body["stats"]["scan"].is_null(),
        "Tier-1 answers must stay visibly scan-free: {body}"
    );
}
