//! The SQL result cache must not serve one request's answer to a request that
//! is entitled to a DIFFERENT one.
//!
//! THE DEFECTS THIS PINS. The key was `(namespace, snapshot, limits, SQL)`, and
//! the lookup ran before the request's shard selector was even resolved. Two
//! wrong-answer paths followed, both independent of the query text:
//!
//!   1. **Exactness.** `exact: true` is the caller's opt-out from an approximate
//!      top-K, enforced inside the group-count fast path — which a cache hit
//!      never reaches. One ordinary request warming a sketch answer made every
//!      later `exact: true` request replay it, error bound and all, for as long
//!      as the snapshot stood still.
//!   2. **Shard scope.** `shard` is a public field on `/api/v1/sql` and
//!      `/api/v1/sql/local`, and it restricts the scan to a subset of the file
//!      set. Whichever request arrived first defined the answer for all of them:
//!      a whole-table count served to a shard, or a shard's partial served as
//!      the whole table.
//!
//! Both are exercised through the real router with real cache hits, and the
//! `outcome` counter is read on every step so a green run cannot be one where
//! the cache was simply never consulted. Same-mode repeats must still HIT: the
//! fix separates modes, it does not disable caching.
//!
//! ONE test function, and its own binary, on purpose: the result cache and the
//! metrics recorder are both process-wide, so a sibling test's queries would
//! read as this test's cache traffic.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};
use siglake_storage::ScanShard;
use tower::util::ServiceExt;

/// Cache-outcome counts SINCE THE LAST CALL (`snapshot()` drains), keyed by the
/// `outcome` label: `hit`, `miss`, `insert`, ….
fn outcomes(snapshotter: &Snapshotter) -> HashMap<String, u64> {
    let mut by_outcome = HashMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        if key.key().name() != "siglake_query_sql_result_cache_requests_total" {
            continue;
        }
        let Some(outcome) = key
            .key()
            .labels()
            .find(|l| l.key() == "outcome")
            .map(|l| l.value().to_string())
        else {
            continue;
        };
        if let DebugValue::Counter(c) = value {
            *by_outcome.entry(outcome).or_insert(0) += c;
        }
    }
    by_outcome
}

fn hits(snapshotter: &Snapshotter) -> u64 {
    outcomes(snapshotter).get("hit").copied().unwrap_or(0)
}

async fn post_sql(app: &Router, body: serde_json::Value) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql/local")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{body}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

/// Six commits, so the file set splits across shards. Every row matches the
/// query's `LIKE`, which makes each shard's count exactly the row count of the
/// files it owns.
async fn sharded_table(ice: &IcebergContext) {
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    for file in 0..6i64 {
        let events: Vec<Event> = (0..300i64)
            .map(|i| {
                let k = file * 300 + i;
                let mut e = Event::now(format!("row {k} needle"));
                e.timestamp = base + ChronoDuration::milliseconds(k * 100);
                e.host = format!("host-{}", k % 4);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
}

/// `host` far over the exact-aggregate cap (so only the sketch can answer it),
/// with a tie-free top-10 to assert against. The cap must exceed the inline
/// ceiling (4,096) or the sketch is switched off entirely — see
/// `siglake-storage/tests/storage/agg_sketch_fallback.rs`, whose plan this
/// mirrors at a tenth of the rows.
const CAP: usize = 8_192;
const HEAVY: usize = 20;
const TAIL: usize = 9_000;

async fn wide_column_table(ice: &IcebergContext) {
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let mut hosts: Vec<String> = Vec::new();
    for r in 0..HEAVY {
        for _ in 0..(200 - r) {
            hosts.push(format!("h{r:06}"));
        }
    }
    for r in 0..TAIL {
        let name = format!("h{:06}", HEAVY + r);
        hosts.push(name.clone());
        hosts.push(name);
    }
    // ONE commit: split into several, each could fall under the cardinality cap
    // and be tallied exactly, which is the path this table exists to avoid.
    let events: Vec<Event> = hosts
        .iter()
        .enumerate()
        .map(|(i, host)| {
            let mut e = Event::now(format!("row {i}"));
            e.timestamp = base + ChronoDuration::milliseconds(i as i64);
            e.host = host.clone();
            e
        })
        .collect();
    ice.append_events(&events).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cached_answer_never_crosses_a_serving_mode() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    // ===== Shard scope =====================================================
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    sharded_table(&ice).await;
    let app = router(AppState::new(ice.clone(), AuthConfig::open()));

    // The truth to judge every shard answer against, computed from the live
    // file list with the same ownership function the scan uses.
    let files = ice.live_data_files(ice.events_table_ident()).await.unwrap();
    let total_rows: u64 = files.iter().map(|f| f.record_count()).sum();
    assert_eq!(total_rows, 1_800);
    let expected = |count: usize| -> Vec<u64> {
        (0..count)
            .map(|index| {
                let shard = ScanShard::new(index, count).expect("a real shard");
                files
                    .iter()
                    .filter(|f| shard.owns(f.file_path()))
                    .map(|f| f.record_count())
                    .sum()
            })
            .collect()
    };
    // A split that actually splits: with every file in one shard the contamination
    // being tested would be indistinguishable from a correct answer. File paths
    // carry a UUID, so which `count` splits them is not predictable — pick the
    // first that does.
    //
    // "Splits" is exactly that: no single shard owns every file. Demanding every
    // shard own something is a stricter thing than this test needs and is a
    // flake — six hashed paths leave one of four shards empty most of the time,
    // and 2, 3 and 4 can all leave one empty together (CI, 2026-09-08). An empty
    // shard still detects the defect, because the whole table's 1,800 is not 0.
    let splits_evenly = |count: usize| expected(count).iter().all(|rows| *rows > 0);
    let splits_at_all = |count: usize| expected(count).iter().all(|rows| *rows < total_rows);
    let shard_count = (2..=4)
        .find(|count| splits_evenly(*count))
        .or_else(|| (2..=8).find(|count| splits_at_all(*count)))
        .expect("6 files must fall in more than one shard for some count in 2..=8");
    let per_shard = expected(shard_count);

    let sql = "SELECT count(*) AS n FROM events WHERE raw LIKE '%needle%'";
    // `max_rows_returned` is part of the key, so a fresh value gives each
    // scenario below its own key space (the cache is process-wide and this
    // table is queried repeatedly).
    let count_request = |max_rows: usize, shard: Option<(usize, usize)>| {
        let mut body = serde_json::json!({
            "query": sql,
            "limits": { "max_rows_returned": max_rows },
        });
        if let Some((index, count)) = shard {
            body["shard"] = serde_json::json!({ "index": index, "count": count });
        }
        body
    };
    let count_of = |body: &serde_json::Value| -> u64 {
        body["rows"][0]["n"]
            .as_u64()
            .unwrap_or_else(|| panic!("no count in {body}"))
    };

    // Cold whole-table: a real scan (a metadata fast path would make the shard
    // comparison below meaningless — it ignores the file set).
    let _ = outcomes(&snapshotter);
    let whole = post_sql(&app, count_request(1_001, None)).await;
    assert_eq!(count_of(&whole), total_rows);
    assert_eq!(
        whole["stats"]["served_by"].as_str(),
        Some("scan"),
        "this shape must scan, not be answered from metadata: {whole}"
    );
    let cold = outcomes(&snapshotter);
    assert_eq!(cold.get("miss").copied(), Some(1), "{cold:?}");
    assert_eq!(cold.get("insert").copied(), Some(1), "{cold:?}");

    // Warm whole-table: a REAL hit, and the same answer.
    let warm = post_sql(&app, count_request(1_001, None)).await;
    assert_eq!(warm["rows"], whole["rows"]);
    assert_eq!(
        hits(&snapshotter),
        1,
        "an immutable same-mode repeat must still be served from the cache"
    );

    // Warmed whole table ⇒ a shard request must not be given it. Under the
    // defect every shard replayed the whole-table count, so the parts summed to
    // `shard_count * total_rows`.
    let mut summed = 0;
    for (index, want) in per_shard.iter().enumerate() {
        let body = post_sql(&app, count_request(1_001, Some((index, shard_count)))).await;
        assert_eq!(
            count_of(&body),
            *want,
            "shard {index}/{shard_count} must count only the files it owns: {body}"
        );
        summed += count_of(&body);
    }
    assert_eq!(summed, total_rows, "the shards must partition the table");

    // A shard's own repeat still hits, and one shard's entry never answers
    // another shard's question or the whole table's. Fresh key space, shards
    // first this time, so a leak would flow the other way.
    let _ = outcomes(&snapshotter);
    for (index, want) in per_shard.iter().enumerate() {
        for _ in 0..2 {
            let body = post_sql(&app, count_request(1_002, Some((index, shard_count)))).await;
            assert_eq!(count_of(&body), *want, "{body}");
        }
    }
    let shard_first = post_sql(&app, count_request(1_002, None)).await;
    assert_eq!(
        count_of(&shard_first),
        total_rows,
        "a shard's partial was served as the whole table: {shard_first}"
    );
    let shard_outcomes = outcomes(&snapshotter);
    assert_eq!(
        shard_outcomes.get("hit").copied(),
        Some(shard_count as u64),
        "each shard's second request must be a cache hit: {shard_outcomes:?}"
    );

    // ===== Exactness =======================================================
    let tmp2 = tempfile::tempdir().unwrap();
    let wide = Arc::new(
        IcebergContext::open(&tmp2.path().join("warehouse"))
            .await
            .unwrap()
            .with_tuning(IcebergTuning {
                table_group_count_cardinality: Some(CAP),
                group_count_sketch_counters: Some(2048),
                ..Default::default()
            }),
    );
    wide_column_table(&wide).await;
    let wide_app = router(AppState::new(wide, AuthConfig::open()));

    let top_k = "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 10";
    let top_k_request = |exact: bool| {
        serde_json::json!({
            "query": top_k,
            "exact": exact,
            "limits": { "max_rows_returned": 1_003 },
        })
    };

    // Default request: `host` is over the cap, so this is the sketch answer —
    // labelled, per `RecordsResponse::approximation`.
    let _ = outcomes(&snapshotter);
    let approximate = post_sql(&wide_app, top_k_request(false)).await;
    assert!(
        !approximate["approximation"].is_null(),
        "the precondition failed: this query was answered exactly, so no \
         approximation can leak into the exact request below: {approximate}"
    );
    // Warm it, and confirm the approximate mode keeps its own hits.
    let approximate_again = post_sql(&wide_app, top_k_request(false)).await;
    assert_eq!(approximate_again["rows"], approximate["rows"]);
    assert_eq!(
        approximate_again["approximation"],
        approximate["approximation"]
    );
    assert_eq!(
        hits(&snapshotter),
        1,
        "the approximate repeat must be served from the cache"
    );

    // THE ACCEPTANCE CRITERION: with the approximate answer warm, `exact: true`
    // must not be handed it.
    let exact = post_sql(&wide_app, top_k_request(true)).await;
    assert!(
        exact["approximation"].is_null(),
        "an `exact: true` request was served the warmed approximation: {exact}"
    );
    let want: Vec<serde_json::Value> = (0..10)
        .map(|r| serde_json::json!({ "host": format!("h{r:06}"), "n": 200 - r }))
        .collect();
    assert_eq!(
        exact["rows"],
        serde_json::Value::Array(want),
        "the exact top-10 is the plan's, tie-free by construction: {exact}"
    );
    let exact_outcomes = outcomes(&snapshotter);
    assert_eq!(
        exact_outcomes.get("hit").copied().unwrap_or(0),
        0,
        "the exact request must MISS the approximate entry: {exact_outcomes:?}"
    );
    assert_eq!(
        exact_outcomes.get("miss").copied().unwrap_or(0),
        1,
        "the exact request must reach the query, not the cache: {exact_outcomes:?}"
    );

    // The exact mode caches on its own key…
    let exact_again = post_sql(&wide_app, top_k_request(true)).await;
    assert_eq!(exact_again["rows"], exact["rows"]);
    assert!(exact_again["approximation"].is_null());
    assert_eq!(
        hits(&snapshotter),
        1,
        "an exact repeat must be served from the exact entry"
    );
    // …and it did not overwrite the approximate one.
    let approximate_third = post_sql(&wide_app, top_k_request(false)).await;
    assert_eq!(approximate_third["rows"], approximate["rows"]);
    assert_eq!(
        approximate_third["approximation"], approximate["approximation"],
        "the exact answer displaced the approximate entry"
    );
    assert_eq!(hits(&snapshotter), 1);
}
