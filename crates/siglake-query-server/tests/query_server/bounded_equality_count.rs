//! #7787 through the real request path: `POST /api/v1/sql`, one execution,
//! router to response body.
//!
//! On the aged `query_audit` table a successful cold equality `COUNT`
//! reported 43,042 outstanding delta names scheduled ahead of an 8-file
//! footer fallback, and such counts ran 17.7s and 21.97s against a 30s
//! deadline. A positive `=` / `IN` count now takes a lookup that will not
//! fold a wide aggregate still COLD for the generation, and continues to the
//! footer tier instead.
//!
//! What these tests can and cannot see. They assert the count and
//! `stats.served_by` on the response a client receives, across cold and warm
//! generations and with sealed-but-uncommitted WAL rows in play — none of
//! which a test that calls the fast path and then runs the plan separately
//! can observe. They do NOT prove the absence of the fold: the I/O trap is
//! `#[cfg(test)]` inside the storage crate and is asserted there, in
//! `iceberg::bounded_predicate_count_tests`.
//!
//! The sibling contracts live in `promoted_prune.rs` and `typed_promotion.rs`:
//! a Tier-1 miss must still be served by the footers at zero scan. Gate 3409
//! failed both when this path sent every miss to a scan.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use serde_json::Value;
use tower::util::ServiceExt;

use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};

async fn sql(app: &Router, query: &str) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({ "query": query })).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(status, StatusCode::OK, "{query}: {body}");
    body
}

fn count(body: &Value) -> i64 {
    body["rows"][0]["n"]
        .as_i64()
        .unwrap_or_else(|| panic!("no count in {body}"))
}

/// Which route produced the answer. `"scan"` is the filtered fallback; a
/// metadata answer reports no scan at all.
fn served_by(body: &Value) -> String {
    body["stats"]["served_by"]
        .as_str()
        .unwrap_or("metadata")
        .to_string()
}

fn host_events(range: std::ops::Range<usize>) -> Vec<Event> {
    range
        .map(|i| {
            let mut event = Event::now(format!("event-{i}"));
            event.host = format!("host-{i:05}");
            event
        })
        .collect()
}

fn tuning() -> IcebergTuning {
    IcebergTuning {
        // Above the 4096-entry inline cap, so `host` is answerable only by the
        // wide aggregate or the footer tier, which is where a cold lookup must
        // land without folding.
        table_group_count_cardinality: Some(10_000),
        result_caches: Some(false),
        ..Default::default()
    }
}

/// 4,100 distinct hosts across two commits, plus one SEALED BUT UNCOMMITTED
/// row for `host-00007` in the WAL buffer. The committed answer for that host
/// is 1; the answer a client must see is 2.
async fn fixture(tmp: &tempfile::TempDir, buffered: bool) -> Router {
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_tuning(tuning());
    ice.append_events(&host_events(0..4_000)).await.unwrap();
    ice.append_events(&host_events(4_000..4_100)).await.unwrap();
    drop(ice);

    let wal_root = tmp.path().join("wal");
    if buffered {
        let mut extra = Event::now("buffered");
        extra.host = "host-00007".to_string();
        let mut writer = siglake_wal::WalWriter::with_thresholds(
            &wal_root,
            "bounded-equality",
            10_000,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        writer.append_events(&[extra]).unwrap();
        writer.seal().unwrap().expect("sealed buffered segment");
        drop(writer);
    }

    let server = Arc::new(
        IcebergContext::open(&warehouse)
            .await
            .unwrap()
            .with_tuning(tuning()),
    );
    let mut state = AppState::new(server, AuthConfig::open());
    if buffered {
        state = state.with_wal_buffer_dir(Some(wal_root));
    }
    router(state)
}

#[tokio::test]
async fn a_cold_equality_count_is_exact_and_still_costs_no_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let app = fixture(&tmp, false).await;

    // Twice in the same process. One request each: the declined fold, the
    // tier that answers instead, and the count, all inside the execution the
    // client waited on.
    for attempt in 1..=2 {
        let body = sql(
            &app,
            "SELECT count(*) AS n FROM events WHERE host = 'host-00007'",
        )
        .await;
        assert_eq!(count(&body), 1, "attempt {attempt}: {body}");
        assert_eq!(
            served_by(&body),
            "metadata",
            "attempt {attempt}: declining the fold must not cost a scan: {body}"
        );
    }

    let body = sql(
        &app,
        "SELECT count(*) AS n FROM events WHERE host IN ('host-00007', 'host-00008')",
    )
    .await;
    assert_eq!(count(&body), 2, "{body}");
    assert_eq!(served_by(&body), "metadata", "{body}");

    // The other polarity cannot afford to lose Tier-1 and keeps folding.
    let body = sql(
        &app,
        "SELECT count(*) AS n FROM events WHERE host != 'host-00007'",
    )
    .await;
    assert_eq!(count(&body), 4_099, "{body}");
    assert_eq!(
        served_by(&body),
        "metadata",
        "a negation must still be served without scanning: {body}"
    );
}

#[tokio::test]
async fn buffered_rows_are_counted_by_the_fallback_and_by_the_warm_aggregate() {
    let tmp = tempfile::tempdir().unwrap();
    let app = fixture(&tmp, true).await;
    let equality = "SELECT count(*) AS n FROM events WHERE host = 'host-00007'";

    // Cold: the fold is declined, the footer tier serves the committed rows,
    // and the #61 delta adds the sealed-but-uncommitted one. A path that lost
    // the buffered row would be a silent wrong count, not an error.
    let body = sql(&app, equality).await;
    assert_eq!(count(&body), 2, "the buffered row must be counted: {body}");
    assert_eq!(served_by(&body), "metadata", "{body}");

    // Warm: some other query folds the wide aggregate for this generation
    // through the same server, and the fast path serves the shape again — the
    // committed counts plus the same buffered delta, at zero scan.
    let grouped = sql(
        &app,
        "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 1",
    )
    .await;
    assert_eq!(count(&grouped), 2, "{grouped}");

    let body = sql(&app, equality).await;
    assert_eq!(count(&body), 2, "{body}");
    assert_eq!(
        served_by(&body),
        "metadata",
        "an aggregate already folded for this generation must still serve: {body}"
    );
}

/// A commit under a warm aggregate puts the next request back on a cold entry.
/// It must decline the fold again and stay exact for the NEW generation,
/// rather than serve the older one.
#[tokio::test]
async fn a_commit_under_a_warm_aggregate_returns_the_request_to_a_cold_entry() {
    let tmp = tempfile::tempdir().unwrap();
    // One context, shared with the server: the later commit must invalidate
    // the same table cache the requests read, which is what a live ingest
    // does and what a second handle would only do after its TTL.
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap()
            .with_tuning(tuning()),
    );
    ice.append_events(&host_events(0..4_000)).await.unwrap();
    ice.append_events(&host_events(4_000..4_100)).await.unwrap();

    let app = router(AppState::new(ice.clone(), AuthConfig::open()));
    let equality = "SELECT count(*) AS n FROM events WHERE host = 'host-00007'";

    sql(
        &app,
        "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 1",
    )
    .await;
    let body = sql(&app, equality).await;
    assert_eq!(count(&body), 1, "{body}");
    assert_eq!(served_by(&body), "metadata", "warm: {body}");

    // A second row for the same host lands. The aggregate that answered above
    // describes the previous snapshot and must not answer for this one.
    let mut extra = Event::now("later");
    extra.host = "host-00007".to_string();
    ice.append_events(&[extra]).await.unwrap();

    let body = sql(&app, equality).await;
    assert_eq!(count(&body), 2, "the new generation's answer: {body}");
    assert_eq!(
        served_by(&body),
        "metadata",
        "a fresh generation is cold again, and still must not cost a scan: {body}"
    );
}
