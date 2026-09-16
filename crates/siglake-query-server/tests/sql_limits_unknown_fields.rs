//! A misspelt per-request limit is refused, on every route that takes one.
//!
//! THE CONTRACT THIS PINS. `limits` carries safety constraints the caller
//! asked for — a row cap, a byte cap, a timeout. Until `RequestLimits` grew
//! `#[serde(deny_unknown_fields)]`, `{"limits":{"max_rows":5000}}` parsed to an
//! EMPTY `RequestLimits`: the cap silently vanished and the tier default
//! (10,000,000 interactive / 100,000 batch) applied, with a 200 and nothing in
//! the body to say so. The docs site named that exact spelling for a while
//! (siglake-docs #2177), so the typo was being sent, not imagined.
//!
//! The status is 422, not 400: these handlers take `Json<SqlRequest>` and
//! pinned axum 0.8.9 maps a deserialization *data* error to
//! `UNPROCESSABLE_ENTITY`. That is the existing extractor boundary and this
//! change does not move it — there is no custom mapper here and the body is
//! axum's plain-text rejection, not an `ApiErrorBody`.
//!
//! Every refusal is proved to land BEFORE the handler by choosing bodies whose
//! handler answer would be a different, recognisable status: unparseable SQL
//! (400 from planning), `/distributed` with no membership (400 before
//! anything), a batch submission (202 plus a live job). Strictness is scoped
//! to `limits`: an unknown key at the top level of `SqlRequest` is still
//! ignored, and that is asserted too.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use siglake_query_server::{router, AppState, AuthConfig, JobStore};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

/// Every route whose body is a `SqlRequest`, i.e. every route that reads
/// `limits`. `/api/v1/sql/shard` is deliberately absent: it takes a
/// `ShardQueryRequest` and resolves `RequestLimits::default()`, so it has no
/// caller-supplied limits to mis-spell.
const SQL_ROUTES: [&str; 4] = [
    "/api/v1/sql",
    "/api/v1/sql/local",
    "/api/v1/sql/explain",
    "/api/v1/sql/distributed",
];

/// SQL that no planner accepts. If a request ever reaches the handler, the
/// answer is 400 — so a 422 can only have come from the extractor.
const UNPLANNABLE_SQL: &str = "SELECT FROM WHERE";

struct Harness {
    app: Router,
    jobs: Arc<JobStore>,
    _tmp: tempfile::TempDir,
}

async fn harness() -> Harness {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let state =
        AppState::new(ice, AuthConfig::open()).with_jobs(JobStore::new(1, Duration::from_secs(30)));
    Harness {
        jobs: Arc::clone(&state.jobs),
        app: router(state),
        _tmp: tmp,
    }
}

async fn post(app: &Router, path: &str, body: serde_json::Value) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn body_text(response: axum::response::Response) -> String {
    String::from_utf8_lossy(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).into_owned()
}

#[tokio::test]
async fn the_documented_misspelling_of_the_row_cap_is_refused_on_every_sql_route() {
    let h = harness().await;
    for path in SQL_ROUTES {
        let response = post(
            &h.app,
            path,
            serde_json::json!({
                "query": UNPLANNABLE_SQL,
                "limits": {"max_rows": 5000},
            }),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{path} must refuse a dropped row cap rather than apply the tier default"
        );
        let body = body_text(response).await;
        assert!(
            body.contains("unknown field") && body.contains("max_rows"),
            "{path} must name the offending key, got: {body}"
        );
    }
}

#[tokio::test]
async fn another_unknown_nested_key_is_refused_on_every_sql_route() {
    let h = harness().await;
    for path in SQL_ROUTES {
        let response = post(
            &h.app,
            path,
            serde_json::json!({
                "query": UNPLANNABLE_SQL,
                "limits": {"timeout_secs": 30},
            }),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{path}"
        );
    }
}

/// The dangerous shape: a typo riding alongside a limit that IS honoured, so
/// the request looks accepted and only half-governed.
#[tokio::test]
async fn an_unknown_key_beside_a_valid_limit_is_refused_on_every_sql_route() {
    let h = harness().await;
    for path in SQL_ROUTES {
        let response = post(
            &h.app,
            path,
            serde_json::json!({
                "query": UNPLANNABLE_SQL,
                "limits": {"timeout_seconds": 30, "max_rows": 5000},
            }),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{path}"
        );
    }
}

/// POSITIVE CONTROL for "before planning". The same bodies without the typo
/// reach the handler and are answered 400 — the planner's verdict on
/// `UNPLANNABLE_SQL`, or `/distributed`'s "no membership". A 400 here and a
/// 422 above is the whole proof that nothing ran.
#[tokio::test]
async fn the_same_request_without_the_typo_reaches_the_handler() {
    let h = harness().await;
    for path in SQL_ROUTES {
        let response = post(
            &h.app,
            path,
            serde_json::json!({
                "query": UNPLANNABLE_SQL,
                "limits": {"timeout_seconds": 30, "max_rows_returned": 5000},
            }),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{path} must have reached the handler with the canonical spelling"
        );
    }
}

/// A batch submission is refused before it is ENQUEUED, not after: the job
/// store must be untouched. The positive control in the same test submits the
/// canonical spelling and gets a 202 plus a job, so the assertion is about the
/// typo and not about a store that never accepts anything.
#[tokio::test]
async fn a_batch_submission_with_a_misspelt_limit_never_enqueues() {
    let h = harness().await;
    let refused = post(
        &h.app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "priority": "batch",
            "limits": {"max_rows": 5000},
        }),
    )
    .await;
    assert_eq!(refused.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        h.jobs.active_count().await,
        0,
        "a refused batch submission must not create a job"
    );

    let accepted = post(
        &h.app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "priority": "batch",
            "limits": {"max_rows_returned": 5000},
        }),
    )
    .await;
    assert_eq!(
        accepted.status(),
        StatusCode::ACCEPTED,
        "the canonical spelling must still submit"
    );
    let body: serde_json::Value = serde_json::from_str(&body_text(accepted).await).unwrap();
    assert!(
        body.get("job_id").is_some(),
        "a submitted batch job must report its id, got: {body}"
    );
}

/// Three rows, no table, so the cap is the only thing that can clip the
/// answer. The envelope reports `max_rows` only when it truncated, which is
/// exactly the signal the misspelling used to destroy.
const THREE_ROWS: &str = "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3";

/// POSITIVE CONTROL: the canonical cap is APPLIED, not merely accepted. A
/// tripped row cap is this server's 413 — the truncated `RecordsResponse` —
/// and the envelope names the cap in `max_rows`, which is the reporting
/// spelling the request typo came from.
#[tokio::test]
async fn the_canonical_row_cap_is_applied_to_the_response() {
    let h = harness().await;
    let response = post(
        &h.app,
        "/api/v1/sql/local",
        serde_json::json!({
            "query": THREE_ROWS,
            "limits": {"max_rows_returned": 2},
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
    assert_eq!(
        body.get("row_count").and_then(serde_json::Value::as_u64),
        Some(2),
        "the requested cap must clip the answer, got: {body}"
    );
    assert_eq!(
        body.get("truncated").and_then(serde_json::Value::as_bool),
        Some(true),
        "got: {body}"
    );
    assert_eq!(
        body.get("max_rows").and_then(serde_json::Value::as_u64),
        Some(2),
        "the envelope must report the cap it applied, got: {body}"
    );
}

/// POSITIVE CONTROLS for the shapes strictness must NOT break: an omitted
/// `limits`, an empty one, and a request that asks for far more than the
/// ceiling (clamped, never refused). None of the three clips a 3-row answer.
#[tokio::test]
async fn omitted_empty_and_over_ceiling_limits_all_still_succeed() {
    let h = harness().await;
    let cases = [
        ("omitted", serde_json::json!({"query": THREE_ROWS})),
        (
            "empty",
            serde_json::json!({"query": THREE_ROWS, "limits": {}}),
        ),
        (
            "over the ceiling",
            serde_json::json!({
                "query": THREE_ROWS,
                "limits": {"max_rows_returned": 99_000_000_000u64},
            }),
        ),
    ];
    for (label, request) in cases {
        let response = post(&h.app, "/api/v1/sql/local", request).await;
        assert_eq!(response.status(), StatusCode::OK, "{label}");
        let body: serde_json::Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(
            body.get("row_count").and_then(serde_json::Value::as_u64),
            Some(3),
            "{label} must return the whole answer, got: {body}"
        );
        // `truncated` is skipped when false, so absent is the success shape.
        assert!(
            !body
                .get("truncated")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            "{label} must not truncate, got: {body}"
        );
    }
}

/// The change is SCOPED. `SqlRequest` itself stays permissive, so a client
/// sending an extra top-level key (a future field, a tracing hint) is not
/// broken by this. If that ever tightens it should be a deliberate, separate
/// decision — this test is the tripwire.
#[tokio::test]
async fn an_unknown_top_level_key_is_still_ignored() {
    let h = harness().await;
    let response = post(
        &h.app,
        "/api/v1/sql/local",
        serde_json::json!({
            "query": "SELECT 1 AS n",
            "no_such_field": "ignored",
            "limits": {"max_rows_returned": 7},
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}
