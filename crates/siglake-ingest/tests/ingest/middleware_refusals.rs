//! The two ingest middlewares answer on routes that look like they could not
//! possibly care: the ES pings and the search-family stubs.
//!
//! `docs/api/openapi-ingest.yaml` declares `401` and `429` on every operation
//! behind them (pinned spec-side by `middleware_refusals_are_declared` in
//! `siglake-openapi`). A spec claim is only worth the behaviour it describes,
//! so the claim is measured here: a `HEAD /` probe is rate-limited like any
//! write. The search-family stubs carry no spec claim at all any more — they
//! are the one undocumented family (`es_read_stubs`) — so for them this file is
//! the whole record: a stub that answers `501` to everyone still answers `401`
//! to a caller without credentials.
//!
//! The layer ORDER is part of it. `ingest_rate_limit_middleware` is applied after
//! `ingest_auth_middleware` in `router()` and axum runs `route_layer`s
//! bottom-up, so the rate limiter sees the request first — a throttled caller
//! gets `429` even when it sent no credentials at all, which is what the `429`
//! descriptions say.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use tokio::sync::Mutex;
use tower::util::ServiceExt;

use siglake_ingest::rate_limit::RateLimiter;
use siglake_ingest::{router, AppState, AuthTokens, TenantRouting};
use siglake_wal::WalWriter;

const TOKEN: &str = "s3cret";

/// Routes that ignore the tenant and (for the stubs) refuse everything with
/// `501` — the ones whose middleware statuses a client cannot infer from the
/// path.
const IGNORING: &[(&str, &str)] = &[
    ("GET", "/"),
    ("HEAD", "/"),
    ("GET", "/_cluster/health"),
    ("GET", "/api/v1/_elastic/_cluster/health"),
    ("POST", "/api/v1/_elastic/_search"),
    ("GET", "/api/v1/_elastic/_msearch"),
    ("POST", "/api/v1/_elastic/_field_caps"),
    ("GET", "/api/v1/_elastic/_search/scroll"),
    ("GET", "/api/v1/_elastic/_cat/indices"),
];

/// A search-family or `_cat` stub, i.e. one that answers `501` when nothing
/// refuses it first. The two `_cluster/health` routes are the only
/// `_elastic`-prefixed entries in [`IGNORING`] that are not stubs.
fn is_stub(uri: &str) -> bool {
    uri.starts_with("/api/v1/_elastic/") && !uri.contains("_cluster/health")
}

fn app_with(tokens: Option<&str>, rate: Option<(f64, f64)>) -> (Router, tempfile::TempDir) {
    app_with_routing(tokens, rate, TenantRouting::default())
}

fn app_with_routing(
    tokens: Option<&str>,
    rate: Option<(f64, f64)>,
    tenant_routing: TenantRouting,
) -> (Router, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let writer =
        WalWriter::with_thresholds(tmp.path(), "test", 64, Duration::from_secs(60)).unwrap();
    let state = AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(writer)),
        tenants: None,
        allowed_tenants: None,
        tenant_routing,
        max_tenants: 0,
        tenant_admission: Default::default(),
        backpressure: None,
        events_tx: None,
        tokens: tokens.map(|t| Arc::new(AuthTokens::from_csv(t))),
        rate_limiter: rate.map(|(r, burst)| {
            Arc::new(RateLimiter::new(r, burst)) as Arc<dyn siglake_ingest::rate_limit::RateBudget>
        }),
        mem_guard: None,
        commit_force_timeout: Duration::from_secs(2),
        remote_wal_drain: true,
    };
    (router(state), tmp)
}

async fn probe(app: &Router, method: &str, uri: &str, auth: Option<&str>) -> (StatusCode, Vec<u8>) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(a) = auth {
        req = req.header("authorization", a);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

/// Missing credentials are a `401` on the pings and the stubs too — the stubs'
/// `501` is not the first thing a client meets.
#[tokio::test]
async fn tenant_ignoring_routes_answer_401_before_their_own_status() {
    let (app, _tmp) = app_with(Some(TOKEN), None);
    for (method, uri) in IGNORING {
        let (status, body) = probe(&app, method, uri, None).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{method} {uri} served an unauthenticated caller"
        );
        // A HEAD response carries no content, which is why the spec declares no
        // body for it; everything else answers in the siglake envelope, even the
        // stubs, which use the ES one for their own `501`.
        if *method == "HEAD" {
            assert!(body.is_empty(), "HEAD {uri} returned a body");
            continue;
        }
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body["code"], 401,
            "{method} {uri}: not the siglake envelope"
        );
        assert!(
            body["text"].is_string() && body.get("error").is_none(),
            "{method} {uri}: expected `{{text, code}}`, got {body}"
        );
    }

    // And the credentialled answer is the route's own, so the assertion above is
    // measuring the auth layer rather than a route that is broken outright.
    for (method, uri) in IGNORING {
        let (status, _) = probe(&app, method, uri, Some(&format!("Bearer {TOKEN}"))).await;
        assert_eq!(
            status,
            if is_stub(uri) {
                StatusCode::NOT_IMPLEMENTED
            } else {
                StatusCode::OK
            },
            "{method} {uri} refused a valid token"
        );
    }
}

/// Throttling reaches every one of them as well, ahead of the auth check, and
/// carries `Retry-After`.
#[tokio::test]
async fn tenant_ignoring_routes_are_rate_limited_ahead_of_auth() {
    // burst 1 with a refill measured in minutes: the first request spends the
    // only token and every later one is throttled, whatever the wall clock does
    // between them. Every request below sends no tenant header and no
    // credentials, so they all charge the one `anonymous` bucket.
    let (app, _tmp) = app_with(Some(TOKEN), Some((0.01, 1.0)));
    // The warm-up is itself the ordering proof: it is answered 401, so the rate
    // limiter had already admitted it and spent its token BEFORE the auth check
    // refused it.
    let (first, _) = probe(&app, "GET", "/", None).await;
    assert_eq!(
        first,
        StatusCode::UNAUTHORIZED,
        "the warm-up request should reach the auth layer and be refused there"
    );

    for (method, uri) in IGNORING {
        let (status, body) = probe(&app, method, uri, None).await;
        // No credentials were sent, yet this is a 429 and not a 401: the rate
        // limiter is the outer layer.
        assert_eq!(
            status,
            StatusCode::TOO_MANY_REQUESTS,
            "{method} {uri} was not rate-limited"
        );
        if *method == "HEAD" {
            assert!(body.is_empty(), "HEAD {uri} returned a body");
            continue;
        }
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], 429, "{method} {uri}: not the rate-limit body");
        assert!(
            body["retry_after_secs"].is_number(),
            "{method} {uri}: expected retry_after_secs, got {body}"
        );
    }

    // `retry-after` is the only part of the refusal a HEAD caller can read, and
    // the spec declares it on every 429 including that one.
    for (method, uri) in IGNORING {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(*method)
                    .uri(*uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            resp.headers().contains_key("retry-after"),
            "{method} {uri}: 429 without a retry-after header"
        );
    }
}

/// On a single-tenant ingester the tenant header does not buy a fresh rate
/// budget.
///
/// The rate limiter runs ahead of authentication — that is what protects the
/// auth path — so it reads `X-Scope-OrgID` raw, before anything has decided
/// whether this ingester routes on it. Keying on it where it routes nothing
/// hands every caller an unlimited supply of buckets: vary the header, vary the
/// budget, land in `default` either way.
#[tokio::test]
async fn a_single_tenant_ingester_does_not_key_the_rate_budget_by_the_header() {
    // burst 1, refill measured in minutes: the first request spends the only
    // token, so anything sharing its bucket is throttled whatever the clock did.
    let (app, _tmp) = app_with(None, Some((0.01, 1.0)));

    let spend = |tenant: &'static str| {
        let app = app.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/")
                    .header("X-Scope-OrgID", tenant)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
        }
    };

    assert_eq!(spend("default").await, StatusCode::OK);
    assert_eq!(
        spend("acme").await,
        StatusCode::TOO_MANY_REQUESTS,
        "a different X-Scope-OrgID got its own rate budget on a single-tenant ingester"
    );
}

/// With header routing turned on, the header IS the tenant, and the README's
/// "rate budgets are per-tenant" holds again: each tenant gets its own bucket.
#[tokio::test]
async fn trusting_the_header_restores_per_tenant_rate_budgets() {
    let (app, _tmp) = app_with_routing(None, Some((0.01, 1.0)), TenantRouting::TrustHeader);
    let spend = |tenant: &'static str| {
        let app = app.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/")
                    .header("X-Scope-OrgID", tenant)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
        }
    };

    assert_eq!(spend("acme").await, StatusCode::OK);
    assert_eq!(
        spend("widgets").await,
        StatusCode::OK,
        "a second tenant was throttled by the first tenant's burst"
    );
    assert_eq!(
        spend("acme").await,
        StatusCode::TOO_MANY_REQUESTS,
        "acme's own second request should have found its bucket empty"
    );
}
