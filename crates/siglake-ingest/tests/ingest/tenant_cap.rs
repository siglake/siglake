//! `--max-tenants`: the backstop when the tenant set is not known.
//!
//! THE DEFECT THIS GUARDS. `--max-tenants` / `SIGLAKE_MAX_TENANTS` /
//! `ingester.maxTenants` parsed, threaded into `AppState` and rendered by the
//! chart, and then nothing read it. Every caller on the tenancy path returned
//! after the allow-list check, so an operator who set the cap as their only
//! bound on client-minted tenants had no bound at all — while the README and
//! the docs site said the cap was the backstop for exactly that deployment.
//! Against the pre-repair code every assertion below that expects a `403`
//! gets a `200`.
//!
//! WHAT THE CAP COUNTS. Distinct resolved tenants on this process, not
//! `(tenant, index)` lanes: one tenant writing to twelve indexes is one
//! tenant, and `--ingest-max-lanes` is the bound on the cross product. A
//! tenant already admitted keeps writing once the cap is full; only a novel
//! one is turned away, and it is turned away before a writer opens, a
//! directory is made or a row is published.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use tokio::sync::Mutex;
use tonic::metadata::MetadataValue;
use tonic::{Code, Request as TonicRequest};
use tower::util::ServiceExt;

use siglake_ingest::{
    router, AppState, OtlpGrpcLogsService, TenantAdmission, TenantRouting, TenantWalRouter,
    INDEX_HEADER, TENANT_HEADER,
};
use siglake_wal::WalWriter;

const DENIED: &str = "siglake_ingest_tenant_denied_total";

fn otlp_body() -> String {
    serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "host.name", "value": { "stringValue": "h1" } }
            ]},
            "scopeLogs": [{
                "scope": { "name": "t" },
                "logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "body": { "stringValue": "hello" },
                    "severityText": "INFO"
                }]
            }]
        }]
    })
    .to_string()
}

/// An ingester that honours the tenant header — the configuration the cap
/// exists for — capped at `max_tenants` (0 = unbounded).
fn capped_state(root: &std::path::Path, max_tenants: usize) -> AppState {
    AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(
            WalWriter::with_thresholds(root, "cap", 5, Duration::from_secs(60)).unwrap(),
        )),
        tenants: Some(Arc::new(TenantWalRouter::new(
            root,
            "cap",
            5,
            Duration::from_secs(60),
        ))),
        backpressure: None,
        events_tx: None,
        tokens: None,
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: Duration::from_secs(30),
        remote_wal_drain: true,
        allowed_tenants: None,
        tenant_routing: TenantRouting::TrustHeader,
        max_tenants,
        tenant_admission: TenantAdmission::default(),
    }
}

async fn post_as(app: &Router, tenant: &str) -> StatusCode {
    post_to_index(app, tenant, None).await
}

async fn post_to_index(app: &Router, tenant: &str, index: Option<&str>) -> StatusCode {
    let mut req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/json")
        .header(TENANT_HEADER, tenant);
    if let Some(i) = index {
        req = req.header(INDEX_HEADER, i);
    }
    app.clone()
        .oneshot(req.body(Body::from(otlp_body())).unwrap())
        .await
        .unwrap()
        .status()
}

/// How many times the denial counter was incremented under each `reason`.
fn denials_by_reason(snap: Snapshot) -> Vec<(String, u64)> {
    let mut rows: Vec<_> = snap
        .into_vec()
        .into_iter()
        .filter(|(k, _, _, _)| k.key().name() == DENIED)
        .map(|(k, _, _, v)| {
            let key = k.key();
            let reason = key
                .labels()
                .find(|l| l.key() == "reason")
                .map(|l| l.value().to_string())
                .unwrap_or_else(|| panic!("{DENIED} recorded without a reason label"));
            let count = match v {
                DebugValue::Counter(c) => c,
                other => panic!("{DENIED} is not a counter: {other:?}"),
            };
            (reason, count)
        })
        .collect();
    rows.sort();
    rows
}

/// The (N+1)th tenant is refused with `403`, counted, and leaves nothing
/// behind — while the N already writing keep writing.
#[tokio::test]
async fn the_tenant_past_the_cap_is_refused_and_counted() {
    let tmp = tempfile::tempdir().unwrap();
    let app = router(capped_state(tmp.path(), 2));

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);

    assert_eq!(post_as(&app, "acme").await, StatusCode::OK);
    assert_eq!(post_as(&app, "widgets").await, StatusCode::OK);
    let over = post_as(&app, "intruder").await;
    // At capacity, the tenants already admitted are unaffected.
    let existing = post_as(&app, "acme").await;

    let denials = denials_by_reason(snapshotter.snapshot());
    drop(guard);

    assert_eq!(
        over,
        StatusCode::FORBIDDEN,
        "the third tenant was accepted on an ingester capped at two"
    );
    assert_eq!(
        existing,
        StatusCode::OK,
        "a tenant already admitted was refused once the cap filled up"
    );
    assert_eq!(
        denials,
        vec![("at_capacity".to_string(), 1)],
        "the capacity refusal did not land exactly once on {DENIED}"
    );
    // Refused BEFORE the mint: without a WAL subtree there is no writer, so
    // nothing holds a file descriptor and nothing was acknowledged.
    assert!(
        !tmp.path().join("intruder").exists(),
        "a tenant refused by the cap still had its WAL directory created"
    );
}

/// Concurrent novel tenants cannot all read "there is room" and all get in.
///
/// The admission decision is one locked section — membership and the count
/// together. Split into a read and a later insert it would admit as many
/// tenants as happen to be in flight, which on the path this bounds (a client
/// varying a header) is the whole defect back again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_novel_tenants_are_admitted_up_to_the_cap_exactly() {
    const CAP: usize = 4;
    const CALLERS: usize = 32;

    let tmp = tempfile::tempdir().unwrap();
    let app = router(capped_state(tmp.path(), CAP));

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..CALLERS {
        let app = app.clone();
        tasks.spawn(async move { post_as(&app, &format!("tenant-{i}")).await });
    }
    let mut accepted = 0usize;
    let mut refused = 0usize;
    while let Some(status) = tasks.join_next().await {
        match status.unwrap() {
            StatusCode::OK => accepted += 1,
            StatusCode::FORBIDDEN => refused += 1,
            other => panic!("unexpected status {other}"),
        }
    }

    assert_eq!(
        (accepted, refused),
        (CAP, CALLERS - CAP),
        "{CALLERS} concurrent novel tenants against a cap of {CAP} admitted {accepted}"
    );
}

/// One tenant across many indexes is one tenant. The cap counts tenants;
/// `--ingest-max-lanes` counts `(tenant, index)` lanes.
#[tokio::test]
async fn repeated_tenants_across_indexes_spend_one_slot() {
    let tmp = tempfile::tempdir().unwrap();
    let app = router(capped_state(tmp.path(), 1));

    for index in [None, Some("app"), Some("audit"), Some("metrics")] {
        assert_eq!(
            post_to_index(&app, "acme", index).await,
            StatusCode::OK,
            "index {index:?} for an already-admitted tenant was charged to the tenant cap"
        );
    }
    assert_eq!(
        post_as(&app, "second").await,
        StatusCode::FORBIDDEN,
        "a second tenant got in under a cap of one"
    );
}

/// `0` is unbounded — the historical behaviour, and the default.
#[tokio::test]
async fn an_unbounded_cap_admits_every_tenant() {
    let tmp = tempfile::tempdir().unwrap();
    let state = capped_state(tmp.path(), 0);
    let admission = state.tenant_admission.clone();
    let app = router(state);

    for i in 0..16 {
        assert_eq!(
            post_as(&app, &format!("tenant-{i}")).await,
            StatusCode::OK,
            "an ingester with --max-tenants 0 refused a tenant"
        );
    }
    // And it records nothing while doing it: keeping every tenant name to
    // enforce no bound is the same unbounded growth the cap exists to stop.
    assert!(
        admission.is_empty(),
        "the unbounded cap accumulated tenant names"
    );
}

/// The gRPC exporter refuses at the same cap with `PermissionDenied`, against
/// the same count as the HTTP surface — they share one `AppState`.
#[tokio::test]
async fn grpc_export_past_the_cap_is_permission_denied() {
    let tmp = tempfile::tempdir().unwrap();
    let state = capped_state(tmp.path(), 1);
    let app = router(state.clone());
    let service = OtlpGrpcLogsService::new(state);

    async fn export(service: &OtlpGrpcLogsService, tenant: &str) -> Option<Code> {
        let mut request = TonicRequest::new(ExportLogsServiceRequest::default());
        request
            .metadata_mut()
            .insert(TENANT_HEADER, MetadataValue::try_from(tenant).unwrap());
        service.export(request).await.err().map(|s| s.code())
    }

    assert_eq!(export(&service, "acme").await, None);
    assert_eq!(
        export(&service, "intruder").await,
        Some(Code::PermissionDenied),
        "the gRPC exporter accepted a tenant past the cap"
    );
    // The slot spent over gRPC is the same slot HTTP sees: one ingester, one
    // count, whichever port the tenant arrives on.
    assert_eq!(
        post_as(&app, "over-http").await,
        StatusCode::FORBIDDEN,
        "HTTP admitted a tenant the gRPC port had already filled the cap with"
    );
    assert_eq!(
        post_as(&app, "acme").await,
        StatusCode::OK,
        "HTTP refused the tenant gRPC admitted"
    );
}

/// The allow-list is checked first, so a tenant it refuses does not spend a
/// slot under the cap. Otherwise a client naming junk tenants could exhaust
/// the cap against an ingester that was never going to accept any of them.
#[tokio::test]
async fn a_tenant_the_allowlist_refuses_does_not_spend_a_slot() {
    let tmp = tempfile::tempdir().unwrap();
    let mut state = capped_state(tmp.path(), 1);
    state.allowed_tenants = Some(Arc::new(["acme".to_string()].into_iter().collect()));
    let admission = state.tenant_admission.clone();
    let app = router(state);

    for i in 0..8 {
        assert_eq!(
            post_as(&app, &format!("intruder-{i}")).await,
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(admission.len(), 0, "a refused tenant was admitted anyway");
    assert_eq!(
        post_as(&app, "acme").await,
        StatusCode::OK,
        "the listed tenant was crowded out of the cap by tenants that were refused"
    );
}
