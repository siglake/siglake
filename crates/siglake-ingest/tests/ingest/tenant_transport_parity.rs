//! One tenancy boundary, two transports.
//!
//! THE DEFECT THIS GUARDS. Ingest verified OIDC and bound the tenant to the
//! verified claim in the axum middleware — and the OTLP/gRPC exporters never
//! ran it. They read `x-scope-orgid` out of the request metadata, checked the
//! static bearer-token list and nothing else, and wrote where the metadata
//! said. So an ingester started with `--oidc-issuer`, `--oidc-audience` and
//! `--oidc-tenant-claim` — the configuration an operator chooses precisely to
//! stop cross-tenant writes — refused an unauthenticated HTTP export with
//! `401` and accepted the same export over gRPC, into any tenant the caller
//! named. Turning authentication on hardened one port and left the other open.
//!
//! The second defect is the default. `X-Scope-OrgID` is a client header that
//! nothing authenticates, and it selects both the WAL subtree a request is
//! written to and the Iceberg namespace the drain commits it to. Every shipped
//! configuration without `--oidc-tenant-claim` honoured it, so any accepted
//! credential could write as, and be read as, any tenant. Single-tenant is the
//! default now (task #2954); the header opts in.
//!
//! Every case below runs against both transports from one table, because
//! "HTTP refuses it" has already once been a different fact from "gRPC refuses
//! it".

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use axum::Router;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest as ProtoLogsRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest as ProtoTracesRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use serde_json::json;
use tokio::sync::Mutex;
use tonic::metadata::MetadataValue;
use tonic::{Code, Request as TonicRequest};
use tower::util::ServiceExt;

use siglake_core::oidc::OidcVerifier;
use siglake_ingest::{
    router, AppState, AuthTokens, OtlpGrpcLogsService, OtlpGrpcTracesService, TenantRouting,
    TenantWalRouter, TENANT_HEADER,
};
use siglake_wal::WalWriter;

use crate::fake_idp::{issue_jwt_with_tenant, spawn_fake_idp};

/// What a transport answered. The two return different types and different
/// vocabularies; the outcome they encode is the same one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Accepted,
    BadRequest,
    Unauthenticated,
    Forbidden,
}

impl Outcome {
    fn from_http(status: StatusCode) -> Self {
        match status {
            StatusCode::OK => Self::Accepted,
            StatusCode::BAD_REQUEST => Self::BadRequest,
            StatusCode::UNAUTHORIZED => Self::Unauthenticated,
            StatusCode::FORBIDDEN => Self::Forbidden,
            other => panic!("unexpected HTTP status {other}"),
        }
    }

    fn from_grpc(code: Option<Code>) -> Self {
        match code {
            None => Self::Accepted,
            Some(Code::InvalidArgument) => Self::BadRequest,
            Some(Code::Unauthenticated) => Self::Unauthenticated,
            Some(Code::PermissionDenied) => Self::Forbidden,
            Some(other) => panic!("unexpected gRPC code {other:?}"),
        }
    }
}

/// One ingester, plus the WAL root its writes land under.
struct Ingester {
    state: AppState,
    root: tempfile::TempDir,
}

impl Ingester {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let tenants = TenantWalRouter::new(root.path(), "parity", 1, Duration::from_secs(60));
        let writer =
            WalWriter::with_thresholds(root.path(), "parity", 1, Duration::from_secs(60)).unwrap();
        let state = AppState {
            writer: Arc::new(Mutex::new(writer)),
            tenants: Some(Arc::new(tenants)),
            allowed_tenants: None,
            tenant_routing: TenantRouting::default(),
            max_tenants: 0,
            tenant_admission: Default::default(),
            backpressure: None,
            events_tx: None,
            tokens: None,
            oidc_verifier: None,
            rate_limiter: None,
            mem_guard: None,
            commit_force_timeout: Duration::from_secs(5),
            remote_wal_drain: true,
        };
        Self { state, root }
    }

    fn with(mut self, f: impl FnOnce(AppState) -> AppState) -> Self {
        self.state = f(self.state);
        self
    }

    /// Did anything get written for `tenant`? A refusal that still minted the
    /// target tenant's WAL subtree has leaked half the write.
    fn wrote_for(&self, tenant: &str) -> bool {
        self.root.path().join(tenant).exists()
    }

    async fn http_logs(&self, tenant: Option<&str>, auth: Option<&str>) -> Outcome {
        let mut req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/logs")
            .header("content-type", "application/json");
        if let Some(t) = tenant {
            req = req.header(TENANT_HEADER, t);
        }
        if let Some(a) = auth {
            req = req.header("authorization", a);
        }
        let app: Router = router(self.state.clone());
        let status = app
            .oneshot(req.body(Body::from(otlp_logs_json())).unwrap())
            .await
            .unwrap()
            .status();
        Outcome::from_http(status)
    }

    async fn grpc_logs(&self, tenant: Option<&str>, auth: Option<&str>) -> Outcome {
        let mut request = TonicRequest::new(proto_logs());
        set_metadata(request.metadata_mut(), tenant, auth);
        let service = OtlpGrpcLogsService::new(self.state.clone());
        Outcome::from_grpc(service.export(request).await.err().map(|s| s.code()))
    }

    async fn grpc_traces(&self, tenant: Option<&str>, auth: Option<&str>) -> Outcome {
        let mut request = TonicRequest::new(proto_traces());
        set_metadata(request.metadata_mut(), tenant, auth);
        let service = OtlpGrpcTracesService::new(self.state.clone());
        Outcome::from_grpc(service.export(request).await.err().map(|s| s.code()))
    }
}

fn set_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    tenant: Option<&str>,
    auth: Option<&str>,
) {
    if let Some(t) = tenant {
        metadata.insert(TENANT_HEADER, MetadataValue::try_from(t).unwrap());
    }
    if let Some(a) = auth {
        metadata.insert("authorization", MetadataValue::try_from(a).unwrap());
    }
}

fn otlp_logs_json() -> String {
    json!({
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

fn attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(
                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                    value.to_string(),
                ),
            ),
        }),
        ..Default::default()
    }
}

fn proto_logs() -> ProtoLogsRequest {
    ProtoLogsRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![attr("host.name", "h1"), attr("service.name", "svc")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "hello".to_string(),
                            ),
                        ),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn proto_traces() -> ProtoTracesRequest {
    ProtoTracesRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![attr("host.name", "h1"), attr("service.name", "svc")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![1u8; 16],
                    span_id: vec![2u8; 8],
                    name: "parity".to_string(),
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_000_100_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

const AUD: &str = "siglake-ingest";

/// The shipped default: `X-Scope-OrgID` cannot select a tenant, on any
/// transport, and the refusal does not create the tenant it refused.
#[tokio::test]
async fn by_default_the_scope_header_cannot_select_a_tenant() {
    let ingester = Ingester::new();

    assert_eq!(
        ingester.http_logs(Some("acme"), None).await,
        Outcome::Forbidden,
        "HTTP honoured an unverified tenant header on a single-tenant ingester"
    );
    assert_eq!(
        ingester.grpc_logs(Some("acme"), None).await,
        Outcome::Forbidden,
        "the gRPC logs exporter honoured an unverified tenant header"
    );
    assert_eq!(
        ingester.grpc_traces(Some("acme"), None).await,
        Outcome::Forbidden,
        "the gRPC traces exporter honoured an unverified tenant header"
    );
    assert!(
        !ingester.wrote_for("acme"),
        "a refused request still minted the tenant's WAL subtree"
    );

    // The tenant this ingester already writes to: sending the header is a
    // no-op, not a refusal, so a client that always sends it keeps working.
    assert_eq!(
        ingester.http_logs(Some("default"), None).await,
        Outcome::Accepted
    );
    assert_eq!(
        ingester.grpc_logs(Some("default"), None).await,
        Outcome::Accepted
    );
    // And with no header at all.
    assert_eq!(ingester.http_logs(None, None).await, Outcome::Accepted);
    assert_eq!(ingester.grpc_logs(None, None).await, Outcome::Accepted);
    assert!(ingester.wrote_for("default"));
}

/// The opt-in, on both transports: with `--trust-scope-header` the header
/// routes again, and each tenant gets its own WAL subtree.
#[tokio::test]
async fn the_opt_in_restores_header_routing_on_both_transports() {
    let ingester = Ingester::new().with(|s| s.with_tenant_routing(TenantRouting::TrustHeader));

    assert_eq!(
        ingester.http_logs(Some("acme"), None).await,
        Outcome::Accepted
    );
    assert_eq!(
        ingester.grpc_logs(Some("widgets"), None).await,
        Outcome::Accepted
    );
    assert_eq!(
        ingester.grpc_traces(Some("umbrella"), None).await,
        Outcome::Accepted
    );
    assert!(ingester.wrote_for("acme"));
    assert!(ingester.wrote_for("widgets"));
    assert!(ingester.wrote_for("umbrella"));

    // Opting into header routing does not opt out of validating the value: it
    // still becomes a directory name and a namespace.
    assert_eq!(
        ingester.http_logs(Some("../escape"), None).await,
        Outcome::BadRequest
    );
    assert_eq!(
        ingester.grpc_logs(Some("../escape"), None).await,
        Outcome::BadRequest
    );
    assert_eq!(
        ingester.grpc_traces(Some("acme.corp"), None).await,
        Outcome::BadRequest
    );
}

/// Static bearer tokens are enforced on every transport, with or without a
/// tenant header.
#[tokio::test]
async fn static_tokens_are_enforced_on_both_transports() {
    let ingester = Ingester::new().with(|s| s.with_tokens(AuthTokens::from_csv("s3cret")));

    for auth in [None, Some("Bearer wrong")] {
        assert_eq!(
            ingester.http_logs(None, auth).await,
            Outcome::Unauthenticated,
            "HTTP accepted {auth:?}"
        );
        assert_eq!(
            ingester.grpc_logs(None, auth).await,
            Outcome::Unauthenticated,
            "the gRPC logs exporter accepted {auth:?}"
        );
        assert_eq!(
            ingester.grpc_traces(None, auth).await,
            Outcome::Unauthenticated,
            "the gRPC traces exporter accepted {auth:?}"
        );
    }
    assert_eq!(
        ingester.grpc_logs(None, Some("Bearer s3cret")).await,
        Outcome::Accepted
    );
}

/// A configured OIDC verifier is mandatory on every transport.
///
/// THE BYPASS. gRPC checked `ensure_authorized(state.tokens, …)` — the STATIC
/// token list — and an OIDC-only ingester has none, so that check passed
/// vacuously and every unauthenticated export was accepted.
#[tokio::test]
async fn a_configured_verifier_is_mandatory_on_both_transports() {
    // No token is presented in this test, so the JWKS endpoint is never
    // reached and does not need to exist: `verify` fails before any fetch.
    let verifier = OidcVerifier::with_jwks_uri(
        "https://idp.invalid".to_string(),
        AUD.to_string(),
        "https://idp.invalid/jwks".to_string(),
        reqwest::Client::new(),
    );
    let ingester = Ingester::new().with(|s| s.with_oidc_verifier(verifier));

    for auth in [None, Some("Bearer "), Some("Bearer not-a-jwt")] {
        assert_eq!(
            ingester.http_logs(None, auth).await,
            Outcome::Unauthenticated,
            "HTTP accepted {auth:?}"
        );
        assert_eq!(
            ingester.grpc_logs(None, auth).await,
            Outcome::Unauthenticated,
            "the gRPC logs exporter accepted {auth:?} against a configured verifier"
        );
        assert_eq!(
            ingester.grpc_traces(None, auth).await,
            Outcome::Unauthenticated,
            "the gRPC traces exporter accepted {auth:?} against a configured verifier"
        );
    }
    assert!(
        !ingester.wrote_for("default"),
        "an unauthenticated export was written anyway"
    );
}

/// With a tenant claim configured, the verified JWT decides the tenant on
/// every transport: the header may agree with it and nothing else, and a token
/// carrying no usable claim is refused rather than falling back.
#[tokio::test]
async fn the_verified_claim_decides_the_tenant_on_both_transports() {
    let (issuer, idp) = spawn_fake_idp().await;
    let verifier = OidcVerifier::from_issuer(issuer.clone(), AUD.to_string())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    // Header routing is ALSO on, so this proves the claim wins over an
    // explicitly trusted header rather than merely over an ignored one.
    let ingester = Ingester::new().with(|s| {
        s.with_oidc_verifier(verifier)
            .with_tenant_routing(TenantRouting::TrustHeader)
    });

    let acme = format!(
        "Bearer {}",
        issue_jwt_with_tenant(&issuer, AUD, "u1", "acme")
    );

    // No header: the claim supplies the tenant, on both transports.
    assert_eq!(
        ingester.http_logs(None, Some(&acme)).await,
        Outcome::Accepted
    );
    assert_eq!(
        ingester.grpc_logs(None, Some(&acme)).await,
        Outcome::Accepted
    );
    assert_eq!(
        ingester.grpc_traces(None, Some(&acme)).await,
        Outcome::Accepted
    );
    assert!(
        ingester.wrote_for("acme"),
        "the claim did not route the write"
    );

    // A header that agrees is fine; one that disagrees is refused.
    assert_eq!(
        ingester.grpc_logs(Some("acme"), Some(&acme)).await,
        Outcome::Accepted
    );
    for outcome in [
        ingester.http_logs(Some("widgets"), Some(&acme)).await,
        ingester.grpc_logs(Some("widgets"), Some(&acme)).await,
        ingester.grpc_traces(Some("widgets"), Some(&acme)).await,
    ] {
        assert_eq!(
            outcome,
            Outcome::Forbidden,
            "a token for `acme` wrote into `widgets` because the header was believed"
        );
    }
    assert!(
        !ingester.wrote_for("widgets"),
        "a refused cross-tenant write still created the target tenant's WAL"
    );

    idp.abort();
}

/// A token whose tenant claim is missing, or carries a value that is not usable
/// as a tenant id, is refused on every transport. Token validity is not tenant
/// authorization, and an unusable claim never falls back to `default`.
#[tokio::test]
async fn an_unusable_claim_is_refused_on_both_transports() {
    let (issuer, idp) = spawn_fake_idp().await;
    // The deployment says tenancy comes from `org_id`...
    let verifier = OidcVerifier::from_issuer(issuer.clone(), AUD.to_string())
        .await
        .unwrap()
        .with_tenant_claim("org_id");
    let ingester = Ingester::new().with(|s| s.with_oidc_verifier(verifier));

    // ...but these tokens carry `tenant`, so the claim is missing.
    let token = format!(
        "Bearer {}",
        issue_jwt_with_tenant(&issuer, AUD, "u1", "acme")
    );
    for outcome in [
        ingester.http_logs(None, Some(&token)).await,
        ingester.grpc_logs(None, Some(&token)).await,
        ingester.grpc_traces(None, Some(&token)).await,
    ] {
        assert_eq!(
            outcome,
            Outcome::Forbidden,
            "a token with no tenant claim was routed somewhere anyway"
        );
    }
    assert!(
        !ingester.wrote_for("default"),
        "a token with no tenant claim fell back to the default tenant"
    );

    idp.abort();
}

/// A claim that cannot be a tenant id — it would become a directory name and an
/// Iceberg namespace — is refused, not repaired, on every transport.
#[tokio::test]
async fn a_claim_that_is_not_a_usable_identifier_is_refused() {
    let (issuer, idp) = spawn_fake_idp().await;
    let verifier = OidcVerifier::from_issuer(issuer.clone(), AUD.to_string())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    let ingester = Ingester::new().with(|s| s.with_oidc_verifier(verifier));

    let token = format!(
        "Bearer {}",
        issue_jwt_with_tenant(&issuer, AUD, "u1", "acme.corp")
    );
    for outcome in [
        ingester.http_logs(None, Some(&token)).await,
        ingester.grpc_logs(None, Some(&token)).await,
        ingester.grpc_traces(None, Some(&token)).await,
    ] {
        assert_eq!(outcome, Outcome::Forbidden);
    }
    assert!(
        !ingester.wrote_for("acmecorp"),
        "an unusable claim was repaired into a namespace instead of refused"
    );

    idp.abort();
}

/// `--allowed-tenants` bounds the tenant that was actually resolved.
///
/// It used to be checked against the HEADER's value and then overwritten by the
/// claim, so on the configuration that binds tenancy to a verified identity the
/// allow-list bounded nothing: a token claiming a tenant outside the list was
/// accepted, and a legitimate one with no header was refused because `default`
/// was not in the list.
#[tokio::test]
async fn the_allow_list_bounds_the_resolved_tenant_not_the_header() {
    let (issuer, idp) = spawn_fake_idp().await;
    let verifier = OidcVerifier::from_issuer(issuer.clone(), AUD.to_string())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    let ingester = Ingester::new().with(|s| {
        let mut s = s.with_oidc_verifier(verifier);
        s.allowed_tenants = Some(Arc::new(["acme".to_string()].into_iter().collect()));
        s
    });

    let acme = format!(
        "Bearer {}",
        issue_jwt_with_tenant(&issuer, AUD, "u1", "acme")
    );
    let widgets = format!(
        "Bearer {}",
        issue_jwt_with_tenant(&issuer, AUD, "u2", "widgets")
    );

    assert_eq!(
        ingester.http_logs(None, Some(&acme)).await,
        Outcome::Accepted,
        "a listed tenant's own token was refused"
    );
    assert_eq!(
        ingester.grpc_logs(None, Some(&acme)).await,
        Outcome::Accepted
    );
    for outcome in [
        ingester.http_logs(None, Some(&widgets)).await,
        ingester.grpc_logs(None, Some(&widgets)).await,
        ingester.grpc_traces(None, Some(&widgets)).await,
    ] {
        assert_eq!(
            outcome,
            Outcome::Forbidden,
            "a claimed tenant outside the allow-list was accepted"
        );
    }
    assert!(!ingester.wrote_for("widgets"));

    idp.abort();
}
