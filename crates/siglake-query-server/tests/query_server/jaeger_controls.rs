//! Socket-only owned primary and signed synthetic JWTs. No provider authority.
use super::*;
use siglake_core::index_config::IndexConfig;
use siglake_core::{events_to_record_batch, map_carrier_batch};
use siglake_storage::index_manager::builtin_traces_template;

async fn traces(ice: &IcebergContext) {
    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        common::v1::{any_value, AnyValue, KeyValue},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };
    let config = IndexConfig {
        index_id: "controlled-traces".into(),
        doc_mapping: builtin_traces_template().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("checkout".into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: "checkout".into(),
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_000_010_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let events = siglake_ingest::otlp_traces::otlp_proto_traces_to_events(request);
    let carrier = events_to_record_batch(&events).unwrap();
    let mapped = map_carrier_batch(&carrier, &config).unwrap();
    let bloom = config
        .doc_mapping
        .tag_fields
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    ice.append_to_table(&ice.index_table_ident("controlled-traces"), mapped, &bloom)
        .await
        .unwrap();
}
const ROUTES: [&str; 4] = [
    "/api/v1/jaeger/controlled-traces/api/services",
    "/api/v1/jaeger/controlled-traces/api/services/checkout/operations",
    "/api/v1/jaeger/controlled-traces/api/traces?service=checkout",
    "/api/v1/jaeger/controlled-traces/api/traces/01010101010101010101010101010101",
];
async fn get(server: &Server, jwt: &str, route: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{}{route}", server.base))
        .bearer_auth(jwt)
        .header(
            "x-siglake-query-controls",
            "v=1;timeout_ms=999999;rev=999999",
        )
        .header("x-client-id", "forged-client")
        .header("x-scope-orgid", "foreign")
        .send()
        .await
        .unwrap()
}
fn headers(
    response: &reqwest::Response,
    expected: &str,
    revision: &str,
    generation: i64,
    warning: bool,
) {
    let h = response.headers();
    assert_eq!(h.get_all("x-siglake-query-controls").iter().count(), 1);
    assert_eq!(h.get_all("x-siglake-query-warnings").iter().count(), 1);
    let c = h["x-siglake-query-controls"].to_str().unwrap();
    let w = h["x-siglake-query-warnings"].to_str().unwrap();
    assert!(c.starts_with(&format!("v=1;state={expected};rev={revision};gen={generation};priority=interactive;mode=request;scope=request;mandatory=1;")), "{c}");
    assert!(c.ends_with("estimate=unavailable;exact=na;estimated_bytes=na"));
    assert!(c.contains(";output_rows=na;output_enforced=0;"));
    assert_eq!(
        w,
        if warning {
            "scan_estimate_unavailable,scan_threshold_not_evaluated"
        } else {
            "scan_estimate_unavailable"
        }
    );
    assert!(c.is_ascii() && w.is_ascii() && c.len() <= 1024 && w.len() <= 512);
    assert!(
        c.len() + w.len() + "x-siglake-query-controls: \r\nx-siglake-query-warnings: \r\n".len()
            <= 2048
    );
    for forbidden in [
        "controlled-client",
        "acme",
        "forged",
        "http://",
        "SQL",
        "\r",
        "\n",
        "synthetic-human",
    ] {
        assert!(!c.contains(forbidden) && !w.contains(forbidden));
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned signed primary fixture; check-query-policy.sh"]
async fn signed_jaeger_four_routes_primary_freshness_native_payloads_headers_and_failures() {
    let (socket, admin) = fixture().await;
    let manager = PolicyManager::connect(policy_config(&socket, false))
        .await
        .unwrap();
    let reader = Arc::new(
        PolicyReader::connect(policy_config(&socket, true))
            .await
            .unwrap(),
    );
    let (issuer, task) = spawn_fake_idp().await;
    let idp = Server {
        base: issuer.clone(),
        task,
        state: None,
    };
    let jwt = token(&issuer, "acme", "jaeger-client");
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    traces(&ice.for_namespace("tenant_acme").await.unwrap()).await;
    let mut limits = ServerLimits::default();
    limits.interactive.default_timeout = Duration::from_secs(3);
    limits.interactive.ceiling_timeout = Duration::from_secs(3);
    limits.admission_wait_timeout = Duration::from_secs(2);
    let base = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice))
        .with_limits(limits);
    let disabled = serve(base.clone()).await;
    let enabled_state = base
        .clone()
        .with_query_controls(reader.clone(), 17)
        .unwrap();
    let enabled = serve(enabled_state.clone()).await;
    let mut native = Vec::new();
    for route in ROUTES {
        let response = get(&disabled, &jwt, route).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!response.headers().contains_key("x-siglake-query-controls"));
        native.push(response.bytes().await.unwrap());
    }
    let binding = ManagementBinding::from_trusted_metadata(
        RuntimeKey::from_trusted_metadata("acme", &issuer, "jaeger-client").unwrap(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    )
    .unwrap();
    let actor = VerifiedHumanActor::from_trusted_metadata("synthetic-human").unwrap();
    for (expected, revision, settings) in [
        ("inherited", 0, None),
        (
            "inherited",
            1,
            Some(SavedSettings::new(None, None).unwrap()),
        ),
        (
            "applied",
            2,
            Some(SavedSettings::new(Some(1), Some(1)).unwrap()),
        ),
        (
            "inherited",
            3,
            Some(SavedSettings::new(None, None).unwrap()),
        ),
    ] {
        if let Some(settings) = settings {
            manager
                .save(
                    &binding,
                    &actor,
                    SaveRequest::new(revision - 1, settings).unwrap(),
                )
                .await
                .unwrap();
        }
        // Second pass is a hit for names, but each caller obtains its own fresh primary snapshot.
        for _ in 0..2 {
            for (i, route) in ROUTES.iter().enumerate() {
                let response = get(&enabled, &jwt, route).await;
                assert_eq!(response.status(), StatusCode::OK, "{route}");
                headers(
                    &response,
                    expected,
                    &if revision == 0 {
                        "na".into()
                    } else {
                        revision.to_string()
                    },
                    17,
                    expected == "applied",
                );
                assert_eq!(
                    response.bytes().await.unwrap(),
                    native[i],
                    "native JSON bytes changed: {route}"
                );
            }
        }
        if expected == "applied" {
            // Real primary lock delay consumes the admitted one-second policy
            // from ingress, not a fresh clock after lookup. Four real routes.
            for route in ROUTES {
                let mut lock = admin.begin().await.unwrap();
                sqlx::query("LOCK TABLE query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
                    .execute(&mut *lock)
                    .await
                    .unwrap();
                let start = std::time::Instant::now();
                let (response, ()) = tokio::join!(get(&enabled, &jwt, route), async {
                    tokio::time::sleep(Duration::from_millis(1100)).await;
                    lock.commit().await.unwrap();
                });
                assert_eq!(
                    response.status(),
                    StatusCode::GATEWAY_TIMEOUT,
                    "policy read reset ingress budget: {route}"
                );
                headers(&response, "applied", "2", 17, true);
                assert!(start.elapsed() < Duration::from_secs(3));
                response.bytes().await.unwrap();
            }
        }
    }
    let next = serve(
        base.clone()
            .with_query_controls(reader.clone(), 18)
            .unwrap(),
    )
    .await;
    for route in ROUTES {
        let r = get(&next, &jwt, route).await;
        assert_eq!(r.status(), StatusCode::OK);
        headers(&r, "inherited", "3", 18, false);
        r.bytes().await.unwrap();
    }
    // Native empty and error bytes/status are compared independently of headers.
    for route in [
        "/api/v1/jaeger/controlled-traces/api/traces?service=not-found",
        "/api/v1/jaeger/controlled-traces/api/traces?minDuration=invalid",
        "/api/v1/jaeger/missing/api/services",
        "/api/v1/jaeger/missing/api/services/checkout/operations",
        "/api/v1/jaeger/missing/api/traces",
        "/api/v1/jaeger/missing/api/traces/01010101010101010101010101010101",
    ] {
        let old = get(&disabled, &jwt, route).await;
        let new = get(&enabled, &jwt, route).await;
        assert_eq!(old.status(), new.status(), "{route}");
        headers(&new, "inherited", "3", 17, false);
        assert_eq!(
            old.bytes().await.unwrap(),
            new.bytes().await.unwrap(),
            "{route}"
        );
    }
    // Auth boundaries still precede policy/work; a forged principal header is not authority.
    let wrong = token(&issuer, "foreign", "jaeger-client");
    let response = get(&enabled, &wrong, ROUTES[0]).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    headers(&response, "inherited", "na", 17, false);
    // A primary failure must win before admission and catalog (even on a warm name cache).
    reader.close();
    let held = enabled_state
        .admission
        .acquire(enabled_state.limits.admission_budget_bytes)
        .await
        .unwrap();
    for route in ROUTES {
        let r = get(&enabled, &jwt, route).await;
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!r.headers().contains_key("x-siglake-query-controls"));
        assert_eq!(
            r.json::<Value>().await.unwrap()["error"],
            "query_policy_unavailable"
        );
    }
    let response = get(
        &enabled,
        &jwt,
        "/api/v1/jaeger/missing/api/traces?limit=18446744073709551615",
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(!response.headers().contains_key("x-siglake-query-controls"));
    drop(held);
    next.shutdown().await;
    enabled.shutdown().await;
    disabled.shutdown().await;
    idp.shutdown().await;
    manager.close();
    admin.close().await;
}
