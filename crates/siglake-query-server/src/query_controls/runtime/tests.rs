use super::*;
use crate::query_controls::{SavedPolicy, SavedSettings};

fn policy(revision: i64, timeout: Option<i64>, warning: Option<i64>) -> PolicyLookup {
    PolicyLookup::Found(SavedPolicy {
        revision,
        settings: SavedSettings::new(timeout, warning).unwrap(),
    })
}
fn snapshot(budget: Duration) -> RequestControls {
    let started = Instant::now();
    RequestControls {
        started,
        deadline: tokio::time::Instant::from_std(started + budget),
        timeout: budget,
        state: PolicyState::Inherited,
        revision: None,
        generation: 1,
        scan_warning: None,
        timeout_opt_out_ignored: false,
    }
}
#[test]
fn timeout_formula_inheritance_reset_warning_and_both_priorities() {
    for tier in [
        TierLimits::interactive_defaults(),
        TierLimits::batch_defaults(),
    ] {
        let empty = RequestLimits::default();
        assert_eq!(
            timeout(&tier, &empty, PolicyLookup::Absent),
            tier.default_timeout
        );
        assert_eq!(
            timeout(&tier, &empty, policy(7, None, None)),
            tier.default_timeout
        );
        assert_eq!(policy(7, None, None).state(), PolicyState::Inherited);
        assert_eq!(policy(7, None, None).revision(), Some(7));
        let warning = policy(8, None, Some(1));
        assert_eq!(warning.state(), PolicyState::Applied);
        assert_eq!(warning.revision(), Some(8));
        assert_eq!(timeout(&tier, &empty, warning), tier.default_timeout);
        assert_eq!(
            timeout(&tier, &empty, policy(9, Some(1), None)),
            Duration::from_secs(1)
        );
        let huge = RequestLimits {
            timeout_seconds: Some(u64::MAX),
            ..Default::default()
        };
        assert_eq!(
            timeout(&tier, &huge, PolicyLookup::Absent),
            tier.ceiling_timeout
        );
        assert_eq!(
            timeout(&tier, &huge, policy(1, Some(2), None)),
            Duration::from_secs(2)
        );
        let zero_false = RequestLimits {
            timeout_seconds: Some(0),
            circuit_breakers: Some(false),
            ..Default::default()
        };
        assert_eq!(
            timeout(&tier, &zero_false, policy(1, Some(2), None)),
            Duration::ZERO
        );
    }
}
#[test]
fn operator_timeout_validation_rejects_zero_overflow_and_inversion() {
    let mut tier = TierLimits::interactive_defaults();
    assert!(validate_tier(&tier).is_ok());
    for invalid in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
        tier.default_timeout = invalid;
        assert_eq!(validate_tier(&tier), Err(PolicyError::Configuration));
    }
    tier.default_timeout = Duration::from_secs(2);
    tier.ceiling_timeout = Duration::from_secs(1);
    assert_eq!(validate_tier(&tier), Err(PolicyError::Configuration));
    tier.ceiling_timeout = Duration::MAX;
    assert_eq!(validate_tier(&tier), Err(PolicyError::Configuration));
}
#[test]
fn runtime_key_uses_only_verified_provider_tuple_not_subject_or_hints() {
    let mut caller = CallerIdentity {
        subject: "untrusted-client-hint".into(),
        email: Some("untrusted-issuer-hint".into()),
        tenant: Some("acme".into()),
        ..Default::default()
    };
    assert_eq!(runtime_key(&caller).unwrap(), None);
    caller.provider_principal = Some(crate::oidc::VerifiedProviderPrincipal {
        issuer: "https://verified.example/exact/".into(),
        client_id: "verified-client".into(),
    });
    let key = runtime_key(&caller).unwrap().unwrap();
    assert_eq!(
        key,
        RuntimeKey::from_trusted_metadata(
            "acme",
            "https://verified.example/exact/",
            "verified-client"
        )
        .unwrap()
    );
    caller.subject = "different-sub".into();
    assert_eq!(runtime_key(&caller).unwrap(), Some(key.clone()));
    caller.tenant = Some("foreign".into());
    assert_ne!(runtime_key(&caller).unwrap(), Some(key));
    caller.tenant = None;
    assert!(runtime_key(&caller).is_err());
    caller.coordinator = true;
    assert!(runtime_key(&caller).is_err());
}
#[tokio::test]
async fn zero_and_late_ready_success_never_poll_or_escape_deadline() {
    let polled = std::sync::atomic::AtomicBool::new(false);
    let result = until(tokio::time::Instant::now(), async {
        polled.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    })
    .await;
    assert_eq!(result.unwrap_err().status, 504);
    assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
    let result = until(
        tokio::time::Instant::now() + Duration::from_millis(10),
        async {
            // Model non-yielding native parser/serialization; no OS-preemption claim.
            std::thread::sleep(Duration::from_millis(20));
            Ok(())
        },
    )
    .await;
    assert_eq!(result.unwrap_err().status, 504);
}
#[tokio::test]
async fn delegation_retains_original_clock_and_optional_row_breakers() {
    let controls = snapshot(Duration::from_millis(40));
    CONTROLS
        .scope(controls, async {
            let req = RequestLimits {
                circuit_breakers: Some(false),
                max_rows_scanned: Some(1),
                ..Default::default()
            };
            let limits = resolve(
                &req,
                Priority::Interactive,
                &TierLimits::interactive_defaults(),
            );
            assert!(!limits.circuit_breakers);
            assert_eq!(limits.max_rows_scanned, 1);
            assert_eq!(limits.timeout, controls.timeout);
            phase(async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok(())
            })
            .await
            .unwrap();
            let result = phase(async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok(())
            })
            .await;
            assert_eq!(result.unwrap_err().status, 504);
            assert_eq!(current().unwrap().started, controls.started);
        })
        .await;
}
#[tokio::test]
async fn cache_control_discriminator_changes_with_trusted_snapshot_and_effective_values() {
    let limits = ResolvedLimits::resolve(
        &RequestLimits::default(),
        Priority::Interactive,
        &TierLimits::interactive_defaults(),
    );
    assert!(cache_discriminator(&limits).is_none());
    let original = snapshot(Duration::from_secs(1));
    let key = CONTROLS
        .scope(original, async { cache_discriminator(&limits).unwrap() })
        .await;
    for altered in [
        RequestControls {
            revision: Some(2),
            ..original
        },
        RequestControls {
            generation: 2,
            ..original
        },
        RequestControls {
            scan_warning: Some(1),
            ..original
        },
        RequestControls {
            timeout: Duration::from_secs(2),
            ..original
        },
    ] {
        assert_ne!(
            key,
            CONTROLS
                .scope(altered, async { cache_discriminator(&limits).unwrap() })
                .await
        );
    }
    let tightened = ResolvedLimits {
        max_rows_scanned: 1,
        ..limits.clone()
    };
    assert_ne!(
        key,
        CONTROLS
            .scope(original, async { cache_discriminator(&tightened).unwrap() })
            .await
    );
}
#[tokio::test]
#[ignore = "owned primary fixture; scripts/check-query-policy.sh"]
async fn generic_primary_preflight_has_no_named_revision_and_closed_is_not_absent() {
    use crate::query_controls::{ConnectionBounds, PolicyConfig};
    let socket = std::path::PathBuf::from(
        std::env::var("SIGLAKE_POLICY_FIXTURE_DIR").expect("use check-query-policy.sh"),
    );
    assert!(
        socket.starts_with("/tmp")
            && socket
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("siglake-policy-")
    );
    assert_eq!(
        std::fs::read_to_string(socket.join("fixture-marker")).unwrap(),
        "new-owned-policy-fixture-v1\n"
    );
    let encoded = socket.to_str().unwrap().replace('/', "%2F");
    let config = PolicyConfig::reader(
        &format!("postgres://qp_reader:@{encoded}:5432/qh_fixture?sslmode=disable"),
        ConnectionBounds {
            max_connections: 1,
            operation_timeout: Duration::from_secs(2),
        },
    )
    .unwrap();
    let reader = Arc::new(PolicyReader::connect(config).await.unwrap());
    let mut limits = ServerLimits::default();
    assert!(matches!(
        ControlsAuthority::new(reader.clone(), &limits, 0),
        Err(PolicyError::Configuration)
    ));
    limits.batch.default_timeout = Duration::ZERO;
    assert!(matches!(
        ControlsAuthority::new(reader.clone(), &limits, 1),
        Err(PolicyError::Configuration)
    ));
    limits = ServerLimits::default();
    let authority = Arc::new(ControlsAuthority::new(reader.clone(), &limits, 1).unwrap());
    let missing_ingress = run(
        Some(authority.clone()),
        CallerIdentity::default(),
        RequestLimits::default(),
        Priority::Interactive,
        async { panic!("configured work must not poll without authenticated ingress scope") },
    )
    .await;
    assert_eq!(missing_ingress.unwrap_err().status, 503);
    ingress(Instant::now(), async {
        run(
            Some(authority.clone()),
            CallerIdentity::default(),
            RequestLimits::default(),
            Priority::Interactive,
            async {
                let c = current().unwrap();
                assert_eq!(c.state, PolicyState::Inherited);
                assert_eq!(c.revision, None);
                assert_eq!(c.timeout, limits.interactive.default_timeout);
                Ok(Response::new(axum::body::Body::empty()))
            },
        )
        .await
        .unwrap();
        reader.close();
        let polled = std::sync::atomic::AtomicBool::new(false);
        let result = run(
            Some(authority.clone()),
            CallerIdentity::default(),
            RequestLimits::default(),
            Priority::Interactive,
            async {
                polled.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(Response::new(axum::body::Body::empty()))
            },
        )
        .await;
        assert_eq!(result.unwrap_err().status, 503);
        assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
        let result = ingress(
            Instant::now() - limits.interactive.default_timeout,
            run(
                Some(authority.clone()),
                CallerIdentity::default(),
                RequestLimits::default(),
                Priority::Interactive,
                async { Ok(Response::new(axum::body::Body::empty())) },
            ),
        )
        .await;
        assert_eq!(result.unwrap_err().status, 504);
    })
    .await;
    authority.shutdown().await;
}

// Reference the single private fixture rather than duplicating key bytes/modules.
use crate::signing_fixture as mixed_signing_fixture;

#[tokio::test]
#[ignore = "owned primary fixture; scripts/check-query-policy.sh"]
async fn cognito_mixed_real_middleware_controls_id_has_no_named_revision_access_uses_stable_tuple()
{
    use crate::query_controls::{
        ConnectionBounds, ManagementBinding, PolicyConfig, PolicyManager, SaveRequest,
        SavedSettings, VerifiedHumanActor,
    };
    use crate::{AppState, AuthConfig, OidcVerifier, TenantRegistry};
    use axum::{body::Body, http::Request, routing::get, Extension, Json, Router};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde_json::{json, Value};
    use siglake_storage::iceberg::IcebergContext;
    use tower::ServiceExt;
    let socket = std::path::PathBuf::from(
        std::env::var("SIGLAKE_POLICY_FIXTURE_DIR").expect("use check-query-policy.sh"),
    );
    assert!(
        socket.starts_with("/tmp")
            && socket
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("siglake-policy-")
    );
    assert_eq!(
        std::fs::read_to_string(socket.join("fixture-marker")).unwrap(),
        "new-owned-policy-fixture-v1\n"
    );
    let encoded = socket.to_str().unwrap().replace('/', "%2F");
    let bounds = ConnectionBounds {
        max_connections: 1,
        operation_timeout: Duration::from_secs(2),
    };
    let manager = PolicyManager::connect(
        PolicyConfig::manager(
            &format!("postgres://qp_manager:@{encoded}:5432/qh_fixture?sslmode=disable"),
            bounds,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let reader = Arc::new(
        PolicyReader::connect(
            PolicyConfig::reader(
                &format!("postgres://qp_reader:@{encoded}:5432/qh_fixture?sslmode=disable"),
                bounds,
            )
            .unwrap(),
        )
        .await
        .unwrap(),
    );
    let (issuer, idp) = mixed_signing_fixture::spawn_fake_idp().await;
    let client = "mixed-controlled-client";
    let binding = ManagementBinding::from_trusted_metadata(
        RuntimeKey::from_trusted_metadata("acme", &issuer, client).unwrap(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
    )
    .unwrap();
    let actor = VerifiedHumanActor::from_trusted_metadata("synthetic-human").unwrap();
    let saved = manager
        .save(
            &binding,
            &actor,
            SaveRequest::new(0, SavedSettings::new(Some(1), None).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    let verifier = OidcVerifier::from_cognito_mixed_issuer(
        issuer.clone(),
        "human-api".into(),
        "tenant".into(),
    )
    .await
    .unwrap();
    let state = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice))
        .with_allowed_tenants(["acme"])
        .with_query_controls(reader.clone(), 1)
        .unwrap();
    let authority = state.query_controls.clone();
    let probe = Router::new().route("/probe", get(move |Extension(identity): Extension<CallerIdentity>| {
        let authority = authority.clone();
        async move {
            run(authority, identity, RequestLimits::default(), Priority::Interactive, async {
                use axum::response::IntoResponse;
                let controls = current().unwrap();
                Ok(Json(json!({"revision":controls.revision,"timeout_ms":controls.timeout.as_millis()})).into_response())
            }).await
        }
    })).route_layer(axum::middleware::from_fn_with_state(state.clone(), crate::auth::middleware));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut claims = json!({"iss":issuer,"sub":client,"exp":now+600,"iat":now,"token_use":"id","aud":"human-api","tenant":"acme", "client_id":client,"name":"same-label", "email":"synthetic@example.invalid", "provider_principal":{"issuer":issuer,"client_id":client}});
    let sign = |claims: &Value| {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(mixed_signing_fixture::TEST_KID.into());
        encode(
            &header,
            claims,
            &EncodingKey::from_rsa_pem(mixed_signing_fixture::TEST_PRIVATE_KEY_PEM).unwrap(),
        )
        .unwrap()
    };
    let request = |claims: &Value| {
        Request::builder()
            .uri("/probe")
            .header("Authorization", format!("Bearer {}", sign(claims)))
            .header("X-Client-Id", client)
            .header("X-Scope-OrgID", "foreign")
            .body(Body::empty())
            .unwrap()
    };
    let response = probe.clone().oneshot(request(&claims)).await.unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["revision"], Value::Null);
    assert_eq!(
        body["timeout_ms"].as_u64().unwrap(),
        state.limits.interactive.default_timeout.as_millis() as u64
    );
    claims["token_use"] = json!("access");
    claims.as_object_mut().unwrap().remove("aud");
    for renewal in [false, true] {
        if renewal {
            claims["sub"] = json!("renewed-subject");
            claims["name"] = json!("changed-label");
            claims["exp"] = json!(now + 660);
        }
        let response = probe.clone().oneshot(request(&claims)).await.unwrap();
        assert_eq!(response.status(), 200);
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["revision"], saved.revision());
        assert_eq!(body["timeout_ms"], 1000);
    }
    claims["client_id"] = json!("different-id-same-label");
    claims["name"] = json!("same-label");
    let response = probe.clone().oneshot(request(&claims)).await.unwrap();
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(body["revision"].is_null());
    // Tenant refusals precede the primary/named-policy consumer entirely: even
    // a closed reader must not turn these into policy-unavailable responses.
    reader.close();
    for use_ in ["id", "access"] {
        claims["token_use"] = json!(use_);
        claims["aud"] = json!("human-api");
        for tenant in [
            json!(null),
            json!(12),
            json!(""),
            json!("bad.tenant"),
            json!("active"),
            json!("foreign"),
        ] {
            claims["tenant"] = tenant;
            assert_eq!(
                probe
                    .clone()
                    .oneshot(request(&claims))
                    .await
                    .unwrap()
                    .status(),
                403
            );
        }
        claims.as_object_mut().unwrap().remove("tenant");
        assert_eq!(
            probe
                .clone()
                .oneshot(request(&claims))
                .await
                .unwrap()
                .status(),
            403
        );
    }
    state.shutdown_query_controls().await;
    idp.abort();
}

#[test]
fn jaeger_typed_headers_are_unavailable_estimate_not_output_clipping_and_encoding_fails_closed() {
    let c = RequestControls {
        state: PolicyState::Applied,
        revision: Some(7),
        generation: 9,
        scan_warning: Some(i64::MAX),
        ..snapshot(Duration::from_secs(1))
    };
    let headers = jaeger_headers(c).unwrap();
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].0, "x-siglake-query-controls");
    assert_eq!(headers[0].1,"v=1;state=applied;rev=7;gen=9;priority=interactive;mode=request;scope=request;mandatory=1;timeout_ms=1000;output_rows=na;output_enforced=0;scan_warning=9223372036854775807;estimate=unavailable;exact=na;estimated_bytes=na");
    assert_eq!(
        headers[1].1,
        "scan_estimate_unavailable,scan_threshold_not_evaluated"
    );
    let invalid = RequestControls { generation: 0, ..c };
    let error = jaeger_headers(invalid).unwrap_err();
    assert_eq!(error.status, 503);
    assert_eq!(error.msg, "query_controls_metadata_unavailable");
    assert!(error.headers.is_empty());
}

#[tokio::test]
async fn jaeger_timeout_adapter_preserves_native_budget_and_generic_errors() {
    for scenario in 0..3 {
        let started = Instant::now();
        let budget = Duration::from_millis(10);
        let deadline = tokio::time::Instant::from_std(started + budget);
        if scenario == 0 {
            std::thread::sleep(Duration::from_millis(15));
        }
        let error = until(deadline, async {
            if scenario == 1 {
                // Late-ready native error: expiry must win before adaptation.
                std::thread::sleep(Duration::from_millis(15));
                return Err(ApiError::internal("late native error"));
            }
            if scenario == 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(())
        })
        .await
        .unwrap_err();
        assert_eq!(error.msg, "query deadline exceeded");
        let native = jaeger_timeout(error, true, budget, started);
        assert_eq!(native.status, 504);
        assert!(native
            .msg
            .starts_with("query exceeded 0s timeout (elapsed: "));
        assert_eq!(
            native.context,
            ApiError::timeout(&crate::cost::CostReport::unknown_after_timeout(), 0.0, 0).context
        );
    }
    let started = Instant::now();
    let effective = jaeger_timeout(expired(), true, Duration::from_secs(1), started);
    assert!(effective.msg.starts_with("query exceeded 1s timeout"));
    // Already-native timeout context/budget must not be replaced by an outer
    // operator bound, and unrelated native 504s are not generic controls expiry.
    let preserved = jaeger_timeout(effective, true, Duration::from_secs(3), started);
    assert!(preserved.msg.starts_with("query exceeded 1s timeout"));
    let other = jaeger_timeout(
        ApiError::new_status(
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            "other native timeout",
        ),
        true,
        Duration::from_secs(1),
        started,
    );
    assert_eq!(other.msg, "other native timeout");
    assert!(other.context.is_none());
    let generic = jaeger_timeout(expired(), false, Duration::from_secs(1), started);
    assert_eq!(generic.msg, "query deadline exceeded");
    assert!(generic.context.is_none());
    let unavailable = jaeger_timeout(unavailable(), true, Duration::from_secs(1), started);
    assert_eq!(unavailable.status, 503);
    assert_eq!(unavailable.msg, "query_policy_unavailable");
    assert!(unavailable.context.is_none());
}

#[tokio::test]
async fn shard_timeout_adapter_preserves_exact_native_body_and_does_not_change_sql_or_jaeger() {
    for budget in [
        Duration::ZERO,
        Duration::from_millis(10),
        Duration::from_secs(1),
    ] {
        for priority in [Priority::Interactive, Priority::Batch] {
            let expected = crate::sql::shard_timeout_error(budget, priority);
            for scenario in 0..3 {
                let deadline = tokio::time::Instant::now() + Duration::from_millis(5);
                if scenario == 0 {
                    std::thread::sleep(Duration::from_millis(10));
                }
                let error = until(deadline, async {
                    if scenario == 1 {
                        std::thread::sleep(Duration::from_millis(10));
                        return Err::<(), _>(ApiError::internal("late-native-error"));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok(())
                })
                .await
                .unwrap_err();
                let native = NativeResponse::Shard.timeout(error, budget, Instant::now(), priority);
                assert_eq!(native.status, expected.status);
                assert_eq!(native.msg, expected.msg);
                assert_eq!(native.context, expected.context);
                assert!(
                    native.context.is_none(),
                    "shard must not gain SQL/Jaeger cost"
                );
            }
        }
    }
    let native = NativeResponse::Shard.timeout(
        crate::sql::shard_timeout_error(Duration::from_secs(1), Priority::Interactive),
        Duration::from_secs(3),
        Instant::now(),
        Priority::Interactive,
    );
    assert_eq!(native.msg, "shard exceeded its wall-clock budget of 1s");
    for error in [ApiError::bad_request("native-invalid-sql"), unavailable()] {
        let status = error.status;
        let msg = error.msg.clone();
        let result = NativeResponse::Shard.timeout(
            error,
            Duration::from_secs(1),
            Instant::now(),
            Priority::Interactive,
        );
        assert_eq!(result.status, status);
        assert_eq!(result.msg, msg);
    }
    assert_eq!(
        NativeResponse::Sql
            .timeout(
                expired(),
                Duration::from_secs(1),
                Instant::now(),
                Priority::Interactive
            )
            .msg,
        "query deadline exceeded"
    );
}
#[test]
fn shard_headers_exact_order_unavailable_estimate_false_optout_and_encoding_failure() {
    let c = RequestControls {
        revision: Some(8),
        state: PolicyState::Applied,
        scan_warning: Some(1),
        timeout_opt_out_ignored: true,
        ..snapshot(Duration::from_secs(1))
    };
    let h = unestimated_headers(c, Priority::Batch).unwrap();
    assert_eq!(h[0].1, "v=1;state=applied;rev=8;gen=1;priority=batch;mode=request;scope=request;mandatory=1;timeout_ms=1000;output_rows=na;output_enforced=0;scan_warning=1;estimate=unavailable;exact=na;estimated_bytes=na");
    assert_eq!(
        h[1].1,
        "timeout_opt_out_ignored,scan_estimate_unavailable,scan_threshold_not_evaluated"
    );
    assert_eq!(
        unestimated_headers(RequestControls { generation: 0, ..c }, Priority::Batch)
            .unwrap_err()
            .msg,
        "query_controls_metadata_unavailable"
    );
}

#[tokio::test]
#[ignore = "owned primary fixture; check-query-policy.sh"]
async fn shard_primary_precedes_work_original_clock_zero_false_and_coordinator_guard() {
    use crate::query_controls::{ConnectionBounds, PolicyConfig};
    let socket = std::path::PathBuf::from(
        std::env::var("SIGLAKE_POLICY_FIXTURE_DIR").expect("use check-query-policy.sh"),
    );
    assert!(
        socket.starts_with("/tmp")
            && socket
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("siglake-policy-")
    );
    assert_eq!(
        std::fs::read_to_string(socket.join("fixture-marker")).unwrap(),
        "new-owned-policy-fixture-v1\n"
    );
    let reader = Arc::new(
        PolicyReader::connect(
            PolicyConfig::reader(
                &format!(
                    "postgres://qp_reader:@{}:5432/qh_fixture?sslmode=disable",
                    socket.to_str().unwrap().replace('/', "%2F")
                ),
                ConnectionBounds {
                    max_connections: 1,
                    operation_timeout: Duration::from_secs(2),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap(),
    );
    let limits = ServerLimits::default();
    let authority = Arc::new(ControlsAuthority::new(reader.clone(), &limits, 1).unwrap());
    for caller in [
        RequestLimits {
            timeout_seconds: Some(0),
            circuit_breakers: Some(false),
            ..Default::default()
        },
        RequestLimits::default(),
    ] {
        let start = if caller.timeout_seconds == Some(0) {
            Instant::now()
        } else {
            Instant::now() - limits.interactive.default_timeout
        };
        let error = ingress(
            start,
            run_inner(
                Some(authority.clone()),
                CallerIdentity::default(),
                caller,
                Priority::Interactive,
                false,
                NativeResponse::Shard,
                async { panic!("expired before primary/work") },
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, 504);
        assert!(error
            .msg
            .starts_with("shard exceeded its wall-clock budget of "));
        assert!(error.context.is_none());
    }
    let error = ingress(
        Instant::now(),
        run_inner(
            Some(authority.clone()),
            CallerIdentity::default(),
            RequestLimits {
                circuit_breakers: Some(false),
                ..Default::default()
            },
            Priority::Interactive,
            false,
            NativeResponse::Shard,
            async {
                let c = current().unwrap();
                assert!(c.timeout_opt_out_ignored);
                // Immediately-ready empty work that crosses the original clock cannot
                // commit success, even if optional breakers are off.
                let mut short = c;
                short.deadline = tokio::time::Instant::now();
                body_scope(
                    short,
                    phase(async { Ok(Response::new(axum::body::Body::empty())) }),
                )
                .await
            },
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, 504);
    assert!(error
        .headers
        .iter()
        .any(|(n, v)| n == "x-siglake-query-warnings"
            && v.to_str().unwrap().starts_with("timeout_opt_out_ignored,")));
    reader.close();
    let error = ingress(
        Instant::now(),
        run_shard(
            Some(authority.clone()),
            CallerIdentity::default(),
            Priority::Interactive,
            async { panic!("closed primary before work") },
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(error.msg, "query_policy_unavailable");
    let error = run_shard(
        Some(authority.clone()),
        CallerIdentity {
            coordinator: true,
            ..Default::default()
        },
        Priority::Interactive,
        async { panic!("unsupported coordinator before policy/work") },
    )
    .await
    .unwrap_err();
    assert_eq!(error.msg, "query_controls_path_unavailable");
    let response = run_shard(
        None,
        CallerIdentity {
            coordinator: true,
            ..Default::default()
        },
        Priority::Interactive,
        async { Ok(Response::new(axum::body::Body::empty())) },
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 200);
    authority.shutdown().await;
}

#[tokio::test]
async fn request_entry_boxes_large_work_before_constructing_lifetime_future() {
    let work = || {
        let payload = [0u8; 64 * 1024];
        async move {
            std::hint::black_box(payload);
            Ok(Response::new(axum::body::Body::empty()))
        }
    };
    let work_size = std::mem::size_of_val(&work());
    macro_rules! checked_entry {
        ($future:expr) => {{
            let entry = $future;
            assert!(
                std::mem::size_of_val(&entry) < work_size,
                "request lifetime frame retained inline work instead of an owned box"
            );
            assert_eq!(entry.await.unwrap().status(), 200);
        }};
    }
    checked_entry!(run_inner(
        None,
        CallerIdentity::default(),
        RequestLimits::default(),
        Priority::Interactive,
        false,
        NativeResponse::Sql,
        work()
    ));
    checked_entry!(run(
        None,
        CallerIdentity::default(),
        RequestLimits::default(),
        Priority::Interactive,
        work()
    ));
    checked_entry!(run_handoff(
        None,
        CallerIdentity::default(),
        RequestLimits::default(),
        Priority::Batch,
        work()
    ));
    checked_entry!(run_jaeger(None, CallerIdentity::default(), work()));
    checked_entry!(run_shard(
        None,
        CallerIdentity::default(),
        Priority::Interactive,
        work()
    ));
}
