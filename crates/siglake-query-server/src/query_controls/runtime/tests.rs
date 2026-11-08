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
