use super::*;
use serde_json::{json, Value};
pub(super) fn context() -> Context {
    serde_json::from_value(json!({"version":2,"schema":1,"revision":1,"state":"applied","generation":1,"priority":"interactive","timeout_ms":1000,"scan_warning":1,"timeout_opt_out_ignored":false,"binding":{"kind":"provider","tenant":"acme","issuer":"https://synthetic.invalid","client":"synthetic"},"original_expires_unix_ms":2000,"remaining_ms":900,"clock_skew_ms":50})).unwrap()
}
#[test]
fn wire_clock_both_signs_boundary_transit_and_operator_clamp_never_extend() {
    // Pure integer clock model, NOT a real cross-host measurement. Coordinator
    // now=1100, expiry=2000, transit=0..1000, W-C in [-S,S].
    for transit in 0..=1000 {
        for skew in -50..=50 {
            let coordinator_now = 1100 + transit;
            let worker_now = coordinator_now + skew;
            let got = remaining_ms(2000, worker_now, 50, 900, 700);
            if let Some(n) = got {
                assert!(n <= 2000 - coordinator_now);
                assert!(n <= 700);
            } else {
                assert!(2000 - worker_now - 50 <= 0);
            }
        }
    }
    assert_eq!(remaining_ms(2000, 1050, 50, 900, 1000), Some(900));
    assert_eq!(remaining_ms(2000, 1150, 50, 900, 1000), Some(800));
    // Beyond the explicit bound the theorem is not universal. Ahead remains
    // conservative; a behind clock can extend after transit (shown honestly).
    assert_eq!(remaining_ms(2000, 1151, 50, 900, 1000), Some(799));
    let out_of_contract = remaining_ms(2000, 1900, 50, 900, 1000).unwrap();
    assert!(out_of_contract > 2000 - 1951); // worker behind by 51 > S
}
#[test]
fn wire_checked_floor_ceil_negative_overflow_zero_and_expiry() {
    assert_eq!(
        unix_ms_floor(UNIX_EPOCH + Duration::from_nanos(1_999_999)),
        Some(1)
    );
    assert_eq!(
        unix_ms_ceil(UNIX_EPOCH + Duration::from_nanos(1_000_001)),
        Some(2)
    );
    assert_eq!(unix_ms_ceil(UNIX_EPOCH - Duration::from_nanos(1)), None);
    assert_eq!(
        unix_ms_floor(UNIX_EPOCH + Duration::from_secs(i64::MAX as u64)),
        None
    );
    for args in [
        (1, -1, 0, 1, 1),
        (1, 0, -1, 1, 1),
        (1, 0, 0, 0, 1),
        (1, 0, 0, 1, 0),
        (1, 1, 0, 1, 1),
        (i64::MAX, i64::MAX, 1, 1, 1),
    ] {
        assert_eq!(remaining_ms(args.0, args.1, args.2, args.3, args.4), None);
    }
    assert!(DistributedControlsConfig::new(Duration::from_nanos(1)).is_err());
    assert!(DistributedControlsConfig::new(Duration::MAX).is_err());
}
#[test]
fn wire_strict_missing_unsupported_bounds_binding_and_no_jwt_provenance() {
    let good = serde_json::to_value(context()).unwrap();
    for field in good.as_object().unwrap().keys() {
        let mut bad = good.clone();
        bad.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<Context>(bad).is_err(),
            "missing {field}"
        );
    }
    for (field, value) in [
        ("version", json!(1)),
        ("priority", json!("urgent")),
        ("timeout_ms", json!(9223372036854775808u64)),
        ("extension", json!({})),
        ("state", json!("unavailable")),
    ] {
        let mut bad = good.clone();
        bad[field] = value;
        assert!(serde_json::from_value::<Context>(bad).is_err(), "{field}");
    }
    let config = DistributedControlsConfig::new(Duration::from_millis(50)).unwrap();
    assert!(context().validate(config).is_ok());
    for (field, value) in [
        ("schema", json!(2)),
        ("remaining_ms", json!(0)),
        ("remaining_ms", json!(1001)),
        ("timeout_ms", json!(-1)),
        ("revision", Value::Null),
        ("generation", json!(0)),
        ("scan_warning", json!(0)),
        ("clock_skew_ms", json!(49)),
    ] {
        let mut bad = good.clone();
        bad[field] = value;
        assert!(
            serde_json::from_value::<Context>(bad)
                .unwrap()
                .validate(config)
                .is_err(),
            "{field}"
        );
    }
    let mut bad = good;
    bad["binding"]["tenant"] = json!("bad.tenant");
    assert!(serde_json::from_value::<Context>(bad)
        .unwrap()
        .validate(config)
        .is_err());
    assert!(matches!(
        Binding::verified(&CallerIdentity::default()).unwrap(),
        Binding::Operator { .. }
    ));
}
#[tokio::test]
async fn wire_disabled_and_external_context_refuse_before_polling_work() {
    let context = context();
    let coordinator = CallerIdentity {
        coordinator: true,
        ..Default::default()
    };
    let error = run_forwarded(None, &coordinator, &context, async {
        panic!("disabled worker polled work");
    })
    .await
    .unwrap_err();
    assert_eq!(error.status, 503);
    let error = run_forwarded(None, &CallerIdentity::default(), &context, async {
        panic!("external caller polled work");
    })
    .await
    .unwrap_err();
    assert_eq!(error.status, 403);
    use tower::ServiceExt;
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        siglake_storage::iceberg::IcebergContext::open(tmp.path())
            .await
            .unwrap(),
    );
    for token in ["", " "] {
        let state = crate::AppState::new(ice.clone(), crate::AuthConfig::open())
            .with_coordinator(vec![], Some(token.into()));
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(PATH)
            .header("authorization", "Bearer ")
            .header("content-type", "application/json")
            .body(axum::body::Body::from("{}"))
            .unwrap();
        let response = crate::router(state).oneshot(request).await.unwrap();
        assert_eq!(
            response.status(),
            403,
            "empty service token authenticated v2 coordinator"
        );
    }
}
#[tokio::test]
async fn wire_retry_context_uses_same_original_expiry_and_decreasing_remaining() {
    let dispatch = Dispatch {
        context: context(),
        deadline: tokio::time::Instant::now() + Duration::from_millis(800),
    };
    let first = dispatch.context().unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = dispatch.clone().context().unwrap();
    assert_eq!(
        first.original_expires_unix_ms,
        second.original_expires_unix_ms
    );
    assert!(second.remaining_ms < first.remaining_ms);
    let expired = Dispatch {
        context: context(),
        deadline: tokio::time::Instant::now(),
    };
    assert!(expired.context().is_err());
}
