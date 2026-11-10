//! Local injected clock/checkpoint proofs, not provider/runtime activation.
use super::*;
use crate::query_controls::runtime::{body_scope, test_controls};
use std::time::Duration;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Point {
    Register,
    Collect,
    Render,
}
tokio::task_local! { static DELAY: Point; }
pub(super) async fn checkpoint(point: Point) {
    if DELAY.try_with(|p| *p == point).unwrap_or(false) {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
pub(super) fn render_checkpoint() {
    if DELAY.try_with(|p| *p == Point::Render).unwrap_or(false) {
        // Cooperative native CPU model: timer cannot preempt this fragment.
        std::thread::sleep(Duration::from_millis(200));
    }
}
async fn state() -> (
    AppState,
    std::sync::Arc<siglake_storage::iceberg::IcebergContext>,
    tempfile::TempDir,
) {
    let (state, ice, tmp) = tests::build_state().await;
    tests::append_trace_fixture(&ice, "siglake-traces-default").await;
    (
        state.with_limits(tests::tuned_limits(
            Duration::from_secs(60),
            64 * 1024 * 1024,
        )),
        ice,
        tmp,
    )
}
#[tokio::test]
async fn original_controls_clock_covers_reservation_register_collect_render_and_releases() {
    let (state, _ice, _tmp) = state().await;
    for point in [Point::Register, Point::Collect, Point::Render] {
        let controls = test_controls(Duration::from_millis(100));
        let started = Instant::now();
        let result = body_scope(
            controls,
            DELAY.scope(point, async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let request = TraceRequest::begin(&state).await?;
                assert_eq!(request.started, controls.started);
                assert_eq!(request.deadline, controls.deadline);
                let traces = request
                    .open(&state, &CallerIdentity::default(), "siglake-traces-default")
                    .await?;
                let names = request.name_list(&traces, NameList::Services).await?;
                request.response(json!({"data":names,"total":names.len()}))
            }),
        )
        .await;
        assert_eq!(result.unwrap_err().status, 504);
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "cooperative fixture exceeded measured bound: {elapsed:?}"
        );
        // A real admission acquire, not merely absence of a handler guard.
        tokio::time::timeout(
            Duration::from_millis(500),
            state.admission.acquire(state.limits.admission_budget_bytes),
        )
        .await
        .unwrap()
        .unwrap();
        eprintln!("checkpoint {} release observed at {elapsed:?}", point as u8);
    }
    let held = state
        .admission
        .acquire(state.limits.admission_budget_bytes)
        .await
        .unwrap();
    let c = test_controls(Duration::from_millis(20));
    let started = Instant::now();
    let error = body_scope(c, TraceRequest::begin(&state))
        .await
        .err()
        .unwrap();
    assert_eq!(error.status, 504);
    assert!(started.elapsed() < Duration::from_millis(500));
    drop(held);
    state
        .admission
        .acquire(state.limits.admission_budget_bytes)
        .await
        .unwrap();
}
#[tokio::test]
async fn controlled_two_phase_budget_empty_ready_error_and_render_never_restart_clock() {
    let (state, _ice, _tmp) = state().await;
    body_scope(test_controls(Duration::from_millis(100)), async {
        let request = TraceRequest::begin(&state).await.unwrap();
        request
            .phase(async {
                tokio::time::sleep(Duration::from_millis(60)).await;
                Ok(())
            })
            .await
            .unwrap();
        let error = request
            .phase(async {
                tokio::time::sleep(Duration::from_millis(60)).await;
                Ok(())
            })
            .await
            .unwrap_err();
        assert_eq!(error.status, 504);
        assert_eq!(
            request
                .phase(async { Ok(Vec::<String>::new()) })
                .await
                .unwrap_err()
                .status,
            504
        );
        assert_eq!(
            request
                .phase(async { Err::<(), _>(ApiError::internal("late")) })
                .await
                .unwrap_err()
                .status,
            504
        );
        assert_eq!(
            request
                .response(json!({"data":[],"total":0}))
                .unwrap_err()
                .status,
            504
        );
    })
    .await;
}
#[tokio::test]
async fn controlled_name_cache_hit_and_follower_have_own_deadline_leader_drop_wakes() {
    let (state, _ice, _tmp) = state().await;
    for list in [
        NameList::Services,
        NameList::Operations {
            service: "checkout".into(),
        },
    ] {
        body_scope(test_controls(Duration::from_secs(3)), async {
            let request = TraceRequest::begin(&state).await.unwrap();
            let traces = request
                .open(&state, &CallerIdentity::default(), "siglake-traces-default")
                .await
                .unwrap();
            // Claim the exact retained cache key, then exercise the real follower.
            let key = format!(
                "{}|{}",
                traces.name_cache_key(&list).unwrap(),
                crate::query_controls::runtime::cache_discriminator(&request.resolved).unwrap()
            );
            let leader = match crate::sql::prepare_name_list_cache(
                key.clone(),
                &request.resolved,
                request.deadline,
            )
            .await
            {
                crate::sql::SqlResultCacheDecision::Leader(leader) => leader,
                _ => panic!("fixture must start as leader"),
            };
            let mut short = crate::query_controls::runtime::current().unwrap();
            short.deadline = tokio::time::Instant::now() + Duration::from_millis(20);
            let error = body_scope(short, request.name_list(&traces, list.clone()))
                .await
                .unwrap_err();
            assert_eq!(error.status, 504, "follower inherited leader budget");
            let follower = traces.list_names(&list, &request.resolved, request.deadline);
            tokio::pin!(follower);
            assert!(futures::poll!(&mut follower).is_pending());
            drop(leader); // synchronous marker removal and notification, no cleanup spawn
            let names = tokio::time::timeout(Duration::from_secs(1), follower)
                .await
                .unwrap()
                .unwrap();
            assert!(!names.is_empty());
            assert_eq!(
                traces
                    .list_names(&list, &request.resolved, request.deadline)
                    .await
                    .unwrap(),
                names
            );
            let mut expired = crate::query_controls::runtime::current().unwrap();
            expired.deadline = tokio::time::Instant::now();
            let error = body_scope(expired, request.name_list(&traces, list.clone()))
                .await
                .unwrap_err();
            assert_eq!(error.status, 504, "expired hit was delivered");
        })
        .await;
    }
}
#[tokio::test]
async fn controlled_buffered_body_never_polled_releases_admission_at_original_deadline() {
    // Reuse the actual authority body owner without policy I/O in this unit seam.
    let (state, _ice, _tmp) = state().await;
    let bodies = crate::query_controls::body::BodyTasks::default();
    let controls = test_controls(Duration::from_millis(100));
    let started = Instant::now();
    let response = body_scope(controls, async {
        let request = TraceRequest::begin(&state).await.unwrap();
        let response = request.response(json!({"data":[],"total":0})).unwrap();
        bodies
            .reserve()
            .unwrap()
            .start(response, controls, None)
            .unwrap()
    })
    .await;
    assert!(state
        .admission
        .acquire(state.limits.admission_budget_bytes)
        .await
        .is_err());
    tokio::time::sleep(Duration::from_millis(120)).await;
    tokio::time::timeout(
        Duration::from_millis(500),
        state.admission.acquire(state.limits.admission_budget_bytes),
    )
    .await
    .unwrap()
    .unwrap();
    bodies.shutdown().await; // observed join, unlike synchronous Drop's abort-only fallback
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(1));
    eprintln!("never-polled source release and producer join observed at {elapsed:?}");
    assert!(axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .is_err());
}

#[tokio::test]
async fn cancelled_name_query_leader_drops_marker_and_wakes_real_follower() {
    let (state, _ice, _tmp) = state().await;
    for list in [
        NameList::Services,
        NameList::Operations {
            service: "checkout".into(),
        },
    ] {
        body_scope(test_controls(Duration::from_secs(3)), async {
            let leader = TraceRequest::begin(&state).await.unwrap();
            let leader_traces = leader
                .open(&state, &CallerIdentity::default(), "siglake-traces-default")
                .await
                .unwrap();
            let follower = TraceRequest::begin(&state).await.unwrap();
            let follower_traces = follower
                .open(&state, &CallerIdentity::default(), "siglake-traces-default")
                .await
                .unwrap();
            let mut pending_leader = Box::pin(DELAY.scope(
                Point::Collect,
                leader.name_list(&leader_traces, list.clone()),
            ));
            assert!(futures::poll!(&mut pending_leader).is_pending());
            let mut pending_follower = Box::pin(follower.name_list(&follower_traces, list));
            assert!(futures::poll!(&mut pending_follower).is_pending());
            let started = Instant::now();
            drop(pending_leader); // cancellation of actual query future, including its cache guard
            let names = tokio::time::timeout(Duration::from_secs(1), pending_follower)
                .await
                .unwrap()
                .unwrap();
            assert!(!names.is_empty());
            let flag = leader.cancel.clone();
            drop(leader);
            assert!(flag.is_cancelled());
            drop(follower);
            state
                .admission
                .acquire(state.limits.admission_budget_bytes)
                .await
                .unwrap();
            eprintln!(
                "cancelled leader wake/query/release observed at {:?}",
                started.elapsed()
            );
        })
        .await;
    }
}

#[tokio::test]
async fn late_ready_native_empty_success_and_error_are_expired_not_delivered() {
    let (state, _ice, _tmp) = state().await;
    for fail in [false, true] {
        body_scope(test_controls(Duration::from_millis(20)), async {
            let request = TraceRequest::begin(&state).await.unwrap();
            let error = request
                .phase(async {
                    std::thread::sleep(Duration::from_millis(30));
                    if fail {
                        Err(ApiError::internal("native late error"))
                    } else {
                        Ok(Vec::<String>::new())
                    }
                })
                .await
                .unwrap_err();
            assert_eq!(error.status, 504);
        })
        .await;
    }
    state
        .admission
        .acquire(state.limits.admission_budget_bytes)
        .await
        .unwrap();
}
