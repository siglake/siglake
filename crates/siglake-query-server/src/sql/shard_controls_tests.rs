//! Local cooperative stage injection; no forwarded wire or runtime activation.
use super::*;
use crate::query_controls::runtime::{body_scope, test_controls};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    Catalog,
    PreferredOrder,
    Register,
    Pin,
    Schema,
    Collect,
    Render,
}
tokio::task_local! {
    static CAPTURE_CANCEL: Arc<std::sync::Mutex<Option<siglake_storage::QueryCancel>>>;
    static DELAY: Point;
    static VISITED: std::cell::RefCell<Vec<Point>>;
}
pub(crate) async fn capture<F: std::future::Future>(
    cancel: Arc<std::sync::Mutex<Option<siglake_storage::QueryCancel>>>,
    future: F,
) -> F::Output {
    CAPTURE_CANCEL
        .scope(cancel, DELAY.scope(Point::Collect, future))
        .await
}
pub(super) fn capture_cancel(cancel: &siglake_storage::QueryCancel) {
    let _ = CAPTURE_CANCEL.try_with(|slot| *slot.lock().unwrap() = Some(cancel.clone()));
}
pub(super) async fn checkpoint(point: Point) {
    let _ = VISITED.try_with(|v| v.borrow_mut().push(point));
    if DELAY.try_with(|p| *p == point).unwrap_or(false) {
        if point == Point::Render {
            // Model uninterruptible native CPU work; only the next checkpoint
            // can refuse late completion. No OS-preemption claim.
            std::thread::sleep(Duration::from_millis(150));
        } else {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
async fn state() -> (AppState, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        siglake_storage::iceberg::IcebergContext::open(tmp.path())
            .await
            .unwrap(),
    );
    ice.append_events(&[siglake_core::Event::now("shard-value")])
        .await
        .unwrap();
    (AppState::new(ice, crate::AuthConfig::open()), tmp)
}
fn request(point: Point) -> ShardQueryRequest {
    serde_json::from_value(serde_json::json!({
        "query":"SELECT raw FROM events",
        "pin": match point {
            Point::Pin => serde_json::json!({"table":"events", "empty":true,"snapshot_id":null,"schema_id":0}),
            Point::Schema => serde_json::json!({"table":"events", "empty":true,"snapshot_id":null}),
            _ => serde_json::Value::Null,
        }
    })).unwrap()
}
#[tokio::test]
async fn original_shard_clock_covers_preferred_order_register_pin_schema_collect_and_render() {
    let (state, _tmp) = state().await;
    for point in [
        Point::Catalog,
        Point::PreferredOrder,
        Point::Register,
        Point::Pin,
        Point::Schema,
        Point::Collect,
        Point::Render,
    ] {
        let controls = test_controls(Duration::from_millis(80));
        let started = Instant::now();
        let (result, visited) = body_scope(
            controls,
            VISITED.scope(
                std::cell::RefCell::new(Vec::new()),
                DELAY.scope(point, async {
                    let result =
                        shard_scoped(state.clone(), CallerIdentity::default(), request(point))
                            .await;
                    (result, VISITED.with(|v| v.take()))
                }),
            ),
        )
        .await;
        let error = result.unwrap_err();
        assert_eq!(error.status, 504, "{point:?}");
        assert_eq!(error.msg, "shard exceeded its wall-clock budget of 0s");
        assert!(error.context.is_none());
        assert_eq!(
            visited.last(),
            Some(&point),
            "work continued after {point:?}: {visited:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        // No new DataFusion admission reservation, including direct shards.
        tokio::time::timeout(
            Duration::from_millis(200),
            state.admission.acquire(state.limits.admission_budget_bytes),
        )
        .await
        .unwrap()
        .unwrap();
        eprintln!("shard {point:?} refusal/release at {:?}", started.elapsed());
    }
}
#[tokio::test]
async fn expired_shard_never_polls_catalog_even_for_ready_empty_results_and_no_double_admission() {
    let (state, _tmp) = state().await;
    for query in ["SELECT 1", "SELECT raw FROM events WHERE false"] {
        let req = || serde_json::from_value(serde_json::json!({"query":query})).unwrap();
        let controls = test_controls(Duration::ZERO);
        let error = body_scope(
            controls,
            VISITED.scope(std::cell::RefCell::new(Vec::new()), async {
                let error = shard_scoped(state.clone(), CallerIdentity::default(), req())
                    .await
                    .unwrap_err();
                assert!(VISITED.with(|v| v.borrow().is_empty()));
                error
            }),
        )
        .await;
        assert_eq!(error.status, 504);
        // Saturate the actual existing controller. Controlled direct collection
        // still succeeds; body-registry ownership is not query admission.
        let held = state
            .admission
            .acquire(state.limits.admission_budget_bytes)
            .await
            .unwrap();
        let response = body_scope(
            test_controls(Duration::from_secs(3)),
            shard_scoped(state.clone(), CallerIdentity::default(), req()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), 200);
        drop(held);
    }
}

/// The recorder is thread-local only DURING EACH POLL, never merely during
/// future construction and never installed globally across concurrent tests.
async fn recorded_timeout_trips<F: std::future::Future>(future: F) -> (F::Output, u64) {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let recorder = DebuggingRecorder::new();
    let snapshot = recorder.snapshotter();
    let mut future = Box::pin(future);
    let result = futures::future::poll_fn(|cx| {
        metrics::with_local_recorder(&recorder, || future.as_mut().poll(cx))
    })
    .await;
    let trips = snapshot
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| {
            let key = key.key();
            (key.name() == "siglake_query_breaker_trips_total"
                && key
                    .labels()
                    .any(|l| l.key() == "breaker" && l.value() == "shard_timeout")
                && key
                    .labels()
                    .any(|l| l.key() == "priority" && l.value() == "interactive"))
            .then_some(value)
        })
        .map(|value| match value {
            DebugValue::Counter(n) => n,
            other => panic!("not a counter: {other:?}"),
        })
        .sum();
    (result, trips)
}

#[tokio::test]
#[ignore = "owned primary fixture; check-shard-policy.sh"]
async fn controlled_shard_timeout_counter_has_one_owner_through_complete_wrapper() {
    use crate::query_controls::runtime::ingress;
    use crate::query_controls::{ConnectionBounds, PolicyConfig, PolicyReader};
    let socket = std::path::PathBuf::from(
        std::env::var("SIGLAKE_POLICY_FIXTURE_DIR").expect("use check-shard-policy.sh"),
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
    let (base, _tmp) = state().await;
    let mut limits = crate::ServerLimits::default();
    limits.interactive.default_timeout = Duration::from_millis(80);
    limits.interactive.ceiling_timeout = Duration::from_millis(80);
    let base = base.with_limits(limits);
    let enabled = base.clone().with_query_controls(reader.clone(), 1).unwrap();
    for (name, configured, delay, expired, query, expected_status, expected_trips) in [
        (
            "controlled-late-render",
            true,
            Some(Point::Render),
            false,
            None,
            504,
            1,
        ),
        (
            "controlled-awaited-collect",
            true,
            Some(Point::Collect),
            false,
            None,
            504,
            1,
        ),
        ("controlled-early-ingress", true, None, true, None, 504, 1),
        (
            "controlled-unrelated-error",
            true,
            None,
            false,
            Some(""),
            400,
            0,
        ),
        (
            "disabled-late-render",
            false,
            Some(Point::Render),
            false,
            None,
            504,
            1,
        ),
        (
            "disabled-awaited-collect",
            false,
            Some(Point::Collect),
            false,
            None,
            504,
            1,
        ),
        (
            "disabled-unrelated-error",
            false,
            None,
            false,
            Some(""),
            400,
            0,
        ),
        ("controlled-policy503", true, None, false, None, 503, 0),
    ] {
        if name == "controlled-policy503" {
            reader.close();
        }
        let state = if configured {
            enabled.clone()
        } else {
            base.clone()
        };
        let mut req = request(Point::Render);
        if let Some(query) = query {
            req.query = query.into();
        }
        let started = Instant::now()
            - if expired {
                Duration::from_secs(1)
            } else {
                Duration::ZERO
            };
        let work = Box::pin(shard(
            State(state),
            axum::extract::Extension(CallerIdentity::default()),
            Json(req),
        ));
        let execute = async {
            let future = ingress(started, work);
            match delay {
                Some(point) => DELAY.scope(point, future).await,
                None => future.await,
            }
        };
        let (result, trips) = recorded_timeout_trips(execute).await;
        let error = result.unwrap_err();
        assert_eq!(error.status.as_u16(), expected_status, "{name}");
        if expected_status == 504 {
            assert_eq!(
                error.msg, "shard exceeded its wall-clock budget of 0s",
                "{name}"
            );
            assert!(
                error.context.is_none(),
                "native shard timeout must not gain SQL/Jaeger cost"
            );
            assert!(
                started.elapsed() >= Duration::from_millis(80),
                "original deadline: {name}"
            );
        }
        if configured && !expired && expected_status != 503 {
            let value = error
                .headers
                .iter()
                .find(|(n, _)| n == "x-siglake-query-controls")
                .unwrap()
                .1
                .to_str()
                .unwrap();
            assert!(value.starts_with("v=1;state=inherited;rev=na;gen=1;"));
            assert!(
                value.contains(";mandatory=1;timeout_ms=80;"),
                "{name}: {value}"
            );
        } else {
            assert!(error.headers.is_empty(), "no fabricated snapshot: {name}");
        }
        eprintln!(
            "{name}: status={expected_status} shard_timeout_trips={trips} elapsed={:?}",
            started.elapsed()
        );
        assert_eq!(
            trips, expected_trips,
            "{name}: exactly one pre-header accounting owner"
        );
    }
    enabled.shutdown_query_controls().await;
}
