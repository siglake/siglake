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
    static DELAY: Point;
    static VISITED: std::cell::RefCell<Vec<Point>>;
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
