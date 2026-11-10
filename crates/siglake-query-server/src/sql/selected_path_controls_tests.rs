//! Actual explicit-router selection seams with cooperative stage injection.
//! No production delay, clock override, flag-based lifetime or fake response.
use super::*;
use crate::query_controls::{
    runtime, ConnectionBounds, DistributedControlsConfig, PolicyConfig, PolicyReader,
};
use crate::ServerLimits;
use std::{cell::Cell, time::Duration};
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Point {
    Classification,
    ScanLocal,
}
tokio::task_local! {
    static DELAY: (Point, bool);
    static VISITED: Cell<usize>;
}
pub(super) async fn checkpoint(point: Point) {
    let _ = VISITED.try_with(|n| n.set(n.get() + 1));
    if let Ok((selected, native)) = DELAY.try_with(|p| *p) {
        if point == selected {
            if native {
                std::thread::sleep(Duration::from_millis(1200));
            } else {
                tokio::time::sleep(Duration::from_millis(1200)).await;
            }
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "owned policy PG fixture; check-selected-paths.sh"]
async fn actual_explicit_classification_and_scan_local_cannot_upgrade_late_ready_or_pending_work() {
    let socket = std::path::PathBuf::from(std::env::var("SIGLAKE_POLICY_FIXTURE_DIR").unwrap());
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
                    max_connections: 2,
                    operation_timeout: Duration::from_secs(2),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap(),
    );
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        siglake_storage::iceberg::IcebergContext::open(tmp.path())
            .await
            .unwrap(),
    );
    ice.append_events(&[siglake_core::Event::now("selected-row")])
        .await
        .unwrap();
    let mut limits = ServerLimits::default();
    limits.batch.default_timeout = Duration::from_secs(3);
    limits.batch.ceiling_timeout = Duration::from_secs(3);
    let state = AppState::new(ice, crate::AuthConfig::open())
        .with_limits(limits)
        .with_coordinator(
            vec!["http://127.0.0.1:9".into()],
            Some("synthetic-coordinator".into()),
        )
        .with_distributed_query_controls(
            reader.clone(),
            1,
            DistributedControlsConfig::new(Duration::ZERO).unwrap(),
        )
        .unwrap();
    for (point, query) in [
        (Point::Classification, "SELECT 1"),
        (Point::ScanLocal, "SELECT raw FROM events LIMIT 1"),
    ] {
        for native in [false, true] {
            let request=serde_json::from_value(serde_json::json!({"query":query,"priority":"batch","default_order":false,"limits":{"timeout_seconds":1,"circuit_breakers":false}})).unwrap();
            let response = runtime::ingress(
                Instant::now(),
                DELAY.scope(
                    (point, native),
                    distributed(
                        State(state.clone()),
                        axum::Extension(CallerIdentity::default()),
                        Json(request),
                    ),
                ),
            )
            .await;
            assert_eq!(response.unwrap_err().status, 504);
            assert_eq!(
                state.jobs.active_count().await,
                0,
                "late selection fabricated a job"
            );
        }
    }
    for query in [
        "SELECT 1",
        "SELECT raw FROM events LIMIT 1",
        "SELECT count(*) FROM events",
    ] {
        let request=serde_json::from_value(serde_json::json!({"query":query,"priority":"batch","limits":{"timeout_seconds":0,"circuit_breakers":false}})).unwrap();
        VISITED
            .scope(Cell::new(0), async {
                let response = runtime::ingress(
                    Instant::now(),
                    distributed(
                        State(state.clone()),
                        axum::Extension(CallerIdentity::default()),
                        Json(request),
                    ),
                )
                .await;
                assert_eq!(response.unwrap_err().status, 504);
                assert_eq!(
                    VISITED.with(Cell::get),
                    0,
                    "zero budget polled classification"
                );
            })
            .await;
    }
    state.shutdown_query_controls().await;
    state.jobs.shutdown().await.unwrap();
    reader.close();
}
