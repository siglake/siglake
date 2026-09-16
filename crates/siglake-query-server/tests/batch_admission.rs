//! Batch submissions consume the same admission budget as interactive work.
//!
//! Its own binary on purpose: the debugging recorder is process-global. All
//! configuration is passed directly; this test never mutates the environment.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};
use siglake_query_server::{router, AppState, AuthConfig, JobStore, ServerLimits};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

fn gauge_value(snapshot: Snapshot, name: &str) -> Option<f64> {
    snapshot
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, value)| {
            (key.key().name() == name)
                .then(|| match value {
                    DebugValue::Gauge(value) => Some(value.into_inner()),
                    _ => None,
                })
                .flatten()
        })
}

async fn post(app: &Router, path: &str, body: serde_json::Value) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(path)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn two_batch_submissions_reserve_admission_and_the_second_gets_429() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    const BUDGET: u64 = 64 * 1024 * 1024;
    let limits = ServerLimits {
        admission_budget_bytes: BUDGET,
        admission_wait_timeout: Duration::from_millis(25),
        ..ServerLimits::default()
    };
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let jobs = JobStore::new(1, Duration::from_secs(5));

    // Occupy the only batch worker so the first admitted job remains queued
    // while the second submission reaches admission.
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    jobs.batch_runtime().spawn(async move {
        let _ = started_tx.send(());
        let _ = release_rx.recv();
    });
    started_rx.await.unwrap();

    let state = AppState::new(ice, AuthConfig::open())
        .with_limits(limits)
        .with_jobs(jobs);
    let batch_reservation = state.admission.batch_reservation_bytes();
    assert_eq!(batch_reservation, BUDGET / 4);

    // Leave exactly one batch share free. This makes the first submission's
    // reservation and the second submission's rejection deterministic while
    // still driving both through the HTTP batch path.
    let baseline = state
        .admission
        .acquire(BUDGET - batch_reservation)
        .await
        .unwrap();
    let app = router(state.clone());
    let request = serde_json::json!({
        "query": "SELECT count(*) AS n FROM events",
        "priority": "batch"
    });

    let first = post(&app, "/api/v1/sql", request.clone()).await;
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    assert_eq!(
        gauge_value(
            snapshotter.snapshot(),
            "siglake_query_admission_reserved_bytes"
        ),
        Some(BUDGET as f64),
        "the accepted job must hold its batch share after submission returns"
    );
    let first_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(first.into_body(), usize::MAX).await.unwrap()).unwrap();

    let second = post(&app, "/api/v1/sql", request).await;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        second
            .headers()
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );

    // DELETE aborts the queued future. Once the runtime can poll that abort,
    // its captured admission guard is dropped and the gauge returns to the
    // explicit baseline reservation.
    let job_id = first_body["job_id"].as_str().unwrap();
    let cancelled = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("/api/v1/jobs/{job_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::ACCEPTED);
    release_tx.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if gauge_value(
                snapshotter.snapshot(),
                "siglake_query_admission_reserved_bytes",
            ) == Some((BUDGET - batch_reservation) as f64)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled batch job did not release its admission reservation");

    let status = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/jobs/{job_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(status.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(status_body["status"], "cancelled");

    drop(baseline);
    assert_eq!(
        gauge_value(
            snapshotter.snapshot(),
            "siglake_query_admission_reserved_bytes"
        ),
        Some(0.0)
    );
}
