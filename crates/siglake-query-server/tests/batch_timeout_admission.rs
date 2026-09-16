//! A batch run that times out counts the timeout breaker once and releases the
//! admission share it was holding.
//!
//! Its own binary on purpose: the debugging recorder is process-global (same
//! reason as `batch_admission.rs`). All configuration is passed directly; this
//! test never mutates the environment.
//!
//! The reservation is taken at submission and lives in the spawned run, so the
//! run's wall-clock budget is what bounds how long a batch job can hold the
//! tier's share. Before the run-wide deadline, preparation was outside that
//! budget entirely.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use chrono::Utc;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde_json::Value;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, JobStore, ServerLimits};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

#[derive(Default)]
struct TimeoutMetrics {
    reserved: Option<f64>,
    breaker: u64,
    jobs: u64,
}

impl TimeoutMetrics {
    fn read(snapshotter: &Snapshotter) -> Self {
        let mut metrics = Self::default();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            if key.key().name() == "siglake_query_admission_reserved_bytes" {
                if let DebugValue::Gauge(value) = value {
                    metrics.reserved = Some(value.into_inner());
                }
                continue;
            }
            let labels = key.key().labels().collect::<Vec<_>>();
            let DebugValue::Counter(value) = value else {
                continue;
            };
            if key.key().name() == "siglake_query_breaker_trips_total"
                && labels
                    .iter()
                    .any(|label| label.key() == "breaker" && label.value() == "timeout")
                && labels
                    .iter()
                    .any(|label| label.key() == "priority" && label.value() == "batch")
            {
                metrics.breaker = value;
            }
            if key.key().name() == "siglake_query_jobs_total"
                && labels
                    .iter()
                    .any(|label| label.key() == "outcome" && label.value() == "timeout")
                && labels
                    .iter()
                    .any(|label| label.key() == "priority" && label.value() == "batch")
            {
                metrics.jobs = value;
            }
        }
        metrics
    }
}

async fn json(app: &Router, method: Method, path: &str, body: Option<serde_json::Value>) -> Value {
    let request = Request::builder().method(method).uri(path);
    let request = match body {
        Some(body) => request
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap(),
        None => request.body(Body::empty()).unwrap(),
    };
    let response = app.clone().oneshot(request).await.unwrap();
    assert!(
        matches!(response.status(), StatusCode::OK | StatusCode::ACCEPTED),
        "unexpected status {}",
        response.status()
    );
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn a_timed_out_batch_run_counts_one_breaker_and_releases_its_reservation() {
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
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let events: Vec<Event> = (0..4)
        .map(|i| Event {
            timestamp: Utc::now(),
            host: format!("host-{i}"),
            source: "batch-timeout".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: format!("event {i}"),
            attributes: None,
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    let state = AppState::new(Arc::new(ice), AuthConfig::open())
        .with_limits(limits)
        .with_jobs(JobStore::new(1, Duration::from_secs(30)));
    let batch_reservation = state.admission.batch_reservation_bytes();
    let app = router(state);

    // `count(*)` is served from the footers in one poll: the shape that used to
    // outrun the collect-only timeout wrapper.
    let submitted = json(
        &app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "priority": "batch",
            "limits": { "timeout_seconds": 0 }
        })),
    )
    .await;
    let job_id = submitted["job_id"].as_str().unwrap().to_string();
    assert!(batch_reservation > 0, "batch tier reserves nothing");

    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let body = json(&app, Method::GET, &format!("/api/v1/jobs/{job_id}"), None).await;
            if matches!(
                body["status"].as_str().unwrap(),
                "succeeded" | "failed" | "cancelled" | "timeout"
            ) {
                return body;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("batch job never finished");
    assert_eq!(
        status["status"], "timeout",
        "the zero-budget run did not time out: {status}"
    );

    // The row becomes terminal before the spawned run has necessarily emitted
    // both counters and dropped its reservation. Wait for that whole terminal
    // report rather than treating the first zero gauge as a metric barrier.
    let metrics = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let metrics = TimeoutMetrics::read(&snapshotter);
            if metrics.reserved == Some(0.0) && metrics.breaker == 1 && metrics.jobs == 1 {
                return metrics;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the timed-out batch job metrics did not converge");

    assert_eq!(
        metrics.breaker, 1,
        "one batch timeout must be one timeout breaker trip"
    );
    assert_eq!(
        metrics.jobs, 1,
        "the separately graphed job outcome must still be counted once"
    );
}
