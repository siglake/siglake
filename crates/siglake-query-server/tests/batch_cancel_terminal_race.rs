//! A queued batch run whose start is refused after a peer cancelled it must
//! never execute.
//!
//! THE DEFECT THIS GUARDS. Every terminal write on a job row is conditional on
//! `status IN ('pending','running')`, so a cancellation that lands first wins.
//! The executor used to log that its `running` publication was refused and
//! execute the whole query anyway, holding admission until a terminal write
//! rediscovered the same cancellation.
//!
//! The race is made deterministic rather than raced for: A's only batch worker
//! is occupied while B persists the cancellation, and A's cancellation sweep is
//! deliberately NOT run. When the worker is freed, its first store operation
//! sees the cancellation and the query future must be dropped unpolled.
//!
//! Its own binary on purpose: the debugging recorder is process-global. All
//! configuration is passed directly; this test never mutates the environment.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use chrono::Utc;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuditService, AuthConfig, JobStore, ServerLimits};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

/// `Snapshotter::snapshot` drains every counter it reports, so a polling loop
/// destroys increments it has not read yet. Accumulate instead.
#[derive(Default)]
struct Counters {
    seen: HashMap<(String, Vec<(String, String)>), u64>,
}

impl Counters {
    fn read(&mut self, snapshotter: &Snapshotter) {
        for (key, _, _, observed) in snapshotter.snapshot().into_vec() {
            if let DebugValue::Counter(n) = observed {
                let labels = key
                    .key()
                    .labels()
                    .map(|l| (l.key().to_string(), l.value().to_string()))
                    .collect();
                *self
                    .seen
                    .entry((key.key().name().to_string(), labels))
                    .or_default() += n;
            }
        }
    }

    fn total(&self, name: &str) -> u64 {
        self.seen
            .iter()
            .filter(|((n, _), _)| n == name)
            .map(|(_, v)| *v)
            .sum()
    }

    /// The series under `name` carrying every one of `labels`, summed.
    fn with_labels(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.seen
            .iter()
            .filter(|((n, have), _)| {
                n == name
                    && labels
                        .iter()
                        .all(|(k, v)| have.iter().any(|(hk, hv)| hk == k && hv.as_str() == *v))
            })
            .map(|(_, v)| *v)
            .sum()
    }
}

async fn send(
    app: &Router,
    method: Method,
    path: &str,
    body: Option<serde_json::Value>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(path);
    let body = match body {
        Some(json) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&json).unwrap())
        }
        None => Body::empty(),
    };
    app.clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

/// `status` of the most recent batch row in `query_audit`, or `None` while the
/// table has no batch row yet.
async fn batch_audit_status(ice: &IcebergContext) -> Option<String> {
    use datafusion::arrow::array::StringArray;
    let ctx = datafusion::prelude::SessionContext::new();
    ice.register_query_audit_with_datafusion(&ctx).await.ok()?;
    let df = ctx
        .sql(
            "SELECT status FROM query_audit WHERE priority = 'batch' \
             ORDER BY timestamp DESC LIMIT 1",
        )
        .await
        .ok()?;
    let batches = df.collect().await.ok()?;
    let batch = batches.first()?;
    if batch.num_rows() == 0 {
        return None;
    }
    Some(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()?
            .value(0)
            .to_string(),
    )
}

#[tokio::test]
async fn a_start_refused_by_a_peers_cancellation_never_executes() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");
    let mut counters = Counters::default();

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    // Real rows, so the run has a genuine answer to have discarded.
    let events: Vec<Event> = (0..8)
        .map(|i| Event {
            timestamp: Utc::now(),
            host: format!("h{i}"),
            source: "cancel-race".into(),
            sourcetype: "t".into(),
            index: "main".into(),
            raw: format!("e{i}"),
            attributes: None,
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    let (service, writer) = AuditService::new(ice.clone(), 1, Duration::from_millis(50));
    let audit = tokio::spawn(service.run());

    // --- replica A: the executor, with its only batch worker occupied so the
    // submitted job cannot reach its completion arm until we say so.
    let a_jobs = JobStore::new(1, Duration::from_secs(600));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    a_jobs.batch_runtime().spawn(async move {
        let _ = started_tx.send(());
        let _ = release_rx.recv();
    });
    started_rx.await.unwrap();

    const BUDGET: u64 = 64 * 1024 * 1024;
    let limits = ServerLimits {
        admission_budget_bytes: BUDGET,
        admission_wait_timeout: Duration::from_millis(25),
        ..ServerLimits::default()
    };
    let state_a = AppState::new(ice.clone(), AuthConfig::open())
        .with_limits(limits)
        .with_jobs(a_jobs)
        .with_audit(writer);
    let batch_reservation = state_a.admission.batch_reservation_bytes();
    let _baseline = state_a
        .admission
        .acquire(BUDGET - batch_reservation)
        .await
        .expect("leave exactly one batch reservation free");
    let app_a = router(state_a.clone());

    // --- replica B: same job table, its own abort map.
    let b_jobs = JobStore::new_peer_of(&state_a.jobs, 1, Duration::from_secs(600))
        .expect("in-memory peer store");
    let app_b = router(AppState::new(ice.clone(), AuthConfig::open()).with_jobs(b_jobs));

    let submitted = send(
        &app_a,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "priority": "batch"
        })),
    )
    .await;
    assert_eq!(submitted.status(), StatusCode::ACCEPTED);
    let job_id = json_body(submitted).await["job_id"]
        .as_str()
        .expect("submit response carries job_id")
        .to_string();

    let cancelled = send(
        &app_b,
        Method::DELETE,
        &format!("/api/v1/jobs/{job_id}"),
        None,
    )
    .await;
    assert_eq!(cancelled.status(), StatusCode::ACCEPTED);

    // Release the worker WITHOUT sweeping. The start publication itself sees
    // the cancellation and must drop the still-unpolled query future.
    release_tx.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            counters.read(&snapshotter);
            if counters.total("siglake_query_job_terminal_conflict_total") > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the batch run never reported that its start had been refused");

    // Let the spawned frame finish `clear_abort`, then prove both process-local
    // resources were cleaned up. A stale abort registration would make this
    // cancellation sweep return the id; a held reservation would time out.
    tokio::task::yield_now().await;
    assert!(
        state_a
            .jobs
            .propagate_cancellations()
            .await
            .expect("cancellation sweep")
            .is_empty(),
        "the refused start left its abort registration behind"
    );
    let _released = state_a
        .admission
        .acquire(batch_reservation)
        .await
        .expect("the refused start releases its admission reservation");
    counters.read(&snapshotter);

    assert_eq!(
        counters.with_labels(
            "siglake_query_job_terminal_conflict_total",
            &[
                ("attempted", "running"),
                ("actual", "cancelled"),
                ("cause", "cancellation"),
            ]
        ),
        1,
        "the refused start must name both what it attempted and what won"
    );
    assert_eq!(
        counters.with_labels("siglake_query_jobs_total", &[("outcome", "succeeded")]),
        0,
        "a job whose row reads `cancelled` must not be counted as succeeded"
    );
    assert_eq!(
        counters.total("siglake_query_jobs_total"),
        0,
        "a refused start claims no outcome at all"
    );
    assert_eq!(
        counters.total("siglake_query_jobs_cancel_propagated_total"),
        0,
        "this test exercises the race, not the sweep -- no abort was propagated"
    );

    // The row is unchanged, and execution never populated start or cost.
    let status = send(&app_a, Method::GET, &format!("/api/v1/jobs/{job_id}"), None).await;
    assert_eq!(status.status(), StatusCode::OK);
    let body = json_body(status).await;
    assert_eq!(body["status"], "cancelled", "the cancellation must stand");
    assert_eq!(body["started_at"], serde_json::Value::Null);
    assert_eq!(body["cost"], serde_json::Value::Null);
    let result = send(
        &app_a,
        Method::GET,
        &format!("/api/v1/jobs/{job_id}/result"),
        None,
    )
    .await;
    assert_eq!(
        result.status(),
        StatusCode::CONFLICT,
        "a cancelled job must not serve a result"
    );

    // And the audit trail agrees with the row rather than inventing an outcome.
    let audited = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = batch_audit_status(&ice).await {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("no batch row landed in query_audit");
    assert_eq!(
        audited, "cancelled",
        "the audit row must report the job's actual fate, not the run's verdict"
    );

    audit.abort();
}
