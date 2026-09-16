//! A cancellation persisted by one replica has to stop the work on another.
//!
//! THE DEFECT THIS GUARDS. `DELETE /api/v1/jobs/<id>` answered `202` and
//! documented that it released the job's admission reservation and cancelled
//! its storage scans. Both of those are owned by the spawned batch future, and
//! the only thing that drops that future is an `AbortHandle` in the executing
//! process's own map. With `query.jobs.persistent` the store is shared by every
//! query replica, so the replica serving the `DELETE` is usually *not* the
//! executor: its abort map held no handle for the id, it wrote
//! `status = 'cancelled'`, and returned `202` while the other pod kept scanning
//! and kept a quarter of its admission budget reserved indefinitely.
//!
//! Two `AppState`s over one job table, each with its own abort map, is that
//! shape. The single batch worker is deliberately occupied so the moment the
//! abort takes effect is *observable* rather than raced: while it is blocked,
//! nothing about the submitted job can change by itself.
//!
//! Its own binary on purpose: the debugging recorder is process-global. All
//! configuration is passed directly; this test never mutates the environment.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_query_server::{router, AppState, AuthConfig, JobStore, ServerLimits};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

/// `Snapshotter::snapshot` **drains** what it reports — every counter and
/// gauge is `swap(0)`-ed. So a test that polls a gauge in a loop destroys
/// every counter increment it has not already read, and a gauge only reads
/// back its real value if something has re-set it since the last snapshot.
/// This accumulates counters across every snapshot taken and hands back the
/// gauge from the same read, so the two can never describe different moments.
#[derive(Default)]
struct Metrics {
    counters: HashMap<(String, Vec<(String, String)>), u64>,
}

impl Metrics {
    fn read(&mut self, snapshotter: &Snapshotter, gauge: &str) -> Option<f64> {
        let mut value = None;
        for (key, _, _, observed) in snapshotter.snapshot().into_vec() {
            match observed {
                DebugValue::Counter(n) => {
                    let labels = key
                        .key()
                        .labels()
                        .map(|l| (l.key().to_string(), l.value().to_string()))
                        .collect();
                    *self
                        .counters
                        .entry((key.key().name().to_string(), labels))
                        .or_default() += n;
                }
                DebugValue::Gauge(g) if key.key().name() == gauge => value = Some(g.into_inner()),
                _ => {}
            }
        }
        value
    }

    /// Every series recorded under `name`, summed.
    fn total(&self, name: &str) -> u64 {
        self.counters
            .iter()
            .filter(|((n, _), _)| n == name)
            .map(|(_, v)| *v)
            .sum()
    }

    /// The series under `name` carrying `label`, summed.
    fn with_label(&self, name: &str, label: (&str, &str)) -> u64 {
        self.counters
            .iter()
            .filter(|((n, labels), _)| {
                n == name
                    && labels
                        .iter()
                        .any(|(k, v)| k == label.0 && v.as_str() == label.1)
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

const RESERVED: &str = "siglake_query_admission_reserved_bytes";

#[tokio::test]
async fn a_delete_on_one_replica_releases_the_executing_replicas_resources() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");
    let mut metrics = Metrics::default();

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

    // --- replica A: the executor. Occupy its only batch worker so the
    // submitted job stays queued and holds its reservation until something
    // explicitly drops the future.
    let a_jobs = JobStore::new(1, Duration::from_secs(600));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    a_jobs.batch_runtime().spawn(async move {
        let _ = started_tx.send(());
        let _ = release_rx.recv();
    });
    started_rx.await.unwrap();

    let state_a = AppState::new(ice.clone(), AuthConfig::open())
        .with_limits(limits.clone())
        .with_jobs(a_jobs);
    let app_a = router(state_a.clone());
    let batch_reservation = state_a.admission.batch_reservation_bytes();
    assert_eq!(batch_reservation, BUDGET / 4);

    // An explicit baseline reservation, as in `batch_admission.rs`: it makes
    // the released value a distinct NON-zero number, which a drained gauge
    // read can never be mistaken for.
    let baseline = state_a
        .admission
        .acquire(BUDGET - batch_reservation)
        .await
        .unwrap();

    // --- replica B: same job table, its own abort map, its own admission
    // budget. B never runs a query here, so it never touches the reserved-bytes
    // gauge -- every value below is A's.
    let b_jobs = JobStore::new_peer_of(&state_a.jobs, 1, Duration::from_secs(600))
        .expect("in-memory peer store");
    let state_b = AppState::new(ice.clone(), AuthConfig::open())
        .with_limits(limits)
        .with_jobs(b_jobs);
    let app_b = router(state_b.clone());

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

    // B can see A's job -- one table -- and cancels it. Nothing snapshots
    // before this point, so the gauge below still carries the value submission
    // set rather than a drained zero.
    let seen_by_b = send(&app_b, Method::GET, &format!("/api/v1/jobs/{job_id}"), None).await;
    assert_eq!(seen_by_b.status(), StatusCode::OK);
    let cancelled = send(
        &app_b,
        Method::DELETE,
        &format!("/api/v1/jobs/{job_id}"),
        None,
    )
    .await;
    assert_eq!(cancelled.status(), StatusCode::ACCEPTED);
    let seen_by_a = send(&app_a, Method::GET, &format!("/api/v1/jobs/{job_id}"), None).await;
    assert_eq!(
        json_body(seen_by_a).await["status"],
        "cancelled",
        "the cancellation must be visible on the executing replica"
    );

    // The whole defect in one assertion: the row is terminal and the `202` is
    // sent, and A is still holding a quarter of its budget for a job nobody
    // wants. B has no abort handle for A's future, so the status write cannot
    // have released anything.
    assert_eq!(
        metrics.read(&snapshotter, RESERVED),
        Some(BUDGET as f64),
        "a peer's status write must not be mistaken for the work stopping"
    );

    // A observes it. This is the step `--jobs-cancel-poll-secs` runs on a
    // timer in the Postgres store; driving it by hand makes the bound an
    // assertion rather than a sleep.
    let aborted = state_a
        .jobs
        .propagate_cancellations()
        .await
        .expect("A's cancellation sweep");
    assert_eq!(
        aborted.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        vec![job_id.clone()],
        "A must abort the job its peer cancelled"
    );

    // The abort needs one poll to take effect, which needs the worker back.
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if metrics.read(&snapshotter, RESERVED) == Some((BUDGET - batch_reservation) as f64) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the remotely cancelled job never released its admission reservation");

    // Terminal state is unchanged by the abort, on both replicas, and the
    // discarded result is not readable.
    for (which, app) in [("A", &app_a), ("B", &app_b)] {
        let status = send(app, Method::GET, &format!("/api/v1/jobs/{job_id}"), None).await;
        assert_eq!(status.status(), StatusCode::OK, "status on {which}");
        assert_eq!(
            json_body(status).await["status"],
            "cancelled",
            "the row must stay cancelled on {which}"
        );
        let result = send(
            app,
            Method::GET,
            &format!("/api/v1/jobs/{job_id}/result"),
            None,
        )
        .await;
        assert_eq!(
            result.status(),
            StatusCode::CONFLICT,
            "a cancelled job must not serve rows on {which}"
        );
    }

    // A second DELETE is a 409 on either replica: the row is terminal.
    for (which, app) in [("A", &app_a), ("B", &app_b)] {
        let again = send(app, Method::DELETE, &format!("/api/v1/jobs/{job_id}"), None).await;
        assert_eq!(
            again.status(),
            StatusCode::CONFLICT,
            "re-cancelling a terminal job on {which}"
        );
    }

    metrics.read(&snapshotter, RESERVED);
    assert_eq!(
        metrics.total("siglake_query_jobs_cancel_propagated_total"),
        1,
        "the cross-replica abort must be countable"
    );
    // The future never reached its completion arm, so it claimed no outcome at
    // all -- least of all `succeeded`.
    assert_eq!(
        metrics.with_label("siglake_query_jobs_total", ("outcome", "succeeded")),
        0,
        "an aborted job must not report a successful batch outcome"
    );
    assert_eq!(
        metrics.total("siglake_query_jobs_total"),
        0,
        "an aborted job must not report a batch outcome"
    );

    drop(baseline);
    assert_eq!(metrics.read(&snapshotter, RESERVED), Some(0.0));
}
