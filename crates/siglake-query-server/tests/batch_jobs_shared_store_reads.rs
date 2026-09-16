//! A batch job submitted to one replica has to be readable on another.
//!
//! THE DEFECT THIS GUARDS. The chart installs `query.replicas: 2` behind one
//! Service with no session affinity, and `query.jobs.persistent` shipped
//! `false` — each pod holding its own in-memory job table. A client that reads
//! `GET /api/v1/jobs/{id}` (or `…/result`) with the id a `202` just handed it
//! gets whichever replica the Service picks, so roughly half of those reads
//! answered `404 job not found` with nothing restarted and nothing wrong: the
//! job was running, and its result was returned to nobody. Since 2026-09-11 the
//! store defaults to the catalog Postgres, shared by every replica.
//!
//! Two `AppState`s over one job table is the fixed shape; two `AppState`s over
//! two tables is the shipped one, and the second test pins it so "the default
//! stopped being rendered" cannot pass as a green suite.
//!
//! Its own binary on purpose: `tests/batch_cancel_across_replicas.rs` reads the
//! process-global metrics recorder, and admission traffic from this file would
//! be indistinguishable from its own.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use chrono::Utc;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, JobStore, ServerLimits};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

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

/// A warehouse holding four events, so `count(*)` has an answer to carry.
async fn warehouse() -> (tempfile::TempDir, Arc<IcebergContext>) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let events: Vec<Event> = (0..4)
        .map(|i| Event {
            timestamp: Utc::now(),
            host: format!("host-{i}"),
            source: "shared-job-store".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: format!("event {i}"),
            attributes: None,
        })
        .collect();
    ice.append_events(&events).await.unwrap();
    (tmp, Arc::new(ice))
}

fn state(ice: Arc<IcebergContext>, jobs: JobStore) -> AppState {
    AppState::new(ice, AuthConfig::open())
        .with_limits(ServerLimits::default())
        .with_jobs(jobs)
}

async fn submit(app: &Router) -> String {
    let submitted = send(
        app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "priority": "batch"
        })),
    )
    .await;
    assert_eq!(submitted.status(), StatusCode::ACCEPTED);
    json_body(submitted).await["job_id"]
        .as_str()
        .expect("submit response carries job_id")
        .to_string()
}

/// Poll `app` until the job reaches a terminal state, and return that body.
async fn settled(app: &Router, job_id: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let response = send(app, Method::GET, &format!("/api/v1/jobs/{job_id}"), None).await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "the replica polling the job lost sight of it"
            );
            let body = json_body(response).await;
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
    .expect("batch job never finished")
}

#[tokio::test]
async fn a_peer_replica_answers_status_and_result_from_the_shared_store() {
    let (_tmp, ice) = warehouse().await;

    let state_a = state(ice.clone(), JobStore::new(1, Duration::from_secs(600)));
    let app_a = router(state_a.clone());
    // B executes nothing here: its own abort map, its own admission budget, one
    // job table. That is the pod the Service routes the client's poll to.
    let b_jobs = JobStore::new_peer_of(&state_a.jobs, 1, Duration::from_secs(600))
        .expect("in-memory peer store");
    let app_b = router(state(ice, b_jobs));

    let job_id = submit(&app_a).await;

    // The read the shipped chart used to lose: B never saw the submission.
    let status = settled(&app_b, &job_id).await;
    assert_eq!(
        status["status"], "succeeded",
        "the peer must report the executor's outcome: {status}"
    );

    let result = send(
        &app_b,
        Method::GET,
        &format!("/api/v1/jobs/{job_id}/result"),
        None,
    )
    .await;
    assert_eq!(
        result.status(),
        StatusCode::OK,
        "the result belongs to the job, not to the pod that ran it"
    );
    let body = json_body(result).await;
    assert_eq!(
        body["rows"][0]["n"], 4,
        "the peer served a different answer than the executor computed: {body}"
    );

    // And the executor still answers for it, so the shared store is not a
    // hand-off: both replicas are correct at once.
    let on_a = send(&app_a, Method::GET, &format!("/api/v1/jobs/{job_id}"), None).await;
    assert_eq!(on_a.status(), StatusCode::OK);
    assert_eq!(json_body(on_a).await["status"], "succeeded");
}

/// The state `query.jobs.persistent: false` still installs, and the reason the
/// default moved: nothing about the job is wrong, and the client is told it
/// does not exist.
#[tokio::test]
async fn separate_stores_lose_a_peers_job_with_nothing_restarted() {
    let (_tmp, ice) = warehouse().await;

    let app_a = router(state(
        ice.clone(),
        JobStore::new(1, Duration::from_secs(600)),
    ));
    let app_b = router(state(ice, JobStore::new(1, Duration::from_secs(600))));

    let job_id = submit(&app_a).await;
    let finished = settled(&app_a, &job_id).await;
    assert_eq!(finished["status"], "succeeded", "{finished}");

    for path in [
        format!("/api/v1/jobs/{job_id}"),
        format!("/api/v1/jobs/{job_id}/result"),
    ] {
        let response = send(&app_b, Method::GET, &path, None).await;
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "a per-pod store can only answer for its own pod ({path})"
        );
    }
}
