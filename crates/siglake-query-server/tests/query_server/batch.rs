//! Tiered execution: submit a batch job, poll status, fetch result,
//! exercise cancellation.

use crate::support;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;

use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, JobStore};
use siglake_storage::iceberg::IcebergContext;

macro_rules! require_loopback {
    () => {
        if !support::loopback_available() {
            return;
        }
    };
}

struct Server {
    base: String,
    _tmp: tempfile::TempDir,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn spawn(n: usize) -> Server {
    spawn_with_jobs(n, JobStore::new(2, Duration::from_secs(5))).await
}

/// Same server, over a job store the caller built — so a test can occupy the
/// batch runtime before any job is submitted to it.
async fn spawn_with_jobs(n: usize, jobs: JobStore) -> Server {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    if n > 0 {
        let events: Vec<Event> = (0..n)
            .map(|i| Event {
                timestamp: Utc::now(),
                host: format!("host-{}", i % 4),
                source: "smoke".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: format!("event {i}"),
                attributes: None,
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    let state = AppState::new(Arc::new(ice), AuthConfig::open()).with_jobs(jobs);
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        base: format!("http://{addr}"),
        _tmp: tmp,
        handle,
    }
}

/// Poll `/api/v1/jobs/<id>` until it reaches a terminal state or
/// `timeout` elapses.
async fn await_terminal(
    client: &reqwest::Client,
    base: &str,
    job_id: &str,
    timeout: Duration,
) -> Value {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let resp = client
            .get(format!("{base}/api/v1/jobs/{job_id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        let status = body["status"].as_str().unwrap();
        if matches!(status, "succeeded" | "failed" | "cancelled" | "timeout") {
            return body;
        }
        if std::time::Instant::now() > deadline {
            panic!("job didn't finish in time: {body}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn batch_submit_returns_202_with_job_id() {
    require_loopback!();
    let srv = spawn(5).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":    "SELECT count(*) AS n FROM events",
            "priority": "batch"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: Value = resp.json().await.unwrap();
    assert!(body["job_id"].is_string());
    assert_eq!(body["priority"], "batch");
    let job_id = body["job_id"].as_str().unwrap();
    assert_eq!(body["status_url"], format!("/api/v1/jobs/{job_id}"));
    assert_eq!(body["result_url"], format!("/api/v1/jobs/{job_id}/result"));
}

#[tokio::test]
async fn batch_job_completes_and_returns_result() {
    require_loopback!();
    let srv = spawn(10).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":    "SELECT count(*) AS n FROM events",
            "priority": "batch"
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let job_id = body["job_id"].as_str().unwrap().to_string();

    let final_status = await_terminal(&client, &srv.base, &job_id, Duration::from_secs(10)).await;
    assert_eq!(final_status["status"], "succeeded");
    assert_eq!(final_status["priority"], "batch");
    assert!(final_status["cost"].is_object());

    // Fetch the result.
    let resp = client
        .get(format!("{}/api/v1/jobs/{}/result", srv.base, job_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["row_count"], 1);
    assert_eq!(body["rows"].as_array().unwrap()[0]["n"], 10);
}

/// The lifecycle a client actually polls, in order, with the batch runtime's
/// only worker occupied so the "not started yet" half is a decision rather than
/// a race.
///
/// THE DEFECT THIS GUARDS. `set_running` was called from the completed branches
/// of the spawned future, so `started_at` recorded the END of execution and a
/// job that failed after planning never got a `started_at` or a `cost` at all —
/// while `GET /api/v1/jobs/{id}` documents `cost` as populated "once planning
/// has run". The refusal here is the pre-flight bytes ceiling, which is the
/// estimate being read: a job refused by a number the row does not carry is
/// unanswerable from the API.
#[tokio::test]
async fn a_queued_batch_job_has_not_started_and_a_refused_one_carries_its_estimate() {
    require_loopback!();
    let jobs = JobStore::new(1, Duration::from_secs(5));
    // Occupy the only batch worker, so the submitted job cannot be polled
    // until this test lets go of the thread.
    let (occupied_tx, occupied_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    jobs.batch_runtime().spawn(async move {
        let _ = occupied_tx.send(());
        let _ = release_rx.recv();
    });
    occupied_rx.await.unwrap();

    let srv = spawn_with_jobs(5, jobs).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":    "SELECT count(*) AS n FROM events",
            "priority": "batch",
            // Refused by the pre-flight ceiling, i.e. after planning: the
            // estimate exists and the run ends without one being computed
            // for a result.
            "limits":   { "max_bytes_scanned": 1 }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: Value = resp.json().await.unwrap();
    let job_id = body["job_id"].as_str().unwrap().to_string();

    let queued: Value = client
        .get(format!("{}/api/v1/jobs/{job_id}", srv.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(queued["status"], "pending", "queued job: {queued}");
    assert!(
        queued["started_at"].is_null(),
        "a job waiting for a worker has not started: {queued}"
    );
    assert!(queued["cost"].is_null(), "nothing is planned yet: {queued}");

    release_tx.send(()).unwrap();
    let done = await_terminal(&client, &srv.base, &job_id, Duration::from_secs(10)).await;
    assert_eq!(done["status"], "failed", "terminal job: {done}");
    assert!(
        done["error"]
            .as_str()
            .unwrap()
            .contains("exceeds batch limit"),
        "refused for the wrong reason: {done}"
    );
    assert!(
        done["cost"]["estimated_bytes_scanned"].is_number(),
        "the estimate that refused the job must be readable: {done}"
    );
    let submitted_at = timestamp(&done, "submitted_at");
    let started_at = timestamp(&done, "started_at");
    let ended_at = timestamp(&done, "ended_at");
    assert!(
        submitted_at <= started_at && started_at <= ended_at,
        "lifecycle timestamps out of order: {done}"
    );
}

/// RFC 3339 timestamp from a job row, or a failure naming the field.
fn timestamp(job: &Value, field: &str) -> chrono::DateTime<Utc> {
    let raw = job[field]
        .as_str()
        .unwrap_or_else(|| panic!("job row has no {field}: {job}"));
    chrono::DateTime::parse_from_rfc3339(raw)
        .unwrap_or_else(|e| panic!("{field} is not a timestamp ({e}): {raw}"))
        .with_timezone(&Utc)
}

#[tokio::test]
async fn batch_job_404_for_unknown_id() {
    require_loopback!();
    let srv = spawn(0).await;
    let client = reqwest::Client::new();
    let job_id = "00000000-0000-0000-0000-000000000000";
    let resp = client
        .get(format!("{}/api/v1/jobs/{job_id}", srv.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    let resp = client
        .delete(format!("{}/api/v1/jobs/{job_id}", srv.base))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn batch_job_cancel_after_complete_returns_conflict() {
    require_loopback!();
    let srv = spawn(5).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":    "SELECT count(*) FROM events",
            "priority": "batch"
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let job_id = body["job_id"].as_str().unwrap().to_string();
    let _ = await_terminal(&client, &srv.base, &job_id, Duration::from_secs(10)).await;

    // Cancel after terminal: 409.
    let resp = client
        .delete(format!("{}/api/v1/jobs/{}", srv.base, job_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["error"],
        format!("job {job_id} already reached a terminal state")
    );
}

#[tokio::test]
async fn batch_result_404_while_pending_or_running() {
    require_loopback!();
    let srv = spawn(5).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":    "SELECT count(*) FROM events",
            "priority": "batch"
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let job_id = body["job_id"].as_str().unwrap().to_string();

    // Result fetched immediately: either 404 (still pending/running)
    // or 200 (already finished — possible if the batch runtime is
    // ahead of our request). Both outcomes are valid for v0.
    let resp = client
        .get(format!("{}/api/v1/jobs/{}/result", srv.base, job_id))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == 404 || resp.status() == 200,
        "expected 404 or 200, got {}",
        resp.status()
    );

    // Always eventually succeed.
    let final_status = await_terminal(&client, &srv.base, &job_id, Duration::from_secs(10)).await;
    assert_eq!(final_status["status"], "succeeded");
}

#[tokio::test]
async fn interactive_path_unaffected_by_batch_submission() {
    require_loopback!();
    let srv = spawn(5).await;
    let client = reqwest::Client::new();
    // Submit a batch first.
    let _ = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({
            "query":    "SELECT count(*) FROM events",
            "priority": "batch"
        }))
        .send()
        .await
        .unwrap();
    // Interactive (default) still works synchronously.
    let resp = client
        .post(format!("{}/api/v1/sql", srv.base))
        .json(&serde_json::json!({"query": "SELECT count(*) AS n FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["rows"].as_array().unwrap()[0]["n"], 5);
}
