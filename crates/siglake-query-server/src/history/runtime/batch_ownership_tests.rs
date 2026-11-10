//! Actual router/driver ownership, independent of volatile sink delivery.
//! The consumer is stopped, not the observer: only production may select and
//! enqueue terminal evidence. No test-owned strong observation masks a lost owner.
use super::*;
use crate::{
    admission::AdmissionController,
    jobs::{JobId, JobStore},
    AppState, AuthConfig, OidcVerifier, TenantRegistry,
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use futures::FutureExt;
use serde_json::{json, Value};
use siglake_storage::iceberg::IcebergContext;
use sqlx::Connection;
use tower::ServiceExt;

use crate::signing_fixture as signing;

// One existing batch runtime thread is held by fixture backpressure. Drop also
// releases it on panic, without spawning a second runtime or a detached owner.
struct QueueGate(Option<std::sync::mpsc::Sender<()>>);
impl QueueGate {
    async fn hold(jobs: &JobStore) -> Self {
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        jobs.batch_runtime().spawn(async move {
            let _ = ready.send(());
            let _ = held.recv();
        });
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        Self(Some(release))
    }
}
impl Drop for QueueGate {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

#[tokio::test]
#[ignore = "owned PG fixture; history ownership oracle runner"]
async fn postgres_actual_router_dropped_batch_selects_original_terminal_without_sink() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    let task = service.task.take().unwrap();
    task.abort();
    let _ = task.await;
    assert!(
        !service.inner.closed(),
        "stopping the consumer must not close observation"
    );
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    ice.for_namespace("tenant_acme")
        .await
        .unwrap()
        .append_events(&[siglake_core::Event::now("OWNED_BATCH_FIXTURE")])
        .await
        .unwrap();
    let (issuer, issuer_task) = signing::spawn_fake_idp().await;
    let scenario = Box::pin(async {
        let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "tenant".into())
            .await
            .unwrap();
        // Actual production driver with the in-memory backend: this also runs
        // in the original v1 history fixture, without adding roles/migrations.
        // The signed integration separately covers durable PG job transitions.
        let jobs = JobStore::new(1, Duration::from_secs(600));
        let mut state = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
            .with_tenants(TenantRegistry::new(ice))
            .with_jobs(jobs)
            .with_history(service.observer());
        state.admission = Arc::new(AdmissionController::new(4, 1, Duration::from_secs(2)));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = json!({"iss":issuer,"sub":"original-subject","exp":now+600,"iat":now,"token_use":"access","client_id":"original-client","tenant":"acme"});
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some(signing::TEST_KID.into());
        let token = jsonwebtoken::encode(
            &header,
            &claims,
            &jsonwebtoken::EncodingKey::from_rsa_pem(signing::TEST_PRIVATE_KEY_PEM).unwrap(),
        )
        .unwrap();
        let run = Box::pin(async {
            for cancel in [false, true] {
                let gate = QueueGate::hold(&state.jobs).await;
                let shares = state.admission.acquire(3).await.unwrap();
                let request = Request::builder()
                    .method("POST")
                    .uri("/api/v1/sql/local")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"query":"SELECT raw FROM events","priority":"batch"}).to_string(),
                    ))
                    .unwrap();
                let mut request = Box::pin(crate::router(state.clone()).oneshot(request));
                let response = (&mut request).await.unwrap();
                assert_eq!(response.status(), StatusCode::ACCEPTED);
                let execution =
                    Uuid::parse_str(response.headers()["x-siglake-history-id"].to_str().unwrap())
                        .unwrap();
                let body: Value = serde_json::from_slice(
                    &axum::body::to_bytes(response.into_body(), 4096)
                        .await
                        .unwrap(),
                )
                .unwrap();
                let job = JobId::parse(body["job_id"].as_str().unwrap()).unwrap();
                let initial = json_row(&f, execution).await;
                assert_eq!(initial["observation_state"], "pending");
                assert!(initial["execution_admitted"].is_null() && initial["job_id"].is_null());
                assert_eq!(initial["client_id"], "original-client");
                assert_eq!(initial["owner_uuid"], service.owner_uuid().to_string());
                // Hold the original row across association's bounded I/O wait.
                // This also prevents a resumed/reconstructed observer from
                // masking loss of the actual retained owner: no job link can
                // commit before this test has checked the true terminal.
                let mut row_lock = f.admin.acquire().await.unwrap();
                row_lock.close_on_drop();
                let mut transaction = row_lock.begin().await.unwrap();
                sqlx::query("SELECT execution_id FROM query_history.executions WHERE execution_id=$1 FOR UPDATE")
                    .bind(execution).fetch_one(&mut *transaction).await.unwrap();
                // The202 body/response is already consumed/dropped. Drop its
                // real HTTP future before any batch polling; neither may own
                // the original observation through association/execution.
                drop(request);
                let mut probe = Box::pin(state.admission.acquire(1));
                assert!(futures::poll!(&mut probe).is_pending());
                drop(probe);
                if cancel {
                    let response = crate::router(state.clone())
                        .oneshot(
                            Request::builder()
                                .method("DELETE")
                                .uri(format!("/api/v1/jobs/{job}"))
                                .header("authorization", format!("Bearer {token}"))
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(response.status(), StatusCode::ACCEPTED);
                }
                // DELETE, when selected, has committed BEFORE dequeue. Its
                // not-admitted verdict cannot depend on winning a100ms race.
                drop(gate);
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND query LIKE 'UPDATE query_history.executions SET job_id%' AND wait_event_type='Lock')")
                            .fetch_one(&f.admin).await.unwrap();
                        if waiting { break; }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("real association did not reach the original row gate");
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let status = state.jobs.info(job).await.unwrap().unwrap().status;
                        if status.is_terminal() {
                            assert_eq!(
                                status,
                                if cancel {
                                    crate::jobs::JobStatus::Cancelled
                                } else {
                                    crate::jobs::JobStatus::Succeeded
                                }
                            );
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("actual batch did not complete");
                let free = tokio::time::timeout(Duration::from_secs(2), state.admission.acquire(1))
                    .await
                    .unwrap()
                    .unwrap();
                drop(free);
                eprintln!("actual batch checked: cancel={cancel}; expected_authoritative_terminal=true; admission_released=true");
                // Reservation release is after driver observation. With no
                // consumer and no concurrent inspection, this must be the true
                // terminal snapshot, not a later test-generated finish/replay.
                let snapshot = {
                    let local = service.inner.local.lock().unwrap();
                    local
                        .queue
                        .entries
                        .iter()
                        .find_map(|entry| match &entry.payload {
                            Payload::Observation(o)
                                if o.event.receipt.execution_id == execution =>
                            {
                                Some((o.event.clone(), o.terminal.clone(), o.abandoned))
                            }
                            _ => None,
                        })
                        .expect("original observation snapshot missing")
                };
                assert!(!snapshot.2, "a dropped owner is not a known terminal");
                let terminal = snapshot
                    .1
                    .expect("real driver did not select terminal evidence");
                assert_eq!(snapshot.0.owner_uuid, service.owner_uuid());
                assert_eq!(snapshot.0.receipt.execution_id, execution);
                assert_eq!(
                    snapshot.0.principal.client_id.as_deref(),
                    Some("original-client")
                );
                let received: chrono::DateTime<Utc> =
                    serde_json::from_value(initial["received_at"].clone()).unwrap();
                let expiry: chrono::DateTime<Utc> =
                    serde_json::from_value(initial["expires_at"].clone()).unwrap();
                assert_eq!(snapshot.0.receipt.received_at, received);
                assert_eq!(snapshot.0.receipt.expires_at, expiry);
                let terminal = serde_json::to_value(terminal).unwrap();
                assert_eq!(
                    terminal["outcome"],
                    if cancel { "cancelled" } else { "success" }
                );
                assert_eq!(
                    terminal["admission_evidence"],
                    if cancel { "not_admitted" } else { "admitted" }
                );
                assert_eq!(
                    terminal["error_code"],
                    if cancel {
                        json!("cancelled")
                    } else {
                        Value::Null
                    }
                );
                assert_eq!(terminal["job_id"], job.to_string());
                assert_eq!(terminal["cache_state"], "bypass");
                assert_eq!(
                    terminal["truncated"],
                    if cancel { Value::Null } else { json!(false) }
                );
                assert!(terminal["duration_ms"]
                    .as_i64()
                    .is_some_and(|elapsed| elapsed >= 0));
                assert_eq!(
                    terminal["returned_rows"],
                    if cancel { Value::Null } else { json!(1) }
                );
                assert!(
                    terminal["measured_rows_scanned"].is_null()
                        && terminal["measured_bytes_scanned"].is_null()
                );
                // This test proves ownership WITHOUT terminal persistence. The
                // live signed integration separately validates observed delivery.
                assert_eq!(
                    json_row(&f, execution).await["observation_state"],
                    "pending"
                );
                transaction.rollback().await.unwrap();
                drop(shares);
            }
            assert_eq!(service.health().loss_signals(LossReason::QueueContended), 0);
            assert_eq!(service.health().loss_signals(LossReason::QueueFull), 0);
        });
        let result = std::panic::AssertUnwindSafe(run).catch_unwind().await;
        state.jobs.shutdown().await.unwrap();
        result
    });
    let result = tokio::time::timeout(Duration::from_secs(30), scenario).await;
    issuer_task.abort();
    match result.unwrap() {
        Ok(()) => {}
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
