//! Deterministic probes for the held batch-history observation failure.
//! These distinguish possible mechanisms; they do not identify the untraced
//! historical failure or promise durable best-effort terminal delivery.
use super::*;
use crate::history::batch::BatchObservation;
use crate::history::postgres_tests::{fixture, json_row};
use crate::jobs::{JobStatus, JobStore};
use sqlx::Connection;

async fn manual_batch_service(f: &Fixture) -> (HistoryService, Arc<ObservationHandle>) {
    let mut service = ready_service(f).await;
    // Drive the existing retry seam explicitly, without a competing consumer.
    let task = service.task.take().unwrap();
    task.abort();
    let _ = task.await;
    let mut metadata = request();
    metadata.priority = Priority::Batch;
    let handle = Arc::new(
        service
            .begin(principal(), metadata, RedactedSql::parse("SELECT 1"))
            .await,
    );
    assert_eq!(handle.initial_delivery(), Delivery::Persisted);
    (service, handle)
}

#[tokio::test]
#[ignore = "owned PG fixture; terminal diagnosis runner"]
async fn postgres_batch_terminal_diagnosis_legacy_timeout_retains_server_row_lock() {
    let f = fixture().await;
    let (service, handle) = manual_batch_service(&f).await;
    let execution = handle.receipt().unwrap().execution_id();
    let initial = json_row(&f, execution).await;
    let timeouts = service.health().loss_signals(LossReason::Timeout);
    let job = Uuid::new_v4();
    let mut lock = f.admin.acquire().await.unwrap();
    lock.close_on_drop();
    let mut transaction = lock.begin().await.unwrap();
    sqlx::query(
        "SELECT execution_id FROM query_history.executions WHERE execution_id=$1 FOR UPDATE",
    )
    .bind(execution)
    .fetch_one(&mut *transaction)
    .await
    .unwrap();
    let observer = service.observer();
    let association = tokio::spawn(async move { observer.associate_job(execution, job).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND query LIKE 'UPDATE query_history.executions SET job_id%' AND wait_event_type='Lock')")
                .fetch_one(&f.admin).await.unwrap();
            if waiting { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("association did not reach owned row gate");
    association.await.unwrap();
    assert_eq!(
        service.health().loss_signals(LossReason::Timeout),
        timeouts + 1
    );
    let still_waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND query LIKE 'UPDATE query_history.executions SET job_id%' AND wait_event_type='Lock')")
        .fetch_one(&f.admin).await.unwrap();
    assert!(
        still_waiting,
        "a bounded client wait did not cancel the legacy server statement"
    );

    let mut terminal = terminal(Outcome::Success);
    terminal.measurements.job_id = Some(job);
    assert_eq!(handle.finish_queued(terminal).unwrap(), Delivery::Queued);
    assert!(!retry(&service.inner).await);
    assert_eq!(
        service.health().loss_signals(LossReason::Timeout),
        timeouts + 2
    );
    assert_eq!(service.health().queued_entries, 1);
    assert_eq!(
        json_row(&f, execution).await["observation_state"],
        "pending"
    );
    transaction.rollback().await.unwrap();
    // The same bounded retry can now settle; no wait/sleep budget is enlarged.
    assert!(retry(&service.inner).await);
    let row = json_row(&f, execution).await;
    assert_eq!(row["observation_state"], "terminal");
    assert_eq!(row["job_id"], job.to_string());
    assert_eq!(row["received_at"], initial["received_at"]);
    assert_eq!(row["expires_at"], initial["expires_at"]);
    assert_eq!(service.health().queued_entries, 0);
}

#[tokio::test]
#[ignore = "owned PG fixture; terminal diagnosis runner"]
async fn postgres_batch_terminal_diagnosis_queue_contention_loses_selected_terminal() {
    let f = fixture().await;
    let (service, handle) = manual_batch_service(&f).await;
    let execution = handle.receipt().unwrap().execution_id();
    let initial = json_row(&f, execution).await;
    let jobs = JobStore::new(1, Duration::from_secs(600));
    let job = jobs
        .submit_observed(
            "SELECT 1".into(),
            crate::limits::Priority::Batch,
            Some("runtime-fixture".into()),
            Some(execution),
        )
        .await
        .unwrap();
    service.observer().associate_job(execution, job.0).await;
    let batch = Arc::new(BatchObservation::new(
        handle.clone(),
        None,
        job.0,
        Some(false),
    ));
    batch.admit();
    assert!(retry(&service.inner).await);
    // This is a synthetic authoritative job transition, not native-engine proof.
    jobs.set_running(job).await.unwrap();
    jobs.finish_succeeded(
        job,
        crate::format::RecordsResponse {
            columns: vec![],
            row_count: 0,
            rows: serde_json::json!([]),
            truncated: false,
            max_rows: None,
            cost: None,
            stats: None,
            approximation: None,
        },
    )
    .await
    .unwrap();
    let status = jobs.info(job).await.unwrap().unwrap().status;
    assert_eq!(status, JobStatus::Succeeded);
    jobs.track_history(job, batch.clone());
    {
        // An actual retained owner selects the genuine store verdict while the
        // collector's existing nonblocking queue lock is unavailable.
        let _gate = service.inner.local.lock().unwrap();
        jobs.history_observation(job).unwrap().finish(status, false);
    }
    assert_eq!(service.health().loss_signals(LossReason::QueueContended), 1);
    assert!(handle
        .observation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .terminal
        .is_some());
    batch.finish(status, false); // Already selected, not a new persistence retry.
    drop(batch);
    drop(handle);
    assert_eq!(service.health().queued_entries, 0);
    assert!(!retry(&service.inner).await);
    let row = json_row(&f, execution).await;
    assert_eq!(row["observation_state"], "pending");
    assert_eq!(row["execution_admitted"], true);
    assert_eq!(row["job_id"], job.to_string());
    assert_eq!(row["received_at"], initial["received_at"]);
    assert_eq!(row["expires_at"], initial["expires_at"]);
    jobs.shutdown().await.unwrap();
}
