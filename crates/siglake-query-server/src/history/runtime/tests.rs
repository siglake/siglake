use super::*;
use crate::history::postgres_tests::{expire, fixture, json_row, options, terminal, Fixture};
use sqlx::Row;
use std::future::Future;

#[path = "terminal_diagnosis_tests.rs"]
mod terminal_diagnosis_tests;

fn principal() -> Principal {
    Principal::from_caller(
        "runtime-fixture",
        &CallerIdentity {
            subject: "SECRET_SUBJECT".into(),
            email: Some("SECRET_EMAIL".into()),
            ..Default::default()
        },
    )
    .unwrap()
}
fn request() -> RequestMetadata {
    RequestMetadata {
        operation: Operation::Sql,
        format: OutputFormat::Records,
        priority: Priority::Interactive,
        dry_run: false,
    }
}
fn point() -> Payload {
    Payload::LossPoint {
        id: Uuid::new_v4(),
        tenant: "runtime-fixture".into(),
        queue_full: false,
    }
}
async fn begin(service: &HistoryService) -> ObservationHandle {
    service
        .begin(
            principal(),
            request(),
            RedactedSql::parse("SELECT 'SECRET_SQL' /*SECRET_COMMENT*/"),
        )
        .await
}
async fn ready_service(f: &Fixture) -> HistoryService {
    let service = HistoryService::new(
        HistoryWriter::connect(options(&f.socket, "qh_writer"))
            .await
            .unwrap(),
    );
    wait_ready(&service).await;
    service
}
async fn wait_ready(service: &HistoryService) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !service.health().history_ready {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("history failed to become ready");
}
async fn wait_row(f: &Fixture, id: Uuid, state: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let row: Option<serde_json::Value> = sqlx::query_scalar(
                "SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1",
            )
            .bind(id)
            .fetch_optional(&f.admin)
            .await
            .unwrap();
            if let Some(row) = row {
                if row["observation_state"] == state {
                    return row;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("history state did not converge")
}

#[tokio::test(start_paused = true)]
async fn default_budget_is_100ms_not_an_unbounded_future() {
    assert_eq!(WRITE_BUDGET, Duration::from_millis(100));
    let start = Instant::now();
    let result = bounded(async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(())
    })
    .await;
    assert!(matches!(result, Err(AttemptError::Timeout)));
    assert_eq!(start.elapsed(), WRITE_BUDGET);
    assert!(bounded(async {
        tokio::time::sleep(Duration::from_millis(99)).await;
        Ok(())
    })
    .await
    .is_ok());
}
#[tokio::test(start_paused = true)]
async fn queue_entry_limit_includes_inflight_and_age_is_not_refreshed() {
    let mut q = RetryQueue::default();
    for _ in 0..QUEUE_ENTRIES {
        assert!(q.push(Entry::new(point())));
    }
    assert!(!q.push(Entry::new(point())));
    let mut inflight = q.take().unwrap();
    inflight.store_failure_observed = true;
    assert_eq!(q.count, QUEUE_ENTRIES);
    assert!(!q.push(Entry::new(point())));
    let original = inflight.first_attempt;
    q.release(&inflight);
    tokio::time::advance(Duration::from_secs(29)).await;
    assert!(q.push(inflight));
    let id = q.entries.back().unwrap().payload.id();
    assert!(q.push(Entry::new(Payload::LossPoint {
        id,
        tenant: "runtime-fixture".into(),
        queue_full: false
    })));
    let entry = q.entries.back().unwrap();
    assert_eq!(entry.first_attempt, original);
    assert!(entry.store_failure_observed);
    assert!(!entry.expired());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(q.entries.back().unwrap().expired());
    assert_eq!(q.count, QUEUE_ENTRIES);
}
#[test]
fn effective_selection_survives_older_inflight_snapshot_and_bounded_queue_coalescing() {
    let now = Utc::now();
    let event = PendingExecution::new(
        Receipt {
            execution_id: Uuid::new_v4(),
            received_at: now,
            expires_at: now + chrono::Duration::hours(720),
        },
        principal(),
        Uuid::new_v4(),
        request(),
        RedactedSql::parse("SELECT 1"),
    )
    .unwrap();
    let original = Observation {
        event,
        effective: None,
        admitted: true,
        terminal: None,
        abandoned: false,
    };
    let mut selected = original.clone();
    let effective = EffectiveExecution {
        format: OutputFormat::Ndjson,
        dry_run: false,
    };
    selected.effective = Some(effective);
    let mut q = RetryQueue::default();
    assert!(q.push(Entry::new(Payload::Observation(Box::new(selected)))));
    assert!(q.push(Entry::new(Payload::Observation(Box::new(original)))));
    assert_eq!(q.count, 1);
    let entry = q.take().unwrap();
    assert_eq!(q.bytes, entry.payload.bytes());
    let Payload::Observation(snapshot) = &entry.payload else {
        panic!("observation")
    };
    assert_eq!(snapshot.effective, Some(effective));
    q.release(&entry);
    assert_eq!(q.bytes, 0);
}

#[test]
fn queue_payload_byte_limit_and_safe_serialization() {
    let mut q = RetryQueue::default();
    let mut entry = Entry::new(point());
    // Synthetic reservation exercises the independent byte cap without a
    // production API for constructing oversized/unsafe event fields.
    entry.bytes = QUEUE_BYTES;
    assert!(q.push(entry));
    assert!(!q.push(Entry::new(point())));
    assert_eq!(q.bytes, QUEUE_BYTES);
    let p = point();
    assert!(!serde_json::to_string(&p).unwrap().contains("SECRET"));
    assert!(AttemptError::Store(HistoryError::Unavailable).retryable());
    assert!(!AttemptError::Store(HistoryError::Conflict).retryable());
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_terminal_duplicate_concurrent_and_drop() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    let handle = Arc::new(begin(&service).await);
    let receipt = handle.receipt().unwrap();
    assert_eq!(handle.initial_delivery(), Delivery::Persisted);
    handle.admit().unwrap();
    let a = handle.clone();
    let b = handle.clone();
    let (success, conflict) = tokio::join!(
        a.finish(terminal(Outcome::Success)),
        b.finish(terminal(Outcome::Cancelled))
    );
    assert!(success.is_ok());
    assert_eq!(conflict.err(), Some(HistoryError::Conflict));
    assert_eq!(
        handle.finish(terminal(Outcome::Success)).await.unwrap(),
        Delivery::AlreadyFinalized
    );
    drop(a);
    drop(b);
    drop(handle);
    wait_ready(&service).await;
    let row = json_row(&f, receipt.execution_id()).await;
    assert_eq!(row["outcome"], "success");
    assert_eq!(row["duration_ms"], 7);
    assert_eq!(row["finished_at"], serde_json::Value::Null);
    assert_eq!(row["admitted_at"], serde_json::Value::Null);
    assert!(row["finished_observed_at"].is_string());
    assert_eq!(row["redacted_sql"], serde_json::Value::Null);
    assert!(!row.to_string().contains("SECRET"));
    assert_eq!(
        serde_json::from_value::<DateTime<Utc>>(row["expires_at"].clone()).unwrap(),
        receipt.expires_at()
    );
    let abandoned = begin(&service).await;
    let id = abandoned.receipt().unwrap().execution_id();
    drop(abandoned);
    let row = wait_row(&f, id, "indeterminate").await;
    assert!(row["outcome"].is_null());
    assert!(row["execution_admitted"].is_null());
    assert!(service.shutdown().await);
    assert!(service.task.is_none());
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_missing_initial_and_cancellation_safe_terminal_retry() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    sqlx::query("REVOKE INSERT,UPDATE ON query_history.executions FROM qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    let handle = begin(&service).await;
    let receipt = handle.receipt().unwrap();
    assert_eq!(handle.initial_delivery(), Delivery::Queued);
    handle.admit().unwrap();
    assert_eq!(
        handle.finish(terminal(Outcome::Success)).await.unwrap(),
        Delivery::Queued
    );
    sqlx::query("GRANT INSERT,UPDATE ON query_history.executions TO qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    let row = wait_row(&f, receipt.execution_id(), "terminal").await;
    assert_eq!(row["outcome"], "success");
    assert_eq!(row["execution_admitted"], true);
    assert_eq!(
        serde_json::from_value::<DateTime<Utc>>(row["received_at"].clone()).unwrap(),
        receipt.received_at()
    );
    assert_eq!(
        serde_json::from_value::<DateTime<Utc>>(row["expires_at"].clone()).unwrap(),
        receipt.expires_at()
    );
    // Ambiguous acknowledgement: commit exists but replay still carries the
    // ORIGINAL immutable evidence. This is not a claim to inject a TCP failure.
    let observation = handle.observation.lock().unwrap().as_ref().unwrap().clone();
    assert_eq!(
        service
            .inner
            .queue(Payload::Observation(Box::new(observation))),
        Delivery::Queued
    );
    wait_ready(&service).await;
    assert_eq!(json_row(&f, receipt.execution_id()).await, row);

    // Cancellation while finish is awaiting a locked row retains its queue copy.
    let cancelled = begin(&service).await;
    let id = cancelled.receipt().unwrap().execution_id();
    let mut lock = f.admin.begin().await.unwrap();
    sqlx::query(
        "SELECT execution_id FROM query_history.executions WHERE execution_id=$1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut *lock)
    .await
    .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_millis(20),
        cancelled.finish(terminal(Outcome::Success))
    )
    .await
    .is_err());
    drop(cancelled);
    lock.rollback().await.unwrap();
    assert_eq!(wait_row(&f, id, "terminal").await["outcome"], "success");
    wait_ready(&service).await;
    assert!(service.shutdown().await);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_no_receipt_outage_continues_without_inventing_time() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    // Revoke CONNECT and terminate only this fixture's writer connections. A
    // real connection outage, not an environment URI or an in-memory backend.
    sqlx::query("REVOKE CONNECT ON DATABASE qh_fixture FROM qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='qh_writer' AND datname=current_database() AND pid<>pg_backend_pid()")
        .execute(&f.admin).await.unwrap();
    let start = Instant::now();
    let handle = begin(&service).await;
    // Real-clock tolerance is scheduling headroom, NOT the configured budget.
    assert!(start.elapsed() < Duration::from_millis(500));
    assert!(handle.receipt().is_none());
    assert_eq!(
        handle.finish(terminal(Outcome::Success)).await.unwrap(),
        Delivery::Unobserved
    );
    // The disabled handle permits the caller to continue its ordinary work.
    // This is a core API test, not evidence of routed SQL instrumentation.
    assert_eq!(handle.initial_delivery(), Delivery::Unobserved);
    assert!(service.health().loss_signals(LossReason::NoReceipt) >= 1);
    sqlx::query("GRANT CONNECT ON DATABASE qh_fixture TO qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    wait_ready(&service).await;
    let row = sqlx::query("SELECT started_at=received_at AND ended_at=received_at AS point, reason, expires_at-received_at=interval '720 hours' AS retention FROM query_history.coverage WHERE tenant='runtime-fixture'")
        .fetch_one(&f.admin).await.unwrap();
    assert!(row.get::<bool, _>("point"));
    assert!(row.get::<bool, _>("retention"));
    assert_eq!(row.get::<&str, _>("reason"), "store_unavailable");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_history.executions")
        .fetch_one(&f.admin)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(service.shutdown().await);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_write_timeout_uses_original_receipt_and_finish_budget() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    let handle = begin(&service).await;
    let id = handle.receipt().unwrap().execution_id();
    let mut lock = f.admin.begin().await.unwrap();
    sqlx::query(
        "SELECT execution_id FROM query_history.executions WHERE execution_id=$1 FOR UPDATE",
    )
    .bind(id)
    .fetch_one(&mut *lock)
    .await
    .unwrap();
    let start = Instant::now();
    assert_eq!(
        handle.finish(terminal(Outcome::Success)).await.unwrap(),
        Delivery::Queued
    );
    assert!(start.elapsed() >= WRITE_BUDGET);
    assert!(start.elapsed() < Duration::from_millis(500));
    assert!(service.health().loss_signals(LossReason::Timeout) >= 1);
    lock.rollback().await.unwrap();
    assert_eq!(wait_row(&f, id, "terminal").await["outcome"], "success");
    wait_ready(&service).await;
    assert!(service.shutdown().await);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_owner_heartbeat_recovery_and_late_genuine_evidence() {
    let f = fixture().await;
    let mut a = ready_service(&f).await;
    let handle = begin(&a).await;
    let id = handle.receipt().unwrap().execution_id();
    handle.admit().unwrap();
    wait_ready(&a).await;
    let mut b = ready_service(&f).await;
    assert!(b.health().last_heartbeat_success.is_some());
    assert_eq!(json_row(&f, id).await["observation_state"], "pending");
    sqlx::query("UPDATE query_history.owners SET heartbeat_at=clock_timestamp()-interval '121 seconds' WHERE owner_uuid=$1")
        .bind(a.owner_uuid()).execute(&f.admin).await.unwrap();
    assert_eq!(
        b.inner.writer.recover_stale(b.owner_uuid()).await.unwrap(),
        1
    );
    let row = json_row(&f, id).await;
    assert_eq!(row["observation_state"], "indeterminate");
    assert!(row["outcome"].is_null());
    handle.finish(terminal(Outcome::Success)).await.unwrap();
    assert_eq!(json_row(&f, id).await["outcome"], "success");
    wait_ready(&a).await;
    assert!(a.shutdown().await);
    assert!(b.shutdown().await);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_expiry_during_outage_never_resurrects() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    let handle = begin(&service).await;
    let mut event = handle
        .observation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .event
        .clone();
    expire(&f, &mut event).await;
    // Only a private test fixture can replace an opaque DB receipt with an
    // already-expired one. No production clock/re-admission seam is exposed.
    handle.observation.lock().unwrap().as_mut().unwrap().event = event.clone();
    f.writer.purge_expired(1000).await.unwrap();
    assert_eq!(
        handle.finish(terminal(Outcome::Success)).await.unwrap(),
        Delivery::Unobserved
    );
    wait_ready(&service).await;
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM query_history.executions WHERE execution_id=$1")
            .bind(event.receipt.execution_id())
            .fetch_one(&f.admin)
            .await
            .unwrap();
    assert_eq!(count, 0);
    assert!(service.health().loss_signals(LossReason::Expired) >= 1);
    assert!(service.shutdown().await);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_retry_age_queue_full_and_shutdown_uncertainty() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    let handle = begin(&service).await;
    let id = handle.receipt().unwrap().execution_id();
    // Stop the task for deterministic queue-state injection, not a different
    // execution mode; production always runs the single bounded worker.
    service.task.take().unwrap().abort();
    let payload = Payload::Observation(Box::new(
        handle.observation.lock().unwrap().as_ref().unwrap().clone(),
    ));
    let mut entry = Entry::new(payload);
    entry.first_attempt = Instant::now() - RETRY_AGE;
    service.inner.local.lock().unwrap().queue.push(entry);
    assert!(retry(&service.inner).await);
    assert_eq!(service.health().loss_signals(LossReason::RetryExpired), 1);
    // Age alone is not evidence of store unavailability. Do not fabricate a
    // durable store_unavailable reason for an unattempted queued payload.
    assert_eq!(service.health().queued_entries, 0);
    let mut known_failure = Entry::new(Payload::Observation(Box::new(
        handle.observation.lock().unwrap().as_ref().unwrap().clone(),
    )));
    known_failure.first_attempt = Instant::now() - RETRY_AGE;
    known_failure.store_failure_observed = true;
    service
        .inner
        .local
        .lock()
        .unwrap()
        .queue
        .push(known_failure);
    assert!(retry(&service.inner).await);
    assert_eq!(service.health().queued_entries, 1);
    assert!(matches!(
        service
            .inner
            .local
            .lock()
            .unwrap()
            .queue
            .entries
            .front()
            .unwrap()
            .payload,
        Payload::LossPoint {
            queue_full: false,
            ..
        }
    ));
    {
        let mut local = service.inner.local.lock().unwrap();
        local.queue = RetryQueue::default();
        for _ in 0..QUEUE_ENTRIES {
            assert!(local.queue.push(Entry::new(point())));
        }
    }
    assert_eq!(handle.admit().unwrap(), Delivery::Unobserved);
    assert_eq!(service.health().queued_entries, QUEUE_ENTRIES);
    assert_eq!(service.health().loss_signals(LossReason::QueueFull), 1);
    assert!(service.health().queued_payload_bytes <= QUEUE_BYTES);
    sqlx::query("REVOKE INSERT,UPDATE ON query_history.coverage FROM qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    service.task = Some(tokio::spawn(run(service.inner.clone())));
    let abort = service.task.as_ref().unwrap().abort_handle();
    assert!(!service.shutdown().await);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!service.shutdown().await);
    assert!(service.task.is_none());
    assert!(service.health().closed);
    assert!(!service.health().history_ready);
    drop(handle);
    assert_eq!(json_row(&f, id).await["observation_state"], "pending");
    sqlx::query("GRANT INSERT,UPDATE ON query_history.coverage TO qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    // Service Drop also aborts without waiting/spawning; the saved AbortHandle
    // verifies that task reaches cancellation, rather than leaking a worker.
    let dropped = ready_service(&f).await;
    let abort = dropped.task.as_ref().unwrap().abort_handle();
    drop(dropped);
    tokio::task::yield_now().await;
    assert!(abort.is_finished());
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_cleanup_startup_competition_failure_restart_and_backlog() {
    let f = fixture().await;
    let event = PendingExecution::new(
        f.writer.receipt().await.unwrap(),
        principal(),
        Uuid::new_v4(),
        request(),
        RedactedSql::parse(""),
    )
    .unwrap();
    f.writer.persist_pending(&event).await.unwrap();
    let mut expired = event.clone();
    expire(&f, &mut expired).await;
    sqlx::query("REVOKE DELETE ON query_history.executions FROM qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    let mut a = HistoryService::new(
        HistoryWriter::connect(options(&f.socket, "qh_writer"))
            .await
            .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!a.health().startup_caught_up);
    assert!(!a.health().history_ready);
    sqlx::query("GRANT DELETE ON query_history.executions TO qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    let mut b = HistoryService::new(
        HistoryWriter::connect(options(&f.socket, "qh_writer"))
            .await
            .unwrap(),
    );
    wait_ready(&a).await;
    wait_ready(&b).await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_history.executions")
        .fetch_one(&f.admin)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(a.health().last_cleanup_success.is_some());
    assert_eq!(a.health().oldest_overdue_ms, None);
    assert!(a.shutdown().await);
    assert!(b.shutdown().await);
    let mut restarted = ready_service(&f).await;
    assert!(restarted.health().startup_caught_up);
    assert!(restarted.shutdown().await);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_begin_initial_timeout_has_receipt_without_gating_work() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    let mut lock = f.admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE query_history.executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let start = Instant::now();
    let handle = begin(&service).await;
    assert!(start.elapsed() >= WRITE_BUDGET);
    assert!(start.elapsed() < Duration::from_millis(500));
    let receipt = handle.receipt().unwrap();
    assert_eq!(handle.initial_delivery(), Delivery::Queued);
    // Genuine terminal evidence after bounded initial failure, same receipt.
    handle.admit().unwrap();
    lock.rollback().await.unwrap();
    handle.finish(terminal(Outcome::Success)).await.unwrap();
    assert_eq!(
        wait_row(&f, receipt.execution_id(), "terminal").await["outcome"],
        "success"
    );
    wait_ready(&service).await;
    assert!(service.shutdown().await);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_bounded_owner_recovery_missing_owner_and_self_exclusion() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    // No owner registration: a DB receipt older than the lease is uncertain.
    sqlx::query(r#"
      WITH t AS MATERIALIZED (SELECT clock_timestamp()-interval '121 seconds' AS now)
      INSERT INTO query_history.executions(execution_id,tenant,principal_kind,received_at,expires_at,owner_uuid,operation,format,priority,dry_run,redaction_version,redaction_status)
      SELECT gen_random_uuid(),'runtime-fixture','unknown',now,now+interval '720 hours',gen_random_uuid(),'sql','records','interactive',false,1,'omitted'
      FROM t,generate_series(1,1001)
    "#).execute(&f.admin).await.unwrap();
    assert_eq!(
        service
            .inner
            .writer
            .recover_stale(service.owner_uuid())
            .await
            .unwrap(),
        1000
    );
    assert_eq!(
        service
            .inner
            .writer
            .recover_stale(service.owner_uuid())
            .await
            .unwrap(),
        1
    );
    let handle = begin(&service).await;
    let id = handle.receipt().unwrap().execution_id();
    sqlx::query("UPDATE query_history.owners SET heartbeat_at=clock_timestamp()-interval '121 seconds' WHERE owner_uuid=$1")
        .bind(service.owner_uuid()).execute(&f.admin).await.unwrap();
    assert_eq!(
        service
            .inner
            .writer
            .recover_stale(service.owner_uuid())
            .await
            .unwrap(),
        0
    );
    assert_eq!(json_row(&f, id).await["observation_state"], "pending");
    drop(handle);
    wait_ready(&service).await;
    assert!(service.shutdown().await);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_competing_lock_is_not_catchup_and_batches_retry() {
    let f = fixture().await;
    // 1001 expired rows need multiple bounded batches; no synthetic reader clock.
    sqlx::query(r#"
      WITH t AS MATERIALIZED (SELECT clock_timestamp()-interval '720 hours 1 second' AS now)
      INSERT INTO query_history.executions(execution_id,tenant,principal_kind,received_at,expires_at,owner_uuid,operation,format,priority,dry_run,redaction_version,redaction_status)
      SELECT gen_random_uuid(),'runtime-fixture','unknown',now,now+interval '720 hours',gen_random_uuid(),'sql','records','interactive',false,1,'omitted'
      FROM t,generate_series(1,1001)
    "#).execute(&f.admin).await.unwrap();
    let mut lock = f.admin.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(724918220194)")
        .execute(&mut *lock)
        .await
        .unwrap();
    let mut service = HistoryService::new(
        HistoryWriter::connect(options(&f.socket, "qh_writer"))
            .await
            .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!service.health().history_ready);
    assert!(!service.health().startup_caught_up);
    assert!(service.health().oldest_overdue_ms.is_some());
    lock.rollback().await.unwrap();
    wait_ready(&service).await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_history.executions")
        .fetch_one(&f.admin)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(service.shutdown().await);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_periodic_primary_purge_and_heartbeat_real_clock() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    let initial_cleanup = service.health().last_cleanup_success.unwrap();
    let initial_heartbeat: DateTime<Utc> =
        sqlx::query_scalar("SELECT heartbeat_at FROM query_history.owners WHERE owner_uuid=$1")
            .bind(service.owner_uuid())
            .fetch_one(&f.admin)
            .await
            .unwrap();
    let handle = begin(&service).await;
    let mut event = handle
        .observation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .event
        .clone();
    expire(&f, &mut event).await;
    // Real wall time, no paused Tokio clock while driving a PostgreSQL socket.
    tokio::time::timeout(Duration::from_secs(66), async {
        loop {
            if service
                .health()
                .last_cleanup_success
                .is_some_and(|last| last > initial_cleanup)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("periodic purge did not run");
    assert!(initial_cleanup.elapsed() >= PURGE_INTERVAL);
    let heartbeat: DateTime<Utc> =
        sqlx::query_scalar("SELECT heartbeat_at FROM query_history.owners WHERE owner_uuid=$1")
            .bind(service.owner_uuid())
            .fetch_one(&f.admin)
            .await
            .unwrap();
    assert!(heartbeat > initial_heartbeat);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM query_history.executions WHERE execution_id=$1")
            .bind(event.receipt.execution_id())
            .fetch_one(&f.admin)
            .await
            .unwrap();
    assert_eq!(count, 0);
    // Avoid deliberately retrying an unmodified, live in-memory receipt after
    // the fixture-only expiry rewrite. No public production clock seam exists.
    handle.observation.lock().unwrap().as_mut().unwrap().event = event;
    drop(handle);
    wait_ready(&service).await;
    assert!(service.shutdown().await);
}

#[tokio::test(start_paused = true)]
async fn retry_deadline_uses_remaining_age_and_expires_entries_behind_head() {
    let mut q = RetryQueue::default();
    let mut first = Entry::new(point());
    first.first_attempt = Instant::now() - Duration::from_secs(29);
    assert!(q.push(first));
    assert!(q.push(Entry::new(point())));
    tokio::time::advance(Duration::from_millis(990)).await;
    let deadline = q.entries.front().unwrap().first_attempt + RETRY_AGE;
    let start = Instant::now();
    assert!(matches!(
        bounded_before(
            async { std::future::pending::<Result<(), HistoryError>>().await },
            deadline
        )
        .await,
        Err(AttemptError::Timeout)
    ));
    assert_eq!(start.elapsed(), Duration::from_millis(10));
    assert_eq!(q.expire().len(), 1);
    assert_eq!(q.count, 1);
    tokio::time::advance(RETRY_AGE).await;
    assert_eq!(q.expire().len(), 1);
    assert_eq!(q.count, 0);
    assert_eq!(q.bytes, 0);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_handle_and_service_drop_never_wait_for_contended_state() {
    let f = fixture().await;
    let service = ready_service(&f).await;
    let handle = begin(&service).await;
    let id = handle.receipt().unwrap().execution_id();
    let inner = service.inner.clone();
    let abort = service.task.as_ref().unwrap().abort_handle();
    {
        // Same-thread contention would deadlock a blocking Drop implementation.
        let _guard = inner.local.lock().unwrap();
        drop(handle);
        drop(service);
        assert!(inner.closed());
        assert_eq!(
            inner.signals[LossReason::QueueContended as usize].load(Ordering::Relaxed),
            1
        );
    }
    tokio::task::yield_now().await;
    assert!(abort.is_finished());
    // Lost Drop evidence is uncertainty, never a synthetic terminal.
    assert_eq!(json_row(&f, id).await["observation_state"], "pending");
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_cancelled_shutdown_retains_unavailable_worker_for_drop() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    sqlx::query("REVOKE INSERT,UPDATE ON query_history.coverage FROM qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    assert_eq!(service.inner.queue(point()), Delivery::Queued);
    tokio::time::timeout(Duration::from_secs(8), async {
        while service.health().loss_signals(LossReason::StoreUnavailable) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(service.health().queued_entries, 1);
    let abort = service.task.as_ref().unwrap().abort_handle();
    let worker_owner = Arc::downgrade(&service.inner);
    {
        let mut shutdown = Box::pin(service.shutdown());
        // Poll the actual drain wait once, then cancel before its budget elapses.
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(shutdown.as_mut().poll(cx)))
                .await
                .is_pending()
        );
    }
    assert!(service.task.is_some());
    assert!(service.health().closed);
    assert!(!abort.is_finished());
    drop(service);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !abort.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled shutdown detached its worker");
    assert!(worker_owner.upgrade().is_none());
    sqlx::query("GRANT INSERT,UPDATE ON query_history.coverage TO qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_shutdown_deadline_retains_abort_join_ownership() {
    let f = fixture().await;
    for drop_after_timeout in [false, true] {
        let mut service = ready_service(&f).await;
        // Isolate the join seam from DB scheduling: the real worker is joined
        // first; a cooperative pending task cannot complete before abort.
        let task = service.task.take().unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        service.task = Some(tokio::spawn(std::future::pending()));
        let abort = service.task.as_ref().unwrap().abort_handle();
        tokio::time::pause();
        let start = Instant::now();
        {
            let mut shutdown = Box::pin(service.shutdown());
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(shutdown.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            // Tokio's timer wheel rounds deadlines to millisecond ticks.
            tokio::time::advance(WRITE_BUDGET + Duration::from_millis(1)).await;
            // One poll crosses the actual timeout and abort/join branch. The
            // aborted task has not been scheduled again to finish its join.
            assert_eq!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(shutdown.as_mut().poll(cx))).await,
                std::task::Poll::Ready(false)
            );
        }
        assert_eq!(start.elapsed(), WRITE_BUDGET + Duration::from_millis(1));
        assert_eq!(service.health().loss_signals(LossReason::Shutdown), 1);
        assert!(service.task.is_some());
        assert!(!abort.is_finished());
        assert!(service.health().closed);
        assert_eq!(service.inner.queue(point()), Delivery::Unobserved);
        if drop_after_timeout {
            // Cancellation of another join wait must also leave Drop owning it.
            {
                let mut shutdown = Box::pin(service.shutdown());
                assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(
                    shutdown.as_mut().poll(cx)
                ))
                .await
                .is_pending());
            }
            assert!(service.task.is_some());
            drop(service);
        } else {
            tokio::task::yield_now().await;
            assert!(abort.is_finished());
            assert!(!service.shutdown().await); // cancelled join, not success
            assert!(service.task.is_none());
            assert!(service.shutdown().await); // no duplicate polling of join
        }
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !abort.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_runtime_retry_expiring_during_persistence_preserves_tenant_loss_point() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    let handle = begin(&service).await;
    let receipt = handle.receipt().unwrap();
    let task = service.task.take().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let observation = handle.observation.lock().unwrap().as_ref().unwrap().clone();
    let mut lock = f.admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE query_history.executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let mut entry = Entry::new(Payload::Observation(Box::new(observation)));
    entry.first_attempt = Instant::now() - RETRY_AGE + Duration::from_millis(50);
    // No injected known-failure flag: the actual blocked retry must establish it.
    assert!(!entry.store_failure_observed);
    assert!(service.inner.local.lock().unwrap().queue.push(entry));
    {
        let mut attempt = Box::pin(retry(&service.inner));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(attempt.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        {
            let local = service.inner.local.lock().unwrap();
            assert_eq!(local.queue.count, 1); // reserved in-flight, not pre-expiry
            assert!(local.queue.entries.is_empty());
        }
        assert!(!attempt.await);
    }
    assert_eq!(service.health().loss_signals(LossReason::Timeout), 1);
    assert_eq!(service.health().loss_signals(LossReason::RetryExpired), 1);
    {
        let local = service.inner.local.lock().unwrap();
        assert_eq!(local.queue.count, 1);
        assert_eq!(local.queue.entries.len(), 1);
        assert!(local.queue.bytes <= QUEUE_BYTES);
        let entry = local.queue.entries.front().unwrap();
        assert!(!entry.expired());
        assert!(
            matches!(&entry.payload, Payload::LossPoint { tenant, queue_full: false, .. }
            if tenant == "runtime-fixture")
        );
    }
    let absent: i64 = sqlx::query_scalar("SELECT count(*) FROM query_history.coverage")
        .fetch_one(&f.admin)
        .await
        .unwrap();
    assert_eq!(absent, 0);
    lock.rollback().await.unwrap();
    let recovery: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&f.admin)
        .await
        .unwrap();
    assert!(retry(&service.inner).await);
    assert_eq!(service.health().queued_entries, 0);
    let coverage = sqlx::query("SELECT started_at, ended_at, received_at, reason, expires_at-received_at=interval '720 hours' AS retention FROM query_history.coverage WHERE tenant='runtime-fixture'")
        .fetch_one(&f.admin).await.unwrap();
    let received: DateTime<Utc> = coverage.get("received_at");
    assert!(received >= recovery);
    assert_eq!(coverage.get::<DateTime<Utc>, _>("started_at"), received);
    assert_eq!(coverage.get::<DateTime<Utc>, _>("ended_at"), received);
    assert_eq!(coverage.get::<&str, _>("reason"), "store_unavailable");
    assert!(coverage.get::<bool, _>("retention"));
    let row = json_row(&f, receipt.execution_id()).await;
    assert_eq!(row["observation_state"], "pending");
    assert!(row["outcome"].is_null());
    assert_eq!(
        serde_json::from_value::<DateTime<Utc>>(row["received_at"].clone()).unwrap(),
        receipt.received_at()
    );
    assert_eq!(
        serde_json::from_value::<DateTime<Utc>>(row["expires_at"].clone()).unwrap(),
        receipt.expires_at()
    );
    assert!(service.shutdown().await);
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_stream_queue_terminal_contention_duplicates_and_shutdown() {
    let f = fixture().await;
    let mut service = ready_service(&f).await;
    let handle = Arc::new(begin(&service).await);
    let receipt = handle.receipt().unwrap();
    handle.admit().unwrap();
    // Contended stream finalization never waits on a state lock. No selected
    // success or fabricated delivery acknowledgement survives this attempt.
    {
        let lock = handle.observation.lock().unwrap();
        let start = std::time::Instant::now();
        assert_eq!(
            handle.finish_queued(terminal(Outcome::Success)).unwrap(),
            Delivery::Unobserved
        );
        assert!(start.elapsed() < Duration::from_millis(100));
        assert!(lock.as_ref().unwrap().terminal.is_none());
    }
    assert!(service.health().loss_signals(LossReason::QueueContended) > 0);
    // Real simultaneous source-terminal/drop choices still have one winner.
    let barrier = std::sync::Barrier::new(3);
    let choices = std::thread::scope(|scope| {
        let a = scope.spawn(|| {
            barrier.wait();
            (
                Outcome::Success,
                handle.finish_queued(terminal(Outcome::Success)),
            )
        });
        let b = scope.spawn(|| {
            barrier.wait();
            (
                Outcome::Disconnected,
                handle.finish_queued(terminal(Outcome::Disconnected)),
            )
        });
        barrier.wait();
        [a.join().unwrap(), b.join().unwrap()]
    });
    assert_eq!(
        choices
            .iter()
            .filter(|(_, r)| *r == Ok(Delivery::Queued))
            .count(),
        1
    );
    let winner = choices
        .iter()
        .find(|(_, r)| *r == Ok(Delivery::Queued))
        .unwrap()
        .0;
    for (_, result) in &choices {
        assert!(matches!(
            result,
            Ok(Delivery::Queued | Delivery::Unobserved) | Err(HistoryError::Conflict)
        ));
    }
    assert_eq!(
        handle.finish_queued(terminal(winner)).unwrap(),
        Delivery::AlreadyFinalized
    );
    assert_eq!(
        handle.finish(terminal(winner)).await.unwrap(),
        Delivery::AlreadyFinalized
    );
    let row = wait_row(&f, receipt.execution_id(), "terminal").await;
    assert_eq!(row["outcome"], serde_json::to_value(winner).unwrap());
    let expires = row["expires_at"].clone();
    drop(handle);
    let _ = service.shutdown().await;
    assert_eq!(
        json_row(&f, receipt.execution_id()).await["expires_at"],
        expires
    );
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_batch_resume_requires_original_binding_and_expiry_never_resurrects() {
    let f = fixture().await;
    let v2: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='query_history.executions'::regclass AND conname='executions_batch_job_v2')").fetch_one(&f.admin).await.unwrap();
    if !v2 {
        sqlx::raw_sql(include_str!("../schema-v2-batch.sql"))
            .execute(&f.admin)
            .await
            .unwrap();
    }
    let mut service = ready_service(&f).await;
    let observer = service.observer();
    let mut batch_request = request();
    batch_request.priority = Priority::Batch;
    let handle = Arc::new(
        observer
            .begin(
                principal(),
                batch_request,
                RedactedSql::parse("SELECT 'SECRET_SQL'"),
            )
            .await,
    );
    let execution = handle.receipt().unwrap().execution_id();
    let jobs = crate::jobs::JobStore::new(1, Duration::from_secs(600));
    let job = jobs
        .submit_observed(
            "SELECT 'legacy raw payload outside history guarantee'".into(),
            crate::limits::Priority::Batch,
            Some("runtime-fixture".into()),
            Some(execution),
        )
        .await
        .unwrap();
    observer.associate_job(execution, job.0).await;
    assert!(observer
        .resume_job(execution, Uuid::new_v4(), "runtime-fixture")
        .await
        .is_none());
    assert!(observer
        .resume_job(execution, job.0, "foreign")
        .await
        .is_none());
    assert!(observer
        .resume_job(Uuid::new_v4(), job.0, "runtime-fixture")
        .await
        .is_none());
    let resumed = observer
        .resume_job(execution, job.0, "runtime-fixture")
        .await
        .unwrap();
    assert_eq!(resumed.execution_id(), Some(execution));
    drop(resumed); // Unknown admission remains indeterminate, not guessed false.
    wait_row(&f, execution, "indeterminate").await;
    wait_ready(&service).await;

    // Privileged fixture-only clock rewrite of the ACTUAL begin-created row
    // and its original opaque DTO, as in the established runtime expiry tests.
    // No execution row is seeded and no production clock/receipt API is added.
    let mut event = handle
        .observation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .event
        .clone();
    expire(&f, &mut event).await;
    handle.observation.lock().unwrap().as_mut().unwrap().event = event;
    let original_receipt = handle.receipt().unwrap();
    assert!(observer
        .resume_job(execution, job.0, "runtime-fixture")
        .await
        .is_none());
    f.writer.purge_expired(1000).await.unwrap();
    assert!(jobs.cancel(job).await.unwrap());
    let observation =
        super::super::batch::BatchObservation::new(handle.clone(), None, job.0, Some(false));
    observation.finish(crate::jobs::JobStatus::Cancelled, false);
    wait_ready(&service).await;
    assert_eq!(
        handle.receipt().unwrap().execution_id(),
        original_receipt.execution_id()
    );
    assert_eq!(
        handle.receipt().unwrap().expires_at(),
        original_receipt.expires_at()
    );
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM query_history.executions WHERE execution_id=$1)",
    )
    .bind(execution)
    .fetch_one(&f.admin)
    .await
    .unwrap();
    assert!(
        !exists,
        "expired queued terminal must never reinsert its parent"
    );
    let _ = service.shutdown().await;
    let mut replacement = ready_service(&f).await;
    assert!(replacement
        .observer()
        .resume_job(execution, job.0, "runtime-fixture")
        .await
        .is_none());
    assert!(replacement.shutdown().await);
}
