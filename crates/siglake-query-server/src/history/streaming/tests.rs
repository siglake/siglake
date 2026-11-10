use super::*;
use crate::{
    auth::CallerIdentity,
    format::NdjsonStream,
    history::postgres_tests::{fixture, terminal},
};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    error::DataFusionError, physical_plan::stream::RecordBatchStreamAdapter,
    prelude::SessionContext,
};
use futures::{channel::mpsc, StreamExt};
use std::time::Duration;

async fn observation(service: &HistoryService) -> (Uuid, StreamObservation) {
    let handle = Arc::new(
        service
            .observer()
            .begin(
                Principal::from_caller("stream-fixture", &CallerIdentity::default()).unwrap(),
                RequestMetadata {
                    operation: Operation::Sql,
                    format: OutputFormat::Ndjson,
                    priority: Priority::Interactive,
                    dry_run: false,
                },
                RedactedSql::parse("SECRET_SQL"),
            )
            .await,
    );
    let id = handle.receipt().unwrap().execution_id();
    handle.admit().unwrap();
    (
        id,
        StreamObservation::new(
            handle,
            Instant::now(),
            ExecutionMeasurements {
                counters: Counters::default(),
                duration_ms: None,
                cache_state: CacheState::Bypass,
                truncated: None,
                job_id: None,
            },
        ),
    )
}
async fn stored(db: &sqlx::PgPool, id: Uuid) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if let Some(row) = sqlx::query_scalar::<_, serde_json::Value>("SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1 AND observation_state='terminal'").bind(id).fetch_optional(db).await.unwrap() { return row; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap()
}
#[tokio::test]
#[ignore = "own disposable PG; scripts/check-query-history.sh"]
async fn controlled_ndjson_backpressure_error_cap_eof_and_drop_choose_one_terminal() {
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    let plan = SessionContext::new()
        .sql("SELECT 1")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_meta",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            "error\ntruncated",
            "error",
        ]))],
    )
    .unwrap();
    // Channel backpressure is controlled without sleeps, network or runtime
    // knobs. Marker-shaped values and newline content do not affect evidence.
    for choice in [
        "eof",
        "error",
        "drop",
        "cap",
        "scan-cap",
        "pending-trailer-drop",
    ] {
        let (id, observation) = observation(&service).await;
        let (mut tx, rx) = mpsc::channel(1);
        let source = RecordBatchStreamAdapter::new(schema.clone(), rx);
        let mut stream = NdjsonStream::new(
            Box::pin(source),
            Some(if choice == "cap" || choice == "pending-trailer-drop" {
                1
            } else {
                10
            }),
        )
        .with_history(Some(observation), plan.clone());
        if choice == "scan-cap" {
            stream =
                stream.with_rows_scanned_cap(plan.clone(), 0, crate::limits::Priority::Interactive);
        }
        assert!(futures::poll!(stream.next()).is_pending());
        tx.try_send(Ok(batch.clone())).unwrap();
        assert!(stream.next().await.unwrap().is_ok());
        let pending: bool = sqlx::query_scalar(
            "SELECT outcome IS NULL FROM query_history.executions WHERE execution_id=$1",
        )
        .bind(id)
        .fetch_one(&f.admin)
        .await
        .unwrap();
        assert!(pending, "a yielded batch is not terminal success");
        match choice {
            "eof" => {
                drop(tx);
                assert!(stream.next().await.is_none());
            }
            "error" => {
                assert!(futures::poll!(stream.next()).is_pending());
                tx.try_send(Err(DataFusionError::Execution(
                    "PRIVATE_FAILURE_NOT_FOR_HISTORY".into(),
                )))
                .unwrap();
                assert!(
                    stream.next().await.unwrap().is_ok(),
                    "existing errors are JSON bytes, not Rust Err"
                );
                assert!(stream.next().await.is_none());
            }
            "cap" | "scan-cap" => {
                assert!(stream.next().await.unwrap().is_ok());
                assert!(stream.next().await.is_none());
            }
            _ => {}
        }
        drop(stream);
        let row = stored(&f.admin, id).await;
        match choice {
            "eof" => {
                assert_eq!(row["outcome"], "success");
                assert_eq!(row["returned_rows"], 2);
                assert_eq!(row["truncated"], false);
            }
            "cap" | "scan-cap" => {
                assert_eq!(row["outcome"], "success");
                assert_eq!(row["returned_rows"], if choice == "cap" { 1 } else { 2 });
                assert_eq!(row["truncated"], true);
            }
            "error" => {
                assert_eq!(row["outcome"], "failed");
                assert_eq!(row["error_code"], "execution");
            }
            _ => {
                assert_eq!(row["outcome"], "disconnected");
                assert!(row["returned_rows"].is_null());
                assert!(row["truncated"].is_null());
            }
        }
        assert!(row["measured_rows_scanned"].is_null());
        assert!(row["measured_bytes_scanned"].is_null());
        let safe = row.to_string();
        assert!(!safe.contains("PRIVATE_FAILURE"));
        assert!(!safe.contains("SECRET_SQL"));
    }
    // EOF/engine terminal vs drop/shutdown is one immutable choice. Observers
    // are non-owning; the final queue selection cannot keep the task alive.
    let (id, mut o) = observation(&service).await;
    o.complete(2, false, None, None);
    o.failed();
    let row = stored(&f.admin, id).await;
    assert_eq!(row["outcome"], "success");
    assert!(o.handle.finish(terminal(Outcome::Failed)).await.is_err());
    let _ = service.shutdown().await;
    drop(o);
}

#[tokio::test]
#[ignore = "own disposable PG; scripts/check-query-history.sh"]
async fn controlled_source_completion_races_shutdown_with_loss_not_synthetic_outcome() {
    let f = fixture().await;
    for _ in 0..4 {
        let writer = HistoryWriter::connect(crate::history::postgres_tests::options(
            &f.socket,
            "qh_writer",
        ))
        .await
        .unwrap();
        let mut service = HistoryService::new(writer);
        let (id, mut observation) = observation(&service).await;
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let source_barrier = barrier.clone();
        let source = std::thread::spawn(move || {
            source_barrier.wait();
            observation.complete(2, false, None, None);
            // A later delivery drop cannot replace typed completion, even if
            // shutdown prevented its queue delivery.
            drop(observation);
        });
        barrier.wait();
        let _ = service.shutdown().await;
        source.join().unwrap();
        let outcome: Option<String> = sqlx::query_scalar(
            "SELECT outcome FROM query_history.executions WHERE execution_id=$1",
        )
        .bind(id)
        .fetch_one(&f.admin)
        .await
        .unwrap();
        assert!(outcome.is_none() || outcome.as_deref() == Some("success"));
        assert!(service.health().closed);
    }
}

#[tokio::test]
#[ignore = "own disposable PG; scripts/check-query-history.sh"]
async fn body_delivery_gate_source_eof_never_poll_timeout_abandonment_and_error_are_distinct() {
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    for choice in ["success", "timeout", "drop", "error"] {
        let (id, mut source) = observation(&service).await;
        let delivery = source.defer_delivery();
        source.complete(1, false, None, None);
        drop(source);
        let pending: bool = sqlx::query_scalar(
            "SELECT outcome IS NULL FROM query_history.executions WHERE execution_id=$1",
        )
        .bind(id)
        .fetch_one(&f.admin)
        .await
        .unwrap();
        assert!(
            pending,
            "tiny producer EOF cannot fabricate delivered-body success"
        );
        match choice {
            "success" => delivery.delivered(),
            "timeout" => delivery.timed_out(),
            "error" => delivery.failed(),
            _ => delivery.abandoned(),
        }
        // Immutable typed choice wins any later EOF/drop/error race.
        delivery.delivered();
        delivery.timed_out();
        delivery.abandoned();
        let row = stored(&f.admin, id).await;
        assert_eq!(
            row["outcome"],
            match choice {
                "success" => "success",
                "timeout" => "timed_out",
                "error" => "failed",
                _ => "disconnected",
            }
        );
        if choice == "success" {
            assert_eq!(row["returned_rows"], 1);
        } else {
            assert!(row["returned_rows"].is_null());
        }
        assert!(row["measured_rows_scanned"].is_null());
        assert!(row["measured_bytes_scanned"].is_null());
    }
    let _ = service.shutdown().await;
}

#[tokio::test]
#[ignore = "own disposable PG; scripts/check-query-history.sh"]
async fn producer_late_ready_error_crossing_deadline_commits_timeout_transport_and_history() {
    use crate::query_controls::{body::BodyTasks, runtime::test_controls};
    use axum::{body::Body, response::Response};
    use bytes::Bytes;
    use std::{
        io,
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Poll},
    };
    struct LateError {
        observation: Option<StreamObservation>,
        deadline: tokio::time::Instant,
        entered: Arc<AtomicBool>,
        _guard: crate::admission::AdmissionGuard,
        _exec: tokio::sync::OwnedSemaphorePermit,
        _cancel: siglake_storage::CancelOnDrop,
    }
    impl futures::Stream for LateError {
        type Item = io::Result<Bytes>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            assert!(
                tokio::time::Instant::now() < self.deadline,
                "source must enter after timer Pending but before expiry"
            );
            self.entered.store(true, Ordering::SeqCst);
            // One synchronous native poll crosses the actual original clock.
            // No timer repoll/OS preemption is possible on this test executor.
            std::thread::sleep(
                self.deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    + Duration::from_millis(1),
            );
            if let Some(observation) = &mut self.observation {
                observation.failed();
            }
            Poll::Ready(Some(Err(io::Error::other("private-native-source-error"))))
        }
    }
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    for with_source_candidate in [false, true] {
        let (id, mut source) = observation(&service).await;
        let delivery = source.defer_delivery();
        let tasks = BodyTasks::default();
        let admission = crate::admission::AdmissionController::new(1, 1, Duration::from_secs(1));
        let exec = Arc::new(tokio::sync::Semaphore::new(1));
        let cancel = siglake_storage::QueryCancel::new();
        let entered = Arc::new(AtomicBool::new(false));
        let guard = admission.acquire(1).await.unwrap();
        let permit = exec.clone().acquire_owned().await.unwrap();
        let controls = test_controls(Duration::from_millis(100));
        let mut response = Response::new(Body::from_stream(LateError {
            observation: with_source_candidate.then_some(source),
            deadline: controls.deadline,
            entered: entered.clone(),
            _guard: guard,
            _exec: permit,
            _cancel: siglake_storage::CancelOnDrop(cancel.clone()),
        }));
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            "application/x-ndjson".parse().unwrap(),
        );
        let response = tasks
            .reserve()
            .unwrap()
            .start(response, controls, Some(delivery))
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert!(entered.load(Ordering::SeqCst));
        assert!(
            bytes == b"{\"_meta\":\"timeout\"}\n"[..],
            "late-ready error must use timeout transport, not execution failure"
        );
        tasks.shutdown().await;
        assert_eq!(exec.available_permits(), 1);
        assert!(cancel.is_cancelled());
        drop(
            tokio::time::timeout(Duration::from_millis(300), admission.acquire(1))
                .await
                .unwrap()
                .unwrap(),
        );
        let row = stored(&f.admin, id).await;
        assert_eq!(row["outcome"], "timed_out");
        assert_eq!(row["error_code"], "timeout");
        assert!(row["returned_rows"].is_null());
        assert!(!row.to_string().contains("private-native-source-error"));
    }
    let _ = service.shutdown().await;
}

#[tokio::test]
#[ignore = "own disposable PG; scripts/check-query-history.sh"]
async fn controlled_error_marker_candidate_waits_for_delivery_timeout_or_abandonment_arbiter() {
    use crate::query_controls::{body::BodyTasks, runtime::test_controls};
    use axum::{body::Body, response::Response};
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    let plan = SessionContext::new()
        .sql("SELECT 1")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    for choice in ["deliver", "timeout", "abandon"] {
        let (id, mut source) = observation(&service).await;
        let delivery = source.defer_delivery();
        let tasks = BodyTasks::default();
        let schema = Arc::new(Schema::empty());
        let stream = RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter([Err(DataFusionError::Execution(
                "private-marker-error".into(),
            ))]),
        );
        let stream =
            NdjsonStream::new(Box::pin(stream), Some(1)).with_history(Some(source), plan.clone());
        let controls = test_controls(Duration::from_secs(1));
        let mut response = Response::new(Body::from_stream(stream));
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            "application/x-ndjson".parse().unwrap(),
        );
        let response = tasks
            .reserve()
            .unwrap()
            .start(response, controls, Some(delivery.clone()))
            .unwrap();
        tokio::time::timeout(Duration::from_millis(300), async {
            while delivery.0.lock().unwrap().pending.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let pending: bool = sqlx::query_scalar(
            "SELECT outcome IS NULL FROM query_history.executions WHERE execution_id=$1",
        )
        .bind(id)
        .fetch_one(&f.admin)
        .await
        .unwrap();
        assert!(
            pending,
            "source error marker is only a bounded typed candidate until arbitration"
        );
        match choice {
            "deliver" => {
                let bytes = axum::body::to_bytes(response.into_body(), 4096)
                    .await
                    .unwrap();
                let marker: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(
                    marker["_meta"], "error",
                    "existing timely error marker remains wire-compatible"
                );
            }
            "timeout" => {
                tokio::time::sleep_until(controls.deadline + Duration::from_millis(5)).await;
                let bytes = axum::body::to_bytes(response.into_body(), 4096)
                    .await
                    .unwrap();
                assert!(bytes == b"{\"_meta\":\"timeout\"}\n"[..]);
            }
            _ => drop(response),
        }
        tasks.shutdown().await;
        // A later candidate-delivery/drop cannot replace the committed winner.
        delivery.delivered();
        delivery.abandoned();
        let row = stored(&f.admin, id).await;
        assert_eq!(
            row["outcome"],
            match choice {
                "deliver" => "failed",
                "timeout" => "timed_out",
                _ => "disconnected",
            }
        );
        assert_eq!(
            row["error_code"],
            match choice {
                "deliver" => "execution",
                "timeout" => "timeout",
                _ => "disconnected",
            }
        );
        assert!(row["returned_rows"].is_null());
        assert!(!row.to_string().contains("private-marker-error"));
    }
    let _ = service.shutdown().await;
}
