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
