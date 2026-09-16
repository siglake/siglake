//! Query-audit table tests: accepted interactive and batch audit rows persist
//! with the expected fields, while bounded best-effort submission stays out of
//! query responses.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::Utc;
use datafusion::arrow::array::Array;
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value;
use tokio::sync::Notify;
use tower::util::ServiceExt;

use siglake_core::Event;
use siglake_query_server::{
    router, AppState, AuditAppender, AuditLimits, AuditService, AuthConfig, JobStore,
};
use siglake_storage::iceberg::IcebergContext;

struct Server {
    app: Router,
    ice: Arc<IcebergContext>,
    _tmp: tempfile::TempDir,
    audit_handle: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.audit_handle.abort();
    }
}

async fn spawn() -> Server {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    // Seed a few events so the cost estimator has something to surface.
    let events: Vec<Event> = (0..20)
        .map(|i| Event {
            timestamp: Utc::now(),
            host: format!("h{}", i % 4),
            source: "audit-test".into(),
            sourcetype: "t".into(),
            index: "main".into(),
            raw: format!("e{i}"),
            attributes: None,
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    // Audit writer: flush on every submit so the test doesn't wait.
    let (service, writer) = AuditService::new(ice.clone(), 1, Duration::from_millis(50));
    let audit_handle = tokio::spawn(service.run());

    let state = AppState::new(ice.clone(), AuthConfig::open())
        .with_jobs(JobStore::new(2, Duration::from_secs(5)))
        .with_audit(writer);
    let app = router(state);
    Server {
        app,
        ice,
        _tmp: tmp,
        audit_handle,
    }
}

async fn request_json(
    app: &Router,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Option<Value>) {
    let mut builder = Request::builder().method(method).uri(path);
    let body = if let Some(body) = body {
        builder = builder.header(axum::http::header::CONTENT_TYPE, "application/json");
        Body::from(serde_json::to_vec(&body).unwrap())
    } else {
        Body::empty()
    };

    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    if bytes.is_empty() {
        (status, None)
    } else {
        (status, Some(serde_json::from_slice(&bytes).unwrap()))
    }
}

/// Wait until the audit table has at least `min` rows. Polls every
/// 50ms up to `timeout`.
async fn await_audit_rows(ice: &IcebergContext, min: usize, timeout: Duration) -> usize {
    use datafusion::prelude::SessionContext;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let ctx = SessionContext::new();
        ice.register_query_audit_with_datafusion(&ctx)
            .await
            .unwrap();
        let df = ctx
            .sql("SELECT count(*) AS n FROM query_audit")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        if let Some(b) = batches.first() {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            let n = col.value(0) as usize;
            if n >= min {
                return n;
            }
        }
        if std::time::Instant::now() > deadline {
            return 0;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(Debug, Default)]
struct HeldFailingAppender {
    calls: AtomicUsize,
    appended_rows: AtomicUsize,
    first_started: Notify,
    release_first: Notify,
}

#[async_trait]
impl AuditAppender for HeldFailingAppender {
    async fn append_query_audit(&self, batch: RecordBatch) -> Result<usize> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            self.first_started.notify_one();
            self.release_first.notified().await;
            anyhow::bail!("injected audit append failure");
        }
        let rows = batch.num_rows();
        self.appended_rows.fetch_add(rows, Ordering::AcqRel);
        Ok(rows)
    }
}

async fn await_released(writer: &siglake_query_server::AuditWriter) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if writer.retained_usage().rows == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("audit accounting did not release");
}

/// A stalled append holds one charged row while later requests keep their HTTP
/// answers. Saturation and an individually oversized SQL string are refused as
/// whole audit rows; append failure, recovery, and worker cancellation release
/// their reservations.
#[tokio::test]
async fn stalled_append_bounds_retention_without_changing_query_responses() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let appender = Arc::new(HeldFailingAppender::default());
    let limits = AuditLimits {
        max_rows: 2,
        max_bytes: 4 * 1024,
    };
    let (service, writer) =
        AuditService::with_limits(appender.clone(), 1, Duration::from_secs(60), limits);
    let app = router(AppState::new(ice.clone(), AuthConfig::open()).with_audit(writer.clone()));
    let handle = tokio::spawn(service.run());

    let (status, _) = request_json(
        &app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({"query": ""})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    appender.first_started.notified().await;

    for _ in 0..20 {
        let (status, _) = request_json(
            &app,
            Method::POST,
            "/api/v1/sql",
            Some(serde_json::json!({"query": ""})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    let large_query = " ".repeat(16 * 1024);
    let (status, _) = request_json(
        &app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({"query": large_query})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let retained = writer.retained_usage();
    assert_eq!(retained.rows, limits.max_rows);
    assert!(retained.bytes <= limits.max_bytes);

    appender.release_first.notify_one();
    await_released(&writer).await;

    let (status, _) = request_json(
        &app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({"query": ""})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    await_released(&writer).await;
    assert!(appender.appended_rows.load(Ordering::Acquire) >= 2);

    handle.abort();
    let _ = handle.await;
    assert_eq!(writer.retained_usage().rows, 0);

    let shutdown_appender = Arc::new(HeldFailingAppender::default());
    let (shutdown_service, shutdown_writer) = AuditService::with_limits(
        shutdown_appender.clone(),
        1,
        Duration::from_secs(60),
        limits,
    );
    let shutdown_app =
        router(AppState::new(ice, AuthConfig::open()).with_audit(shutdown_writer.clone()));
    let shutdown_handle = tokio::spawn(shutdown_service.run());
    let (status, _) = request_json(
        &shutdown_app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({"query": ""})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    shutdown_appender.first_started.notified().await;
    assert_eq!(shutdown_writer.retained_usage().rows, 1);
    shutdown_handle.abort();
    let _ = shutdown_handle.await;
    await_released(&shutdown_writer).await;
}

/// The same stall, with nobody to release it: the append deadline is what
/// returns the charged row and lets the worker write the next one, and the
/// query responses either side of it are the ones they always were.
#[tokio::test]
async fn an_append_past_its_deadline_frees_the_audit_path_without_changing_responses() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let appender = Arc::new(HeldFailingAppender::default());
    let limits = AuditLimits {
        max_rows: 2,
        max_bytes: 4 * 1024,
    };
    let (service, writer) =
        AuditService::with_limits(appender.clone(), 1, Duration::from_secs(60), limits);
    let app = router(AppState::new(ice, AuthConfig::open()).with_audit(writer.clone()));
    let handle = tokio::spawn(
        service
            .with_append_deadline(Duration::from_millis(200))
            .run(),
    );

    let (status, _) = request_json(
        &app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({"query": ""})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    appender.first_started.notified().await;
    assert_eq!(writer.retained_usage().rows, 1);

    // `release_first` is never notified: the deadline, and nothing else,
    // returns this capacity.
    await_released(&writer).await;

    let (status, _) = request_json(
        &app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({"query": ""})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    tokio::time::timeout(Duration::from_secs(5), async {
        while appender.appended_rows.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the worker never appended after the deadline");
    assert_eq!(
        appender.appended_rows.load(Ordering::Acquire),
        1,
        "the abandoned batch was re-appended"
    );

    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn successful_sql_query_lands_audit_row() {
    let srv = spawn().await;
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({"query": "SELECT count(*) AS n FROM events"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let rows = await_audit_rows(&srv.ice, 1, Duration::from_secs(3)).await;
    assert!(rows >= 1, "expected >=1 audit row, got {rows}");

    // Read the row back and verify fields.
    let ctx = datafusion::prelude::SessionContext::new();
    srv.ice
        .register_query_audit_with_datafusion(&ctx)
        .await
        .unwrap();
    let df = ctx
        .sql(
            "SELECT subject, endpoint, priority, status, complexity, \
             estimated_rows_processed FROM query_audit ORDER BY timestamp DESC LIMIT 1",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let b = &batches[0];
    let subject = b
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .value(0);
    let endpoint = b
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .value(0);
    let priority = b
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .value(0);
    let status = b
        .column(3)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .value(0);
    let complexity = b
        .column(4)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let est_rows = b
        .column(5)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap();

    assert_eq!(subject, "anonymous");
    assert_eq!(endpoint, "sql");
    assert_eq!(priority, "interactive");
    assert_eq!(status, "succeeded");
    assert!(!complexity.is_null(0));
    assert_eq!(complexity.value(0), "small");
    assert_eq!(est_rows.value(0), 20);
}

#[tokio::test]
async fn rejected_query_lands_audit_row_with_error() {
    let srv = spawn().await;
    let (status, _) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({"query": ""})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let rows = await_audit_rows(&srv.ice, 1, Duration::from_secs(3)).await;
    assert!(rows >= 1);

    let ctx = datafusion::prelude::SessionContext::new();
    srv.ice
        .register_query_audit_with_datafusion(&ctx)
        .await
        .unwrap();
    let df = ctx
        .sql("SELECT status, error FROM query_audit ORDER BY timestamp DESC LIMIT 1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let b = &batches[0];
    let status = b
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .value(0);
    let error = b
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(status, "rejected");
    assert!(!error.is_null(0));
    assert!(error.value(0).contains("empty"));
}

#[tokio::test]
async fn batch_job_lands_audit_row_on_terminal() {
    let srv = spawn().await;
    // Submit a batch job.
    let (status, body) = request_json(
        &srv.app,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query":    "SELECT count(*) FROM events",
            "priority": "batch"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let body = body.unwrap();
    let job_id = body["job_id"].as_str().unwrap().to_string();
    // Wait for the job to terminate.
    for _ in 0..50 {
        let (_, status) = request_json(
            &srv.app,
            Method::GET,
            &format!("/api/v1/jobs/{job_id}"),
            None,
        )
        .await;
        if status.unwrap()["status"] == "succeeded" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let rows = await_audit_rows(&srv.ice, 1, Duration::from_secs(3)).await;
    assert!(rows >= 1);

    let ctx = datafusion::prelude::SessionContext::new();
    srv.ice
        .register_query_audit_with_datafusion(&ctx)
        .await
        .unwrap();
    let df = ctx
        .sql(
            "SELECT priority, status FROM query_audit \
             WHERE priority = 'batch' ORDER BY timestamp DESC LIMIT 1",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let b = &batches[0];
    assert_eq!(b.num_rows(), 1);
    let priority = b
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .value(0);
    let status = b
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(priority, "batch");
    assert_eq!(status, "succeeded");
}
