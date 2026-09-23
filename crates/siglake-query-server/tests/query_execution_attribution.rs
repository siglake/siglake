//! Every scan event a request produces names that request's execution.
//!
//! The scan's `siglake query scan reader tuning` and
//! `siglake query source partition profile` events are emitted from
//! DataFusion pumps that carry no request span, and the partition event can
//! be emitted after the handler has already logged `sql query profile` — an
//! NDJSON body streams after its handler returns, which is the case this file
//! pins. A reader grouping those events by position therefore hands them to
//! the next execution. `query_execution_id` is set on the session by the
//! handler and stamped on both scan events, so the join holds regardless.
//!
//! The worker endpoint is here for the same reason: `/api/v1/sql/shard` logs
//! no terminal line at all, so on a two-replica deployment a pod's log mixes
//! shard scans into whatever coordinator request happens to bracket them.
//! Its `query execution start` line is what names them.
//!
//! One test function in its own binary: the tracing subscriber is
//! process-global, and the test reads the whole captured log.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

const START: &str = "query execution start";
const SQL_PROFILE: &str = "sql query profile";
const TUNING: &str = "siglake query scan reader tuning";
const PARTITION_PROFILE: &str = "siglake query source partition profile";

const FILES: usize = 8;
const ROWS_PER_FILE: usize = 2_000;
const PREDICATE: &str = "host > 'host-ml' AND host < 'host-mn'";

// ---------------------------------------------------------------- capture

#[derive(Debug, Clone)]
struct Captured {
    seq: usize,
    message: String,
    execution_id: Option<u64>,
    endpoint: Option<String>,
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Captured>>>);

impl Recorder {
    fn all(&self) -> Vec<Captured> {
        self.0.lock().expect("recorder").clone()
    }
}

#[derive(Default)]
struct Visitor {
    message: String,
    execution_id: Option<u64>,
    endpoint: Option<String>,
}

impl tracing::field::Visit for Visitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "query_execution_id" {
            self.execution_id = Some(value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "message" => self.message = value.to_string(),
            "endpoint" => self.endpoint = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            // `endpoint` is a `&'static str` literal at most call sites, which
            // arrives here rather than through `record_str`.
            "endpoint" => self.endpoint = Some(format!("{value:?}").trim_matches('"').to_string()),
            _ => {}
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for Recorder {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let mut visitor = Visitor::default();
        event.record(&mut visitor);
        if ![START, SQL_PROFILE, TUNING, PARTITION_PROFILE].contains(&visitor.message.as_str()) {
            return;
        }
        self.0.lock().expect("recorder").push(Captured {
            seq: SEQ.fetch_add(1, Ordering::SeqCst),
            message: visitor.message,
            execution_id: visitor.execution_id,
            endpoint: visitor.endpoint,
        });
    }
}

// ---------------------------------------------------------------- fixture

async fn needle_table(ice: &IcebergContext) {
    for file in 0..FILES {
        let events: Vec<Event> = (0..ROWS_PER_FILE)
            .map(|i| {
                let mut e = Event::now(format!("row {file}-{i} payload"));
                e.host = if file == 0 && i < 4 {
                    "host-mm".to_string()
                } else {
                    format!("host-{}", (b'a' + (i % 26) as u8) as char)
                };
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
}

async fn post(app: &Router, uri: &str, body: serde_json::Value) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 64 * 1024 * 1024)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

/// The one `query execution start` line for `endpoint`, and the id it names.
fn started(log: &[Captured], endpoint: &str) -> (usize, u64) {
    let lines: Vec<&Captured> = log
        .iter()
        .filter(|e| e.message == START && e.endpoint.as_deref() == Some(endpoint))
        .collect();
    assert_eq!(
        lines.len(),
        1,
        "expected exactly one {endpoint} execution start: {lines:?}"
    );
    (
        lines[0].seq,
        lines[0].execution_id.expect("start line names an id"),
    )
}

fn of_execution<'a>(log: &'a [Captured], id: u64, message: &str) -> Vec<&'a Captured> {
    log.iter()
        .filter(|e| e.message == message && e.execution_id == Some(id))
        .collect()
}

// ------------------------------------------------------------------ test

#[tokio::test(flavor = "multi_thread")]
async fn scan_events_name_the_request_that_produced_them() {
    let recorder = Recorder::default();
    tracing_subscriber::registry().with(recorder.clone()).init();

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    needle_table(&ice).await;
    let app = router(AppState::new(Arc::new(ice), AuthConfig::open()));

    // 1. A buffered request: start line, terminal line and scan events all
    //    carry one id.
    let (status, _) = post(
        &app,
        "/api/v1/sql",
        serde_json::json!({ "query": format!("SELECT host, raw FROM events WHERE {PREDICATE}") }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // 2. The worker endpoint, which logs no terminal line: its scan events are
    //    attributable only through its own start line.
    let (status, _) = post(
        &app,
        "/api/v1/sql/shard",
        serde_json::json!({
            "query": format!("SELECT host, raw FROM events WHERE {PREDICATE}"),
            "shard": { "index": 0, "count": 1 },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // 3. A streaming request: the terminal line is logged when the handler
    //    returns, and the body — and therefore the partition profiles —
    //    follows it.
    let (status, ndjson) = post(
        &app,
        "/api/v1/sql",
        serde_json::json!({
            "query": format!("SELECT host, raw FROM events WHERE {PREDICATE}"),
            "format": "ndjson",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!ndjson.is_empty(), "the NDJSON body carried no rows");

    let log = recorder.all();
    let endpoints: HashMap<&str, usize> =
        log.iter()
            .filter(|e| e.message == START)
            .fold(HashMap::new(), |mut acc, e| {
                *acc.entry(e.endpoint.as_deref().unwrap_or("?")).or_default() += 1;
                acc
            });

    // The two `/api/v1/sql` requests both took the local path, so they are the
    // two `sql` start lines; the worker request is the one `sql_shard` line.
    assert_eq!(
        endpoints.get("sql_shard").copied(),
        Some(1),
        "worker start lines: {endpoints:?}"
    );
    assert_eq!(
        endpoints.get("sql").copied(),
        Some(2),
        "local start lines: {endpoints:?}"
    );

    let (_, shard_id) = started(&log, "sql_shard");
    let sql_ids: Vec<u64> = log
        .iter()
        .filter(|e| e.message == START && e.endpoint.as_deref() == Some("sql"))
        .map(|e| e.execution_id.expect("start line names an id"))
        .collect();
    let (buffered_id, streamed_id) = (sql_ids[0], sql_ids[1]);
    assert_ne!(buffered_id, streamed_id);
    assert_ne!(buffered_id, shard_id);

    for (label, id) in [
        ("buffered", buffered_id),
        ("worker shard", shard_id),
        ("streamed", streamed_id),
    ] {
        assert_eq!(
            of_execution(&log, id, TUNING).len(),
            1,
            "{label}: one scan node, one tuning event"
        );
        assert!(
            !of_execution(&log, id, PARTITION_PROFILE).is_empty(),
            "{label}: the scan executed, so it profiled its partitions"
        );
    }

    // The worker endpoint logs no `sql query profile`, so its scan events have
    // no terminal line to be positioned against — the start line is the only
    // thing that names them.
    assert!(
        of_execution(&log, shard_id, SQL_PROFILE).is_empty(),
        "the shard endpoint is not expected to log a terminal line"
    );

    // The streaming case: at least one partition profile is logged BELOW the
    // request's own terminal line, and still carries that request's id rather
    // than the next execution's.
    let streamed_terminal = of_execution(&log, streamed_id, SQL_PROFILE);
    assert_eq!(
        streamed_terminal.len(),
        1,
        "the streaming request logged its terminal line"
    );
    let boundary = streamed_terminal[0].seq;
    let late: Vec<&Captured> = of_execution(&log, streamed_id, PARTITION_PROFILE)
        .into_iter()
        .filter(|e| e.seq > boundary)
        .collect();
    assert!(
        !late.is_empty(),
        "no partition profile followed the streaming request's terminal line, \
         so this run does not exercise the late-event case"
    );
}
