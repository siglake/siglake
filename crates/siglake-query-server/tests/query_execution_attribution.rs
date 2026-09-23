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
const EXECUTION_PROFILE: &str = "sql execution profile";
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

/// How long a start event waits for its partner before giving up.
const OVERLAP_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone, Default)]
struct Recorder {
    events: Arc<Mutex<Vec<Captured>>>,
    sql_starts: Arc<AtomicUsize>,
}

impl Recorder {
    fn with_overlapping_sql_starts() -> Self {
        Self::default()
    }

    /// Hold each of the first two `sql` start events until both have arrived,
    /// so the two executions are in flight together and a reader grouping scan
    /// events by position would mix them.
    ///
    /// The overlap is REQUIRED, not decorative: the two requests run the same
    /// SQL, so a second one that starts after the first has finished is
    /// answered from the result cache, executes nothing, and logs no start
    /// line at all. `SIGLAKE_QUERY_RESULT_CACHE` is a process-wide `OnceLock`
    /// over the environment and tests do not write the environment, and
    /// `IcebergTuning::result_caches` does not reach this path — the SQL
    /// handler asks the free function. Holding both requests at their start
    /// events is what keeps the second one a real execution.
    ///
    /// BOUNDED all the same, because this blocks the thread it runs on. As an
    /// unbounded `std::sync::Barrier::wait` on a worker thread it deadlocked
    /// 10 of 240 runs under load — first start parked in `futex_do_wait`, the
    /// other worker asleep in `ep_poll` still holding the second request, no
    /// second start line, no CPU consumed — and took a CI gate down for 85
    /// minutes. The caller now drives each request from its own thread, so
    /// this wait cannot starve the runtime; the bound is what turns a
    /// pathological case into a failed assertion rather than a hung gate.
    fn await_overlap(&self) {
        let deadline = std::time::Instant::now() + OVERLAP_WAIT;
        while self.sql_starts.load(Ordering::SeqCst) < 2 {
            if std::time::Instant::now() >= deadline {
                eprintln!(
                    "OVERLAP TIMEOUT after {OVERLAP_WAIT:?}: {} sql start(s) arrived",
                    self.sql_starts.load(Ordering::SeqCst)
                );
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    fn all(&self) -> Vec<Captured> {
        self.events.lock().expect("recorder").clone()
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
        if ![
            START,
            SQL_PROFILE,
            EXECUTION_PROFILE,
            TUNING,
            PARTITION_PROFILE,
        ]
        .contains(&visitor.message.as_str())
        {
            return;
        }
        if visitor.message == START && visitor.endpoint.as_deref() == Some("sql") {
            let index = self.sql_starts.fetch_add(1, Ordering::SeqCst);
            if index < 2 {
                self.await_overlap();
            }
        }
        self.events.lock().expect("recorder").push(Captured {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_events_name_the_request_that_produced_them() {
    let recorder = Recorder::with_overlapping_sql_starts();
    tracing_subscriber::registry().with(recorder.clone()).init();

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    needle_table(&ice).await;
    let app = router(AppState::new(Arc::new(ice), AuthConfig::open()));

    // 1. Two buffered requests overlap: their start, execution-profile and
    //    terminal lines must still form two exact id joins. The recorder holds
    //    each request at its start event until the other has reached its own.
    let buffered_query =
        serde_json::json!({ "query": format!("SELECT host, raw FROM events WHERE {PREDICATE}") });
    //
    //    ONE OS THREAD PER REQUEST, not `tokio::spawn`. The hold blocks the
    //    thread the start event is logged on, and a blocked worker polls
    //    nothing else: with both requests spawned onto a two-worker runtime,
    //    the first one's hold left the second sitting in a run queue that only
    //    a work-steal could empty, and when the steal did not happen the pair
    //    never formed. That deadlocked 10 of 240 runs under load and hung a CI
    //    gate for 85 minutes. Driving each request from its own thread through
    //    the runtime handle keeps both workers free, so the second request
    //    always reaches its start event and releases the first.
    let drive = |query: serde_json::Value| {
        let handle = tokio::runtime::Handle::current();
        let app = app.clone();
        std::thread::spawn(move || handle.block_on(post(&app, "/api/v1/sql", query)))
    };
    let first = drive(buffered_query.clone());
    let second = drive(buffered_query);
    let (first_status, _) = first.join().expect("buffered request thread");
    let (second_status, _) = second.join().expect("buffered request thread");
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);

    let buffered_log = recorder.all();
    let buffered_ids: Vec<u64> = buffered_log
        .iter()
        .filter(|e| e.message == START && e.endpoint.as_deref() == Some("sql"))
        .map(|e| e.execution_id.expect("start line names an id"))
        .collect();
    assert_eq!(buffered_ids.len(), 2, "buffered starts: {buffered_log:?}");
    assert_ne!(buffered_ids[0], buffered_ids[1]);
    for id in &buffered_ids {
        assert_eq!(
            of_execution(&buffered_log, *id, EXECUTION_PROFILE).len(),
            1,
            "buffered execution {id} logged its execution profile"
        );
        assert_eq!(
            of_execution(&buffered_log, *id, SQL_PROFILE).len(),
            1,
            "buffered execution {id} logged its terminal profile"
        );
    }

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

    // All three `/api/v1/sql` requests took the local path; the worker request
    // is the one `sql_shard` line.
    assert_eq!(
        endpoints.get("sql_shard").copied(),
        Some(1),
        "worker start lines: {endpoints:?}"
    );
    assert_eq!(
        endpoints.get("sql").copied(),
        Some(3),
        "local start lines: {endpoints:?}"
    );

    let (_, shard_id) = started(&log, "sql_shard");
    let sql_ids: Vec<u64> = log
        .iter()
        .filter(|e| e.message == START && e.endpoint.as_deref() == Some("sql"))
        .map(|e| e.execution_id.expect("start line names an id"))
        .collect();
    let streamed_id = *sql_ids.last().expect("streamed execution start");
    assert_eq!(&sql_ids[..2], buffered_ids.as_slice());
    assert!(!buffered_ids.contains(&streamed_id));
    assert!(!buffered_ids.contains(&shard_id));

    for (label, id) in buffered_ids
        .iter()
        .copied()
        .map(|id| ("buffered", id))
        .chain([("worker shard", shard_id), ("streamed", streamed_id)])
    {
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
