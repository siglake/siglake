//! Every millisecond a `/api/v1/sql` records request spends is named, and the
//! names nest the way the boundaries do (#7456).
//!
//! The 2026-10-26 matched round could not say where ~1.3-1.7 ms per request
//! went: the phases the harness reads (physical plan, collect, render) summed
//! to less than the handler's own `total_ms`, which summed to less than the
//! `x-siglake-server-micros` header, which summed to less than client wall
//! time. Subtracting independently aggregated medians cannot locate a
//! request's missing time, so this pins the relation PER REQUEST:
//!
//!   sum(phases) + unattributed == total_ms <= server header <= client wall
//!
//! The inner equality is the one that regresses silently: add a phase to the
//! handler without a timer and `unattributed_ms` absorbs it. The test asserts
//! the sum is exact to the handler's own arithmetic and that no phase overlaps
//! another (the sum never exceeds the total), and prints the measured split so
//! a reader can see which phase a change moved.
//!
//! Absolute durations are NOT asserted: this runs on whatever core the test
//! harness gets. One test function in its own binary, because the tracing
//! subscriber is process-global.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, SERVER_MICROS_HEADER};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

const SQL_PROFILE: &str = "sql query profile";
const EXECUTION_PROFILE: &str = "sql execution profile";

const FILES: usize = 6;
const ROWS_PER_FILE: usize = 3_000;
const REQUESTS: usize = 5;

/// Phases of the handler's terminal line, in the order the handler runs them.
/// Their sum plus `unattributed_ms` is `total_ms`.
const HANDLER_PHASES: &[&str] = &[
    "resolve_ms",
    "rewrite_ms",
    "buffer_delta_ms",
    "cache_probe_ms",
    "session_ms",
    "register_ms",
    "logical_plan_ms",
    "estimate_ms",
    "battery_ms",
    "admission_ms",
    "probe_ms",
    "execute_ms",
];

/// Phases of the execution line. Their sum is bounded by `execute_ms`, which
/// also covers the timeout wrapper and the handoff around them.
const EXECUTION_PHASES: &[&str] = &[
    "physical_plan_ms",
    "collect_ms",
    "plan_stats_ms",
    "render_ms",
    "cache_fill_ms",
    "plan_text_ms",
    "serialize_ms",
];

// ---------------------------------------------------------------- capture

#[derive(Debug, Clone, Default)]
struct Captured {
    message: String,
    execution_id: Option<u64>,
    endpoint: Option<String>,
    timings: HashMap<String, f64>,
}

#[derive(Clone, Default)]
struct Recorder {
    events: Arc<Mutex<Vec<Captured>>>,
}

impl Recorder {
    fn all(&self) -> Vec<Captured> {
        self.events.lock().expect("recorder").clone()
    }

    /// The one line of `message` belonging to `id`.
    fn line(&self, id: u64, message: &str) -> Captured {
        let all = self.all();
        let mut found: Vec<Captured> = all
            .into_iter()
            .filter(|e| e.message == message && e.execution_id == Some(id))
            .collect();
        assert_eq!(found.len(), 1, "one {message} for execution {id}");
        found.pop().expect("checked above")
    }
}

impl tracing::field::Visit for Captured {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.timings.insert(field.name().to_string(), value);
    }

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
        let mut captured = Captured::default();
        event.record(&mut captured);
        if ![SQL_PROFILE, EXECUTION_PROFILE].contains(&captured.message.as_str()) {
            return;
        }
        self.events.lock().expect("recorder").push(captured);
    }
}

// ---------------------------------------------------------------- fixture

async fn needle_table(ice: &IcebergContext) {
    for file in 0..FILES {
        let events: Vec<Event> = (0..ROWS_PER_FILE)
            .map(|i| {
                let mut e = Event::now(format!("row {file}-{i} payload needle-{}", i % 97));
                e.host = format!("host-{}", (b'a' + (i % 26) as u8) as char);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
}

struct Observed {
    wall_ms: f64,
    server_header_ms: f64,
}

async fn post_records(app: &Router, query: &str) -> Observed {
    let body = serde_json::json!({ "query": query });
    let started = Instant::now();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let server_header_ms = response
        .headers()
        .get(SERVER_MICROS_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<f64>().ok())
        .expect("every API response carries the server-micros header")
        / 1000.0;
    let bytes = to_bytes(response.into_body(), 64 * 1024 * 1024)
        .await
        .unwrap();
    // The body is already encoded when the header is stamped, so reading it
    // costs the client nothing the server did not already spend.
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(status, StatusCode::OK, "query failed: {query}");
    assert!(!bytes.is_empty());
    Observed {
        wall_ms,
        server_header_ms,
    }
}

fn sum(line: &Captured, phases: &[&str]) -> f64 {
    phases
        .iter()
        .map(|phase| {
            *line
                .timings
                .get(*phase)
                .unwrap_or_else(|| panic!("{} names {phase}: {:?}", line.message, line.timings))
        })
        .sum()
}

// ------------------------------------------------------------------ test

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_request_time_is_fully_attributed() {
    let recorder = Recorder::default();
    tracing_subscriber::registry().with(recorder.clone()).init();

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    needle_table(&ice).await;
    let app = router(AppState::new(ice.clone(), AuthConfig::open()));

    println!(
        "{:<4} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "req", "wall", "header", "total", "accounted", "unattr", "execute", "exec_sum"
    );
    for request in 0..REQUESTS {
        // A distinct literal per request: an identical query is answered from
        // the result cache, which executes nothing and logs no profile.
        let query = format!(
            "SELECT host, raw FROM events WHERE raw LIKE '%needle-{}%' LIMIT 100",
            request + 1
        );
        let before = recorder.all().len();
        let observed = post_records(&app, &query).await;

        let new_lines = recorder.all();
        let new_lines = &new_lines[before..];
        let terminal: Vec<&Captured> = new_lines
            .iter()
            .filter(|e| e.message == SQL_PROFILE)
            .collect();
        assert_eq!(
            terminal.len(),
            1,
            "one terminal line per request: {new_lines:?}"
        );
        let id = terminal[0]
            .execution_id
            .expect("the terminal line names its execution");
        let terminal = recorder.line(id, SQL_PROFILE);
        let execution = recorder.line(id, EXECUTION_PROFILE);

        let total = terminal.timings["total_ms"];
        let unattributed = terminal.timings["unattributed_ms"];
        let accounted = sum(&terminal, HANDLER_PHASES);
        let execute = terminal.timings["execute_ms"];
        let execution_sum = sum(&execution, EXECUTION_PHASES);
        println!(
            "{request:<4} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>9.3}",
            observed.wall_ms,
            observed.server_header_ms,
            total,
            accounted,
            unattributed,
            execute,
            execution_sum
        );
        for phase in HANDLER_PHASES {
            print!("  {phase}={:.3}", terminal.timings[*phase]);
        }
        for phase in EXECUTION_PHASES {
            print!("  {phase}={:.3}", execution.timings[*phase]);
        }
        println!();

        // 1. The handler's own arithmetic: every phase plus the residual it
        //    publishes IS the total it reports. Adding a phase without a timer
        //    cannot pass this silently -- it widens `unattributed_ms`, which
        //    the next assertion bounds.
        assert!(
            (accounted + unattributed - total).abs() < 0.05,
            "phases {accounted:.3} + unattributed {unattributed:.3} != total {total:.3}"
        );

        // 2. The phases are disjoint intervals of the request, so no sum of
        //    them can exceed it. Double-counting (the prelude charged both to
        //    the endpoint and to the local path, say) shows up here.
        assert!(
            accounted <= total + 0.05,
            "phases {accounted:.3} exceed total {total:.3}: a phase is counted twice"
        );
        assert!(
            unattributed >= 0.0,
            "unattributed must not be negative: {unattributed:.3}"
        );
        assert!(
            execution_sum <= execute + 0.05,
            "execution phases {execution_sum:.3} exceed execute_ms {execute:.3}"
        );

        // 3. The boundaries the round's attribution got wrong. Serialization
        //    runs inside the handler, so `total_ms` covers it; the header
        //    covers the handler plus the extractor, auth, audit and the layer
        //    stack; the client sees the header plus its own transport.
        assert!(
            total <= observed.server_header_ms,
            "total_ms {total:.3} exceeds the server header {:.3}",
            observed.server_header_ms
        );
        assert!(
            observed.server_header_ms <= observed.wall_ms,
            "server header {:.3} exceeds client wall {:.3}",
            observed.server_header_ms,
            observed.wall_ms
        );

        // 4. Serialization is measured where it happens. A records response
        //    with rows cannot encode in zero time, and if the timer were moved
        //    back outside `total_ms` this would read 0.
        assert!(
            execution.timings["serialize_ms"] > 0.0,
            "serialize_ms is measured on the records path"
        );
    }
}
