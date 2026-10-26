//! What `x-siglake-server-micros` measures that the handler's `total_ms` does
//! not (#7456).
//!
//! The header is stamped by a middleware that starts when the request HEAD has
//! been parsed and stops when the response leaves the handler stack. The
//! handler's clock starts later: the `Json<SqlRequest>` extractor reads the
//! request BODY off the socket first. So `server_micros - total_ms` is not
//! server compute — a client that sends its head and its body in separate
//! writes charges the gap between them to the header and to nothing else.
//!
//! This drives the server over a real loopback socket and splits the body
//! write from the head write by a known delay. An HTTP `100 Continue`
//! acknowledgement synchronizes the delay after the middleware has started.
//! If the gap were handler work the delay could not move it.
//!
//! One test function in its own binary: the tracing subscriber is
//! process-global and the test reads the whole captured log.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, SERVER_MICROS_HEADER};
use siglake_storage::iceberg::IcebergContext;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

const SQL_PROFILE: &str = "sql query profile";
/// Long enough to dwarf both the handler and the measurement noise, short
/// enough that the request's own 30s budget is untouched.
const BODY_DELAY: Duration = Duration::from_millis(250);

#[derive(Clone, Default)]
struct Totals(Arc<Mutex<Vec<f64>>>);

#[derive(Default)]
struct Visitor {
    message: String,
    total_ms: Option<f64>,
}

impl tracing::field::Visit for Visitor {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if field.name() == "total_ms" {
            self.total_ms = Some(value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for Totals {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = Visitor::default();
        event.record(&mut visitor);
        if visitor.message != SQL_PROFILE {
            return;
        }
        if let Some(total) = visitor.total_ms {
            self.0.lock().expect("totals").push(total);
        }
    }
}

/// Whether this environment allows binding a loopback listener. Restricted
/// sandboxes forbid it; the skip is loud so a green run there is visibly
/// partial.
fn loopback_available() -> bool {
    let ok = std::net::TcpListener::bind("127.0.0.1:0").map(drop).is_ok();
    if !ok {
        eprintln!("SKIP: loopback bind unavailable in this environment");
    }
    ok
}

/// POST the query, writing the body `delay` after the server acknowledges that
/// it is ready for it. Returns the `x-siglake-server-micros` the response
/// carries, in milliseconds.
fn post_split(addr: std::net::SocketAddr, query: &str, delay: Duration) -> f64 {
    let body = serde_json::json!({ "query": query }).to_string();
    let head = format!(
        "POST /api/v1/sql HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Expect: 100-continue\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.set_nodelay(true).expect("nodelay");
    stream.write_all(head.as_bytes()).expect("write head");
    stream.flush().expect("flush head");

    // Hyper sends this when the Json extractor polls the body. By then the
    // outer server-micros middleware has entered and started its clock. A
    // flush alone does not provide that ordering: the client can begin its
    // sleep a few microseconds before the server parses the request head.
    let mut reader = BufReader::new(stream);
    let mut status = String::new();
    reader.read_line(&mut status).expect("continue status line");
    assert!(
        status.starts_with("HTTP/1.1 100 Continue"),
        "continue status: {status}"
    );
    let mut end = String::new();
    reader.read_line(&mut end).expect("continue header end");
    assert_eq!(end, "\r\n", "continue response header end");

    std::thread::sleep(delay);
    reader
        .get_mut()
        .write_all(body.as_bytes())
        .expect("write body");
    reader.get_mut().flush().expect("flush body");

    status.clear();
    reader.read_line(&mut status).expect("status line");
    assert!(status.starts_with("HTTP/1.1 200"), "status: {status}");
    let mut server_micros = None;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).expect("header line");
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            if name.eq_ignore_ascii_case(SERVER_MICROS_HEADER) {
                server_micros = value.trim().parse::<f64>().ok();
            }
        }
    }
    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).expect("body");
    assert!(!rest.is_empty(), "the response carried no body");
    server_micros.expect("response carries the server-micros header") / 1000.0
}

async fn small_table(ice: &IcebergContext) {
    let events: Vec<Event> = (0..500)
        .map(|i| {
            let mut e = Event::now(format!("row {i} payload needle"));
            e.host = format!("host-{}", i % 7);
            e
        })
        .collect();
    ice.append_events(&events).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_micros_covers_request_body_receipt_and_total_ms_does_not() {
    if !loopback_available() {
        return;
    }
    let totals = Totals::default();
    tracing_subscriber::registry().with(totals.clone()).init();

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    small_table(&ice).await;
    let app = router(AppState::new(ice.clone(), AuthConfig::open()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Each request uses a distinct literal: an identical query is answered
    // from the result cache, which logs no terminal line.
    let prompt = tokio::task::spawn_blocking(move || {
        post_split(
            addr,
            "SELECT host, raw FROM events WHERE raw LIKE '%needle-a%' LIMIT 10",
            Duration::ZERO,
        )
    })
    .await
    .unwrap();
    let delayed = tokio::task::spawn_blocking(move || {
        post_split(
            addr,
            "SELECT host, raw FROM events WHERE raw LIKE '%needle-b%' LIMIT 10",
            BODY_DELAY,
        )
    })
    .await
    .unwrap();

    let totals = totals.0.lock().expect("totals").clone();
    assert_eq!(totals.len(), 2, "one terminal line per request: {totals:?}");
    let (prompt_total, delayed_total) = (totals[0], totals[1]);
    println!(
        "prompt: header {prompt:.3} ms, total {prompt_total:.3} ms, gap {:.3} ms\n\
         delayed ({BODY_DELAY:?}): header {delayed:.3} ms, total {delayed_total:.3} ms, gap {:.3} ms",
        prompt - prompt_total,
        delayed - delayed_total
    );

    // The handler's clock never sees the delay: it starts after the extractor
    // has the body.
    let delay_ms = BODY_DELAY.as_secs_f64() * 1000.0;
    assert!(
        delayed_total < delay_ms / 2.0,
        "the handler charged itself the client's body delay: {delayed_total:.3} ms"
    );
    // The header does, in full.
    assert!(
        delayed - delayed_total >= delay_ms,
        "the server header did not cover the body delay: header {delayed:.3} ms, \
         total {delayed_total:.3} ms"
    );
    // Without the delay the same gap is a fraction of it, so the term is the
    // client's writes rather than a fixed server cost.
    assert!(
        prompt - prompt_total < delay_ms / 2.0,
        "prompt gap {:.3} ms is not a transport term",
        prompt - prompt_total
    );

    server.abort();
}
