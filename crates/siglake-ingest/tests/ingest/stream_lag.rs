//! A subscriber that falls behind loses events, not the subscription.
//!
//! WHAT THIS PINS. `GET /api/v1/stream` subscribes to a bounded
//! `broadcast::Sender` of `STREAM_BROADCAST_CAPACITY` events. When a subscriber
//! falls further behind than that, `recv()` returns `RecvError::Lagged(n)` and
//! the handler (`crates/siglake-ingest/src/lib.rs`, `get_stream`) counts `n` on
//! `siglake_ingest_stream_lagged_total`, emits one SSE frame named `lagged`
//! whose data is `n`, and keeps looping on the same receiver.
//!
//! That contract is stated in three places -- the constant's doc comment, the
//! operation description and the 200 response description -- and until this
//! file nothing tested it. Task #1779 existed precisely because two of those
//! three had drifted from the handler and no test noticed.
//!
//! HOW THE OVERRUN IS FORCED. The route is driven with `oneshot`, so the
//! response body is a stream this test polls by hand: between the subscribe and
//! the first poll the receiver cannot advance, however fast the machine is.
//! Publishing `STREAM_BROADCAST_CAPACITY + 5` events into that window drops
//! exactly the first 5. Nothing here depends on socket buffering or on a sleep.
//!
//! HOW THE COUNTER IS OBSERVED. `metrics::set_global_recorder` is process-wide
//! and this binary runs its tests concurrently, so a global `DebuggingRecorder`
//! would see every other test's metrics (and only one test could install one).
//! `set_default_local_recorder` is thread-local instead: `#[tokio::test]` is a
//! current-thread runtime and nothing below spawns, so the poll that drives the
//! handler's `stream!` -- and therefore the `counter!` in its `Lagged` arm --
//! runs on the very thread the guard covers. No serial lock, no shared state,
//! and the snapshot contains this test's metrics only.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, BodyDataStream};
use axum::http::{Request, StatusCode};
use futures::StreamExt;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use tokio::sync::{broadcast, Mutex};
use tower::ServiceExt;

use siglake_core::Event;
use siglake_ingest::{
    router, AppState, StreamedEvent, TenantRouting, TenantWalRouter, STREAM_BROADCAST_CAPACITY,
};
use siglake_wal::WalWriter;

/// Events published beyond the channel's capacity before the subscriber is
/// first polled -- i.e. the number the subscriber must be told it lost.
const OVERRUN: u64 = 5;

const TENANT: &str = "tenant-a";

fn streaming_state(root: &std::path::Path) -> (AppState, broadcast::Sender<StreamedEvent>) {
    let tenants = TenantWalRouter::new(root, "test", 5, Duration::from_secs(60));
    let writer = WalWriter::with_thresholds(root, "test", 5, Duration::from_secs(60)).unwrap();
    let (tx, _) = broadcast::channel(STREAM_BROADCAST_CAPACITY);
    let state = AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(writer)),
        tenants: Some(Arc::new(tenants)),
        allowed_tenants: None,
        tenant_routing: TenantRouting::TrustHeader,
        max_tenants: 0,
        tenant_admission: Default::default(),
        backpressure: None,
        events_tx: Some(tx.clone()),
        tokens: None,
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: Duration::from_secs(30),
        remote_wal_drain: true,
    };
    (state, tx)
}

/// Reassembles SSE frames from the response body. Frames are split on the blank
/// line that terminates them rather than on chunk boundaries, and keep-alive
/// comments (`:\n\n`) are skipped so a slow machine cannot turn one into a
/// failure.
struct Frames {
    body: BodyDataStream,
    buf: String,
}

impl Frames {
    fn new(body: Body) -> Self {
        Self {
            body: body.into_data_stream(),
            buf: String::new(),
        }
    }

    async fn next(&mut self) -> String {
        let frame = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(end) = self.buf.find("\n\n") {
                    let frame = self.buf[..end].to_string();
                    self.buf.drain(..end + 2);
                    if frame.starts_with(':') {
                        continue; // keep-alive
                    }
                    return frame;
                }
                let chunk = self
                    .body
                    .next()
                    .await
                    .expect("stream ended: the subscription did not survive")
                    .expect("stream error");
                self.buf.push_str(&String::from_utf8_lossy(&chunk));
            }
        })
        .await;
        frame.expect("timed out waiting for the next SSE frame")
    }
}

/// The value of an SSE field line, e.g. `field(frame, "event")`.
fn field<'a>(frame: &'a str, name: &str) -> Option<&'a str> {
    frame
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
}

/// The `raw` of the event carried by a data frame.
fn raw_of(frame: &str) -> String {
    let data = field(frame, "data").unwrap_or_else(|| panic!("frame has no data line:\n{frame}"));
    let json: serde_json::Value = serde_json::from_str(data).expect("data is not JSON");
    json["raw"].as_str().expect("event has no raw").to_string()
}

/// The total of one counter in a snapshot, or `None` if that key was never
/// touched. Take a single snapshot: the debugging recorder drains on each call.
fn counter_total(snap: metrics_util::debugging::Snapshot, name: &str) -> Option<u64> {
    snap.into_vec()
        .into_iter()
        .filter(|(k, _, _, _)| k.key().name() == name)
        .map(|(_, _, _, v)| match v {
            DebugValue::Counter(c) => c,
            other => panic!("{name} is not a counter: {other:?}"),
        })
        .reduce(|a, b| a + b)
}

fn event(raw: &str) -> StreamedEvent {
    StreamedEvent {
        tenant: TENANT.to_string(),
        event: Event::now(raw),
    }
}

#[tokio::test]
async fn a_lagging_subscriber_is_told_how_many_it_lost_and_stays_subscribed() {
    let tmp = tempfile::tempdir().unwrap();
    let (state, tx) = streaming_state(tmp.path());

    let resp = router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/stream")
                .header("X-Scope-OrgID", TENANT)
                .header("accept", "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // The handler subscribes before it returns the response, so the overrun
    // below cannot land in front of the receiver.
    assert_eq!(tx.receiver_count(), 1, "handler did not subscribe");

    // Overrun. The body has not been polled once, so the receiver is still at
    // the position it was created at and loses exactly the first OVERRUN.
    let sent = STREAM_BROADCAST_CAPACITY as u64 + OVERRUN;
    for i in 0..sent {
        tx.send(event(&format!("evt-{i}"))).unwrap();
    }

    let mut frames = Frames::new(resp.into_body());

    // (a) the shape of the lag signal: a `lagged` event whose data is the
    // number of dropped events, as a bare integer -- and, from the same arm of
    // the same overrun, the counter that alerting reads.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let lagged = frames.next().await;
    let lagged_total = counter_total(snapshotter.snapshot(), "siglake_ingest_stream_lagged_total");
    drop(guard);

    assert_eq!(
        field(&lagged, "event"),
        Some("lagged"),
        "first frame is not the lag signal:\n{lagged}"
    );
    assert_eq!(
        field(&lagged, "data"),
        Some(OVERRUN.to_string().as_str()),
        "lagged data is not the number of dropped events:\n{lagged}"
    );
    // The delta is the whole point: a handler that incremented by 1 per lag
    // event instead of by `n` would still emit the frame above, and would
    // under-report a 5,000-event drop as a single one.
    assert_eq!(
        lagged_total,
        Some(OVERRUN),
        "siglake_ingest_stream_lagged_total did not move by the number of \
         dropped events (None = the counter was never touched)"
    );

    // (b) the subscription survives. Everything still buffered is delivered,
    // starting at the first event that was not dropped...
    for i in OVERRUN..sent {
        let frame = frames.next().await;
        assert_eq!(
            raw_of(&frame),
            format!("evt-{i}"),
            "delivery did not resume in order after the lag"
        );
    }

    // ...and an event published after the lag reaches the same subscriber.
    tx.send(event("after-the-lag")).unwrap();
    let frame = frames.next().await;
    assert_eq!(raw_of(&frame), "after-the-lag");
}
