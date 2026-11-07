//! A deadline on reading an ingest request body.
//!
//! Without one, a client can send most of a body and then stop, and the server
//! holds what it has buffered for as long as the connection stays open. For an
//! uncompressed body that costs the client every byte it makes the server hold.
//! For a gzip body it costs almost nothing: a few kilobytes on the wire hold up
//! to a body cap of decoded bytes, and a share of the gzip decode budget
//! (`content_encoding`), so a handful of stalled connections could hold that
//! budget and refuse every other gzip request.
//!
//! The deadline is a total, not an idle timeout. An idle timeout resets on
//! every chunk, so a client trickling a byte at a time would never trip it.
//! It starts once the request has passed the rate-limit and auth middlewares,
//! so a slow identity-provider lookup does not count against the client, and it
//! only matters while the body is being read: a handler that has its body, or
//! never reads one, is not timed. A request still reading its body at the
//! deadline gets `408`. `SIGLAKE_INGEST_BODY_READ_TIMEOUT_SECS` sets it
//! ([`DEFAULT_BODY_READ_TIMEOUT`] by default); `0` turns it off.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, BodyDataStream, Bytes};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::Stream;
use tokio::time::{Instant, Sleep};

use crate::{ingest_endpoint_label, ApiError};

/// How long a client has to send its request body when
/// `SIGLAKE_INGEST_BODY_READ_TIMEOUT_SECS` is unset. The Collector's exporters
/// time a whole request out at 30 seconds by default, so a body still arriving
/// at that point has already been given up on by the client that sent it.
pub const DEFAULT_BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// The body-read deadline [`crate::router`] applies; `None` turns it off.
#[derive(Clone, Copy)]
pub(crate) struct BodyReadTimeout(Option<Duration>);

impl BodyReadTimeout {
    /// From `SIGLAKE_INGEST_BODY_READ_TIMEOUT_SECS`: unset or unparseable means
    /// the default, `0` means no deadline.
    pub(crate) fn from_env() -> Self {
        match std::env::var("SIGLAKE_INGEST_BODY_READ_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
        {
            Some(0) => Self(None),
            Some(secs) => Self(Some(Duration::from_secs(secs))),
            None => Self(Some(DEFAULT_BODY_READ_TIMEOUT)),
        }
    }
}

/// Wrap the request body in the deadline, and answer `408` if it expired.
pub(crate) async fn body_read_timeout_middleware(
    State(timeout): State<BodyReadTimeout>,
    request: Request,
    next: Next,
) -> Response {
    let Some(limit) = timeout.0 else {
        return next.run(request).await;
    };
    let endpoint = ingest_endpoint_label(request.uri().path());
    let expired = Arc::new(AtomicBool::new(false));
    let (parts, body) = request.into_parts();
    let body = Body::from_stream(DeadlineBody {
        inner: body.into_data_stream(),
        deadline: Box::pin(tokio::time::sleep_until(Instant::now() + limit)),
        expired: expired.clone(),
        done: false,
    });
    let response = next.run(Request::from_parts(parts, body)).await;
    // The expired body failed the read, so the handler never ran and `response`
    // is the framework's `400`. Say what actually happened.
    if expired.load(Ordering::Relaxed) {
        metrics::counter!("siglake_ingest_requests_total",
            "endpoint" => endpoint, "status" => "408")
        .increment(1);
        return ApiError {
            status: StatusCode::REQUEST_TIMEOUT,
            msg: format!(
                "Request Timeout: the request body was not received within {}s",
                limit.as_secs()
            ),
            retry_after_secs: None,
        }
        .into_response();
    }
    response
}

/// A request body that fails once its deadline passes.
struct DeadlineBody {
    inner: BodyDataStream,
    deadline: Pin<Box<Sleep>>,
    expired: Arc<AtomicBool>,
    done: bool,
}

impl DeadlineBody {
    fn expire(&mut self) -> Poll<Option<std::io::Result<Bytes>>> {
        self.done = true;
        self.expired.store(true, Ordering::Relaxed);
        Poll::Ready(Some(Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "request body read timed out",
        ))))
    }
}

impl Stream for DeadlineBody {
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        // Checked first, so a body that always has a byte ready is still cut
        // off at the deadline.
        if this.deadline.is_elapsed() {
            return this.expire();
        }
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(chunk)) => Poll::Ready(Some(chunk.map_err(std::io::Error::other))),
            Poll::Ready(None) => {
                this.done = true;
                Poll::Ready(None)
            }
            // Register the deadline's wakeup alongside the body's.
            Poll::Pending => match this.deadline.as_mut().poll(cx) {
                Poll::Ready(()) => this.expire(),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}
