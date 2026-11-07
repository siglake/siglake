//! `Content-Encoding` on the OTLP/HTTP routes: gzip decoding, bounded.
//!
//! The OTLP specification requires every server to accept `gzip` and to limit
//! the body size *after* decompression. [`otlp_content_encoding_middleware`]
//! does the decoding. The size limit needs nothing here:
//! [`axum::extract::DefaultBodyLimit`] is enforced by the handler's `Bytes`
//! extractor, which wraps the body it is given, and that body is the decoded
//! stream. So the ingest body cap counts decoded bytes, and a body that inflates
//! past it is a `413` once the cap is reached.
//!
//! WHY NOT tower-http's `RequestDecompressionLayer`. Its gzip decoder stops at
//! the end of the first gzip member and reports EOF. RFC 1952 allows a gzip
//! stream of several members back to back, and pigz, Go's gzip writer and any
//! client that compresses chunks and concatenates them send exactly that. With
//! the first member taken as the whole body, everything after it was dropped
//! and the request still answered `200`: silent data loss. The decoder here
//! reads every member (`multiple_members`).
//!
//! WHY A BUDGET. Gzip makes a large decoded body cheap to send: a few kilobytes
//! on the wire inflate to the full body cap, held in memory until the handler is
//! done. The memory circuit breaker is off by default, runs in the handler only
//! after the body is read, and samples RSS on an interval, so a burst of small
//! gzip requests could inflate gigabytes before it trips. Two checks close that:
//!
//! - A tripped memory breaker refuses a gzip request before reading its body.
//! - [`GzipDecodeGate`] holds one decoded-byte budget per server
//!   (`SIGLAKE_INGEST_GZIP_DECODE_BUDGET_BYTES`,
//!   [`DEFAULT_GZIP_DECODE_BUDGET_BYTES`] by default). Every decoded chunk is
//!   charged to it before the handler sees the chunk, and a request's charge is
//!   returned when its handler returns, because the decoded bytes live that
//!   long. A chunk that does not fit fails its request with `503` and a short
//!   `Retry-After`, which OTLP exporters retry.
//!
//! The budget counts bytes, not requests, so the many small gzip requests of an
//! SDK fleet barely touch it, and only large inflations compete for it. It never
//! waits: a request blocked mid-body while holding part of the budget could
//! deadlock against others doing the same. What it bounds is decoded request
//! bytes; parsing adds a multiple of that (the JSON path copies the body, and
//! the decoded OTLP structs and Arrow batch follow), so size it at a fraction of
//! the pod's memory. Uncompressed requests are not charged: the client pays for
//! every byte it makes the server hold, as before gzip was accepted.
//!
//! A gzip body that arrives slowly holds its charge while it trickles in; the
//! body-read timeout (`body_timeout`) is what bounds how long.
//!
//! The middleware is a `route_layer` inside the rate-limit and auth middlewares,
//! so a request they refuse is never decoded. The gate reaches it as a request
//! extension that [`crate::router`] installs.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_compression::tokio::bufread::GzipDecoder;
use axum::body::{Body, Bytes};
use axum::extract::{Extension, Request};
use axum::http::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::Semaphore;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::mem_guard::MemoryGuard;
use crate::{ingest_endpoint_label, mem_breaker_response, ApiError};

/// The decoded-byte budget shared by in-flight gzip requests when
/// `SIGLAKE_INGEST_GZIP_DECODE_BUDGET_BYTES` is unset: 128 MiB, eight bodies at
/// the default 16 MiB cap. Parse overhead on top of it stays well inside the
/// chart's default 1 GiB ingester limit.
pub const DEFAULT_GZIP_DECODE_BUDGET_BYTES: usize = 128 * 1024 * 1024;

/// `Retry-After` on a budget refusal. Budget frees up as requests finish, which
/// is milliseconds, so the client is told to come back soon.
const GZIP_BUDGET_RETRY_AFTER_SECS: u64 = 1;

/// Size of the chunks the decoder hands to the budget, the body cap and the
/// handler. Also the most a request can be charged past the body cap before the
/// cap refuses it.
const DECODED_CHUNK_BYTES: usize = 64 * 1024;

/// What a gzip request must pass before and while it is decoded. One per
/// [`crate::router`].
#[derive(Clone)]
pub(crate) struct GzipDecodeGate {
    budget: Arc<Semaphore>,
    mem_guard: Option<Arc<MemoryGuard>>,
}

impl GzipDecodeGate {
    /// A gate with `budget_bytes` of decoded-byte budget. The budget is raised
    /// to one body cap plus a chunk if it is smaller: below that, a request at
    /// the cap could never fit, and would be told to retry forever.
    pub(crate) fn new(
        budget_bytes: usize,
        body_limit: usize,
        mem_guard: Option<Arc<MemoryGuard>>,
    ) -> Self {
        let floor = body_limit.saturating_add(DECODED_CHUNK_BYTES);
        if budget_bytes < floor {
            tracing::warn!(
                budget_bytes,
                body_limit,
                "gzip decode budget is below one body cap; raising it to fit one"
            );
        }
        let budget = budget_bytes.max(floor).min(Semaphore::MAX_PERMITS);
        Self {
            budget: Arc::new(Semaphore::new(budget)),
            mem_guard,
        }
    }

    /// The gate [`crate::router`] installs: the budget from
    /// `SIGLAKE_INGEST_GZIP_DECODE_BUDGET_BYTES` (unset, unparseable or `0`
    /// means the default) and the ingester's memory breaker.
    pub(crate) fn from_env(body_limit: usize, mem_guard: Option<Arc<MemoryGuard>>) -> Self {
        let budget_bytes = std::env::var("SIGLAKE_INGEST_GZIP_DECODE_BUDGET_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_GZIP_DECODE_BUDGET_BYTES);
        Self::new(budget_bytes, body_limit, mem_guard)
    }
}

/// One request's share of the budget. Returned in full when the last handle is
/// dropped: the middleware's, once the handler has returned.
struct Charge {
    budget: Arc<Semaphore>,
    held: AtomicUsize,
    exhausted: AtomicBool,
}

impl Charge {
    fn new(budget: Arc<Semaphore>) -> Self {
        Self {
            budget,
            held: AtomicUsize::new(0),
            exhausted: AtomicBool::new(false),
        }
    }

    /// Charge `bytes` to the budget, or record that it did not fit. Never waits.
    fn take(&self, bytes: usize) -> bool {
        if bytes == 0 {
            return true;
        }
        let taken = u32::try_from(bytes)
            .ok()
            .and_then(|n| self.budget.try_acquire_many(n).ok());
        match taken {
            Some(permit) => {
                permit.forget();
                self.held.fetch_add(bytes, Ordering::Relaxed);
                true
            }
            None => {
                self.exhausted.store(true, Ordering::Relaxed);
                false
            }
        }
    }

    fn exhausted(&self) -> bool {
        self.exhausted.load(Ordering::Relaxed)
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.add_permits(*self.held.get_mut());
    }
}

enum Coding {
    Identity,
    Gzip,
    Unsupported,
}

/// The request's content coding. Codings are case-insensitive and `x-gzip` is
/// `gzip` (RFC 9110 §8.4.1). More than one `Content-Encoding` header means
/// stacked codings, which are refused rather than half-decoded.
fn request_coding(headers: &HeaderMap) -> Coding {
    let mut values = headers.get_all(CONTENT_ENCODING).iter();
    let Some(value) = values.next() else {
        return Coding::Identity;
    };
    if values.next().is_some() {
        return Coding::Unsupported;
    }
    let Ok(value) = value.to_str() else {
        return Coding::Unsupported;
    };
    let value = value.trim();
    if value.eq_ignore_ascii_case("gzip") || value.eq_ignore_ascii_case("x-gzip") {
        Coding::Gzip
    } else if value.eq_ignore_ascii_case("identity") {
        Coding::Identity
    } else {
        Coding::Unsupported
    }
}

/// Decode a gzip request body for the OTLP routes; pass `identity` through;
/// refuse any other coding with `415` and `Accept-Encoding: gzip`, instead of a
/// misleading `400 invalid OTLP` from parsing compressed bytes.
pub(crate) async fn otlp_content_encoding_middleware(
    Extension(gate): Extension<GzipDecodeGate>,
    request: Request,
    next: Next,
) -> Response {
    match request_coding(request.headers()) {
        Coding::Identity => return next.run(request).await,
        Coding::Unsupported => return unsupported_coding_response(),
        Coding::Gzip => {}
    }
    let endpoint = ingest_endpoint_label(request.uri().path());
    if gate.mem_guard.as_ref().is_some_and(|g| g.is_over_limit()) {
        return mem_breaker_response(endpoint);
    }
    // Held until the handler returns: the decoded bytes live that long.
    let charge = Arc::new(Charge::new(gate.budget.clone()));
    let (mut parts, body) = request.into_parts();
    parts.headers.remove(CONTENT_ENCODING);
    parts.headers.remove(CONTENT_LENGTH);
    let request = Request::from_parts(parts, gunzip(body, charge.clone()));
    let response = next.run(request).await;
    // A chunk that did not fit failed the body read, so the handler never ran
    // and `response` is the framework's `400`. Say what actually happened.
    if charge.exhausted() {
        return budget_exhausted_response(endpoint);
    }
    response
}

/// A lazy gzip decoder over `body` that reads every gzip member and charges
/// each decoded chunk to `charge`. Bytes are inflated only as the handler reads
/// them, so the body cap stops a bomb at the cap. A corrupt or truncated stream
/// is a body error, which the `Bytes` extractor turns into a plain-text `400`.
fn gunzip(body: Body, charge: Arc<Charge>) -> Body {
    let compressed = StreamReader::new(body.into_data_stream().map_err(std::io::Error::other));
    let mut decoder = GzipDecoder::new(compressed);
    decoder.multiple_members(true);
    let decoded = ReaderStream::with_capacity(decoder, DECODED_CHUNK_BYTES).map(move |chunk| {
        chunk.and_then(|chunk: Bytes| {
            if charge.take(chunk.len()) {
                Ok(chunk)
            } else {
                Err(std::io::Error::other("gzip decode budget exhausted"))
            }
        })
    });
    Body::from_stream(decoded)
}

fn unsupported_coding_response() -> Response {
    let mut resp = StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    resp.headers_mut()
        .insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
    resp
}

/// Counted like the other load-shedding refusals:
/// `siglake_ingest_requests_total{status="503"}` is what the KEDA trigger and
/// the offered-load query read, and `siglake_ingest_gzip_budget_rejected_total`
/// tells this refusal apart from the breaker and backpressure ones.
fn budget_exhausted_response(endpoint: &'static str) -> Response {
    metrics::counter!("siglake_ingest_requests_total",
        "endpoint" => endpoint, "status" => "503")
    .increment(1);
    metrics::counter!("siglake_ingest_gzip_budget_rejected_total", "endpoint" => endpoint)
        .increment(1);
    ApiError::service_unavailable(
        GZIP_BUDGET_RETRY_AFTER_SECS,
        "Service Unavailable: the gzip decode budget is exhausted",
    )
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coding_of(values: &[&str]) -> &'static str {
        let mut headers = HeaderMap::new();
        for v in values {
            headers.append(CONTENT_ENCODING, HeaderValue::from_str(v).unwrap());
        }
        match request_coding(&headers) {
            Coding::Identity => "identity",
            Coding::Gzip => "gzip",
            Coding::Unsupported => "unsupported",
        }
    }

    #[test]
    fn codings_are_case_insensitive_and_stacked_codings_are_refused() {
        assert_eq!(coding_of(&[]), "identity");
        assert_eq!(coding_of(&["identity"]), "identity");
        assert_eq!(coding_of(&["Identity"]), "identity");
        for gzip in ["gzip", "GZIP", "Gzip", " gzip ", "x-gzip", "X-GZIP"] {
            assert_eq!(coding_of(&[gzip]), "gzip", "{gzip:?}");
        }
        for other in ["zstd", "deflate", "br", "snappy", "", "gzip, gzip"] {
            assert_eq!(coding_of(&[other]), "unsupported", "{other:?}");
        }
        assert_eq!(coding_of(&["gzip", "gzip"]), "unsupported");
    }

    /// A charge is returned in full when dropped, and a charge that does not fit
    /// takes nothing and is recorded.
    #[test]
    fn charges_return_to_the_budget_and_never_overdraw_it() {
        let budget = Arc::new(Semaphore::new(100));
        let a = Charge::new(budget.clone());
        assert!(a.take(60));
        let b = Charge::new(budget.clone());
        assert!(!b.take(41), "41 bytes do not fit in the 40 left");
        assert!(b.exhausted() && !a.exhausted());
        assert!(b.take(40));
        assert_eq!(budget.available_permits(), 0);
        drop(a);
        assert_eq!(budget.available_permits(), 60);
        drop(b);
        assert_eq!(budget.available_permits(), 100);
    }

    /// A budget below one body cap is raised to fit one, so a request at the
    /// cap is a `413` or lands, never an endless `503`.
    #[test]
    fn the_budget_always_fits_one_body_cap() {
        let gate = GzipDecodeGate::new(1, 1024 * 1024, None);
        assert_eq!(
            gate.budget.available_permits(),
            1024 * 1024 + DECODED_CHUNK_BYTES
        );
    }
}
