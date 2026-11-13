//! `Content-Encoding` on the OTLP/HTTP routes.
//!
//! The OTLP specification requires every server to accept `gzip` and to limit
//! the body size, "including after decompression".
//! [`otlp_content_encoding_middleware`] does the decoding, and the ingest body
//! cap applies on both sides of the decoder:
//!
//! - Decoded bytes: [`axum::extract::DefaultBodyLimit`] is enforced by the
//!   handler's `Bytes` extractor, which wraps the body it is given, and that body
//!   is the decoded stream. A body that inflates past the cap is a `413` once the
//!   cap is reached, without being inflated whole.
//! - Received bytes: before, the extractor's limit also bounded what the client
//!   sent, and now it no longer sees those bytes. Gzip can make a large body
//!   decode to almost nothing (empty members are 20 bytes each), so the
//!   compressed stream is counted against the same cap here, and is a `413`
//!   like any other body over it.
//!
//! WHY NOT tower-http's `RequestDecompressionLayer`. Its gzip decoder stops at
//! the end of the first gzip member and reports EOF. RFC 1952 allows a gzip
//! stream of several members back to back, and pigz, Go's gzip writer and any
//! client that compresses chunks and concatenates them send exactly that. With
//! the first member taken as the whole body, everything after it is dropped and
//! the request still answers `200`: silent data loss. The decoder here reads
//! every member (`multiple_members`).
//!
//! A body that fails while the handler's `Bytes` extractor reads it gets the
//! framework's plain-text `400`. When the failure is the decoder's, the
//! middleware answers instead with the JSON error envelope every other `400` on
//! these routes uses, counted the same way. A failure of the connection itself
//! is left as the framework reports it, as for an uncompressed body.
//!
//! The middleware is a `route_layer` inside the rate-limit and auth middlewares,
//! so a request they refuse is never decoded.

use std::sync::{Arc, OnceLock};

use async_compression::tokio::bufread::GzipDecoder;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::ApiError;

/// Size of the chunks the decoder hands to the body cap and the handler.
const DECODED_CHUNK_BYTES: usize = 64 * 1024;

/// The body the framework sends with its own `413`, matched so a compressed
/// body over the cap gets the same answer as an uncompressed one.
const LENGTH_LIMIT_BODY: &str = "Failed to buffer the request body: length limit exceeded";

/// An OTLP route's `siglake_ingest_requests_total` labels, the ones its handler
/// counts under, so a refusal the middleware makes is counted alike.
#[derive(Clone, Copy)]
pub(crate) struct OtlpRoute {
    endpoint: &'static str,
    signal: Option<&'static str>,
}

impl OtlpRoute {
    pub(crate) const LOGS: Self = Self {
        endpoint: "otlp",
        signal: None,
    };
    pub(crate) const TRACES: Self = Self {
        endpoint: "otlp_traces",
        signal: Some("traces"),
    };

    fn count(self, status: &'static str) {
        let endpoint = self.endpoint;
        match self.signal {
            Some(signal) => metrics::counter!("siglake_ingest_requests_total",
                "endpoint" => endpoint, "status" => status, "signal" => signal)
            .increment(1),
            None => metrics::counter!("siglake_ingest_requests_total",
                "endpoint" => endpoint, "status" => status)
            .increment(1),
        }
    }
}

/// The middleware's state: which route it guards, and the ingest body cap.
#[derive(Clone, Copy)]
pub(crate) struct OtlpDecoding {
    pub(crate) route: OtlpRoute,
    pub(crate) body_limit: usize,
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

/// Why a gzip body failed to read. The first failure is the one recorded: a
/// failure before the decoder also fails the decoder, and is not its fault.
enum Failure {
    /// Reading the body from the client failed.
    Connection,
    /// The compressed body is larger than the ingest body cap.
    TooLarge,
    /// The bytes are not a valid gzip stream.
    Decode(String),
}

/// Decode a gzip request body for the OTLP routes; pass `identity` through;
/// refuse any other coding with `415` and `Accept-Encoding: gzip`, instead of a
/// misleading `400 invalid OTLP` from parsing compressed bytes.
pub(crate) async fn otlp_content_encoding_middleware(
    State(OtlpDecoding { route, body_limit }): State<OtlpDecoding>,
    request: Request,
    next: Next,
) -> Response {
    match request_coding(request.headers()) {
        Coding::Identity => return next.run(request).await,
        Coding::Unsupported => {
            route.count("415");
            return unsupported_coding_response();
        }
        Coding::Gzip => {}
    }
    let (mut parts, body) = request.into_parts();
    // The handler sees the decoded body, so the wire headers no longer apply.
    parts.headers.remove(CONTENT_ENCODING);
    parts.headers.remove(CONTENT_LENGTH);
    let failure = Arc::new(OnceLock::new());
    let request = Request::from_parts(parts, gunzip(body, body_limit, failure.clone()));
    let response = next.run(request).await;
    // A failure failed the body read, so the handler never ran and `response`
    // is the framework's plain-text `400`.
    match failure.get() {
        Some(Failure::Decode(e)) => {
            route.count("400");
            ApiError::bad_request(format!("invalid gzip body: {e}")).into_response()
        }
        Some(Failure::TooLarge) => {
            (StatusCode::PAYLOAD_TOO_LARGE, LENGTH_LIMIT_BODY).into_response()
        }
        Some(Failure::Connection) | None => response,
    }
}

/// A lazy gzip decoder over `body` that reads every gzip member. Bytes are
/// inflated only as the handler reads them, so the body cap stops a bomb at
/// the cap. The compressed bytes are counted against `body_limit` on the way
/// in. Why a body failed is recorded in `failure`; the body cap on decoded
/// bytes is enforced outside this stream, so it never records one.
fn gunzip(body: Body, body_limit: usize, failure: Arc<OnceLock<Failure>>) -> Body {
    let mut received = 0usize;
    let wire_failure = failure.clone();
    let compressed = body.into_data_stream().map(move |chunk| {
        let chunk = chunk.map_err(|e| {
            let _ = wire_failure.set(Failure::Connection);
            std::io::Error::other(e)
        })?;
        received = received.saturating_add(chunk.len());
        if received > body_limit {
            let _ = wire_failure.set(Failure::TooLarge);
            return Err(std::io::Error::other(
                "compressed body exceeds the ingest body cap",
            ));
        }
        Ok::<Bytes, std::io::Error>(chunk)
    });
    let mut decoder = GzipDecoder::new(StreamReader::new(compressed));
    decoder.multiple_members(true);
    let decoded = ReaderStream::with_capacity(decoder, DECODED_CHUNK_BYTES).inspect(move |chunk| {
        if let Err(e) = chunk {
            // A no-op when a failure before the decoder was recorded first.
            let _ = failure.set(Failure::Decode(e.to_string()));
        }
    });
    Body::from_stream(decoded)
}

fn unsupported_coding_response() -> Response {
    let mut resp = StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    resp.headers_mut()
        .insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
    resp
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
}
