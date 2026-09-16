//! Per-query limit resolution + enforcement.
//!
//! Two layers:
//!
//! 1. **Server ceilings** ([`TierLimits`]). Set at startup via env /
//!    CLI; never bypassable by request. One set per priority.
//! 2. **Per-request overrides** ([`RequestLimits`]). The caller can ask
//!    for a *tighter* limit but never a looser one — the resolver
//!    clamps against the ceiling.
//!
//! Resolved limits drive three enforcement points:
//!
//! - **Pre-flight rejection**: if the cost estimate exceeds the
//!   resolved `max_bytes_scanned`, return 400 with the cost report and
//!   no execution. Saves the cluster from work nobody approved.
//! - **Wall-clock timeout**: `tokio::time::timeout` wraps the
//!   DataFusion collect. On expiry the stream is dropped, the query
//!   is cancelled at the next yield point, and the response is 504.
//! - **Row cap**: enforced inside the format layer (see
//!   [`crate::format`]).
//!
//! v0 intentionally does **not** poll `ExecutionPlan::metrics()` for
//! mid-flight bytes-scanned enforcement — the pre-flight estimate +
//! wall-clock timeout catch the common failure modes. The
//! `max_bytes_scanned` field stays in the API so the contract is
//! stable; document any mid-flight gap.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Per-tier ceilings + defaults. Server-side only; never overridable.
#[derive(Debug, Clone)]
pub struct TierLimits {
    pub default_timeout: Duration,
    pub ceiling_timeout: Duration,
    pub ceiling_bytes_scanned: u64,
    /// Rows returned when the request does not ask for a number.
    ///
    /// Split out from the ceiling because they were the same value, so an
    /// UNSPECIFIED request resolved to the MAXIMUM — the wrong default
    /// direction, and the one the timeout pair above already gets right. On the
    /// batch tier that meant a plain `SELECT raw FROM events` asked for a
    /// billion rows and got them collected into memory.
    ///
    /// A caller who genuinely wants more still asks for it and is clamped by
    /// the ceiling.
    pub default_rows_returned: usize,
    pub ceiling_rows_returned: usize,
    /// Mid-flight cap on rows pulled out of the table scan(s). The
    /// query is aborted at the next batch boundary once this is
    /// exceeded. Catches the "estimator was wrong" case where a
    /// query passed pre-flight but ends up scanning far more than
    /// expected.
    pub ceiling_rows_scanned: usize,
}

impl TierLimits {
    pub fn interactive_defaults() -> Self {
        Self {
            default_timeout: Duration::from_secs(60),
            ceiling_timeout: Duration::from_secs(300),
            ceiling_bytes_scanned: 100 * 1024 * 1024 * 1024, // 100 GB
            // Unchanged behaviour for interactive: it already defaulted to this,
            // and the injected `ORDER BY ... LIMIT` keeps real results far below.
            default_rows_returned: 10_000_000,
            ceiling_rows_returned: 10_000_000,
            ceiling_rows_scanned: 100_000_000,
        }
    }

    pub fn batch_defaults() -> Self {
        Self {
            default_timeout: Duration::from_secs(3600),
            ceiling_timeout: Duration::from_secs(21_600), // 6 h
            ceiling_bytes_scanned: 10 * 1024 * 1024 * 1024 * 1024, // 10 TB
            // NOT the ceiling. The batch result is collected into memory and
            // rendered to a JSON body held in the job store, so a billion rows
            // was never servable — it just failed by OOM-killing the pod rather
            // than by refusing. One million is itself generous for that render
            // (~2 GB of `serde_json::Value` at ~200 B/row); it is chosen to
            // match what the tier can plausibly hold, and results larger than
            // this want streaming to object storage rather than a bigger number.
            //
            // 100k, not 1M: `batches_to_records` still holds the collected Arrow
            // result and its `serde_json::Value` tree, and axum subsequently
            // builds the response buffer. The Arrow-to-Value conversion itself
            // uses only one encoded row of scratch, but the two full render
            // forms remain generous at a million ~200-byte rows on the chart's
            // 4Gi pod.
            default_rows_returned: 100_000,
            ceiling_rows_returned: 1_000_000_000,
            ceiling_rows_scanned: 10_000_000_000,
        }
    }
}

/// Per-request limit overrides. Every field optional; missing fields
/// fall back to the tier defaults.
///
/// UNKNOWN KEYS ARE REJECTED (HTTP 422), because every field here is a safety
/// constraint the caller asked for and silently dropping a misspelt one is the
/// worst available outcome. `{"limits":{"max_rows":5000}}` is refused before
/// the query is planned, executed or enqueued, rather than answered 200 with
/// the tier default (10,000,000 interactive / 100,000 batch) quietly applied.
/// A known field over the tier ceiling is still clamped, not refused, and an
/// omitted or empty `limits` still means "tier defaults".
///
/// Mind the asymmetry: the REQUEST field is `max_rows_returned`; the response
/// envelope reports the cap it applied as `max_rows`. There is deliberately no
/// alias for the response spelling — one request name, and a wrong one is
/// loud.
///
/// Only this object is strict. The enclosing request body still ignores
/// unknown keys.
// The strictness landed after the docs site named the response spelling as the
// request field for long enough that clients were plausibly sending it (task
// #2179, siglake-docs #2177). 422 rather than 400 is not a choice made here:
// these handlers take `Json<SqlRequest>` and pinned axum 0.8.9 maps a
// deserialization data error to `UNPROCESSABLE_ENTITY`, with its own plain-text
// body rather than an `ApiErrorBody`. That boundary is left exactly as it was.
#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestLimits {
    pub timeout_seconds: Option<u64>,
    pub max_bytes_scanned: Option<u64>,
    pub max_rows_returned: Option<usize>,
    pub max_rows_scanned: Option<usize>,
    /// `false` bypasses the wall-clock timeout but **does not** lift
    /// the bytes-scanned or row-returned ceilings. Default `true`.
    pub circuit_breakers: Option<bool>,
}

/// Query priority — picks which tier's defaults + ceilings apply.
#[derive(Debug, Copy, Clone, Default, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    #[default]
    Interactive,
    Batch,
}

impl Priority {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Batch => "batch",
        }
    }
}

/// Resolved limits after clamping the request against the server
/// ceilings. The actual values the enforcement points read.
#[derive(Debug, Clone)]
pub struct ResolvedLimits {
    pub priority: Priority,
    pub timeout: Duration,
    pub max_bytes_scanned: u64,
    pub max_rows_returned: usize,
    pub max_rows_scanned: usize,
    pub circuit_breakers: bool,
}

impl ResolvedLimits {
    pub fn resolve(req: &RequestLimits, priority: Priority, tier: &TierLimits) -> Self {
        let timeout = req
            .timeout_seconds
            .map(Duration::from_secs)
            .unwrap_or(tier.default_timeout)
            .min(tier.ceiling_timeout);
        let max_bytes_scanned = req
            .max_bytes_scanned
            .unwrap_or(tier.ceiling_bytes_scanned)
            .min(tier.ceiling_bytes_scanned);
        let max_rows_returned = req
            .max_rows_returned
            .unwrap_or(tier.default_rows_returned)
            .min(tier.ceiling_rows_returned);
        let max_rows_scanned = req
            .max_rows_scanned
            .unwrap_or(tier.ceiling_rows_scanned)
            .min(tier.ceiling_rows_scanned);
        let circuit_breakers = req.circuit_breakers.unwrap_or(true);
        Self {
            priority,
            timeout,
            max_bytes_scanned,
            max_rows_returned,
            max_rows_scanned,
            circuit_breakers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_when_request_is_empty() {
        let tier = TierLimits::interactive_defaults();
        let req = RequestLimits::default();
        let r = ResolvedLimits::resolve(&req, Priority::Interactive, &tier);
        assert_eq!(r.timeout, tier.default_timeout);
        assert_eq!(r.max_bytes_scanned, tier.ceiling_bytes_scanned);
        assert!(r.circuit_breakers);
    }

    #[test]
    fn request_can_tighten_but_never_loosen() {
        let tier = TierLimits::interactive_defaults();
        let req = RequestLimits {
            timeout_seconds: Some(3600),         // > ceiling → clamped
            max_bytes_scanned: Some(1),          // < ceiling → kept
            max_rows_returned: Some(usize::MAX), // > ceiling → clamped
            max_rows_scanned: None,
            circuit_breakers: None,
        };
        let r = ResolvedLimits::resolve(&req, Priority::Interactive, &tier);
        assert_eq!(r.timeout, tier.ceiling_timeout);
        assert_eq!(r.max_bytes_scanned, 1);
        assert_eq!(r.max_rows_returned, tier.ceiling_rows_returned);
    }

    /// An UNSPECIFIED request must not resolve to the maximum.
    ///
    /// THE DEFECT. `max_rows_returned` defaulted to `ceiling_rows_returned`, so
    /// a batch request that asked for nothing asked for a BILLION rows — and the
    /// batch path collects its whole result into memory before truncating, skips
    /// the default-order `LIMIT` rewrite, and (until this change) passed no
    /// mid-flight rows-scanned cap. One request,
    /// `{"priority":"batch","query":"SELECT raw FROM events"}`, against a
    /// two-billion-row table OOM-killed the pod. No concurrency, and `priority`
    /// is an unauthenticated `#[serde(default)]` body field.
    ///
    /// The timeout pair on this same struct always had it right — a default
    /// separate from a ceiling. Rows and bytes did not.
    #[test]
    fn an_unspecified_request_gets_the_default_not_the_ceiling() {
        let tier = TierLimits::batch_defaults();
        let r = ResolvedLimits::resolve(&RequestLimits::default(), Priority::Batch, &tier);
        assert_eq!(
            r.max_rows_returned, tier.default_rows_returned,
            "an empty request must resolve to the DEFAULT"
        );
        assert!(
            r.max_rows_returned < tier.ceiling_rows_returned,
            "default and ceiling are the same value again ({}); an unspecified \
             request is asking for the maximum",
            r.max_rows_returned
        );
        // Small enough that the collected Arrow rows, the `serde_json::Value`
        // tree and axum's response buffer fit a 4Gi pod. The conversion scratch
        // is bounded to one encoded row; the remaining full-size forms are what
        // a larger default would have to fit.
        assert!(
            r.max_rows_returned <= 100_000,
            "batch default {} is too large to render on the packaged 4Gi pod",
            r.max_rows_returned
        );
    }

    /// A caller who explicitly wants more can still have it, up to the ceiling —
    /// the point is the DIRECTION of the default, not a hard cap.
    #[test]
    fn an_explicit_request_can_still_reach_the_ceiling() {
        let tier = TierLimits::batch_defaults();
        let req = RequestLimits {
            max_rows_returned: Some(usize::MAX),
            ..RequestLimits::default()
        };
        let r = ResolvedLimits::resolve(&req, Priority::Batch, &tier);
        assert_eq!(r.max_rows_returned, tier.ceiling_rows_returned);
    }

    #[test]
    fn batch_tier_has_longer_default_timeout() {
        let i = TierLimits::interactive_defaults();
        let b = TierLimits::batch_defaults();
        assert!(b.default_timeout > i.default_timeout);
        assert!(b.ceiling_timeout > i.ceiling_timeout);
    }

    fn parse(json: &str) -> Result<RequestLimits, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// THE DEFECT. `max_rows` is the `RecordsResponse` spelling and was for a
    /// while the spelling the docs site gave for the REQUEST. It parsed to an
    /// empty `RequestLimits`, so the caller's row cap vanished and the tier
    /// default applied with no signal.
    #[test]
    fn the_documented_misspelling_of_the_row_cap_is_rejected() {
        let err = parse(r#"{"max_rows":5000}"#).expect_err("max_rows must not parse");
        let message = err.to_string();
        assert!(
            message.contains("unknown field") && message.contains("max_rows"),
            "the error must name the offending key, got: {message}"
        );
        assert!(
            message.contains("max_rows_returned"),
            "the error must offer the canonical spelling, got: {message}"
        );
    }

    #[test]
    fn any_other_unknown_key_is_rejected() {
        let err = parse(r#"{"timeout_secs":30}"#).expect_err("timeout_secs must not parse");
        assert!(err.to_string().contains("unknown field"));
    }

    /// The dangerous shape: the typo rides ALONGSIDE a valid limit, so the
    /// request looks accepted and partially honoured. It is refused whole.
    #[test]
    fn an_unknown_key_beside_a_valid_limit_is_rejected() {
        let err = parse(r#"{"timeout_seconds":30,"max_rows":5000}"#)
            .expect_err("a valid neighbour must not rescue the typo");
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn the_canonical_row_cap_still_parses() {
        let req = parse(r#"{"max_rows_returned":5000}"#).unwrap();
        assert_eq!(req.max_rows_returned, Some(5000));
        let tier = TierLimits::interactive_defaults();
        let r = ResolvedLimits::resolve(&req, Priority::Interactive, &tier);
        assert_eq!(r.max_rows_returned, 5000);
    }

    #[test]
    fn every_known_limit_field_still_parses() {
        let req = parse(
            r#"{"timeout_seconds":30,"max_bytes_scanned":1024,
                "max_rows_returned":10,"max_rows_scanned":20,
                "circuit_breakers":false}"#,
        )
        .unwrap();
        assert_eq!(req.timeout_seconds, Some(30));
        assert_eq!(req.max_bytes_scanned, Some(1024));
        assert_eq!(req.max_rows_returned, Some(10));
        assert_eq!(req.max_rows_scanned, Some(20));
        assert_eq!(req.circuit_breakers, Some(false));
    }

    /// Strictness is about SPELLING, not about requiring fields: an empty
    /// object still means "tier defaults".
    #[test]
    fn an_empty_limits_object_still_parses_to_the_defaults() {
        let req = parse("{}").unwrap();
        let tier = TierLimits::batch_defaults();
        let r = ResolvedLimits::resolve(&req, Priority::Batch, &tier);
        assert_eq!(r.max_rows_returned, tier.default_rows_returned);
        assert_eq!(r.timeout, tier.default_timeout);
        assert!(r.circuit_breakers);
    }

    /// A known field is still clamped, not trusted.
    #[test]
    fn a_known_field_over_the_ceiling_is_still_clamped_not_rejected() {
        let req = parse(r#"{"timeout_seconds":100000}"#).unwrap();
        let tier = TierLimits::interactive_defaults();
        let r = ResolvedLimits::resolve(&req, Priority::Interactive, &tier);
        assert_eq!(r.timeout, tier.ceiling_timeout);
    }
}
