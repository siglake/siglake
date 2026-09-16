//! Integration test for `siglake_ingest::rate_limit::RedisRateBudget`.
//!
//! Gated behind `#[ignore]` because it needs a real Redis listening
//! on `REDIS_URL` (default `redis://localhost:6379/0`). Run with:
//!
//!     # in one terminal
//!     docker run --rm -p 6379:6379 redis:7-alpine
//!
//!     # in another
//!     cargo test -p siglake-ingest --test ingest redis_rate_budget:: -- --ignored
//!
//! What the test covers (because the in-binary unit tests can't
//! exercise the Lua script without a running Redis):
//!
//! - Cold key: first `burst` requests get `Allowed`.
//! - Subsequent request hits `Throttled` with a sane
//!   `retry_after_secs`.
//! - Different keys are independent.
//! - The fail-open path on an unreachable Redis is in the doctest
//!   for the impl itself — here we focus on happy-path math.

use std::sync::Arc;

use siglake_ingest::rate_limit::{RateBudget, RateOutcome, RedisRateBudget};

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/0".to_string())
}

fn unique_prefix() -> String {
    // Per-test prefix so concurrent CI runs don't share state.
    format!("siglake-test:{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test]
#[ignore]
async fn cold_burst_then_throttle() {
    let prefix = unique_prefix();
    let rb: Arc<dyn RateBudget> = Arc::new(
        RedisRateBudget::connect(&redis_url(), 1.0, 3.0, Some(&prefix))
            .await
            .expect("connect"),
    );
    for i in 0..3 {
        assert_eq!(
            rb.try_acquire("key-a").await,
            RateOutcome::Allowed,
            "burst slot {i}"
        );
    }
    match rb.try_acquire("key-a").await {
        RateOutcome::Throttled { retry_after_secs } => assert!(retry_after_secs >= 1),
        other => panic!("expected Throttled, got {other:?}"),
    }
}

#[tokio::test]
#[ignore]
async fn per_key_independence_via_redis() {
    let prefix = unique_prefix();
    let rb: Arc<dyn RateBudget> = Arc::new(
        RedisRateBudget::connect(&redis_url(), 1.0, 2.0, Some(&prefix))
            .await
            .expect("connect"),
    );
    assert_eq!(rb.try_acquire("alpha").await, RateOutcome::Allowed);
    assert_eq!(rb.try_acquire("alpha").await, RateOutcome::Allowed);
    // alpha exhausted; beta independent.
    assert_eq!(rb.try_acquire("beta").await, RateOutcome::Allowed);
    assert!(matches!(
        rb.try_acquire("alpha").await,
        RateOutcome::Throttled { .. }
    ));
}
