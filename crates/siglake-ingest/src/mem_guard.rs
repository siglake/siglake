//! RSS memory circuit breaker (WS-8 ingest hardening).
//!
//! A background task samples this process's resident set size (RSS) from
//! `/proc/self/status` on a fixed interval and publishes it into an atomic.
//! The ingest handler reads that atomic on the hot path (one relaxed load, no
//! syscall) and sheds load with `503 Service Unavailable` + `Retry-After`
//! when RSS is over the configured limit. This caps the worst case where a
//! burst of large requests outruns the compactor's drain and drives the pod
//! into the OOM killer — a clean 503 lets clients retry instead.
//!
//! Linux-only (reads `/proc`); on other platforms the sampler reports 0 so the
//! breaker never trips. No new dependencies.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Default RSS sampling interval.
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

/// Shared RSS-vs-limit gate. Cheap to clone (it's an `Arc` internally at the
/// call sites that hold it). The current sample is updated out-of-band by
/// [`spawn_sampler`]; reads are a single relaxed atomic load.
#[derive(Debug)]
pub struct MemoryGuard {
    /// Most recent RSS sample, in bytes. 0 until the first sample lands (so the
    /// breaker is open — accepting — during startup).
    current_rss: AtomicU64,
    /// Hard limit in bytes; requests are shed when `current_rss >= limit`.
    limit_bytes: u64,
}

impl MemoryGuard {
    /// Build a guard with the given RSS ceiling (bytes). A `limit_bytes` of 0
    /// disables the breaker (it never trips).
    pub fn new(limit_bytes: u64) -> Self {
        Self {
            current_rss: AtomicU64::new(0),
            limit_bytes,
        }
    }

    /// True when the last RSS sample is at or above the configured limit.
    /// Always false when the limit is 0 (disabled).
    pub fn is_over_limit(&self) -> bool {
        self.limit_bytes != 0 && self.current_rss.load(Ordering::Relaxed) >= self.limit_bytes
    }

    /// The configured limit in bytes (0 ⇒ disabled).
    pub fn limit_bytes(&self) -> u64 {
        self.limit_bytes
    }

    /// Latest RSS sample in bytes (0 before the first sample).
    pub fn current_rss(&self) -> u64 {
        self.current_rss.load(Ordering::Relaxed)
    }

    /// Publish an RSS reading (bytes). Called by [`spawn_sampler`]; also useful
    /// for injecting a reading from an alternate source or in tests.
    pub fn observe(&self, rss_bytes: u64) {
        self.current_rss.store(rss_bytes, Ordering::Relaxed);
    }
}

/// Read this process's RSS in bytes from `/proc/self/status` (`VmRSS:` line,
/// reported in kB). Returns `None` on any platform/parse error — the caller
/// treats that as "no sample" and leaves the breaker open.
pub fn sample_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Spawn the background RSS sampler. It samples immediately, then every
/// `interval`, publishing each reading into `guard` and exporting the
/// `siglake_ingest_rss_bytes` gauge plus the `siglake_ingest_mem_breaker_open`
/// gauge (1 when shedding). A disabled guard (limit 0) still samples so the
/// RSS gauge stays populated for observability.
pub fn spawn_sampler(guard: Arc<MemoryGuard>, interval: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if let Some(rss) = sample_rss_bytes() {
                guard.observe(rss);
                metrics::gauge!("siglake_ingest_rss_bytes").set(rss as f64);
                metrics::gauge!("siglake_ingest_mem_breaker_open").set(if guard.is_over_limit() {
                    1.0
                } else {
                    0.0
                });
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_guard_never_trips() {
        let g = MemoryGuard::new(0);
        g.observe(1 << 40); // 1 TiB
        assert!(!g.is_over_limit());
    }

    #[test]
    fn trips_at_or_above_limit() {
        let g = MemoryGuard::new(100);
        assert!(!g.is_over_limit(), "no sample yet ⇒ open");
        g.observe(99);
        assert!(!g.is_over_limit());
        g.observe(100);
        assert!(g.is_over_limit(), "at limit ⇒ shed");
        g.observe(101);
        assert!(g.is_over_limit());
    }

    #[test]
    fn samples_own_rss_on_linux() {
        // On the Linux CI/dev host this process has a non-zero RSS; elsewhere
        // the sampler returns None and the breaker simply stays open.
        if let Some(rss) = sample_rss_bytes() {
            assert!(rss > 0, "a running process has non-zero RSS");
        }
    }
}
