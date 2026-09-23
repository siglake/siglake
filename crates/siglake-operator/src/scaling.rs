//! Pure scaling-decision logic.
//!
//! Given a snapshot of a [`SiglakeCluster`]'s spec + the observed
//! metric values + the current replica counts, compute the desired
//! replica counts for each component.
//!
//! Kept deliberately small and dependency-free: this module is the
//! testable core of the operator. The kube-rs reconciler in
//! [`crate::reconciler`] is the thin shell that calls this function,
//! diffs against the live state, and applies the change.
//!
//! All three components use standard HPA-style proportional scaling with a
//! 10% deadband: the reading names a pod count, we clamp it to `[min, max]`,
//! and we refuse to move if that count is within `1 ± deadband` of the current
//! one. The reconciler rejects invalid policies before calling here.
//!
//! What the reading names depends on the signal, which is what [`Load`]
//! carries. Ingest rate and in-flight queries are per pod, so N pods do N times
//! the reading and `desired = ceil(current * observed / target)`. The
//! catalog-claim compactor backlog is one queue that every worker reports in
//! full, so `desired = ceil(observed / target)` and the replica divisor is
//! applied exactly once.
//!
//! Query used to be pinned to a fixed `min == max` because its `--query-peers`
//! list was rendered from the replica count. Since #967 the pods discover each
//! other through the headless Service's SRV record, so query takes the
//! ordinary decision on its in-flight signal and a replica the decision adds
//! receives shard work as soon as it is Ready.

use crate::crd::{ComponentAutoscale, SiglakeClusterSpec};

/// Per-component metric snapshot the reconciler pulls from
/// Prometheus on each cycle.
#[derive(Clone, Debug, Default)]
pub struct ObservedMetrics {
    /// Ingest requests per second, *per pod* (already averaged by the
    /// metrics adapter). 0.0 means "no traffic": an unusable reading — a query
    /// error, an empty vector, a non-finite sample — never reaches this struct.
    /// It travels as the absent observation of [`fold_observation`], which the
    /// reconciler turns into [`replicas_without_signal`].
    pub ingester_rps_per_pod: f64,
    /// Sealed-segment backlog gauge. NOT per pod in claim mode: `peek_pending`
    /// counts the whole sealed queue with no worker filter, so every compactor
    /// replica publishes the same total and the query reads that total once.
    /// [`Load`] is what says which of the two it is; the filesystem drain runs
    /// at one replica, where the distinction has no effect.
    pub compactor_backlog: f64,
    /// In-flight queries, *per pod*.
    pub query_in_flight_per_pod: f64,
}

/// One cycle's readings, PER SIGNAL: `None` is "this component has no usable
/// reading", which puts that component — and only that component — on
/// [`replicas_without_signal`].
///
/// The all-or-nothing shape this replaced took the whole snapshot down on the
/// first bad query, so a compactor whose series had gone absent froze ingest
/// and query sizing with it (#6011, `docs/DESIGN_compactor_wakeup_signal.md`).
/// A whole Prometheus outage is the case where all three are `None`, which is
/// byte-for-byte the old behaviour.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ObservedSamples {
    pub ingester_rps_per_pod: Option<f64>,
    pub compactor_backlog: Option<f64>,
    pub query_in_flight_per_pod: Option<f64>,
}

impl ObservedSamples {
    /// No component has a reading — a Prometheus outage rather than one stale
    /// series.
    pub fn all_absent(&self) -> bool {
        self.ingester_rps_per_pod.is_none()
            && self.compactor_backlog.is_none()
            && self.query_in_flight_per_pod.is_none()
    }
}

impl From<ObservedMetrics> for ObservedSamples {
    fn from(m: ObservedMetrics) -> Self {
        Self {
            ingester_rps_per_pod: Some(m.ingester_rps_per_pod),
            compactor_backlog: Some(m.compactor_backlog),
            query_in_flight_per_pod: Some(m.query_in_flight_per_pod),
        }
    }
}

impl From<&ObservedMetrics> for ObservedSamples {
    fn from(m: &ObservedMetrics) -> Self {
        m.clone().into()
    }
}

/// `None` is the whole-outage reading: every signal absent.
impl From<Option<ObservedMetrics>> for ObservedSamples {
    fn from(m: Option<ObservedMetrics>) -> Self {
        m.map(Into::into).unwrap_or_default()
    }
}

impl From<&ObservedSamples> for ObservedSamples {
    fn from(s: &ObservedSamples) -> Self {
        s.clone()
    }
}

/// How a component's observed reading relates to one replica.
///
/// Both arms divide by `target` and clamp to `[min, max]`; they differ in
/// whether the current replica count multiplies the result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Load {
    /// One pod's share of the work. Adding a replica lowers the reading, so the
    /// work the fleet is doing is `current × observed` and the count that
    /// reaches `target` is `current × observed / target`.
    PerPod,
    /// One queue every replica reports in full. Adding a replica does not lower
    /// the reading — the workers share the queue — so multiplying by the current
    /// count counts the same backlog once per pod: a fixed backlog then asks for
    /// more workers at every cycle until the ceiling stops it (#3692). The count
    /// that reaches `target` is `observed / target`, whatever `current` is.
    Shared,
}

/// Which arithmetic the compactor backlog takes.
///
/// Read from the same policy-fixed predicate that selects the drain protocol
/// ([`crate::render::uses_catalog_claim`]), never from the current replica
/// count: the ownership protocol is fixed for the life of the policy, and so is
/// the meaning of the gauge it publishes. A filesystem-drain policy caps at one
/// replica, where `current` is 0 or 1 and both arms agree.
pub fn compactor_load(policy: &ComponentAutoscale) -> Load {
    if crate::render::uses_catalog_claim(policy) {
        Load::Shared
    } else {
        Load::PerPod
    }
}

#[derive(Clone, Debug, Default)]
pub struct CurrentReplicas {
    pub ingester: i32,
    pub compactor: i32,
    pub query: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesiredReplicas {
    pub ingester: i32,
    pub compactor: i32,
    pub query: i32,
}

/// Standard HPA deadband: don't move if the ratio is within ±10% of
/// 1.0. Avoids flapping at the boundary.
pub const SCALE_DEADBAND: f64 = 0.10;

/// The desired size of each tier, one component at a time.
///
/// A component with a reading takes the ordinary proportional decision; a
/// component whose reading is absent takes [`replicas_without_signal`] on its
/// own, leaving the other two untouched. Takes anything convertible into
/// [`ObservedSamples`], so a caller holding a complete [`ObservedMetrics`] —
/// every test that predates the per-signal split — passes it directly.
pub fn reconcile_replicas(
    spec: &SiglakeClusterSpec,
    observed: impl Into<ObservedSamples>,
    current: &CurrentReplicas,
) -> DesiredReplicas {
    let observed = observed.into();
    let sized =
        |policy: &ComponentAutoscale, sample: Option<f64>, current: i32, load: Load| match sample {
            Some(value) => decide(policy, value, current, load),
            None => replicas_without_signal(policy, current),
        };
    DesiredReplicas {
        ingester: sized(
            &spec.autoscaling.ingester,
            observed.ingester_rps_per_pod,
            current.ingester,
            Load::PerPod,
        ),
        compactor: sized(
            &spec.autoscaling.compactor,
            observed.compactor_backlog,
            current.compactor,
            compactor_load(&spec.autoscaling.compactor),
        ),
        query: sized(
            &spec.autoscaling.query,
            observed.query_in_flight_per_pod,
            current.query,
            Load::PerPod,
        ),
    }
}

/// A tier's size when its load signal is unusable (Prometheus down, the scrape
/// empty, or this component's series absent). A cold tier starts at `min` —
/// but never below ONE — while an existing tier holds its size unless it is
/// outside the configured range. This converges every tier up to its floor
/// without scaling down a running fleet blindly.
///
/// A ZERO-FLOOR TIER WITH NO READING IS RESTORED TO ONE REPLICA, not held at
/// zero. Returning `policy.min` there would read a monitoring outage as
/// idleness and leave the tier stopped for as long as the signal was missing,
/// with backlog accumulating unobserved — the exact failure the surrounding
/// code exists to prevent. One pod is the bounded cost, and it republishes
/// `siglake_compactor_sealed_pending`, which the activation query falls back
/// to, so the tier can still size itself while the independent signal is
/// missing (#6011). The positive-floor arm is unchanged.
pub(crate) fn replicas_without_signal(policy: &ComponentAutoscale, current: i32) -> i32 {
    if current <= 0 {
        policy.min.max(1)
    } else {
        current.clamp(policy.min, policy.max.max(policy.min))
    }
}

/// How long a zero-floor tier may stay parked before the operator runs it
/// anyway, and how long that wake lasts.
///
/// Retention, delete tasks, orphan disposal, claim reclaim and the
/// `sync_mirror_to_catalog` recovery sweep are all compactor-resident, and at
/// zero replicas none of them run. The sweep is the only repair for a mirror
/// object whose registration was abandoned — a segment that is durable,
/// unregistered, and therefore invisible to the catalog-depth signal that
/// would otherwise ask for a worker.
///
/// An hour bounds that repair delay to the same order as the mirror sync
/// interval while keeping the tier off for most of an idle day. The wake holds
/// ten minutes so the sweeps that run on their own cadences inside the pod —
/// claim reclaim and mirror sync, both a minute by default — come round
/// several times before the tier is allowed to park again.
pub const MAINTENANCE_WAKE_AFTER: std::time::Duration = std::time::Duration::from_secs(3600);
pub const MAINTENANCE_WAKE_HOLD: std::time::Duration = std::time::Duration::from_secs(600);

/// Where a zero-floor tier is in the park / maintenance-wake cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZeroFloorPhase {
    /// Parked at zero since this instant.
    Parked { since: std::time::Instant },
    /// Woken for maintenance, held at one replica until this instant whatever
    /// the reading says.
    Waking { until: std::time::Instant },
}

/// Bound how long a tier stays at zero.
///
/// Takes the decision the reading asked for and returns the one to apply,
/// with the phase to carry into the next cycle. A tier the reading wants
/// running is not in the cycle at all: whatever maintenance is due runs
/// alongside the work.
pub fn maintenance_wake(
    phase: Option<ZeroFloorPhase>,
    desired: i32,
    now: std::time::Instant,
    wake_after: std::time::Duration,
    hold: std::time::Duration,
) -> (i32, Option<ZeroFloorPhase>) {
    if desired > 0 {
        return (desired, None);
    }
    match phase {
        // The wake is a floor, not a decision: it holds the pod for `hold`
        // even though the reading keeps saying zero, which is what gives the
        // in-pod sweeps time to come round.
        Some(ZeroFloorPhase::Waking { until }) if now < until => {
            (1, Some(ZeroFloorPhase::Waking { until }))
        }
        Some(ZeroFloorPhase::Waking { .. }) => (0, Some(ZeroFloorPhase::Parked { since: now })),
        Some(ZeroFloorPhase::Parked { since })
            if now.saturating_duration_since(since) >= wake_after =>
        {
            (1, Some(ZeroFloorPhase::Waking { until: now + hold }))
        }
        Some(parked @ ZeroFloorPhase::Parked { .. }) => (0, Some(parked)),
        // First cycle that parks the tier starts its clock.
        None => (0, Some(ZeroFloorPhase::Parked { since: now })),
    }
}

fn decide(policy: &ComponentAutoscale, observed: f64, current: i32, load: Load) -> i32 {
    let min = policy.min;
    let max = policy.max;
    if current <= 0 {
        // Cold start, or a tier the user has just given a floor back. With
        // `min >= 1` — the only shape the reconciler accepts — bootstrap at
        // min. The `min == 0` branch below stays as pure-function behaviour:
        // no spec reaches it, because a zero floor has no way to ask for the
        // pod that would publish the signal this branch reads.
        return if min == 0 {
            if observed > 0.0 {
                1
            } else {
                0
            }
        } else {
            min
        };
    }
    // The pod count the reading asks for, before the deadband and the clamp.
    let wanted = match load {
        Load::PerPod => (current as f64) * (observed / policy.target),
        Load::Shared => observed / policy.target,
    };
    // The deadband is on the SIZE CHANGE, not on the raw signal: hold when the
    // count the reading asks for is within 10% of the count we have. On the
    // per-pod arm that is `observed / target`, the ratio this has always
    // compared, because `wanted` is `current` times it.
    let ratio = wanted / (current as f64);
    if (ratio - 1.0).abs() <= SCALE_DEADBAND {
        return current.clamp(min, max);
    }
    // Anything above zero rounds up to at least one pod, so the clamp to `min`
    // is what holds the floor. With `min == 0` a zero signal drives `wanted` to
    // 0, gated by `fold_observation`'s idle window — the EWMA residual delays
    // the decision and the window ends it. No accepted spec sets a zero floor,
    // so that path is exercised by the unit tests only.
    let desired = wanted.ceil() as i32;
    desired.clamp(min, max)
}

/// EWMA smoothing factor for a sample taken `dt_secs` after the previous one,
/// given a `half_life_secs` (the time over which an old sample's weight halves).
/// At `dt == half_life` the new sample gets weight 0.5; `half_life <= 0` (or a
/// non-positive `dt`) disables smoothing (factor 1.0 ⇒ use the raw sample).
pub fn ewma_alpha(half_life_secs: f64, dt_secs: f64) -> f64 {
    if half_life_secs <= 0.0 || dt_secs <= 0.0 {
        return 1.0;
    }
    (1.0 - 0.5_f64.powf(dt_secs / half_life_secs)).clamp(0.0, 1.0)
}

/// Blend a `raw` metric snapshot into the `prev` smoothed one with `alpha`,
/// per field: `smoothed = prev + alpha * (raw - prev)`. `alpha == 1.0` returns
/// `raw` (no smoothing); the reconciler persists the result as the next cycle's
/// `prev`. Smoothing the saturation signal damps autoscaler flapping. On a
/// zero-floor tier it would also be the delay before the last pod goes away —
/// the blend only decays toward 0 and never arrives, so [`fold_observation`]
/// ends the decay itself once the raw signal has read idle for
/// [`IDLE_HALF_LIVES`] half-lives. The operator refuses a zero floor, so that
/// part carries no shipped behaviour today.
pub fn ewma_smooth(prev: &ObservedMetrics, raw: &ObservedMetrics, alpha: f64) -> ObservedMetrics {
    let blend = |p: f64, r: f64| p + alpha * (r - p);
    ObservedMetrics {
        ingester_rps_per_pod: blend(prev.ingester_rps_per_pod, raw.ingester_rps_per_pod),
        compactor_backlog: blend(prev.compactor_backlog, raw.compactor_backlog),
        query_in_flight_per_pod: blend(prev.query_in_flight_per_pod, raw.query_in_flight_per_pod),
    }
}

/// How long a signal must read idle before smoothing stops holding a residual
/// above zero. Ten half-lives leave the smoothed value at 2^-10 — under a
/// thousandth — of the load it decayed from, so by then the delay has done its
/// job and all that is left is the tail that keeps `decide`'s `ceil` at one pod.
pub const IDLE_HALF_LIVES: f64 = 10.0;

/// Seconds of continuously OBSERVED idleness per signal, ending at
/// [`SmoothingState::at`]. A positive raw sample resets its signal's count; so
/// does an unusable reading, whose gap nobody watched.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IdleSeconds {
    pub ingester: f64,
    pub compactor: f64,
    pub query: f64,
}

/// Per-signal "the interval ending here was not observed", set by an unusable
/// reading of that signal and cleared by its next usable one. A marked signal
/// cannot extend an idle window and reseeds its average rather than blending
/// across the gap.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Interrupted {
    pub ingester: bool,
    pub compactor: bool,
    pub query: bool,
}

impl Interrupted {
    /// Every signal marked — what a whole-outage cycle leaves behind.
    pub const ALL: Self = Self {
        ingester: true,
        compactor: true,
        query: true,
    };
}

/// One cluster's EWMA history.
#[derive(Clone, Debug)]
pub struct SmoothingState {
    /// The instant of the last cycle that carried at least one usable reading.
    pub at: std::time::Instant,
    /// The smoothed value of each signal that has had a usable reading.
    /// `None` for a signal that has never been read, or whose history was
    /// never seeded.
    pub smoothed: ObservedSamples,
    /// Idle window per signal, accumulated up to `at`.
    pub idle: IdleSeconds,
    /// Per-signal gap marker; see [`Interrupted`].
    pub interrupted: Interrupted,
}

/// What one reconcile cycle does with an observation.
#[derive(Clone, Debug)]
pub struct Smoothed {
    /// Readings for the scaling decision, per signal. A `None` signal puts its
    /// own tier on [`replicas_without_signal`]; all three `None` is the whole
    /// outage that holds the fleet.
    pub observed: ObservedSamples,
    /// The history to carry into the next cycle. `None` clears the entry.
    pub state: Option<SmoothingState>,
}

/// Fold one Prometheus observation into a cluster's EWMA history.
///
/// A signal's `observation` is `None` when its reading was unusable: a query
/// error, an empty result vector, or a non-finite sample (`prom::observed`
/// rejects all three, per signal). An unusable reading leaves that signal's
/// smoothed value, and the history's timestamp, as they were — it neither seeds them nor advances them, and only marks the
/// history `interrupted`. Recording the outage as a sample is what made the
/// fleet downscale on recovery: zeros decayed the smoothed signal toward idle
/// while the fleet was still hot, and the first reading after the outage was
/// blended against that decayed value. Holding the last valid sample instead
/// means an outage of any length, from a cold start or from a running fleet,
/// costs the decision nothing.
///
/// `half_life_secs <= 0` disables smoothing: the raw sample passes through and
/// the entry is cleared.
///
/// A signal whose raw sample has read zero for [`IDLE_HALF_LIVES`] half-lives
/// of observed time is reported as exactly 0.0 rather than as the decaying
/// residual. That is what would let a `min: 0` tier reach zero at all, since
/// `decide` rounds any positive ratio up to one pod — the operator refuses a
/// zero floor, so on a shipped spec this only stops a long-idle signal from
/// reporting a residual. Only an exactly-zero raw sample extends the window,
/// so a backlog that is small but real keeps its pod, and an unusable reading
/// restarts it.
pub fn fold_observation(
    previous: Option<SmoothingState>,
    observation: impl Into<ObservedSamples>,
    now: std::time::Instant,
    half_life_secs: f64,
) -> Smoothed {
    let raw = observation.into();
    if half_life_secs <= 0.0 {
        return Smoothed {
            observed: raw,
            state: None,
        };
    }
    // Every signal unusable is the whole-outage cycle: the history is held
    // exactly as it was — not seeded, not advanced — and every signal is
    // marked so the gap cannot extend an idle window.
    if raw.all_absent() {
        return Smoothed {
            observed: ObservedSamples::default(),
            state: previous.map(|prev| SmoothingState {
                interrupted: Interrupted::ALL,
                ..prev
            }),
        };
    }
    let idle_window = IDLE_HALF_LIVES * half_life_secs;
    // The interval behind this cycle. It ends at the last cycle that carried
    // any usable reading, so a signal usable then and now has exactly this
    // much observed time behind it.
    let dt = previous
        .as_ref()
        .map(|prev| now.saturating_duration_since(prev.at).as_secs_f64())
        .unwrap_or_default();
    let alpha = ewma_alpha(half_life_secs, dt);
    // One signal's fold: `(reading for the decision, smoothed history, idle
    // window, gap marker)`.
    //
    // An unusable reading of THIS signal leaves its history alone and marks
    // it, whatever the other two did. A usable one blends against the history
    // it has — an interruption does not throw the EWMA residual away, it only
    // restarts the idle window, because the residual is what keeps the last
    // pod until a window of *observed* idleness has run its course.
    let fold = |raw: Option<f64>,
                prev_smoothed: Option<f64>,
                prev_idle: f64,
                prev_interrupted: bool|
     -> (Option<f64>, Option<f64>, f64, bool) {
        let Some(raw) = raw else {
            return (None, prev_smoothed, prev_idle, true);
        };
        // `None` means the interval ending in this sample was not observed: a
        // cold seed, or the first reading after this signal's outage. Either
        // way the window starts over at this sample rather than counting the
        // gap behind it.
        let (blended, extended) = match prev_smoothed {
            Some(prev) if !prev_interrupted => (prev + alpha * (raw - prev), prev_idle + dt),
            // First usable sample of this signal seeds its average.
            Some(prev) => (prev + alpha * (raw - prev), 0.0),
            None => (raw, 0.0),
        };
        // A positive raw sample clears the window, an idle one extends it, and
        // a window past `idle_window` settles the residual to zero.
        let (value, idle) = if raw > 0.0 {
            (blended, 0.0)
        } else if extended >= idle_window {
            (0.0, extended)
        } else {
            (blended, extended)
        };
        (Some(value), Some(value), idle, false)
    };
    let prev_smoothed = previous
        .as_ref()
        .map(|prev| prev.smoothed.clone())
        .unwrap_or_default();
    let prev_idle = previous.as_ref().map(|prev| prev.idle).unwrap_or_default();
    let prev_interrupted = previous
        .as_ref()
        .map(|prev| prev.interrupted)
        .unwrap_or_default();
    let (ingester, ingester_smoothed, ingester_idle, ingester_interrupted) = fold(
        raw.ingester_rps_per_pod,
        prev_smoothed.ingester_rps_per_pod,
        prev_idle.ingester,
        prev_interrupted.ingester,
    );
    let (compactor, compactor_smoothed, compactor_idle, compactor_interrupted) = fold(
        raw.compactor_backlog,
        prev_smoothed.compactor_backlog,
        prev_idle.compactor,
        prev_interrupted.compactor,
    );
    let (query, query_smoothed, query_idle, query_interrupted) = fold(
        raw.query_in_flight_per_pod,
        prev_smoothed.query_in_flight_per_pod,
        prev_idle.query,
        prev_interrupted.query,
    );
    Smoothed {
        observed: ObservedSamples {
            ingester_rps_per_pod: ingester,
            compactor_backlog: compactor,
            query_in_flight_per_pod: query,
        },
        state: Some(SmoothingState {
            at: now,
            smoothed: ObservedSamples {
                ingester_rps_per_pod: ingester_smoothed,
                compactor_backlog: compactor_smoothed,
                query_in_flight_per_pod: query_smoothed,
            },
            idle: IdleSeconds {
                ingester: ingester_idle,
                compactor: compactor_idle,
                query: query_idle,
            },
            interrupted: Interrupted {
                ingester: ingester_interrupted,
                compactor: compactor_interrupted,
                query: query_interrupted,
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{AutoscalingSpec, ComponentAutoscale, SiglakeClusterSpec};

    fn cluster_with(policy: ComponentAutoscale) -> SiglakeClusterSpec {
        SiglakeClusterSpec {
            image: "siglake:test".into(),
            warehouse_url: "s3://test/warehouse".into(),
            catalog_uri: "postgres://test/db".into(),
            autoscaling: AutoscalingSpec {
                compactor: policy,
                ingester: ComponentAutoscale::default(),
                query: ComponentAutoscale::default(),
                ewma_half_life_secs: 0.0,
            },
            tenants: vec![],
            auth_tokens_secret_ref: None,
            storage: Default::default(),
            retention: Default::default(),
            aws_region: String::new(),
            service_account_name: String::new(),
            schema_version: None,
            extra_env: Vec::new(),
            resources: Default::default(),
        }
    }

    /// The compactor policies here all carry `max: 8`, so they are claim-mode
    /// policies and the backlog is the shared queue depth: a reading of 10 at a
    /// target of 5 asks for two workers, and the deadband holds a tier that
    /// already has two.
    #[test]
    fn deadband_pins_replicas_at_target() {
        let spec = cluster_with(ComponentAutoscale {
            min: 1,
            max: 8,
            target: 5.0,
        });
        let m = ObservedMetrics {
            compactor_backlog: 10.0,
            ..Default::default()
        };
        let d = reconcile_replicas(
            &spec,
            &m,
            &CurrentReplicas {
                compactor: 2,
                ..Default::default()
            },
        );
        assert_eq!(d.compactor, 2, "deadband must hold");
    }

    #[test]
    fn scale_up_above_target() {
        let spec = cluster_with(ComponentAutoscale {
            min: 1,
            max: 8,
            target: 5.0,
        });
        // A shared queue of 20 at 5 per worker wants 4 workers.
        let m = ObservedMetrics {
            compactor_backlog: 20.0,
            ..Default::default()
        };
        let d = reconcile_replicas(
            &spec,
            &m,
            &CurrentReplicas {
                compactor: 2,
                ..Default::default()
            },
        );
        assert_eq!(d.compactor, 4);
    }

    #[test]
    fn scale_down_below_target() {
        let spec = cluster_with(ComponentAutoscale {
            min: 1,
            max: 8,
            target: 5.0,
        });
        // Four workers on a queue of 4: deadband-cleared scale-down to one.
        let m = ObservedMetrics {
            compactor_backlog: 4.0,
            ..Default::default()
        };
        let d = reconcile_replicas(
            &spec,
            &m,
            &CurrentReplicas {
                compactor: 4,
                ..Default::default()
            },
        );
        assert_eq!(d.compactor, 1);
    }

    /// #3692's acceptance. `peek_pending` counts the whole sealed queue with no
    /// worker filter and every replica publishes that same total, so the count
    /// it asks for cannot depend on how many replicas are publishing it.
    /// Backlog 8, target 4, bounds 1–8: two workers, from any current size.
    #[test]
    fn a_shared_claim_backlog_asks_for_the_same_workers_at_every_replica_count() {
        let policy = ComponentAutoscale {
            min: 1,
            max: 8,
            target: 4.0,
        };
        let spec = cluster_with(policy.clone());
        assert_eq!(
            compactor_load(&policy),
            Load::Shared,
            "a ceiling above one pod is the catalog-claim drain"
        );
        let m = ObservedMetrics {
            compactor_backlog: 8.0,
            ..Default::default()
        };
        for current in [1, 2, 3, 4, 5, 8] {
            let d = reconcile_replicas(
                &spec,
                &m,
                &CurrentReplicas {
                    compactor: current,
                    ..Default::default()
                },
            );
            assert_eq!(
                d.compactor, 2,
                "a shared backlog of 8 at a target of 4 wants two workers, not {}",
                d.compactor
            );
        }

        // The control: the per-pod arithmetic this replaced read the same
        // backlog once per pod, so it asked for four workers at two and eight at
        // four — and then read 8 again from those and stayed at the ceiling.
        for (current, pre_fix) in [(2, 4), (4, 8)] {
            assert_eq!(
                (f64::from(current) * (8.0 / 4.0)).ceil() as i32,
                pre_fix,
                "the control arm must reproduce the defect, or this test proves nothing"
            );
        }
    }

    /// The shared arm keeps the per-pod arm's answer to a reading that is not a
    /// number. `prom::observed` refuses a non-finite sample before it gets here,
    /// and this is the reason: a NaN is unordered against the deadband, so the
    /// hold does not fire and the cast decides — to the floor for a NaN, to the
    /// ceiling for an infinity. Dividing by `target` once rather than `current`
    /// times does not change either.
    #[test]
    fn a_non_finite_shared_backlog_still_pins_to_a_floor_or_a_ceiling() {
        let spec = cluster_with(ComponentAutoscale {
            min: 2,
            max: 6,
            target: 4.0,
        });
        let at = |backlog: f64, current: i32| {
            reconcile_replicas(
                &spec,
                &ObservedMetrics {
                    compactor_backlog: backlog,
                    ..Default::default()
                },
                &CurrentReplicas {
                    compactor: current,
                    ..Default::default()
                },
            )
            .compactor
        };
        assert_eq!(at(f64::NAN, 4), 2, "NaN casts to 0 and clamps to the floor");
        assert_eq!(
            at(f64::INFINITY, 4),
            6,
            "an infinity saturates the cast and clamps to the ceiling"
        );
    }

    /// The filesystem drain keeps the per-pod arithmetic. Its ceiling is one
    /// pod, so the two formulas agree there — this pins that the mode is read
    /// from the policy and not from the replica count.
    #[test]
    fn a_filesystem_drain_policy_keeps_the_per_pod_arithmetic() {
        let policy = ComponentAutoscale {
            min: 1,
            max: 1,
            target: 4.0,
        };
        assert_eq!(compactor_load(&policy), Load::PerPod);
        let spec = cluster_with(policy);
        let m = ObservedMetrics {
            compactor_backlog: 8.0,
            ..Default::default()
        };
        for current in [0, 1] {
            let d = reconcile_replicas(
                &spec,
                &m,
                &CurrentReplicas {
                    compactor: current,
                    ..Default::default()
                },
            );
            assert_eq!(d.compactor, 1, "one pod is the whole range");
        }
    }

    #[test]
    fn max_clamp_holds() {
        let spec = cluster_with(ComponentAutoscale {
            min: 1,
            max: 3,
            target: 5.0,
        });
        let m = ObservedMetrics {
            compactor_backlog: 100.0,
            ..Default::default()
        };
        let d = reconcile_replicas(
            &spec,
            &m,
            &CurrentReplicas {
                compactor: 2,
                ..Default::default()
            },
        );
        assert_eq!(d.compactor, 3);
    }

    #[test]
    fn min_clamp_holds() {
        let spec = cluster_with(ComponentAutoscale {
            min: 2,
            max: 8,
            target: 5.0,
        });
        let m = ObservedMetrics {
            compactor_backlog: 0.0,
            ..Default::default()
        };
        let d = reconcile_replicas(
            &spec,
            &m,
            &CurrentReplicas {
                compactor: 4,
                ..Default::default()
            },
        );
        assert_eq!(d.compactor, 2);
    }

    /// #967: query takes the ordinary proportional decision on its in-flight
    /// signal. Before discovery this returned `policy.min` whatever the load
    /// was, because a replica beyond the rendered peer list received no shard
    /// work — so the pre-#967 code fails the saturated case below.
    #[test]
    fn query_scales_on_in_flight_within_its_range() {
        let mut spec = cluster_with(ComponentAutoscale::default());
        spec.autoscaling.query = ComponentAutoscale {
            min: 2,
            max: 8,
            target: 4.0,
        };

        let saturated = ObservedMetrics {
            query_in_flight_per_pod: 8.0,
            ..Default::default()
        };
        let desired = reconcile_replicas(
            &spec,
            &saturated,
            &CurrentReplicas {
                query: 2,
                ..Default::default()
            },
        );
        assert_eq!(desired.query, 4, "2 pods at 2x target ⇒ 4");

        // The ceiling is still the ceiling.
        let desired = reconcile_replicas(
            &spec,
            &saturated,
            &CurrentReplicas {
                query: 6,
                ..Default::default()
            },
        );
        assert_eq!(desired.query, 8);

        // And the floor is still the floor when the tier goes idle.
        let desired = reconcile_replicas(
            &spec,
            ObservedMetrics::default(),
            &CurrentReplicas {
                query: 8,
                ..Default::default()
            },
        );
        assert_eq!(desired.query, 2);
    }

    /// The load signal is unusable (Prometheus down). Query holds its size —
    /// scaling a fan-out tier down on no evidence is the one direction that
    /// costs correctness-adjacent capacity — but still converges up to `min`.
    #[test]
    fn without_a_signal_query_holds_but_converges_up_to_min() {
        let policy = ComponentAutoscale {
            min: 2,
            max: 8,
            target: 4.0,
        };
        assert_eq!(replicas_without_signal(&policy, 0), 2);
        assert_eq!(replicas_without_signal(&policy, 1), 2);
        assert_eq!(replicas_without_signal(&policy, 6), 6);
        assert_eq!(replicas_without_signal(&policy, 99), 8);
    }

    #[test]
    fn without_a_signal_ingester_bootstraps_and_holds() {
        let policy = ComponentAutoscale {
            min: 1,
            max: 4,
            target: 5.0,
        };
        assert_eq!(replicas_without_signal(&policy, 0), 1);
        assert_eq!(replicas_without_signal(&policy, 3), 3);

        let raised_floor = ComponentAutoscale { min: 2, ..policy };
        assert_eq!(replicas_without_signal(&raised_floor, 1), 2);
    }

    /// #6011's fail-safe. A monitoring outage is not idleness: a zero-floor
    /// tier with no usable reading is RESTORED TO ONE, not held at zero, so
    /// the backlog it cannot see is not accumulating behind a stopped tier.
    /// One pod is the bounded cost, and it publishes the compactor gauge the
    /// activation query falls back to.
    #[test]
    fn without_a_signal_a_zero_floor_tier_is_restored_to_one() {
        let policy = ComponentAutoscale {
            min: 0,
            max: 4,
            target: 5.0,
        };
        assert_eq!(
            replicas_without_signal(&policy, 0),
            1,
            "the pre-#6011 arm returned policy.min — 0 — and left the tier stopped for as \
             long as the signal was missing"
        );
        // A running tier still holds its size; the floor only decides a cold
        // one, and a zero floor does not scale a warm fleet down.
        assert_eq!(replicas_without_signal(&policy, 3), 3);

        let positive_floor = ComponentAutoscale { min: 1, ..policy };
        assert_eq!(replicas_without_signal(&positive_floor, 0), 1);
    }

    #[test]
    fn zero_replicas_bootstraps_at_min() {
        let spec = cluster_with(ComponentAutoscale {
            min: 2,
            max: 8,
            target: 5.0,
        });
        let m = ObservedMetrics::default();
        let d = reconcile_replicas(
            &spec,
            &m,
            &CurrentReplicas {
                compactor: 0,
                ..Default::default()
            },
        );
        assert_eq!(d.compactor, 2);
    }

    /// Scale-to-zero (`min == 0`): an idle component scales down to and stays at
    /// 0, then re-activates to 1 when the signal returns. Pure-function
    /// coverage only — since #3693 the reconciler refuses a zero floor, because
    /// the returning signal this test hands `decide` cannot exist in a cluster
    /// whose pods are stopped (see
    /// `a_zero_replica_tier_needs_a_raised_floor_to_come_back`).
    #[test]
    fn scale_to_zero_idles_at_zero_and_reactivates() {
        let spec = cluster_with(ComponentAutoscale {
            min: 0,
            max: 8,
            target: 5.0,
        });
        let idle = ObservedMetrics::default();
        let busy = ObservedMetrics {
            compactor_backlog: 20.0,
            ..Default::default()
        };
        let at = |obs: &ObservedMetrics, cur: i32| {
            reconcile_replicas(
                &spec,
                obs,
                &CurrentReplicas {
                    compactor: cur,
                    ..Default::default()
                },
            )
            .compactor
        };
        assert_eq!(at(&idle, 1), 0, "idle single pod scales to zero");
        assert_eq!(
            at(&idle, 0),
            0,
            "stays at zero while idle (no bootstrap to 1)"
        );
        assert_eq!(at(&busy, 0), 1, "re-activates to one pod when work appears");
        // A shared queue of 20 at a target of 5 is four workers' worth, and
        // stays four workers' worth however many are already running.
        assert_eq!(at(&busy, 2), 4, "still scales up under load");
        assert_eq!(at(&busy, 6), 4, "and back down from above it");
    }

    /// #3693's dead end, and the two ways out #6011 built.
    ///
    /// The dead end is on the READING, not on the floor: an idle backlog reads
    /// 0.0 and a stopped tier's own gauge can never read anything else, so a
    /// zero-floor tier that only ever sees its own signal stays at zero
    /// forever. What changed is where the reading comes from — the ingesters
    /// publish the queue — and what an ABSENT reading means, which is now one
    /// replica rather than zero.
    #[test]
    fn a_zero_replica_tier_comes_back_on_an_absent_reading_or_a_raised_floor() {
        let stuck = ComponentAutoscale {
            min: 0,
            max: 4,
            target: 5.0,
        };
        assert_eq!(
            replicas_without_signal(&stuck, 0),
            1,
            "no reading is a monitoring outage, not an idle queue"
        );
        // A reading of exactly zero is still a decision to stay parked: that
        // is the whole point of an activation signal that reaches zero.
        assert_eq!(decide(&stuck, 0.0, 0, compactor_load(&stuck)), 0);
        assert_eq!(
            decide(&stuck, 3.0, 0, compactor_load(&stuck)),
            1,
            "and a queue the ingesters publish starts one worker"
        );

        let repaired = ComponentAutoscale { min: 1, ..stuck };
        assert_eq!(
            replicas_without_signal(&repaired, 0),
            1,
            "raising the floor bootstraps the tier without waiting for a reading"
        );
        assert_eq!(
            decide(&repaired, 0.0, 0, compactor_load(&repaired)),
            1,
            "and the same on the usable-but-idle path"
        );
    }

    /// #6011's maintenance wake. Retention, delete tasks, claim reclaim and
    /// the mirror recovery sweep are compactor-resident, so a tier parked at
    /// zero owes the warehouse a pod now and then whatever the queue says.
    #[test]
    fn a_parked_tier_is_woken_for_maintenance_and_then_parks_again() {
        use std::time::Duration;
        let t0 = std::time::Instant::now();
        let after = Duration::from_secs(3600);
        let hold = Duration::from_secs(600);
        let at = |phase, desired, secs: u64| {
            maintenance_wake(phase, desired, t0 + Duration::from_secs(secs), after, hold)
        };

        // The first cycle that parks the tier starts its clock.
        let (replicas, phase) = at(None, 0, 0);
        assert_eq!(replicas, 0);
        assert_eq!(phase, Some(ZeroFloorPhase::Parked { since: t0 }));

        // An hour short, nothing happens.
        let (replicas, phase) = at(phase, 0, 3599);
        assert_eq!(replicas, 0, "still parked");
        assert_eq!(
            phase,
            Some(ZeroFloorPhase::Parked { since: t0 }),
            "and the clock is not restarted by a cycle that changes nothing"
        );

        // At the bound, one pod — and the wake is a floor, so it holds even
        // though the reading keeps asking for zero.
        let (replicas, phase) = at(phase, 0, 3600);
        assert_eq!(replicas, 1);
        assert_eq!(
            phase,
            Some(ZeroFloorPhase::Waking {
                until: t0 + Duration::from_secs(3600) + hold
            })
        );
        let (replicas, phase) = at(phase, 0, 3900);
        assert_eq!(replicas, 1, "held through the wake");

        // The hold ends and the tier parks again, with the hour starting over.
        let (replicas, phase) = at(phase, 0, 4200);
        assert_eq!(replicas, 0);
        assert_eq!(
            phase,
            Some(ZeroFloorPhase::Parked {
                since: t0 + Duration::from_secs(4200)
            })
        );
        assert_eq!(at(phase, 0, 4201).0, 0, "not woken again for another hour");

        // A tier the reading wants running is not in the cycle at all: the
        // maintenance runs alongside the work, and the phase is dropped so the
        // next park starts a fresh hour.
        let (replicas, phase) = at(phase, 3, 4500);
        assert_eq!(replicas, 3, "the wake never lowers a decision");
        assert_eq!(phase, None);
    }

    /// #6011 slice 1's acceptance. One absent reading sizes ITS OWN tier by
    /// `replicas_without_signal` while the other two take the ordinary
    /// proportional decision.
    ///
    /// The pre-fix arm is the last assertion: all three absent, which is what
    /// a single failed query used to produce, holds the whole fleet.
    #[test]
    fn one_absent_reading_sizes_its_own_tier_and_leaves_the_others_deciding() {
        let mut spec = cluster_with(ComponentAutoscale {
            min: 1,
            max: 8,
            target: 5.0,
        });
        spec.autoscaling.ingester = ComponentAutoscale {
            min: 1,
            max: 8,
            target: 100.0,
        };
        spec.autoscaling.query = ComponentAutoscale {
            min: 1,
            max: 8,
            target: 4.0,
        };
        let current = CurrentReplicas {
            ingester: 2,
            compactor: 3,
            query: 2,
        };

        // The compactor's series has gone absent — a tier stopped at zero, a
        // scrape that missed. Ingest is at twice its target and query is idle.
        let desired = reconcile_replicas(
            &spec,
            &ObservedSamples {
                ingester_rps_per_pod: Some(200.0),
                compactor_backlog: None,
                query_in_flight_per_pod: Some(0.0),
            },
            &current,
        );
        assert_eq!(desired.ingester, 4, "2 pods at 2x target ⇒ 4");
        assert_eq!(
            desired.compactor, 3,
            "the tier with no reading holds its size"
        );
        assert_eq!(desired.query, 1, "an idle query tier falls to its floor");

        // And the other way round: ingest absent, compactor reading a queue.
        let desired = reconcile_replicas(
            &spec,
            &ObservedSamples {
                ingester_rps_per_pod: None,
                compactor_backlog: Some(20.0),
                query_in_flight_per_pod: Some(8.0),
            },
            &current,
        );
        assert_eq!(desired.ingester, 2, "held");
        assert_eq!(desired.compactor, 4, "a shared queue of 20 at 5 per worker");
        assert_eq!(desired.query, 4, "2 pods at 2x target ⇒ 4");

        // The control: every reading absent is the whole outage, and every
        // tier holds — the behaviour one failed query used to produce for all
        // three.
        let held = reconcile_replicas(&spec, ObservedSamples::default(), &current);
        assert_eq!(
            held,
            DesiredReplicas {
                ingester: 2,
                compactor: 3,
                query: 2
            }
        );
    }

    #[test]
    fn ewma_alpha_honors_half_life() {
        assert_eq!(
            ewma_alpha(0.0, 30.0),
            1.0,
            "half_life<=0 disables smoothing"
        );
        assert_eq!(ewma_alpha(30.0, 0.0), 1.0, "dt<=0 disables smoothing");
        assert!(
            (ewma_alpha(30.0, 30.0) - 0.5).abs() < 1e-9,
            "dt==half_life ⇒ 0.5"
        );
        assert!(ewma_alpha(30.0, 300.0) > 0.99, "dt>>half_life ⇒ ~raw");
    }

    #[test]
    fn ewma_smooth_blends_then_decays() {
        let zero = ObservedMetrics::default();
        let hot = ObservedMetrics {
            compactor_backlog: 10.0,
            ingester_rps_per_pod: 4.0,
            query_in_flight_per_pod: 2.0,
        };
        // alpha=1.0 ⇒ raw passes through.
        assert_eq!(ewma_smooth(&zero, &hot, 1.0).compactor_backlog, 10.0);
        // alpha=0.5 from 0 toward 10 ⇒ 5; all fields blend independently.
        let s = ewma_smooth(&zero, &hot, 0.5);
        assert_eq!(s.compactor_backlog, 5.0);
        assert_eq!(s.ingester_rps_per_pod, 2.0);
        assert_eq!(s.query_in_flight_per_pod, 1.0);
        // From a hot smoothed value back toward idle ⇒ decays gradually (so
        // scale-to-zero won't trip on a single quiet sample).
        let decayed = ewma_smooth(&hot, &zero, 0.5);
        assert_eq!(decayed.compactor_backlog, 5.0);
    }
}

#[cfg(test)]
mod outage_tests {
    use super::*;
    use crate::crd::{AutoscalingSpec, ComponentAutoscale, SiglakeClusterSpec};
    use std::time::{Duration, Instant};

    const HALF_LIFE: f64 = 60.0;

    fn ingester_spec(policy: ComponentAutoscale, half_life: f64) -> SiglakeClusterSpec {
        SiglakeClusterSpec {
            autoscaling: AutoscalingSpec {
                ingester: policy,
                ewma_half_life_secs: half_life,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn hot(rps: f64) -> ObservedMetrics {
        ObservedMetrics {
            ingester_rps_per_pod: rps,
            ..Default::default()
        }
    }

    /// A cycle in which no query answered: every signal absent.
    fn outage() -> ObservedSamples {
        ObservedSamples::default()
    }

    /// The decision one cycle takes, given the folded observation.
    fn ingester_decision(
        spec: &SiglakeClusterSpec,
        observed: impl Into<ObservedSamples>,
        current: i32,
    ) -> i32 {
        let current = CurrentReplicas {
            ingester: current,
            ..Default::default()
        };
        reconcile_replicas(spec, observed, &current).ingester
    }

    /// The pre-fix fold, kept as the control arm: an unusable reading was
    /// substituted with zeros and written to the history like any sample.
    fn fold_with_zero_substitution(
        previous: Option<SmoothingState>,
        observation: Option<ObservedMetrics>,
        now: Instant,
        half_life: f64,
    ) -> Smoothed {
        fold_observation(
            previous,
            Some(observation.unwrap_or_default()),
            now,
            half_life,
        )
    }

    /// An unusable reading leaves the history byte-for-byte alone: it does not
    /// seed an empty history, and it does not advance the timestamp or the
    /// value of an existing one.
    #[test]
    fn an_unusable_reading_neither_seeds_nor_updates_the_history() {
        let t0 = Instant::now();

        let cold = fold_observation(None, outage(), t0, HALF_LIFE);
        assert!(cold.observed.all_absent(), "no metrics reach the decision");
        assert!(cold.state.is_none(), "an outage does not seed the history");

        let previous = SmoothingState {
            at: t0,
            smoothed: hot(500.0).into(),
            idle: IdleSeconds::default(),
            interrupted: Interrupted::default(),
        };
        let held = fold_observation(
            Some(previous.clone()),
            outage(),
            t0 + Duration::from_secs(300),
            HALF_LIFE,
        );
        assert!(held.observed.all_absent());
        let state = held.state.expect("the last valid sample is retained");
        assert_eq!(state.at, previous.at, "the timestamp does not advance");
        assert_eq!(
            state.smoothed.ingester_rps_per_pod,
            Some(500.0),
            "the value is unchanged"
        );
        assert_eq!(
            state.interrupted,
            Interrupted::ALL,
            "the gap is marked on every signal, so it cannot extend an idle window"
        );
    }

    /// #6011 slice 1, through the smoothing history. One signal's gap marks,
    /// holds and reseeds THAT signal only: the other two keep blending and
    /// keep accumulating their idle windows across the same cycles.
    #[test]
    fn one_signal_s_gap_does_not_interrupt_the_other_two() {
        let t0 = Instant::now();
        let all = ObservedSamples {
            ingester_rps_per_pod: Some(100.0),
            compactor_backlog: Some(8.0),
            query_in_flight_per_pod: Some(0.0),
        };
        let seeded = fold_observation(None, all.clone(), t0, HALF_LIFE)
            .state
            .expect("a seeded history");

        // The compactor query fails; the other two answer as before.
        let partial = fold_observation(
            Some(seeded),
            ObservedSamples {
                compactor_backlog: None,
                ..all.clone()
            },
            t0 + Duration::from_secs(60),
            HALF_LIFE,
        );
        assert_eq!(
            partial.observed.compactor_backlog, None,
            "the failed signal reaches no decision"
        );
        assert_eq!(
            partial.observed.ingester_rps_per_pod,
            Some(100.0),
            "a steady sample smooths to itself"
        );
        let state = partial.state.expect("history");
        assert_eq!(
            state.interrupted,
            Interrupted {
                compactor: true,
                ..Interrupted::default()
            },
            "only the compactor's gap is marked"
        );
        assert_eq!(
            state.smoothed.compactor_backlog,
            Some(8.0),
            "the failed signal's smoothed value is held, not decayed toward zero"
        );
        assert_eq!(
            state.idle.query, 60.0,
            "the query signal's idle window keeps accumulating through another \
             component's gap"
        );
        assert_eq!(state.idle.compactor, 0.0, "the compactor's does not");
        assert_eq!(
            state.at,
            t0 + Duration::from_secs(60),
            "a cycle with any usable reading advances the history's clock"
        );

        // The compactor's next reading opens a fresh window behind it, while
        // the query signal's window is 120 s of continuously observed idleness.
        let recovered = fold_observation(
            Some(state),
            ObservedSamples {
                compactor_backlog: Some(0.0),
                ..all
            },
            t0 + Duration::from_secs(120),
            HALF_LIFE,
        );
        let state = recovered.state.expect("history");
        assert_eq!(state.interrupted, Interrupted::default(), "all cleared");
        assert_eq!(state.idle.compactor, 0.0, "the gap cannot count as idle");
        assert_eq!(state.idle.query, 120.0);
    }

    /// Hot load, a monitoring outage, then the IDENTICAL hot load again. The
    /// recovered reading must reproduce the pre-outage decision.
    #[test]
    fn an_outage_between_two_identical_hot_readings_does_not_downscale() {
        let spec = ingester_spec(
            ComponentAutoscale {
                min: 1,
                max: 10,
                target: 100.0,
            },
            HALF_LIFE,
        );
        let t0 = Instant::now();
        let load = hot(100.0); // exactly at target ⇒ the fleet holds at 8
        let fleet = 8;

        let first = fold_observation(None, Some(load.clone()), t0, HALF_LIFE);
        assert_eq!(ingester_decision(&spec, &first.observed, fleet), fleet);

        let interrupted = fold_observation(
            first.state.clone(),
            outage(),
            t0 + Duration::from_secs(30),
            HALF_LIFE,
        );
        assert_eq!(
            ingester_decision(&spec, &interrupted.observed, fleet),
            fleet,
            "#3488: the outage cycle itself holds the fleet"
        );

        let recovered = fold_observation(
            interrupted.state,
            Some(load.clone()),
            t0 + Duration::from_secs(60),
            HALF_LIFE,
        );
        assert_eq!(
            ingester_decision(&spec, &recovered.observed, fleet),
            fleet,
            "the reading after the outage is the same load, so it is the same decision"
        );

        // Control: with the outage recorded as zeros, the smoothed signal has
        // decayed below target and the recovered reading scales the fleet down.
        let bad_outage =
            fold_with_zero_substitution(first.state, None, t0 + Duration::from_secs(30), HALF_LIFE);
        let bad_recovery = fold_with_zero_substitution(
            bad_outage.state,
            Some(load),
            t0 + Duration::from_secs(60),
            HALF_LIFE,
        );
        assert!(
            ingester_decision(&spec, &bad_recovery.observed, fleet) < fleet,
            "the control arm must reproduce the defect, or this test proves nothing"
        );
    }

    /// A cold start whose FIRST cycles are an outage: the first usable reading
    /// seeds the average raw, with no phantom idle history behind it.
    #[test]
    fn the_first_reading_after_a_cold_start_outage_seeds_the_raw_sample() {
        let spec = ingester_spec(
            ComponentAutoscale {
                min: 1,
                max: 10,
                target: 100.0,
            },
            HALF_LIFE,
        );
        let t0 = Instant::now();

        let mut state = None;
        for cycle in 0..3 {
            let missed = fold_observation(
                state,
                outage(),
                t0 + Duration::from_secs(30 * cycle),
                HALF_LIFE,
            );
            assert_eq!(
                ingester_decision(&spec, &missed.observed, 0),
                1,
                "a cold tier still bootstraps to its configured floor"
            );
            state = missed.state;
        }

        let first = fold_observation(
            state,
            Some(hot(400.0)),
            t0 + Duration::from_secs(90),
            HALF_LIFE,
        );
        assert_eq!(
            first
                .observed
                .ingester_rps_per_pod
                .expect("a usable reading"),
            400.0,
            "seeded raw, not blended against the outage"
        );
        assert_eq!(
            ingester_decision(&spec, &first.observed, 1),
            4,
            "4x the target on one pod scales to four"
        );
    }

    fn backlog(queue_depth: f64) -> ObservedMetrics {
        ObservedMetrics {
            compactor_backlog: queue_depth,
            ..Default::default()
        }
    }

    /// A compactor tier reconciled once a minute, seeded with one busy reading:
    /// `min` pods at the floor, 8 at the ceiling, and a backlog target of 5
    /// unless [`Compactor::with_target`] sets another. A ceiling of 8 makes
    /// every tier here a catalog-claim one, so `backlog` is the shared queue
    /// depth every worker publishes.
    struct Compactor {
        spec: SiglakeClusterSpec,
        state: Option<SmoothingState>,
        at: Instant,
        replicas: i32,
    }

    impl Compactor {
        fn new(min: i32, replicas: i32, seed: f64, half_life: f64) -> Self {
            Self::with_target(min, replicas, seed, half_life, 5.0)
        }

        fn with_target(min: i32, replicas: i32, seed: f64, half_life: f64, target: f64) -> Self {
            let at = Instant::now();
            let spec = SiglakeClusterSpec {
                autoscaling: AutoscalingSpec {
                    compactor: ComponentAutoscale {
                        min,
                        max: 8,
                        target,
                    },
                    ewma_half_life_secs: half_life,
                    ..Default::default()
                },
                ..Default::default()
            };
            let state = fold_observation(None, Some(backlog(seed)), at, half_life).state;
            Self {
                spec,
                state,
                at,
                replicas,
            }
        }

        /// One reconcile cycle, a minute after the last. `None` is an unusable
        /// reading, which takes the tier down the no-signal path.
        fn cycle(&mut self, observation: impl Into<ObservedSamples>) -> i32 {
            self.at += Duration::from_secs(60);
            let half_life = self.spec.autoscaling.ewma_half_life_secs;
            let folded = fold_observation(self.state.take(), observation, self.at, half_life);
            self.replicas = reconcile_replicas(
                &self.spec,
                &folded.observed,
                &CurrentReplicas {
                    compactor: self.replicas,
                    ..Default::default()
                },
            )
            .compactor;
            self.state = folded.state;
            self.replicas
        }

        fn idle(&mut self) -> i32 {
            self.cycle(Some(ObservedMetrics::default()))
        }

        /// The backlog the decision just saw.
        fn smoothed(&self) -> f64 {
            self.state
                .as_ref()
                .expect("smoothing is on and the last reading was usable")
                .smoothed
                .compactor_backlog
                .expect("the compactor signal has been read at least once")
        }
    }

    /// #3692 through the smoothed path. A backlog that neither grows nor
    /// drains asks for the same number of workers every cycle — and it is a
    /// FIXED backlog that made the pre-fix arithmetic walk to the ceiling, each
    /// cycle reading the shared queue once per pod it had just added.
    ///
    /// Backlog 8, target 4, bounds 1–8: two workers, whether the tier starts at
    /// two or at four, and after a replica change under the same history.
    #[test]
    fn a_steady_shared_backlog_never_walks_to_the_ceiling() {
        for start in [2, 4] {
            let mut tier = Compactor::with_target(1, start, 8.0, HALF_LIFE, 4.0);
            let mut sizes = Vec::new();
            for _ in 1..=15 {
                sizes.push(tier.cycle(Some(backlog(8.0))));
            }
            assert!(
                sizes.iter().all(|&n| n == 2),
                "from {start} workers a steady queue of 8 at a target of 4 is two \
                 workers every cycle: {sizes:?}"
            );
            assert_eq!(
                tier.smoothed(),
                8.0,
                "a steady raw sample smooths to itself"
            );

            // A replica change from outside the operator — a manual scale, a
            // node draining — against the SAME smoothing history. The decision
            // reads the queue, not the count that is publishing it.
            tier.replicas = 6;
            assert_eq!(
                tier.cycle(Some(backlog(8.0))),
                2,
                "the same queue from six workers is still two workers"
            );
            tier.replicas = 1;
            assert_eq!(tier.cycle(Some(backlog(8.0))), 2, "and still two from one");
        }
    }

    /// The documented contract for `ewmaHalfLifeSecs` with a `min: 0` tier: a
    /// sustained idle window retires every pod, including the last one, after
    /// [`IDLE_HALF_LIVES`] half-lives of idle readings. The EWMA residual is
    /// what delays that; before this it also prevented it, because `decide`
    /// rounds any positive ratio up to one pod.
    #[test]
    fn a_sustained_idle_window_takes_a_min_zero_compactor_to_zero() {
        let mut tier = Compactor::new(0, 8, 40.0, HALF_LIFE);
        let mut smoothed = Vec::new();
        let mut cycles_to_zero = None;
        for cycle in 1..=30 {
            let replicas = tier.idle();
            smoothed.push(tier.smoothed());
            if replicas == 0 {
                cycles_to_zero = Some(cycle);
                break;
            }
        }
        assert_eq!(
            cycles_to_zero,
            Some(10),
            "one idle reading a minute at a 60 s half-life ⇒ zero after ten of them: {smoothed:?}"
        );
        assert_eq!(
            f64::from(cycles_to_zero.expect("a zero cycle")) * 60.0,
            IDLE_HALF_LIVES * HALF_LIFE,
            "the window the cycles measure is the documented one"
        );
        assert!(
            smoothed[..9].windows(2).all(|w| w[1] < w[0]),
            "until the window closes, each idle reading only decays the backlog: {smoothed:?}"
        );
        assert!(
            smoothed[8] < 0.1,
            "the residual the window discards is a rounding tail: {smoothed:?}"
        );
        assert_eq!(smoothed[9], 0.0, "and the window settles it exactly");

        // A positive floor is untouched: the same window leaves it at min.
        let mut floored = Compactor::new(1, 8, 40.0, HALF_LIFE);
        for _ in 1..=15 {
            floored.idle();
        }
        assert_eq!(floored.replicas, 1, "a min: 1 tier idles at its floor");
    }

    /// Smoothing still has to buy what it was added for: one quiet sample must
    /// not retire the last pod. The control arm is the same sample with
    /// smoothing off, which retires it immediately — as it always has.
    #[test]
    fn a_single_quiet_sample_does_not_retire_the_last_pod() {
        let mut smoothed = Compactor::new(0, 1, 5.0, HALF_LIFE);
        assert_eq!(
            smoothed.idle(),
            1,
            "one quiet reading is not an idle window"
        );
        assert_eq!(smoothed.idle(), 1, "nor are two");

        let mut unsmoothed = Compactor::new(0, 1, 5.0, 0.0);
        assert_eq!(
            unsmoothed.idle(),
            0,
            "with ewmaHalfLifeSecs: 0 the raw sample still decides on its own"
        );
    }

    /// Work returning after the tier has been retired brings it back.
    #[test]
    fn work_after_the_idle_window_reactivates_the_tier() {
        let mut tier = Compactor::new(0, 8, 40.0, HALF_LIFE);
        for _ in 1..=10 {
            tier.idle();
        }
        assert_eq!(tier.replicas, 0, "retired");
        assert_eq!(
            tier.cycle(Some(backlog(12.0))),
            1,
            "a backlog reappears and the tier activates"
        );
        assert_eq!(
            tier.cycle(Some(backlog(40.0))),
            5,
            "and it scales on the blended signal from there"
        );
    }

    /// An outage cannot establish idleness: the reading it did not take says
    /// nothing about the backlog, so the window starts over on recovery.
    #[test]
    fn an_outage_restarts_the_idle_window() {
        let mut tier = Compactor::new(0, 8, 40.0, HALF_LIFE);
        for _ in 1..=9 {
            tier.idle();
        }
        assert_eq!(tier.replicas, 1, "one cycle short of the window");
        assert_eq!(tier.cycle(None), 1, "the outage cycle holds the tier");

        // The recovery reading closes an interval nobody watched, so it opens
        // the window instead of extending it: ten observed intervals follow.
        for cycle in 1..=10 {
            assert_eq!(
                tier.idle(),
                1,
                "cycle {cycle} after the outage is inside a fresh window"
            );
        }
        assert_eq!(
            tier.idle(),
            0,
            "a full observed window after the outage retires the tier"
        );
    }

    /// The window is opened by an exactly-zero raw reading, never by a smoothed
    /// value that has merely become small: a backlog of 0.4 segments per pod is
    /// work, and the tier that would do it keeps its pod.
    #[test]
    fn a_small_but_real_backlog_never_rounds_away() {
        let mut tier = Compactor::new(0, 8, 40.0, HALF_LIFE);
        for _ in 1..=9 {
            tier.idle();
        }
        assert!(
            tier.smoothed() < 0.1,
            "the smoothed backlog is already deep under the epsilon a naive cutoff would use"
        );
        assert_eq!(tier.cycle(Some(backlog(0.4))), 1, "real work, real pod");

        for cycle in 1..=9 {
            assert_eq!(tier.idle(), 1, "cycle {cycle} of the restarted window");
        }
        assert_eq!(tier.idle(), 0, "the restarted window then runs its course");
    }

    /// `ewmaHalfLifeSecs: 0` keeps its behaviour: the raw sample passes through
    /// and no history is kept, including across an outage.
    #[test]
    fn smoothing_disabled_passes_the_raw_sample_and_keeps_no_history() {
        let t0 = Instant::now();
        let raw = hot(37.0);

        let passed = fold_observation(None, Some(raw.clone()), t0, 0.0);
        assert_eq!(
            passed
                .observed
                .ingester_rps_per_pod
                .expect("raw sample passes through"),
            37.0
        );
        assert!(passed.state.is_none(), "no history when smoothing is off");

        // A stale entry from a previous `ewmaHalfLifeSecs > 0` spec is cleared,
        // and an outage still reports "no signal" rather than a zero sample.
        let stale = SmoothingState {
            at: t0,
            smoothed: raw.into(),
            idle: IdleSeconds::default(),
            interrupted: Interrupted::default(),
        };
        let cleared = fold_observation(Some(stale), outage(), t0 + Duration::from_secs(30), 0.0);
        assert!(cleared.observed.all_absent());
        assert!(cleared.state.is_none());
    }

    /// A monitoring outage must not scale the data plane down.
    ///
    /// THE DEFECT THIS DOCUMENTS. On a Prometheus query error the reconciler
    /// substituted `ObservedMetrics::default()` — all zeros — and the comment
    /// said so plainly: "degrade to zero-everywhere, which collapses to
    /// scale=min". For a SATURATION signal that is exactly backwards. `decide`
    /// computes ratio = 0/target, which clears the deadband, so desired =
    /// ceil(current * 0) = 0, clamped to min: an 8-pod ingester fleet collapses
    /// to 1 during a monitoring outage, and to 0 on the scale-to-zero path the
    /// CRD documents. EWMA smoothing only delays it — the smoothed value decays
    /// toward the zeros.
    ///
    /// The reconciler now HOLDS the current counts instead. This pins the
    /// arithmetic that made holding necessary, so the reason survives even if
    /// someone reintroduces the zero-substitution.
    #[test]
    fn zero_metrics_would_collapse_the_fleet() {
        let current = 8.0_f64;
        let observed = 0.0_f64;
        let target = 1000.0_f64;
        let ratio = observed / target;
        assert!(
            (ratio - 1.0).abs() > SCALE_DEADBAND,
            "a zero reading sits outside the deadband, so it DOES trigger a scale"
        );
        assert_eq!(
            (current * ratio).ceil() as i32,
            0,
            "zeros scale the fleet to nothing before clamping"
        );
    }

    /// Why `prom::observed` refuses a non-finite sample rather than passing it
    /// on: a NaN or an infinity is not a load reading, it is a floor or a
    /// ceiling. `decide` would take it at face value.
    #[test]
    fn non_finite_readings_would_pin_the_fleet_to_a_floor_or_a_ceiling() {
        let spec = ingester_spec(
            ComponentAutoscale {
                min: 1,
                max: 10,
                target: 100.0,
            },
            0.0,
        );
        let fleet = 8;

        assert!(
            (f64::NAN / 100.0 - 1.0)
                .abs()
                .partial_cmp(&SCALE_DEADBAND)
                .is_none(),
            "a NaN ratio is unordered against the deadband, so the guard does not hold the \
             decision and it proceeds to the multiply"
        );
        assert_eq!(
            ingester_decision(&spec, hot(f64::NAN), fleet),
            1,
            "NaN casts to 0 and clamps to the floor"
        );
        assert_eq!(
            ingester_decision(&spec, hot(f64::INFINITY), fleet),
            10,
            "an infinity saturates the cast and clamps to the ceiling"
        );

        // A single non-finite sample would also poison the smoothed history for
        // good, which is the second reason it never becomes an observation.
        let poisoned = ewma_smooth(&hot(f64::NAN), &hot(100.0), 0.5);
        assert!(
            poisoned.ingester_rps_per_pod.is_nan(),
            "NaN never blends out"
        );
    }
}
