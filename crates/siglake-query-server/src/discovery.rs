//! Dynamic query-peer discovery (#967), per
//! `docs/DESIGN_dynamic_query_peer_discovery_2026-09.md`.
//!
//! ## Why this exists
//!
//! `--query-peers` is a list rendered at deploy time from the replica count,
//! so a pod KEDA adds beyond that count coordinates but appears in nobody's
//! peer list and receives no shard work. The chart and the operator refused a
//! scaling range above the peer count rather than ship that silently. This
//! module replaces the rendered list: every query server resolves the headless
//! Service's `_http._tcp` SRV record and publishes a membership snapshot, so a
//! ready pod becomes eligible for shard work without a rollout.
//!
//! ## The contract that keeps answers correct
//!
//! A query captures ONE [`PeerSnapshot`] and uses it for everything that
//! depends on membership: the shard count `N`, the shard→peer mapping, the
//! WAL-partial branch, every primary and failover request, and the response
//! attribution. Snapshots are immutable; a refresh publishes a NEW one and
//! never mutates a captured one. So a peer that joins mid-query is simply not
//! addressed by that query, and a peer that leaves still has its shard
//! `(i, N)` failed over unchanged — the file predicate stays
//! `fnv1a(path) % N == i`, which is a partition of the file set for any fixed
//! `N`. Consistency is required WITHIN a query, not between queries.
//!
//! ## What DNS is and is not trusted for
//!
//! Only Ready endpoints are published by the Service (the chart and operator
//! stop setting `publishNotReadyAddresses`), so joins wait for the readiness
//! probe and departures show up on the next view. A resolver error or an empty
//! answer RETAINS the last known good snapshot: a DNS blip must not shrink the
//! cluster to one member mid-flight. It can therefore briefly retain a dead
//! member, which the coordinator's same-shard failover turns into a latency
//! event rather than a wrong answer.
//!
//! Self-identification is explicit rather than positional. The pod matches its
//! own hostname against the first label of a normalized SRV target and is
//! eligible to fan out only on exactly one match — DNS answer position zero is
//! not this pod, and `peers[0]` is only the coordinator on ordinal zero. Until
//! there is a match, transparent `/api/v1/sql` runs locally, which is correct
//! and merely slower.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;

/// How often the background task re-resolves the SRV record. CoreDNS is the
/// TTL authority (the resolver's own cache is disabled); this is how quickly a
/// scale event becomes visible to fan-out, and a miss costs one local query,
/// not an error.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// URL scheme peers are addressed with. SRV records carry a target and a port
/// and no scheme, so this stays an explicit deployment setting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PeerScheme {
    #[default]
    Http,
    Https,
}

impl PeerScheme {
    pub fn as_str(self) -> &'static str {
        match self {
            PeerScheme::Http => "http",
            PeerScheme::Https => "https",
        }
    }
}

/// Pure resolver twin for `--query-peer-scheme`: tests drive this, never the
/// environment.
pub fn peer_scheme_from(raw: Option<&str>) -> Result<PeerScheme, String> {
    match raw.map(str::trim).unwrap_or("") {
        "" | "http" => Ok(PeerScheme::Http),
        "https" => Ok(PeerScheme::Https),
        other => Err(format!(
            "unsupported query peer scheme {other:?}: expected `http` or `https`"
        )),
    }
}

/// One SRV answer, reduced to the two facts dispatch needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SrvTarget {
    pub target: String,
    pub port: u16,
}

impl SrvTarget {
    pub fn new(target: impl Into<String>, port: u16) -> Self {
        Self {
            target: target.into(),
            port,
        }
    }
}

/// An immutable membership view. A request clones one `Arc` of this and uses
/// that same value for its whole lifetime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerSnapshot {
    /// Monotonic per-process publish counter. Surfaced in attribution so a
    /// churn window is identifiable in a log or a response.
    pub generation: u64,
    /// Ordered peer base URLs. `peers[i]` serves shard `i`, and `peers.len()`
    /// is the shard count `N`.
    pub peers: Vec<String>,
    /// THIS coordinator's own worker base URL — the failover target. Never
    /// `peers[0]`: that is only this pod on ordinal zero, and SRV ordering
    /// makes the assumption worse still.
    pub self_url: String,
}

impl PeerSnapshot {
    pub fn len(&self) -> usize {
        self.peers.len()
    }
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

/// What one refresh did, as recorded on
/// `siglake_query_peer_discovery_refresh_total{outcome}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// A new membership was published.
    Changed,
    /// The answer matched the published snapshot exactly.
    Unchanged,
    /// The resolver answered with no usable target. Last known good retained.
    Empty,
    /// A usable answer that does not contain exactly one target for this pod.
    /// Nothing is published: fanning out from a membership this pod is not in
    /// would make it a coordinator with no failover target of its own.
    Unmatched,
    /// The lookup failed. Last known good retained.
    Error,
}

impl RefreshOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            RefreshOutcome::Changed => "changed",
            RefreshOutcome::Unchanged => "unchanged",
            RefreshOutcome::Empty => "empty",
            RefreshOutcome::Unmatched => "unmatched",
            RefreshOutcome::Error => "error",
        }
    }
}

/// The process-wide published membership. Readers take one snapshot per query;
/// the background task replaces the whole `Arc` on a change.
pub struct PeerDirectory {
    current: RwLock<Option<Arc<PeerSnapshot>>>,
    generation: AtomicU64,
    /// This pod's identity — the StatefulSet pod name, resolved once in `main`
    /// and passed in, so tests never mutate `HOSTNAME`.
    hostname: String,
}

impl PeerDirectory {
    pub fn new(hostname: impl Into<String>) -> Self {
        Self {
            current: RwLock::new(None),
            generation: AtomicU64::new(0),
            hostname: hostname.into(),
        }
    }

    /// The published snapshot, or `None` before the first usable answer.
    pub fn snapshot(&self) -> Option<Arc<PeerSnapshot>> {
        self.current.read().expect("peer directory lock").clone()
    }

    /// Fold one resolver answer into the published membership. Emits nothing:
    /// [`refresh_once`] owns the metrics so tests can drive this directly.
    pub fn apply(&self, answers: &[SrvTarget], scheme: PeerScheme) -> RefreshOutcome {
        let peers = normalize_peers(answers, scheme);
        if peers.is_empty() {
            return RefreshOutcome::Empty;
        }
        let Some(self_url) = self_url_in(&self.hostname, &peers) else {
            tracing::warn!(
                hostname = %self.hostname,
                members = ?peers,
                "query peer discovery: no unique self match in the SRV answer; \
                 retaining the previous membership and running queries locally"
            );
            return RefreshOutcome::Unmatched;
        };
        self.publish(peers, self_url)
    }

    /// Publish an explicit membership, skipping normalization and the
    /// self-match. [`Self::apply`] is the deployment path and calls this;
    /// direct callers are the churn gates, whose loopback peers all share one
    /// hostname and so cannot be told apart by DNS label.
    ///
    /// Republishing the identical membership is [`RefreshOutcome::Unchanged`]
    /// and does NOT bump the generation: a snapshot's identity should move
    /// only when the membership does.
    pub fn publish(&self, peers: Vec<String>, self_url: String) -> RefreshOutcome {
        let mut current = self.current.write().expect("peer directory lock");
        if current
            .as_ref()
            .is_some_and(|s| s.peers == peers && s.self_url == self_url)
        {
            return RefreshOutcome::Unchanged;
        }
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let previous = current
            .as_ref()
            .map(|s| s.peers.clone())
            .unwrap_or_default();
        tracing::info!(
            generation,
            old = ?previous,
            new = ?peers,
            self_url = %self_url,
            "query peer discovery: membership changed"
        );
        *current = Some(Arc::new(PeerSnapshot {
            generation,
            peers,
            self_url,
        }));
        RefreshOutcome::Changed
    }
}

/// Normalize a set of SRV answers into ordered peer base URLs.
///
/// Lower-cases and de-dots the targets, drops the `.` "service unavailable"
/// target and port 0, removes duplicate `(target, port)` pairs, and sorts by
/// that pair. DNS answer order is deliberately discarded: it varies per query
/// and would otherwise register as membership churn on every refresh.
pub fn normalize_peers(answers: &[SrvTarget], scheme: PeerScheme) -> Vec<String> {
    let mut pairs: Vec<(String, u16)> = answers
        .iter()
        .filter_map(|a| {
            let target = a
                .target
                .trim()
                .trim_end_matches('.')
                .to_ascii_lowercase()
                .to_string();
            (!target.is_empty() && a.port != 0).then_some((target, a.port))
        })
        .collect();
    pairs.sort();
    pairs.dedup();
    pairs
        .into_iter()
        .map(|(target, port)| format!("{}://{target}:{port}", scheme.as_str()))
        .collect()
}

/// This pod's own entry in `peers`, matched on the first DNS label (the
/// hostname of a StatefulSet pod is its pod name; the SRV target is its FQDN).
///
/// `None` on zero matches AND on more than one: an ambiguous match would pick
/// a failover target that is not this process, which is exactly the
/// `peers[0]`-is-self bug this replaces.
pub fn self_url_in(hostname: &str, peers: &[String]) -> Option<String> {
    let label = hostname
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase()
        .split('.')
        .next()
        .unwrap_or("")
        .to_string();
    if label.is_empty() {
        return None;
    }
    let mut found: Option<&String> = None;
    for peer in peers {
        if peer_host_label(peer).as_deref() == Some(label.as_str()) {
            if found.is_some() {
                return None;
            }
            found = Some(peer);
        }
    }
    found.cloned()
}

/// The first DNS label of a `scheme://host:port` peer URL.
fn peer_host_label(peer: &str) -> Option<String> {
    let after_scheme = peer.split_once("://").map(|(_, rest)| rest)?;
    let host_port = after_scheme.split('/').next()?;
    let host = host_port
        .rsplit_once(':')
        .map_or(host_port, |(host, _)| host);
    let label = host.split('.').next()?;
    (!label.is_empty()).then(|| label.to_ascii_lowercase())
}

/// Where a coordinator's membership comes from. The two modes are mutually
/// exclusive by configuration ([`peer_config_from`]).
#[derive(Clone)]
pub enum PeerSource {
    /// `--query-peers`: a fixed list, retained for non-Kubernetes and test
    /// deployments. Its documented legacy contract is that the coordinator is
    /// peer zero, so the snapshot copies that URL into `self_url` once, at
    /// construction.
    Static(Arc<PeerSnapshot>),
    /// `--query-peer-discovery-srv`: whatever the discovery task has
    /// published, or nothing yet.
    Dynamic(Arc<PeerDirectory>),
}

impl PeerSource {
    /// A static source, or `None` for an empty list (⇒ not a coordinator).
    pub fn from_static(peers: Vec<String>) -> Option<Self> {
        let self_url = peers.first()?.clone();
        Some(PeerSource::Static(Arc::new(PeerSnapshot {
            generation: 0,
            peers,
            self_url,
        })))
    }

    pub fn from_directory(directory: Arc<PeerDirectory>) -> Self {
        PeerSource::Dynamic(directory)
    }

    /// Capture the ONE membership this request will use. `None` ⇒ this pod
    /// cannot fan out right now (dynamic discovery has published nothing
    /// usable yet) and the caller must run the query locally.
    pub fn capture(&self) -> Option<Arc<PeerSnapshot>> {
        match self {
            PeerSource::Static(snapshot) => Some(snapshot.clone()),
            PeerSource::Dynamic(directory) => directory.snapshot(),
        }
    }
}

/// The mutually exclusive peer-source configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerConfig {
    /// Not a coordinator: `/api/v1/sql` runs single-pod, `/shard` still serves.
    None,
    Static(Vec<String>),
    Srv(String),
}

/// Pure resolver twin for the peer-source flags. Configuring both a static
/// list and SRV discovery is an error rather than a precedence rule: the two
/// disagree about what the coordinator's own URL is, and silently preferring
/// one of them is how a cluster ends up fanning out to a membership its
/// operator did not intend.
pub fn peer_config_from(static_peers: &[String], srv: Option<&str>) -> Result<PeerConfig, String> {
    let static_peers: Vec<String> = static_peers
        .iter()
        .map(|p| p.trim().trim_end_matches('/').to_string())
        .filter(|p| !p.is_empty())
        .collect();
    let srv = srv.map(str::trim).filter(|s| !s.is_empty());
    match (static_peers.is_empty(), srv) {
        (false, Some(_)) => Err("--query-peers and --query-peer-discovery-srv are mutually \
             exclusive: the static list names the coordinator as peer zero while discovery \
             matches it by hostname, so a deployment must choose one"
            .to_string()),
        (true, Some(srv)) => Ok(PeerConfig::Srv(srv.to_string())),
        (false, None) => Ok(PeerConfig::Static(static_peers)),
        (true, None) => Ok(PeerConfig::None),
    }
}

/// Resolves SRV records. A trait so the churn and normalization gates run
/// hermetically, with no DNS at all.
#[async_trait]
pub trait SrvResolver: Send + Sync {
    async fn lookup_srv(&self, name: &str) -> Result<Vec<SrvTarget>>;
}

/// The real resolver: `/etc/resolv.conf` (so CoreDNS in-cluster), with the
/// resolver's own answer cache switched OFF. A cached answer would add a
/// second, invisible TTL on top of the Service's and delay a scale event by an
/// unbounded amount; CoreDNS is the single TTL authority.
pub struct DnsSrvResolver {
    resolver: hickory_resolver::TokioResolver,
}

impl DnsSrvResolver {
    pub fn from_system() -> Result<Self> {
        let mut builder = hickory_resolver::Resolver::builder_tokio()
            .context("read the system resolver configuration")?;
        builder.options_mut().cache_size = 0;
        Ok(Self {
            resolver: builder.build().context("build the system DNS resolver")?,
        })
    }
}

#[async_trait]
impl SrvResolver for DnsSrvResolver {
    async fn lookup_srv(&self, name: &str) -> Result<Vec<SrvTarget>> {
        let lookup = self
            .resolver
            .srv_lookup(name)
            .await
            .with_context(|| format!("SRV lookup {name}"))?;
        // Answers to an SRV query can still carry other record types (a CNAME
        // in the chain, say), so filter to SRV rather than assuming.
        Ok(lookup
            .answers()
            .iter()
            .filter_map(|record| match &record.data {
                hickory_resolver::proto::rr::RData::SRV(srv) => {
                    Some(SrvTarget::new(srv.target.to_utf8(), srv.port))
                }
                _ => None,
            })
            .collect())
    }
}

/// One discovery cycle: resolve, fold, record. Separated from the loop so a
/// test can drive exactly one refresh and assert the published snapshot.
pub async fn refresh_once(
    directory: &PeerDirectory,
    resolver: &dyn SrvResolver,
    srv_name: &str,
    scheme: PeerScheme,
) -> RefreshOutcome {
    let outcome = match resolver.lookup_srv(srv_name).await {
        Ok(answers) => directory.apply(&answers, scheme),
        Err(err) => {
            tracing::warn!(srv = %srv_name, error = ?err,
                "query peer discovery: SRV lookup failed; retaining the last known \
                 good membership");
            RefreshOutcome::Error
        }
    };
    metrics::counter!(
        "siglake_query_peer_discovery_refresh_total",
        "outcome" => outcome.as_str()
    )
    .increment(1);
    metrics::gauge!("siglake_query_peer_discovery_members").set(
        directory
            .snapshot()
            .map_or(0.0, |snapshot| snapshot.len() as f64),
    );
    if matches!(outcome, RefreshOutcome::Changed | RefreshOutcome::Unchanged) {
        metrics::gauge!("siglake_query_peer_discovery_last_success_seconds")
            .set(unix_now_seconds());
    }
    outcome
}

fn unix_now_seconds() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// Poll DNS forever, publishing each usable answer. The first cycle runs
/// immediately so a pod that starts into a warm cluster is eligible for shard
/// work within one lookup rather than one interval.
pub fn spawn_peer_discovery(
    directory: Arc<PeerDirectory>,
    resolver: Arc<dyn SrvResolver>,
    srv_name: String,
    scheme: PeerScheme,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            refresh_once(&directory, resolver.as_ref(), &srv_name, scheme).await;
            tokio::time::sleep(interval).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn targets(pairs: &[(&str, u16)]) -> Vec<SrvTarget> {
        pairs.iter().map(|(t, p)| SrvTarget::new(*t, *p)).collect()
    }

    /// Shuffled and duplicated SRV answers normalize to ONE stable ordered
    /// membership. Without this, every refresh that got a different DNS answer
    /// order would look like a membership change and republish a snapshot with
    /// a different shard→peer mapping.
    #[test]
    fn shuffled_and_duplicate_answers_normalize_to_one_order() {
        let a = normalize_peers(
            &targets(&[
                ("q-2.hl.ns.svc.cluster.local.", 8089),
                ("q-0.hl.ns.svc.cluster.local.", 8089),
                ("Q-1.HL.ns.svc.cluster.local", 8089),
            ]),
            PeerScheme::Http,
        );
        let b = normalize_peers(
            &targets(&[
                ("q-1.hl.ns.svc.cluster.local", 8089),
                ("q-0.hl.ns.svc.cluster.local", 8089),
                ("q-0.hl.ns.svc.cluster.local.", 8089),
                ("q-2.hl.ns.svc.cluster.local", 8089),
                ("q-1.hl.ns.svc.cluster.local.", 8089),
            ]),
            PeerScheme::Http,
        );
        assert_eq!(a, b);
        assert_eq!(
            a,
            vec![
                "http://q-0.hl.ns.svc.cluster.local:8089",
                "http://q-1.hl.ns.svc.cluster.local:8089",
                "http://q-2.hl.ns.svc.cluster.local:8089",
            ]
        );
    }

    #[test]
    fn unusable_targets_and_ports_are_dropped() {
        assert!(normalize_peers(&targets(&[(".", 8089)]), PeerScheme::Http).is_empty());
        assert!(normalize_peers(&targets(&[("", 8089)]), PeerScheme::Http).is_empty());
        assert!(normalize_peers(&targets(&[("q-0.hl", 0)]), PeerScheme::Http).is_empty());
    }

    #[test]
    fn scheme_is_an_explicit_setting_srv_cannot_carry() {
        assert_eq!(
            normalize_peers(&targets(&[("q-0.hl", 8089)]), PeerScheme::Https),
            vec!["https://q-0.hl:8089"]
        );
        assert_eq!(peer_scheme_from(None), Ok(PeerScheme::Http));
        assert_eq!(peer_scheme_from(Some(" https ")), Ok(PeerScheme::Https));
        assert!(peer_scheme_from(Some("grpc")).is_err());
    }

    #[test]
    fn self_match_is_by_pod_label_and_must_be_unique() {
        let peers = vec![
            "http://q-0.hl.ns.svc.cluster.local:8089".to_string(),
            "http://q-1.hl.ns.svc.cluster.local:8089".to_string(),
        ];
        assert_eq!(
            self_url_in("q-1", &peers).as_deref(),
            Some("http://q-1.hl.ns.svc.cluster.local:8089")
        );
        // The pod's FQDN is accepted too — only the first label is compared.
        assert_eq!(
            self_url_in("q-0.hl.ns.svc.cluster.local", &peers).as_deref(),
            Some("http://q-0.hl.ns.svc.cluster.local:8089")
        );
        assert_eq!(self_url_in("q-2", &peers), None);
        assert_eq!(self_url_in("", &peers), None);
        // Two ports for the same pod: ambiguous, so NOT a self match. Picking
        // either would hand failover a URL that may not be this listener.
        let ambiguous = vec![
            "http://q-0.hl:8089".to_string(),
            "http://q-0.hl:9089".to_string(),
        ];
        assert_eq!(self_url_in("q-0", &ambiguous), None);
    }

    /// A resolver error, an empty answer, and an answer this pod is not in all
    /// RETAIN the last known good membership. A DNS blip that shrank the
    /// published membership would change `N` between queries for no reason.
    #[test]
    fn error_empty_and_unmatched_retain_the_last_known_good() {
        let dir = PeerDirectory::new("q-0");
        assert_eq!(
            dir.apply(
                &targets(&[("q-0.hl", 8089), ("q-1.hl", 8089)]),
                PeerScheme::Http
            ),
            RefreshOutcome::Changed
        );
        let good = dir.snapshot().expect("published");
        assert_eq!(good.len(), 2);
        assert_eq!(good.self_url, "http://q-0.hl:8089");

        assert_eq!(dir.apply(&[], PeerScheme::Http), RefreshOutcome::Empty);
        assert_eq!(dir.snapshot().as_deref(), Some(&*good));

        assert_eq!(
            dir.apply(&targets(&[("q-7.hl", 8089)]), PeerScheme::Http),
            RefreshOutcome::Unmatched
        );
        assert_eq!(dir.snapshot().as_deref(), Some(&*good));
    }

    #[test]
    fn startup_without_an_answer_publishes_nothing() {
        let dir = PeerDirectory::new("q-0");
        assert!(dir.snapshot().is_none());
        assert!(PeerSource::from_directory(Arc::new(dir))
            .capture()
            .is_none());
    }

    /// An unchanged answer does not bump the generation: republishing would
    /// churn the snapshot identity for no membership change.
    #[test]
    fn an_unchanged_answer_keeps_the_same_generation() {
        let dir = PeerDirectory::new("q-0");
        let answers = targets(&[("q-0.hl", 8089), ("q-1.hl", 8089)]);
        assert_eq!(
            dir.apply(&answers, PeerScheme::Http),
            RefreshOutcome::Changed
        );
        let first = dir.snapshot().expect("published");
        assert_eq!(
            dir.apply(&answers, PeerScheme::Http),
            RefreshOutcome::Unchanged
        );
        assert_eq!(
            dir.snapshot().expect("published").generation,
            first.generation
        );
    }

    /// A snapshot captured at N=2 is unaffected by a later publish at N=3.
    /// This is the whole correctness contract: the shard set a query dispatches
    /// is decided once.
    #[test]
    fn a_captured_snapshot_is_immutable_across_a_membership_change() {
        let dir = Arc::new(PeerDirectory::new("q-0"));
        dir.apply(
            &targets(&[("q-0.hl", 8089), ("q-1.hl", 8089)]),
            PeerScheme::Http,
        );
        let source = PeerSource::from_directory(dir.clone());
        let captured = source.capture().expect("published");
        assert_eq!(captured.len(), 2);

        dir.apply(
            &targets(&[("q-0.hl", 8089), ("q-1.hl", 8089), ("q-2.hl", 8089)]),
            PeerScheme::Http,
        );
        // The in-flight query still sees exactly its two shards...
        assert_eq!(captured.len(), 2);
        assert_eq!(
            captured.peers,
            vec!["http://q-0.hl:8089", "http://q-1.hl:8089"]
        );
        // ...and the NEXT query sees three.
        let next = source.capture().expect("published");
        assert_eq!(next.len(), 3);
        assert!(next.generation > captured.generation);
    }

    #[test]
    fn static_mode_keeps_the_legacy_peer_zero_coordinator_contract() {
        assert!(PeerSource::from_static(Vec::new()).is_none());
        let source =
            PeerSource::from_static(vec!["http://a:8089".into(), "http://b:8089".into()]).unwrap();
        let snapshot = source.capture().expect("static is always available");
        assert_eq!(snapshot.self_url, "http://a:8089");
        assert_eq!(snapshot.len(), 2);
    }

    #[test]
    fn peer_config_refuses_both_modes_at_once() {
        assert_eq!(peer_config_from(&[], None), Ok(PeerConfig::None));
        assert_eq!(
            peer_config_from(&["http://a:8089/".into(), "  ".into()], None),
            Ok(PeerConfig::Static(vec!["http://a:8089".into()]))
        );
        assert_eq!(
            peer_config_from(&[], Some(" _http._tcp.hl.ns.svc.cluster.local ")),
            Ok(PeerConfig::Srv("_http._tcp.hl.ns.svc.cluster.local".into()))
        );
        assert!(peer_config_from(&["http://a:8089".into()], Some("_http._tcp.hl")).is_err());
        // An all-blank static list is not "both modes".
        assert_eq!(
            peer_config_from(&["".into()], Some("_http._tcp.hl")),
            Ok(PeerConfig::Srv("_http._tcp.hl".into()))
        );
    }

    struct ScriptedResolver(std::sync::Mutex<Vec<Result<Vec<SrvTarget>>>>);

    #[async_trait]
    impl SrvResolver for ScriptedResolver {
        async fn lookup_srv(&self, _name: &str) -> Result<Vec<SrvTarget>> {
            self.0
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| Ok(Vec::new()))
        }
    }

    #[tokio::test]
    async fn a_failed_lookup_records_error_and_keeps_serving() {
        let dir = PeerDirectory::new("q-0");
        let resolver = ScriptedResolver(std::sync::Mutex::new(vec![
            Err(anyhow::anyhow!("SERVFAIL")),
            Ok(targets(&[("q-0.hl", 8089), ("q-1.hl", 8089)])),
        ]));
        assert_eq!(
            refresh_once(&dir, &resolver, "_http._tcp.hl", PeerScheme::Http).await,
            RefreshOutcome::Changed
        );
        assert_eq!(
            refresh_once(&dir, &resolver, "_http._tcp.hl", PeerScheme::Http).await,
            RefreshOutcome::Error
        );
        assert_eq!(dir.snapshot().expect("retained").len(), 2);
    }

    /// The one part of discovery no mock can cover: that `DnsSrvResolver`
    /// actually pulls targets out of a REAL answer. Every other gate here
    /// injects `SrvTarget`s directly, so a `lookup_srv` that parsed nothing
    /// would pass all of them and fail only in a cluster — as a silently
    /// single-pod query tier, since an empty answer is a legitimate
    /// "retain the last known good" outcome and never an error.
    ///
    /// That is not hypothetical: hickory 0.26 replaced the SRV-only iterator
    /// this used with `answers()` over all record types, and turned the rdata
    /// accessors into plain fields — a shape that compiles clean while
    /// yielding an empty vector.
    ///
    /// Ignored because it needs working DNS. Point `SIGLAKE_TEST_SRV_NAME` at
    /// any resolvable SRV record and run:
    ///   cargo test -p siglake-query-server srv_lookup_parses -- --ignored
    #[tokio::test]
    #[ignore = "needs working DNS; set SIGLAKE_TEST_SRV_NAME"]
    async fn srv_lookup_parses_a_real_answer() {
        let Ok(name) = std::env::var("SIGLAKE_TEST_SRV_NAME") else {
            panic!("set SIGLAKE_TEST_SRV_NAME to a resolvable SRV record");
        };
        let resolver = DnsSrvResolver::from_system().expect("system resolver");
        let targets = resolver.lookup_srv(&name).await.expect("SRV lookup");
        assert!(
            !targets.is_empty(),
            "{name} resolved to no targets — the answer parsed to nothing"
        );
        for target in &targets {
            assert!(!target.target.is_empty(), "empty target name: {target:?}");
            assert_ne!(target.port, 0, "zero port: {target:?}");
        }
    }
}
