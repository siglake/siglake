//! Leader election via `coordination.k8s.io/v1.Lease`.
//!
//! Multi-replica operator deployments need exactly one replica
//! reconciling at a time — otherwise both pods race to PATCH the
//! same Deployments and burn API quota. The standard Kubernetes
//! pattern is a `Lease` object whose `spec.holderIdentity` names the
//! current leader.
//!
//! v0 implementation:
//!
//! - At startup, every replica calls [`acquire`] which tries to
//!   create the Lease with itself as the holder. On `409 AlreadyExists`,
//!   read the Lease and decide: take over if `renewTime + lease_duration`
//!   is in the past (the holder is dead), otherwise sleep and retry.
//! - Once leader, [`renew_loop`] refreshes the lease every
//!   `lease_duration / 3` seconds. Failure to renew (someone took over,
//!   API server down) terminates the loop and the caller exits — the
//!   Deployment's restart cycle brings us back into the race.
//!
//! # Hermetic testing
//!
//! [`Election`] talks to its backend through the [`LeaseBackend`]
//! trait. The production path wraps `kube::Api<Lease>` via
//! [`KubeLeaseBackend`]; tests use the in-memory
//! [`InMemoryLeaseBackend`] to race two Elections against each
//! other and assert the expected handoff behavior without
//! spinning up a real K8s.
//!
//! [`acquire`]: Election::acquire
//! [`renew_loop`]: Election::renew_loop

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::{
    api::{ObjectMeta, Patch, PatchParams, PostParams},
    Api, Client,
};
use tokio::sync::Mutex;

// ---- Backend trait --------------------------------------------------------

/// Minimum K8s lease-store surface `Election` needs. Three operations:
///
/// - `create_if_absent` — atomic create; succeeds only when no Lease
///   with this name exists. Returns
///   [`LeaseBackendError::AlreadyExists`] otherwise.
/// - `get` — fetch the current Lease.
/// - `patch` — apply (or overwrite) the Lease. Idempotent.
#[async_trait]
pub trait LeaseBackend: Send + Sync {
    async fn create_if_absent(&self, lease: &Lease) -> Result<(), LeaseBackendError>;
    async fn get(&self, name: &str) -> Result<Lease, LeaseBackendError>;
    async fn patch(&self, name: &str, lease: &Lease) -> Result<(), LeaseBackendError>;
}

#[derive(Debug)]
pub enum LeaseBackendError {
    /// Returned by `create_if_absent` when a Lease with that name
    /// already exists. Used by `Election::acquire` to decide whether
    /// to steal vs back off.
    AlreadyExists,
    /// Returned by `get` when the named Lease isn't present.
    NotFound,
    /// Any other backend failure (network, auth, etc).
    Other(anyhow::Error),
}

impl std::fmt::Display for LeaseBackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists => write!(f, "lease already exists"),
            Self::NotFound => write!(f, "lease not found"),
            Self::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for LeaseBackendError {}

// ---- Production backend (kube-rs) ----------------------------------------

/// Real K8s implementation. Wraps a namespaced `Api<Lease>`.
pub struct KubeLeaseBackend {
    api: Api<Lease>,
}

impl KubeLeaseBackend {
    pub fn new(client: Client, namespace: &str) -> Self {
        Self {
            api: Api::namespaced(client, namespace),
        }
    }
}

#[async_trait]
impl LeaseBackend for KubeLeaseBackend {
    async fn create_if_absent(&self, lease: &Lease) -> Result<(), LeaseBackendError> {
        match self.api.create(&PostParams::default(), lease).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(api)) if api.code == 409 => Err(LeaseBackendError::AlreadyExists),
            Err(e) => Err(LeaseBackendError::Other(
                anyhow::Error::from(e).context("create Lease"),
            )),
        }
    }

    async fn get(&self, name: &str) -> Result<Lease, LeaseBackendError> {
        match self.api.get(name).await {
            Ok(l) => Ok(l),
            Err(kube::Error::Api(api)) if api.code == 404 => Err(LeaseBackendError::NotFound),
            Err(e) => Err(LeaseBackendError::Other(
                anyhow::Error::from(e).context("get Lease"),
            )),
        }
    }

    async fn patch(&self, name: &str, lease: &Lease) -> Result<(), LeaseBackendError> {
        self.api
            .patch(
                name,
                &PatchParams::apply("siglake-operator-leader").force(),
                &Patch::Apply(lease),
            )
            .await
            .map(|_| ())
            .map_err(|e| {
                LeaseBackendError::Other(
                    anyhow::Error::from(e).context(format!("PATCH Lease/{name}")),
                )
            })
    }
}

// ---- In-memory backend (tests) -------------------------------------------

/// Hermetic implementation used in the leader-election stress tests
/// and any other test wanting to race two `Election`s without a real
/// K8s. Behavioral parity:
///
/// - `create_if_absent` is atomic against concurrent callers (the
///   inner Mutex serializes the check + insert).
/// - `get` errors with [`LeaseBackendError::NotFound`] until the
///   Lease has been created.
/// - `patch` always succeeds (operator-side server-side-apply
///   doesn't need conflict detection — `force=true`).
#[derive(Default, Clone)]
pub struct InMemoryLeaseBackend {
    inner: Arc<Mutex<HashMap<String, Lease>>>,
}

impl InMemoryLeaseBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LeaseBackend for InMemoryLeaseBackend {
    async fn create_if_absent(&self, lease: &Lease) -> Result<(), LeaseBackendError> {
        let name = lease
            .metadata
            .name
            .clone()
            .ok_or_else(|| LeaseBackendError::Other(anyhow::anyhow!("lease has no name")))?;
        let mut g = self.inner.lock().await;
        if g.contains_key(&name) {
            return Err(LeaseBackendError::AlreadyExists);
        }
        g.insert(name, lease.clone());
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Lease, LeaseBackendError> {
        let g = self.inner.lock().await;
        g.get(name).cloned().ok_or(LeaseBackendError::NotFound)
    }

    async fn patch(&self, name: &str, lease: &Lease) -> Result<(), LeaseBackendError> {
        let mut g = self.inner.lock().await;
        g.insert(name.to_string(), lease.clone());
        Ok(())
    }
}

// ---- Election -------------------------------------------------------------

/// One leader election binding. Cheap to clone.
#[derive(Clone)]
pub struct Election {
    backend: Arc<dyn LeaseBackend>,
    lease_name: String,
    holder: String,
    lease_duration: Duration,
}

impl Election {
    pub fn new(
        client: Client,
        namespace: &str,
        lease_name: impl Into<String>,
        holder: impl Into<String>,
        lease_duration: Duration,
    ) -> Self {
        Self::with_backend(
            Arc::new(KubeLeaseBackend::new(client, namespace)),
            lease_name,
            holder,
            lease_duration,
        )
    }

    /// Build an `Election` against a custom backend. Production code
    /// uses [`Self::new`] (which wraps `kube::Client` via
    /// [`KubeLeaseBackend`]); tests pass an
    /// [`InMemoryLeaseBackend`] directly.
    pub fn with_backend(
        backend: Arc<dyn LeaseBackend>,
        lease_name: impl Into<String>,
        holder: impl Into<String>,
        lease_duration: Duration,
    ) -> Self {
        Self {
            backend,
            lease_name: lease_name.into(),
            holder: holder.into(),
            lease_duration,
        }
    }

    /// Block until we hold the lease.
    pub async fn acquire(&self) -> Result<()> {
        loop {
            match self
                .backend
                .create_if_absent(&self.lease_doc(Some(self.holder.clone())))
                .await
            {
                Ok(_) => {
                    tracing::info!(holder = %self.holder, "leader lease acquired (created)");
                    return Ok(());
                }
                Err(LeaseBackendError::AlreadyExists) => {
                    if self.try_steal_expired().await? {
                        return Ok(());
                    }
                    // Live leader holds it; sleep and retry.
                    tokio::time::sleep(self.lease_duration / 3).await;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("create Lease: {e}"));
                }
            }
        }
    }

    async fn try_steal_expired(&self) -> Result<bool> {
        let current = self
            .backend
            .get(&self.lease_name)
            .await
            .with_context(|| format!("get Lease/{}", self.lease_name))?;
        let renew = current
            .spec
            .as_ref()
            .and_then(|s| s.renew_time.as_ref())
            .map(|m| m.0);
        let lease_seconds = current
            .spec
            .as_ref()
            .and_then(|s| s.lease_duration_seconds)
            .unwrap_or(self.lease_duration.as_secs() as i32);
        if is_lease_alive(renew, Utc::now(), lease_seconds) {
            return Ok(false);
        }
        // Holder is dead. PATCH to take over.
        let new_doc = self.lease_doc(Some(self.holder.clone()));
        self.backend
            .patch(&self.lease_name, &new_doc)
            .await
            .with_context(|| format!("PATCH Lease/{}", self.lease_name))?;
        tracing::info!(holder = %self.holder, "leader lease acquired (stolen)");
        Ok(true)
    }

    /// Background loop that refreshes the lease. Returns on the
    /// first refresh failure — the caller is expected to abort.
    pub async fn renew_loop(self) -> Result<()> {
        let interval = self.lease_duration / 3;
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // skip the immediate first tick
        loop {
            ticker.tick().await;
            let doc = self.lease_doc(Some(self.holder.clone()));
            self.backend
                .patch(&self.lease_name, &doc)
                .await
                .with_context(|| format!("renew Lease/{}", self.lease_name))?;
        }
    }

    fn lease_doc(&self, holder: Option<String>) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some(self.lease_name.clone()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: holder,
                lease_duration_seconds: Some(self.lease_duration.as_secs() as i32),
                renew_time: Some(MicroTime(Utc::now())),
                ..Default::default()
            }),
        }
    }
}

/// Returns `true` if a lease with this `renew_time` and
/// `lease_seconds` is still valid at `now`. A `None` `renew_time`
/// counts as expired (the lease never claimed a holder).
///
/// Extracted from `try_steal_expired` so the time arithmetic can be
/// unit-tested without spinning up a kube API.
fn is_lease_alive(
    renew_time: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
    lease_seconds: i32,
) -> bool {
    match renew_time {
        Some(r) => {
            let age = now.signed_duration_since(r);
            age.num_seconds() < lease_seconds as i64
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(secs: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).single().unwrap()
    }

    #[test]
    fn alive_when_freshly_renewed() {
        // Renewed 1 second ago, 15s lease → alive.
        assert!(is_lease_alive(Some(t(99)), t(100), 15));
    }

    #[test]
    fn alive_at_boundary_minus_one_second() {
        // Renewed exactly (lease - 1) seconds ago → still alive.
        assert!(is_lease_alive(Some(t(86)), t(100), 15));
    }

    #[test]
    fn expired_at_boundary() {
        // Renewed exactly lease_seconds ago → expired (strict <).
        assert!(!is_lease_alive(Some(t(85)), t(100), 15));
    }

    #[test]
    fn expired_long_ago() {
        // Renewed 10 minutes ago, 15s lease → solidly expired.
        assert!(!is_lease_alive(Some(t(0)), t(600), 15));
    }

    #[test]
    fn missing_renew_time_counts_expired() {
        // No renew_time means the Lease was never claimed. Steal.
        assert!(!is_lease_alive(None, t(100), 15));
    }

    #[test]
    fn clock_skew_in_the_future_is_alive() {
        // Holder renewed "1s in the future" relative to us — clock
        // skew between pods. Treat as alive (age is negative).
        assert!(is_lease_alive(Some(t(101)), t(100), 15));
    }

    // ---- Multi-pod stress tests -------------------------------------------

    /// 2 pods race acquire() on a fresh lease; exactly one wins, the
    /// other is still blocked. The winner's identity is on the
    /// Lease.spec.holderIdentity.
    #[tokio::test]
    async fn exactly_one_pod_acquires_initial_lease() {
        let backend = Arc::new(InMemoryLeaseBackend::new());
        let e1 = Election::with_backend(
            backend.clone(),
            "test-lease",
            "pod-1",
            Duration::from_secs(15),
        );
        let e2 = Election::with_backend(
            backend.clone(),
            "test-lease",
            "pod-2",
            Duration::from_secs(15),
        );

        // Race them. The mutex inside InMemoryLeaseBackend serializes
        // create_if_absent; whichever wins the lock first becomes
        // the holder. Spawn-then-join with a timeout on the loser.
        let h1 = tokio::spawn(async move { e1.acquire().await });
        let h2 = tokio::spawn(async move { e2.acquire().await });

        // Wait for one to resolve. The loser is in a sleep loop
        // (lease/3 = 5s); we don't wait for it.
        let winner = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                r = h1 => ("pod-1", r),
                r = h2 => ("pod-2", r),
            }
        })
        .await
        .expect("one acquire must resolve quickly");

        assert!(
            winner.1.as_ref().unwrap().is_ok(),
            "winner returned Err: {winner:?}"
        );

        // The lease in the backend now identifies the winner.
        let lease = backend.get("test-lease").await.unwrap();
        assert_eq!(
            lease.spec.unwrap().holder_identity.as_deref(),
            Some(winner.0),
            "lease holder matches the acquire() winner"
        );
    }

    /// Pod-1 acquires, then "dies" (doesn't renew). After the lease
    /// expires, pod-2's acquire() takes it over via the steal path.
    #[tokio::test]
    async fn second_pod_steals_when_lease_expires() {
        let backend = Arc::new(InMemoryLeaseBackend::new());
        // 1-second lease so the expiry happens fast.
        let short = Duration::from_secs(1);
        let e1 = Election::with_backend(backend.clone(), "test-lease", "pod-1", short);
        let e2 = Election::with_backend(backend.clone(), "test-lease", "pod-2", short);

        e1.acquire().await.expect("pod-1 acquires");
        let lease1 = backend.get("test-lease").await.unwrap();
        assert_eq!(
            lease1.spec.unwrap().holder_identity.as_deref(),
            Some("pod-1")
        );

        // pod-1 doesn't renew — wait past the lease.
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // pod-2 now acquires via steal.
        e2.acquire().await.expect("pod-2 steals");
        let lease2 = backend.get("test-lease").await.unwrap();
        assert_eq!(
            lease2.spec.unwrap().holder_identity.as_deref(),
            Some("pod-2"),
            "lease holder must flip to pod-2 after the steal"
        );
    }

    /// While a leader is actively renewing, a second pod stays in
    /// the back-off loop and never takes over.
    #[tokio::test]
    async fn renew_loop_keeps_lease_alive_against_a_challenger() {
        let backend = Arc::new(InMemoryLeaseBackend::new());
        let short = Duration::from_secs(1);
        let e1 = Election::with_backend(backend.clone(), "test-lease", "pod-1", short);
        let e2 = Election::with_backend(backend.clone(), "test-lease", "pod-2", short);

        e1.acquire().await.unwrap();
        // Spawn pod-1's renew loop.
        let renew = tokio::spawn(e1.clone().renew_loop());
        // Spawn pod-2's acquire — it should stay in the back-off
        // loop because pod-1's renew keeps the lease alive.
        let challenger = tokio::spawn(async move { e2.acquire().await });

        // Wait > 2 lease lifetimes. With renew at lease/3 = 333ms,
        // pod-1 renews at ~0.33s, 0.66s, 1.0s, 1.33s. pod-2's
        // backoff sleeps lease/3 = 333ms between attempts.
        tokio::time::sleep(Duration::from_millis(2500)).await;

        // Lease still says pod-1.
        let lease = backend.get("test-lease").await.unwrap();
        assert_eq!(
            lease.spec.unwrap().holder_identity.as_deref(),
            Some("pod-1"),
            "renew loop must hold the lease"
        );
        assert!(!renew.is_finished(), "renew loop must still be running");
        assert!(
            !challenger.is_finished(),
            "challenger must still be in the back-off loop"
        );

        renew.abort();
        challenger.abort();
    }
}
