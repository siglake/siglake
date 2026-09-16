//! Real-cluster leader-election chaos test (Operator GA #3).
//!
//! The hermetic stress tests in `src/leader.rs` race two `Election`s
//! against the in-memory backend. This file exercises the *production*
//! path — `Election::new` → `KubeLeaseBackend` → a real
//! `coordination.k8s.io/v1.Lease` — and injects chaos (kill the leader,
//! challenge a live leader) to prove failover behaves against a real API
//! server, not just the in-memory model.
//!
//! Gated with `#[ignore]` so `cargo test` stays hermetic. Run against a
//! reachable cluster (kind / k3d / EKS):
//!
//! ```bash
//! cargo test -p siglake-operator --test operator leader_chaos:: -- --ignored --nocapture
//! ```
//!
//! Each test uses a unique namespace + a unique lease name and cleans up
//! on success; failures leave the namespace for inspection.

use std::time::Duration;

use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::Namespace;
use kube::api::{ObjectMeta, PostParams};
use kube::{Api, Client};

use siglake_operator::leader::Election;

fn unique_ns() -> String {
    let id: u32 = rand::random();
    format!("siglake-leader-it-{id:08x}")
}

fn unique_lease() -> String {
    let id: u32 = rand::random();
    format!("siglake-op-leader-{id:08x}")
}

async fn try_client() -> Option<Client> {
    // kube-rs's reqwest uses rustls, which needs an installed
    // CryptoProvider before any TLS connection (same as the operator
    // binary's main). Idempotent — `install_default` no-ops if already set.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    match Client::try_default().await {
        Ok(client) => Some(client),
        Err(error) if super::require_cluster() => panic!(
            "Kubernetes client required by {}=1 but unavailable: {error}",
            super::REQUIRE_CLUSTER_ENV
        ),
        Err(_) => None,
    }
}

async fn ensure_namespace(client: &Client, name: &str) {
    let api: Api<Namespace> = Api::all(client.clone());
    let _ = api
        .create(
            &PostParams::default(),
            &Namespace {
                metadata: ObjectMeta {
                    name: Some(name.into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
}

async fn delete_namespace(client: &Client, name: &str) {
    let api: Api<Namespace> = Api::all(client.clone());
    let _ = api.delete(name, &Default::default()).await;
}

async fn holder(client: &Client, ns: &str, lease: &str) -> Option<String> {
    let api: Api<Lease> = Api::namespaced(client.clone(), ns);
    api.get(lease)
        .await
        .ok()
        .and_then(|l| l.spec)
        .and_then(|s| s.holder_identity)
}

/// Full failover cycle against a real Lease:
///
/// 1. pod-1 acquires the Lease — `holderIdentity` is pod-1.
/// 2. pod-2 challenges while the lease is alive (pod-1 not yet dead) and
///    stays blocked — the holder does not flip.
/// 3. pod-1 "dies" (stops renewing). After the lease expires, pod-2's
///    acquire steals it — `holderIdentity` flips to pod-2.
/// 4. pod-2 renews; a fresh challenger stays blocked — the live leader
///    is not displaced.
#[tokio::test]
#[ignore]
async fn leader_failover_against_real_lease() {
    let Some(client) = try_client().await else {
        eprintln!("kubeconfig unavailable — skipping");
        return;
    };
    let ns = unique_ns();
    ensure_namespace(&client, &ns).await;
    let lease_name = unique_lease();

    // Short lease so expiry-driven steal happens fast. lease_duration is
    // second-granularity (the Lease stores `leaseDurationSeconds`), so 2s
    // is the smallest value that still survives sub-second clock skew.
    let dur = Duration::from_secs(2);
    let e1 = Election::new(client.clone(), &ns, &lease_name, "pod-1", dur);
    let e2 = Election::new(client.clone(), &ns, &lease_name, "pod-2", dur);

    // --- Phase 1: pod-1 acquires -------------------------------------------
    e1.acquire().await.expect("pod-1 acquires fresh lease");
    assert_eq!(
        holder(&client, &ns, &lease_name).await.as_deref(),
        Some("pod-1"),
        "fresh lease must name pod-1"
    );

    // --- Phase 2: challenger blocked while leader alive --------------------
    // pod-1 keeps the lease warm via renew_loop; pod-2's acquire should
    // not resolve within a couple of lease lifetimes.
    let renew1 = tokio::spawn(e1.clone().renew_loop());
    {
        let e2 = e2.clone();
        let blocked =
            tokio::time::timeout(Duration::from_secs(5), async move { e2.acquire().await }).await;
        assert!(
            blocked.is_err(),
            "pod-2 must stay blocked while pod-1 renews; got {blocked:?}"
        );
    }
    assert_eq!(
        holder(&client, &ns, &lease_name).await.as_deref(),
        Some("pod-1"),
        "holder must not flip while pod-1 renews"
    );

    // --- Phase 3: kill the leader, challenger steals -----------------------
    renew1.abort(); // pod-1 dies — no more renews.
                    // Wait past the lease lifetime so the lease is provably expired.
    tokio::time::sleep(dur + Duration::from_millis(1500)).await;
    tokio::time::timeout(Duration::from_secs(10), e2.acquire())
        .await
        .expect("pod-2 acquire must not hang")
        .expect("pod-2 steals the expired lease");
    assert_eq!(
        holder(&client, &ns, &lease_name).await.as_deref(),
        Some("pod-2"),
        "holder must flip to pod-2 after pod-1 dies"
    );

    // --- Phase 4: new leader holds against a fresh challenger --------------
    let renew2 = tokio::spawn(e2.clone().renew_loop());
    let e3 = Election::new(client.clone(), &ns, &lease_name, "pod-3", dur);
    let blocked = tokio::time::timeout(Duration::from_secs(5), e3.acquire()).await;
    assert!(
        blocked.is_err(),
        "pod-3 must stay blocked while pod-2 renews; got {blocked:?}"
    );
    assert_eq!(
        holder(&client, &ns, &lease_name).await.as_deref(),
        Some("pod-2"),
        "pod-2 must keep the lease against pod-3"
    );
    renew2.abort();

    delete_namespace(&client, &ns).await;
}

/// Many pods race `acquire()` on a single fresh Lease simultaneously;
/// exactly one wins. Proves the real API server's create-conflict
/// semantics give mutual exclusion (only one `create` succeeds; the rest
/// see 409 and back off because the winner's lease is alive).
#[tokio::test]
#[ignore]
async fn many_pods_one_winner_against_real_lease() {
    let Some(client) = try_client().await else {
        eprintln!("kubeconfig unavailable — skipping");
        return;
    };
    let ns = unique_ns();
    ensure_namespace(&client, &ns).await;
    let lease_name = unique_lease();

    // Long-ish lease so no one steals during the race window.
    let dur = Duration::from_secs(30);
    let mut handles = Vec::new();
    for i in 0..8 {
        let e = Election::new(client.clone(), &ns, &lease_name, format!("pod-{i}"), dur);
        handles.push(tokio::spawn(async move {
            // Each pod tries to acquire; the loser blocks in back-off, so
            // bound it — we only care that AT MOST one resolves quickly.
            tokio::time::timeout(Duration::from_secs(3), e.acquire())
                .await
                .ok()
                .map(|r| r.map(|_| ()))
        }));
    }

    let mut winners = 0;
    for h in handles {
        if let Ok(Some(Some(Ok(())))) = h.await.map(Some) {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one pod may win the initial race");

    // The Lease names one of the racers.
    let h = holder(&client, &ns, &lease_name).await;
    assert!(
        h.as_deref().is_some_and(|s| s.starts_with("pod-")),
        "lease holder must be one of the racers, got {h:?}"
    );

    delete_namespace(&client, &ns).await;
}
