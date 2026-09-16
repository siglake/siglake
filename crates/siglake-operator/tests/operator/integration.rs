//! End-to-end operator tests against a real Kubernetes API server.
//!
//! Gated with `#[ignore]` so `cargo test` stays hermetic. Run with a
//! reachable cluster (kind / k3d / a managed cluster):
//!
//! ```bash
//! kind create cluster --name siglake-op-test
//! kubectl apply -f deploy/operator/crd.yaml
//! cargo test -p siglake-operator -- --ignored
//! kind delete cluster --name siglake-op-test
//! ```
//!
//! The tests use a per-test unique namespace and clean it up on
//! success. Failures leave the namespace for inspection.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::core::v1::{Namespace, Secret};
use kube::api::{ListParams, ObjectMeta, Patch, PatchParams, PostParams};
use kube::{runtime::controller::Action, Api, Client};

use siglake_operator::crd::{
    AutoscalingSpec, ComponentAutoscale, SecretRef, SiglakeCluster, SiglakeClusterSpec, TenantSpec,
};
use siglake_operator::prom::PromClient;
use siglake_operator::reconciler::{reconcile, Context};

fn unique_namespace_name() -> String {
    let id: u32 = rand::random();
    format!("siglake-op-it-{id:08x}")
}

async fn try_client() -> Option<Client> {
    // rustls needs an installed CryptoProvider before any TLS connection
    // (same as the operator binary's main and the chaos tests). Idempotent.
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

/// A spec the reconciler honours. The query range is 2/2 because that is the
/// distributed default the chart's StatefulSet and
/// `deploy/operator/sample-cluster.yaml` carry, not because a range is
/// refused: #967 made `query.max > query.min` an ordinary supported policy
/// once the pods started discovering each other at runtime. Keeping the
/// happy path on the packaged default also keeps its expected replica counts
/// stable — the scaling decision has its own unit gates in `scaling.rs`.
///
/// This CR asked for 1/4 until 2026-09-05, and while the (now removed)
/// refusal existed the happy-path test below failed with an empty Deployment
/// list, which read as "nothing was created" rather than "the spec was
/// refused".
fn sample_cr(ns: &str, name: &str) -> SiglakeCluster {
    SiglakeCluster {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some(ns.into()),
            ..Default::default()
        },
        spec: SiglakeClusterSpec {
            image: "ghcr.io/siglake/siglake:0.1.0".into(),
            warehouse_url: "s3://it/warehouse".into(),
            catalog_uri: "postgres://it/db".into(),
            autoscaling: AutoscalingSpec {
                ingester: ComponentAutoscale {
                    min: 1,
                    max: 4,
                    target: 100.0,
                },
                compactor: ComponentAutoscale {
                    min: 1,
                    max: 4,
                    target: 5.0,
                },
                query: ComponentAutoscale {
                    min: 2,
                    max: 2,
                    target: 4.0,
                },
                ewma_half_life_secs: 0.0,
            },
            tenants: vec![TenantSpec {
                name: "acme".into(),
            }],
            auth_tokens_secret_ref: Some(SecretRef {
                name: "siglake-auth".into(),
                key: "tokens".into(),
            }),
            storage: Default::default(),
            retention: Default::default(),
            aws_region: String::new(),
            service_account_name: String::new(),
            schema_version: None,
            extra_env: Vec::new(),
            resources: Default::default(),
        },
        status: None,
    }
}

/// Seed the auth Secret the sample CR's `authTokensSecretRef` points at.
async fn seed_auth_secret(client: &Client, ns: &str) {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
    let mut data = std::collections::BTreeMap::new();
    data.insert(
        "tokens".to_string(),
        k8s_openapi::ByteString("tok1,tok2".as_bytes().to_vec()),
    );
    let _ = secrets
        .create(
            &PostParams::default(),
            &Secret {
                metadata: ObjectMeta {
                    name: Some("siglake-auth".into()),
                    namespace: Some(ns.into()),
                    ..Default::default()
                },
                data: Some(data),
                ..Default::default()
            },
        )
        .await;
}

/// Server-side apply the CR, then fetch it back so the reconciler sees the
/// server-set `uid` and `generation`.
async fn apply_cr(crs: &Api<SiglakeCluster>, cr: &SiglakeCluster) -> SiglakeCluster {
    let name = cr.metadata.name.as_deref().expect("CR has a name");
    crs.patch(
        name,
        &PatchParams::apply("siglake-operator-tests").force(),
        &Patch::Apply(cr),
    )
    .await
    .expect("apply SiglakeCluster");
    crs.get(name).await.expect("get CR")
}

/// A reconciler context. The Prometheus client points at a bogus URL — the
/// kind cluster has none; the reconciler treats the failed query as "hold"
/// and lands each component on its bootstrap-min replica count.
fn test_context(client: &Client) -> Arc<Context> {
    let prom = PromClient::new("http://prometheus.invalid:9090").unwrap();
    Arc::new(Context {
        client: client.clone(),
        prom,
        scaling_state: Default::default(),
    })
}

/// Names of every Deployment and StatefulSet in the namespace.
async fn workload_names(client: &Client, ns: &str) -> Vec<String> {
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), ns);
    let stateful: Api<StatefulSet> = Api::namespaced(client.clone(), ns);
    let mut names: Vec<String> = deployments
        .list(&ListParams::default())
        .await
        .expect("list Deployments")
        .items
        .iter()
        .filter_map(|d| d.metadata.name.clone())
        .map(|n| format!("Deployment/{n}"))
        .collect();
    names.extend(
        stateful
            .list(&ListParams::default())
            .await
            .expect("list StatefulSets")
            .items
            .iter()
            .filter_map(|s| s.metadata.name.clone())
            .map(|n| format!("StatefulSet/{n}")),
    );
    names
}

/// Bring up a per-test namespace, seed a cluster-wide auth Secret and a
/// `SiglakeCluster`, run one `reconcile` cycle directly, and assert the
/// the core workloads materialize with the auth-token env (valueFrom) on
/// the ingester.
#[tokio::test]
#[ignore]
async fn reconcile_creates_deployments_and_threads_auth_env() {
    let Some(client) = try_client().await else {
        eprintln!("kubeconfig unavailable — skipping");
        return;
    };
    let ns = unique_namespace_name();
    ensure_namespace(&client, &ns).await;
    seed_auth_secret(&client, &ns).await;

    let crs: Api<SiglakeCluster> = Api::namespaced(client.clone(), &ns);
    let cr_live = apply_cr(&crs, &sample_cr(&ns, "example")).await;
    match reconcile(Arc::new(cr_live), test_context(&client)).await {
        Ok(Action { .. }) => {}
        Err(e) => panic!("reconcile failed: {e}"),
    }

    // Allow the API server a moment to surface the SSA writes.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // A refused spec leaves this list empty; the status-driven test below is
    // the one that reads as a refusal, so name the status reason here too.
    let names = workload_names(&client, &ns).await;
    let status_reason = crs
        .get("example")
        .await
        .ok()
        .and_then(|cr| cr.status)
        .and_then(|s| s.conditions.into_iter().find(|c| c.type_ == "Ready"))
        .map(|c| format!("{}: {}", c.reason, c.message));
    assert!(
        names.contains(&"Deployment/example-ingester".to_string()),
        "names={names:?} ready={status_reason:?}"
    );
    assert!(
        names.contains(&"Deployment/example-compactor".to_string()),
        "names={names:?} ready={status_reason:?}"
    );
    // Query is a StatefulSet (stable per-pod DNS for distributed peers),
    // not a Deployment — the pre-StatefulSet assertion lived here.
    assert!(
        names.contains(&"StatefulSet/example-query".to_string()),
        "names={names:?} ready={status_reason:?}"
    );

    let deployments: Api<Deployment> = Api::namespaced(client.clone(), &ns);

    let ing = deployments
        .get("example-ingester")
        .await
        .expect("get ingester");
    let env = ing
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.containers.first())
        .and_then(|c| c.env.as_ref())
        .cloned()
        .unwrap_or_default();
    let auth = env
        .iter()
        .find(|e| e.name == "SIGLAKE_AUTH_TOKENS")
        .expect("ingester missing SIGLAKE_AUTH_TOKENS");
    // Operator wires the token via valueFrom secretKeyRef (never reads bytes).
    let sel = auth
        .value_from
        .as_ref()
        .and_then(|s| s.secret_key_ref.as_ref())
        .expect("secretKeyRef set");
    assert_eq!(sel.name, "siglake-auth");
    assert_eq!(sel.key, "tokens");

    delete_namespace(&client, &ns).await;
}

/// The refusal path against a real API server. Same namespace, Secret and CR
/// as the happy path above, except the query autoscaling range is inverted
/// (`min: 4, max: 1`). One `reconcile` must return `Ok`, write `Ready=False` /
/// `InvalidSpec` plus the `AutoscalingRangeInvalid` condition to the status
/// subresource at the CR's current generation, and create no Deployment or
/// StatefulSet.
///
/// #967 replaced the case this used to carry. A query range of 1/4 is now
/// SUPPORTED — the pods discover each other at runtime — so the refusal this
/// exercises is the surviving one: a malformed range. The CRD carries no
/// validation rule for it, so the operator's refusal is the only thing
/// standing between the user and an unsatisfiable policy.
///
/// The hermetic suite proves the condition is computed and the status JSON is
/// shaped; it cannot prove the API server accepts the status merge-patch or
/// that no child resource is written first.
#[tokio::test]
#[ignore]
async fn reconcile_refuses_an_inverted_autoscaling_range_and_creates_nothing() {
    let Some(client) = try_client().await else {
        eprintln!("kubeconfig unavailable — skipping");
        return;
    };
    let ns = unique_namespace_name();
    ensure_namespace(&client, &ns).await;
    seed_auth_secret(&client, &ns).await;

    let crs: Api<SiglakeCluster> = Api::namespaced(client.clone(), &ns);
    let mut cr = sample_cr(&ns, "refused");
    cr.spec.autoscaling.query = ComponentAutoscale {
        min: 4,
        max: 1,
        target: 4.0,
    };
    let cr_live = apply_cr(&crs, &cr).await;
    let generation = cr_live
        .metadata
        .generation
        .expect("API server sets metadata.generation");
    assert!(
        cr_live.status.is_none(),
        "fresh CR must carry no status: {:?}",
        cr_live.status
    );

    match reconcile(Arc::new(cr_live), test_context(&client)).await {
        Ok(Action { .. }) => {}
        Err(e) => panic!("reconcile of a refused spec must return Ok, got: {e}"),
    }

    // Same grace as the happy path, so "nothing exists" is not "nothing yet".
    tokio::time::sleep(Duration::from_millis(500)).await;

    let status = crs
        .get("refused")
        .await
        .expect("get CR")
        .status
        .expect("reconcile must write status for a refused spec");
    assert_eq!(status.observed_generation, generation);
    let ready = status
        .conditions
        .iter()
        .find(|c| c.type_ == "Ready")
        .unwrap_or_else(|| panic!("no Ready condition in {:?}", status.conditions));
    assert_eq!(ready.status, "False");
    assert_eq!(ready.reason, "InvalidSpec");
    let invalid = status
        .conditions
        .iter()
        .find(|c| c.type_ == "InvalidSpec")
        .unwrap_or_else(|| panic!("no InvalidSpec condition in {:?}", status.conditions));
    assert_eq!(invalid.status, "True");
    assert_eq!(invalid.reason, "AutoscalingRangeInvalid");
    assert!(
        invalid.message.contains("spec.autoscaling.query.min (4)")
            && invalid.message.contains("max (1)"),
        "message={}",
        invalid.message
    );
    assert_eq!(ready.message, invalid.message);
    assert_eq!(status.message.as_deref(), Some(invalid.message.as_str()));

    let names = workload_names(&client, &ns).await;
    assert!(
        names.is_empty(),
        "a refused spec must create no workload, found {names:?}"
    );

    delete_namespace(&client, &ns).await;
}
