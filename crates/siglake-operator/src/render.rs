//! Render Kubernetes workloads from a `SiglakeCluster` spec.
//!
//! The three data-plane workloads owned by the CR use the same workload kinds
//! as the Helm chart: `<name>-ingester` and `<name>-compactor` are Deployments,
//! and `<name>-query` is a StatefulSet (stable per-pod DNS for distributed
//! fan-out). The operator also renders the WAL PVC, Services, retention
//! CronJobs and the one-shot schema-migration Job.
//!
//! The kind of each SCALABLE tier is load-bearing beyond the render: the
//! reconciler reads current replica counts through a typed API and a mismatch
//! fails silently as "not provisioned" — see
//! `scalable_tier_names_and_kinds_match_what_the_reconciler_reads`.
//!
//! The reconciler server-side applies them every cycle so:
//!
//! - Spec changes (image roll, env update) propagate without manual
//!   intervention.
//! - Manual edits get stomped, which is the operator pattern users
//!   expect.
//! - Garbage collection follows the OwnerReference: deleting the CR
//!   deletes the Deployments.
//!
//! 4.12+ scope:
//!
//! - Deployment shape (image, args, env, metrics port, resource
//!   requests).
//! - WAL PVC (`<name>-wal`) sized + access-moded from
//!   `spec.storage`. Ingester + compactor both mount it at
//!   `/var/lib/siglake/wal`.
//!
//! ExternalSecret integration + NetworkPolicy are still on the
//! Helm chart ([`deploy/helm/siglake/`]).

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{
    Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment, StatefulSet,
    StatefulSetSpec, StatefulSetUpdateStrategy,
};
use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, Job, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, ContainerPort, EmptyDirVolumeSource, EnvVar, EnvVarSource, ExecAction,
    HTTPGetAction, Lifecycle, LifecycleHandler, ObjectFieldSelector, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, PersistentVolumeClaimVolumeSource, PodSecurityContext, PodSpec,
    PodTemplateSpec, Probe, ResourceRequirements, SecretKeySelector, SecurityContext, Service,
    ServicePort, ServiceSpec, Volume, VolumeMount, VolumeResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::Resource;

use crate::crd::{SiglakeCluster, TierResources};

pub const WAL_VOLUME_NAME: &str = "wal";

/// Packaged `requests`/`limits` for one tier. Every tier carries memory and
/// CPU; query also carries ephemeral storage for its spill `emptyDir`.
///
/// THESE MIRROR THE CHART, KEY FOR KEY: `deploy/helm/siglake/values.yaml`
/// `ingester.resources`, `compactor.resources` and `query.resources`. The two
/// are one decision written twice — a change to either side is a change to
/// both — and nothing diffs them, so the pointer is the guard. The query tier
/// drifted once already: cb0d0e1 raised the chart from 2Gi to 4Gi when the
/// read caches and the memory pool began deriving from the pod limit, and left
/// the operator at 2Gi; an operator-managed cluster then ran every query
/// against roughly half the pool the published numbers were measured with
/// (item 28 of the 2026-08-28 query-memory audit, fixed by task #545).
///
/// `spec.resources.<tier>` merges over these per key; see [`tier_resources`].
#[derive(Clone, Copy, Debug)]
pub struct ResourceDefaults {
    pub request_memory: &'static str,
    pub request_cpu: &'static str,
    pub limit_memory: &'static str,
    pub limit_cpu: &'static str,
    pub request_ephemeral_storage: Option<&'static str>,
    pub limit_ephemeral_storage: Option<&'static str>,
}

/// values.yaml `ingester.resources`.
pub const INGESTER_RESOURCES: ResourceDefaults = ResourceDefaults {
    request_memory: "256Mi",
    request_cpu: "200m",
    limit_memory: "1Gi",
    limit_cpu: "2",
    request_ephemeral_storage: None,
    limit_ephemeral_storage: None,
};

/// values.yaml `compactor.resources`. The 1Gi limit supports
/// `SIGLAKE_COMPACTOR_BIN_CONCURRENCY=1` and nothing more; see
/// [`compactor_memory_floor_mib`].
pub const COMPACTOR_RESOURCES: ResourceDefaults = ResourceDefaults {
    request_memory: "256Mi",
    request_cpu: "200m",
    limit_memory: "1Gi",
    limit_cpu: "2",
    request_ephemeral_storage: None,
    limit_ephemeral_storage: None,
};

/// values.yaml `query.resources`. EVERYTHING the query server sizes comes off
/// the memory limit: read caches take 37.5% of it, the memory pool half of
/// what is left, and the process runs in the remainder. At 4Gi that is ~1.5Gi
/// caches, ~1.25Gi pool, ~1.25Gi working room — the shape the 2026-08
/// validation rounds measured. The full sizing note lives above
/// `query.resources.limits.memory` in values.yaml; keep the two in step.
///
/// 4Gi is also the FLOOR, not just a default: it is the smallest limit whose
/// derived pool still holds one compacted file's decode working set. Below it
/// every scan degrades to one file at a time and that file's decode buffers go
/// unaccounted — which measured as an accounting loss, not a slowdown; see
/// [`query_memory_warning`].
///
/// The two text-index caches take only what is left above that reservation, so
/// they do not move this floor: at 4Gi they are zero and the pod deserializes a
/// Puffin index per text query, and a 5Gi pod holds the full 400Mi derived
/// budget. That is the trade the floor makes; raising the limit buys both.
pub const QUERY_RESOURCES: ResourceDefaults = ResourceDefaults {
    request_memory: "256Mi",
    request_cpu: "200m",
    limit_memory: "4Gi",
    limit_cpu: "2",
    request_ephemeral_storage: Some("10Gi"),
    limit_ephemeral_storage: Some("12Gi"),
};

/// Retention CronJob and schema-migration Job containers: short-lived CLI
/// invocations, sized like the chart's `schemaMigration.resources`. Not
/// overridable through `spec.resources`; nothing in a Job derives from them.
const JOB_RESOURCES: ResourceDefaults = ResourceDefaults {
    request_memory: "64Mi",
    request_cpu: "50m",
    limit_memory: "256Mi",
    limit_cpu: "500m",
    request_ephemeral_storage: None,
    limit_ephemeral_storage: None,
};
pub const WAL_MOUNT_PATH: &str = "/var/lib/siglake/wal";
pub const QUERY_SPILL_VOLUME_NAME: &str = "query-spill";
pub const QUERY_SPILL_MOUNT_PATH: &str = "/var/lib/siglake/spill";
pub const QUERY_SPILL_MAX_BYTES: &str = "8589934592";
pub const QUERY_SPILL_SIZE_LIMIT: &str = "10Gi";
const QUERY_FILE_CACHE_MAX_BYTES: &str = "0";
const QUERY_FILE_CACHE_MAX_ENTRIES: &str = "0";

/// Graceful-shutdown window for every rendered pod (mirrors the chart's
/// `terminationGracePeriodSeconds`). The ingester force-seals its active WAL
/// segment on SIGTERM and the query server drains in-flight queries; this must
/// outlast the preStop sleep + that drain or the kubelet SIGKILLs mid-seal.
const TERMINATION_GRACE_SECONDS: i64 = 60;
/// preStop pause before SIGTERM so the Service stops routing new requests
/// (endpoint deregistration propagates) before the process shuts down.
const PRESTOP_SLEEP_SECONDS: u32 = 5;

/// `sleep N` preStop hook applied to every rendered pod for graceful
/// scale-down. `/bin/sh` is present in the debian-slim runtime image.
fn prestop_sleep_lifecycle() -> Lifecycle {
    Lifecycle {
        pre_stop: Some(LifecycleHandler {
            exec: Some(ExecAction {
                command: Some(vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    format!("sleep {PRESTOP_SLEEP_SECONDS}"),
                ]),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// HTTP GET readiness/liveness probe on `path` at the named container `port`.
fn http_probe(path: &str, port: &str, initial_delay: i32, period: i32) -> Probe {
    Probe {
        http_get: Some(HTTPGetAction {
            path: Some(path.to_string()),
            port: IntOrString::String(port.to_string()),
            ..Default::default()
        }),
        initial_delay_seconds: Some(initial_delay),
        period_seconds: Some(period),
        ..Default::default()
    }
}

/// Default object-store prefix the WAL mirror writes to and compactor claims.
///
/// The effective value remains one decision with two halves. The compactor gets
/// `--catalog-claim --mirror-prefix` whenever its fixed autoscaling policy can
/// run more than one replica,
/// and `Compactor::run_once` is exclusive: the claim path reads the
/// `wal_segments` catalog table and NEVER local `sealed/`. Rows land there only
/// if an ingester runs with `SIGLAKE_WAL_MIRROR_PREFIX`. If the halves differ,
/// zero segments are claimed and compaction silently stops — with
/// `siglake_compactor_sealed_pending` reading zero, because the local sealed
/// count is not what the claim path looks at.
pub const WAL_MIRROR_PREFIX: &str = "wal-mirror";

/// WAL-mirror namespace the ingester writes and catalog-claim compactor reads.
///
/// `spec.extraEnv` is appended to every tier, and Kubernetes gives the final
/// duplicate variable precedence. Match the binaries by trimming that value:
/// blank disables mirroring, while an unset override selects the operator
/// default.
pub(crate) fn effective_wal_mirror_prefix(spec: &crate::crd::SiglakeClusterSpec) -> Option<&str> {
    match spec
        .extra_env
        .iter()
        .rev()
        .find(|entry| entry.name == "SIGLAKE_WAL_MIRROR_PREFIX")
        .map(|entry| entry.value.trim())
    {
        Some("") => None,
        Some(prefix) => Some(prefix),
        None => Some(WAL_MIRROR_PREFIX),
    }
}

/// Does this policy use the catalog-coordinated drain?
///
/// The ownership protocol is fixed for the life of an autoscaling policy. A
/// policy that can scale out stays on catalog claims at one or zero replicas;
/// changing the current replica count must never hand retained WAL from one
/// drain implementation to the other.
pub fn uses_catalog_claim(policy: &crate::crd::ComponentAutoscale) -> bool {
    policy.max > 1
}

pub fn ingester_deployment(
    cr: &SiglakeCluster,
    replicas: i32,
    auth_tokens_secret: Option<&crate::crd::SecretRef>,
) -> Deployment {
    let name = name_of(cr, "ingester");
    let labels = component_labels(cr, "ingester");
    let mut env = base_env(cr);
    let remote_wal_drain = uses_catalog_claim(&cr.spec.autoscaling.compactor);
    let effective_mirror_prefix = effective_wal_mirror_prefix(&cr.spec)
        .unwrap_or_default()
        .to_owned();
    // The ingester's pure resolver trims this variable. Normalize the rendered
    // duplicates too, so the pod template states the namespace that the writer
    // and claim reader will actually use.
    for entry in env
        .iter_mut()
        .filter(|entry| entry.name == "SIGLAKE_WAL_MIRROR_PREFIX")
    {
        if let Some(value) = entry.value.as_mut() {
            *value = value.trim().to_owned();
        }
    }
    // The WAL mirror, stated rather than inherited, at EVERY compactor replica
    // count. Two separate reasons, and both have to hold:
    //
    //  - Durability. The binary defaults it on since 2026-09-11 wherever a
    //    warehouse URL is set; without it the WAL PVC is the only copy of
    //    everything acknowledged and not yet committed.
    //  - THE OTHER HALF of the claim decision. Above one compactor replica the
    //    drain claims from this prefix and reads local `sealed/` never, so an
    //    unmirrored ingester stops compaction with no error and a
    //    sealed-pending gauge reading zero.
    //
    // Spliced BEFORE `spec.extraEnv` so the escape hatch still wins: an empty
    // `SIGLAKE_WAL_MIRROR_PREFIX` there is the binary's mirror opt-out, and
    // Kubernetes resolves a duplicate env name to the final one. That opt-out
    // and a compactor autoscaling ceiling above one are rejected as InvalidSpec
    // before any workload is changed.
    let defaults_at = env.len().saturating_sub(cr.spec.extra_env.len());
    env.splice(
        defaults_at..defaults_at,
        [
            EnvVar {
                name: "SIGLAKE_WAL_MIRROR_PREFIX".into(),
                value: Some(effective_mirror_prefix),
                ..Default::default()
            },
            EnvVar {
                name: "SIGLAKE_REMOTE_WAL_DRAIN".into(),
                value: Some(if remote_wal_drain { "1" } else { "0" }.into()),
                ..Default::default()
            },
        ],
    );
    // Optional cluster-wide bearer-token auth. Surfaced as a `valueFrom`
    // secretKeyRef so the operator never reads the token bytes — the
    // kubelet injects them into the ingester container at start.
    if let Some(secret) = auth_tokens_secret {
        env.push(EnvVar {
            name: "SIGLAKE_AUTH_TOKENS".into(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: secret.name.clone(),
                    key: secret.key.clone(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    let args = vec![
        "ingest-server".to_string(),
        "--bind".into(),
        "0.0.0.0:8088".into(),
        "--metrics-bind".into(),
        "0.0.0.0:9100".into(),
        "--otlp-grpc-listen".into(),
        "0.0.0.0:4317".into(),
        "--wal".into(),
        WAL_MOUNT_PATH.into(),
    ];
    deployment(
        cr,
        &name,
        &labels,
        replicas,
        Container {
            name: "ingester".into(),
            image: Some(cr.spec.image.clone()),
            args: Some(args),
            env: Some(env),
            ports: Some(named_ports(INGESTER_PORTS)),
            resources: Some(tier_resources(
                INGESTER_RESOURCES,
                cr.spec.resources.ingester.as_ref(),
            )),
            volume_mounts: Some(vec![wal_volume_mount()]),
            liveness_probe: Some(http_probe("/healthz", "ingest", 10, 10)),
            readiness_probe: Some(http_probe("/healthz", "ingest", 5, 5)),
            ..Default::default()
        },
        /* mounts_wal */ true,
        /* recreate */ false,
    )
}

pub fn compactor_deployment(cr: &SiglakeCluster, replicas: i32) -> Deployment {
    let name = name_of(cr, "compactor");
    let labels = component_labels(cr, "compactor");
    let mut env = base_env(cr);
    // Shipped write-feature defaults, stated rather than inherited. The chart
    // renders the same variables either way, so the operator-managed compactor
    // says what it runs. `spec.extraEnv` stays the escape hatch in both
    // directions — an opt-out for the on defaults, the opt-in for the
    // post-rewrite rebuild — because these entries are spliced before it and
    // Kubernetes resolves a duplicate name to the last entry.
    let defaults_at = env.len().saturating_sub(cr.spec.extra_env.len());
    env.splice(
        defaults_at..defaults_at,
        [
            EnvVar {
                name: "SIGLAKE_DELETE_TASKS".into(),
                value: Some("1".into()),
                ..Default::default()
            },
            EnvVar {
                name: "SIGLAKE_INVERTED_INDEX".into(),
                value: Some("1".into()),
                ..Default::default()
            },
            // Off since 2026-09-14 (#4162): a rewrite leaves its output's
            // index coverage as it found it.
            EnvVar {
                name: "SIGLAKE_INDEX_REBUILD".into(),
                value: Some("0".into()),
                ..Default::default()
            },
        ],
    );
    let mut args = vec![
        "compactor".to_string(),
        "--metrics-bind".into(),
        "0.0.0.0:9101".into(),
        "--wal".into(),
        WAL_MOUNT_PATH.into(),
        "--interval-secs".into(),
        "1".into(),
    ];
    let catalog_claim = uses_catalog_claim(&cr.spec.autoscaling.compactor);
    // A scale-out-capable policy always uses catalog claims, including while
    // the autoscaler currently holds it at one or zero replicas.
    if catalog_claim {
        // Reconciliation rejects a disabled mirror whenever the autoscaling
        // ceiling permits claim mode. Keep the empty fallback for direct
        // renderer callers: the CLI then refuses it instead of reading the
        // bucket root.
        let mirror_prefix = effective_wal_mirror_prefix(&cr.spec).unwrap_or_default();
        args.extend([
            "--catalog-claim".into(),
            "--mirror-prefix".into(),
            mirror_prefix.into(),
            "--catalog-claim-batch".into(),
            "256".into(),
        ]);
    }
    let resources = tier_resources(COMPACTOR_RESOURCES, cr.spec.resources.compactor.as_ref());
    deployment(
        cr,
        &name,
        &labels,
        replicas,
        Container {
            name: "compactor".into(),
            image: Some(cr.spec.image.clone()),
            args: Some(args),
            env: Some(env),
            ports: Some(named_ports(COMPACTOR_PORTS)),
            resources: Some(resources),
            volume_mounts: Some(vec![wal_volume_mount()]),
            ..Default::default()
        },
        /* mounts_wal */ true,
        // Filesystem ownership cannot tolerate a rollout surge. Catalog
        // claims coordinate overlapping revisions at every replica count.
        /* recreate */
        !catalog_claim,
    )
}

/// Query server as a **StatefulSet** (stable per-pod DNS via the headless
/// Service) so distributed query can address each shard's pod. Every pod
/// coordinates `/api/v1/sql` transparently.
///
/// #967: the peer list is no longer rendered. The pods resolve the headless
/// Service's `_http._tcp` SRV record at runtime and pin one membership per
/// query, so `autoscaling.query` is an ordinary range: `autoscaled_replicas`
/// is the scaling decision's output and is used as-is, and a replica the
/// decision adds receives shard work as soon as it is Ready — no rollout, and
/// no ceiling tied to the count that happened to be rendered.
pub fn query_statefulset(cr: &SiglakeCluster, autoscaled_replicas: i32) -> StatefulSet {
    let replicas = autoscaled_replicas;
    let name = name_of(cr, "query");
    let labels = component_labels(cr, "query");
    let mut env = base_env(cr);
    // Keep `spec.extraEnv` last: it is the operator escape hatch and Kubernetes
    // resolves a duplicate env name to the final entry.
    let defaults_at = env.len().saturating_sub(cr.spec.extra_env.len());
    env.splice(
        defaults_at..defaults_at,
        [
            EnvVar {
                name: "SIGLAKE_QUERY_SPILL_DIR".into(),
                value: Some(QUERY_SPILL_MOUNT_PATH.into()),
                ..Default::default()
            },
            EnvVar {
                name: "SIGLAKE_QUERY_SPILL_MAX_BYTES".into(),
                value: Some(QUERY_SPILL_MAX_BYTES.into()),
                ..Default::default()
            },
            EnvVar {
                name: "SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_BYTES".into(),
                value: Some(QUERY_FILE_CACHE_MAX_BYTES.into()),
                ..Default::default()
            },
            EnvVar {
                name: "SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_ENTRIES".into(),
                value: Some(QUERY_FILE_CACHE_MAX_ENTRIES.into()),
                ..Default::default()
            },
            // #967: the coordinator finds its OWN entry in the SRV answer by
            // pod name, and that URL is where a departed peer's shard is
            // retried. From the downward API, not `$HOSTNAME`: the CRI sets
            // the container's hostname but does not guarantee the environment
            // variable, and without a unique self match the pod never fans out.
            EnvVar {
                name: "SIGLAKE_QUERY_PEER_SELF_NAME".into(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.name".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            // Batch-job store, stated rather than left to the binary's
            // in-memory fallback (the chart defaults it on too). An
            // operator-managed query tier is a StatefulSet of 1..N pods behind
            // one Service, so a per-pod store answers 404 for a job another
            // pod is running, and loses every in-flight job on a rollout.
            // Empty when the catalog is not Postgres: there is no other store,
            // and a sqlite catalog file is per-pod storage, not a shared one.
            // `spec.extraEnv` stays the opt-out — spliced in AFTER this, so a
            // blank value there wins and reads as "in-memory".
            EnvVar {
                name: "SIGLAKE_JOBS_POSTGRES_URI".into(),
                value: Some(
                    jobs_store_uri(&cr.spec.catalog_uri)
                        .unwrap_or_default()
                        .to_string(),
                ),
                ..Default::default()
            },
        ],
    );
    let mut args = vec![
        "--bind".to_string(),
        "0.0.0.0:8089".into(),
        "--metrics-bind".into(),
        "0.0.0.0:9105".into(),
    ];
    // Rendered at EVERY replica count, deliberately: a one-member membership
    // takes the local path anyway, so scaling out later needs no rollout —
    // which is the point of moving off the rendered list. The scheme is
    // explicit because an SRV record carries a target and a port and no
    // scheme; the operator has no query TLS knob, so it is always `http`.
    args.push("--query-peer-discovery-srv".into());
    args.push(query_peer_srv(cr));
    args.push("--query-peer-scheme".into());
    args.push("http".into());
    let resources = tier_resources(QUERY_RESOURCES, cr.spec.resources.query.as_ref());
    let container = Container {
        name: "query-server".into(),
        image: Some(cr.spec.image.clone()),
        // The query server is its own binary, not a `siglake <sub>` form.
        command: Some(vec!["/usr/local/bin/siglake-query-server".into()]),
        args: Some(args),
        env: Some(env),
        ports: Some(named_ports(QUERY_PORTS)),
        resources: Some(resources),
        volume_mounts: Some(vec![query_spill_volume_mount()]),
        liveness_probe: Some(http_probe("/healthz", "http", 10, 10)),
        readiness_probe: Some(http_probe("/readyz", "http", 5, 5)),
        ..Default::default()
    };
    statefulset(StatefulSetArgs {
        cr,
        name: &name,
        labels: &labels,
        replicas,
        service_name: &name_of(cr, "query-headless"),
        container,
        volumes: vec![query_spill_volume()],
        volume_claim_templates: Vec::new(),
    })
}

/// The persistent batch-job store for a CR whose catalog is `catalog_uri`.
///
/// The store is the catalog database itself — there is no second connection to
/// configure — and the implementation is Postgres-only, so a CR pointed at an
/// sqlite catalog gets `None` and keeps the in-memory store rather than a pod
/// that crash-loops on `connect Postgres sqlite://…`. `starts_with("postgres")`
/// is the same test `IcebergContext::open_with_namespace` uses to pick its bind
/// style, so the two cannot disagree about what a Postgres catalog is.
fn jobs_store_uri(catalog_uri: &str) -> Option<&str> {
    let uri = catalog_uri.trim();
    uri.starts_with("postgres").then_some(uri)
}

/// The job-store URI the query pods effectively receive after `spec.extraEnv`
/// applies its last-value-wins override. A blank value selects the in-memory
/// store, matching the query-server's resolver.
pub(crate) fn effective_query_jobs_store_uri(
    spec: &crate::crd::SiglakeClusterSpec,
) -> Option<&str> {
    if let Some(value) = spec
        .extra_env
        .iter()
        .rev()
        .find(|entry| entry.name == "SIGLAKE_JOBS_POSTGRES_URI")
        .map(|entry| entry.value.trim())
    {
        return (!value.is_empty()).then_some(value);
    }
    jobs_store_uri(&spec.catalog_uri)
}

/// The SRV record the query pods resolve to discover each other, matching
/// `siglake.queryPeerSrv` in the Helm chart. The headless Service publishes
/// one SRV answer per READY pod on its named `http` port, so membership
/// follows the live replica count rather than the count that was rendered.
fn query_peer_srv(cr: &SiglakeCluster) -> String {
    let hl = name_of(cr, "query-headless");
    let ns = cr.metadata.namespace.as_deref().unwrap_or("default");
    format!("_http._tcp.{hl}.{ns}.svc.cluster.local")
}

fn deployment(
    cr: &SiglakeCluster,
    name: &str,
    labels: &BTreeMap<String, String>,
    replicas: i32,
    mut container: Container,
    mounts_wal: bool,
    recreate: bool,
) -> Deployment {
    // Container-level security: the chart's `securityContext` shape.
    // No privilege escalation, drop every capability. Doesn't break
    // anything we use (the binary is statically linked, no socket
    // binding under 1024).
    container.security_context = Some(SecurityContext {
        allow_privilege_escalation: Some(false),
        capabilities: Some(Capabilities {
            drop: Some(vec!["ALL".into()]),
            ..Default::default()
        }),
        ..Default::default()
    });
    // Graceful scale-down: preStop pause so the Service stops routing before
    // SIGTERM (the ingester then force-seals its active WAL; others drain).
    if container.lifecycle.is_none() {
        container.lifecycle = Some(prestop_sleep_lifecycle());
    }
    let owner = owner_ref(cr);
    let mut selector_labels = BTreeMap::new();
    selector_labels.insert(
        "app.kubernetes.io/name".to_string(),
        labels
            .get("app.kubernetes.io/name")
            .cloned()
            .unwrap_or_default(),
    );
    selector_labels.insert(
        "app.kubernetes.io/instance".to_string(),
        labels
            .get("app.kubernetes.io/instance")
            .cloned()
            .unwrap_or_default(),
    );
    selector_labels.insert(
        "app.kubernetes.io/component".to_string(),
        labels
            .get("app.kubernetes.io/component")
            .cloned()
            .unwrap_or_default(),
    );

    Deployment {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: cr.metadata.namespace.clone(),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(selector_labels.clone()),
                ..Default::default()
            },
            // A filesystem-drain compactor must NEVER overlap with its
            // successor on the shared local WAL (the chart uses Recreate for
            // exactly this); a surge pod racing the drain double-claims
            // segments. Catalog-claim revisions coordinate through the shared
            // table and roll safely, even while the policy is at one replica.
            strategy: Some(if recreate {
                DeploymentStrategy {
                    type_: Some("Recreate".into()),
                    rolling_update: None,
                }
            } else {
                DeploymentStrategy {
                    type_: Some("RollingUpdate".into()),
                    rolling_update: Some(RollingUpdateDeployment {
                        max_unavailable: Some(IntOrString::Int(0)),
                        max_surge: Some(IntOrString::Int(1)),
                    }),
                }
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    // Phase 4.13j: bind the per-CR
                    // ServiceAccount so IRSA-annotated SAs
                    // give the pod S3 PutObject perms. Without
                    // this, operator-rendered pods fall back to
                    // the node IAM role which doesn't have
                    // bucket-level grants.
                    service_account_name: optional_service_account(cr),
                    // Match the chart's `podSecurityContext`. The
                    // `fs_group: 65532` is the load-bearing setting
                    // — kubelet chowns the WAL PVC mount to gid
                    // 65532 so the non-root user can create
                    // `/var/lib/siglake/wal/active`. Without it the
                    // ingester pod crashloops with
                    // `Permission denied` on first start (bug #20,
                    // Phase 4.12.18).
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        run_as_user: Some(65532),
                        run_as_group: Some(65532),
                        fs_group: Some(65532),
                        ..Default::default()
                    }),
                    volumes: if mounts_wal {
                        Some(vec![wal_pod_volume(cr)])
                    } else {
                        None
                    },
                    termination_grace_period_seconds: Some(TERMINATION_GRACE_SECONDS),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

struct StatefulSetArgs<'a> {
    cr: &'a SiglakeCluster,
    name: &'a str,
    labels: &'a BTreeMap<String, String>,
    replicas: i32,
    /// Headless Service name for stable per-pod DNS (`StatefulSet.serviceName`).
    service_name: &'a str,
    container: Container,
    volumes: Vec<Volume>,
    volume_claim_templates: Vec<PersistentVolumeClaim>,
}

/// Shared StatefulSet builder — the analogue of [`deployment`] for the stateful
/// tiers (query + detection). `podManagementPolicy: Parallel` (symmetric pods,
/// no startup ordering); per-pod PVCs via `volume_claim_templates`.
fn statefulset(a: StatefulSetArgs) -> StatefulSet {
    let mut container = a.container;
    container.security_context = Some(SecurityContext {
        allow_privilege_escalation: Some(false),
        capabilities: Some(Capabilities {
            drop: Some(vec!["ALL".into()]),
            ..Default::default()
        }),
        ..Default::default()
    });
    if container.lifecycle.is_none() {
        container.lifecycle = Some(prestop_sleep_lifecycle());
    }
    let selector = LabelSelector {
        match_labels: Some(selector_labels_from(a.labels)),
        ..Default::default()
    };
    StatefulSet {
        metadata: ObjectMeta {
            name: Some(a.name.to_string()),
            namespace: a.cr.metadata.namespace.clone(),
            labels: Some(a.labels.clone()),
            owner_references: Some(vec![owner_ref(a.cr)]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(a.replicas),
            service_name: Some(a.service_name.to_string()),
            pod_management_policy: Some("Parallel".into()),
            update_strategy: Some(StatefulSetUpdateStrategy {
                type_: Some("RollingUpdate".into()),
                ..Default::default()
            }),
            selector,
            volume_claim_templates: (!a.volume_claim_templates.is_empty())
                .then_some(a.volume_claim_templates),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(a.labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    service_account_name: optional_service_account(a.cr),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        run_as_user: Some(65532),
                        run_as_group: Some(65532),
                        fs_group: Some(65532),
                        ..Default::default()
                    }),
                    volumes: (!a.volumes.is_empty()).then_some(a.volumes),
                    termination_grace_period_seconds: Some(TERMINATION_GRACE_SECONDS),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The three `app.kubernetes.io/{name,instance,component}` selector labels.
fn selector_labels_from(labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    [
        "app.kubernetes.io/name",
        "app.kubernetes.io/instance",
        "app.kubernetes.io/component",
    ]
    .into_iter()
    .map(|k| (k.to_string(), labels.get(k).cloned().unwrap_or_default()))
    .collect()
}

/// The ports each component exposes, as `(name, port)`.
///
/// ONE DEFINITION, used to build the container's ports AND the Service that
/// targets them. They used to be written out separately in two files, and a
/// rename landed in one of them: the operator's ingester Service targeted the
/// port's old name while the container declared `ingest`. A named `targetPort`
/// matching no container port is silently dropped by the endpoints controller,
/// so the EndpointSlice carried no port for 8088 and every connection to
/// `<cr>-ingester:8088` was refused — while the pods stayed Ready, because the
/// probes address the container port directly.
///
/// Deriving both sides from one constant makes that drift unrepresentable, and
/// `service_ports_match_container_ports` pins it.
pub const INGESTER_PORTS: &[(&str, i32)] =
    &[("ingest", 8088), ("otlp-grpc", 4317), ("metrics", 9100)];
pub const QUERY_PORTS: &[(&str, i32)] = &[("http", 8089), ("metrics", 9105)];
pub const COMPACTOR_PORTS: &[(&str, i32)] = &[("metrics", 9101)];

fn named_ports(spec: &[(&str, i32)]) -> Vec<ContainerPort> {
    spec.iter().map(|(n, p)| named_port(n, *p)).collect()
}

fn named_port(name: &str, port: i32) -> ContainerPort {
    ContainerPort {
        name: Some(name.into()),
        container_port: port,
        protocol: Some("TCP".into()),
        ..Default::default()
    }
}

/// A headless Service (`clusterIP: None`) governing a StatefulSet's pods —
/// stable per-pod DNS for distributed query + the `pod-index` shard label.
///
/// #967: this Service's SRV record is also the peer DIRECTORY, so it publishes
/// READY endpoints only. `publishNotReadyAddresses: true` used to let a
/// coordinator resolve a peer that was still warming up, which was harmless
/// while the peer list was rendered; as a membership source it would hand
/// shard work to pods that cannot serve it.
pub fn headless_service(cr: &SiglakeCluster, component: &str, ports: &[(&str, i32)]) -> Service {
    build_service(
        cr,
        &format!("{component}-headless"),
        component,
        ports,
        Some("None"),
        false,
    )
}

/// A ClusterIP Service fronting a component's pods (client traffic).
pub fn clusterip_service(cr: &SiglakeCluster, component: &str, ports: &[(&str, i32)]) -> Service {
    build_service(cr, component, component, ports, None, false)
}

fn build_service(
    cr: &SiglakeCluster,
    name_suffix: &str,
    selector_component: &str,
    ports: &[(&str, i32)],
    cluster_ip: Option<&str>,
    publish_not_ready: bool,
) -> Service {
    Service {
        metadata: ObjectMeta {
            name: Some(name_of(cr, name_suffix)),
            namespace: cr.metadata.namespace.clone(),
            labels: Some(component_labels(cr, selector_component)),
            owner_references: Some(vec![owner_ref(cr)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip: cluster_ip.map(|s| s.to_string()),
            publish_not_ready_addresses: publish_not_ready.then_some(true),
            selector: Some(selector_labels_from(&component_labels(
                cr,
                selector_component,
            ))),
            ports: Some(
                ports
                    .iter()
                    .map(|(n, p)| ServicePort {
                        name: Some((*n).into()),
                        port: *p,
                        target_port: Some(IntOrString::String((*n).into())),
                        protocol: Some("TCP".into()),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Render the WAL `PersistentVolumeClaim`, named `<cr.name>-wal`.
///
/// NOT owned by the CR, deliberately. It used to carry a controller
/// `ownerReference` with `blockOwnerDeletion`, so `kubectl delete
/// siglakecluster` cascaded into deleting the WAL — and acks are
/// WAL-append-based: a segment is durable here and NOT in Iceberg until the
/// compactor commits it, minutes under load, longer with commit batching and a
/// backlog. With `wal.mirror.enabled` off (the default) there is no S3 copy
/// either, so the cascade discarded acknowledged data.
///
/// Note the asymmetry that made this easy to miss: per-pod PVCs from
/// `volumeClaimTemplates` are deliberately KEPT by Kubernetes on StatefulSet
/// deletion. The one claim holding un-committed writes was the one that went.
///
/// The cost of not owning it is an orphaned PVC after the CR is deleted, which
/// an operator can see and remove deliberately. That is the right way round: a
/// leftover volume is a chore, a deleted WAL is data loss. `storage.
/// walRetainOnDelete: false` restores the cascade for a deployment that is
/// certain its WAL is mirrored or empty.
pub fn wal_pvc(cr: &SiglakeCluster) -> PersistentVolumeClaim {
    let name = format!("{}-wal", cr.metadata.name.as_deref().unwrap_or("siglake"));
    let labels = component_labels(cr, "wal");
    let storage_class = if cr.spec.storage.wal_storage_class_name.is_empty() {
        None
    } else {
        Some(cr.spec.storage.wal_storage_class_name.clone())
    };
    let mut requests = BTreeMap::new();
    requests.insert(
        "storage".to_string(),
        Quantity(cr.spec.storage.wal_size.clone()),
    );
    PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cr.metadata.namespace.clone(),
            labels: Some(labels),
            owner_references: if cr.spec.storage.wal_retain_on_delete {
                None
            } else {
                Some(vec![owner_ref(cr)])
            },
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec![cr.spec.storage.wal_access_mode.clone()]),
            storage_class_name: storage_class,
            resources: Some(VolumeResourceRequirements {
                requests: Some(requests),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn wal_volume_mount() -> VolumeMount {
    VolumeMount {
        name: WAL_VOLUME_NAME.into(),
        mount_path: WAL_MOUNT_PATH.into(),
        ..Default::default()
    }
}

fn wal_pod_volume(cr: &SiglakeCluster) -> Volume {
    Volume {
        name: WAL_VOLUME_NAME.into(),
        persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
            claim_name: format!("{}-wal", cr.metadata.name.as_deref().unwrap_or("siglake")),
            read_only: Some(false),
        }),
        ..Default::default()
    }
}

fn query_spill_volume_mount() -> VolumeMount {
    VolumeMount {
        name: QUERY_SPILL_VOLUME_NAME.into(),
        mount_path: QUERY_SPILL_MOUNT_PATH.into(),
        ..Default::default()
    }
}

fn query_spill_volume() -> Volume {
    Volume {
        name: QUERY_SPILL_VOLUME_NAME.into(),
        empty_dir: Some(EmptyDirVolumeSource {
            size_limit: Some(Quantity(QUERY_SPILL_SIZE_LIMIT.into())),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// `spec.serviceAccountName` reduced to the
/// `PodSpec::service_account_name` shape — `Some(name)` when set,
/// `None` (= namespace default SA) when empty. The kube-rs API
/// distinguishes "use the default" from "unset" only at the
/// `Option` level.
fn optional_service_account(cr: &SiglakeCluster) -> Option<String> {
    if cr.spec.service_account_name.is_empty() {
        None
    } else {
        Some(cr.spec.service_account_name.clone())
    }
}

pub fn name_of(cr: &SiglakeCluster, component: &str) -> String {
    format!(
        "{}-{component}",
        cr.metadata.name.as_deref().unwrap_or("siglake")
    )
}

pub fn component_labels(cr: &SiglakeCluster, component: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("app.kubernetes.io/name".into(), "siglake".into());
    m.insert(
        "app.kubernetes.io/instance".into(),
        cr.metadata.name.clone().unwrap_or_default(),
    );
    m.insert("app.kubernetes.io/component".into(), component.into());
    m.insert(
        "app.kubernetes.io/managed-by".into(),
        "siglake-operator".into(),
    );
    m
}

fn owner_ref(cr: &SiglakeCluster) -> OwnerReference {
    OwnerReference {
        api_version: SiglakeCluster::api_version(&()).into_owned(),
        kind: SiglakeCluster::kind(&()).into_owned(),
        name: cr.metadata.name.clone().unwrap_or_default(),
        uid: cr.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

fn base_env(cr: &SiglakeCluster) -> Vec<EnvVar> {
    let mut env = vec![
        EnvVar {
            name: "SIGLAKE_WAREHOUSE_URL".into(),
            value: Some(cr.spec.warehouse_url.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "SIGLAKE_CATALOG_URI".into(),
            value: Some(cr.spec.catalog_uri.clone()),
            ..Default::default()
        },
    ];
    // Phase 4.13j: opendal's S3 builder requires a region — both
    // for explicit static-key flows and for IRSA's
    // AssumeRoleWithWebIdentity sigv4 signing. The chart's
    // terraform-emitted helm_values surfaces this via
    // `s3.region` → AWS_REGION env on the chart-managed pods;
    // the operator now does the analogous wiring from
    // `spec.awsRegion`.
    if !cr.spec.aws_region.is_empty() {
        env.push(EnvVar {
            name: "AWS_REGION".into(),
            value: Some(cr.spec.aws_region.clone()),
            ..Default::default()
        });
    }
    // `spec.extraEnv` escape hatch: appended last so an operator-supplied
    // knob can also OVERRIDE a rendered default (Kubernetes takes the last
    // duplicate name in the list).
    for extra in &cr.spec.extra_env {
        env.push(EnvVar {
            name: extra.name.clone(),
            value: Some(extra.value.clone()),
            ..Default::default()
        });
    }
    env
}

/// Render a `CronJob` that periodically runs `siglake audit-rotate`.
///
/// Returns `None` when the CR opts out via
/// `spec.retention.queryAuditRotateIntervalDays == None`. When set,
/// the CronJob runs the same image as the main Deployments
/// (`spec.image`), invokes the `audit-rotate` subcommand with the
/// CR's `warehouseUrl` + `catalogUri`, and inherits the OwnerReference
/// so the CronJob is GC'd with the CR.
///
/// Schedule: a daily cron at midnight, multiplied to the configured
/// interval via `cron-utils`-style expansion. To keep the
/// implementation simple we use the most common cadences directly:
/// 1 day → "0 0 * * *", 7 days → "0 0 * * 0" (Sunday), 30 days →
/// "0 0 1 * *" (first of the month). The reconciler rejects every other value
/// because this renderer cannot represent it exactly.
pub fn audit_rotate_cronjob(cr: &SiglakeCluster) -> Option<CronJob> {
    let days = cr.spec.retention.query_audit_rotate_interval_days?;
    // query_audit uses the destructive drop+recreate (no --max-age-secs).
    let args = vec![
        "audit-rotate".to_string(),
        "--warehouse-url".into(),
        cr.spec.warehouse_url.clone(),
        "--catalog-uri".into(),
        cr.spec.catalog_uri.clone(),
    ];
    Some(rotate_cronjob(
        cr,
        "audit-rotate",
        args,
        cron_schedule_for_interval(days),
    ))
}

/// Build a retention CronJob running `siglake <args>` on `schedule`, named
/// `<cr>-<suffix>`, owned by the CR, inheriting the cluster image + base env.
fn rotate_cronjob(
    cr: &SiglakeCluster,
    suffix: &str,
    args: Vec<String>,
    schedule: String,
) -> CronJob {
    let name = name_of(cr, suffix);
    let labels = component_labels(cr, suffix);
    let owner = owner_ref(cr);
    let env = base_env(cr);
    let container = Container {
        name: suffix.into(),
        image: Some(cr.spec.image.clone()),
        args: Some(args),
        env: Some(env),
        resources: Some(tier_resources(JOB_RESOURCES, None)),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".into()]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    CronJob {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: cr.metadata.namespace.clone(),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(CronJobSpec {
            schedule,
            // No concurrent rotations — Forbid means a job that's
            // still running blocks the next scheduled one. The
            // rotate itself should complete in well under a minute
            // even with a large audit table, but better-safe.
            concurrency_policy: Some("Forbid".into()),
            // Garbage-collect old Jobs aggressively — we don't
            // need a 7-day audit-rotate-Job retention window.
            successful_jobs_history_limit: Some(1),
            failed_jobs_history_limit: Some(3),
            job_template: JobTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(JobSpec {
                    backoff_limit: Some(2),
                    template: PodTemplateSpec {
                        metadata: Some(ObjectMeta {
                            labels: Some(labels),
                            ..Default::default()
                        }),
                        spec: Some(PodSpec {
                            containers: vec![container],
                            restart_policy: Some("OnFailure".into()),
                            service_account_name: optional_service_account(cr),
                            security_context: Some(PodSecurityContext {
                                run_as_non_root: Some(true),
                                run_as_user: Some(65532),
                                run_as_group: Some(65532),
                                fs_group: Some(65532),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// A short stable digest of the inputs that determine a rendered Job's pod
/// template. A Job's `spec.template` is immutable, so anything that can change
/// the template must change the NAME too or the server-side apply 422s.
fn template_digest(parts: &[&str]) -> String {
    // FNV-1a: no dependency, and this needs to be stable across releases, not
    // cryptographic. Six hex chars is ample for distinguishing the handful of
    // templates one CR ever renders.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        for b in part.as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100_0000_01b3);
        }
        h ^= 0xff;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    format!("{:06x}", h & 0xff_ffff)
}

/// One-shot additive schema-migration Job, rendered when `spec.schemaVersion`
/// is set (and non-zero). Runs `siglake migrate-schema --all-tables
/// --all-namespaces`, which is additive-only and idempotent, so a retried or
/// duplicated run is safe.
///
/// The name carries the version AND a digest of everything that shapes the pod
/// template. A Job's `spec.template` is immutable, and the template is derived
/// from `spec.image`, the warehouse/catalog URIs and `base_env` (which
/// includes the `extraEnv` escape hatch) — so keying the name on the version
/// alone meant an image bump re-rendered a DIFFERENT template under the SAME
/// name. The API server rejects that with 422, and the reconciler returns Err
/// before `write_status`: the workloads upgrade while observedGeneration,
/// replicas, conditions and schemaVersion all freeze at their last successful
/// values, `kubectl wait` hangs, and it self-heals only when
/// `ttlSecondsAfterFinished` reaps the old Job up to a day later.
///
/// Including the digest also makes the migration RETRYABLE at a given version.
/// `migrate-schema` migrates to whatever schema the running binary declares,
/// so requesting `schemaVersion: 3` while the image is still the v2 binary
/// adds nothing and exits 0 — and with a version-only name, bumping the image
/// afterwards could never re-run it, leaving the correct migration unreachable
/// at v3 forever. With the digest, the new image renders a new Job name and
/// the migration actually runs.
pub fn migration_job(cr: &SiglakeCluster) -> Option<Job> {
    let version = cr.spec.schema_version?;
    if version == 0 {
        return None;
    }
    let digest = template_digest(&[
        &cr.spec.image,
        &cr.spec.warehouse_url,
        &cr.spec.catalog_uri,
        &cr.spec.aws_region.clone(),
        &cr.spec
            .extra_env
            .iter()
            .map(|e| format!("{}={}", e.name, e.value.clone()))
            .collect::<Vec<_>>()
            .join(","),
    ]);
    let suffix = format!("migrate-schema-v{version}-{digest}");
    let name = name_of(cr, &suffix);
    let labels = component_labels(cr, &suffix);
    let owner = owner_ref(cr);
    let args = vec![
        "migrate-schema".to_string(),
        "--all-tables".into(),
        // Tenancy is header-based, so `events` exists once per tenant
        // namespace. Migrating only the default namespace reports success
        // having left every other tenant's table narrow.
        "--all-namespaces".into(),
        "--warehouse-url".into(),
        cr.spec.warehouse_url.clone(),
        "--catalog-uri".into(),
        cr.spec.catalog_uri.clone(),
    ];
    let container = Container {
        name: "migrate-schema".into(),
        image: Some(cr.spec.image.clone()),
        args: Some(args),
        env: Some(base_env(cr)),
        resources: Some(tier_resources(JOB_RESOURCES, None)),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".into()]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    Some(Job {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: cr.metadata.namespace.clone(),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        spec: Some(JobSpec {
            backoff_limit: Some(4),
            // Reap the finished Job after a day so a long-lived cluster
            // doesn't accumulate one completed migration Job per version.
            ttl_seconds_after_finished: Some(86_400),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    restart_policy: Some("OnFailure".into()),
                    service_account_name: optional_service_account(cr),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        run_as_user: Some(65532),
                        run_as_group: Some(65532),
                        fs_group: Some(65532),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Pick a crontab expression for a validated retention interval.
fn cron_schedule_for_interval(days: u32) -> String {
    match days {
        1 => "0 0 * * *".into(), // every day at 00:00
        7 => "0 0 * * 0".into(), // every Sunday at 00:00
        // 30 days is the documented monthly cadence: the first of the month at
        // 00:00, because cron cannot express a sliding 30-day window.
        30 => "0 0 1 * *".into(),
        _ => unreachable!("retention interval must be validated before rendering"),
    }
}

/// Compactor memory limit (Mi) below which a given bin concurrency is unsafe.
///
/// Each in-flight bin holds its own decoded merge working set. A 2026-08-05
/// 200G round with `bin_concurrency=4` peaked at 12.03 GB of compaction-only
/// RSS (isolated by sampling the cgroup through settle, when ingest is done) —
/// about **4 GiB per bin**, on top of a ~1 GiB floor for the process. This
/// figure has been revised twice because it GROWS WITH CORPUS SIZE: 2.76 GiB
/// per bin measured at 200G, 3.36 GiB at 1TB. 4 GiB covers the 1TB measurement
/// with margin; a materially larger deployment should re-measure.
///
/// Note `3072 * n`, not `* (n - 1)`: the first bin needs its working set too.
/// Modelling it as free made the floor for 4 bins 10 GiB against a measured
/// 12.03 GiB, so the guard would have accepted a configuration that OOMs — and
/// it disagreed with `derive_bin_concurrency`, which divides `(limit - 1GB)` by
/// the per-bin budget and had it right.
fn compactor_memory_floor_mib(bin_concurrency: u32) -> u64 {
    1024 + 4096 * u64::from(bin_concurrency)
}

/// Check an explicit `SIGLAKE_COMPACTOR_BIN_CONCURRENCY` against the rendered
/// memory limit. Concurrent bins multiply peak memory, so the failure mode is
/// an OOMKill loop in which compaction silently stops. The reconciler turns a
/// violation into `InvalidSpec` before this renderer is called.
///
/// Pure so it is unit-testable without a cluster (the operator has no
/// cluster-level CI, which is why the kind job below exists).
fn bin_concurrency_memory_error(concurrency: u32, limit_mib: u64) -> Option<String> {
    if concurrency <= 1 {
        return None;
    }
    let floor = compactor_memory_floor_mib(concurrency);
    if limit_mib >= floor {
        return None;
    }
    Some(format!(
        "spec.extraEnv sets SIGLAKE_COMPACTOR_BIN_CONCURRENCY={concurrency} but the compactor's \
         memory limit is {limit_mib}Mi; budget at least {floor}Mi (1Gi base plus ~4Gi per \
         concurrent bin, INCLUDING the first). Under-provisioned, the compactor OOMKills \
         instead of compacting faster."
    ))
}

/// Validate the compactor concurrency knob the operator understands. Unknown
/// `extraEnv` remains opaque, but this knob has a load-bearing relationship to
/// `spec.resources.compactor.limits.memory` and therefore cannot be allowed to
/// fall back or merely warn.
pub(crate) fn compactor_bin_concurrency_error(
    spec: &crate::crd::SiglakeClusterSpec,
) -> Option<(&'static str, String)> {
    let configured = spec
        .extra_env
        .iter()
        .rev()
        .find(|e| e.name == "SIGLAKE_COMPACTOR_BIN_CONCURRENCY")?;
    let concurrency = match configured.value.parse::<u32>() {
        Ok(value) if value >= 1 => value,
        _ => {
            return Some((
                "CompactorBinConcurrencyInvalid",
                format!(
                    "spec.extraEnv SIGLAKE_COMPACTOR_BIN_CONCURRENCY={} must be a positive integer",
                    configured.value
                ),
            ));
        }
    };
    let resources = tier_resources(COMPACTOR_RESOURCES, spec.resources.compactor.as_ref());
    let limit_mib = memory_limit_mib(&resources)?;
    let message = bin_concurrency_memory_error(concurrency, limit_mib)?;
    Some(("CompactorBinConcurrencyExceedsMemory", message))
}

/// Largest data file the default compaction policy emits, in MiB.
///
/// `ReclusterPolicy::cold_target_file_bytes` is 256 MiB and
/// `target_bytes_for_age` clamps every target to `max_pass_bytes`, also
/// 256 MiB — so under the packaged configuration nothing bigger than this is
/// written. A deployment that raises `max_pass_bytes` writes larger files and
/// needs a proportionally larger query limit than this guard asks for.
const QUERY_COMPACTED_FILE_MIB: u64 = 256;

/// `SIGLAKE_SCAN_DECOMPRESSION_FACTOR`'s default
/// (`DEFAULT_SCAN_DECOMPRESSION_FACTOR`, siglake-storage `query_provider.rs`).
const QUERY_SCAN_DECOMPRESSION_FACTOR: u64 = 5;

/// One file's decode working set as the scan itself estimates it:
/// `estimated_decoded_bytes_per_file` = compressed bytes per file ×
/// decompression factor. This is the number `reserve_decode_budget` tries to
/// grow the pool reservation by, so it is the number the pool has to hold.
const QUERY_DECODE_PER_FILE_MIB: u64 = QUERY_COMPACTED_FILE_MIB * QUERY_SCAN_DECOMPRESSION_FACTOR;

/// Smallest `spec.resources.query.limits.memory` (MiB) whose derived pool holds
/// [`QUERY_DECODE_PER_FILE_MIB`]. Pinned by
/// `query_memory_floor_is_the_packaged_default`, which searches for it rather
/// than trusting this constant.
const QUERY_MEMORY_FLOOR_MIB: u64 = 4096;

/// The DataFusion memory pool the query server will derive from a container
/// memory limit of `limit_mib`, in MiB.
///
/// Mirrors siglake-storage: `derive_read_cache_bytes` takes limit/4 for the
/// object cache (64 MiB … 16 GiB), `derive_metadata_cache_bytes` takes limit/8
/// for footers/file-lists/aggregates (64 MiB … 2 GiB),
/// `derive_text_index_cache_bytes` takes limit/16 + limit/64 for parsed
/// inverted indexes and their Puffin blobs but only out of what is left ABOVE
/// one file's decode estimate, and `query_pool_bytes_full` gives the pool
/// `SIGLAKE_QUERY_MEMORY_FRACTION` (0.5) of the remainder — never less than a
/// quarter of the whole limit, and never below 256 MiB. Duplicated rather than
/// imported: the operator does not depend on siglake-storage, and the test
/// below pins the two together at the packaged 4Gi.
///
/// At and below the floor this is simply `limit × 0.3125`, because the
/// text-index caches take nothing there; above it they take their share and
/// the pool keeps the decode estimate by construction.
///
/// In BYTES, like the code it mirrors, then rounded down to MiB: the cache
/// shares are limit/4 and limit/8, so doing it in whole MiB truncates each
/// share upward and hands the pool tens of MiB it will not have — enough to
/// move the floor off 4Gi.
fn query_memory_pool_mib(limit_mib: u64) -> u64 {
    const MIB: u64 = 1024 * 1024;
    let limit = limit_mib.saturating_mul(MIB);
    let object_cache = (limit / 4).clamp(64 * MIB, 16 * 1024 * MIB);
    let metadata_cache = (limit / 8).clamp(64 * MIB, 2 * 1024 * MIB);
    let text_index_caches = query_text_index_cache_bytes(limit, object_cache + metadata_cache);
    let after_caches = limit.saturating_sub(object_cache + metadata_cache + text_index_caches);
    // `query_pool_bytes_full`'s floor is a share of the WHOLE limit, so cache
    // configuration cannot squeeze the pool to nothing.
    (after_caches.max(limit / 4) / 2).max(256 * MIB) / MIB
}

/// The text-index caches siglake-storage's `derive_text_index_cache_bytes`
/// would take, in bytes: limit/16 parsed plus limit/64 of Puffin blobs, capped
/// at 1 GiB and 256 MiB, and taken only from what remains once the pool can
/// still reserve one file's decode working set. Zero at the packaged floor,
/// which is what keeps that floor at 4Gi.
fn query_text_index_cache_bytes(limit: u64, other_caches: u64) -> u64 {
    const MIB: u64 = 1024 * 1024;
    let parsed = (limit / 16).clamp(64 * MIB, 1024 * MIB);
    let blob = (limit / 64).clamp(16 * MIB, 256 * MIB);
    let room = limit
        .saturating_sub(other_caches)
        .saturating_sub(2 * QUERY_DECODE_PER_FILE_MIB * MIB);
    if parsed + blob <= room {
        return parsed + blob;
    }
    let (parsed, blob) = (room / 5 * 4, room / 5);
    if parsed < 64 * MIB || blob < 16 * MIB {
        return 0;
    }
    parsed + blob
}

/// Describe when the rendered query memory limit is too small for one file's
/// decode working set. The query analogue of [`bin_concurrency_memory_error`]
/// becomes an advisory condition, not an `InvalidSpec`, because the failure
/// mode is different.
///
/// DERIVATION. `reserve_decode_budget` (siglake-storage `query_provider.rs`)
/// reserves `concurrency × estimated_decoded_bytes_per_file` from the shared
/// pool and halves the concurrency until it fits. At concurrency 1 it gives up
/// and proceeds WITHOUT a reservation, counting
/// `siglake_query_scan_decode_reservation_total{outcome="unreserved"}`. So the
/// condition worth warning about is: pool share × limit cannot hold ONE file's
/// estimate. With the packaged compaction policy that estimate is 256 MiB ×
/// 5 = 1.25 GiB, and the pool is 0.3125 × the limit — which meet exactly at
/// 4Gi, the packaged default. Anything smaller can never reserve a first file.
///
/// NOT AN ERROR. The unreserved fallback is the safe state, not a fault: a
/// 2026-08-22 1TB round found that making reservations cheaper took the process
/// from 8.8 GiB RSS to a 31.6 GiB OOM-kill, so falling back to one file at a
/// time is what keeps a small pod alive.
///
/// WHAT IT ACTUALLY COSTS — MEASURED, 2026-09-06. This used to say the cost was
/// scan parallelism, "a speed cliff". It is not. Sweeping the pool over 80× around
/// one file's estimate on a decode-bound scan (9.87 GB decoded per query) moved
/// the median from 1.777 s to 1.949 s — the STARVED side being the faster one —
/// and at the boundary itself, 0.8× vs 1.2× the estimate, the ratio is 0.98×,
/// inside the run-to-run spread. The control says the measurement can see
/// parallelism when there is any: pinning per-partition file concurrency to 1
/// with an unconstrained pool costs nothing (1.767 s), while dropping partition
/// fan-out from 4 to 1 costs 3.6× (6.433 s). Per-partition file concurrency
/// overlaps object-store waits; it does not spread decode across cores. The
/// pool governs it; the pool does not govern partition fan-out.
///
/// So what an undersized limit costs is ACCOUNTING: below the floor the decode
/// working set is held outside the pool, so the one number that is supposed to
/// bound the process no longer sees it. That is worth a warning and is not
/// worth an `InvalidSpec` — but do not promise a speed recovery from raising
/// the limit. Not settled here: local storage was the only backend, and
/// overlapping S3 first-byte latency is the one case where file concurrency
/// could still pay.
///
/// Pure so it is unit-testable without a cluster.
fn query_memory_warning(limit_mib: u64) -> Option<String> {
    let pool_mib = query_memory_pool_mib(limit_mib);
    if pool_mib >= QUERY_DECODE_PER_FILE_MIB {
        return None;
    }
    Some(format!(
        "the query memory limit is {limit_mib}Mi, which derives a ~{pool_mib}Mi DataFusion memory \
         pool; one compacted data file's decode working set is estimated at \
         {QUERY_DECODE_PER_FILE_MIB}Mi ({QUERY_COMPACTED_FILE_MIB}Mi compacted × \
         {QUERY_SCAN_DECOMPRESSION_FACTOR} decompression), so no scan can reserve even its FIRST \
         file: every scan reads one file at a time AND holds that file's decode buffers outside \
         the pool, where the bound that is supposed to keep this process inside its limit cannot \
         see them. Measured on 2026-09-06, running one file at a time costs no wall time on local \
         storage, so raise the limit for the accounting, not for speed. Budget at least \
         {QUERY_MEMORY_FLOOR_MIB}Mi (the packaged default)."
    ))
}

/// Describe an effective query memory limit that cannot account for one
/// compacted file's decode working set. The reconciler turns this into the
/// non-blocking `QueryMemoryUndersized` condition; workload rendering itself
/// stays free of reconcile-by-reconcile warning side effects.
pub(crate) fn query_memory_advisory(spec: &crate::crd::SiglakeClusterSpec) -> Option<String> {
    let resources = tier_resources(QUERY_RESOURCES, spec.resources.query.as_ref());
    memory_limit_mib(&resources).and_then(query_memory_warning)
}

/// `limits.memory` of a rendered container, in MiB.
fn memory_limit_mib(resources: &ResourceRequirements) -> Option<u64> {
    let q = resources.limits.as_ref()?.get("memory")?;
    quantity_mib(&q.0)
}

/// Parse a Kubernetes memory quantity (`1Gi`, `1536Mi`, `2G`, `1.5Gi`,
/// `1073741824`) to whole MiB, rounding down. `None` for anything that is not
/// a number with an optional binary (`Ki` … `Ei`) or decimal (`k` … `E`)
/// suffix; the milli suffix is meaningless for memory and is rejected too.
pub fn quantity_mib(q: &str) -> Option<u64> {
    const SUFFIXES: &[(&str, f64)] = &[
        ("Ki", 1024.0),
        ("Mi", 1024.0 * 1024.0),
        ("Gi", 1024.0 * 1024.0 * 1024.0),
        ("Ti", 1024.0 * 1024.0 * 1024.0 * 1024.0),
        ("Pi", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
        ("Ei", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
        ("k", 1e3),
        ("M", 1e6),
        ("G", 1e9),
        ("T", 1e12),
        ("P", 1e15),
        ("E", 1e18),
    ];
    let q = q.trim();
    let (number, scale) = SUFFIXES
        .iter()
        .find_map(|(suffix, scale)| q.strip_suffix(suffix).map(|n| (n, *scale)))
        .unwrap_or((q, 1.0));
    if number.is_empty() || !number.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let value: f64 = number.parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    Some((value * scale / (1024.0 * 1024.0)).floor() as u64)
}

/// The container `resources` for a tier: the packaged defaults with
/// `spec.resources.<tier>` merged over them key by key.
///
/// Merge, not replace, for two reasons. The packaged numbers are load-bearing
/// — the query server sizes itself from the memory limit and the compactor
/// guard reads it — so raising one number must not silently drop the others.
/// And it is how the chart behaves: `--set query.resources.limits.memory=8Gi`
/// deep-merges over values.yaml, so an adopted release's `resources` block
/// (`adopt.rs`) means the same thing on both sides. The cost is that a default
/// cannot be REMOVED through the CR, only changed.
pub fn tier_resources(
    defaults: ResourceDefaults,
    overrides: Option<&TierResources>,
) -> ResourceRequirements {
    let mut requests = BTreeMap::new();
    requests.insert(
        "memory".to_string(),
        Quantity(defaults.request_memory.into()),
    );
    requests.insert("cpu".to_string(), Quantity(defaults.request_cpu.into()));
    let mut limits = BTreeMap::new();
    limits.insert("memory".to_string(), Quantity(defaults.limit_memory.into()));
    limits.insert("cpu".to_string(), Quantity(defaults.limit_cpu.into()));
    if let Some(value) = defaults.request_ephemeral_storage {
        requests.insert("ephemeral-storage".to_string(), Quantity(value.into()));
    }
    if let Some(value) = defaults.limit_ephemeral_storage {
        limits.insert("ephemeral-storage".to_string(), Quantity(value.into()));
    }
    if let Some(o) = overrides {
        for (k, v) in &o.requests {
            requests.insert(k.clone(), Quantity(v.clone()));
        }
        for (k, v) in &o.limits {
            limits.insert(k.clone(), Quantity(v.clone()));
        }
    }
    ResourceRequirements {
        requests: Some(requests),
        limits: Some(limits),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{AutoscalingSpec, ComponentAutoscale, SiglakeCluster, SiglakeClusterSpec};
    use kube::core::ObjectMeta as KubeMeta;

    /// Concurrent compaction bins multiply the compactor's peak memory. Raising
    /// `SIGLAKE_COMPACTOR_BIN_CONCURRENCY` through `spec.extraEnv` without
    /// raising the memory limit yields an OOMKill loop — compaction silently
    /// stops, which nothing at runtime distinguishes from "nothing to compact".
    /// The chart fails the install; the operator now rejects the CR too.
    #[test]
    fn bin_concurrency_errors_only_when_memory_is_short() {
        // Default concurrency: always fits, whatever the limit.
        assert!(bin_concurrency_memory_error(1, 512).is_none());

        // Raised concurrency on the packaged 1Gi limit: fails, and says what to do.
        let w = bin_concurrency_memory_error(4, 1024).expect("4 bins on a 1Gi limit must fail");
        assert!(w.contains("BIN_CONCURRENCY=4"), "{w}");
        assert!(w.contains("17408Mi"), "must state the required floor: {w}");

        // 4Gi is nowhere near enough at ~4GiB/bin (floor for 4 bins is 17Gi).
        assert!(bin_concurrency_memory_error(4, 4096).is_some());
        // 14Gi is BELOW the measured 14.44 GiB 1TB peak — must still fail. An
        // earlier floor passed this, which is how it shipped 10% too low twice.
        assert!(bin_concurrency_memory_error(4, 14336).is_some());
        assert!(bin_concurrency_memory_error(4, 20480).is_none());
        // This floor and siglake-storage's `derive_bin_concurrency` are two
        // views of one policy and MUST agree, but the operator does not depend
        // on siglake-storage, so the link is pinned by value on both sides.
        // `derive` computes (limit - 1GiB) / 3GiB; at exactly this floor it must
        // be willing to pick n. The mirror assertion lives in
        // `bin_concurrency_derives_from_memory_and_cpus`.
        for n in 1u32..=4 {
            let floor_gib = compactor_memory_floor_mib(n) / 1024;
            assert_eq!(
                (floor_gib - 1) / 4,
                u64::from(n),
                "floor for {n} bins must be exactly what the derivation inverts"
            );
        }
        // Exactly at the floor is enough.
        let floor = compactor_memory_floor_mib(4);
        assert!(bin_concurrency_memory_error(4, floor).is_none());
        assert!(bin_concurrency_memory_error(4, floor - 1).is_some());
    }

    /// The autoscaler reads each scalable tier's current replica count from a
    /// typed API, and that type has to be the kind `render` actually creates.
    ///
    /// It did not, for query: `current_replicas` issued a Deployment GET against
    /// a StatefulSet. That does not error — the GET 404s, 404 is treated as "not
    /// provisioned yet, start from zero", and the tier stays pinned at `min`
    /// forever with status permanently Progressing. Nothing at runtime
    /// distinguishes that from a genuinely absent workload.
    ///
    /// This pins the names and kinds the reconciler depends on. It is a
    /// STRUCTURAL pin, not a live one — it cannot catch the reconciler querying
    /// the wrong API, only a rename or a kind flip on the render side. Closing
    /// that properly needs an envtest/kind gate in CI, which does not exist yet.
    #[test]
    fn scalable_tier_names_and_kinds_match_what_the_reconciler_reads() {
        let mut cr = sample_cr();
        cr.spec.autoscaling.query = ComponentAutoscale {
            min: 2,
            max: 2,
            target: 4.0,
        };
        let base = cr.metadata.name.clone().unwrap();

        // Deployments — read via `deployment_replicas`.
        let ingester: Deployment = ingester_deployment(&cr, 1, None);
        let compactor: Deployment = compactor_deployment(&cr, 1);
        assert_eq!(
            ingester.metadata.name.as_deref(),
            Some(&*format!("{base}-ingester"))
        );
        assert_eq!(
            compactor.metadata.name.as_deref(),
            Some(&*format!("{base}-compactor"))
        );

        // StatefulSet — read via `statefulset_replicas`. Query is a StatefulSet
        // because distributed fan-out addresses peers by stable per-pod DNS;
        // moving it back to a Deployment would break that AND the autoscaler.
        let query: StatefulSet = query_statefulset(&cr, 2);
        assert_eq!(
            query.metadata.name.as_deref(),
            Some(&*format!("{base}-query"))
        );
        assert_eq!(
            query.spec.as_ref().and_then(|s| s.replicas),
            Some(2),
            "the reconciler reads spec.replicas back off this object"
        );
    }

    fn sample_cr() -> SiglakeCluster {
        SiglakeCluster {
            metadata: KubeMeta {
                name: Some("acme".into()),
                namespace: Some("siglake-prod".into()),
                uid: Some("abc-123".into()),
                ..Default::default()
            },
            spec: SiglakeClusterSpec {
                image: "ghcr.io/siglake/siglake:0.1.0".into(),
                warehouse_url: "s3://acme/warehouse".into(),
                catalog_uri: "postgres://k/db".into(),
                autoscaling: AutoscalingSpec {
                    compactor: ComponentAutoscale {
                        min: 1,
                        max: 4,
                        target: 5.0,
                    },
                    ..Default::default()
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
            },
            status: None,
        }
    }

    fn container_resources(template: PodTemplateSpec) -> ResourceRequirements {
        template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap()
            .resources
            .unwrap()
    }

    fn quantity_of(res: &ResourceRequirements, side: &str, key: &str) -> Option<String> {
        let map = match side {
            "requests" => res.requests.as_ref(),
            "limits" => res.limits.as_ref(),
            _ => unreachable!(),
        };
        map.and_then(|m| m.get(key)).map(|q| q.0.clone())
    }

    /// The operator's packaged resources ARE the chart's (values.yaml
    /// `<tier>.resources`), tier by tier. The query tier is the one that
    /// drifted: cb0d0e1 raised the chart to 4Gi because the read caches and
    /// the memory pool derive from the pod limit, and the operator stayed at
    /// 2Gi — half the pool every published number was measured with, with no
    /// CR field to raise it (audit item 28, task #545).
    #[test]
    fn packaged_resources_match_the_chart_tier_by_tier() {
        let cr = sample_cr();
        let query = container_resources(query_statefulset(&cr, 1).spec.unwrap().template);
        assert_eq!(
            quantity_of(&query, "limits", "memory").as_deref(),
            Some("4Gi"),
            "values.yaml query.resources.limits.memory"
        );
        assert_eq!(quantity_of(&query, "limits", "cpu").as_deref(), Some("2"));
        assert_eq!(
            quantity_of(&query, "limits", "ephemeral-storage").as_deref(),
            Some("12Gi")
        );
        assert_eq!(
            quantity_of(&query, "requests", "memory").as_deref(),
            Some("256Mi")
        );
        assert_eq!(
            quantity_of(&query, "requests", "cpu").as_deref(),
            Some("200m")
        );
        assert_eq!(
            quantity_of(&query, "requests", "ephemeral-storage").as_deref(),
            Some("10Gi")
        );

        for (tier, res) in [
            (
                "ingester",
                container_resources(ingester_deployment(&cr, 1, None).spec.unwrap().template),
            ),
            (
                "compactor",
                container_resources(compactor_deployment(&cr, 1).spec.unwrap().template),
            ),
        ] {
            assert_eq!(
                quantity_of(&res, "limits", "memory").as_deref(),
                Some("1Gi"),
                "{tier}: values.yaml {tier}.resources.limits.memory"
            );
            assert_eq!(
                quantity_of(&res, "limits", "cpu").as_deref(),
                Some("2"),
                "{tier}"
            );
            assert_eq!(
                quantity_of(&res, "requests", "memory").as_deref(),
                Some("256Mi"),
                "{tier}"
            );
            assert_eq!(
                quantity_of(&res, "requests", "cpu").as_deref(),
                Some("200m"),
                "{tier}"
            );
        }
    }

    /// `spec.resources.<tier>` merges over the packaged defaults per key:
    /// raising the query memory limit keeps the CPU limit and both requests,
    /// the packaged ephemeral-storage limit can be raised independently, and
    /// an untouched tier is unaffected.
    #[test]
    fn resources_override_merges_over_the_defaults() {
        let mut cr = sample_cr();
        cr.spec.resources.query = Some(TierResources {
            requests: BTreeMap::from([("memory".to_string(), "1Gi".to_string())]),
            limits: BTreeMap::from([
                ("memory".to_string(), "8Gi".to_string()),
                ("ephemeral-storage".to_string(), "20Gi".to_string()),
            ]),
        });
        let query = container_resources(query_statefulset(&cr, 1).spec.unwrap().template);
        assert_eq!(
            quantity_of(&query, "limits", "memory").as_deref(),
            Some("8Gi")
        );
        assert_eq!(
            quantity_of(&query, "limits", "ephemeral-storage").as_deref(),
            Some("20Gi")
        );
        assert_eq!(
            quantity_of(&query, "limits", "cpu").as_deref(),
            Some("2"),
            "an override of memory must not drop the packaged cpu limit"
        );
        assert_eq!(
            quantity_of(&query, "requests", "memory").as_deref(),
            Some("1Gi")
        );
        assert_eq!(
            quantity_of(&query, "requests", "cpu").as_deref(),
            Some("200m"),
            "requests not named in the override keep their defaults"
        );

        // The other tiers render their defaults untouched.
        let ingester =
            container_resources(ingester_deployment(&cr, 1, None).spec.unwrap().template);
        assert_eq!(
            quantity_of(&ingester, "limits", "memory").as_deref(),
            Some("1Gi")
        );

        // An override on the compactor reaches the compactor.
        cr.spec.resources.compactor = Some(TierResources {
            limits: BTreeMap::from([("memory".to_string(), "17Gi".to_string())]),
            ..Default::default()
        });
        let compactor = container_resources(compactor_deployment(&cr, 1).spec.unwrap().template);
        assert_eq!(
            quantity_of(&compactor, "limits", "memory").as_deref(),
            Some("17Gi")
        );
    }

    /// The compactor's concurrency warning must read the limit the render
    /// emits. With `spec.resources.compactor` in play the packaged 1Gi is no
    /// longer the truth: a user who raised the limit to hold four bins must
    /// not be rejected, and one who raised concurrency without the limit must.
    #[test]
    fn compactor_validation_reads_the_effective_memory_limit() {
        let mut cr = sample_cr();
        cr.spec.extra_env = vec![crate::crd::ExtraEnvVar {
            name: "SIGLAKE_COMPACTOR_BIN_CONCURRENCY".into(),
            value: "4".into(),
        }];
        assert!(compactor_bin_concurrency_error(&cr.spec).is_some());

        cr.spec.resources.compactor = Some(TierResources {
            limits: BTreeMap::from([("memory".to_string(), "20Gi".to_string())]),
            ..Default::default()
        });
        let raised = tier_resources(COMPACTOR_RESOURCES, cr.spec.resources.compactor.as_ref());
        assert_eq!(memory_limit_mib(&raised), Some(20 * 1024));
        assert!(
            compactor_bin_concurrency_error(&cr.spec).is_none(),
            "20Gi holds 4 bins; rejecting it would use the wrong fixed limit"
        );

        // A limit the operator cannot read makes no claim either way.
        cr.spec.resources.compactor = Some(TierResources {
            limits: BTreeMap::from([("memory".to_string(), "lots".to_string())]),
            ..Default::default()
        });
        let junk = tier_resources(COMPACTOR_RESOURCES, cr.spec.resources.compactor.as_ref());
        assert!(compactor_bin_concurrency_error(&cr.spec).is_none());
        assert!(memory_limit_mib(&junk).is_none());
    }

    /// The two derivations the warning sits between — the pool share the query
    /// server computes and the per-file decode estimate the scan reserves —
    /// meet at the packaged 4Gi. Searched rather than asserted against
    /// `QUERY_MEMORY_FLOOR_MIB` so that changing either side (the cache shares,
    /// the memory fraction, the compaction target, the decompression factor)
    /// fails HERE instead of silently moving the threshold.
    #[test]
    fn query_memory_floor_is_the_packaged_default() {
        // The pool the query server derives at the packaged limit, MiB.
        assert_eq!(query_memory_pool_mib(4096), 1280);
        assert_eq!(QUERY_DECODE_PER_FILE_MIB, 1280);

        let smallest_clean = (256..=16 * 1024)
            .find(|mib| query_memory_warning(*mib).is_none())
            .expect("some limit in range clears the floor");
        assert_eq!(
            smallest_clean, QUERY_MEMORY_FLOOR_MIB,
            "the floor moved; update QUERY_MEMORY_FLOOR_MIB and the QUERY_RESOURCES / values.yaml \
             sizing notes together"
        );
        assert_eq!(
            quantity_mib(QUERY_RESOURCES.limit_memory),
            Some(QUERY_MEMORY_FLOOR_MIB),
            "the packaged default must not sit below its own floor"
        );

        // The text-index caches (#4056) are why the floor did NOT move when
        // they entered the budget: they take only what is left above the decode
        // estimate, which at the floor is nothing.
        const MIB: u64 = 1024 * 1024;
        let caches = |limit_mib: u64| {
            let limit = limit_mib * MIB;
            query_text_index_cache_bytes(limit, limit / 4 + limit / 8) / MIB
        };
        assert_eq!(caches(QUERY_MEMORY_FLOOR_MIB), 0);
        assert_eq!(caches(5 * 1024), 400, "a 5Gi pod holds the derived budget");
        assert!(
            query_memory_pool_mib(5 * 1024) >= QUERY_DECODE_PER_FILE_MIB,
            "a pod above the floor must keep the decode estimate as well as its caches"
        );
    }

    /// The guard must read the limit the render emits, so an override is what
    /// it judges — and must make no claim about a limit it cannot parse.
    #[test]
    fn query_memory_warning_reads_the_effective_memory_limit() {
        let mut cr = sample_cr();
        let packaged = tier_resources(QUERY_RESOURCES, cr.spec.resources.query.as_ref());
        assert_eq!(memory_limit_mib(&packaged), Some(4096));
        assert!(
            memory_limit_mib(&packaged)
                .and_then(query_memory_warning)
                .is_none(),
            "the packaged default must not warn about itself"
        );

        cr.spec.resources.query = Some(TierResources {
            limits: BTreeMap::from([("memory".to_string(), "2Gi".to_string())]),
            ..Default::default()
        });
        let halved = tier_resources(QUERY_RESOURCES, cr.spec.resources.query.as_ref());
        let message = memory_limit_mib(&halved)
            .and_then(query_memory_warning)
            .expect("2Gi cannot hold one file's decode estimate");
        assert!(message.contains("2048Mi"), "{message}");
        assert!(message.contains("640Mi"), "{message}");
        assert!(message.contains("1280Mi"), "{message}");
        // Rendering still succeeds: this warns, it does not reject.
        let rendered = container_resources(query_statefulset(&cr, 1).spec.unwrap().template);
        assert_eq!(
            quantity_of(&rendered, "limits", "memory").as_deref(),
            Some("2Gi")
        );

        cr.spec.resources.query = Some(TierResources {
            limits: BTreeMap::from([("memory".to_string(), "plenty".to_string())]),
            ..Default::default()
        });
        let junk = tier_resources(QUERY_RESOURCES, cr.spec.resources.query.as_ref());
        assert!(memory_limit_mib(&junk).is_none());
    }

    #[test]
    fn quantity_mib_parses_kubernetes_memory_quantities() {
        assert_eq!(quantity_mib("1Gi"), Some(1024));
        assert_eq!(quantity_mib("4Gi"), Some(4096));
        assert_eq!(quantity_mib("1536Mi"), Some(1536));
        assert_eq!(quantity_mib("1.5Gi"), Some(1536));
        assert_eq!(quantity_mib("1048576Ki"), Some(1024));
        assert_eq!(quantity_mib("1073741824"), Some(1024));
        assert_eq!(quantity_mib(" 2Gi "), Some(2048));
        // Decimal suffixes are decimal: 1G is 1e9 bytes, 953 MiB rounded down.
        assert_eq!(quantity_mib("1G"), Some(953));
        assert_eq!(quantity_mib("2000M"), Some(1907));
        assert_eq!(quantity_mib("1Ti"), Some(1024 * 1024));
        // Not a memory quantity.
        assert_eq!(quantity_mib(""), None);
        assert_eq!(quantity_mib("lots"), None);
        assert_eq!(quantity_mib("Gi"), None);
        assert_eq!(quantity_mib("-1Gi"), None);
        assert_eq!(quantity_mib("500m"), None);
        assert_eq!(quantity_mib("1GiB"), None);
    }

    #[test]
    fn ingester_enables_otlp_grpc_on_the_standard_port() {
        let d = ingester_deployment(&sample_cr(), 1, None);
        let container = d
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap();
        let args = container.args.unwrap();
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--otlp-grpc-listen", "0.0.0.0:4317"]),
            "operator ingester does not enable OTLP/gRPC on 4317: {args:?}"
        );
        assert!(INGESTER_PORTS.contains(&("otlp-grpc", 4317)));
    }

    #[test]
    fn ingester_has_health_probes_and_graceful_shutdown() {
        let d = ingester_deployment(&sample_cr(), 1, None);
        let pod = d.spec.unwrap().template.spec.unwrap();
        // Graceful scale-down: grace window must outlast the seal/drain.
        assert_eq!(pod.termination_grace_period_seconds, Some(60));
        let c = pod.containers.into_iter().next().unwrap();
        // /healthz HTTP probes on the ingest port (matches the chart).
        let r = c.readiness_probe.unwrap();
        assert_eq!(
            r.http_get.as_ref().unwrap().path.as_deref(),
            Some("/healthz")
        );
        assert!(c.liveness_probe.is_some());
        // preStop sleep so the Service deregisters before SIGTERM.
        let pre = c.lifecycle.unwrap().pre_stop.unwrap();
        let cmd = pre.exec.unwrap().command.unwrap();
        assert!(
            cmd.last().unwrap().contains("sleep"),
            "preStop runs a sleep: {cmd:?}"
        );
    }

    #[test]
    fn query_pods_get_graceful_shutdown() {
        // Query: stateful, /readyz probe + grace.
        let q = query_statefulset(&sample_cr(), 2)
            .spec
            .unwrap()
            .template
            .spec
            .unwrap();
        assert_eq!(q.termination_grace_period_seconds, Some(60));
        let qc = q.containers.into_iter().next().unwrap();
        assert_eq!(
            qc.readiness_probe
                .unwrap()
                .http_get
                .unwrap()
                .path
                .as_deref(),
            Some("/readyz")
        );
        assert!(qc.lifecycle.unwrap().pre_stop.is_some());
    }

    #[test]
    fn ingester_carries_owner_ref_and_warehouse_env() {
        let d = ingester_deployment(&sample_cr(), 3, None);
        assert_eq!(d.metadata.name.as_deref(), Some("acme-ingester"));
        let owners = d.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].kind, "SiglakeCluster");
        assert_eq!(owners[0].name, "acme");
        let env = d
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap()
            .env
            .unwrap();
        assert!(env.iter().any(|e| e.name == "SIGLAKE_WAREHOUSE_URL"
            && e.value.as_deref() == Some("s3://acme/warehouse")));
    }

    #[test]
    fn ingester_pod_has_fs_group_for_wal_pvc() {
        // Regression for bug #20 (Phase 4.12.18): the operator-rendered
        // PodSpec must set `fsGroup` so kubelet chowns the WAL PVC's
        // mount to gid 65532 — without it the non-root user
        // crashloops on `Permission denied` the first time it tries to
        // create `/var/lib/siglake/wal/active`.
        let d = ingester_deployment(&sample_cr(), 1, None);
        let pod_sc = d
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .security_context
            .expect("pod-level security_context must be set");
        assert_eq!(pod_sc.fs_group, Some(65532));
        assert_eq!(pod_sc.run_as_user, Some(65532));
        assert_eq!(pod_sc.run_as_group, Some(65532));
        assert_eq!(pod_sc.run_as_non_root, Some(true));
    }

    #[test]
    fn ingester_container_drops_capabilities() {
        // Belt-and-braces: container-level securityContext must
        // disallow privilege escalation and drop every capability.
        let d = ingester_deployment(&sample_cr(), 1, None);
        let container_sc = d
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap()
            .security_context
            .expect("container-level security_context must be set");
        assert_eq!(container_sc.allow_privilege_escalation, Some(false));
        let caps = container_sc.capabilities.unwrap();
        assert_eq!(caps.drop, Some(vec!["ALL".to_string()]));
    }

    #[test]
    fn ingester_carries_auth_tokens_secret_ref_when_configured() {
        let secret = crate::crd::SecretRef {
            name: "siglake-auth".into(),
            key: "tokens".into(),
        };
        let d = ingester_deployment(&sample_cr(), 1, Some(&secret));
        let env = d
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap()
            .env
            .unwrap();
        let auth = env
            .iter()
            .find(|e| e.name == "SIGLAKE_AUTH_TOKENS")
            .expect("SIGLAKE_AUTH_TOKENS env present");
        // valueFrom secretKeyRef (operator never reads the bytes), not value.
        assert!(auth.value.is_none());
        let sel = auth
            .value_from
            .as_ref()
            .and_then(|s| s.secret_key_ref.as_ref())
            .expect("secretKeyRef set");
        assert_eq!(sel.name, "siglake-auth");
        assert_eq!(sel.key, "tokens");
    }

    #[test]
    fn ingester_omits_auth_tokens_when_unset() {
        let d = ingester_deployment(&sample_cr(), 1, None);
        let env = d
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap()
            .env
            .unwrap();
        assert!(env.iter().all(|e| e.name != "SIGLAKE_AUTH_TOKENS"));
    }

    #[test]
    fn compactor_claim_mode_follows_the_fixed_policy() {
        let mut local = sample_cr();
        local.spec.autoscaling.compactor.max = 1;
        let d1 = compactor_deployment(&local, 1);
        let args1 = d1
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap()
            .args
            .unwrap();
        assert!(
            !args1.iter().any(|a| a == "--catalog-claim"),
            "a max-one policy must stay on filesystem ownership"
        );

        let d2 = compactor_deployment(&sample_cr(), 1);
        let args2 = d2
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap()
            .args
            .unwrap();
        assert!(
            args2.iter().any(|a| a == "--catalog-claim"),
            "a scale-out-capable policy must claim at one replica too"
        );
    }

    /// A filesystem-drain policy rolls with Recreate — a surge pod racing its
    /// predecessor on the shared local WAL double-claims segments. A
    /// catalog-claim policy coordinates through the shared table and keeps
    /// RollingUpdate at one replica too; its claim batch defaults to the
    /// fleet-validated 256.
    #[test]
    fn compactor_strategy_and_claim_batch_follow_policy_mode() {
        let cr = sample_cr();
        let spec = compactor_deployment(&cr, 1).spec.unwrap();
        assert_eq!(
            spec.strategy.unwrap().type_.as_deref(),
            Some("RollingUpdate")
        );
        let args = spec
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap()
            .args
            .unwrap();
        let i = args
            .iter()
            .position(|a| a == "--catalog-claim-batch")
            .expect("batch flag");
        assert_eq!(args[i + 1], "256");

        let mut local = sample_cr();
        local.spec.autoscaling.compactor.max = 1;
        for replicas in [0, 1, 2] {
            let spec = compactor_deployment(&local, replicas).spec.unwrap();
            assert_eq!(spec.strategy.unwrap().type_.as_deref(), Some("Recreate"));
            assert!(!spec.template.spec.unwrap().containers[0]
                .args
                .as_ref()
                .unwrap()
                .iter()
                .any(|arg| arg == "--catalog-claim"));
        }
    }

    #[test]
    fn compactor_scale_out_and_back_does_not_change_drain_templates() {
        for max in [1, 4] {
            let mut cr = sample_cr();
            cr.spec.autoscaling.compactor.max = max;
            let mut rendered = Vec::new();
            for replicas in [1, 2, 1] {
                let compactor = compactor_deployment(&cr, replicas);
                let compactor_spec = compactor.spec.unwrap();
                let args = compactor_spec.template.spec.unwrap().containers[0]
                    .args
                    .clone()
                    .unwrap();
                let strategy = compactor_spec.strategy.unwrap().type_.unwrap();
                let ingester = ingester_deployment(&cr, 1, None);
                let remote_drain = ingester.spec.unwrap().template.spec.unwrap().containers[0]
                    .env
                    .as_ref()
                    .unwrap()
                    .iter()
                    .rfind(|entry| entry.name == "SIGLAKE_REMOTE_WAL_DRAIN")
                    .and_then(|entry| entry.value.clone())
                    .unwrap();
                rendered.push((args, strategy, remote_drain));
            }
            assert_eq!(rendered[0], rendered[1], "1→2 changed max={max} mode");
            assert_eq!(rendered[1], rendered[2], "2→1 changed max={max} mode");
        }
    }

    /// GA: `spec.extraEnv` appends to every rendered container, after the
    /// operator's own env so a knob can override a rendered default.
    #[test]
    fn extra_env_flows_into_all_tiers() {
        let mut cr = sample_cr();
        cr.spec.extra_env = vec![crate::crd::ExtraEnvVar {
            name: "SIGLAKE_MIRROR_SYNC_INTERVAL_SECS".into(),
            value: "120".into(),
        }];
        for env in [
            ingester_deployment(&cr, 1, None)
                .spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .containers[0]
                .env
                .clone()
                .unwrap(),
            compactor_deployment(&cr, 1)
                .spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .containers[0]
                .env
                .clone()
                .unwrap(),
            query_statefulset(&cr, 1)
                .spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .containers[0]
                .env
                .clone()
                .unwrap(),
        ] {
            assert!(
                env.iter()
                    .any(|e| e.name == "SIGLAKE_MIRROR_SYNC_INTERVAL_SECS"
                        && e.value.as_deref() == Some("120")),
                "extraEnv must reach the container: {env:?}"
            );
        }
    }

    /// Delete-task execution is on for an operator-managed compactor, and the
    /// opt-out is `spec.extraEnv` — which only works if the operator's own
    /// entry comes FIRST, since Kubernetes takes the last duplicate name.
    #[test]
    fn compactor_executes_delete_tasks_unless_extra_env_opts_out() {
        let compactor_env = |cr: &SiglakeCluster| {
            compactor_deployment(cr, 1)
                .spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .containers[0]
                .env
                .clone()
                .unwrap()
        };
        let effective = |env: &[EnvVar]| {
            env.iter()
                .rfind(|e| e.name == "SIGLAKE_DELETE_TASKS")
                .and_then(|e| e.value.clone())
                .expect("SIGLAKE_DELETE_TASKS rendered")
        };

        let cr = sample_cr();
        assert_eq!(effective(&compactor_env(&cr)), "1");

        let mut opted_out = sample_cr();
        opted_out.spec.extra_env = vec![crate::crd::ExtraEnvVar {
            name: "SIGLAKE_DELETE_TASKS".into(),
            value: "0".into(),
        }];
        let env = compactor_env(&opted_out);
        assert_eq!(
            effective(&env),
            "0",
            "spec.extraEnv must win over the rendered default: {env:?}"
        );
    }

    /// Footer inverted indexes are on for operator-managed writes. The CRD's
    /// long-tail `spec.extraEnv` escape hatch remains an effective opt-out.
    #[test]
    fn compactor_builds_inverted_indexes_unless_extra_env_opts_out() {
        let effective = |cr: &SiglakeCluster| {
            compactor_deployment(cr, 1)
                .spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .containers[0]
                .env
                .clone()
                .unwrap()
                .iter()
                .rfind(|entry| entry.name == "SIGLAKE_INVERTED_INDEX")
                .and_then(|entry| entry.value.clone())
                .expect("SIGLAKE_INVERTED_INDEX rendered")
        };

        assert_eq!(effective(&sample_cr()), "1");

        let mut opted_out = sample_cr();
        opted_out.spec.extra_env = vec![crate::crd::ExtraEnvVar {
            name: "SIGLAKE_INVERTED_INDEX".into(),
            value: "0".into(),
        }];
        assert_eq!(effective(&opted_out), "0");
    }

    /// Operator-managed rewrites do NOT rebuild missing Puffin indexes: the
    /// rendered `0` is the shipped default said out loud, and the CRD's
    /// `spec.extraEnv` escape hatch is how a cluster opts in.
    #[test]
    fn compactor_leaves_missing_indexes_unless_extra_env_opts_in() {
        let effective = |cr: &SiglakeCluster| {
            compactor_deployment(cr, 1)
                .spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .containers[0]
                .env
                .clone()
                .unwrap()
                .iter()
                .rfind(|entry| entry.name == "SIGLAKE_INDEX_REBUILD")
                .and_then(|entry| entry.value.clone())
                .expect("SIGLAKE_INDEX_REBUILD rendered")
        };

        assert_eq!(effective(&sample_cr()), "0");

        let mut opted_in = sample_cr();
        opted_in.spec.extra_env = vec![crate::crd::ExtraEnvVar {
            name: "SIGLAKE_INDEX_REBUILD".into(),
            value: "1".into(),
        }];
        assert_eq!(effective(&opted_in), "1");
    }

    /// The batch-job store is the catalog Postgres, for every replica count:
    /// a per-pod store 404s a job the other pod is running, with no restart
    /// involved. A non-Postgres catalog has no store to share, and
    /// `spec.extraEnv` is the opt-out — which only works if the rendered entry
    /// comes first, since Kubernetes takes the last duplicate name.
    #[test]
    fn query_pods_share_the_catalog_backed_job_store() {
        let effective = |cr: &SiglakeCluster, replicas| {
            query_statefulset(cr, replicas)
                .spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .containers[0]
                .env
                .clone()
                .unwrap()
                .iter()
                .rfind(|e| e.name == "SIGLAKE_JOBS_POSTGRES_URI")
                .and_then(|e| e.value.clone())
                .expect("SIGLAKE_JOBS_POSTGRES_URI rendered")
        };

        let cr = sample_cr();
        assert_eq!(effective(&cr, 1), "postgres://k/db");
        assert_eq!(effective(&cr, 4), "postgres://k/db");

        let mut sqlite = sample_cr();
        sqlite.spec.catalog_uri = "sqlite:///var/lib/siglake/catalog.db?mode=rwc".into();
        assert_eq!(
            effective(&sqlite, 1),
            "",
            "a non-Postgres catalog is no store; blank keeps the in-memory one \
             instead of crash-looping the pod"
        );

        let mut opted_out = sample_cr();
        opted_out.spec.extra_env = vec![crate::crd::ExtraEnvVar {
            name: "SIGLAKE_JOBS_POSTGRES_URI".into(),
            value: String::new(),
        }];
        assert_eq!(
            effective(&opted_out, 1),
            "",
            "spec.extraEnv must win over the rendered default"
        );
    }

    #[test]
    fn query_source_file_cache_is_disabled_unless_extra_env_enables_it() {
        let effective_limits = |cr: &SiglakeCluster| {
            let env = query_statefulset(cr, 1)
                .spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .containers[0]
                .env
                .clone()
                .unwrap();
            let value = |name| {
                env.iter()
                    .rev()
                    .find(|entry| entry.name == name)
                    .and_then(|entry| entry.value.as_deref())
                    .map(str::to_owned)
            };
            (
                value("SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_BYTES"),
                value("SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_ENTRIES"),
            )
        };

        assert_eq!(
            effective_limits(&sample_cr()),
            (Some("0".into()), Some("0".into()))
        );

        let mut enabled = sample_cr();
        enabled.spec.extra_env.extend([
            crate::crd::ExtraEnvVar {
                name: "SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_BYTES".into(),
                value: "536870912".into(),
            },
            crate::crd::ExtraEnvVar {
                name: "SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_ENTRIES".into(),
                value: "512".into(),
            },
        ]);
        assert_eq!(
            effective_limits(&enabled),
            (Some("536870912".into()), Some("512".into()))
        );
    }

    #[test]
    fn query_is_a_statefulset_with_dedicated_binary_and_peer_discovery() {
        let mut cr = sample_cr();
        cr.spec.autoscaling.query = ComponentAutoscale {
            min: 2,
            max: 8,
            target: 4.0,
        };
        let sts = query_statefulset(&cr, 2);
        let spec = sts.spec.clone().unwrap();
        // StatefulSet shape: headless serviceName + Parallel pod management.
        assert_eq!(spec.service_name.as_deref(), Some("acme-query-headless"));
        assert_eq!(spec.pod_management_policy.as_deref(), Some("Parallel"));
        let container = spec
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(
            container.command.unwrap()[0],
            "/usr/local/bin/siglake-query-server"
        );
        // #967: the peer list is discovered, not rendered. The SRV name must
        // be the headless Service's `http` port — the same Service the
        // StatefulSet is governed by, or the tier never fans out.
        let args = container.args.unwrap();
        assert!(
            !args.contains(&"--query-peers".to_string()),
            "a rendered peer list caps fan-out at the rendered count: {args:?}"
        );
        let i = args
            .iter()
            .position(|a| a == "--query-peer-discovery-srv")
            .expect("--query-peer-discovery-srv");
        assert_eq!(
            args[i + 1],
            "_http._tcp.acme-query-headless.siglake-prod.svc.cluster.local"
        );
        let s = args
            .iter()
            .position(|a| a == "--query-peer-scheme")
            .expect("--query-peer-scheme");
        assert_eq!(args[s + 1], "http");
        // The coordinator's own worker URL — the failover target — comes from
        // the downward API, not from $HOSTNAME.
        let self_name = container
            .env
            .unwrap()
            .into_iter()
            .find(|e| e.name == "SIGLAKE_QUERY_PEER_SELF_NAME")
            .expect("SIGLAKE_QUERY_PEER_SELF_NAME");
        assert_eq!(
            self_name
                .value_from
                .and_then(|from| from.field_ref)
                .map(|field| field.field_path),
            Some("metadata.name".into())
        );
    }

    /// #967: the scaling decision now reaches the StatefulSet's replica count,
    /// and the POD TEMPLATE is independent of it. Those two together are what
    /// make a query range usable: the count moves without reshaping the
    /// template, so a scale event does not roll every pod (which the rendered
    /// `--query-peers` list forced it to).
    #[test]
    fn query_replicas_follow_the_decision_without_reshaping_the_template() {
        let mut cr = sample_cr();
        cr.spec.autoscaling.query = ComponentAutoscale {
            min: 2,
            max: 8,
            target: 4.0,
        };
        let expected = query_statefulset(&cr, 2).spec.unwrap();

        for candidate in [1, 2, 3, 8] {
            let actual = query_statefulset(&cr, candidate).spec.unwrap();
            assert_eq!(
                actual.replicas,
                Some(candidate),
                "candidate replicas: {candidate}"
            );
            assert_eq!(
                actual.template, expected.template,
                "pod template changed for candidate replicas: {candidate}"
            );
        }
    }

    /// Discovery is rendered even at one replica: a one-member membership
    /// takes the local path anyway, so scaling out later needs no rollout.
    #[test]
    fn query_single_replica_still_renders_discovery() {
        let sts = query_statefulset(&sample_cr(), 1);
        let args = sts.spec.unwrap().template.spec.unwrap().containers[0]
            .args
            .clone()
            .unwrap();
        assert!(args.contains(&"--query-peer-discovery-srv".to_string()));
        assert!(!args.contains(&"--query-peers".to_string()));
    }

    #[test]
    fn deployment_replicas_carries_through() {
        let d = compactor_deployment(&sample_cr(), 4);
        assert_eq!(d.spec.unwrap().replicas, Some(4));
    }

    #[test]
    fn ingester_and_compactor_mount_wal_pvc() {
        let cr = sample_cr();
        for dep in [
            ingester_deployment(&cr, 1, None),
            compactor_deployment(&cr, 1),
        ] {
            let spec = dep.spec.unwrap().template.spec.unwrap();
            let vols = spec.volumes.expect("volumes set");
            assert_eq!(vols.len(), 1, "exactly one volume");
            assert_eq!(vols[0].name, WAL_VOLUME_NAME);
            assert_eq!(
                vols[0].persistent_volume_claim.as_ref().unwrap().claim_name,
                "acme-wal"
            );
            let mounts = spec.containers[0].volume_mounts.as_ref().unwrap();
            assert_eq!(mounts.len(), 1);
            assert_eq!(mounts[0].mount_path, WAL_MOUNT_PATH);
        }
    }

    #[test]
    fn query_mounts_bounded_spill_empty_dir_and_no_wal() {
        let sts = query_statefulset(&sample_cr(), 1);
        let spec = sts.spec.unwrap().template.spec.unwrap();
        let volumes = spec.volumes.unwrap();
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].name, QUERY_SPILL_VOLUME_NAME);
        assert_eq!(
            volumes[0]
                .empty_dir
                .as_ref()
                .and_then(|empty_dir| empty_dir.size_limit.as_ref())
                .map(|quantity| quantity.0.as_str()),
            Some(QUERY_SPILL_SIZE_LIMIT)
        );
        assert!(volumes[0].persistent_volume_claim.is_none());

        let container = &spec.containers[0];
        let mounts = container.volume_mounts.as_ref().unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].name, QUERY_SPILL_VOLUME_NAME);
        assert_eq!(mounts[0].mount_path, QUERY_SPILL_MOUNT_PATH);
        let env = container.env.as_ref().unwrap();
        for (name, value) in [
            ("SIGLAKE_QUERY_SPILL_DIR", QUERY_SPILL_MOUNT_PATH),
            ("SIGLAKE_QUERY_SPILL_MAX_BYTES", QUERY_SPILL_MAX_BYTES),
        ] {
            assert_eq!(
                env.iter()
                    .rev()
                    .find(|entry| entry.name == name)
                    .and_then(|entry| entry.value.as_deref()),
                Some(value),
                "{name}"
            );
        }
    }

    #[test]
    fn wal_pvc_uses_storage_spec_defaults() {
        let cr = sample_cr();
        let pvc = wal_pvc(&cr);
        assert_eq!(pvc.metadata.name.as_deref(), Some("acme-wal"));
        let spec = pvc.spec.unwrap();
        assert_eq!(spec.access_modes.unwrap(), vec!["ReadWriteMany"]);
        assert!(spec.storage_class_name.is_none()); // default empty → omitted
        let req = spec
            .resources
            .unwrap()
            .requests
            .unwrap()
            .get("storage")
            .cloned()
            .unwrap();
        assert_eq!(req.0, "5Gi");
    }

    /// The ingester mirrors at every compactor replica count, and the prefix
    /// the claim path reads is the one it writes.
    ///
    /// THE DEFECT THIS GUARDS. At compactor replicas > 1 the operator added
    /// `--catalog-claim --mirror-prefix` to the compactor and nothing to the
    /// ingester. `Compactor::run_once` is exclusive: the claim path reads the
    /// `wal_segments` catalog table and NEVER local `sealed/`, and rows land
    /// there only if an ingester runs with `SIGLAKE_WAL_MIRROR_PREFIX`. So the
    /// mirror was empty, zero segments were claimed, and compaction silently
    /// stopped — with `siglake_compactor_sealed_pending` reading zero, because
    /// the local sealed count is not what the claim path consults. Turning on
    /// multi-pod compaction turned compaction off.
    ///
    /// Since 2026-09-11 the single-compactor case mirrors too, for durability
    /// rather than for the claim: one replica drains local `sealed/`, and
    /// without the mirror the WAL PVC is the only copy of everything
    /// acknowledged and not yet committed.
    #[test]
    fn claim_mode_and_the_wal_mirror_are_enabled_together() {
        let mut cr = sample_cr();
        let mirror_env = |d: &Deployment| -> Option<String> {
            d.spec.clone()?.template.spec?.containers[0]
                .env
                .clone()?
                .into_iter()
                .find(|e| e.name == "SIGLAKE_WAL_MIRROR_PREFIX")
                .and_then(|e| e.value)
        };
        let remote_drain_env = |d: &Deployment| -> Option<String> {
            d.spec.clone()?.template.spec?.containers[0]
                .env
                .clone()?
                .into_iter()
                .find(|e| e.name == "SIGLAKE_REMOTE_WAL_DRAIN")
                .and_then(|e| e.value)
        };
        let claims = |d: &Deployment| -> bool {
            d.spec
                .clone()
                .and_then(|s| s.template.spec)
                .map(|s| s.containers[0].args.clone().unwrap_or_default())
                .unwrap_or_default()
                .iter()
                .any(|a| a == "--catalog-claim")
        };

        let claim_prefix = |d: &Deployment| -> Option<String> {
            let args = d
                .spec
                .clone()
                .and_then(|s| s.template.spec)
                .map(|s| s.containers[0].args.clone().unwrap_or_default())
                .unwrap_or_default();
            let at = args.iter().position(|a| a == "--mirror-prefix")?;
            args.get(at + 1).cloned()
        };

        // Multi-pod compaction: BOTH halves, naming the same prefix.
        let compactor = compactor_deployment(&cr, 3);
        let ingester = ingester_deployment(&cr, 2, None);
        assert!(claims(&compactor), "fixture: compactor should claim at 3");
        assert_eq!(
            mirror_env(&ingester).as_deref(),
            Some(WAL_MIRROR_PREFIX),
            "the compactor claims from a mirror prefix the ingester never writes to"
        );
        assert_eq!(
            claim_prefix(&compactor),
            mirror_env(&ingester),
            "reader and writer must name the same prefix"
        );
        assert_eq!(remote_drain_env(&ingester).as_deref(), Some("1"));

        // A max-one policy uses the FS-rename path, no claim — and the mirror anyway.
        cr.spec.autoscaling.compactor.max = 1;
        let compactor = compactor_deployment(&cr, 1);
        let ingester = ingester_deployment(&cr, 2, None);
        assert!(!claims(&compactor));
        assert_eq!(claim_prefix(&compactor), None);
        assert_eq!(
            mirror_env(&ingester).as_deref(),
            Some(WAL_MIRROR_PREFIX),
            "a single-compactor cluster still needs an off-volume copy of the WAL"
        );
        assert_eq!(remote_drain_env(&ingester).as_deref(), Some("0"));
    }

    #[test]
    fn effective_wal_mirror_prefix_matches_binary_precedence() {
        let mut spec = sample_cr().spec;
        assert_eq!(
            effective_wal_mirror_prefix(&spec),
            Some(WAL_MIRROR_PREFIX),
            "unset uses the operator default"
        );

        spec.extra_env.push(crate::crd::ExtraEnvVar {
            name: "SIGLAKE_WAL_MIRROR_PREFIX".into(),
            value: " custom-mirror ".into(),
        });
        assert_eq!(effective_wal_mirror_prefix(&spec), Some("custom-mirror"));

        spec.extra_env.push(crate::crd::ExtraEnvVar {
            name: "SIGLAKE_WAL_MIRROR_PREFIX".into(),
            value: "   ".into(),
        });
        assert_eq!(
            effective_wal_mirror_prefix(&spec),
            None,
            "a whitespace-only final override is the mirror opt-out"
        );

        spec.extra_env.push(crate::crd::ExtraEnvVar {
            name: "SIGLAKE_WAL_MIRROR_PREFIX".into(),
            value: "last-mirror".into(),
        });
        assert_eq!(
            effective_wal_mirror_prefix(&spec),
            Some("last-mirror"),
            "the final duplicate wins"
        );

        spec.extra_env.last_mut().unwrap().value.clear();
        assert_eq!(effective_wal_mirror_prefix(&spec), None);
    }

    #[test]
    fn custom_wal_mirror_prefix_is_shared_by_writer_and_claim_reader() {
        let mut cr = sample_cr();
        cr.spec.extra_env = vec![crate::crd::ExtraEnvVar {
            name: "SIGLAKE_WAL_MIRROR_PREFIX".into(),
            value: " recovery-prefix ".into(),
        }];

        let ingester = ingester_deployment(&cr, 1, None);
        let writer_prefix = ingester.spec.unwrap().template.spec.unwrap().containers[0]
            .env
            .as_ref()
            .unwrap()
            .iter()
            .rfind(|entry| entry.name == "SIGLAKE_WAL_MIRROR_PREFIX")
            .and_then(|entry| entry.value.as_deref())
            .map(str::to_owned);
        let compactor = compactor_deployment(&cr, 2);
        let args = compactor.spec.unwrap().template.spec.unwrap().containers[0]
            .args
            .clone()
            .unwrap();
        let at = args
            .iter()
            .position(|arg| arg == "--mirror-prefix")
            .unwrap();

        assert_eq!(writer_prefix.as_deref(), Some("recovery-prefix"));
        assert_eq!(args.get(at + 1), writer_prefix.as_ref());
    }

    /// The mirror is a DEFAULT, not a fact: `spec.extraEnv` can still turn it
    /// off, which means the operator's entry has to be spliced BEFORE the
    /// user's (Kubernetes resolves a duplicate name to the last one).
    #[test]
    fn extra_env_can_turn_the_wal_mirror_off() {
        let mut cr = sample_cr();
        cr.spec.extra_env = vec![crate::crd::ExtraEnvVar {
            name: "SIGLAKE_WAL_MIRROR_PREFIX".into(),
            value: String::new(),
        }];
        let env = ingester_deployment(&cr, 1, None)
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers[0]
            .env
            .clone()
            .unwrap();
        let effective = env
            .iter()
            .rfind(|e| e.name == "SIGLAKE_WAL_MIRROR_PREFIX")
            .and_then(|e| e.value.clone());
        assert_eq!(
            effective.as_deref(),
            Some(""),
            "the last duplicate wins in Kubernetes, so the opt-out must be last: {env:?}"
        );
    }

    /// Deleting the CR must NOT delete the WAL.
    ///
    /// THE DEFECT THIS GUARDS. The WAL PVC carried a controller
    /// `ownerReference` with `blockOwnerDeletion`, so `kubectl delete
    /// siglakecluster` cascaded into it. Acks are WAL-append-based: a segment
    /// is durable in the WAL and NOT in Iceberg until the compactor commits it
    /// — minutes under load, longer with commit batching and a backlog — and
    /// `wal.mirror.enabled` is off by default, so there is no S3 copy. The
    /// cascade discarded acknowledged data.
    ///
    /// The asymmetry that hid it: per-pod PVCs from `volumeClaimTemplates` are
    /// deliberately KEPT by Kubernetes. The one claim holding un-committed
    /// writes was the one that went.
    #[test]
    fn deleting_the_cluster_does_not_delete_the_wal() {
        let pvc = wal_pvc(&sample_cr());
        assert!(
            pvc.metadata.owner_references.is_none(),
            "the WAL claim is owned by the CR, so deleting the CR deletes \
             acknowledged-but-uncommitted data"
        );

        // Opting in restores the cascade, for a deployment that knows its WAL
        // is mirrored or empty.
        let mut cr = sample_cr();
        cr.spec.storage.wal_retain_on_delete = false;
        let owned = wal_pvc(&cr);
        assert!(
            owned
                .metadata
                .owner_references
                .as_ref()
                .is_some_and(|o| !o.is_empty()),
            "walRetainOnDelete: false did not restore ownership"
        );
    }

    /// Every Service's named `targetPort` must exist on the workload it selects.
    ///
    /// THE DEFECT THIS GUARDS. The ingester Service targeted the ingest port's
    /// old name while the container declared `ingest` — residue of the OTLP
    /// rename landing in one of two copies. A named `targetPort` matching no
    /// container port is silently DROPPED by the endpoints controller: the
    /// EndpointSlice carries no port for 8088 and every connection to
    /// `<cr>-ingester:8088` is refused, while the pods stay Ready because the
    /// probes address the container port directly. Ingest was unreachable
    /// through its own Service and nothing said so.
    ///
    /// Both sides now derive from one constant, so this asserts the constants
    /// are what the workloads actually declare — the remaining way to
    /// reintroduce the drift is to hand-write a port on a container.
    #[test]
    fn service_ports_match_container_ports() {
        let cr = sample_cr();
        type PortCase<'a> = (
            &'a str,
            &'a [(&'a str, i32)],
            Vec<k8s_openapi::api::core::v1::ContainerPort>,
        );
        let cases: Vec<PortCase<'_>> = vec![
            (
                "ingester",
                INGESTER_PORTS,
                ingester_deployment(&cr, 1, None)
                    .spec
                    .unwrap()
                    .template
                    .spec
                    .unwrap()
                    .containers[0]
                    .ports
                    .clone()
                    .unwrap_or_default(),
            ),
            (
                "query",
                QUERY_PORTS,
                query_statefulset(&cr, 2)
                    .spec
                    .unwrap()
                    .template
                    .spec
                    .unwrap()
                    .containers[0]
                    .ports
                    .clone()
                    .unwrap_or_default(),
            ),
            (
                "compactor",
                COMPACTOR_PORTS,
                compactor_deployment(&cr, 1)
                    .spec
                    .unwrap()
                    .template
                    .spec
                    .unwrap()
                    .containers[0]
                    .ports
                    .clone()
                    .unwrap_or_default(),
            ),
        ];
        for (component, spec, declared) in cases {
            let declared: Vec<(String, i32)> = declared
                .into_iter()
                .map(|p| (p.name.unwrap_or_default(), p.container_port))
                .collect();
            let expected: Vec<(String, i32)> =
                spec.iter().map(|(n, p)| ((*n).to_string(), *p)).collect();
            assert_eq!(
                declared, expected,
                "{component}: container ports do not match the constant its Service targets"
            );
        }
    }

    #[test]
    fn wal_pvc_honors_storage_spec_overrides() {
        let mut cr = sample_cr();
        cr.spec.storage = crate::crd::StorageSpec {
            wal_storage_class_name: "efs-sc".into(),
            wal_size: "100Gi".into(),
            wal_access_mode: "ReadWriteOnce".into(),
            wal_retain_on_delete: true,
        };
        let pvc = wal_pvc(&cr);
        let spec = pvc.spec.unwrap();
        assert_eq!(spec.access_modes.unwrap(), vec!["ReadWriteOnce"]);
        assert_eq!(spec.storage_class_name.as_deref(), Some("efs-sc"));
        let req = spec
            .resources
            .unwrap()
            .requests
            .unwrap()
            .get("storage")
            .cloned()
            .unwrap();
        assert_eq!(req.0, "100Gi");
    }

    // ---- Phase 4.13g: retention CronJob ---------------------------------

    #[test]
    fn cronjob_is_none_when_retention_unset() {
        let cr = sample_cr();
        assert!(
            audit_rotate_cronjob(&cr).is_none(),
            "no retention spec → no CronJob"
        );
    }

    #[test]
    #[should_panic(expected = "retention interval must be validated before rendering")]
    fn cronjob_requires_a_validated_interval() {
        let mut cr = sample_cr();
        cr.spec.retention.query_audit_rotate_interval_days = Some(0);
        let _ = audit_rotate_cronjob(&cr);
    }

    #[test]
    fn cronjob_for_30_day_interval_uses_first_of_month() {
        let mut cr = sample_cr();
        cr.spec.retention.query_audit_rotate_interval_days = Some(30);
        let cj = audit_rotate_cronjob(&cr).unwrap();
        let spec = cj.spec.unwrap();
        assert_eq!(spec.schedule, "0 0 1 * *");
        assert_eq!(spec.concurrency_policy.as_deref(), Some("Forbid"));
        assert_eq!(spec.successful_jobs_history_limit, Some(1));
    }

    #[test]
    fn cronjob_carries_audit_rotate_args() {
        let mut cr = sample_cr();
        cr.spec.retention.query_audit_rotate_interval_days = Some(7);
        let cj = audit_rotate_cronjob(&cr).unwrap();
        let spec = cj.spec.unwrap();
        assert_eq!(spec.schedule, "0 0 * * 0");
        let container = spec
            .job_template
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .unwrap();
        let args = container.args.unwrap();
        assert!(args.contains(&"audit-rotate".to_string()));
        assert!(args.contains(&"--warehouse-url".to_string()));
        assert!(args.contains(&cr.spec.warehouse_url));
        assert!(args.contains(&"--catalog-uri".to_string()));
        assert!(args.contains(&cr.spec.catalog_uri));
        // Image matches the CR's spec.image.
        assert_eq!(container.image, Some(cr.spec.image.clone()));
    }

    #[test]
    fn cronjob_inherits_owner_ref_for_gc() {
        let mut cr = sample_cr();
        cr.spec.retention.query_audit_rotate_interval_days = Some(30);
        let cj = audit_rotate_cronjob(&cr).unwrap();
        let owners = cj.metadata.owner_references.unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].kind, "SiglakeCluster");
        assert_eq!(owners[0].name, "acme");
        // Deleting the CR cascades the CronJob, matching the
        // operator's contract that everything it renders gets
        // GC'd when the CR is deleted.
    }

    #[test]
    fn cronjob_schedules_match_supported_intervals() {
        assert_eq!(cron_schedule_for_interval(1), "0 0 * * *");
        assert_eq!(cron_schedule_for_interval(7), "0 0 * * 0");
        assert_eq!(cron_schedule_for_interval(30), "0 0 1 * *");
    }

    #[test]
    fn no_migration_job_without_schema_version() {
        let cr = sample_cr();
        assert!(migration_job(&cr).is_none(), "unset schemaVersion ⇒ no Job");
        let mut cr0 = sample_cr();
        cr0.spec.schema_version = Some(0);
        assert!(migration_job(&cr0).is_none(), "schemaVersion 0 ⇒ no Job");
    }

    #[test]
    fn migration_job_is_versioned_and_runs_all_tables() {
        let mut cr = sample_cr();
        cr.spec.schema_version = Some(3);
        let job = migration_job(&cr).expect("schemaVersion set ⇒ Job");
        // Name encodes the version so a bump yields a fresh, immutable Job.
        let name = job.metadata.name.clone().unwrap();
        assert!(name.starts_with("acme-migrate-schema-v3-"), "got: {name}");
        // Owned by the CR + reaped after completion.
        assert!(job.metadata.owner_references.is_some());
        let spec = job.spec.as_ref().unwrap();
        assert_eq!(spec.ttl_seconds_after_finished, Some(86_400));
        let c = spec
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers
            .first()
            .unwrap();
        let args = c.args.clone().unwrap();
        assert_eq!(args[0], "migrate-schema");
        assert!(args.contains(&"--all-tables".to_string()));
        assert!(args.contains(&"--all-namespaces".to_string()));
        assert!(args.contains(&cr.spec.warehouse_url));
        assert!(args.contains(&cr.spec.catalog_uri));
    }

    /// A Job's `spec.template` is immutable, so anything that reshapes the
    /// template must reshape the NAME. Keying on the version alone meant an
    /// image bump re-rendered a different template under the same name: the
    /// API server 422s, the reconciler returns Err before writing status, and
    /// the whole control plane freezes for up to `ttlSecondsAfterFinished`
    /// while the workloads upgrade underneath it.
    ///
    /// It also made the migration unreachable at a given version: because
    /// `migrate-schema` migrates to whatever the RUNNING binary declares,
    /// requesting a version the current image cannot provide records success
    /// having done nothing, and no later image could re-run it.
    #[test]
    fn changing_the_image_renames_the_migration_job() {
        let mut a = sample_cr();
        a.spec.schema_version = Some(3);
        let mut b = a.clone();
        b.spec.image = format!("{}-next", a.spec.image);

        let name_a = migration_job(&a).unwrap().metadata.name.unwrap();
        let name_b = migration_job(&b).unwrap().metadata.name.unwrap();
        assert_ne!(
            name_a, name_b,
            "a new image must render a new Job name, or the apply 422s on an immutable template"
        );

        // And the digest must be stable: same inputs, same name, so an
        // ordinary re-reconcile stays a no-op rather than churning Jobs.
        assert_eq!(
            migration_job(&a).unwrap().metadata.name.unwrap(),
            name_a,
            "same spec must render the same name"
        );

        // extraEnv also feeds base_env, so it shapes the template too.
        let mut c = a.clone();
        c.spec.extra_env.push(crate::crd::ExtraEnvVar {
            name: "SIGLAKE_EXTRA".into(),
            value: "1".into(),
        });
        assert_ne!(
            migration_job(&c).unwrap().metadata.name.unwrap(),
            name_a,
            "an extraEnv edit reshapes the pod template and must reshape the name"
        );
    }

    #[test]
    fn headless_service_is_clusterip_none_and_ready_only() {
        let svc = headless_service(&sample_cr(), "query", &[("http", 8089)]);
        assert_eq!(svc.metadata.name.as_deref(), Some("acme-query-headless"));
        let spec = svc.spec.unwrap();
        assert_eq!(spec.cluster_ip.as_deref(), Some("None"));
        // #967: this Service's SRV record is the peer directory, so an unready
        // pod must not appear in it — it would be handed shard work it cannot
        // serve. The port must also stay named `http`: that name is what
        // `_http._tcp.<service>` resolves.
        assert_eq!(spec.publish_not_ready_addresses, None);
        assert_eq!(
            spec.ports.unwrap()[0].name.as_deref(),
            Some("http"),
            "the SRV name the pods resolve is built from this port name"
        );
    }
}
