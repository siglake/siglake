//! `SiglakeCluster` CustomResourceDefinition.
//!
//! One CR per logical siglake deployment. The operator's reconciler
//! watches these objects, computes the desired state for ingester /
//! compactor / query-server Deployments (replica counts in
//! particular), and applies the diff.
//!
//! Why a CRD instead of just Helm? The Helm chart ships a fixed set
//! of HPA manifests that key off Prometheus metrics; the operator
//! adds a unified control plane that can:
//!
//! - React to multi-signal load (e.g. compactor backlog + per-tenant
//!   ingestion rate together).
//! - Apply policy decisions across components (don't scale the
//!   query-server up if the cluster's batch tier is saturated).
//! - Manage per-tenant lifecycle (provision namespace, set rate-limit
//!   budgets) in a single declarative spec. (Tenancy is header-based
//!   at ingest, so there are no per-tenant tokens to mint.)
//!
//! The Helm chart (with HPA or KEDA) remains the supported install
//! surface; the operator is the newer path and renders a deliberate
//! subset — see `docs/ARCHITECTURE.md` (Deployment).
//!
//! # camelCase serialization
//!
//! We use explicit per-field `#[serde(rename = "...")]` rather than a
//! type-level `#[serde(rename_all = "camelCase")]` because bug #17
//! showed that the `#[derive(CustomResource)]` macro from
//! kube-derive generates a wrapper struct around `SiglakeClusterSpec`,
//! and in some build configurations the inner `rename_all` attribute
//! did not survive the macro expansion into the runtime Deserialize
//! impl. Field-level renames bypass that interaction entirely.

use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// `SiglakeCluster` spec — what the user declares.
#[derive(CustomResource, Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "siglake.limnion.ai",
    version = "v1alpha1",
    kind = "SiglakeCluster",
    namespaced,
    status = "SiglakeClusterStatus",
    shortname = "kcluster"
)]
pub struct SiglakeClusterSpec {
    /// Container image to roll across every managed component.
    pub image: String,

    /// Iceberg warehouse URL (e.g. `s3://acme-prod/warehouse`).
    #[serde(rename = "warehouseUrl")]
    pub warehouse_url: String,

    /// Iceberg catalog URI (e.g. `postgres://user:pass@host/db`).
    #[serde(rename = "catalogUri")]
    pub catalog_uri: String,

    /// Per-component autoscaling policy.
    #[serde(default)]
    pub autoscaling: AutoscalingSpec,

    /// Tenants this cluster serves, for documentation/provisioning.
    /// Tenancy itself is resolved by the ingester — single-tenant unless a
    /// verified JWT claim or a trusted `X-Scope-OrgID` routes it — and
    /// namespaces are created lazily on first write, so this list is
    /// advisory: empty means the cluster simply serves whatever tenants
    /// arrive (the `default` namespace for untenanted traffic).
    #[serde(default)]
    pub tenants: Vec<TenantSpec>,

    /// Optional cluster-wide bearer-token auth. References a Kubernetes
    /// Secret whose `<key>` holds a comma-separated allow-list; it's
    /// surfaced to the ingester as `SIGLAKE_AUTH_TOKENS` (valueFrom, so
    /// the operator never reads the token bytes). Omitted ⇒ open ingest
    /// (only safe inside a trusted network). Decoupled from tenancy.
    #[serde(
        rename = "authTokensSecretRef",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub auth_tokens_secret_ref: Option<SecretRef>,

    /// Persistent storage knobs for the WAL.
    /// Optional — sensible defaults work for any cluster with an
    /// RWX StorageClass available (e.g. AWS EFS).
    #[serde(default)]
    pub storage: StorageSpec,

    /// Optional retention policy for managed Iceberg tables. Today
    /// the only supported knob is
    /// `queryAuditRotateIntervalDays` — when set, the operator
    /// renders a Kubernetes `CronJob` that runs
    /// `siglake audit-rotate` on that cadence. iceberg-rust 0.9
    /// doesn't expose `expire_snapshots` through its public
    /// Transaction API, so drop-and-recreate is the working
    /// retention escape hatch; when iceberg-rust ships the
    /// public action we'll swap behind the same CronJob.
    #[serde(default)]
    pub retention: RetentionSpec,

    /// AWS region for the warehouse / RDS / Secrets-Manager calls
    /// the rendered Pods make. Operator-rendered pods (including
    /// the retention CronJob) need this in their env or opendal's
    /// S3 client fails with `region is missing`. Chart-managed
    /// Deployments receive it through the terraform-emitted
    /// helm_values; the operator path bypasses that entirely.
    ///
    /// Empty string = unset; the operator omits the env var and
    /// relies on whatever defaults the binary picks up
    /// (`AWS_REGION` / `AWS_DEFAULT_REGION` / IMDS).
    #[serde(rename = "awsRegion", default)]
    pub aws_region: String,

    /// Name of an existing Kubernetes ServiceAccount to bind to
    /// every rendered Pod (including the retention CronJob).
    /// Operator-rendered pods need an IRSA-bound SA to write to
    /// the warehouse S3 bucket, or the node IAM role's missing
    /// s3:PutObject lands them on 403 AccessDenied.
    ///
    /// Empty string (default) → use the namespace's `default` SA,
    /// which is fine for clusters where the node IAM role
    /// already has the bucket-level permissions but is the
    /// wrong default for the standard EKS+IRSA pattern. The
    /// chart's terraform module creates a `siglake` SA with the
    /// warehouse-rw IRSA annotation; deployments using that chart
    /// + this operator should set
    ///   `serviceAccountName: siglake`.
    #[serde(rename = "serviceAccountName", default)]
    pub service_account_name: String,

    /// Monotonic schema-generation counter. Bump it to run a one-shot,
    /// additive schema migration (`siglake migrate-schema --all-tables`) before
    /// the rolled-out pods write under the new schema. The operator renders an
    /// immutable Job named `<cluster>-migrate-schema-v<N>-<digest>`, where the
    /// digest covers the image, warehouse/catalog URIs, region and extraEnv —
    /// everything that shapes the (immutable) pod template. Re-reconciling the
    /// same version with the same template is a no-op; a bump, or an image
    /// change, creates a fresh Job. Additive-only + idempotent, so it is safe
    /// to leave set across restarts. Unset (or 0) renders no Job. Reverting the
    /// image runs one more Job on the OLD binary: additive migration is diffed
    /// by name, so it adds nothing and does not narrow the table — leave this
    /// field where it is rather than lowering it.
    #[serde(
        rename = "schemaVersion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub schema_version: Option<u32>,

    /// Extra environment variables appended to EVERY siglake container the
    /// operator renders — the escape hatch for the long tail of tuning
    /// knobs (watchdog ceilings, warm interval, overlap-depth bound,
    /// ordered-merge fan-in caps, buffer-delta budget, claim batch, mirror
    /// sync interval, …) without a CRD field per knob. Plain name/value
    /// only; anything needing `valueFrom` deserves a first-class field.
    #[serde(rename = "extraEnv", default, skip_serializing_if = "Vec::is_empty")]
    pub extra_env: Vec<ExtraEnvVar>,

    /// Per-tier container resources. Every tier left unset renders the
    /// operator's packaged requests/limits, which are the chart's
    /// (`deploy/helm/siglake/values.yaml`: `ingester.resources`,
    /// `compactor.resources`, `query.resources`). A tier that IS set merges
    /// its keys over those defaults — `query: {limits: {memory: 8Gi}}` raises
    /// the query memory limit and keeps the packaged CPU limit and requests —
    /// the same way a `--set query.resources.limits.memory=8Gi` behaves against
    /// the chart's values.
    ///
    /// The query server sizes its read caches and its memory pool from the
    /// container's memory limit, so this is THE knob for scan-heavy or
    /// high-concurrency query workloads; see the sizing note above
    /// `query.resources.limits.memory` in values.yaml.
    #[serde(default)]
    pub resources: ResourcesSpec,
}

/// `spec.resources`: per-tier overrides of the rendered container resources.
/// See [`SiglakeClusterSpec::resources`] for the merge rule.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ResourcesSpec {
    /// Ingester Deployment container resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingester: Option<TierResources>,
    /// Compactor Deployment container resources. Raise `limits.memory` together
    /// with `SIGLAKE_COMPACTOR_BIN_CONCURRENCY` in `extraEnv`: the operator
    /// reports `InvalidSpec` when the effective limit cannot hold the requested
    /// concurrency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compactor: Option<TierResources>,
    /// Query StatefulSet container resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<TierResources>,
}

/// One tier's `requests` and `limits`, keyed by resource name (`memory`,
/// `cpu`, `ephemeral-storage`, …) with Kubernetes quantity strings as values
/// (`4Gi`, `500m`, `2`). Keys merge over the packaged defaults; an absent key
/// keeps the default.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TierResources {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requests: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, String>,
}

/// Retention policies the operator drives via scheduled jobs.
///
/// Each field is `Option<u32>` so customers can opt into one
/// retention behavior without committing to the others. When the
/// field is `None`, the operator renders nothing — chart-default
/// behavior is "audit table grows unbounded, ops handle retention
/// out-of-band."
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct RetentionSpec {
    /// Days between `siglake audit-rotate` runs. The operator
    /// renders a CronJob with a schedule of `@every Nd`-equivalent
    /// crontab expression. Supported values:
    ///
    /// - `30` for production deployments (monthly rotation, on the first).
    /// - `7` for high-cardinality audit logging (weekly).
    /// - `1` for daily rotation.
    ///
    /// `None` (default) disables the CronJob entirely.
    #[serde(rename = "queryAuditRotateIntervalDays")]
    pub query_audit_rotate_interval_days: Option<u32>,
}

/// Per-volume storage configuration. Mounted by the
/// operator-managed ingester and compactor (WAL).
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct StorageSpec {
    /// StorageClass to use for the WAL PVC. Empty string = the
    /// cluster default (which EKS doesn't set; you'll want to
    /// override this to e.g. `efs-sc`).
    #[serde(rename = "walStorageClassName", default)]
    pub wal_storage_class_name: String,

    /// WAL PVC size. Defaults to 5Gi for smoke + small-tenant
    /// deployments. Real customer deployments will want >100Gi.
    #[serde(rename = "walSize", default = "default_wal_size")]
    pub wal_size: String,

    /// WAL PVC access mode. Defaults to `ReadWriteMany` because
    /// the ingester and compactor both mount the WAL, and an external consumer may too
    /// all share the WAL. Customers running single-pod
    /// deployments + EBS can override to `ReadWriteOnce`.
    #[serde(rename = "walAccessMode", default = "default_wal_access_mode")]
    pub wal_access_mode: String,
    /// Keep the WAL claim when the `SiglakeCluster` is deleted. Default true.
    ///
    /// Acks are WAL-append-based: a segment is durable in the WAL and not yet
    /// in Iceberg until the compactor commits it. With an ownerReference the
    /// CR's deletion cascaded into the claim and took acknowledged data with
    /// it. The operator mirrors the WAL by default, but asynchronously, so a
    /// deleted claim still takes the segments whose upload had not landed. Set
    /// false only if the WAL is known empty.
    #[serde(rename = "walRetainOnDelete", default = "default_true")]
    pub wal_retain_on_delete: bool,
}

fn default_wal_size() -> String {
    "5Gi".to_string()
}

fn default_wal_access_mode() -> String {
    "ReadWriteMany".to_string()
}

fn default_true() -> bool {
    true
}

impl Default for StorageSpec {
    fn default() -> Self {
        Self {
            wal_storage_class_name: String::new(),
            wal_size: default_wal_size(),
            wal_access_mode: default_wal_access_mode(),
            wal_retain_on_delete: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct AutoscalingSpec {
    #[serde(default)]
    pub ingester: ComponentAutoscale,
    #[serde(default)]
    pub compactor: ComponentAutoscale,
    /// Query scales on in-flight queries per pod like the other components.
    /// A `max` above `min` is supported since #967: the pods discover each
    /// other through the headless Service's SRV record, so a replica the
    /// decision adds receives shard work once it is Ready, and each query pins
    /// the membership it captured. The packaged `max: 1` remains a deliberate
    /// conservative pin until 2→4→2 scaling has been validated in kind; raise
    /// `max` explicitly to enable query-tier scale-out.
    #[serde(default = "default_query_autoscale")]
    pub query: ComponentAutoscale,
    /// EWMA half-life (seconds) for smoothing the per-pod saturation signals
    /// before the scaling decision. `0` (default) ⇒ no smoothing (react to the
    /// raw sample). A non-zero value damps flapping: a tier resizes on the
    /// blended signal rather than on a single busy or quiet sample, and a
    /// monitoring outage leaves the blend untouched instead of decaying it.
    /// REQUIRED ABOVE 0 BY A ZERO COMPACTOR FLOOR: with smoothing off the raw
    /// sample decides on its own, so one idle scrape parks the tier and the
    /// next segment starts it again. With smoothing on, a tier parks after ten
    /// half-lives of observed idleness, which is what `compactor.min: 0`
    /// bounds its own flapping with; `0` alongside that floor is refused with
    /// `InvalidSpec` / `AutoscalingZeroFloorNeedsSmoothing`.
    #[serde(rename = "ewmaHalfLifeSecs", default)]
    pub ewma_half_life_secs: f64,
}

impl Default for AutoscalingSpec {
    fn default() -> Self {
        Self {
            ingester: ComponentAutoscale::default(),
            compactor: ComponentAutoscale::default(),
            query: default_query_autoscale(),
            ewma_half_life_secs: 0.0,
        }
    }
}

/// Keep query scaling conservatively pinned to one replica until the 2→4→2
/// kind validation is complete. Users can opt into the supported range by
/// setting `spec.autoscaling.query.max` explicitly.
fn default_query_autoscale() -> ComponentAutoscale {
    ComponentAutoscale {
        max: 1,
        ..ComponentAutoscale::default()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ComponentAutoscale {
    /// Floor on the replica count. The operator never scales below this even
    /// when the workload signal goes to zero.
    ///
    /// `0` is supported for the COMPACTOR ONLY, and only under the
    /// catalog-claim drain (`compactor.max` above 1) with
    /// `ewmaHalfLifeSecs` above 0. The ingesters publish the shared queue
    /// depth (`siglake_wal_segments_sealed`) from the catalog connection they
    /// already hold, so a reading survives the stopped tier and asks for a
    /// worker back when a segment is registered; the tier parks after ten
    /// `ewmaHalfLifeSecs` half-lives of an empty queue, and is woken for
    /// maintenance — retention, delete tasks, claim reclaim, mirror sync —
    /// after an hour at zero. A zero-floor tier whose reading is missing
    /// entirely is restored to 1: a monitoring outage is not idleness.
    ///
    /// Everywhere else `0` is refused with `InvalidSpec` /
    /// `AutoscalingZeroFloorUnsupported` before any workload changes, because
    /// the ingest and query signals are published by the pods they size and a
    /// tier stopped at zero has nothing left to ask for it back. The
    /// filesystem drain (`compactor.max: 1`) is refused for the same reason:
    /// it never reads the shared queue.
    pub min: i32,

    /// Ceiling on the replica count. Protects shared-cluster
    /// neighbors from runaway scaling.
    pub max: i32,

    /// Target value for the component's primary load signal, per replica:
    /// - ingester: ingest requests/sec/pod.
    /// - compactor: sealed segments pending per worker. Above a `max` of 1 the
    ///   drain is the catalog claim and the backlog is ONE shared queue every
    ///   worker publishes in full, so the operator divides it by this target
    ///   once: a backlog of 8 at a target of 4 asks for 2 workers whatever the
    ///   current replica count.
    /// - query: in-flight queries per replica.
    pub target: f64,
}

impl Default for ComponentAutoscale {
    fn default() -> Self {
        Self {
            min: 1,
            max: 4,
            target: 1.0,
        }
    }
}

/// One row of `SiglakeClusterSpec::tenants`. A declared tenant the
/// cluster expects to serve; tenancy is resolved at ingest, so this is
/// advisory (no per-tenant secret).
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct TenantSpec {
    /// Tenant identifier — used verbatim as the Iceberg namespace
    /// (`tenant_<name>`) and as the per-tenant WAL subdirectory.
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SecretRef {
    pub name: String,
    pub key: String,
}

/// Status the reconciler writes back.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct SiglakeClusterStatus {
    #[serde(
        rename = "lastReconciled",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_reconciled: Option<String>,
    #[serde(default)]
    pub replicas: ReplicaStatus,
    #[serde(rename = "observedGeneration", default)]
    pub observed_generation: i64,
    /// Iceberg schema version the operator has provisioned for the
    /// `siglake.events` table (and per-tenant analogues). Today the
    /// operator doesn't manage schema migrations, but recording the
    /// version on status lets a future reconciler detect when the
    /// desired version moves and apply an `update_schema` Transaction.
    ///
    /// `None` unless the operator has actually OBSERVED a migration — that
    /// is, `spec.schemaVersion` was set and its Job ran to completion. The
    /// operator does not read the warehouse, so it cannot report the version
    /// of a table it never looked at; absent means "not observed", never
    /// "up to date". To read a table's recorded version, run
    /// `siglake migrate-schema --dry-run --all-namespaces`, which prints it
    /// per table from the table's own
    /// `siglake.schema_version.v1` property.
    /// Serialized even when `None` (explicit null): status updates go out as
    /// merge-patches, and omitting the field would LEAVE a stale prior value
    /// — observed live in the GA round: a schemaVersion bump kept reporting
    /// the old version while its migration Job was still running.
    #[serde(rename = "schemaVersion", default)]
    pub schema_version: Option<u32>,

    /// Free-form human-readable reason for the latest reconcile —
    /// e.g. "scaled compactor 1 → 2 on backlog".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Standard Kubernetes-style condition list. Condition types emitted by
    /// the operator include:
    ///
    /// - `Ready` — the managed Deployments are in their desired
    ///   replica counts and the most recent reconcile succeeded.
    /// - `Progressing` — a reconcile changed at least one
    ///   Deployment's `.spec.replicas` (true during scale events).
    /// - `InvalidSpec` — the requested spec cannot be honoured; `reason`
    ///   identifies the rule that rejected it and `Ready=False` accompanies it.
    /// - `QueryMemoryUndersized` — the query memory limit is below the decode
    ///   accounting floor. This is advisory and does not change `Ready`.
    ///
    /// Drives kubectl's `wait --for=condition=ready` workflows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct ReplicaStatus {
    pub ingester: i32,
    pub compactor: i32,
    pub query: i32,
}

/// A plain name/value environment variable for [`SiglakeClusterSpec::extra_env`].
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ExtraEnvVar {
    pub name: String,
    pub value: String,
}

/// Subset of the upstream `metav1.Condition` we actually populate.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct Condition {
    /// Condition type — e.g. `Ready` or `Progressing`.
    #[serde(rename = "type")]
    pub type_: String,
    /// `True`, `False`, or `Unknown`.
    pub status: String,
    /// Last time the status flipped.
    #[serde(rename = "lastTransitionTime")]
    pub last_transition_time: String,
    /// Short PascalCase identifier — e.g. `Scaled` / `NoOp`.
    pub reason: String,
    /// Human-readable detail.
    pub message: String,
}

#[cfg(test)]
mod tests {
    //! Regression suite for bug #17 — the field-level `#[serde(rename
    //! = "...")]` attributes have to survive the
    //! `#[derive(CustomResource)]` macro expansion so that what
    //! kube-rs's watcher sees from the API server (camelCase JSON)
    //! deserializes into our spec/status structs.
    //!
    //! We exercise both the inner spec (what `Deserialize` is derived
    //! on directly) and the kube-generated wrapper `SiglakeCluster`
    //! (which is what runtime callers actually deserialize).
    use super::*;
    use serde_json::json;

    #[test]
    fn spec_deser_camelcase() {
        let v = json!({
            "image": "ghcr.io/siglake/siglake:0.1.0",
            "warehouseUrl": "s3://b/warehouse",
            "catalogUri": "postgres://h/db",
            "storage": {
                "walStorageClassName": "efs-sc",
                "walSize": "10Gi",
                "walAccessMode": "ReadWriteMany"
            },
            "tenants": [{ "name": "acme" }],
            "authTokensSecretRef": {"name": "siglake-auth", "key": "tokens"}
        });
        let spec: SiglakeClusterSpec = serde_json::from_value(v).expect("camelCase deser");
        assert_eq!(spec.warehouse_url, "s3://b/warehouse");
        assert_eq!(spec.catalog_uri, "postgres://h/db");
        assert_eq!(spec.storage.wal_storage_class_name, "efs-sc");
        assert_eq!(spec.storage.wal_size, "10Gi");
        assert_eq!(spec.storage.wal_access_mode, "ReadWriteMany");
        assert_eq!(spec.tenants[0].name, "acme");
        assert_eq!(spec.auth_tokens_secret_ref.unwrap().key, "tokens");
    }

    #[test]
    fn default_query_policy_is_conservatively_pinned() {
        let autoscaling = AutoscalingSpec::default();
        assert_eq!(autoscaling.query.max, 1);
        assert_eq!(autoscaling.query.min, autoscaling.query.max);
        assert!(autoscaling.query.target > 0.0);

        let partial: AutoscalingSpec = serde_json::from_value(json!({})).unwrap();
        assert_eq!(partial.query.min, partial.query.max);
    }

    #[test]
    fn spec_rejects_snake_case() {
        // If a CRD client accidentally sends snake_case, we want the
        // failure to be loud (missing camelCase field), not silently
        // accepted via the wrong rename rule.
        let v = json!({
            "image": "x",
            "warehouse_url": "s3://b",
            "catalog_uri": "postgres://h/db"
        });
        let err = serde_json::from_value::<SiglakeClusterSpec>(v).unwrap_err();
        assert!(
            err.to_string().contains("warehouseUrl"),
            "want 'missing field warehouseUrl', got: {err}"
        );
    }

    #[test]
    fn wrapper_deser_camelcase() {
        // This is the path kube-rs's watcher actually exercises: an
        // ObjectList item with apiVersion/kind/metadata/spec at the
        // top level. The wrapper struct is generated by
        // `#[derive(CustomResource)]` and must propagate the inner
        // spec's Deserialize impl correctly.
        let v = json!({
            "apiVersion": "siglake.limnion.ai/v1alpha1",
            "kind": "SiglakeCluster",
            "metadata": {"name": "r8", "namespace": "siglake"},
            "spec": {
                "image": "x",
                "warehouseUrl": "s3://b",
                "catalogUri": "postgres://h/db",
                "storage": {
                    "walAccessMode": "ReadWriteMany",
                    "walSize": "5Gi",
                    "walStorageClassName": "efs-sc"
                }
            }
        });
        let cr: SiglakeCluster = serde_json::from_value(v).expect("wrapper deser");
        assert_eq!(cr.spec.warehouse_url, "s3://b");
        assert_eq!(cr.spec.catalog_uri, "postgres://h/db");
        assert_eq!(cr.spec.storage.wal_storage_class_name, "efs-sc");
    }

    #[test]
    fn spec_ser_is_camelcase() {
        let spec = SiglakeClusterSpec {
            image: "x".into(),
            warehouse_url: "s3://b".into(),
            catalog_uri: "postgres://h/db".into(),
            autoscaling: AutoscalingSpec::default(),
            tenants: vec![],
            auth_tokens_secret_ref: None,
            storage: StorageSpec::default(),
            retention: Default::default(),
            aws_region: String::new(),
            service_account_name: String::new(),
            schema_version: None,
            extra_env: Vec::new(),
            resources: Default::default(),
        };
        let s = serde_json::to_string(&spec).unwrap();
        assert!(s.contains("\"warehouseUrl\""), "got: {s}");
        assert!(s.contains("\"catalogUri\""), "got: {s}");
        assert!(s.contains("\"walSize\""), "got: {s}");
        assert!(!s.contains("warehouse_url"), "snake_case leaked: {s}");
    }

    /// `spec.resources.<tier>` is what a user writes to raise the query memory
    /// limit (task #545: the operator rendered 2Gi against the chart's 4Gi with
    /// no field to change it). Unset tiers stay `None` so the render falls back
    /// to the packaged defaults; a set tier carries exactly the keys given.
    #[test]
    fn spec_deser_resources_per_tier() {
        let v = json!({
            "image": "x",
            "warehouseUrl": "s3://b",
            "catalogUri": "postgres://h/db",
            "resources": {
                "query": {
                    "limits": { "memory": "8Gi" }
                },
                "compactor": {
                    "requests": { "cpu": "1" },
                    "limits": { "memory": "17Gi", "cpu": "4" }
                }
            }
        });
        let spec: SiglakeClusterSpec = serde_json::from_value(v).expect("resources deser");
        assert!(spec.resources.ingester.is_none(), "unset tier stays None");
        let query = spec.resources.query.expect("query set");
        assert!(query.requests.is_empty());
        assert_eq!(query.limits.get("memory").map(String::as_str), Some("8Gi"));
        let compactor = spec.resources.compactor.expect("compactor set");
        assert_eq!(compactor.requests.get("cpu").map(String::as_str), Some("1"));
        assert_eq!(compactor.limits.len(), 2);

        // Omitted entirely ⇒ all defaults (the pre-#545 CR shape still parses).
        let bare: SiglakeClusterSpec = serde_json::from_value(json!({
            "image": "x",
            "warehouseUrl": "s3://b",
            "catalogUri": "postgres://h/db"
        }))
        .unwrap();
        assert_eq!(bare.resources, ResourcesSpec::default());
        // And an all-default spec does not serialize the empty tiers as nulls,
        // which would otherwise land as `default: {ingester: null, …}` in the
        // generated CRD schema.
        let s = serde_json::to_string(&bare).unwrap();
        assert!(s.contains("\"resources\":{}"), "got: {s}");
    }

    #[test]
    fn autoscaling_ewma_half_life_is_camelcase() {
        let autoscaling: AutoscalingSpec = serde_json::from_value(json!({
            "ewmaHalfLifeSecs": 30.0
        }))
        .expect("camelCase EWMA half-life deser");
        assert_eq!(autoscaling.ewma_half_life_secs, 30.0);

        let value = serde_json::to_value(autoscaling).expect("EWMA half-life ser");
        assert_eq!(value["ewmaHalfLifeSecs"], 30.0);
        assert!(
            value.get("ewma_half_life_secs").is_none(),
            "snake_case leaked: {value}"
        );
    }

    #[test]
    fn status_ser_is_camelcase() {
        let st = SiglakeClusterStatus {
            last_reconciled: Some("2026-05-18T00:00:00Z".into()),
            replicas: ReplicaStatus {
                ingester: 2,
                compactor: 1,
                query: 1,
            },
            observed_generation: 3,
            schema_version: None,
            message: Some("scaled".into()),
            conditions: vec![Condition {
                type_: "Ready".into(),
                status: "True".into(),
                last_transition_time: "2026-05-18T00:00:00Z".into(),
                reason: "Healthy".into(),
                message: "ok".into(),
            }],
        };
        let s = serde_json::to_string(&st).unwrap();
        assert!(s.contains("\"lastReconciled\""), "got: {s}");
        assert!(s.contains("\"observedGeneration\""), "got: {s}");
        assert!(s.contains("\"lastTransitionTime\""), "got: {s}");
        assert!(!s.contains("last_reconciled"), "snake_case leaked: {s}");
        assert!(!s.contains("observed_generation"), "snake_case leaked: {s}");
    }

    #[test]
    fn status_schema_version_serializes_when_set() {
        // The reconciler now reports the events-schema version (was always
        // None). Confirm a Some value lands on the wire as camelCase, and the
        // observed generation round-trips a non-zero value.
        let st = SiglakeClusterStatus {
            observed_generation: 7,
            schema_version: Some(1),
            ..Default::default()
        };
        let s = serde_json::to_string(&st).unwrap();
        assert!(s.contains("\"schemaVersion\":1"), "got: {s}");
        assert!(s.contains("\"observedGeneration\":7"), "got: {s}");
        // None serializes as explicit null: status goes out as a merge-patch,
        // and omission would leave a stale prior value in place (GA-round
        // finding — the old version survived a mid-migration bump).
        let none = serde_json::to_string(&SiglakeClusterStatus::default()).unwrap();
        assert!(
            none.contains("\"schemaVersion\":null"),
            "None must be explicit null: {none}"
        );
    }
}
