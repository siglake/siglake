//! Reconciler — the kube-rs runtime that wires
//! [`crate::scaling::reconcile_replicas`] to a live cluster.
//!
//! For each `SiglakeCluster` watched, every reconcile cycle:
//!
//! 1. Read the spec.
//! 2. Probe Prometheus for the three load signals.
//! 3. Read the current `.spec.replicas` from each managed Deployment.
//! 4. Compute desired replicas via [`reconcile_replicas`].
//! 5. If anything changed, PATCH `.spec.replicas` on the Deployment.
//! 6. Write a status block back to the `SiglakeCluster`.
//!
//! v0 scope: replica scaling only. Per-tenant lifecycle (ingest token
//! Secret rotation, Iceberg namespace provisioning) is the next-step
//! extension; documented in the README.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::Service;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
    Client,
};

use crate::crd::{Condition, ReplicaStatus, SiglakeCluster, SiglakeClusterStatus};
use crate::prom::{PromClient, Queries};
use crate::render;
use crate::scaling::{
    fold_observation, maintenance_wake, reconcile_replicas, CurrentReplicas, DesiredReplicas,
    MAINTENANCE_WAKE_AFTER, MAINTENANCE_WAKE_HOLD,
};

/// Anything the reconciler needs to share across the controller loop.
pub struct Context {
    pub client: Client,
    pub prom: PromClient,
    /// EWMA-smoothed saturation signals per cluster (`namespace/name` →
    /// last-smoothed, when, and how long each signal has read idle), carried
    /// across reconcile cycles so a non-zero `ewma_half_life_secs` can damp the
    /// scaling decision. Empty until the first smoothed cycle; an unusable
    /// reading only marks the entry interrupted — see
    /// [`crate::scaling::fold_observation`].
    pub scaling_state:
        std::sync::Mutex<std::collections::HashMap<String, crate::scaling::SmoothingState>>,
    /// Where each zero-floor compactor tier is in the park / maintenance-wake
    /// cycle (`namespace/name` → phase). Only a `min: 0` policy ever has an
    /// entry, and the phase is dropped as soon as the reading wants the tier
    /// running — see [`crate::scaling::maintenance_wake`]. Held in memory: an
    /// operator restart starts the hour again, which delays a maintenance wake
    /// rather than skipping one.
    pub zero_floor_state:
        std::sync::Mutex<std::collections::HashMap<String, crate::scaling::ZeroFloorPhase>>,
}

/// Invalid specs are watched, so an edit wakes the controller immediately.
/// The long fallback requeue keeps recovery possible without hot-looping on a
/// spec that only a user edit can repair.
const INVALID_SPEC_REQUEUE: Duration = Duration::from_secs(300);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub async fn reconcile(cluster: Arc<SiglakeCluster>, ctx: Arc<Context>) -> Result<Action, Error> {
    let started = std::time::Instant::now();
    let namespace = cluster
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".into());
    let name = cluster
        .metadata
        .name
        .clone()
        .ok_or_else(|| anyhow::anyhow!("SiglakeCluster missing .metadata.name"))?;
    tracing::info!(%namespace, %name, "reconcile start");
    metrics::counter!("siglake_operator_reconciles_total",
        "namespace" => namespace.clone())
    .increment(1);

    // Validate before reading Prometheus or touching any child resource. An
    // unhonourable spec changes status only; the spec watch preempts the long
    // requeue as soon as the user repairs it.
    let now = chrono::Utc::now().to_rfc3339();
    if let Some(condition) = invalid_spec_condition(&cluster.spec, &now) {
        tracing::warn!(
            %namespace,
            %name,
            reason = %condition.reason,
            message = %condition.message,
            "refusing to reconcile invalid SiglakeCluster spec"
        );
        write_invalid_spec_status(
            &ctx.client,
            &namespace,
            &name,
            cluster.metadata.generation.unwrap_or(0),
            condition,
        )
        .await?;
        return Ok(Action::requeue(INVALID_SPEC_REQUEUE));
    }

    // A drain-mode change is a data handover, not a rollout. Read the two
    // existing pod templates before Prometheus or any apply so even a
    // zero-replica workload protects retained local and mirrored segments.
    let deployments: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
    let query_sts: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &namespace);
    let current_workloads = current_workloads(&deployments, &query_sts, &name).await?;
    if let Some(condition) = incompatible_drain_mode_condition(
        &cluster.spec,
        current_workloads.ingester.as_ref(),
        current_workloads.compactor.as_ref(),
        &now,
    ) {
        tracing::warn!(
            %namespace,
            %name,
            reason = %condition.reason,
            message = %condition.message,
            "refusing to change the WAL drain ownership protocol"
        );
        write_drain_mode_refusal_status(
            &ctx.client,
            &namespace,
            &name,
            cluster.metadata.generation.unwrap_or(0),
            condition,
        )
        .await?;
        return Ok(Action::requeue(INVALID_SPEC_REQUEUE));
    }
    let current = current_workloads.replicas;

    // A zero-floor compactor reads the depth the ingesters publish; every
    // other policy keeps the compactor's own gauge.
    let queries = Queries::for_policy(&name, &namespace, &cluster.spec.autoscaling.compactor);
    // Prometheus unavailability should not block reconciliation. A new cluster
    // has no series until its pods start, and a transient PromQL outage should
    // not stall every reconcile. Without a signal, cold tiers converge to
    // their floors while already-running tiers hold their size.
    // Per signal: a component whose query failed or read nothing is absent on
    // its own, and `prom::observed` logs and counts it with a `component`
    // label. Preserve a running fleet during a monitoring outage — cold tiers
    // are handled below, where a positive floor bootstraps the pods that begin
    // exporting metrics.
    let observation = ctx.prom.observed(&queries, &namespace).await;

    // EWMA-smooth the saturation signals when configured, carrying the smoothed
    // state across cycles. Damps autoscaler flapping. (The idle window it also
    // implements only matters to a zero-floor tier, which the spec check above
    // refuses; the smoothing itself applies to every accepted spec.)
    // `half_life == 0` (default) passes the raw sample through (alpha 1.0) and
    // clears any state. An unusable reading leaves the smoothed value and its
    // timestamp alone, so the outage leaves no trace in the history the next
    // usable reading is blended against — it only restarts the idle window.
    let observed = {
        let key = format!("{namespace}/{name}");
        let mut state = ctx.scaling_state.lock().expect("scaling_state poisoned");
        let folded = fold_observation(
            state.get(&key).cloned(),
            observation,
            std::time::Instant::now(),
            cluster.spec.autoscaling.ewma_half_life_secs,
        );
        match folded.state {
            Some(entry) => {
                state.insert(key, entry);
            }
            None => {
                state.remove(&key);
            }
        }
        folded.observed
    };

    // A component with a reading takes the ordinary decision; one without
    // holds its current size and still converges up to its floor — and never
    // below one replica, so a zero-floor tier comes back rather than reading a
    // monitoring outage as idleness.
    let mut desired = reconcile_replicas(&cluster.spec, &observed, &current);

    // Bound how long a parked compactor stays parked: retention, delete tasks,
    // claim reclaim and the mirror recovery sweep are all compactor-resident.
    if cluster.spec.autoscaling.compactor.min == 0 {
        let key = format!("{namespace}/{name}");
        let mut phases = ctx
            .zero_floor_state
            .lock()
            .expect("zero_floor_state poisoned");
        let (replicas, phase) = maintenance_wake(
            phases.get(&key).copied(),
            desired.compactor,
            std::time::Instant::now(),
            MAINTENANCE_WAKE_AFTER,
            MAINTENANCE_WAKE_HOLD,
        );
        if replicas > desired.compactor {
            tracing::info!(%namespace, %name,
                "waking the parked compactor for maintenance (retention, delete tasks, \
                 claim reclaim, mirror sync)");
        }
        desired.compactor = replicas;
        match phase {
            Some(phase) => {
                phases.insert(key, phase);
            }
            None => {
                phases.remove(&key);
            }
        }
    }
    let desired = desired;

    // PVC for the shared WAL. Has to land before the Deployments so
    // their pods can bind on first roll-out (without WaitForFirstConsumer
    // this would be a chicken-and-egg, but it doesn't hurt either way).
    let pvc = render::wal_pvc(&cluster);
    let pvc_api: Api<k8s_openapi::api::core::v1::PersistentVolumeClaim> =
        Api::namespaced(ctx.client.clone(), &namespace);
    pvc_api
        .patch(
            pvc.metadata.name.as_deref().unwrap_or_default(),
            &PatchParams::apply("siglake-operator").force(),
            &Patch::Apply(&pvc),
        )
        .await
        .with_context(|| {
            format!(
                "server-side-apply PVC/{}",
                pvc.metadata.name.as_deref().unwrap_or("")
            )
        })?;

    // Render + server-side apply the workloads. SSA reconciles
    // the entire spec (image, args, env, resources) every cycle; the
    // replica count comes from the scaling decision so a single apply
    // both creates and scales.
    let statefulsets: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &namespace);
    let services: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    let mut changed: Vec<&'static str> = Vec::new();

    // SCHEMA MIGRATION RUNS FIRST, AND THE ROLLOUT WAITS FOR IT.
    //
    // This used to apply the workloads and THEN the Job -- the reverse of what
    // the CRD promises ("run a one-shot, additive schema migration ... before
    // the rolled-out pods write under the new schema") -- and it never blocked
    // on the result: a failed migration wrote a status condition while the new
    // pods stayed applied.
    //
    // That window is silent data loss, not a delay. The new binary writes
    // batches carrying the new column against a table that lacks it, and
    // `align_batch_to_table_schema` projects the batch onto the TABLE's schema
    // BY NAME -- a batch column with no matching table field is simply never
    // written. Accepted, acknowledged, gone.
    // One-shot additive schema-migration Job, rendered only when the CR sets
    // `schemaVersion`. The Job name encodes the version, so applying the same
    // version is a no-op and a bump creates a fresh (immutable) Job. We never
    // delete a prior version's Job here — its TTL reaps it.
    let mut migration: Option<MigrationObservation> = None;
    if let Some(job) = render::migration_job(&cluster) {
        let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), &namespace);
        let job_name = job.metadata.name.as_deref().unwrap_or_default().to_string();
        job_api
            .patch(
                &job_name,
                &PatchParams::apply("siglake-operator").force(),
                &Patch::Apply(&job),
            )
            .await
            .with_context(|| format!("server-side-apply Job/{job_name}"))?;
        // GA: the operator OBSERVES the migration Job it applied, so
        // `status.schemaVersion` reflects what actually completed on the
        // warehouse (it used to report the binary's compiled-in constant —
        // true for fresh tables, a lie mid-migration) and a failed Job
        // surfaces as a condition instead of silently retrying forever.
        let version = cluster.spec.schema_version.unwrap_or(0);
        // Plain GET (not the /status subresource): the object carries status
        // anyway, and get_status needs a separate `jobs/status` RBAC grant —
        // the GA round found the missing grant mapped every observation to
        // the Err arm, i.e. "running" forever.
        migration = Some(match job_api.get(&job_name).await {
            Ok(live) => {
                let st = live.status.unwrap_or_default();
                if st.succeeded.unwrap_or(0) >= 1 {
                    MigrationObservation::Complete(version)
                } else if st.failed.unwrap_or(0) >= 4 {
                    MigrationObservation::Failed(version)
                } else {
                    MigrationObservation::Running(version)
                }
            }
            // Freshly created this reconcile: status not readable yet.
            Err(_) => MigrationObservation::Running(version),
        });
    }

    // Gate the rollout on the migration, but only for an UPGRADE. A fresh
    // install has no table to widen and no old data to lose, and blocking it
    // would mean the cluster never converges at all.
    let is_upgrade = cluster
        .status
        .as_ref()
        .and_then(|st| st.schema_version)
        .is_some();
    if is_upgrade
        && matches!(
            migration,
            Some(MigrationObservation::Running(_)) | Some(MigrationObservation::Failed(_))
        )
    {
        tracing::warn!(
            %namespace, %name, ?migration,
            "holding the rollout: the schema migration has not completed, and rolling pods now \
             would have them write columns the table does not yet have -- which the write path \
             drops silently"
        );
        metrics::counter!("siglake_operator_rollout_held_total").increment(1);
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // Ingester + compactor are stateless/claim-coordinated Deployments.
    let ingester = render::ingester_deployment(
        &cluster,
        desired.ingester,
        cluster.spec.auth_tokens_secret_ref.as_ref(),
    );
    apply_deployment(&deployments, &name_or(&ingester.metadata.name), &ingester).await?;
    if current.ingester != desired.ingester {
        changed.push("ingester");
    }
    let compactor = render::compactor_deployment(&cluster, desired.compactor);
    apply_deployment(&deployments, &name_or(&compactor.metadata.name), &compactor).await?;
    if current.compactor != desired.compactor {
        changed.push("compactor");
    }
    // Ingester client Service (OTLP/HTTP) — ClusterIP.
    let ing_svc = render::clusterip_service(&cluster, "ingester", render::INGESTER_PORTS);
    apply_service(&services, &name_or(&ing_svc.metadata.name), &ing_svc).await?;

    // Query is a StatefulSet (stable per-pod DNS for distributed query) with a
    // headless + a ClusterIP (client) Service.
    let q_hl = render::headless_service(&cluster, "query", render::QUERY_PORTS);
    apply_service(&services, &name_or(&q_hl.metadata.name), &q_hl).await?;
    let q_svc = render::clusterip_service(&cluster, "query", render::QUERY_PORTS);
    apply_service(&services, &name_or(&q_svc.metadata.name), &q_svc).await?;
    let query = render::query_statefulset(&cluster, desired.query);
    apply_statefulset(&statefulsets, &name_or(&query.metadata.name), &query).await?;
    if current.query != desired.query {
        changed.push("query");
    }

    // Phase 4.13g: retention CronJob. When the CR opts into
    // `retention.queryAuditRotateIntervalDays`, render + SSA the
    // CronJob; when it doesn't, leave any prior CronJob alone (an
    // explicit opt-out would `kubectl delete`, but we don't want
    // to delete a CronJob a customer might have manually created).
    let cj_api: Api<CronJob> = Api::namespaced(ctx.client.clone(), &namespace);
    let mut cronjobs = Vec::new();
    if let Some(cj) = render::audit_rotate_cronjob(&cluster) {
        cronjobs.push(cj);
    }
    // Non-destructive detection-table retention sweeps (one CronJob per table).
    for cronjob in &cronjobs {
        let cj_name = cronjob.metadata.name.as_deref().unwrap_or_default();
        cj_api
            .patch(
                cj_name,
                &PatchParams::apply("siglake-operator").force(),
                &Patch::Apply(cronjob),
            )
            .await
            .with_context(|| format!("server-side-apply CronJob/{cj_name}"))?;
    }

    let summary = if changed.is_empty() {
        "no-op".to_string()
    } else {
        format!(
            "scaled {} (ing {}→{}, comp {}→{}, qry {}→{})",
            changed.join(","),
            current.ingester,
            desired.ingester,
            current.compactor,
            desired.compactor,
            current.query,
            desired.query,
        )
    };
    tracing::info!(%namespace, %name, summary = %summary, ?observed, "reconcile decision");

    let progressing = !changed.is_empty();
    // Read readiness AFTER the applies, so the condition reflects this cycle.
    let observed = observed_ready(&deployments, &query_sts, &name).await?;
    write_status(
        &ctx.client,
        &namespace,
        &name,
        &cluster.spec,
        &desired,
        migration,
        summary,
        progressing,
        cluster.metadata.generation.unwrap_or(0),
        &observed,
    )
    .await?;
    let elapsed = started.elapsed().as_secs_f64();
    metrics::histogram!("siglake_operator_reconcile_duration_seconds",
        "namespace" => namespace.clone())
    .record(elapsed);
    metrics::gauge!("siglake_operator_managed_replicas",
        "namespace" => namespace.clone(),
        "component" => "ingester".to_string())
    .set(desired.ingester as f64);
    metrics::gauge!("siglake_operator_managed_replicas",
        "namespace" => namespace.clone(),
        "component" => "compactor".to_string())
    .set(desired.compactor as f64);
    metrics::gauge!("siglake_operator_managed_replicas",
        "namespace" => namespace.clone(),
        "component" => "query".to_string())
    .set(desired.query as f64);

    // Re-reconcile every 30s by default; the watch will preempt
    // earlier on spec changes.
    Ok(Action::requeue(Duration::from_secs(30)))
}

/// A failed reconcile must not leave the previous cycle's `Ready=True` standing.
///
/// Status is written at the END of a successful reconcile, so an error anywhere
/// before that simply skipped the write — and the last good status stayed
/// visible indefinitely. A cluster erroring every 15s reported Ready with a
/// `lastReconciled` that quietly aged, which is the failure mode conditions
/// exist to prevent.
///
/// The trait method is sync, so the patch is spawned. Best-effort by
/// construction: if it fails there is nothing to report it to, and the reconcile
/// is already being retried.
pub fn error_policy(cluster: Arc<SiglakeCluster>, err: &Error, ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "reconcile failed; backing off");
    metrics::counter!("siglake_operator_reconcile_errors_total").increment(1);

    if let (Some(namespace), Some(name)) = (
        cluster.metadata.namespace.clone(),
        cluster.metadata.name.clone(),
    ) {
        let client = ctx.client.clone();
        let detail = err.to_string();
        tokio::spawn(async move {
            let clusters: Api<SiglakeCluster> = Api::namespaced(client, &namespace);
            let now = chrono::Utc::now().to_rfc3339();
            let patch = serde_json::json!({
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "False",
                        "lastTransitionTime": now,
                        "reason": "ReconcileFailed",
                        "message": detail.chars().take(1024).collect::<String>(),
                    }]
                }
            });
            if let Err(e) = clusters
                .patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                tracing::warn!(error = %e, "could not record the reconcile failure in status");
            }
        });
    }
    Action::requeue(Duration::from_secs(15))
}

/// Read each scalable tier's CURRENT replica count and the two drain templates.
///
/// The tier's workload kind has to match what `render` actually creates. Query
/// is a StatefulSet (stable per-pod DNS is what makes distributed fan-out
/// addressable), and reading it as a Deployment does not error — the GET 404s
/// and 404 means "not provisioned yet, bootstrap from zero". So the autoscaler
/// silently saw query at 0 replicas forever, took `decide`'s cold-start branch
/// every cycle, and pinned the tier at `min` no matter what the in-flight
/// metric said. It also made `changed` non-empty on every pass, leaving status
/// permanently Progressing. A kind mismatch here is invisible at runtime, which
/// is why `render_kinds_match_replica_sources` pins it to the render. Keeping
/// the Deployment objects also lets the reconcile reject a drain-mode handover
/// before applying any child resource.
struct CurrentWorkloads {
    replicas: CurrentReplicas,
    ingester: Option<Deployment>,
    compactor: Option<Deployment>,
}

async fn current_workloads(
    deployments: &Api<Deployment>,
    query_sts: &Api<StatefulSet>,
    base: &str,
) -> Result<CurrentWorkloads> {
    let ingester = deployment(deployments, &format!("{base}-ingester")).await?;
    let compactor = deployment(deployments, &format!("{base}-compactor")).await?;
    let replicas = CurrentReplicas {
        ingester: ingester
            .as_ref()
            .and_then(|deployment| deployment.spec.as_ref())
            .and_then(|spec| spec.replicas)
            .unwrap_or(0),
        compactor: compactor
            .as_ref()
            .and_then(|deployment| deployment.spec.as_ref())
            .and_then(|spec| spec.replicas)
            .unwrap_or(0),
        query: statefulset_replicas(query_sts, &format!("{base}-query")).await?,
    };
    Ok(CurrentWorkloads {
        replicas,
        ingester,
        compactor,
    })
}

async fn deployment(api: &Api<Deployment>, name: &str) -> Result<Option<Deployment>> {
    match api.get(name).await {
        Ok(deployment) => Ok(Some(deployment)),
        Err(kube::Error::Api(api)) if api.code == 404 => Ok(None),
        Err(error) => Err(anyhow::Error::from(error).context(format!("get Deployment/{name}"))),
    }
}

async fn statefulset_replicas(api: &Api<StatefulSet>, name: &str) -> Result<i32> {
    match api.get(name).await {
        Ok(s) => Ok(s.spec.and_then(|s| s.replicas).unwrap_or(0)),
        Err(kube::Error::Api(api)) if api.code == 404 => {
            // Not yet provisioned — treat as 0 so the reconciler bootstraps to min.
            Ok(0)
        }
        Err(e) => Err(anyhow::Error::from(e).context(format!("get StatefulSet/{name}"))),
    }
}

/// Ready replica counts, as OBSERVED on the workloads.
///
/// Distinct from `CurrentReplicas`, which is `spec.replicas` — what we asked
/// for. The `Ready` condition has to key on what actually came up, or it means
/// nothing: it was previously hardcoded `True` on every completed reconcile, so
/// a cluster whose pods were all crash-looping reported Ready, and
/// `kubectl wait --for=condition=ready` — which the adoption runbook depends on
/// — returned instantly and proved nothing.
#[derive(Clone, Debug, Default)]
struct ObservedReady {
    ingester: i32,
    compactor: i32,
    query: i32,
}

async fn deployment_ready(deployments: &Api<Deployment>, name: &str) -> Result<i32> {
    match deployments.get(name).await {
        Ok(d) => Ok(d.status.and_then(|s| s.ready_replicas).unwrap_or(0)),
        Err(kube::Error::Api(api)) if api.code == 404 => Ok(0),
        Err(e) => Err(anyhow::Error::from(e).context(format!("get Deployment/{name} status"))),
    }
}

async fn statefulset_ready(api: &Api<StatefulSet>, name: &str) -> Result<i32> {
    match api.get(name).await {
        Ok(s) => Ok(s.status.and_then(|s| s.ready_replicas).unwrap_or(0)),
        Err(kube::Error::Api(api)) if api.code == 404 => Ok(0),
        Err(e) => Err(anyhow::Error::from(e).context(format!("get StatefulSet/{name} status"))),
    }
}

async fn observed_ready(
    deployments: &Api<Deployment>,
    query_sts: &Api<StatefulSet>,
    base: &str,
) -> Result<ObservedReady> {
    Ok(ObservedReady {
        ingester: deployment_ready(deployments, &format!("{base}-ingester")).await?,
        compactor: deployment_ready(deployments, &format!("{base}-compactor")).await?,
        query: statefulset_ready(query_sts, &format!("{base}-query")).await?,
    })
}

/// Server-side apply the rendered Deployment. The operator is the
/// declared field manager (`siglake-operator`), so any field it owns
/// stays under its control; user edits to *other* fields are
/// preserved.
async fn apply_deployment(
    deployments: &Api<Deployment>,
    name: &str,
    dep: &Deployment,
) -> Result<()> {
    deployments
        .patch(
            name,
            &PatchParams::apply("siglake-operator").force(),
            &Patch::Apply(dep),
        )
        .await
        .with_context(|| format!("server-side-apply Deployment/{name}"))?;
    Ok(())
}

fn name_or(name: &Option<String>) -> String {
    name.clone().unwrap_or_default()
}

async fn apply_statefulset(api: &Api<StatefulSet>, name: &str, obj: &StatefulSet) -> Result<()> {
    api.patch(
        name,
        &PatchParams::apply("siglake-operator").force(),
        &Patch::Apply(obj),
    )
    .await
    .with_context(|| format!("server-side-apply StatefulSet/{name}"))?;
    Ok(())
}

async fn apply_service(api: &Api<Service>, name: &str, obj: &Service) -> Result<()> {
    api.patch(
        name,
        &PatchParams::apply("siglake-operator").force(),
        &Patch::Apply(obj),
    )
    .await
    .with_context(|| format!("server-side-apply Service/{name}"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
/// What the operator observed about the CURRENT spec.schemaVersion's
/// migration Job this reconcile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MigrationObservation {
    Complete(u32),
    Running(u32),
    Failed(u32),
}

#[allow(clippy::too_many_arguments)]
async fn write_status(
    client: &Client,
    namespace: &str,
    name: &str,
    spec: &crate::crd::SiglakeClusterSpec,
    desired: &DesiredReplicas,
    migration: Option<MigrationObservation>,
    message: String,
    progressing: bool,
    generation: i64,
    observed: &ObservedReady,
) -> Result<()> {
    // Ready means the workloads are up, not that the reconcile returned Ok. A
    // tier still rolling out is legitimately not-ready, and saying so is the
    // point: this condition is what automation waits on.
    let ready_now = observed.ingester >= desired.ingester
        && observed.compactor >= desired.compactor
        && observed.query >= desired.query;
    let ready_detail = format!(
        "ingester {}/{}, compactor {}/{}, query {}/{}",
        observed.ingester,
        desired.ingester,
        observed.compactor,
        desired.compactor,
        observed.query,
        desired.query
    );
    let clusters: Api<SiglakeCluster> = Api::namespaced(client.clone(), namespace);
    let now = chrono::Utc::now().to_rfc3339();
    let mut conditions = vec![
        Condition {
            type_: "Ready".into(),
            status: if ready_now { "True" } else { "False" }.into(),
            last_transition_time: now.clone(),
            reason: if ready_now {
                "WorkloadsReady"
            } else {
                "WorkloadsNotReady"
            }
            .into(),
            message: if ready_now {
                message.clone()
            } else {
                format!("{message}; ready {ready_detail}")
            },
        },
        Condition {
            type_: "Progressing".into(),
            status: if progressing { "True" } else { "False" }.into(),
            last_transition_time: now.clone(),
            reason: if progressing { "Scaling" } else { "NoOp" }.into(),
            message: message.clone(),
        },
    ];
    if let Some(m) = migration {
        let (status, reason, detail) = match m {
            MigrationObservation::Complete(v) => (
                "True",
                "JobSucceeded",
                format!("schema v{v} migration complete"),
            ),
            MigrationObservation::Running(v) => (
                "Unknown",
                "JobRunning",
                format!("schema v{v} migration in progress"),
            ),
            MigrationObservation::Failed(v) => (
                "False",
                "JobFailed",
                format!("schema v{v} migration Job exhausted retries"),
            ),
        };
        conditions.push(Condition {
            type_: "SchemaMigrated".into(),
            status: status.into(),
            last_transition_time: now.clone(),
            reason: reason.into(),
            message: detail,
        });
    }
    if let Some(condition) = query_memory_advisory_condition(spec, &now) {
        conditions.push(condition);
    }
    let status = serde_json::json!({
        "status": SiglakeClusterStatus {
            last_reconciled: Some(now),
            // OBSERVED, not desired: `status` is what IS, `spec`/autoscaling is
            // what was asked for. Reporting desired here made status a mirror of
            // intent that could never disagree with it, so it could never
            // surface a tier that failed to come up.
            replicas: ReplicaStatus {
                ingester: observed.ingester,
                compactor: observed.compactor,
                query: observed.query,
            },
            // Reflect the CR generation the operator has acted on, so
            // `kubectl wait` / clients can tell a reconcile has caught up
            // to the latest spec edit (was hardcoded 0).
            observed_generation: generation,
            // Truthful schema version: this field reports an OBSERVATION, and
            // the only observation the operator makes is a migration Job it
            // ran to completion. Report that, or nothing.
            //
            // It used to fall back to the image's compiled-in
            // EVENTS_SCHEMA_VERSION when no migration was requested, justified
            // as "fresh tables are provisioned at the image's current schema".
            // That is true of fresh tables and false of every existing one:
            // for a cluster merely being upgraded, the operator never reads
            // the warehouse at all, so the field flipped to the new version
            // the instant the new image rolled — whether or not anything
            // migrated. That is the one field an operator checks to answer
            // "did my table get migrated?", and it always said yes. Absent is
            // the honest answer to a question nobody asked; `siglake
            // migrate-schema` prints each table's recorded version for the
            // operator who wants to know.
            schema_version: match migration {
                Some(MigrationObservation::Complete(v)) => Some(v),
                Some(_) | None => None,
            },
            message: Some(message),
            conditions,
        }
    });
    // PatchParams::apply() expects an SSA payload (Patch::Apply); we
    // build a partial-document merge below, so pair it with default
    // PatchParams. Mixing apply-params with Patch::Merge causes the
    // API server to 422.
    clusters
        .patch_status(name, &PatchParams::default(), &Patch::Merge(&status))
        .await
        .with_context(|| format!("patch status SiglakeCluster/{name}"))?;
    Ok(())
}

fn query_memory_advisory_condition(
    spec: &crate::crd::SiglakeClusterSpec,
    now: &str,
) -> Option<Condition> {
    render::query_memory_advisory(spec).map(|message| Condition {
        type_: "QueryMemoryUndersized".into(),
        status: "True".into(),
        last_transition_time: now.into(),
        reason: "QueryMemoryBelowDecodeFloor".into(),
        message,
    })
}

fn incompatible_drain_mode_condition(
    spec: &crate::crd::SiglakeClusterSpec,
    ingester: Option<&Deployment>,
    compactor: Option<&Deployment>,
    now: &str,
) -> Option<Condition> {
    let wanted = render::uses_catalog_claim(&spec.autoscaling.compactor);
    let wanted_name = drain_mode_name(wanted);
    let mut conflicts = Vec::new();

    if let Some(deployment) = ingester {
        match ingester_remote_wal_drain(deployment) {
            Some(observed) if observed != wanted => conflicts.push(format!(
                "Deployment/{} uses the {} drain",
                name_or(&deployment.metadata.name),
                drain_mode_name(observed)
            )),
            None => conflicts.push(format!(
                "Deployment/{} has a non-literal SIGLAKE_REMOTE_WAL_DRAIN value",
                name_or(&deployment.metadata.name)
            )),
            Some(_) => {}
        }
    }
    if let Some(deployment) = compactor {
        let observed = compactor_uses_catalog_claim(deployment);
        if observed != wanted {
            conflicts.push(format!(
                "Deployment/{} uses the {} drain",
                name_or(&deployment.metadata.name),
                drain_mode_name(observed)
            ));
        }
    }

    if conflicts.is_empty() {
        return None;
    }

    Some(Condition {
        type_: "DrainModeCompatible".into(),
        status: "False".into(),
        last_transition_time: now.into(),
        reason: "DrainModeHandoverRequired".into(),
        message: format!(
            "{}; spec.autoscaling.compactor.max ({}) selects the {wanted_name} drain. Restore the previous compactor maximum, or perform a manual handover: stop ingestion, finish and verify the current drain, account for retained local and mirrored segments, then delete both Deployments so the operator can create them in the selected mode. The operator does not convert or delete WAL, mirror objects, or catalog rows during a handover",
            conflicts.join("; "),
            spec.autoscaling.compactor.max
        ),
    })
}

fn drain_mode_name(catalog_claim: bool) -> &'static str {
    if catalog_claim {
        "catalog-claim"
    } else {
        "filesystem"
    }
}

fn compactor_uses_catalog_claim(deployment: &Deployment) -> bool {
    deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .and_then(|pod| {
            pod.containers
                .iter()
                .find(|container| container.name == "compactor")
        })
        .and_then(|container| container.args.as_ref())
        .is_some_and(|args| args.iter().any(|arg| arg == "--catalog-claim"))
}

fn ingester_remote_wal_drain(deployment: &Deployment) -> Option<bool> {
    let container = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .and_then(|pod| {
            pod.containers
                .iter()
                .find(|container| container.name == "ingester")
        })?;
    let Some(entry) = container.env.as_ref().and_then(|env| {
        env.iter()
            .rev()
            .find(|entry| entry.name == "SIGLAKE_REMOTE_WAL_DRAIN")
    }) else {
        // Match the ingest binary's conservative default.
        return Some(true);
    };
    let raw = entry.value.as_deref()?;
    Some(!matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "0" | "false"
    ))
}

#[cfg(test)]
const INVALID_SPEC_MESSAGE_PATHS: &[&str] = &[
    "spec.image",
    "spec.warehouseUrl",
    "spec.catalogUri",
    "spec.autoscaling.{component}.min",
    "spec.autoscaling.{component}.min",
    "spec.autoscaling.compactor.min",
    "spec.autoscaling.compactor.min",
    "spec.autoscaling.ewmaHalfLifeSecs",
    "spec.autoscaling.compactor.max",
    "spec.extraEnv",
    "spec.autoscaling.query.max",
    "spec.extraEnv",
    "spec.autoscaling.{component}.target",
    "spec.autoscaling.ewmaHalfLifeSecs",
    "spec.retention.queryAuditRotateIntervalDays",
];

fn invalid_spec_condition(spec: &crate::crd::SiglakeClusterSpec, now: &str) -> Option<Condition> {
    let invalid = |reason: &str, message: String| {
        Some(Condition {
            type_: "InvalidSpec".into(),
            status: "True".into(),
            last_transition_time: now.into(),
            reason: reason.into(),
            message,
        })
    };

    if spec.image.trim().is_empty() {
        return invalid(
            "ImageRequired",
            "spec.image must name the container image to run; set it explicitly".into(),
        );
    }
    if spec.warehouse_url.trim().is_empty() {
        return invalid(
            "WarehouseUrlRequired",
            "spec.warehouseUrl must name the Iceberg warehouse; set it explicitly".into(),
        );
    }
    if spec.catalog_uri.trim().is_empty() {
        return invalid(
            "CatalogUriRequired",
            "spec.catalogUri must name the Iceberg catalog; set it explicitly".into(),
        );
    }

    let policies = [
        ("ingester", &spec.autoscaling.ingester),
        ("compactor", &spec.autoscaling.compactor),
        ("query", &spec.autoscaling.query),
    ];
    for (component, policy) in policies {
        if policy.min < 0 || policy.max < policy.min {
            return invalid(
                "AutoscalingRangeInvalid",
                format!(
                    "spec.autoscaling.{component}.min ({}) must be non-negative and max ({}) must be greater than or equal to min",
                    policy.min, policy.max
                ),
            );
        }
        // A zero floor needs a signal that SURVIVES the stopped tier. The
        // ingest and query signals are per-pod readings exported by the pods
        // they size, so a tier parked at zero publishes nothing and nothing
        // can ask for it back. The compactor is the one component with an
        // independent reading: under the catalog claim the ingesters publish
        // the shared queue depth themselves (#6011), and they keep running
        // because every accepted spec holds their floor at 1 or more.
        if policy.min == 0 && component != "compactor" {
            return invalid(
                "AutoscalingZeroFloorUnsupported",
                format!(
                    "spec.autoscaling.{component}.min is 0, and no supported policy can restart a {component} tier from zero: the {component} load signal is published by the {component} pods themselves, so once they stop nothing requests them back. Set a floor of 1 or more (max is {})",
                    policy.max
                ),
            );
        }
        // The filesystem drain has no such reading. It sweeps the local
        // `sealed/` directory and never consults `wal_segments`, and while the
        // ingester's registrar still inserts rows, nothing transitions them
        // unless the mirror ledger is enabled — so a catalog depth under that
        // drain climbs forever and can never read idle.
        if policy.min == 0 && !crate::render::uses_catalog_claim(policy) {
            return invalid(
                "AutoscalingZeroFloorUnsupported",
                format!(
                    "spec.autoscaling.compactor.min is 0 with max {}, which is the filesystem drain: that mode sweeps each ingester's local sealed directory and never reads the shared `wal_segments` queue, so the catalog depth that would ask for a worker back never falls to zero and never rises for work the drain has not seen. A zero floor needs the catalog-claim drain — raise max above 1 — or a floor of 1",
                    policy.max
                ),
            );
        }
        // At `ewmaHalfLifeSecs: 0` the raw sample passes straight through to
        // the decision, so ONE idle scrape parks the tier and the next
        // registered segment starts it again. Smoothing's idle window is what
        // makes parking a decision about a quiet period rather than about a
        // single sample.
        if policy.min == 0 && spec.autoscaling.ewma_half_life_secs <= 0.0 {
            return invalid(
                "AutoscalingZeroFloorNeedsSmoothing",
                format!(
                    "spec.autoscaling.compactor.min is 0 while spec.autoscaling.ewmaHalfLifeSecs is {}: with smoothing off the raw sample decides on its own, so a single idle scrape parks the tier and it flaps against continuous ingest. Set ewmaHalfLifeSecs above 0 — the tier then parks after {} half-lives of observed idleness — or set a compactor floor of 1",
                    spec.autoscaling.ewma_half_life_secs,
                    crate::scaling::IDLE_HALF_LIVES,
                ),
            );
        }
    }

    if spec.autoscaling.compactor.max > 1
        && crate::render::effective_wal_mirror_prefix(spec).is_none()
    {
        return invalid(
            "WalMirrorRequiredForCompactorScaleOut",
            format!(
                "spec.autoscaling.compactor.max ({}) permits catalog-claim compaction while the effective WAL mirror is disabled by a blank SIGLAKE_WAL_MIRROR_PREFIX in spec.extraEnv; set a non-blank mirror prefix or hold the compactor maximum at 1",
                spec.autoscaling.compactor.max
            ),
        );
    }

    if spec.autoscaling.query.max > 1
        && crate::render::effective_query_jobs_store_uri(spec).is_none()
    {
        return invalid(
            "QueryJobsStoreRequiredForScaleOut",
            format!(
                "spec.autoscaling.query.max ({}) permits more than one query pod while the effective batch-job store is in-memory: status, result and cancel requests served by another healthy pod would answer 404 job not found. Set a non-blank SIGLAKE_JOBS_POSTGRES_URI in spec.extraEnv, use a Postgres catalog URI without overriding the jobs-store URI, or hold its maximum at 1",
                spec.autoscaling.query.max
            ),
        );
    }

    // #967 removed the `QueryAutoscalingRangeUnsupported` refusal that lived
    // here. It existed because the operator rendered `--query-peers` from the
    // replica count, so a pod beyond that count coordinated but received no
    // shard work. The pods now discover each other through the headless
    // Service's SRV record, so a query range is an ordinary supported range and
    // there is no replacement condition — `AutoscalingRangeInvalid` above and
    // `AutoscalingTargetNotPositive` below still catch a malformed one.
    // Discovery health is a query-server metric, not an operator condition: the
    // operator cannot tell DNS cache lag from a data-plane failure.

    for (component, policy) in policies {
        if !policy.target.is_finite() || policy.target <= 0.0 {
            return invalid(
                "AutoscalingTargetNotPositive",
                format!(
                    "spec.autoscaling.{component}.target ({}) must be a finite value greater than 0",
                    policy.target
                ),
            );
        }
    }

    if !spec.autoscaling.ewma_half_life_secs.is_finite()
        || spec.autoscaling.ewma_half_life_secs < 0.0
    {
        return invalid(
            "AutoscalingEwmaHalfLifeInvalid",
            format!(
                "spec.autoscaling.ewmaHalfLifeSecs ({}) must be a finite non-negative value",
                spec.autoscaling.ewma_half_life_secs
            ),
        );
    }

    if let Some(days) = spec.retention.query_audit_rotate_interval_days {
        if !matches!(days, 1 | 7 | 30) {
            return invalid(
                "RetentionIntervalUnsupported",
                format!(
                    "spec.retention.queryAuditRotateIntervalDays ({days}) cannot be represented exactly; use 1 (daily), 7 (weekly), or 30 (monthly)"
                ),
            );
        }
    }

    if let Some((reason, message)) = crate::render::compactor_bin_concurrency_error(spec) {
        return invalid(reason, message);
    }

    None
}

fn invalid_spec_status(generation: i64, condition: Condition) -> serde_json::Value {
    let now = condition.last_transition_time.clone();
    let message = condition.message.clone();
    serde_json::json!({
        "status": {
            "lastReconciled": now,
            "observedGeneration": generation,
            "message": message.clone(),
            "conditions": [
                Condition {
                    type_: "Ready".into(),
                    status: "False".into(),
                    last_transition_time: now,
                    reason: "InvalidSpec".into(),
                    message,
                },
                condition,
            ],
        }
    })
}

async fn write_invalid_spec_status(
    client: &Client,
    namespace: &str,
    name: &str,
    generation: i64,
    condition: Condition,
) -> Result<()> {
    let clusters: Api<SiglakeCluster> = Api::namespaced(client.clone(), namespace);
    let status = invalid_spec_status(generation, condition);
    clusters
        .patch_status(name, &PatchParams::default(), &Patch::Merge(&status))
        .await
        .with_context(|| format!("patch invalid spec status SiglakeCluster/{name}"))?;
    Ok(())
}

async fn write_drain_mode_refusal_status(
    client: &Client,
    namespace: &str,
    name: &str,
    generation: i64,
    condition: Condition,
) -> Result<()> {
    let clusters: Api<SiglakeCluster> = Api::namespaced(client.clone(), namespace);
    let now = condition.last_transition_time.clone();
    let message = condition.message.clone();
    let reason = condition.reason.clone();
    let status = serde_json::json!({
        "status": {
            "lastReconciled": now.clone(),
            "observedGeneration": generation,
            "message": message.clone(),
            "conditions": [
                Condition {
                    type_: "Ready".into(),
                    status: "False".into(),
                    last_transition_time: now,
                    reason,
                    message,
                },
                condition,
            ],
        }
    });
    clusters
        .patch_status(name, &PatchParams::default(), &Patch::Merge(&status))
        .await
        .with_context(|| format!("patch drain-mode refusal status SiglakeCluster/{name}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        incompatible_drain_mode_condition, invalid_spec_condition, invalid_spec_status,
        query_memory_advisory_condition, reconcile, Context, INVALID_SPEC_MESSAGE_PATHS,
    };
    use crate::crd::{
        ComponentAutoscale, ExtraEnvVar, SiglakeCluster, SiglakeClusterSpec, TierResources,
    };
    use kube::CustomResourceExt;
    use kube::{client::Body, Client};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use tower::service_fn;

    fn spec_paths_in_string_literals(source: &str) -> Vec<&str> {
        fn extract_paths<'a>(literal: &'a str, paths: &mut Vec<&'a str>) {
            let mut rest = literal;
            while let Some(start) = rest.find("spec.") {
                let candidate = &rest[start..];
                let end = candidate
                    .find(|ch: char| {
                        !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '{' | '}'))
                    })
                    .unwrap_or(candidate.len());
                paths.push(&candidate[..end]);
                rest = &candidate[end..];
            }
        }

        let mut paths = Vec::new();
        for line in source.lines() {
            let mut literal_start = None;
            let mut escaped = false;
            for (index, ch) in line.char_indices() {
                if ch == '"' && !escaped {
                    if let Some(start) = literal_start.take() {
                        extract_paths(&line[start..index], &mut paths);
                    } else {
                        literal_start = Some(index + 1);
                    }
                }
                escaped = ch == '\\' && !escaped;
            }
        }
        paths
    }

    fn valid_spec() -> SiglakeClusterSpec {
        SiglakeClusterSpec {
            image: "siglake:test".into(),
            warehouse_url: "s3://test/warehouse".into(),
            catalog_uri: "postgres://test/db".into(),
            ..Default::default()
        }
    }

    #[derive(Debug)]
    struct RecordedRequest {
        method: String,
        path: String,
        body: Vec<u8>,
    }

    async fn assert_drain_handover_refused(old_max: i32, new_max: i32) {
        let mut old = SiglakeCluster::new("acme", valid_spec());
        old.metadata.namespace = Some("test".into());
        old.spec.autoscaling.compactor.max = old_max;
        // Zero replicas still leave pod templates and retained WAL ownership
        // behind. The ingester template stands for mirrored segment
        // registration; the compactor template selects local or catalog drain.
        let ingester = crate::render::ingester_deployment(&old, 0, None);
        let compactor = crate::render::compactor_deployment(&old, 0);

        let mut wanted = SiglakeCluster::new("acme", valid_spec());
        wanted.metadata.namespace = Some("test".into());
        wanted.metadata.generation = Some(7);
        wanted.spec.autoscaling.compactor.max = new_max;

        assert!(
            incompatible_drain_mode_condition(
                &wanted.spec,
                Some(&ingester),
                Some(&compactor),
                "now"
            )
            .is_some(),
            "fixture must cross the drain-mode boundary"
        );

        let requests = Arc::new(Mutex::new(Vec::<RecordedRequest>::new()));
        let recorded = Arc::clone(&requests);
        let response_cr = wanted.clone();
        let service = service_fn(move |request: http::Request<Body>| {
            let recorded = Arc::clone(&recorded);
            let ingester = ingester.clone();
            let compactor = compactor.clone();
            let response_cr = response_cr.clone();
            async move {
                let (parts, body) = request.into_parts();
                let bytes = body.collect_bytes().await.unwrap().to_vec();
                let path = parts.uri.path().to_owned();
                recorded.lock().unwrap().push(RecordedRequest {
                    method: parts.method.to_string(),
                    path: path.clone(),
                    body: bytes,
                });

                let (status, body) = if path.ends_with("/deployments/acme-ingester") {
                    (200, serde_json::to_vec(&ingester).unwrap())
                } else if path.ends_with("/deployments/acme-compactor") {
                    (200, serde_json::to_vec(&compactor).unwrap())
                } else if path.ends_with("/statefulsets/acme-query") {
                    (
                        404,
                        serde_json::to_vec(&serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Status",
                            "status": "Failure",
                            "message": "not found",
                            "reason": "NotFound",
                            "code": 404
                        }))
                        .unwrap(),
                    )
                } else if path.ends_with("/siglakeclusters/acme/status") {
                    (200, serde_json::to_vec(&response_cr).unwrap())
                } else {
                    (500, b"unexpected request".to_vec())
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
            }
        });
        let client = Client::new(service, "test");
        let prom = crate::prom::PromClient::new("http://127.0.0.1:9").unwrap();
        let context = Arc::new(Context {
            client,
            prom,
            scaling_state: Default::default(),
            zero_floor_state: Default::default(),
        });

        reconcile(Arc::new(wanted), context)
            .await
            .expect("an unsafe handover is a status refusal, not a reconcile error");

        let requests = requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            4,
            "the refusal may only read workloads and patch status: {requests:?}"
        );
        assert!(requests
            .iter()
            .take(3)
            .all(|request| request.method == "GET"));
        let status_patch = requests
            .iter()
            .find(|request| request.path.ends_with("/status"))
            .expect("status refusal patch");
        assert_eq!(status_patch.method, "PATCH");
        let status: serde_json::Value = serde_json::from_slice(&status_patch.body).unwrap();
        assert_eq!(status["status"]["conditions"][0]["type"], "Ready");
        assert_eq!(status["status"]["conditions"][0]["status"], "False");
        assert_eq!(
            status["status"]["conditions"][0]["reason"],
            "DrainModeHandoverRequired"
        );
        assert!(requests.iter().all(|request| {
            request.method == "GET" || request.path.ends_with("/siglakeclusters/acme/status")
        }));
    }

    #[tokio::test]
    async fn retained_local_and_mirrored_segments_refuse_unsafe_handover_before_apply() {
        assert_drain_handover_refused(1, 4).await;
        assert_drain_handover_refused(4, 1).await;
    }

    #[test]
    fn invalid_spec_message_paths_resolve_in_crd_schema() {
        let source = include_str!("reconciler.rs");
        let invalid_spec_source = source
            .split_once("fn invalid_spec_condition(")
            .expect("invalid_spec_condition source")
            .1
            .split_once("\nfn invalid_spec_status(")
            .expect("end of invalid_spec_condition source")
            .0;
        let extracted = spec_paths_in_string_literals(invalid_spec_source);
        assert_eq!(
            extracted, INVALID_SPEC_MESSAGE_PATHS,
            "keep INVALID_SPEC_MESSAGE_PATHS in source order and in sync with user-visible paths"
        );

        let crd = SiglakeCluster::crd();
        let schema = crd
            .spec
            .versions
            .iter()
            .find(|version| version.storage)
            .and_then(|version| version.schema.as_ref())
            .and_then(|validation| validation.open_api_v3_schema.as_ref())
            .expect("storage version has an OpenAPI schema");

        let mut paths = Vec::new();
        for template in INVALID_SPEC_MESSAGE_PATHS {
            if template.contains("{component}") {
                paths.extend(
                    ["ingester", "compactor", "query"]
                        .map(|component| template.replace("{component}", component)),
                );
            } else {
                paths.push((*template).to_owned());
            }
        }

        for path in paths {
            let mut node = schema;
            for segment in path.split('.') {
                node = node
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.get(segment))
                    .unwrap_or_else(|| {
                        panic!("InvalidSpec path `{path}` does not resolve at `{segment}`")
                    });
            }
        }
    }

    /// #967: a query range is an ordinary supported range now that the pods
    /// discover each other at runtime. It is accepted with NO condition at all
    /// — not a renamed refusal, not an advisory — while a malformed range
    /// still reports `AutoscalingRangeInvalid`. Against the pre-#967 code the
    /// first assertion fails with `QueryAutoscalingRangeUnsupported`.
    #[test]
    fn a_query_autoscaling_range_is_supported() {
        let mut spec = valid_spec();
        spec.autoscaling.query = ComponentAutoscale {
            min: 2,
            max: 12,
            target: 4.0,
        };
        assert!(invalid_spec_condition(&spec, "now").is_none());

        // Malformed is still malformed.
        spec.autoscaling.query.max = 1;
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "AutoscalingRangeInvalid");
        assert!(condition.message.contains("query.min (2)"));

        spec.autoscaling.query.max = 12;
        spec.autoscaling.query.target = 0.0;
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "AutoscalingTargetNotPositive");
    }

    #[test]
    fn a_multi_replica_query_range_requires_a_shared_job_store() {
        let mut spec = valid_spec();
        spec.autoscaling.query.max = 4;
        assert!(
            invalid_spec_condition(&spec, "now").is_none(),
            "the Postgres catalog is the default shared job store"
        );

        spec.extra_env.push(ExtraEnvVar {
            name: "SIGLAKE_JOBS_POSTGRES_URI".into(),
            value: "  ".into(),
        });
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "QueryJobsStoreRequiredForScaleOut");
        assert!(condition.message.contains("query.max (4)"));

        spec.autoscaling.query.max = 1;
        assert!(
            invalid_spec_condition(&spec, "now").is_none(),
            "an in-memory store remains supported at one pod"
        );

        spec.autoscaling.query.max = 4;
        spec.extra_env.push(ExtraEnvVar {
            name: "SIGLAKE_JOBS_POSTGRES_URI".into(),
            value: "postgres://jobs/shared".into(),
        });
        assert!(
            invalid_spec_condition(&spec, "now").is_none(),
            "the last non-blank override is an explicit shared store"
        );

        spec.extra_env.clear();
        spec.catalog_uri = "sqlite:///var/lib/siglake/catalog.db?mode=rwc".into();
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "QueryJobsStoreRequiredForScaleOut");

        spec.extra_env.push(ExtraEnvVar {
            name: "SIGLAKE_JOBS_POSTGRES_URI".into(),
            value: "postgres://jobs/shared".into(),
        });
        assert!(
            invalid_spec_condition(&spec, "now").is_none(),
            "a non-blank override makes a non-Postgres catalog safe to scale"
        );
    }

    #[test]
    fn compactor_scale_out_requires_an_effective_wal_mirror() {
        let mut spec = valid_spec();
        spec.autoscaling.compactor = ComponentAutoscale {
            min: 1,
            max: 4,
            target: 1.0,
        };
        assert!(
            invalid_spec_condition(&spec, "now").is_none(),
            "the default mirror supports claim mode"
        );

        spec.extra_env.push(ExtraEnvVar {
            name: "SIGLAKE_WAL_MIRROR_PREFIX".into(),
            value: "   ".into(),
        });
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "WalMirrorRequiredForCompactorScaleOut");
        assert!(condition.message.contains("compactor.max (4)"));

        spec.autoscaling.compactor.max = 1;
        assert!(
            invalid_spec_condition(&spec, "now").is_none(),
            "one local compactor preserves the mirror opt-out"
        );

        spec.autoscaling.compactor.max = 4;
        spec.extra_env.push(ExtraEnvVar {
            name: "SIGLAKE_WAL_MIRROR_PREFIX".into(),
            value: "custom-mirror".into(),
        });
        assert!(
            invalid_spec_condition(&spec, "now").is_none(),
            "the final non-blank duplicate restores a shared mirror"
        );

        spec.extra_env.push(ExtraEnvVar {
            name: "SIGLAKE_WAL_MIRROR_PREFIX".into(),
            value: String::new(),
        });
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "WalMirrorRequiredForCompactorScaleOut");
    }

    #[test]
    fn non_positive_autoscaling_target_is_invalid() {
        let mut spec = valid_spec();
        spec.autoscaling.compactor.target = 0.0;
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "AutoscalingTargetNotPositive");
        assert!(condition.message.contains("compactor.target (0)"));
    }

    #[test]
    fn normalized_autoscaling_range_is_invalid() {
        let mut spec = valid_spec();
        spec.autoscaling.ingester.min = -1;
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "AutoscalingRangeInvalid");
        assert!(condition.message.contains("ingester.min (-1)"));

        spec.autoscaling.ingester.min = 3;
        spec.autoscaling.ingester.max = 2;
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "AutoscalingRangeInvalid");
        assert!(condition.message.contains("max (2)"));
    }

    /// #3693: a zero floor is accepted by the range check (0 is non-negative)
    /// but leaves the tier with no way back — the signal that would reactivate
    /// it is published by the pods it just stopped. Refuse it per component,
    /// naming the repair.
    ///
    /// #6011 narrowed this to everything except the compactor, which now has a
    /// reading published by the ingesters; the compactor's own two refusals
    /// are next.
    #[test]
    fn a_zero_autoscaling_floor_is_unsupported_for_every_component() {
        for component in ["ingester", "query"] {
            let mut spec = valid_spec();
            match component {
                "ingester" => spec.autoscaling.ingester.min = 0,
                _ => spec.autoscaling.query.min = 0,
            }
            let condition = invalid_spec_condition(&spec, "now")
                .unwrap_or_else(|| panic!("{component} zero floor must be refused"));
            assert_eq!(condition.reason, "AutoscalingZeroFloorUnsupported");
            assert!(
                condition
                    .message
                    .contains(&format!("spec.autoscaling.{component}.min is 0")),
                "message must name the field to repair: {}",
                condition.message
            );
            assert!(
                condition.message.contains("Set a floor of 1 or more"),
                "message must name the repair: {}",
                condition.message
            );
        }

        // The packaged defaults are unaffected, and a negative floor still
        // reports the range error rather than this one.
        assert!(invalid_spec_condition(&valid_spec(), "now").is_none());
        let mut negative = valid_spec();
        negative.autoscaling.compactor.min = -1;
        assert_eq!(
            invalid_spec_condition(&negative, "now").unwrap().reason,
            "AutoscalingRangeInvalid"
        );
    }

    /// #6011's two narrowed refusals, with distinct reasons, and the one
    /// shape that is now accepted.
    ///
    /// The zero floor is supported only where an independent reading exists:
    /// the catalog-claim drain, whose shared queue the ingesters publish,
    /// smoothed so that a quiet period rather than a single scrape parks the
    /// tier.
    #[test]
    fn a_zero_compactor_floor_is_accepted_only_under_the_claim_drain_with_smoothing() {
        let woken = |min: i32, max: i32, half_life: f64| {
            let mut spec = valid_spec();
            spec.autoscaling.compactor = ComponentAutoscale {
                min,
                max,
                target: spec.autoscaling.compactor.target,
            };
            spec.autoscaling.ewma_half_life_secs = half_life;
            invalid_spec_condition(&spec, "now")
        };

        assert!(
            woken(0, 4, 60.0).is_none(),
            "a claim-drain compactor with smoothing may park"
        );

        let drain = woken(0, 1, 60.0).expect("the filesystem drain cannot read the catalog");
        assert_eq!(drain.reason, "AutoscalingZeroFloorUnsupported");
        assert!(
            drain.message.contains("filesystem drain"),
            "the message names the drain mode: {}",
            drain.message
        );
        assert!(
            drain.message.contains("raise max above 1"),
            "and the repair: {}",
            drain.message
        );

        let unsmoothed = woken(0, 4, 0.0).expect("an unsmoothed zero floor flaps");
        assert_eq!(
            unsmoothed.reason, "AutoscalingZeroFloorNeedsSmoothing",
            "the two refusals are distinguishable by reason, not only by prose"
        );
        assert!(
            unsmoothed
                .message
                .contains("spec.autoscaling.ewmaHalfLifeSecs"),
            "the message names the field to repair: {}",
            unsmoothed.message
        );

        // A positive compactor floor is unaffected by either check, smoothing
        // or not.
        assert!(woken(1, 1, 0.0).is_none());
        assert!(woken(1, 4, 0.0).is_none());
    }

    /// The refusal has to land before any child resource changes, so it covers
    /// the fresh install and the cluster whose compactor is already at zero
    /// replicas with a stale series: neither is touched, the workloads are not
    /// even read, and the cluster waits at `Ready=False` until the floor goes
    /// back up.
    ///
    /// The refused floor here is the filesystem drain's, which #6011 left
    /// refused: that mode never reads the shared queue, so nothing can ask the
    /// tier back however the reading is published.
    #[tokio::test]
    async fn a_zero_floor_reconcile_changes_no_child_resource() {
        let mut cluster = SiglakeCluster::new("acme", valid_spec());
        cluster.metadata.namespace = Some("test".into());
        cluster.metadata.generation = Some(9);
        cluster.spec.autoscaling.compactor.min = 0;
        cluster.spec.autoscaling.compactor.max = 1;
        cluster.spec.autoscaling.ewma_half_life_secs = 60.0;

        let requests = Arc::new(Mutex::new(Vec::<RecordedRequest>::new()));
        let recorded = Arc::clone(&requests);
        let response_cr = cluster.clone();
        let service = service_fn(move |request: http::Request<Body>| {
            let recorded = Arc::clone(&recorded);
            let response_cr = response_cr.clone();
            async move {
                let (parts, body) = request.into_parts();
                let bytes = body.collect_bytes().await.unwrap().to_vec();
                let path = parts.uri.path().to_owned();
                recorded.lock().unwrap().push(RecordedRequest {
                    method: parts.method.to_string(),
                    path: path.clone(),
                    body: bytes,
                });
                let (status, body) = if path.ends_with("/siglakeclusters/acme/status") {
                    (200, serde_json::to_vec(&response_cr).unwrap())
                } else {
                    (500, b"unexpected request".to_vec())
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
            }
        });
        let client = Client::new(service, "test");
        // Port 9 (discard) stands in for an unreachable Prometheus: a stopped
        // compactor's series is gone, and the refusal must not depend on the
        // reading either way.
        let prom = crate::prom::PromClient::new("http://127.0.0.1:9").unwrap();
        let context = Arc::new(Context {
            client,
            prom,
            scaling_state: Default::default(),
            zero_floor_state: Default::default(),
        });

        reconcile(Arc::new(cluster), context)
            .await
            .expect("an unsupported floor is a status refusal, not a reconcile error");

        let requests = requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "the refusal may only patch status: {requests:?}"
        );
        assert_eq!(requests[0].method, "PATCH");
        assert!(requests[0].path.ends_with("/siglakeclusters/acme/status"));
        let status: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(status["status"]["observedGeneration"], 9);
        assert_eq!(status["status"]["conditions"][0]["type"], "Ready");
        assert_eq!(status["status"]["conditions"][0]["status"], "False");
        assert_eq!(
            status["status"]["conditions"][1]["reason"],
            "AutoscalingZeroFloorUnsupported"
        );
    }

    #[test]
    fn empty_image_is_invalid() {
        let mut spec = valid_spec();
        spec.image = "  ".into();
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "ImageRequired");
    }

    #[test]
    fn empty_warehouse_url_is_invalid() {
        let mut spec = valid_spec();
        spec.warehouse_url = String::new();
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "WarehouseUrlRequired");
    }

    #[test]
    fn empty_catalog_uri_is_invalid() {
        let mut spec = valid_spec();
        spec.catalog_uri = String::new();
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "CatalogUriRequired");
    }

    #[test]
    fn invalid_ewma_half_life_is_invalid() {
        let mut spec = valid_spec();
        spec.autoscaling.ewma_half_life_secs = -1.0;
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "AutoscalingEwmaHalfLifeInvalid");
    }

    #[test]
    fn rounded_retention_interval_is_invalid() {
        let mut spec = valid_spec();
        spec.retention.query_audit_rotate_interval_days = Some(14);
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "RetentionIntervalUnsupported");
        assert!(condition
            .message
            .contains("use 1 (daily), 7 (weekly), or 30"));
    }

    #[test]
    fn malformed_compactor_bin_concurrency_is_invalid() {
        let mut spec = valid_spec();
        spec.extra_env.push(crate::crd::ExtraEnvVar {
            name: "SIGLAKE_COMPACTOR_BIN_CONCURRENCY".into(),
            value: "lots".into(),
        });
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "CompactorBinConcurrencyInvalid");
    }

    #[test]
    fn underprovisioned_compactor_concurrency_is_invalid() {
        let mut spec = valid_spec();
        spec.extra_env.push(crate::crd::ExtraEnvVar {
            name: "SIGLAKE_COMPACTOR_BIN_CONCURRENCY".into(),
            value: "4".into(),
        });
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        assert_eq!(condition.reason, "CompactorBinConcurrencyExceedsMemory");
        assert!(condition.message.contains("at least 17408Mi"));
    }

    #[test]
    fn invalid_spec_status_sets_ready_false() {
        let mut spec = valid_spec();
        spec.autoscaling.query.max = 0;
        let condition = invalid_spec_condition(&spec, "now").unwrap();
        let status = invalid_spec_status(7, condition);
        let conditions = status["status"]["conditions"].as_array().unwrap();
        assert_eq!(status["status"]["observedGeneration"], 7);
        assert_eq!(conditions[0]["type"], "Ready");
        assert_eq!(conditions[0]["status"], "False");
        assert_eq!(conditions[0]["reason"], "InvalidSpec");
        assert_eq!(conditions[1]["type"], "InvalidSpec");
    }

    #[test]
    fn undersized_query_memory_is_a_non_blocking_advisory_condition() {
        let mut spec = valid_spec();
        spec.resources.query = Some(TierResources {
            limits: BTreeMap::from([("memory".to_string(), "2Gi".to_string())]),
            ..Default::default()
        });
        let advisory = query_memory_advisory_condition(&spec, "now")
            .expect("undersized query memory condition");
        assert_ne!(advisory.type_, "Ready");
        assert_eq!(advisory.status, "True");
        assert_eq!(advisory.reason, "QueryMemoryBelowDecodeFloor");
        assert!(advisory.message.contains("2048Mi"));

        spec.resources.query = None;
        assert!(query_memory_advisory_condition(&spec, "later").is_none());
    }
}
