# siglake-operator (Helm)

Installs the `siglake-operator` controller plus its
`SiglakeCluster` CRD and RBAC.

## Install

```bash
helm install siglake-op \
  --namespace siglake-system --create-namespace \
  deploy/helm/siglake-operator \
  --set-string prometheus.url=http://prometheus-server.monitoring.svc.cluster.local:80
```

`prometheus.url` is the address the reconciler reads its load signals from,
and it is the chart's default above: the `prometheus-server` Service the
prometheus-community/prometheus chart installs, on port 80. Under
kube-prometheus-stack the Service is named after the release and listens on
9090, so an install there passes its own address:

```bash
  --set-string prometheus.url=http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090
```

A URL that resolves to nothing is not fatal and not loud: the reconciler holds
every replica count at its current size and logs `prometheus query failed;
HOLDING` once per cycle while incrementing
`siglake_operator_prom_query_errors_total`. Read that counter to tell a wrong
address from a quiet cluster.

The chart's `crds/siglakecluster.yaml` is applied by Helm *before*
the rest of the manifests, so the operator's Deployment never
starts without its CRD present.

After install, create a `SiglakeCluster` instance — see
[`deploy/operator/sample-cluster.yaml`](../../operator/sample-cluster.yaml).

## Schema migration, upgrade and image revert

`spec.schemaVersion` is a **trigger counter, not a target**: bumping it
makes the operator render a one-shot `migrate-schema --all-tables
--all-namespaces` Job, and that Job migrates the warehouse to whatever
schema the binary in `spec.image` declares. Unset (or `0`) renders no Job.

**Job identity.** The name is
`<cluster>-migrate-schema-v<N>-<digest>`, where the digest covers
everything that shapes the pod template — `spec.image`,
`spec.warehouseUrl`, `spec.catalogUri`, `spec.awsRegion` and
`spec.extraEnv`. A Job's `spec.template` is immutable, so a name keyed on
`N` alone would 422 on an image bump (freezing status behind the
already-rolling workloads), and it would make the migration unreachable
at `N`: bumping the image after a no-op run at `N` could never re-run it.

**Ordering and gating.** The operator applies the Job, reads its status,
and only then rolls the tiers. While the Job is running or has exhausted
its retries it **holds the rollout** — requeue every 30s, one
`siglake_operator_rollout_held_total` increment, a `SchemaMigrated`
condition carrying `JobRunning`/`JobFailed`. The hold applies to upgrades
only (a cluster whose `status.schemaVersion` is already set); a fresh
install has no table to widen and is never blocked.
`status.schemaVersion` is set **only** from a Job observed to completion —
absent means "not observed", never "up to date".

**Reverting the image.** Put the old value back in `spec.image` and leave
`spec.schemaVersion` where it is. Because the digest covers the image, the
revert renders a *new* Job name, so the operator applies and waits on one
more migration Job — this one running the OLD binary. On an already-widened
table that run adds nothing and exits 0 (the additive diff is by name), so
the hold clears and the tiers roll back to the old image. The migration is
**not** reversed: the columns stay, and the reverted binary writes them as
nulls while existing values keep their contents (regression:
`crates/siglake-storage/tests/storage/schema_rollback.rs`). The table's
`siglake.schema_version.v1` stays where the wider binary left it: the
migration stamps the higher of the recorded and declared versions, so the
older binary's Job leaves it alone. An image built before that fix
(anything pre-0.1.0) does stamp its own lower constant — a reporting
artefact that changes no column and no write decision, and rolling forward
restamps it.

Do **not** lower `spec.schemaVersion` to match. It is a trigger, so a lower
value renders yet another Job and leaves `status.schemaVersion` reporting a
version the warehouse never returned to. To ask the warehouse itself, run
`siglake migrate-schema --dry-run --all-namespaces`, which prints each
table's recorded version.

This covers *additive* schema differences only. There is no revert path
across the pre-0.1.0 nanosecond `timestamp` contract (`migrate-schema`
refuses those tables), and no revert has been qualified against an actual
older image — the regression runs one binary against a widened table, and
this section is reasoned from `render.rs` and `reconciler.rs`.

## RBAC

- `siglakeclusters` + `siglakeclusters/status`: full
- `apps/deployments` + `deployments/scale`, `apps/statefulsets` +
  `statefulsets/scale`: full (the operator owns the lifecycle of all
  three data-plane tiers)
- `services`, `configmaps`: full (headless + ClusterIP Services;
  ConfigMap access is retained in RBAC)
- `persistentvolumeclaims`: full (the shared WAL PVC)
- `batch/cronjobs`, `batch/jobs` + `jobs/status`: full (retention
  sweeps and the one-shot schema-migration Job)
- `events`: create + patch (reconcile traces)
- `coordination.k8s.io/leases`: full (leader election)

**No Secrets access.** The ClusterRole deliberately grants none — the
operator never reads a Secret, it only references one by name in a
container's `valueFrom`, which the kubelet resolves. An earlier version
of this list claimed "`secrets`: read-only (HEC token Secret lookups)";
that was never true of the shipped ClusterRole, and HEC itself no longer
exists.

Operator runs as a non-root, read-only-rootfs ServiceAccount.

## Resources

Every tier renders the chart's packaged `requests` and `limits`
(`deploy/helm/siglake/values.yaml`: `ingester.resources`,
`compactor.resources`, `query.resources`). Today that is a 256Mi/200m
request and a 2-CPU limit on each tier, with a 1Gi memory limit for the
ingester and the compactor and 4Gi for the query tier. The two sides are
one decision written twice; a change to the chart's numbers is a change
to `render.rs` too.

`spec.resources.<tier>` overrides them key by key, so naming one quantity
keeps the rest (the same way `--set query.resources.limits.memory=8Gi`
behaves against the chart):

```yaml
spec:
  resources:
    query:
      limits:
        memory: 8Gi      # cpu limit and both requests stay packaged
    compactor:
      limits:
        memory: 17Gi     # what SIGLAKE_COMPACTOR_BIN_CONCURRENCY=4 needs
```

The query memory limit is the one that matters. The query server derives
its read caches and its memory pool from the container's limit, so a
smaller pod runs every query against a smaller pool (it degrades in
speed, not availability); the sizing note above
`query.resources.limits.memory` in the chart's values.yaml applies
unchanged. The compactor logs a warning when `spec.extraEnv` raises
`SIGLAKE_COMPACTOR_BIN_CONCURRENCY` past what its effective memory limit
can hold.

## Adopting a Helm release

```bash
siglake-operator --adopt-values values.yaml \
  --adopt-cluster-name <release> --adopt-catalog-uri postgres://...
```

synthesizes a `SiglakeCluster` from chart values and prints the handover
runbook (design: `docs/DESIGN_operator_adoption.md`; experimental, no
live test coverage). Feed it the release's *effective* values
(`helm get values <release> -n <ns> --all -o yaml`), not only the
override file: `<tier>.resources`, `<tier>.extraEnv`, `wal.*`,
`ingester.auth.existingSecret` (with its `secretKey`) and the replica
counts are carried onto the CR. A `resources` block the file omits adopts
onto the operator's packaged defaults, which are the chart's, so the first
reconcile stays a no-op apply.

`ingester.auth.existingSecret` is the only authentication setting the CR
can express, as `spec.authTokensSecretRef`. The preflight reports each of
the others as a finding naming the tier, what stops holding at cutover and
how to keep it:

| chart setting | what the adopted pod loses |
| --- | --- |
| `query.tokens.existingSecret` / `.list` | the query tier starts with auth **open** and answers `/api/v1/sql` for anything that reaches the Service |
| `query.oidc` (issuer + audience) | the same, plus `tenantClaim` routing: every query reads the default namespace instead of `tenant_<claim>` |
| `ingester.oidc` (issuer + audience) | ingest verifies nothing beyond `spec.authTokensSecretRef`, and with `tenantClaim` set, writes land in `default` and an `X-Scope-OrgID` naming another tenant is refused with 403 |
| `ingester.auth.list` | the chart-rendered `<release>-auth-tokens` Secret is left unowned and unreferenced, so ingest comes back unauthenticated |
| `ingester.trustScopeHeader: true` | the ingester turns single-tenant and refuses every `X-Scope-OrgID` but `default` |
| `ingester.allowedTenants`, `.maxTenants`, `.maxLanes` | the admission bounds on a client-supplied key are gone |

The issuers, claims and caps are not credentials, so `spec.extraEnv`
carries them (`SIGLAKE_OIDC_*`, `SIGLAKE_TRUST_SCOPE_HEADER`,
`SIGLAKE_ALLOWED_TENANTS`, `SIGLAKE_MAX_TENANTS`,
`SIGLAKE_INGEST_MAX_LANES`) — note that it folds cluster-wide, and both
binaries read the same `SIGLAKE_OIDC_*` names, so one tier's block turns
OIDC on for the other. Bearer tokens have no such route: `spec.extraEnv`
is plaintext in the CR, so a release that authenticates its query tier
stays on the chart.

## Watch namespaces

`watchNamespaces: []` (default) means cluster-wide. Multi-tenant
isolation: deploy one operator per namespace and list only that
namespace. Autoscaling queries select the reconciled cluster's namespace via
the `namespace` scrape label that Prometheus Operator attaches to the target.

## Prometheus / ServiceMonitor

The operator always serves `/metrics` on `:9190`. The chart's
`metricsService` exposes a `ClusterIP` Service (enabled by default).
If your cluster runs Prometheus Operator and you want the operator
scraped automatically, flip `serviceMonitor.enabled=true`:

```yaml
serviceMonitor:
  enabled: true
  interval: 30s
  scrapeTimeout: 10s
  labels:
    release: kube-prometheus-stack   # only if your Prometheus selects
                                     # ServiceMonitors by label
```

The ServiceMonitor is rendered only when *both*
`metricsService.enabled` *and* `serviceMonitor.enabled` are true, so
clusters without `monitoring.coreos.com/v1` installed can leave the
flag off and the chart still installs cleanly.
