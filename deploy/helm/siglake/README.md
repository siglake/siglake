# siglake Helm chart

Installs the siglake data plane (ingester, compactor, and query-server)
backed by a customer-provided RDS Postgres + S3 warehouse.

This chart is intentionally light on managed infrastructure: it
references existing Secrets, an existing OIDC-trusted IAM role, and an
existing S3 bucket. The companion Terraform module in
`deploy/terraform/aws/` provisions all three and prints values that
plug straight into `--values`.

## Prerequisites

- Kubernetes 1.27+ with the AWS EBS and EFS CSI drivers installed.
- A StorageClass for the shared WAL volume. Use `ReadWriteMany` (for
  example, EFS) when ingester and compactor may run on different nodes;
  `ReadWriteOnce` requires them to be co-located on one node.
- An RDS Postgres instance reachable from the cluster.
- A Kubernetes Secret holding Postgres credentials (default name
  `siglake-postgres`, override via `--set postgres.existingSecret=...`).
  Required keys: `host`, `port`, `user`, `password`, `database`.
- An S3 bucket for the Iceberg warehouse.
- An IAM role with read/write on the bucket, trusted by the cluster's
  OIDC provider. The role ARN goes onto the ServiceAccount via
  `serviceAccount.annotations.eks\.amazonaws\.com/role-arn`.

## Install

```bash
helm install siglake ./deploy/helm/siglake \
  --namespace siglake --create-namespace \
  --set image.repository=ghcr.io/siglake/siglake \
  --set image.tag=0.2.0 \
  --set s3.bucket=my-customer-warehouse \
  --set s3.region=us-east-1 \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::123456789012:role/siglake-warehouse-rw \
  --set wal.storageClassName=efs-sc
```

Image tags are numeric, matching the chart's `appVersion`: the release
is tagged `v0.2.0` in git but published as
`ghcr.io/siglake/siglake:0.2.0`. Leaving `image.tag` unset picks the
`appVersion` of the chart you installed, which is the paired image.

For non-trivial deployments, write a `values.yaml` and pass `-f`
instead of stacking `--set` flags. The Terraform module emits a
ready-to-use `values.aws.yaml` snippet.

## Upgrade

```bash
helm upgrade siglake ./deploy/helm/siglake -n siglake -f values.aws.yaml
```

The ingester Deployment and query-server StatefulSet use `RollingUpdate`;
the compactor Deployment uses `Recreate`. With `schemaMigration.enabled`
(the default), the chart runs the `migrate-schema` Job from
`job-migrate-schema.yaml` as a `pre-upgrade` hook and waits for it to
succeed before rolling the workloads. The migration is additive and
idempotent. If a newer binary reaches an older table that lacks a column,
it refuses the affected write and names the migration remedy rather than
silently dropping the column.

## Rollback

```bash
helm rollback siglake <revision> -n siglake
```

The migration Job does **not** re-run. It is a `pre-upgrade` hook, and a
rollback runs only `pre-rollback`/`post-rollback` hooks — the chart
declares neither. So the rolled-back pods meet the table the migration
already widened, which is the supported direction: the older binary
writes the columns it declares and the storage layer fills the rest with
nulls, leaving existing values intact (regression:
`crates/siglake-storage/tests/storage/schema_rollback.rs`). **A rollback
rolls back the image, never the schema** — the columns the migration
added stay, and re-running `migrate-schema` on the old image adds and
removes nothing.

Rolling forward again renders a *fresh* Job: the name carries
`.Release.Revision`, and a rollback increments the revision, so the next
`helm upgrade` never reuses a name (a Job's `spec.template` is immutable,
and reuse would 422 the moment the image tag changed). Hook Jobs are not
part of the release manifest, so a rollback neither deletes nor recreates
the ones already there; `schemaMigration.ttlSecondsAfterFinished`
(default `86400`) reaps them.

A migration that FAILS needs no rollback. Helm blocks on the pre-upgrade
hook, so the release fails before any workload is applied and the previous
revision is still the live one. Read the Job's logs
(`kubectl logs -n siglake -l app.kubernetes.io/component=migrate-schema`)
before deleting it; the next attempt's Job has a different name either
way.

Two limits on the above. It covers *additive* differences only — a
rollback across the pre-0.1.0 nanosecond `timestamp` contract has no path
(`migrate-schema` refuses such tables; see the root README's "Things
deliberately not yet done"). And no rollback has been qualified against an
actual older image: the regression runs one binary against a
widened table, and this section is reasoned from the templates.

## Per-service configuration

Each of the three services has its own values block with `enabled`,
`replicas`, `resources`, `nodeSelector`, `tolerations`, `affinity`,
`extraEnv`, and `extraArgs`. Disable a workload with
`--set <service>.enabled=false`.

Service-specific knobs:

| Service     | Knobs                                                                  |
|-------------|-------------------------------------------------------------------------|
| ingester    | `walSegmentMaxEvents`, `walSegmentMaxAgeSecs`                           |
| compactor   | `intervalSecs`, `binConcurrency`, `committedRetentionSecs`, `mirrorLedgerReclaim` |
| query       | `tokens.existingSecret` or `tokens.list`; `jobs.persistent`; `scan.fileCacheMaxBytes`, `scan.fileCacheMaxEntries` |

### The batch-job store

`query.jobs.persistent` is **on by default**. The query pods store batch-job
state (`priority: "batch"` submissions, their status and their result rows) in
the Postgres instance that already holds the Iceberg catalog — the chart
renders `SIGLAKE_JOBS_POSTGRES_URI` from the same Secret, there is no second
connection to configure, and the query-server creates its own tables on start.

One store for the tier is what makes a `job_id` usable: `query.replicas`
defaults to 2 behind a Service with no session affinity, so a status, result
or cancel request lands on either pod. A restart is then bounded rather than
lossy — an in-flight job is failed once its owner's lease expires
(`SIGLAKE_JOBS_OWNER_LEASE_SECS`, default 120 s), and jobs owned by a sibling
that is still heartbeating are untouched.

`--set query.jobs.persistent=false` drops the variable and gives each pod its
own in-memory store. That is a single-pod configuration: the chart refuses the
render if `query.replicas` or an enabled `keda.query.maxReplicas` exceeds 1,
because job reads routed to another pod would answer `404` for a job that is
running normally. Keep the shared store on to scale the query tier.

### Query source-file cache limits

`query.scan.fileCacheMaxBytes` and `query.scan.fileCacheMaxEntries` bound the
per-query-pod source-file batch cache, which holds whole DECODED files. Both
default to `0`, which is off; both have to be positive to enable it, and a pod
that gets one of the two logs a warning at startup and runs without the cache.

Before enabling it, read what the budget buys. An entry larger than a quarter of
`fileCacheMaxBytes` is never cached, and a compacted file is 256Mi of Parquet at
the scan's x5 decode estimate — about 1.25Gi — so a cache that holds one is 5Gi.
The packaged 4Gi query pod is nowhere near that: at its recommended 512Mi it
holds only pre-compaction files, and those 512Mi come out of the query memory
pool, which at 4Gi is exactly one compacted file's decode reservation. Raise
`query.resources.limits.memory` alongside the cache. The recommendation for a
pod that has the room is an eighth of the memory limit, with one entry per MiB
of it. `docs/DESIGN_source_file_cache_qualification.md` has the measurements.

### Scaling the compactor past one pod

A compactor tier that can hold more than one pod requires
`compactor.catalogClaim.enabled: true`, and the chart refuses to render
without it:

```yaml
compactor:
  replicas: 2
  catalogClaim:
    enabled: true    # wal.mirror.enabled is already the default
```

The claim is the only thing that divides the work. Without it each replica
lists the same sealed segments and runs the same maintenance loop — leveled
rewrites, snapshot expiry, retention and orphan GC — against the same tables,
on optimistic-concurrency commits, so the pods spend their budget losing
commit races to each other. Nothing in the metrics says "misconfigured"; the
layout just stops converging. `autoscaling.compactor.maxReplicas` above `1`
is refused the same way: an HPA reaches that state a few minutes after
install rather than at install.

The claim needs the mirror, and that direction is refused too. In claim mode
the drain reads the `wal_segments` catalog table and never the local
`sealed/` directory, and rows land there only from an ingester that mirrors
its segments. With `wal.mirror.enabled: false` it claims nothing, and
`siglake_compactor_sealed_pending` reads zero while the backlog grows, because
in claim mode that gauge counts the sealed rows in `wal_segments` that no
worker has claimed, and the local `sealed/` directory is not what it looks at.

Two consequences of turning the claim on, both already in the templates: the
compactor mounts an `emptyDir` instead of the WAL PVC (it no longer reads the
ingester's filesystem), and the drain purges the mirror objects it commits,
which is the only thing that bounds the mirror prefix.

A third: `autoscaling.compactor.customMetric.enabled: true` is refused with the
claim on. That gauge is the whole shared queue — every compactor publishes the
same total — while the HPA renders it as a `type: Pods` metric, whose algorithm
averages the reading over the running pods and multiplies the ratio by that
count. A fixed backlog of 8 against a target of 5 then asks for 2 pods from 1
and 4 from 2, climbing to `maxReplicas` on a backlog that never moved. Dividing
the target into the queue once needs one aggregated series behind an `Object`
or `External` metric and the adapter rule that publishes it, which this chart
does not render. Claim-mode HPA scaling is therefore CPU-only here; the
backlog signal is `siglake-operator`'s, which does that division itself. The
metric still renders in filesystem mode, where each pod's sealed count is its
own — but the refusal above holds that mode at `maxReplicas: 1`, so it scales
nothing there either.

### Switching an existing filesystem-drain release to the claim

The upgrade re-renders the compactor onto that `emptyDir` and changes nothing
on the volume. The PVC stays — the ingester still mounts it at
`/var/lib/siglake/wal` — but the compactor stops reading it, so anything the
filesystem drain had set aside there is unattended and unreported from then
on.

Held orphans are the case to settle before you switch. A segment the drain
quarantined under `<wal>/**/orphans/` and could not settle is held, counted by
`siglake_compactor_orphans_held{tenant}` and paged by
`SiglakeCompactorOrphansHeld` (see the alert list below). Both readings come
from the filesystem sweep; the claim drain publishes neither, and the pod that
would census the directory no longer has the volume. An absent series after
the switch is therefore not evidence that the old volume is clear.

So, in order: inventory `<wal>/**/orphans/` on the PVC first — from an
ingester pod, which mounts the same claim before and after — and record what
is there. Keep the files. A held orphan's commit status is UNKNOWN, which is
why it was held and not a finding that its rows are missing: they may already
be in the table or may exist nowhere else, so deleting one can lose rows and
requeueing one can duplicate them. Establish which from your own retention and
ingest history before moving anything. The way back changes too: the claim
drain never reads `sealed/`, so a file moved there afterwards reaches the
table by way of the ingester's mirror catch-up sweep and the claim drain's
mirror sync, not the local rename the filesystem drain used.

`siglake-operator` refuses this handover rather than performing it
(`DrainModeHandoverRequired`, `docs/LIMITATIONS.md`); the chart does not
refuse it, which is why the inventory is yours to do.

### The ingester does not compact

`ingest-server --with-compactor` gives one process an in-process compactor.
It is a single-process shape — the dev quickstart, the bench scripts — and
this chart refuses it: `ingester.extraArgs: [--with-compactor]` fails the
render, whatever the replica count.

The embedded compactor is built without a catalog claim, and the chart renders
no claim arguments on the ingester, so there is no setting that divides the
work between two of them. They coordinate on the drain only by atomic rename
on the shared WAL volume, and on the maintenance loop — leveled rewrites,
snapshot expiry, retention, orphan GC — not at all: the same contention the
compactor tier refuses above, reached through a different value.
`compactor.catalogClaim.enabled: true` does not waive it, because the claim it
turns on is rendered on the compactor Deployment and nowhere else.

`ingester.replicas: 1` does not waive it either. The ingester rolls with
`maxSurge: 1` and `maxUnavailable: 0`, so the outgoing and incoming pod overlap
on every upgrade — long enough for two embedded compactors to run maintenance
against one table. Narrowing the rollout instead would trade ingest
availability for a configuration the chart still could not coordinate.

Compact with `compactor.enabled: true` (the default). What that costs, next to
an embedded compactor, is one more Deployment; what it buys is the claim, the
scale guard above, and a memory limit that is not the ingester's.

### `compactor.committedRetentionSecs`

`committedRetentionSecs` bounds how long successfully drained WAL mirror
objects and their catalog rows remain available. It defaults to `86400` seconds
(24 hours) so the mirror prefix and catalog do not grow monotonically; set it to
`0` to opt out and never purge. The compactor floors a non-zero value at
`MIN_COMMITTED_RETENTION_SECS` (901 seconds): it must outlive the default
600-second `SIGLAKE_WAL_LOCAL_SWEEP_SETTLE_SECS` delay plus one 300-second
`SIGLAKE_WAL_LOCAL_SWEEP_SECS` cadence. That gives an ingester time to observe
the committed catalog row and remove its local sealed copy before the mirror
object and row disappear; otherwise its catch-up sweep can re-upload and
re-register the drained segment. If either local-sweep window is increased,
increase `committedRetentionSecs` beyond their sum as well.
If `SIGLAKE_WAL_LOCAL_SWEEP_SECS=0` disables that sweep, keep committed
retention disabled too unless mirror catch-up is also disabled.

The claim-reclaim age is not part of this floor. A claim remains `processing`
and cannot be purged while it awaits disposition; if reclaim proves it already
committed, the retention clock starts from that later disposition.

### `compactor.mirrorLedgerReclaim`

Retention above is enforced by the catalog-claim drain, which deletes a mirror
object because it is the thing that claimed and committed it. The default
single-replica compactor drains local `<wal>/sealed/` and never reads the
mirror, so it purges nothing: the prefix and the `wal_segments` row the
ingester writes per upload grow for as long as the cluster ingests
(`docs/LIMITATIONS.md` gives the daily figures).

`mirrorLedgerReclaim: true` closes that for the segments this drain commits. The
compactor connects the catalog and the mirror store **without** claiming, marks
the ingester's row `committed` for each segment it committed out of local
`committed/`, and the same retention pass deletes the object and then the row.
It never registers an object it did not commit, so a dropped index
incarnation's quarantined segments, and an ingester whose volume was lost
before its segments drained, are still left to an object-store lifecycle rule.

All three inputs are wired: the chart renders `SIGLAKE_CATALOG_URI` from
`postgres.existingSecret` and `postgres.secretKeys`, and `SIGLAKE_WAREHOUSE_URL`
from `s3.bucket` and `s3.warehousePrefix`, on every component, and
`wal.mirror.prefix` as `SIGLAKE_WAL_MIRROR_PREFIX` on the compactor in both
drain modes. A changed prefix no longer needs a `compactor.extraEnv` entry repeating it
(#5880 — until then it reached the compactor under
`compactor.catalogClaim.enabled` alone, and this drain used the binary default
`wal-mirror`). An entry left over from that workaround still wins, since
`compactor.extraEnv` is rendered last. With an empty effective prefix —
`wal.mirror.enabled: false` renders the variable empty — or a missing
connection, the compactor warns and keeps draining.

Off by default: it deletes objects, and it gives a drain that needs no
claim-store connection today a dependency on one. Watch
`siglake_compactor_mirror_unreclaimed_total` — a locally-committed segment
whose mark never became durable has its local copy swept at the 3600-second
ceiling to keep the WAL volume bounded, and its object is then beyond this
drain's reach.

### Consumed-proof rolling upgrades

Versions that write `siglake.consumed_proof.v1` can reclaim committed claims
after their source snapshots expire. Older writers update only the legacy
snapshot summary, so a mixed-version rollout needs a temporary retention
guard. Before starting the first new binary, set:

```yaml
compactor:
  snapshotExpire:
    retainLast: 400
```

Keep `retainLast >= 400` throughout the rollout and for at least 1,025 seconds
after the last old drain or maintenance writer exits. The new reader consults
both sources during that interval. After the wait, `retainLast` may return to
the metadata/time-travel value appropriate for the deployment (the packaged
default is 100); no data-file rewrite is required.

### `compactor.binConcurrency` and memory

Bins in a leveled compaction pass are file-disjoint and independent, so they can
merge concurrently. **Memory, not CPU, is the binding constraint**: each in-flight
bin holds its own decoded working set (roughly `SIGLAKE_MERGE_CHUNK_ROWS` x
`SIGLAKE_MERGE_CHUNK_PREFETCH` rows, plus its open row-group decoders).

**Measured: `binConcurrency: 4` peaked at 12.03 GiB (200G) and 14.44 GiB (1TB)
of compaction-only RSS — roughly 3-3.4 GiB per bin, GROWING with corpus size.** An earlier ~650Mi/bin figure came
from a local bench with much smaller bins and was 4x too low; treat 3 GB/bin as a
floor, since 1TB-class bins are larger.

The packaged `compactor.resources` (1Gi limit, 2 CPU) supports
`binConcurrency: 1` and nothing more. **Raise `resources.limits.memory` and
`resources.limits.cpu` together with this knob** — under-provisioned, the
compactor OOMKills instead of compacting faster, and compaction silently stops.
The chart refuses to render `binConcurrency > 1` while the memory limit is still
the packaged default; the operator (which renders the same defaults) logs a
warning when `spec.extraEnv` raises it past what the limit can hold.

Rough guide: budget `1Gi + 4Gi x binConcurrency` and at least one CPU per
bin. Measured 3.72x of 4 (93% efficient) on a pass with bins to spread across.

## Autoscaling

`keda.enabled=true` renders KEDA `ScaledObject`s for the ingester and the
query tier, scaling on saturation signals (`keda.ingester.*`,
`keda.query.*`) rather than CPU. The KEDA operator must be installed first,
because its CRDs have to exist. The `autoscaling.*` blocks are the legacy
CPU-HPA fallback for the ingester and compactor only; the compactor's ceiling
is bound to the catalog claim (see "Scaling the compactor past one pod"). The legacy query CPU HPA
was removed because the peer list of the day capped useful shard workers at
`query.replicas`; use `keda.query.*` for query-tier autoscaling.

### The query tier scales past `query.replicas`

The chart used to refuse `keda.query.maxReplicas > query.replicas`, because
`--query-peers` was built from `query.replicas` **at render time** and a pod
KEDA added beyond that count coordinated but received no shard work. Since
#967 the pods discover each other instead: each resolves
`_http._tcp.<release>-query-headless.<namespace>.svc.cluster.local`, the
headless Service's SRV record, and publishes the Ready endpoints as its
membership. `query.replicas` is now only the starting count (and the fixed
count when KEDA is off); `keda.query.minReplicas`/`maxReplicas` own the range:

```yaml
query:
  replicas: 2
keda:
  enabled: true
  query:
    minReplicas: 2
    maxReplicas: 8
```

Two convergence properties to expect. A pod becomes eligible one readiness
probe plus one DNS refresh (`SIGLAKE_QUERY_PEER_DISCOVERY_INTERVAL_SECS`,
default 5) after it starts. And a query already running keeps the membership
it captured, so scale-out shows up on the *next* query, not the one in
flight — which is also why a pod that joins or leaves mid-query cannot change
that query's answer.

Scaling *down* is safe but not free: a shard whose peer has left fails over,
with its original shard index and snapshot pin, to the coordinator's own
runner, and counts on `siglake_query_coordinator_failover_total`. Before its
first usable SRV answer a pod answers every query single-pod — correct, merely
not distributed; a pod stuck there is
`SiglakeQueryPeerDiscoveryStalled`, from
`siglake_query_peer_discovery_refresh_total{outcome=~"error|empty|unmatched"}`.
With `query.distributed.enabled: false` no discovery is rendered at all, every
pod runs single-pod, and extra replicas are plain replication.

Non-Kubernetes deployments keep the static `--query-peers` list
(`SIGLAKE_QUERY_PEERS`), whose contract is that the coordinator is peer zero.
Setting both it and discovery is refused at startup rather than resolved by a
precedence rule.

## Auth

The query-server takes a static bearer-token allow-list via
`SIGLAKE_QUERY_TOKENS`. Three ways to wire it:

1. **Customer-managed Secret** (recommended for SOC2 setups):
   ```yaml
   query:
     tokens:
       existingSecret: siglake-query-tokens   # has key `tokens` = "abc,def"
   ```

2. **Chart-managed Secret** (good for dev):
   ```yaml
   query:
     tokens:
       list:
         - dev-token-1
         - dev-token-2
   ```

3. **External Secrets Operator**, pulling the allow-list from your secret
   backend:
   ```yaml
   externalSecrets:
     enabled: true
     secretStore:
       name: siglake
       kind: ClusterSecretStore
     queryTokens:
       remoteKey: siglake/query-tokens
   ```
   The chart renders an `ExternalSecret` whose target is
   `<release>-siglake-query-tokens` with key `query.tokens.secretKey`, and the
   query pods read `SIGLAKE_QUERY_TOKENS` from it. Leave
   `query.tokens.existingSecret` and `query.tokens.list` empty on this path:
   `existingSecret` renames the ExternalSecret's target to the Secret you
   already own. The chart refuses `query.tokens.list` together with
   `externalSecrets.queryTokens.remoteKey` when `existingSecret` is empty,
   because both sources would manage the same Secret name.

OIDC bearer-token verification uses `query.oidc.issuer`,
`query.oidc.audience`, and `query.oidc.tenantClaim`. When issuer and audience
are non-empty, OIDC takes precedence over `query.tokens`.

An OIDC block has to be complete. On each enabled tier — `query.oidc` and
`ingester.oidc` alike — the chart accepts three shapes and refuses the rest:

| `issuer` | `audience` | `tenantClaim` | |
|---|---|---|---|
| empty | empty | empty | no OIDC; `tokens`/`auth`, or open |
| set | set | empty | verified JWTs, single-tenant routing |
| set | set | set | verified JWTs, tenant from the claim |

An issuer without an audience, an audience without an issuer, or a
`tenantClaim` without both fails the render. The variables were emitted only as
a complete set, so an incomplete block used to disappear during rendering: the
tier came up on its token allow-list, or open, and the omission also skipped
the binaries' own startup checks, which see only variables that were rendered.
Bearer tokens beside an incomplete block do not make it valid — they say who
may call, not that the JWT the values asked for was verified.

A complete `query.oidc` block is authentication, so it needs
`query.distributed.coordinatorToken` wherever fan-out is reachable, exactly as
a token allow-list does.

`query.allowedTenants` is an optional exact allow-list for verified query
tenant claims. Empty keeps query admission unrestricted. A non-empty list
requires the complete `query.oidc` block including `tenantClaim`; the chart
refuses an allow-list that has no claim routing to match. The setting is
independent of `ingester.allowedTenants`, so removing a tenant from write
admission does not remove access to retained data. Each worker checks its own
list before opening the forwarded tenant namespace, and a mismatched worker's
`403` is returned through the coordinator.

## Ingress

ClusterIP only by default. Customers typically expose the query-server
via an Ingress they already own:

```yaml
ingress:
  enabled: true
  className: alb
  annotations:
    alb.ingress.kubernetes.io/scheme: internal
    alb.ingress.kubernetes.io/listen-ports: '[{"HTTPS":443}]'
    alb.ingress.kubernetes.io/certificate-arn: arn:aws:acm:...
  hosts:
    - host: siglake.example.com
      paths:
        - path: /api
          pathType: Prefix
          service: query
  tls:
    - hosts: [siglake.example.com]
```

OTLP ingest usually sits behind a separate Ingress with a different
auth model — point it at the `<release>-siglake-ingester` Service on
port 8088 for OTLP/HTTP (`POST /v1/logs`, `POST /v1/traces`) or 4317 for
OTLP/gRPC logs and traces. Both are enabled by default. Set
`ingester.otlpGrpc.enabled=false` to remove the gRPC container and Service
ports and pass the binary's explicit `--disable-otlp-grpc` opt-out.

## Ingest tenancy

The ingester is **single-tenant by default**: every request routes to the
`default` tenant, and an `X-Scope-OrgID` naming any other one is refused with
`403` on both HTTP and OTLP/gRPC. Two values turn multi-tenant routing on:

- `ingester.oidc.tenantClaim` — the tenant comes from the verified JWT, on
  both transports. A header may only agree with the claim, and a token that
  carries no usable claim is refused rather than routed to `default`. This is
  the setting for a shared cluster. It needs `ingester.oidc.issuer` and
  `ingester.oidc.audience` with it: there is no verified token to read a claim
  from without them, and the chart refuses the claim on its own.
- `ingester.trustScopeHeader: true` — the header selects the tenant, on the
  client's word. Only safe where a gateway in front of the ingester sets the
  header itself and strips the client's.

`ingester.allowedTenants` bounds whichever of the two is in use: it is checked
against the tenant actually resolved.

## Observability

Set `serviceMonitor.enabled=true` if you run `kube-prometheus-stack`.
Each pod's `/metrics` endpoint is scraped at `serviceMonitor.interval`.
Exactly one Service per tier carries `siglake.limnion.ai/scrape: "true"`
and the ServiceMonitor requires it — without that, the two Services the
query tier renders would each become a scrape target and every query
metric would read double.

`serviceMonitor.targetLabels` copies `app.kubernetes.io/instance` and
`app.kubernetes.io/component` onto the series, which the operator's
autoscaling PromQL selects on.

Set `prometheusRule.enabled=true` for alerts on the failure modes
siglake has hit or pins with a deterministic loss regression: silent-loss counters (abandoned mirror
registrations, CRC mismatches, refused writes, lost group-count
deltas), a stalled drain, a non-converging layout, query-pool
saturation, sustained pool refusals (`SiglakeQueryPoolRefusing`), incomplete
scan attribution (`SiglakeQueryScanAttributionIncomplete`), sustained shard
pin failures (`SiglakeQueryShardPinUnresolved`), table-metadata cache
reloads that keep being fenced out unpublished
(`SiglakeTableCacheUnpublished` — the `superseded` and `reload` fences are
healthy contention and do not alert; the counter has no `table` label, so the
alert points at the pod's WARN line `table-cache refresh fenced out`, which
carries `table=`, and it is above the default `logLevel`), batch execution
prevented or completed output discarded after recovery
(`SiglakeBatchCompletionRejectedByRecovery`; client cancellations and TTL
expiry do not alert), and a stalled
query-cache
warmer. That last one is the
earliest known signal for the query degradation that only a pod
restart cures; it fires once no warm cycle has completed for three
intervals. The interval is `query.warmIntervalSecs` (default 30), which
the chart renders as `SIGLAKE_QUERY_WARM_INTERVAL_SECS` on every query
pod, so changing the cadence moves the alert threshold with it. Set
`prometheusRule.queryWarmIntervalSecs` only when the pods' cadence comes
from somewhere else (a `query.extraEnv` entry wins over the rendered
value); it overrides what the alert assumes, nothing more. A cadence of
0 means a startup-only warm, and the alert is then not rendered.
`scripts/check-chart.py` renders the chart at the default, a non-default
and a zero cadence and holds the alert's `for:` to three times the
container's value (or to the alert's absence at 0), so the two cannot
drift apart unnoticed.

Five of the silent-loss alerts are about the aggregates rather than rows. A per-commit delta write that exhausts its
four attempts leaves a durable marker; the maintenance compactor normally
rebuilds the aggregate on its next fold, while `GROUP BY` stays exact on the
per-file path. `SiglakeGroupCountDeltaLost` (warning) fires only when that
automatic rebuild fails or remains incomplete and names the Iceberg
namespace and table; use `siglake rebuild-group-counts --namespace <ns> --table
<table>` as the operator fallback.
`SiglakeGroupCountDeltaRetrying` (warning) fires once delta writes for a table
have needed retries for half an hour and names the Iceberg namespace, table and
pod, warning that an exhausted write and automatic rebuild are becoming more
likely.
`SiglakeSideAggregatePublicationLost` (warning) covers the inline aggregate
object: a publication that exhausts the same four attempts loses the commit's
contribution outright, so it fires on the failure itself. The compactor's
rebuild restores the wide group counts; the inline time aggregates are restored
by `siglake rebuild-time-aggregates --table <table>`, and until one of those
runs, windowed `GROUP BY` on that table answers from the per-file path.
`SiglakeInlineCoverageUnproven` (critical) is the state, rather than the event
that produced it. Every 15 minutes the maintenance pass reads each maintained
table's inline aggregate object and asks the read guard's own question: does its
coverage edge reach the current snapshot? A delete task, retention, a foreign
overwrite and the two residual windows at snapshot expiry all leave an object
where the answer is no, and no commit republishes a chain the reader cannot
walk — so the table serves windowed `GROUP BY`, date histograms and windowed
counts from the exact per-file tiers for the rest of its life. Answers stay
exact; the alert is critical because the state is permanent and the repair is
manual: `siglake rebuild-time-aggregates --namespace <ns> --table <table>`,
which the alert renders with both labels filled in. It reads the current-state
gauge `siglake_inline_coverage_unproven{iceberg_namespace,table}` — set to 0 or
1 for every table on every pass, so a repaired table clears at the next census —
paired with `increase(siglake_inline_coverage_census_total[1h]) > 0` on the same
pod, so a compactor that has stopped censusing drops out of the alert instead of
paging from a reading nobody is refreshing. There is no values key for it and
nothing to opt into, because the pass only reads: a
`compactor.extraEnv` entry setting `SIGLAKE_INLINE_COVERAGE_SCAN_INTERVAL_SECS`
changes the cadence, and `off` switches the census off.
`SiglakeGroupCountAggregateShort` (warning) is the one that needs no lost write
at all: every 15 minutes the maintenance pass censuses each maintained table
and fires this when one is short of `total-records` with every commit's
contribution present. A process killed between its commit and its delta PUT
writes neither delta nor marker, and a table upgraded across the per-incarnation
aggregate prefix starts a fresh aggregate at its first commit after the
upgrade; both leave a shortfall no later commit closes. Repairing it
automatically is opt-in (`compactor.shortAggregateRepair`, which renders
`SIGLAKE_AGG_SHORT_REPAIR=1`) because it costs one Tier-2 query per maintained
column; with it off the alert and the compactor's WARN line — which names the
columns — point at
`siglake rebuild-group-counts --namespace <ns> --table <table>`.

`SiglakeCompactorOrphansHeld` (critical) is the one alert whose remedy is a
person rather than a command. A compactor killed mid-commit leaves its claimed
segment quarantined under `<wal>/orphans/`, and the next drain cycle settles it
against the table's cumulative consumed-segment set: named there, the rows are
provably committed and the file is deleted; absent from it with the retained
history covering the segment's whole life, the rows are provably uncommitted and
the file goes back into `sealed/` to be re-committed. Both resolve themselves.
The third case does not: the name is absent *and* snapshot expiry may already
have dropped the snapshot that carried the proof, so commit status cannot be
established from the warehouse, and the compactor holds the file instead of
guessing. It reports the level as
`siglake_compactor_orphans_held{tenant}` — the tenant's whole WAL layout, its
events directory and every managed index summed, republished every cycle so a
settled hold clears the page without a restart — and the alert fires after 15
minutes above zero. **Preserve the files.** Their rows may already be in the
table or may exist nowhere else, so deleting one can lose rows and requeueing
one can duplicate them; establish which from your own evidence (the compactor's
`orphan auto-disposition` INFO line names the directory and table, and the
segment is a readable Arrow stream) before moving anything. Raising
`compactor.snapshotExpire.retainLast` keeps the proof available for orphans a
future crash creates; it cannot restore history that has already expired and
will not clear an existing hold. The level is charted per tenant on the
"Segments quarantined" panel of the starter dashboard, beside the two other
set-aside series, so the page's first two questions — one tenant or the fleet,
steady or growing — are answered without an ad-hoc query. Only the filesystem
drain publishes it: under the catalog claim the series is absent, which says
nothing about what a former WAL PVC still holds — "Switching an existing
filesystem-drain release to the claim" above is what to do about that before
the switch.

The starter Grafana dashboard `deploy/grafana/siglake-overview.json`
groups panels the same way and filters on `namespace` (the label
Prometheus Operator sets on every target). It does *not* need a `role`
label — an earlier version of this README said it did, and no
ServiceMonitor has ever set one. Its "Fast paths" row is the
query tier's cheap-answer machinery: side-aggregate cache outcomes
(`hit` / `miss` / `stale` / `uncovered`), Tier-2 group-count calls by
outcome with the fallback share on the right axis (near 0 is healthy,
near 1 is every call decoding raw pages), Tier-2 files per call, the
live-file-list cache hit ratio, and a second line for the Tier-1
aggregate itself: outstanding deltas folded per read (p50 / p99, near
zero when the compactor fold keeps up) and columns demoted from exact
counts to sketches (should sit at zero). The row ends with the
text-index panels: a text query's per-file startup split by stage
(`permit_wait` / `blob_fetch` / `decode` / `selection` — which one moved
says whether a slowdown is the load queue, object storage, the decode or
the postings work), the parsed-index cache's lookup outcomes beside the
bound that dropped an entry, and its resident bytes against that bound.

Four panels close that row with what the indexes those queries read cost
to build, which was emitted but charted nowhere. The first two
are the segmented (`seg2`) sidecar writer, which builds a sidecar as a
compaction rewrite emits row groups: one arm per `(outcome, reason)` a
sidecar close records, and the sidecar's encoded bytes beside the heap
one row group's postings and dictionary occupy while they are built.
Read the refusal arms first — `column`, `file_rows` and `row_domain` are
the three ways a sidecar would have described a layout the output file
does not have, and each one leaves that file on the scan path with no
index. All four arms are created at 0 on every compactor, so a release
that has not set `SIGLAKE_SEGMENTED_INDEX_WRITES=1` (the default) charts
flat zeros rather than "No data". The byte panel cannot be: quantiles
have nothing to pre-register, and it is charted **per pod** rather than
summed — these are each compactor's own summary quantiles over the
exporter's rolling window, and averaging percentiles across a fleet
produces a number no pod measured. Read the `group index` arm for what
it is: one row group's parsed-index allocation, dropped at the end of
that group, not the writer's peak process heap. The other two panels are
the opt-in post-rewrite v1 rebuild
(`compactor.indexRebuild`), which reads a committed data file back whole:
the files it rebuilt per hour with those files' own size on the right
axis, and the pass duration as a fleet histogram quantile. The rebuild
counters carry the tenant and table, known only at the increment, so
they are absent rather than zero until a rebuild commits.

`scripts/check-chart.py` verifies
that every `siglake_*` series a panel or template variable names is one
the code emits — `crates/` and the owned forks under `third_party/`,
which is where the text-index and object-store read families live — the
same check it applies to the alert rules and the KEDA
trigger queries, so a renamed metric fails CI instead of blanking a panel.
It evaluates selected panel arithmetic with promtool rather than only the
metric names: the drain backlog has to read one queue depth per namespace under
two different drain shapes; the text-index startup quantiles have to read one
number per stage out of a fleet's buckets; and the decoded-file cache's
contended-insert fraction has to keep one line per query pod, including when an
outcome is missing or the pod has no insert activity.
It also holds each reference to the form the exporter renders: `_bucket`
and `histogram_quantile()` only on the histograms `builder()` in
`crates/siglake-core/src/metrics.rs` hands buckets (names ending in
`_seconds` and the `COUNT_HISTOGRAMS` list), a `quantile` label only on
the ones it leaves as summaries. The rule is parsed from that file, so a
new `_bucket` panel on a summary-form histogram fails until the histogram
is bucketed there, and nothing in the script needs updating when it is.

## Storage

- `wal/`: ReadWriteMany PVC, shared by ingester (writer) + compactor
  (reader, archiver), plus any external segment consumer. Defaults to
  50Gi.

For EFS, set `wal.storageClassName` to the name of your EFS
StorageClass. The chart never recreates the PVC implicitly — if you
need to resize, follow the standard k8s PVC expand procedure.

## Uninstall

```bash
helm uninstall siglake -n siglake
kubectl delete pvc -n siglake -l app.kubernetes.io/instance=siglake
```

PVCs are deliberately not deleted by `helm uninstall` so a reinstall
keeps the WAL. Delete them explicitly when you mean it.
