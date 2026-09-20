# deploy/kind — fast inner-dev loop

A single-node kind cluster running:

- **postgres** for the Iceberg catalog
- **minio** for the S3 warehouse
- the **siglake** Helm chart (ingester, compactor, and query-server)

Unlike `deploy/aws/`, this does *not* exercise IRSA, EFS, or RDS — it
exists for chart development and quick functional smoke testing
against an in-cluster stack. EKS via `deploy/aws/up.sh` is still the
production smoke target.

## Quick start

```bash
scripts/kind-up.sh         # kind create cluster + apply manifests + helm install
scripts/kind-smoke.sh      # POST events → poll query-server → assert
scripts/kind-round.sh      # Prometheus + KEDA load/evidence round (~6 minutes of load,
                           # plus up to 5 more for the 2→4→2 query scale step)
KIND_ROUND_EVENTS=100 scripts/kind-round.sh       # set the initial load batch
INGESTER_POD_LABEL_CAPTURE=1 scripts/kind-round.sh  # scale the ingester and retain
                                                    # per-pod label evidence
COMPACTOR_POD_LABEL_CAPTURE=1 scripts/kind-round.sh # install two claim compactors and
                                                    # retain shared-queue evidence
POSTGRES_OUTAGE_PROBE=1 scripts/kind-round.sh  # also measure persistent-job
                                              # backlog through a bounded PG pause
scripts/kind-down.sh       # helm uninstall + kind delete cluster
```

## Monitoring evidence round

`kind-round.sh` is the non-interactive runner used by the manager's
`kind_round` playbook. It installs pinned kube-prometheus-stack and KEDA
charts, upgrades Siglake with its ServiceMonitors, PrometheusRules and
ScaledObjects enabled, and installs the query tier with KEDA headroom
(`minReplicaCount: 2`, `maxReplicaCount: 4`) and short anti-flap windows so a
scale event fits inside the round. It then runs every configured Siglake
benchmark SQL shape, checks that a transparent `GROUP BY` merges the shards
without losing rows, and prints:

- every pre-registered ingester and compactor alerted counter at zero from a
  pre-load pod scrape;
- the series count and a sample for dashboard panels 103, 118, 124, 131,
  134 B and 141;
- `kubectl get scaledobject -o wide`, current KEDA conditions, and the live
  p95 query-pool queue-wait trigger value.

While the workload mix runs, the round drives the query tier 2 → 4 → 2 by
raising and lowering the ScaledObject's `minReplicaCount` (#1838, the gate
#968 waits on) and samples it before, during and after each transition: the
row count every ready pod agrees on, the cross-shard `GROUP BY` sum against
that count, the membership each pod published, and each pod's pinned-shard
work across the sample query. `SCALE_SAMPLE`, `SCALE_POD` and
`SCALE_TRANSITION` lines carry it in the log; `results/scale-2-4-2.json` and
`results/membership.log` carry it as files (the AWS runner copies `results/`
back). The step is bounded — it may extend the load window by at most
`SCALE_GRACE_SECONDS` (300) — and any failure in it is deferred like a panel
failure, so the rest of the round's evidence is still printed before the
non-zero exit.

That step only means something if the tier is installed with room above the
floor it drives: with `keda.query.maxReplicas` back at the base, KEDA clamps
the tier, the round times out on the grace period and the report blames the
transition rather than the range — which is how #968 spent six rounds
unverifiable. `scripts/check-kind-round-scale.sh` (in CI's `shell` job and the
local gate) reads the round script statically and requires
`QUERY_SCALE_TARGET > QUERY_SCALE_BASE`, both KEDA bounds and
`query.replicas` to be installed from those constants, and the evidence file
name to be derived from them and named by this document. It runs its own
mutation fixtures on copies of the script; no cluster is involved.

`KIND_ROUND_EVENTS` sets the initial load size and defaults to `6000`; it must
be a positive integer. Each initial event uses a distinct `host-{i}` value.
Values at or below `4096` may leave panel 141's
`siglake_group_count_deltas_folded_bucket` query without a series, in which
case the panel check exits the round non-zero.

The mirror-reclamation qualification is two separately launched rounds over
one frozen source. Each launch must state the complete recipe; the script
refuses a partial arm before creating a cluster:

```bash
KIND_ROUND_MIRROR_RECLAIM_ARM=off \
KIND_ROUND_CATALOG_CLAIM_ENABLED=false \
KIND_ROUND_WAL_MIRROR_ENABLED=true \
KIND_ROUND_WAL_MIRROR_ACTIVE_INTERVAL_SECS=0 \
KIND_ROUND_COMMITTED_RETENTION_SECS=901 \
KIND_ROUND_MIRROR_LEDGER_RECLAIM=false \
KIND_ROUND_LOAD_SECONDS=3600 \
RESULTS_DIR=results/mirror-reclaim-qualification scripts/kind-round.sh

KIND_ROUND_MIRROR_RECLAIM_ARM=on \
KIND_ROUND_CATALOG_CLAIM_ENABLED=false \
KIND_ROUND_WAL_MIRROR_ENABLED=true \
KIND_ROUND_WAL_MIRROR_ACTIVE_INTERVAL_SECS=0 \
KIND_ROUND_COMMITTED_RETENTION_SECS=901 \
KIND_ROUND_MIRROR_LEDGER_RECLAIM=true \
KIND_ROUND_LOAD_SECONDS=3600 \
RESULTS_DIR=results/mirror-reclaim-qualification scripts/kind-round.sh
```

The arms write to `mirror-reclaim-off/` and `mirror-reclaim-on/` beneath the
given results directory. Each directory retains the exact launch, rendered
configuration, one-minute mirror object/byte and scoped `wal_segments` series,
the retention-purge, unreclaimed, mark-error and committed-row counters, the
actual load window, raw start/end compactor metrics, and sent-versus-Iceberg-
committed row reconciliation. Pod identity and restart count accompany every
counter sample. This mode changes no chart default and is off when
`KIND_ROUND_MIRROR_RECLAIM_ARM` is unset.

`POSTGRES_OUTAGE_PROBE=1` also enables `query.jobs.persistent` for this
throwaway install, submits a bounded batch burst, pauses only the kind Postgres
process, restores it under a shell trap, and retains
`results/postgres-outage-reconnect.json`. That evidence pins the repository and
container revisions; records the effective reconciliation settings, fault and
restoration timestamps, accepted submissions, per-query-pod backlog and
completion-counter series; and derives submission/completion rates, peak
backlog, and restoration-to-drain time. The probe is off by default and its
durations and burst size are bounded by `POSTGRES_OUTAGE_*` environment knobs.

The first three rounds to run it could not show that the pause held. Each sample
now also carries the state and start time of every postgres process in the
paused container and the Prometheus scrape timestamp behind each value, one
bounded write is attempted before, during and after the pause, and the
container's identity and restart count are recorded across the window. Counters
say what was counted, not when the row was written, so the scrape timestamp is
what separates a write that landed during the pause from the delayed
observation of work that finished before it.

Postgres can date the write itself. The kind StatefulSet starts with
`track_commit_timestamp=on` — postmaster-only, defaulted off by Postgres, and
set for this throwaway install alone; the chart and every other deployment keep
the default. After the bounded recovery window, not at readiness, the probe
reads `pg_xact_commit_timestamp(xmin)` for every job row along with the
effective setting, and retains them as `job_commit_times`. The grader
correlates those rows with the accepted submissions and reports how many
committed inside the pause window. A commit inside it means the pause did not
block writes. Every accepted job dated outside it resolves the
zero-backlog-with-rising-completions observation instead of leaving it as a
problem: the completion was observed work, not landed work.

That reading has limits the grader keeps rather than papers over.
`pg_xact_commit_timestamp(xmin)` dates the row version that is visible at
collection time, not every status transition, and a recovered row can be
amended after it went terminal, so a recovered row cannot clear anything. A
missing row, a NULL timestamp, a job still short of a terminal status, and a
commit inside the second the probe's own stamps are truncated to are all held
as gaps, and a gap leaves the observation unexplained.

`scripts/check-kind-postgres-outage-evidence.sh` grades offline fixtures in CI;
missing series, a backlog that never rises, one that never drains, a missing or
running process observation, a bounded write that completed during the pause, a
container restart, an undated or in-pause job-row commit, and an outage sample
with zero backlog and rising completions before restoration are all
`unverified`, not passing evidence. A drain whose accepted job rows are all
dated outside the pause is graded `verified` with the resolution recorded. The
same check runs the probe's three remote readers against a synthetic `/proc`
and psql stand-ins, and drives the probe end to end against recording stand-ins
for `kubectl` and `curl`. No cluster is involved.

Only rounds launched with `INGESTER_POD_LABEL_CAPTURE=1` collect the ingester
per-pod label evidence; ordinary rounds leave the phase off. After every other
observation, and before the panel and ScaledObject evidence, an enabled round
raises the ingester ScaledObject's `minReplicaCount` to 2, drives OTLP
logs and traces at the tier for 90 seconds so both pods carry traffic on more
than one series, and captures four Prometheus answers at one evaluation
timestamp: the raw `siglake_ingest_requests_total` series with their label sets,
the per-series 1m rate, that rate summed by `pod`, and the operator's own
expression (`crates/siglake-operator/src/prom.rs`). The four responses are kept
verbatim as `results/ingester-pod-labels-{raw,per-series,per-pod,expression}.json`
and graded into `results/ingester-pod-labels.json`; the floor goes back to 1
when the phase ends, and a failure is deferred like a panel failure. The floor,
rather than a lowered `keda.ingester.requestsPerSecondTarget`, is what scales
the tier: a threshold the round's steady load crosses would also move the
ingester during the query-scaling window, inside evidence the round has already
collected. What this settles (#3647): `pod` is attached by Prometheus Operator's
target relabeling, so no offline fixture can show it is there, and without two
pods publishing several series each the fleet total, the per-series average and
the per-pod mean are not distinguishable. The grader marks a capture
`unverified` when any series carries a missing or empty `pod`, when fewer than
two pods carried a nonzero rate, when every pod published one series, or when
the operator's value is not the mean of the per-pod sums.
`scripts/check-kind-ingester-pod-labels.sh` (CI's `shell` job and the local
gate) pins the captured expression against `prom.rs`, drives both the
default-off path and the enabled capture against stand-in `kubectl`, `curl` and
`git`, and runs the grader over one passing fixture and eight mutations of it;
no cluster is involved.

Only rounds launched with `COMPACTOR_POD_LABEL_CAPTURE=1` collect the
compactor shared-queue evidence. The phase runs last, after the panel,
ScaledObject and optional schema-rollback observations. It upgrades the same
release with `compactor.replicas=2`, keeps the existing catalog claim and WAL
mirror, and temporarily raises the commit-batch hold to 1,024 MiB or 300
seconds. It then drives 500 events per second for 15 seconds and waits for two
successive Prometheus scrape generations in which both ready compactor pods
publish the same positive `siglake_compactor_sealed_pending{tenant="default"}`
value. The second generation must advance each pod's source scrape timestamp;
one coincidentally equal read is insufficient while the two processes refresh
their gauges independently.

The four verbatim answers are
`results/compactor-pod-labels-{raw,sample-times,per-pod,expression}.json`.
The raw and `timestamp(...)` answers retain labels and source sample times;
the other two retain `sum by (pod)` and the operator's exact
`avg(sum by (pod) (...))` expression at the same evaluation time. The grader
requires exactly the ready pods, `tenant=default`, two advancing settled scrape
generations, equal positive per-pod totals within one 15-second scrape interval,
and an operator value equal to that shared total. A sum of the replicas' copies
therefore fails. `scripts/check-kind-compactor-pod-labels.sh` pins the
expression to `crates/siglake-operator/src/prom.rs`, checks that the chart's
claim, mirror and custom-metric refusals remain in force, drives the settling
helper, and grades mutation fixtures without a cluster.

The command exits non-zero when required panel/trigger data is absent or a
ScaledObject is unhealthy. Before the cluster is deleted the script prints the
state a reader needs from the log alone: on every exit `kubectl get pods -A -o
wide`, per-pod restart counts and `reason=BackOff` events between
`PODS_BEFORE_TEARDOWN_BEGIN` / `BACKOFF_EVENTS_END` marker lines, followed by
the last 60 lines from the previous instance of each restarted regular or init
container between `PREVIOUS_LOGS_BEGIN` / `PREVIOUS_LOGS_END`. On a fresh
install with the KEDA 2.17.2 chart, the cert-rotator writes the certificates
Secret and exits `keda-operator` once within seconds of pod start. That restart
needs no action when its previous tail ends with `Secrets have been updated;
exiting so pod can be restarted (This behaviour can be changed with the option
RestartOnSecretRefresh)`. Within seconds of that rotation,
`keda-admission-webhooks` and `keda-operator-metrics-apiserver` may also restart
once each because they read the Secret volume before the new keys are
projected. Their previous tails end in `open /certs/tls.crt: no such file or
directory` and `open /certs/ca.crt: no such file or directory`, respectively;
these restarts also need no action. Any later restart, a restart count above
one, or any other previous tail still needs action.
On a failed exit the script also prints the release namespace's Jobs and events,
`describe` and the last 200 log lines of every migrate-schema or failed Job, and
`describe` plus logs of every pod that is not Ready, between
`FAILURE_DIAGNOSTICS_BEGIN` /
`FAILURE_DIAGNOSTICS_END`. A pod whose Job was dumped above keeps its
`describe` but not a second copy of its logs, decided by exact membership in
the list of Jobs already dumped. `scripts/check-kind-round-diagnostics.sh` (in
CI's `shell` job and the local gate) drives that decision over match, miss,
no-label and empty-list fixtures with a stand-in `kubectl` — no cluster — and
requires the whole dump to leave the exit status alone. The dump is best-effort
and never changes the exit status. The cluster is deleted on every exit by
default:

```bash
SG_DOCKER=1 KIND_CLUSTER_NAME=siglake-lm scripts/kind-round.sh
KEEP=1 SG_DOCKER=1 KIND_CLUSTER_NAME=siglake-lm scripts/kind-round.sh  # retain it
```

## Prerequisites

- `kind` ≥ 0.20
- `kubectl`, `helm` ≥ 3.12, Docker
- A built `siglake:kind` image. `kind-up.sh` builds and loads it for you
  (uses the same Dockerfile as the AWS path with `--tag siglake:kind`).

## How it's wired

1. `cluster.yaml` defines a single-node cluster with host port mappings:
   - `localhost:8088` → ingester OTLP/HTTP (`POST /v1/logs`)
   - `localhost:8089` → query-server HTTP
   - `localhost:9001` → minio console
2. `manifests/postgres.yaml` brings up the catalog Postgres + a `Secret`
   in the shape the chart expects (`siglake-postgres` with
   `host`/`port`/`user`/`password`/`database` keys).
3. `manifests/minio.yaml` brings up minio + a one-shot Job that
   creates the `siglake-warehouse` bucket.
4. The chart installs with `values.kind.yaml` overrides — endpoint
   override `s3.endpoint=http://minio:9000` and inline
   `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars for minio.
5. `manifests/services-nodeport.yaml` exposes the ingester + query-server
   via NodePort so the host port mappings can reach them.

## Resetting

`kind-down.sh` deletes the cluster entirely — `emptyDir` volumes are
gone with it, so each up/down cycle is a clean slate.
