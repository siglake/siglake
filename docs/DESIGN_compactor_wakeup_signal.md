# An independent compactor wake-up signal (task #3712)

**Status:** design only. No operator behaviour, CRD text or generated artifact
changes with this document, and
`InvalidSpec` / `AutoscalingZeroFloorUnsupported` keeps refusing every
`spec.autoscaling.<component>.min == 0`. **Date:** 2026-09-23.

`spec.autoscaling.compactor.min: 0` is refused because the signal that would
ask for a compactor back is published by the compactor
(`crates/siglake-operator/src/reconciler.rs:931-947`). Two separate defects sit
behind that one refusal:

1. **No reading survives the stopped tier.** The compactor backlog query reads
   `siglake_compactor_sealed_pending`, which the compactor sets from its own
   `peek_pending` (`crates/siglake-compactor/src/lib.rs:5696-5713`). At zero
   replicas the series goes stale and then absent.
2. **One absent reading vetoes the other two.**
   `PromClient::observed` takes all three samples or returns `Err`
   (`crates/siglake-operator/src/prom.rs:86-102`), and the reconciler turns that
   single `Err` into `replicas_without_signal` for every component
   (`crates/siglake-operator/src/reconciler.rs:138-194`,
   `crates/siglake-operator/src/scaling.rs:137-143`). A stopped compactor would
   freeze ingest and query sizing with it.

The scaling half is already written and tested: `decide`'s `current <= 0` branch
returns 1 on a positive reading and 0 otherwise
(`crates/siglake-operator/src/scaling.rs:148-163`), and `fold_observation`'s
per-signal idle window settles a decaying EWMA residual to exactly zero
(`crates/siglake-operator/src/scaling.rs:279-369`), with
`scale_to_zero_idles_at_zero_and_reactivates` (`:748`),
`a_sustained_idle_window_takes_a_min_zero_compactor_to_zero` (`:1188`) and
`work_after_the_idle_window_reactivates_the_tier` (`:1251`) covering it.

## What an activation signal has to satisfy

1. **Published by a component that is still running.** Every accepted spec keeps
   ingest and query floors at 1 or more, so an ingester-resident or
   operator-resident reading survives a compactor at zero.
2. **Counts work the stopped compactor would have done**, not work in flight
   inside it: the queue, not `claimed.len()` (the defect behind
   `crates/siglake-compactor/src/lib.rs:5759`).
3. **Reaches exactly zero when the queue is empty.** `decide` rounds any
   positive ratio up to one pod, so a signal with a residual floor pins the tier
   at 1 forever. This rules out `siglake_wal_local_sealed_segments`
   (`crates/siglake-cli/src/main.rs:1545`): unregistered and
   claimed-but-uncommitted segments are retained on disk indefinitely and keep
   that gauge positive: deletion is gated on a row that is committed and
   settled (`crates/siglake-cli/src/main.rs:1582-1636`, `committed_and_settled`
   at `:1618`).
4. **Missing, stale and non-finite samples are not zero load.**
   `usable_sample` already refuses all three
   (`crates/siglake-operator/src/prom.rs:108-122`); an activation signal adds
   staleness, because a gauge whose publisher stopped refreshing it keeps
   answering with its last value for the whole scrape-staleness window.
5. **Same value from every publisher replica.** The backlog is one shared queue.
   With N ingesters each publishing the whole depth, `avg` reads the depth once
   — the deduplication `Load::Shared` already depends on
   (`crates/siglake-operator/src/prom.rs:144-153`).
6. **Bounded cost.** The reading is taken every reconcile cycle (30 s,
   `crates/siglake-operator/src/reconciler.rs:410`) and scraped every 15 s
   (`deploy/helm/siglake/values.yaml:989`).

## Option A — the ingester publishes the catalog depth

The ingest server already holds an open `SqlSegmentClaim` against the shared
catalog. Wherever a catalog URI and a mirror prefix are set it spawns a mirror
registrar that inserts every uploaded segment as `status = 'sealed'`
(`crates/siglake-cli/src/main.rs:2271-2303`, `register_mirrored_with_retry` at
`:1791`), and the operator renders `SIGLAKE_CATALOG_URI` into every tier's
environment (`crates/siglake-operator/src/render.rs:1047-1059`) against the
ingest server's `--catalog-uri` (`crates/siglake-cli/src/main.rs:156`). So in an
operator-managed cluster the rows an activation signal needs are written by the
ingester, with the compactor's `sync_mirror_to_catalog` reduced to a recovery
sweep (`crates/siglake-compactor/src/lib.rs:5564-5600`).

The addition is a periodic read on that existing connection, publishing two
gauges from the ingest server:

```text
siglake_wal_segments_sealed{tenant="default"}                    # queue depth
siglake_wal_segments_sealed_sample_age_seconds{tenant="default"} # age of the
                                                                 # last success
```

The depth is `peek_pending`'s `segments` (`crates/siglake-storage/src/catalog_claim.rs:812-840`)
widened to include claims nobody will reclaim — see "Segments stranded in
`processing`" below. On a failed read the depth gauge is left at its last value,
matching the compactor's rule at `crates/siglake-compactor/src/lib.rs:5715-5722`,
and the age gauge keeps rising, which is what lets the reader reject it.

**Cost.** One `COUNT(*)`/`MIN()` per publish interval per ingester pod, served by
`wal_segments_status_idx (status, registered_at_ms)`
(`crates/siglake-storage/src/catalog_claim.rs:464-466`) — the same query the
compactor runs once per drain cycle at an `--interval-secs 1`
(`crates/siglake-operator/src/render.rs:389-391`). A 15 s publish interval on a
2-replica ingester is 8 reads/min against a compactor tier that already issues
60/min per replica, so the added catalog load is a fraction of what stopping the
compactor removes.

**Failure of the publisher.** If every ingester is down the cluster is not
accepting data, so no new backlog is arriving; the stale-age guard drops the
series and the compactor's observation is absent. What that should mean is in
"Fail-safe" below.

## Option B — the operator probes the catalog

The operator would open its own pool against `spec.catalogUri` and run the same
`COUNT(*)`.

Against it: `crates/siglake-operator/Cargo.toml` has neither `sqlx` nor
`siglake-storage`, so this adds a database driver, TLS and a connection pool to
a binary that today speaks only to the Kubernetes API and one Prometheus
endpoint. It also gives the operator direct data-plane credentials — the
operator currently passes `spec.catalogUri` through as an env value and never
connects with it — and needs egress from the operator's namespace to Postgres,
a new NetworkPolicy surface. The probe's result would then have to be folded in
beside the Prometheus readings, on a different failure path from the other three
signals.

For it: it works when no Siglake pod runs at all, and it reads the queue with no
scrape or staleness window in between, which is the only wake-up path that would
also serve a future ingest tier at zero.

## Option C — count the mirror prefix (rejected)

Listing `<warehouse>/<prefix>/` needs no catalog. It is rejected: the prefix
grows monotonically under the default never-purge committed retention
(`crates/siglake-compactor/src/lib.rs:5606-5610`), so the count does not reach
zero (requirement 3) without a per-object cross-check against catalog state,
which is the mirror-reclamation ledger's whole subject
(`docs/DESIGN_wal_mirror_reclamation.md`); a full-prefix listing per cycle is
the load pattern that collapsed the claim path in the 2026-07-14 round; and
paginating it is the unfinished work of #1107.

## Recommendation

**Option A.** It reuses a connection, a query and an index that exist, adds no
dependency or credential to the operator, and keeps every scaling reading on one
transport with one failure mode. Option B's advantage — working with no Siglake
pod running — buys nothing while ingest and query floors stay positive, and this
design does not propose lowering those.

The reading Option A publishes is the same `peek_pending` number the compactor
publishes, so scale-out behaviour does not change meaning when the tier is warm.
Only the publisher changes, and only for a policy that asks for it: the operator
selects the catalog-depth query when `min == 0` and keeps
`siglake_compactor_sealed_pending` otherwise, so no existing cluster's sizing
moves and an upgrade that has not yet rolled the ingesters cannot lose its
compactor signal.

```promql
avg(
  sum by (pod) (siglake_wal_segments_sealed{namespace=…,app_kubernetes_io_instance=…,app_kubernetes_io_component="ingester"})
  and on (pod)
  (max by (pod) (siglake_wal_segments_sealed_sample_age_seconds{…}) <= 120)
)
```

The `and on (pod)` drops any pod whose last successful catalog read is older
than the allowance. If every pod is dropped the query returns an empty vector,
which `usable_sample` already refuses as a reading
(`crates/siglake-operator/src/prom.rs:116-120`).

## Drain-mode coverage

`uses_catalog_claim` is `max > 1` (`crates/siglake-operator/src/render.rs:238-241`),
and it is the same predicate that selects `Load::Shared`
(`crates/siglake-operator/src/scaling.rs:77-83`).

- **Claim mode (`max > 1`).** Registered rows move `sealed → processing →
  committed` through the drain, so the depth rises with arriving work and falls
  as it is drained. The signal is sound.
- **Filesystem drain (`max == 1`).** The compactor sweeps local `sealed/` and
  never consults `wal_segments` (`crates/siglake-compactor/src/lib.rs:2617-2620`).
  The ingester's registrar still inserts rows, and nothing transitions them
  unless the mirror ledger is enabled — `mirror_ledger_reclaim_from(None)` is
  false (`crates/siglake-compactor/src/lib.rs:423-433`) and the marking path is
  gated on it (`:3088-3093`, `crates/siglake-storage/src/catalog_claim.rs:955`).
  A catalog depth under a filesystem drain therefore climbs forever and can
  never read idle. **The refusal stays for this mode.**

So the narrowed refusal is one predicate the tree already has: `min == 0` is
acceptable only for the compactor and only when `uses_catalog_claim(policy)`.
`min == 0, max == 1` stays refused with a message that names the drain mode, and
ingest and query keep the refusal unconditionally.

## Per-component observation

`observed()` becomes per-signal rather than all-or-nothing. Each query is
already a separate GET, so the split costs no extra request:

```rust
pub struct ObservedSamples {
    pub ingester_rps_per_pod: Option<f64>,
    pub compactor_backlog: Option<f64>,
    pub query_in_flight_per_pod: Option<f64>,
}
```

`observed()` returns `ObservedSamples` and logs the per-signal error rather than
propagating the first one; `siglake_operator_prom_query_errors_total` gains a
`component` label beside `namespace`. `SmoothingState.interrupted` becomes
per-signal (`IdleSeconds` already is), `fold_observation` folds each signal
independently, and `reconcile_replicas` takes the per-signal `Option` and calls
`replicas_without_signal` for the components whose reading is absent while the
others take the ordinary decision. A whole Prometheus outage is the case where
all three are `None`, which reproduces today's behaviour exactly — the existing
`zero_metrics_would_collapse_the_fleet` (`crates/siglake-operator/src/scaling.rs:1360`)
and `an_outage_restarts_the_idle_window` (`:1272`) stay as written.

## Fail-safe: an absent reading at zero replicas

`replicas_without_signal` returns `policy.min` for a cold tier
(`crates/siglake-operator/src/scaling.rs:137-143`), which for a zero-floor
compactor is 0 — the tier stays stopped for as long as the signal is missing,
and backlog accumulates unobserved. That reads a monitoring outage as idleness,
which is the failure the surrounding code exists to prevent
(`crates/siglake-operator/src/reconciler.rs:141-148`).

A zero-floor tier with no usable reading is therefore restored to **1 replica**,
not held at 0. One pod during a monitoring outage is the bounded cost, and the
pod republishes `siglake_compactor_sealed_pending` itself, so the tier can size
correctly from its own signal while the independent one is missing. The
positive-floor arm of `replicas_without_signal` is unchanged.

## Segments stranded in `processing`

Scaling to zero sends SIGTERM to a compactor that may hold a claim. Its rows
stay `status = 'processing'`, which `peek_pending`'s `status = 'sealed'`
predicate excludes (`crates/siglake-storage/src/catalog_claim.rs:812-823`), and
the reclaim that would reset them runs inside the compactor
(`crates/siglake-compactor/src/lib.rs:5589-5597`,
`crates/siglake-storage/src/catalog_claim.rs:1598-1606` and `:1730-1731`). The
last worker to stop can therefore strand its batch where the activation signal
cannot see it and nothing will free it.

The published depth counts `status = 'sealed'` plus `status = 'processing' AND
claimed_at_ms < now - reclaim_cutoff`, reusing the reclaim predicate. Rows a
live worker is committing right now are excluded by the cutoff, so a warm tier's
reading is unchanged; a stranded batch becomes visible one cutoff after the last
pod stops and wakes the tier, which then reclaims it on its own.

## What stops when the compactor is at zero

Retention, delete tasks, orphan disposal, claim reclaim and the
`sync_mirror_to_catalog` recovery sweep are all compactor-resident. At zero
replicas none of them run, and the sweep is the only repair for a mirror object
whose registration was abandoned
(`siglake_wal_mirror_register_abandoned_total`,
`crates/siglake-cli/src/main.rs:1823`) — a segment that is durable, unregistered
and therefore invisible to a catalog-depth signal.

The design covers this with a **maintenance wake**: an upper bound on how long
the tier may stay at zero, after which the operator sizes it to 1 for at least
one drain interval regardless of the reading. A default of one hour bounds the
repair delay to the same order as the existing mirror sync interval while
keeping the tier off for most of an idle day. Any cluster that would rather not
carry this lands on a positive floor, which is what the shipped default remains.

## Wake-up latency

Publish interval (15 s) + scrape (15 s) + reconcile requeue (30 s) + pod start
bounds first-drain latency at roughly a minute after a segment is registered.
The reverse direction is deliberately slower: `IDLE_HALF_LIVES` is 10
(`crates/siglake-operator/src/scaling.rs:219`), so with
`ewmaHalfLifeSecs: 60` the tier parks ten minutes after the queue empties.

With `ewmaHalfLifeSecs: 0` (the default) `fold_observation` passes the raw
sample straight through (`crates/siglake-operator/src/scaling.rs:285-290`) and a
single idle scrape parks the tier, which flaps against continuous ingest. A
zero floor therefore requires `ewmaHalfLifeSecs > 0`, refused as `InvalidSpec`
with its own reason alongside the narrowed zero-floor check.

## What this design does not change

The refusal at `crates/siglake-operator/src/reconciler.rs:939-947` stays exactly
as it is until the implementation lands and a kind round proves the wake-up. A
document is not a proof, and neither is a released tag. `docs/LIMITATIONS.md`
keeps its "No tier can be scaled to zero" entry, with a pointer here.

## Implementation slices

1. Per-component `observed()` and per-signal folding, with the zero-floor
   refusal untouched. Provable by unit tests alone: one absent reading sizes its
   own tier by `replicas_without_signal` while the other two take the ordinary
   decision.
2. `siglake_wal_segments_sealed` and its sample-age companion, published by the
   ingest server's registrar task over its existing claim connection, with the
   widened pending predicate.
3. The operator's query selection (`min == 0` picks the catalog-depth
   expression), the fail-safe-to-1 rule for an absent reading at zero replicas,
   the maintenance wake, and the two narrowed refusals (claim mode only,
   `ewmaHalfLifeSecs > 0` required), with `crd.rs:303-310` and `:338-345`,
   `deploy/operator/crd.yaml`, the helm CRD copy, `docs/LIMITATIONS.md` and
   siglake-docs updated to the behaviour that ships.

## Acceptance scenario for the kind round

Run after the ordinary round, as an opt-in capture like
`COMPACTOR_POD_LABEL_CAPTURE` (`scripts/kind-round.sh:122-125`, `:2593-2596`):

1. Install with `compactor.min: 0, max: 2`, `ewmaHalfLifeSecs` low enough that
   the idle window fits the round's budget, ingest and query floors at 1.
2. Drive ingest, let the drain catch up, then stop ingest and wait for the
   compactor Deployment to reach `spec.replicas == 0`. Retain the operator's
   decision log and the deployment's replica history.
3. With the compactor at zero, ingest again. Retain: the ingester-published
   depth advancing while no compactor pod exists (`kubectl get pods` at that
   instant), the operator scaling the Deployment back to 1, and the segments
   committed afterwards.
4. In the same window, retain the ingester and query tiers taking their ordinary
   decisions while the compactor series was absent — the per-component half.
5. Negative control: block the ingester's catalog reads (or stop the publisher)
   with the compactor at zero and retain that the tier goes to 1 rather than
   staying at 0.
