# Design: dynamic query-peer discovery

Status: **decision for #967** (2026-09-07). This document replaces the open
discovery question in `DESIGN_distributed_query.md`; the implementation and the
removal of the deployment ceilings remain deliberately separate work.

## Evidence and scope

Dynamic discovery is a capacity-correctness change, not a promise that adding
workers makes the ordinary log-search query faster.

The 2026-08-18 scaling round wired one coordinator to four peers and observed
**7,409 of 7,409 queries execute locally**; each peer received only two health
checks. The mix was small-`LIMIT` browses and Tier-1 aggregates, both of which
are intentionally local because the fan-out overhead is greater than the work
saved. The same round did show query *replication* scaling from 705 QPS on one
replica to 2,269 QPS on three (3.22x). The 2026-09-02 and 2026-09-03 handoffs
carried that result forward when recording the static-peer ceiling. The public
record is in `ARCHITECTURE.md` (Performance) and `LIMITATIONS.md`.

There are nevertheless shapes that use file-shard fan-out: large scans and
mergeable aggregates that miss the Tier-1 paths. Fixed two-peer kind rounds
already prove a genuine distributed `GROUP BY` whose per-key counts sum to the
full row count. Dynamic discovery makes every ready autoscaled replica eligible
for those shards while preserving the local decision for the measured UI mix.
It does not change the classifier, the local-scan threshold, or the merge
algorithms.

## Decision

### Discover through the headless Service's SRV record

Each query server resolves the named `http` port of the existing headless
Service, for example:

```text
_http._tcp.siglake-query-headless.default.svc.cluster.local
```

The SRV answers already contain the two facts dispatch needs: a stable
StatefulSet pod FQDN and its port. The resolver normalizes target names (lower
case, no trailing dot), removes duplicate `(target, port)` pairs, and sorts by
that pair before publishing a membership snapshot. DNS answer order therefore
does not create artificial membership churn. The HTTP/HTTPS scheme remains an
explicit deployment setting because SRV records do not carry it.

A StatefulSet or EndpointSlice watch is rejected. It would require authorized
Kubernetes API access and new RBAC in the query data plane; the Helm chart path
has no such access. Running the watch in the operator would also create two
discovery implementations because Helm installations do not use the operator.
SRV works for both deployment paths and keeps Kubernetes access out of the
query server.

The query server polls DNS in a background task at a short fixed interval. A
successful non-empty answer atomically replaces the published snapshot; a DNS
error or empty answer retains the last known good snapshot and is observable.
Before the first non-empty answer, transparent `/api/v1/sql` executes locally
and the explicit distributed endpoint reports that no peers are available.
This startup fallback is correct, only slower. Static `--query-peers` remains
supported for non-Kubernetes and test deployments; configuring both static and
SRV discovery is an error.

Only Ready pods should be returned. The chart and operator therefore stop
setting `publishNotReadyAddresses: true` on the headless Service. A new pod
joins after its existing readiness probe succeeds; a terminating or unready
pod leaves the next DNS view. Resolver failures can temporarily retain a dead
member, but the failover rule below keeps that a latency/availability event,
not a correctness event.

The coordinator identifies itself by matching its process hostname (the
StatefulSet pod name) to the first label of a normalized SRV target. This input
is resolved once in `main` and passed through a pure helper for tests; tests do
not mutate `HOSTNAME`. A dynamic snapshot is eligible for fan-out on this pod
only when it contains exactly one self match. Until then, transparent requests
run locally. This avoids guessing that DNS answer position zero is self and
covers the brief interval between pod readiness and the next DNS refresh.

### Pin one immutable membership snapshot per query

The discovery task publishes an immutable `PeerSnapshot` containing a
generation, ordered peer URLs, and the coordinator's own worker URL. A request
clones one `Arc<PeerSnapshot>` when the distributed plan is selected and uses
that same value for all of the following:

1. `N`, the shard count;
2. the mapping from shard `i` to `peers[i]`;
3. WAL-buffer partial handling, which currently branches on `peers.len()`;
4. every primary shard request and failover request; and
5. distributed phase attribution.

Membership refreshes never mutate a captured snapshot. If a peer joins while a
query is running, that query does not address it; the next query may use the
larger snapshot. If a peer leaves, its assigned shard is still `(i, N)` and is
failed over unchanged. The file predicate remains
`fnv1a(file_path) % N == i`, so shards `0..N` are disjoint and cover the file
set regardless of which pods serve them. Different coordinators may briefly
observe different DNS generations; each answer is still complete because
consistency is required *within* a query, not between queries.

The table generation remains pinned independently by the existing
`(table, snapshot_id)` shard header. Membership pinning prevents shard omission
or duplication; snapshot pinning prevents mixing file generations. Both are
required for a correct answer.

StatefulSet ordinal reuse is safe under the same rule. If a pod is replaced at
the same DNS name between dispatch and connection, the replacement receives
the original `(i, N)` and table-generation pin. It either serves exactly that
fragment or returns `503 shard_pin_unresolved`; it cannot silently substitute
its current table generation.

### Fail a departed peer's exact shard over to the coordinator

`coordinate_with_failover` keeps its current one-retry policy, with one wiring
correction: the fallback runner is built from the captured snapshot's explicit
coordinator URL, not `peers[0]`. The static list is identical on every pod
today, so `peers[0]` is the coordinator only on ordinal zero; SRV ordering makes
that assumption even less valid.

For SRV mode the explicit URL is the self member matched above. Static mode
keeps its documented legacy contract that the coordinator is shard/peer zero;
the snapshot copies that URL into the explicit field at capture time. The
Kubernetes chart and operator no longer use static mode, which removes the
incorrect all-pods-share-peer-zero wiring from supported autoscaled installs.

On a connection, DNS, dropped-socket, or worker-500 failure, the coordinator
posts the same SQL, shard `(i, N)`, tenant, and table-generation pin once to its
own `/api/v1/sql/shard`. This preserves the worker path's pruning, breakers,
cancellation, and pin checks. A 4xx, `429`, `503`, or `504` remains an answered
verdict and is not retried. If the local fallback also fails, the whole query
fails; partial results are never returned as a complete answer.

A joining peer cannot receive a failed-over shard from an older snapshot. That
would make retry placement depend on live membership and complicate overload
and pin semantics without improving correctness. The coordinator is the sole,
stable fallback for the lifetime of the request.

### Keep admission per coordinator; make peer count observable

The #550 double-reservation premise no longer describes the code. A distributed
query takes one whole-query admission reservation on its coordinator and holds
it through fan-out and merge. Worker `/shard` requests take no admission
reservation; they remain bounded by the process-wide DataFusion pool and the
per-shard breakers. Dynamic discovery does not reintroduce worker admission or
price the same scan twice.

The existing cost estimate is deliberately the unsharded whole-query estimate
and remains independent of `N`. Increasing `N` reduces each worker's file share
but adds HTTP/Arrow partials and can increase coordinator merge work. There is
not enough field evidence to replace that mixed cost with an `N` multiplier,
and doing so would make scale-out consume *more* coordinator slots even when it
reduces scan time. #967 therefore keeps the current admission formula and uses
the captured peer count only for shard planning and attribution.

This retains a known limit: with many coordinators, a worker can receive shard
work from all of them, and worker concurrency is not cluster-admitted. The
deployment's autoscaling maximum bounds the membership size, while the memory
pool and breakers bound each worker. A cluster-wide admission protocol or a
worker reservation priced from its actual shard is separate work and requires
evidence of pool pressure under dynamic fan-out; it is not part of #967.

At minimum the implementation publishes:

- `siglake_query_peer_discovery_members` (current published member count);
- `siglake_query_peer_discovery_refresh_total{outcome="changed|unchanged|error|empty"}`;
- `siglake_query_peer_discovery_last_success_seconds`; and
- the captured peer count in distributed request attribution.

Membership changes log the old and new normalized identities. Existing
coordinator mode, shard-pin, and failover counters remain the correctness and
failure signals.

### Replace the deployment ceilings only when discovery is active

The chart and operator switch distributed Kubernetes deployments from the
rendered `--query-peers` list to the SRV discovery option even when the current
or minimum replica count is one. A one-member snapshot simply takes the local
path, so future scale-out does not require a rollout.

For Helm, #967 removes the render failure for
`keda.query.maxReplicas > query.replicas`; `query.replicas` remains the fixed
replica count when KEDA is off, while KEDA's min/max own the scaled range when
it is on. The normal positive/range validation remains. The chart must not
remove the refusal in a release that still renders static peers.

For the operator, #967 removes `QueryAutoscalingRangeUnsupported`, renders SRV
discovery, feeds query load through the ordinary autoscaling decision instead
of `fixed_query_replicas`, and reconciles any valid query min/max range. There
is no replacement error condition: the range becomes supported. Existing
`InvalidSpec` checks for malformed ranges and non-positive targets remain.
Discovery health is a query-server metric, not an operator condition, because
the operator cannot distinguish DNS cache lag from a data-plane failure.

Static peer mode retains the old operational contract and does not authorize a
dynamic autoscaling range. It is an explicit non-Kubernetes compatibility path,
not an alternate way for the chart or operator to lift their ceilings.

## Implementation sites for #967

| Area | Required change |
|---|---|
| `crates/siglake-query-server/src/main.rs` | Add mutually exclusive SRV discovery/scheme configuration, start the resolver, and derive the pod identity through a pure resolver helper (tests must not mutate the environment). |
| `crates/siglake-query-server/src/lib.rs` | Replace `coordinator_peers: Option<Arc<Vec<String>>>` with a static-or-dynamic peer source that returns an immutable request snapshot. |
| `crates/siglake-query-server/src/coordinator.rs` | Add the snapshot/member types or consume them in `HttpShardRunner`; retain `(i, N)` from the snapshot and use an explicit self URL for failover. |
| `crates/siglake-query-server/src/sql.rs` | Capture one snapshot in `distributed_inner`; use it for WAL partials, runners, shard count, failover, and stats instead of repeated `peers.len()` reads. |
| `crates/siglake-query-server/Cargo.toml` | Add an asynchronous SRV resolver dependency with its cache disabled or otherwise forced to re-query, leaving CoreDNS as the TTL authority. |
| `deploy/helm/siglake/templates/{service-query-headless,statefulset-query-server}.yaml` and `_helpers.tpl` | Advertise only Ready endpoints, render the SRV name/scheme, remove the static list and its KEDA ceiling only after the binary option exists. |
| `deploy/helm/siglake/{values.yaml,README.md}` | Describe the dynamic range, discovery convergence, local startup fallback, and retained static CLI mode. |
| `crates/siglake-operator/src/{render,reconciler,scaling}.rs` | Render discovery, use normal query scaling, remove the fixed-range condition, and update unit/integration tests. |
| `crates/siglake-operator/src/crd.rs` plus committed CRDs | Remove the fixed-query description and regenerate both CRD copies. |
| `README.md` | Remove the deliberate KEDA ceiling only once #967 ships and replace it with the discovery/failover operational contract. |

## Verification before a field round

#967's hermetic gates must prove:

- shuffled and duplicate SRV answers normalize to one stable ordered snapshot;
- resolver error/empty-answer handling retains the last known good snapshot,
  while startup without a snapshot executes locally;
- a query that captures `N=2` dispatches exactly shards `(0,2)` and `(1,2)`
  even if the directory publishes `N=3` before either response completes;
- the next query observes the three-member snapshot;
- a departed peer retries exactly its original `(i, N)` and table-generation
  pin on the captured coordinator URL;
- ordinal reuse cannot answer from a different table generation;
- Helm renders a valid KEDA range above `query.replicas` with SRV discovery and
  no static peers; and
- the operator accepts a query range, scales through the ordinary decision,
  and renders the same discovery contract.

Task #968's no-spend integration gate passed on 2026-09-11 in recorded kind
round #43 from merged commit `b58634b903db`. The installed ScaledObject had
`MIN=2` and `MAX=4`; scale-out was requested at 17:18:09Z and observed at
17:18:15Z, then scale-in was requested at 17:18:28Z and observed at 17:18:30Z.
The before, during, and after samples used exact transparent `/api/v1/sql`
aggregate fan-out across 2, 4, and 2 peers. Their cross-shard `GROUP BY` sums
matched the full tenant row counts of 6,000, 6,000, and 6,320. The newly joined
`siglake-query-2` and `siglake-query-3` pods each answered a pinned shard. The
grader accepts aggregate or ordered-aggregate fan-out; this round observed
aggregate only. The retained evidence is `results/scale-2-4-2.json` and
`results/membership.log` in the round's snapshot.

## Field-round plan (separate spend approval)

Task #909 owns the separate S3-backed round. The free kind gate is complete;
#909 still needs a pinned fleet launch with the churn parameters propagated and
explicit spend approval.

Run the standard local-heavy UI mix and one forced, genuinely distributable
large aggregate while scaling the query tier 2 -> 4 -> 2. For every phase,
capture DNS membership, published member count, shard requests per pod,
failovers, admission/pool pressure, and coordinator mode. Assert:

1. the normal mix remains local and gains throughput only through replication;
2. all four ready pods receive shard work for the distributable shape after
   convergence;
3. transparent `/api/v1/sql` matches `/api/v1/sql/local` exactly;
4. cross-shard `GROUP BY` per-key counts sum to the full row count;
5. a scale-down during an in-flight query either completes through same-shard
   local failover or returns a bounded error, never a partial/wrong answer; and
6. no worker admission reservation reappears and pool/refusal signals remain
   within the existing bounds.

Latency improvement is reported but is not the correctness gate. The measured
UI mix gives no reason to expect fan-out to improve its individual requests;
the round exists to validate membership churn and the rarer distributed path.
