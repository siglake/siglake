# Design: distributed query execution (#7)

Status: **shipped + multi-pod AWS-validated 2026-06-03** — part 1 (scan
sharding) + part 2a (coordinator merge engine) + part 2b (HTTP transport +
endpoints) + transparent `/api/v1/sql` wiring. Validated on real 2-pod EKS in
the 2026-06-03 round-62 validation, which **found and fixed two bugs** the
in-process differential test could not see (correctness alone can't tell a real
fan-out from the whole-table fallback): (1) the classifier never matched
DataFusion's aliasing projection over aggregates, so `/distributed` silently ran
every query single-pod — now `classify_aggregate` peels `Expr::Alias` layers and
keys merges by output names; (2) the worker `/shard` handler did an uncapped
`collect()` — now enforces the per-shard mid-flight rows breaker (per-pod
parity). **Lifted limitation (2026-07-10):** mergeable aggregates under a
top-level `ORDER BY [LIMIT n]` now use `OrderedAggregate`: workers run the
sort/limit-stripped aggregate SQL, then the coordinator re-aggregates and
applies `ORDER BY`/`LIMIT` over global totals.

## API surface (transparent wiring)

`/api/v1/sql` coordinates **transparently** when worker peers are configured
and the request is an ordinary interactive query (not a `shard` worker
sub-request, not `dry_run`, not `Batch` priority) — those + peer-less nodes
fall through to the single-pod path. The pre-distribution single-pod behavior
is preserved verbatim at **`/api/v1/sql/local`** (always single-pod). The
coordinated path keeps governance parity: the same pre-flight bytes breaker
(estimated on the whole-table plan) + audit emission + metrics as single-pod.
`/api/v1/sql/distributed` (explicit) and `/api/v1/sql/shard` (worker) remain.
Gate `distributed_query_over_http_matches_single_pod` exercises `/distributed`,
`/api/v1/sql` (transparent), and `/local`, all == single-pod.

## Part 2b — transport + endpoints (shipped)

- `QueryFormat` is **unchanged**; the worker endpoint always emits Arrow IPC
  (`format::batches_to_arrow_ipc` / `arrow_ipc_to_batches` — lossless), so the
  production records/ndjson handler is untouched.
- **Worker** `POST /api/v1/sql/shard` `{query, shard:{index,count}}` → runs the
  sharded query (the part-1 `ScanShard` `SessionConfig` extension) → Arrow IPC.
- **Coordinator** `POST /api/v1/sql/distributed` `{query, format?}` → fans out
  via `coordinator::HttpShardRunner` to `query_peers[i]` for shard `i`, merges
  (part 2a), renders records/ndjson. 400 when no peers configured.
- Config: either `--query-peers` (comma-separated base URLs, one per shard; env
  `SIGLAKE_QUERY_PEERS`) or `--query-peer-discovery-srv` (#967, the Kubernetes
  path — see "Worker discovery"), never both, plus
  `--query-coordinator-token`. Each node serves the worker endpoint regardless;
  a node with a membership also coordinates.
- Gate: `distributed_e2e::distributed_query_over_http_matches_single_pod`
  (2 in-process workers over a shared warehouse; count / filtered group-by /
  scan all equal single-pod).

## Problem

Query is single-pod today: one query-server plans + scans the whole file set
for a query. The round-40 concurrency ladder found the per-pod ceiling. For
huge warehouses, large-window analytical scans (especially `raw`
materialization) need the scan **fanned out across pods**.

## Part 1 — scan sharding (shipped)

The load-bearing primitive: a query can be told to scan only a deterministic
**shard** of the table's files.

- `siglake_storage::ScanShard { index, count }` — `ScanShard::new(index, count)`
  returns `None` for a no-op (`count <= 1` or `index >= count`).
- A worker keeps only the file tasks it owns:
  `hash_fnv1a(data_file_path) % count == index`. The hash is process-stable, so
  **every worker agrees** on ownership → the `count` shards **partition the
  file set disjointly and cover it completely**.
- Injected per-request as a DataFusion `SessionConfig` extension; the Iceberg
  table provider reads it in `SiglakeIcebergTableScan::try_new` and filters the
  planned file tasks before the usual per-core split.
- API: `POST /api/v1/sql` accepts `"shard": {"index": i, "count": n}`.
  A whole-table scan is the default.

Gates: `scan_shards_partition_files_disjointly_and_cover` (pure: disjoint +
covering over a 200-file set), `sharded_scans_partition_rows_and_union_to_full`
(end-to-end: the shard counts of a forced-scan query union to the full count).

This is immediately usable: an external coordinator can already issue `count`
shard-queries to `count` worker pods and merge the results.

## Part 2 — coordinator + merge (next)

An in-server **coordinator mode**: the receiving query-server splits the query
into `N` shard-subqueries, dispatches them to `N` worker query-servers over the
existing HTTP API (`shard: {i, N}`), and merges the partial results. Workers
are ordinary query-servers — sharding is the only new behavior, and it already
exists.

### Worker discovery

Kubernetes deployments discover peers from the headless Service's `_http._tcp`
SRV record (Ready endpoints only) and publish a normalized, ordered membership
snapshot; a query captures one snapshot and uses it for `N`, the shard→peer
map, the WAL-partial branch, every primary and failover request, and its
attribution. A refresh publishes a new snapshot and never mutates a captured
one. The static peer list (CLI / env `SIGLAKE_QUERY_PEERS`) remains as the
non-Kubernetes compatibility mode, where the coordinator is peer zero;
configuring both is refused. The full contract — normalization, self-match,
retain-on-error, and failover to the snapshot's explicit coordinator URL — is
in `DESIGN_dynamic_query_peer_discovery_2026-09.md` and shipped in #967.
`N` is the captured peer count; the coordinator is also a worker.

### Merge by query shape
Only certain shapes are mergeable from per-shard partials without a reshuffle —
the dominant log-analytics ones:

| Query shape | Merge |
|---|---|
| `count(*)`, `count(col)`, `sum` | sum the partials |
| `min` / `max` | min / max of partials |
| `GROUP BY k … <agg>` | merge partials by key, re-aggregate |
| mergeable aggregate under `ORDER BY … [LIMIT n]` | workers return complete per-shard groups; coordinator re-aggregates, orders, and limits |
| projection / filter (no agg), `LIMIT n` | concat, then re-apply `LIMIT` |
| `ORDER BY … LIMIT n` | each shard sorts+limits; coordinator merge-sorts+limits |
| `avg`, aggregate `DISTINCT`, aggregate over a subquery | **not mergeable by the classifier → run single-pod (fallback)** |
| **JOIN**, window funcs, global `DISTINCT` on high cardinality | **not** shard-mergeable without a shuffle → run single-pod (fallback) |

The coordinator parses the query (already have the DataFusion logical plan),
classifies it, and either fans out + merges or falls back to a local
whole-table scan. `count(*)`-without-predicate is answered from Iceberg
metadata directly (no fan-out needed) — sharding matters for *scanning* queries.

### Transport
Reuse `POST /api/v1/sql` with `format: ndjson` (or Arrow IPC for efficiency —
a follow-on) for partial results. Coordinator applies a per-shard timeout +
partial-failure policy (fail the query, or return a `partial: true` flag).

### Why this split
Part 1 is the correctness-critical, fully-testable core (disjoint + covering
file ownership). Part 2 is mechanical fan-out + a merge table, but it needs a
multi-pod cluster to validate meaningfully — so it lands as a separate,
AWS-smoked increment on top of the shipped primitive.
