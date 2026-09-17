# siglake architecture

The long-form description of how siglake is built: each subsystem's contract,
what it guarantees, what it costs, and where the numbers came from. It moved
here from the README on 2026-09-15 so the README could stay a project
overview; nothing was dropped in the move. For task-oriented documentation —
installing, configuring, querying, operating — use the docs site at
https://docs.siglake.dev, which is the user-facing reference. Focused design
records for individual subsystems are the `DESIGN_*.md` files beside this one,
and the deliberate omissions are listed in [`LIMITATIONS.md`](LIMITATIONS.md).

Sections: [API surface](#api-surface) · [Roles](#roles) ·
[Ingest path](#ingest-path) · [Storage](#storage) · [Compaction](#compaction) ·
[Query](#query) · [Consuming segments](#consuming-segments-external-pipelines) ·
[Multi-tenancy](#multi-tenancy) · [Deployment](#deployment) ·
[Performance](#performance-measured-on-aws-3-node-clusters) · [History](#history)

## API surface

| Endpoint | What |
|---|---|
| `POST /v1/logs`, `POST /v1/traces` | OTLP/HTTP ingest (protobuf or JSON). Single-tenant unless a tenant claim or `ingester.trustScopeHeader` routes `X-Scope-OrgID`. |
| `POST /api/v1/_elastic/_bulk`, `…/{index}/_bulk` | Elasticsearch-compatible NDJSON bulk ingest into user indexes. |
| `GET /_cluster/health`, `GET /api/v1/_elastic/_cluster/health` | Elasticsearch-compatible readiness probes (always `green`). |
| Elasticsearch read APIs | `_search`, `_msearch`, scroll, `_field_caps` and `_cat` are not implemented and return `501` with a pointer to `POST /api/v1/sql`. They route, so an ES client gets an answer rather than a `404`, but no ES query API is planned and they are absent from `docs/api/openapi-ingest.yaml`. |
| `POST /api/v1/sql` | DataFusion SQL. Rows (records or streaming NDJSON) + a pre-flight cost estimate + exact scan stats (`rows_scanned`, `bytes_scanned`) + `x-siglake-server-micros`. Transparently coordinates across query replicas; `/local`, `/shard`, `/distributed` are explicit variants; `/explain` + `dry_run` show plans and costs without executing. A bare interactive `SELECT` over one timestamp-bearing table is ordered newest-first for you (`default_order: false` opts out) — see "Implicit newest-first". |
| `GET /api/v1/jobs/{id}`, `…/result` | Async batch-tier queries (`priority: "batch"`, dedicated runtime). Submission shares the query admission budget: an admitted job returns `202`; a full budget returns `429` + `Retry-After`. |
| `GET /api/v1/stream` | SSE live tail of the ingest path. Events are teed before the WAL append, so a tailed event is not yet acknowledged: a batch refused for backlog or whose append fails is still published, and the caller's retry publishes it again. |
| `POST/GET /api/v1/indexes`, `/api/v1/index-templates` | User index management: typed doc mappings, tag fields, tokenizers, per-index retention, per-table `index_at_flush`; templates auto-create indexes by id pattern. A `PUT` of an index is additive only (append nullable fields; never drop, reorder or retype one) and is validated against the base its transaction actually commits onto, re-validated on every Iceberg CAS attempt: a body computed before another writer's addition is refused with `400` rather than storing a mapping that contradicts the live schema, and re-sending the same additive update is a no-op. Templates are tenant-scoped, with one warehouse object per id (`_siglake/config/index_templates/<namespace>/<template_id>.json`), so tenants are isolated and two replicas editing different templates cannot overwrite each other. |
| `GET /api/v1/jaeger/{index}/api/…` | Jaeger-compatible trace query (services, operations, trace fetch/search) — the HTTP subset Grafana renders. Plans read-only and runs on the same process-wide memory pool as SQL, so a pool refusal is the same capacity answer here: `503` + `Retry-After`, counted under `siglake_query_breaker_trips_total{breaker="pool_exhausted"}`. One interactive request lifecycle, shared with SQL rather than duplicated: one reservation out of the same per-pod admission budget (`429` + `Retry-After` when it stays full through the admission wait) and one interactive wall-clock budget, held from before tenant resolution across BOTH query phases of a trace search (`504`, which also cancels the scan). Dedicated render ceilings, DERIVED from that same reservation rather than configured (no `SIGLAKE_JAEGER_*` knob): `?limit=` above the trace ceiling is `400` before the reservation, the index lookup and the planner; span rows and accumulated Arrow bytes are bounded MID-FLIGHT, at a batch boundary, across the whole request (both phases of a search spend one budget), and the tighter of those and the resolved interactive `max_rows_returned` governs. The PLAN is bounded to match — it carries a fetch of one row past the row bound, which turns the span query's blocking sort into a bounded `TopK` — so a refusal no longer materializes the whole match first. Every one of them refuses WHOLE — `413`, no `data`, no `Retry-After` — because Jaeger's `{data, total}` response cannot express a partial trace the way SQL's `truncated`/`max_rows` envelope can. Which ceiling refused is `siglake_query_breaker_trips_total{breaker="jaeger_trace_limit"|"jaeger_span_rows"|"jaeger_render_bytes"|"jaeger_name_rows"}`. |
| `POST /api/v1/delete-tasks` | GDPR-style predicate deletes, executed by compactor sweeps. Terminal states are final: a `failed` task is recovered by resubmitting its request fields, which answers with a new task id (see [`LIMITATIONS.md`](LIMITATIONS.md)). |
| `GET /debug/memory-pool` | Authenticated snapshot of query-pool reserved/limit bytes and the ten largest live DataFusion consumers. |
| `/healthz`, `/readyz` | Probes. |

AuthN: optional cluster-wide bearer tokens (`--auth-tokens` /
`SIGLAKE_AUTH_TOKENS`) or OIDC (JWKS-cached verifier; a configurable JWT claim
routes each query to that tenant's namespace). TLS terminates at your Ingress
or in-binary (`--tls-cert`/`--tls-key`, rustls).

Machine-readable OpenAPI 3.1 specs are generated from the handler annotations
and committed under [`docs/api/`](api/): `openapi-ingest.yaml` and
`openapi-query.yaml`. Regenerate with `cargo run -p siglake-openapi -- --out
docs/api`; CI fails on drift. There is no runtime `/openapi.json` endpoint by
design.

## Roles

| Role | Responsibility |
|---|---|
| **Ingester** (`siglake ingest-server`) | OTLP/HTTP on 8088 and OTLP/gRPC on 4317, plus bulk endpoints → WAL segments. Backpressure router with bounded per-tenant lanes (full lane ⇒ fast `503` + `Retry-After`), token-bucket rate budgets (in-memory or Redis-backed shared across replicas), WAL mirroring to object storage, force-seals the WAL on SIGTERM for safe scale-down. Can run the compactor in-process (`--with-compactor`) for single-process dev and bench runs; the Helm chart refuses that flag, because the embedded compactor takes no catalog claim. |
| **Compactor / drain** (`siglake compactor`) | Drains sealed WAL segments into Iceberg commits — continuous dispatch with N commits in flight, commit-accumulation batching — and runs **leveled compaction**, snapshot expiry, retention/delete sweeps, and orphan GC on the same budgeted loop, so maintenance never starves the commit path. Multi-pod-safe via SQL catalog claims. |
| **Query** (`siglake-query-server`) | Distributed SQL: replicas behind a headless Service with stable DNS; any replica transparently coordinates (file-shard fan-out, two-phase merge, Arrow IPC transport). Replicas add throughput; fan-out engages for large scans, while small-`LIMIT` browses and Tier-1 aggregates are answered locally by design (see [`LIMITATIONS.md`](LIMITATIONS.md)). A process-wide memory pool bounds every sort, aggregate and join; when it refuses (rather than spills) the client gets `503` + `Retry-After`, the same capacity answer the ingester gives, forwarded from a worker rather than re-run on the coordinator. Serves uncommitted WAL data for the events table via the real-time buffer (`--query-wal-buffer-dir`) plus hot last-value caches. |
| **Operator** (`siglake-operator`) | `SiglakeCluster` CRD → renders the deployment; leader-elected; reports `observedGeneration` + schema versions. |
| **Catalog** | Iceberg on SQLite (dev) or Postgres (prod), through siglake's **vendored Iceberg forks** (`third_party/iceberg`, `third_party/iceberg-catalog-sql`). |

## Ingest path

**Durability + lifecycle.** Events are acked after the WAL append has been
`fsync(2)`ed by default, so acknowledged rows survive a node crash or power
loss. The sync covers the directory entries that name the segment as well as
its bytes: the tenant and index directories when they are first created, the
active segment's own name on its first acknowledged append, and the sealed
name after the rename that publishes it — the active copy is unlinked only
once `sealed/` has been synced, so no crash window drops both names. A
directory sync that fails takes down the acknowledgement or the seal that
needed it, and leaves the rows under a name the next recovery pass finds. The
promise assumes a filesystem that honours `fsync(2)`: ext4 or xfs on the
node-attached persistent volume the chart mounts is the supported case, and a
volume whose write cache reorders flushes, or a network filesystem with its
own rename and sync semantics, weakens it to whatever that filesystem
guarantees. Two limits are ours rather than the filesystem's: a crash that
tears the append following an acknowledged one, and the asynchronous mirror,
which leaves an ack durable locally and not yet in the object store (both
in [`LIMITATIONS.md`](LIMITATIONS.md)). Send `?commit=auto` to opt into
acknowledging after `write(2)` reaches
the kernel page cache — that mode takes no `fsync(2)`, directory ones
included. Neither mode waits for the rows to become QUERYABLE —
the compactor commits asynchronously, at a measured p50 of ~5.5s. A
segment moves `active/ → sealed/ → processing/ → committed/` by atomic rename,
and each of those moves — plus the orphan and stale-owner quarantines, the
directory owner stamp, and both halves of a secondary consumer's position (the
watermark it publishes into the WAL and the cursor it keeps in its own state
directory) — syncs the directories it changed before reporting the move, so a
drain's progress is not something a power loss takes back. Losing one of these renames strands no
acknowledged row on its own (the segment is reachable from one of the two
directories, and the compactor's consumed proof stops a requeued orphan from
committing twice); what it takes back is reported progress. Segments carry
CRC-validated framing (WS-8); a segment without that framing — written before
WS-8, or framed and then corrupted past recognition — has its Arrow IPC length
prefixes walked against the file size before anything is decoded, so no
declared metadata or body length can size an allocation the file cannot back
(#4650). Sealed + active segments
mirror to object storage on configurable intervals — **sealed segments mirror by
default** wherever a warehouse URL is set, so the WAL volume is not the only copy
of what has been acknowledged. Active mirroring
(`wal.mirror.activeIntervalSecs`, off by default) covers every writer that has
rows: each tick flushes the per-tenant router's writers and each backpressure
lane's task-owned writer, and uploads one
`_active/<tenant>[/<index>]/<segment>.arrow.partial` per writer whose segment
grew since the last tick. The flush happens under the writer's own lock (or
inside its lane task) and the PUT outside it, so no acknowledgement waits on
object storage. `siglake wal-recover --from
s3://<bucket>/<prefix> --to <wal-root>` restores from the mirror for DR, in two
steps: without `--apply` it PLANS — it lists the mirror, reconstructs the
layout, prints one line per `(tenant, index)` with the segment count, the byte
total the listing reported, a sample key and the destination it would write,
and creates nothing under `--to`, `--to` itself included. `--apply` performs
the restore, rebuilding the per-tenant and per-index layout so each segment
returns to the namespace and table it came from — including the tenant's own
`sealed/`, the discovery directory the drain enumerates tenants by, which an
index-only restore would otherwise leave out (#4972); each restored segment is
written to a temp name, `fsync(2)`ed and renamed under a synced directory
before it is counted,
so a restore that reports 400 segments has 400 whole ones on the volume. The
report carries the counts that separate a finished restore from one that
understood nothing — segments already present, and keys skipped for a layout
recovery will not guess at — and exits nonzero when every key was skipped and
nothing was restored, which is `--from` naming an ancestor of the mirror root.

The same listing decides whether `--from` IS the mirror root, from the two
markers siglake writes at a fixed depth under it: a first component `_active`
with a `.arrow.partial` tail, or a key ending `/owner` at depth 2. Either at
its own depth confirms the root; either exactly one component deeper means
`--from` is one component above it, and both forms of the command then exit
nonzero naming the directory to pass instead — the apply before it creates
anything. A marker at root depth does not cancel a misplaced one. A mirror
with neither marker (no managed index, no active mirroring) is reported
unverified, and the plan is the only checkpoint: its keys still fit the layout
one component up, so an operator who applies it anyway restores under a tenant
named after the mirror prefix. See `docs/LIMITATIONS.md` and
`docs/DESIGN_wal_recovery_root_identity.md`.
Multi-pod deployments coordinate through a
SQL claim table (`wal_segments`, atomic `try_claim`); crash recovery
quarantines ambiguous `processing/` segments rather than risk double commits.
The Helm chart refuses to render a compactor tier that can hold more than one
pod — `compactor.replicas` or `autoscaling.compactor.maxReplicas` above 1 —
without `compactor.catalogClaim.enabled`, and refuses that claim without the
mirror it claims from. The operator uses `wal-mirror` unless the final
`SIGLAKE_WAL_MIRROR_PREFIX` in `spec.extraEnv` selects another trimmed prefix;
the ingester writes and the catalog-claim compactor reads that same namespace.
A blank value remains the mirror opt-out for a compactor maximum of one. If
`spec.autoscaling.compactor.max` exceeds one, the operator reports `InvalidSpec`
before changing workloads instead of allowing the claim path without its
mirror. That maximum also fixes the drain protocol: a maximum above one keeps
the ingester's remote drain and the compactor's catalog claims enabled at every
current replica count, including zero and one; a maximum of one keeps the
filesystem drain and `Recreate` rollout strategy. Scaling 1→2→1 therefore does
not transfer retained WAL between ownership protocols. The same maximum decides
how the backlog is read. Under the claim, `peek_pending` counts the whole sealed
queue with no worker filter, so every compactor publishes the same total and
`siglake_compactor_sealed_pending` is one queue rather than one pod's share:
`spec.autoscaling.compactor.target` is divided into that queue once, and a
backlog of 8 at a target of 4 asks for two workers whether two or four are
running. The filesystem drain runs at a single pod, whose sealed count is its
own. The chart's own compactor HPA cannot do that division — it can only render
the gauge as a per-pod metric, which multiplies a shared queue by the replica
count — so `autoscaling.compactor.customMetric.enabled` is refused with the
claim on, and claim-mode HPA scaling there is CPU-only. It also refuses
`ingester.extraArgs: [--with-compactor]` outright: that embedded compactor
takes no claim and the chart renders none for it, and a rolling update alone
puts two of them on one table.

**What the mirror costs.** Measured on loopback against a filesystem-backed
object store, five interleaved on/off pairs per shape, ack mode and WAL roll
held constant (`docs/PERF_WAL_MIRROR_2026-09-11.md`, re-measured 2026-09-13 with
a retained harness and per-arm evidence). Each queued segment gets a
durable `mirror-pending/` hard link before it is queued, and keeps it until the
remote object is confirmed, so a remote outage grows that local directory
instead of discarding the only upload source. That link and its directory fsync
are **0.47–0.58 ms of synchronous seal time** (per-shape medians; 0.48–0.66 ms
in every one of the ten arms). The fsync is 94 % of that — the lookup and the
link are 26 µs of it — and it does not batch. One writer owns each lane's
`mirror-pending/` and seals under its own lock, so the directory never holds two
unsynced pins to share a sync between; deferring the sync past the seal would
have to be closed before the compactor retires the sealed name, and the
compactor is a different process, on the `ReadWriteMany` volume this chart
renders, whose `fsync` says nothing about a directory entry the ingester's
client wrote. The measurement that splits the three costs and the crash-ordering
argument are in `docs/PERF_WAL_MIRROR_2026-09-11.md` ("Can the pin share a
directory sync?").
Throughput: a fixed 20K EPS is delivered by both arms with no `503`s — 4.9
seals/s costs 0.3 % of a writer-second — while the saturation ceiling drops
**−3.2 %** (256.8K → 248.7K EPS median), five of five pairs and wider than the
2.2 % spread among the off arms; at 61 seals/s the pin is 2.9 % of a
writer-second, and at the ceiling that comes off the rate. Ack latency: p50 does
not move at either shape (only 1.2 % of requests carry a seal), and p99 is
+0.49 ms at saturation. Upload lag: the mirror stays within **one sealed
segment** of the writer at 61 seals/s, catching up 20–21 ms after the last
request, with no failed or abandoned uploads.
`siglake_wal_mirror_queue_depth` reports waiting segments (never above one here)
and `siglake_wal_mirror_queue_wait_seconds` their dequeue delay (0.6 ms mean at
saturation); both start after the pin, so neither counts it. PUT volume is one
object per sealed segment (4.9/s at 20K EPS with the 4096-event roll). None of
this is an S3 measurement: AWS run 65 measured the S3 uploader at least 64
seconds behind at about 15 seals/s, and the former 60-second committed-file
reaper removed 285 segments before their first upload attempt.

**Reclaiming mirror objects.** Committed retention
(`compactor.committedRetentionSecs`, default 24 h, `0` opts out, non-zero
floored at 901 s) deletes a mirror object and then its `wal_segments` row —
object first, so an interruption leaves a committed row with no object, which
is inert, rather than an unknown object a listing would re-register and drain
twice. That pass belongs to the catalog-claim drain, which knows an object was
committed because it is the thing that claimed it. The filesystem drain commits
out of local `sealed/` and never reads the mirror, so it reclaimed nothing at
all until `compactor.mirrorLedgerReclaim` (off by default). With that on, the
compactor attaches the claim store and the mirror operator **without** the
claim path — no `try_claim`, no mirror-to-catalog reconciliation, no
abandoned-claim reclaim, so it can never register an object it did not commit —
and upserts the ingester's row from `sealed` to `committed` for each file in
local `committed/`. The mark is driven off that directory rather than off the
commit return, which makes it idempotent and repairs a crash between the
Iceberg append and the mark; it skips segments holding a live `mirror-pending/`
pin, whose upload is still owed; and the local sweep is gated on it, so local
commit evidence is destroyed only after remote evidence exists. The 3600 s
`committed/` ceiling still wins over that gate — a catalog outage costs a
bounded leak, counted in `siglake_compactor_mirror_unreclaimed_total`, rather
than an unbounded WAL volume. Objects no local drain ever committed (a dropped
incarnation's quarantined segments, an ingester whose volume was lost) are
deliberately left to an operator-side lifecycle rule.
`docs/DESIGN_wal_mirror_reclamation.md` prices the three options and
`docs/LIMITATIONS.md` states what remains uncollected.

**Drain.** The drain claims bounded batches (default ≤64 segments / 64 MiB per
commit) and keeps `SIGLAKE_DRAIN_CONCURRENCY` commits in flight
*continuously* — topping up the moment one lands, re-listing `sealed/` as new
segments arrive — bounded by a per-pass budget so compaction and gauges keep
their cadence under backlog. Commit-accumulation batching (default on, 64 MiB
target / 10 s age floor) amortizes the fixed per-commit catalog cost; the
vendored `update_table_with_base` elides redundant metadata re-reads.
A claimed segment whose bytes do not decode — a torn restore, a bad sector, a
frame version this build does not know — fails the whole batch it is in, and
releasing it back to `sealed/` only hands it to the next batch. After
`SIGLAKE_COMPACTOR_POISON_ATTEMPTS` consecutive failed reads (3; `0` disables)
the drain moves that file, and only that file, to `<wal>/poison/` with a
`.poison.json` note recording the error and the attempts spent; its batch
siblings commit on the next pass. Nothing under `poison/` is deleted, rewritten
or automatically requeued — unlike `orphans/`, whose residents are disposed of
every cycle — so requeueing is an operator running `siglake wal-requeue
--wal <wal-root>` (`--segment` for one file, `--dry-run` to read the verdicts
first) once the cause is fixed. The set-asides are counted by
`siglake_compactor_segments_poisoned_total` and levelled per tenant by
`siglake_compactor_segments_poisoned`, which fires `SiglakeSegmentsQuarantined`
alongside the catalog-claim path's own quarantine. The rows in a set-aside
segment are acknowledged and not queryable, which is the point: the alternative
is a queue that never drains.
Every other failure is retried inside the pass, bounded per segment: a pass
makes at most three claims on the same segment and then leaves it in `sealed/`
for the next cycle, counting it under
`siglake_compactor_pass_claim_attempts_exhausted_total`. That bound is what
stops a cause which fails fast and names no file — a recurring catalog
conflict, a store refusing writes — from spending a whole cycle budget on
claim/release renames of one set. It is per segment name rather than per batch,
so regrouping buys no further attempts; it is pass-local, so recovery from a
transient cause is one poll away; and the segments a pass has not tried,
including ones sealed while it ran, stay claimable throughout.
Cumulative per-table aggregates are maintained in a **side object** (see
Storage), with an optional write-behind mode
(`SIGLAKE_SIDE_AGG_WRITE_BEHIND=1`) that moves its serialized S3
read-modify-write off the commit path.

**Index-at-flush vs deferred.** By default every flushed file carries its full
search acceleration inline. For firehose streams, per-table
`index_at_flush: false` (or `SIGLAKE_INDEX_AT_FLUSH=0`) defers the raw-text
index work — inverted-index tokenization, trigram + row-group token blooms,
Puffin sidecar upload, measured at **~30 % of append time** — to compaction,
where consolidation materializes it. The query funnel tolerates mixed
indexed/unindexed files by falling back to a scan.

Footer inverted indexes are enabled by default. Set
`SIGLAKE_INVERTED_INDEX=0`, Helm
`compactor.invertedIndex.enabled: false`, or operator
`spec.extraEnv: [{name: SIGLAKE_INVERTED_INDEX, value: "0"}]` to disable them.
User-index mappings still select the indexed text columns and tokenizers;
whole-string `raw` tokenizers keep using Parquet blooms instead of duplicating
their tokens in an inverted index.

Post-rewrite Puffin rebuild is **off** by default (opt-in). Set
`SIGLAKE_INDEX_REBUILD=1`, Helm `compactor.indexRebuild: true`, or operator
`spec.extraEnv: [{name: SIGLAKE_INDEX_REBUILD, value: "1"}]` to enable it.
Streaming rewrites have no whole-file footer index and require a Puffin
sidecar; in-memory rewrite outputs whose index fits the footer do not. With the
rebuild off, a rewrite leaves its output's index coverage as it found it —
streaming outputs come out unindexed and are queried by scan. Reads of indexes
that already exist are unaffected, and so is the writer's footer index on the
flush path: only new sidecars stop being produced. Enabled, the rebuild checks
every `(file, column)` and skips both footer indexes and Puffin registrations,
including registrations whose data snapshot has expired.
Missing indexes affect pruning only: queries still scan and return exact rows.

The default is off because a compacted file's inverted index costs about 40
bytes per indexed row — roughly 294 MB parsed for a 7.3M-row file — so a
50G-class layout's text plan needs several gigabytes of parsed index against a
query pod's 1 GiB parsed-index cache, and the shapes that take the sidecar path
land above the ceilings measured on the scan path. Enable it where the working
set fits, or where pruning is worth more than the decode. That whole-file cost
is a property of the sidecar format, not of its sizing: a row-group-addressable
replacement a reader can touch in part is specified and measured in
`docs/DESIGN_segmented_inverted_index.md`. It is a prototype behind its own
magic, footer-KV key and Puffin blob type. The scan path can read one —
uncompressed, by byte range, through `PuffinReader::blob_range_reader`, with
the sidecar's directory checked against the file's actual row groups and
anything it cannot conclude falling back to the v1 index or an exact scan
(#4561), and the parsed directory held between lookups under a byte budget of
its own (#5006, `SIGLAKE_SEGMENTED_INDEX_DIRECTORY_CACHE_MAX_BYTES`, separate
from the two budgets above) — but only when `SIGLAKE_SEGMENTED_INDEX_READS` is
set, and no writer produces one, so nothing in this section changes by
default, and nothing is retained under the new budget either. The three
formats have been compared through the query path on a 102.76M-row local
corpus (#4562): a rare unclipped text predicate is 11.5x faster than the scan
where the shipped sidecar is 22.4x slower, and the result holds with both
budgets above at zero. The format is 5.59x the shipped sidecar's bytes on disk
until its blocks are compressed, which is what stands between it and a writer.

**Whether to USE an index is decided per execution.** Loading one is a
whole-file cost, so a query that wants a handful of rows cannot pay it: a text
predicate under a `LIMIT` stops the scan after a sliver of the first file,
while the index charges for every row in every planned file. Both forms are
declined — a `LIMIT` under an `ORDER BY timestamp` (including the implicit
newest-first one) because an index row selection defeats the ordered drain's
contiguous tail read, and a bare `LIMIT` because the scan short-circuits
first. An unclipped text scan keeps the index, which is the regime it wins in.
The refusal is attributed by
`siglake_query_inverted_index_declined_total{reason}` and named in the scan's
`EXPLAIN` line (`text_index:[declined:clipped_limit]`), and it never changes a
result: the index only ever produced a superset row selection, blooms stay
active, and the exact predicate is re-evaluated above the scan either way.

**Freshness.** The query tier's WAL buffer serves *uncommitted* sealed +
processing segments for the events table, unioned with Iceberg under the same
name and de-overlapped via commit-stamped consumed-segment lists — measured
**~5.8 s ingest→queryable** for a lone marker on a bench node (seal-age
dominated; sustained streams seal by size). User indexes see commit-cycle
visibility until per-index buffer serving lands (see roadmap).

## Storage

**Layout.** One Iceberg table per index (`events` plus user indexes),
`day(timestamp)`-partitioned, written as ZSTD-3 Parquet v2 with datatype-tuned
encodings (`DELTA_BINARY_PACKED` timestamps, dictionaries on tags). Row groups
are byte-targeted (~256 MB uncompressed) from the in-flight batch's measured
row size; every write path emits files through the same writer.
Fresh tables stamp the minimum Iceberg format version required by their schema.
Every schema siglake ships — `events`, `query_audit`, user indexes — is
**format version 2**: event time is a microsecond `timestamptz`, and no Siglake
schema uses a v3-only type. A schema that did would still be stamped v3.

**Timestamp contract.** `timestamp` is `timestamptz` at microsecond precision
(Parquet INT64 TIMESTAMP(MICROS, isAdjustedToUTC=true)); `events` also carries
`timestamp_ns`, a required `long` holding the OTLP `time_unix_nano` value
verbatim. siglake's own scans filter and order on `timestamp_ns`, so nothing is
lost internally, while external engines get a type they all understand. See
`docs/DESIGN_time_ordered_storage.md` ("Timestamp contract"), including why
pre-0.1.0 warehouses must be recreated rather than migrated.

**Time-ordered invariant.** Every write — drain append, compaction rewrite,
direct append into any index (a consumer's own output tables are ordinary user
indexes) — physically sorts rows by the table's *declared*
Iceberg sort order before the partition split and stamps Parquet
`SortingColumn` footers. Fresh `events` tables declare
`(timestamp ASC, timestamp_ns ASC)` — the sibling breaks microsecond ties, so
the order stays total; some long-lived
tables still declare the legacy `DESC` order, and the query tier is
direction-aware rather than assuming; convergence is a metadata-only flip that
reclustering completes (`docs/DESIGN_time_ordered_storage.md`).

**Search acceleration, self-describing in the files:**
- a **file-level trigram bloom** + **per-row-group token blooms** over `raw`
  for arbitrary `LIKE '%substr%'` pruning;
- **inverted indexes** per configured text column: small blobs ride the Parquet
  footer KV, large ones live in **Puffin sidecars** registered to the snapshot;
- per-file **group-count** and **time-bucket** footers powering the aggregate
  fast paths — group counts use a compact front-coded binary encoding a query
  can read one column out of without touching the rest
  (`docs/DESIGN_group_count_footer_encoding.md`);
- file-layout metadata plus rewrite-generation markers in the file *names*
  (`siglake-g<N>-…`), so compaction policy is computable from the manifest
  alone — nothing lifecycle-like is persisted that could disagree with policy.

Parquet-native per-column blooms are an opt-in, write-only knob:
`SIGLAKE_PARQUET_NATIVE_BLOOMS=on`. They have been off by default since
2026-08-06 because time-sorted files spread every tag value across every row
group, preventing useful pruning.

**Side-object aggregates.** Cumulative whole-table aggregates (group counts,
time buckets, 2-D time×group counts) live in
`metadata/siglake-agg/<table-uuid>/siglake-aggregates.json` per table —
deliberately *not* in snapshot summaries, which bloated `metadata.json` and
slowed every commit. Readers require both `total == total-records` and a
continuous snapshot-coverage chain before trusting them. Each Siglake append
publishes a parent-to-snapshot coverage link with its aggregate contribution;
links may bridge only snapshots marked `siglake.rewrite=recluster`, whose
commit path proves row conservation. An unmarked overwrite therefore breaks
coverage even when it replaces N rows with N different rows. The query then
uses the exact per-file path. The same rule covers the inline object, folded
wide counts, time buckets and 2-D time×group counts.
Artifacts written before coverage links existed deserialize without claiming
coverage and remain on the exact per-file path, and nothing repairs that on its
own: a chain with no head cannot be rejoined, because the first append edge
after the gap names a parent nothing matches and every later edge chains onto
that stranded run. `rebuild-group-counts` restores the wide group-count object
from committed files; `rebuild-time-aggregates` restores the inline object's
time buckets and 2-D time×group counts and republishes a coverage edge, after
which ordinary commit-path maintenance carries the chain forward again.

Every edge is published at one normal form: the deepest ancestor reachable
through nothing but re-clusters, which is the parent an append's link names.
An edge at a re-cluster is an edge the next append cannot join — its link
walks past the re-cluster to the append below — so a republication that named
the snapshot it scanned would last exactly until the next commit on a
compacted table, where the newest snapshot is usually a re-cluster. Both
rebuilds normalize.

Snapshot expiry is the other way the chain was lost. The reader's walk needs
every snapshot between the edge and current, so a re-cluster run longer than
`retain_last` dropped the edge's own snapshot and stranded the object for the
life of the table. `expire_snapshots` now decides, against the metadata it is
about to shrink, whether the edge it can still prove survives the commit; when
it would not, it re-roots the edge onto the deepest snapshot the commit leaves
in place — the same rows, no recompute, one object write — and counts
`siglake_inline_coverage_reroots_total`. The write is fenced on the object
still carrying the edge that was proven, since a publication landing in the
window has moved the edge to its own append and nothing here improves on that
(`siglake_inline_coverage_reroot_conflicts_total`). An expiry that cannot walk
to the edge leaves it alone: ancestry that is gone is never bridged, and equal
row totals are not evidence.

A publication carries counts that exist nowhere else, so a failed one is
retried with the same deltas on the delta write's budget — four attempts, 250,
500 and 750 ms apart — in both the inline and the write-behind arm. The retry
is replay-safe: the merge reads the object first and skips a publication whose
coverage links are already there, which is how a write that landed and lost its
response is told from one that never landed. A publication that spends all four
attempts increments
`siglake_side_aggregate_publish_failures_total{iceberg_namespace="<ns>",table="<table>"}`
and, where the
incremental delta path is active, writes the same `*.rebuild.json` marker a lost
delta does, so the maintenance compactor rebuilds the wide group counts. Nothing
rebuilds the inline object automatically: its time aggregates stay short of
`total-records`, and windowed `GROUP BY` on that table answers from the per-file
path until an operator runs `rebuild-time-aggregates`, which recomputes them
from committed files. `SiglakeSideAggregatePublicationLost` fires on the
counter.
The `<table-uuid>` component is what keeps that guard honest across a recreated
index: every aggregate artifact — this object, the folded wide base, the
per-commit deltas and the rebuild markers — is addressed under the UUID of the
table that wrote it, so a table recreated at the same location reads only what
its own incarnation built (see [`LIMITATIONS.md`](LIMITATIONS.md)).

**Group-count repair and limits.** The per-commit group-count delta write makes
four attempts, waiting 250, 500 and 750 ms between attempts. A write that
eventually succeeds after retry increments
`siglake_group_count_delta_write_retries_total{table="<table>"}` by the retries
it used; a write that exhausts all four attempts increments
`siglake_group_count_delta_write_failures_total{iceberg_namespace="<ns>",table="<table>"}`.
The Helm
chart's `SiglakeGroupCountDeltaRetrying` alert warns on a sustained retry rate,
the precursor. An exhausted write records a per-sequence `*.rebuild.json`
marker alongside the deltas under
`metadata/siglake-agg/<table-uuid>/siglake-agg-deltas/` (so healthy folds need
no additional listing). The maintenance compactor consumes that
marker on its next aggregate fold, rebuilds the affected table's exact maps and
bounded sketches from committed files, and deletes every marker covered by the
rebuild watermark. It increments
`siglake_group_count_auto_rebuilds_total{iceberg_namespace="<ns>",table="<table>",outcome="success|incomplete|failed"}`;
`SiglakeGroupCountDeltaLost` fires only when that automatic repair fails or
completes without restoring full coverage. Both alerts name the affected namespace and
table; the retry alert names the pod and, because its counter is not
namespaced, the table alone. A later delta does not heal the gap; the
marker-driven rebuild does.

Each maintenance pass also adds the number of deltas folded into the base to
`siglake_group_count_deltas_absorbed_total` and the number of already-absorbed
delta objects removed to `siglake_group_count_deltas_deleted_total`.

**The deficit census.** A marker covers one cause. A process killed between its
commit and its delta PUT writes neither, and a table upgraded across the
per-incarnation prefix starts a fresh aggregate at its first commit after the
upgrade; both leave an aggregate that is merely SHORT, which no later delta
heals. Every 15 minutes
(`SIGLAKE_AGG_SHORT_SCAN_INTERVAL_SECS`; `0`, `off`, `disabled` or `never`
switch it off) the same maintenance pass censuses each maintained table for
exactly that: a maintained column whose total falls short of `total-records`
where the FOLDED view — outstanding deltas included — already carries the
current generation's own contribution. It reads one number per column straight
out of the compact base, so a healthy warehouse costs milliseconds per table.

Two rules keep it from firing on work that is merely in flight or hopeless.
A commit publishes its delta after the commit, so the census waits until the
newest generation's contribution has landed in the artifact — counting the
coverage links still waiting on a missing predecessor, and admitting a
row-conserving re-cluster on top through the same bridging rule the read guard
uses. And a column the rebuild could not restore (over the cardinality cap,
unreadable in some live file) is recorded in the base object by the rebuild that
tried, then skipped until another rebuild clears the record — that column is
dropped from the base by the rebuild and re-added short by the next delta, so
without the record one unreadable column would cost a full Tier-2 rebuild every
pass.

Every verdict lands on
`siglake_group_count_short_aggregates_total{iceberg_namespace="<ns>",table="<table>",outcome="detected|repaired|incomplete|failed"}`
and a WARN line naming the columns, and `SiglakeGroupCountAggregateShort` fires
on all but `repaired`. One compactor censuses the base namespace and every
`tenant_*` namespace, each with its own `events`, so this counter and the three
beside it (`siglake_group_count_delta_write_failures_total`,
`siglake_side_aggregate_publish_failures_total`,
`siglake_group_count_auto_rebuilds_total`) carry `iceberg_namespace` as well as
`table` — the name the alert passes to `rebuild-group-counts --namespace`. It
is `iceberg_namespace` rather than `namespace` because Prometheus attaches the
Kubernetes namespace under that name and renames a colliding metric label to
`exported_namespace`. Only the default namespace's `events` series is
pre-registered at 0: a tenant namespace, an index table and a base namespace
moved off the default by `SIGLAKE_TENANT_NAMESPACE` are known only at the
increment. Rebuilding automatically is **opt-in**
(`SIGLAKE_AGG_SHORT_REPAIR=1`, `compactor.shortAggregateRepair` in the chart):
the repair is one Tier-2 query per maintained column — measured ~9 minutes per
column per 250M rows on a local filesystem, so a wide table is hours and the
compactor's 600 s watchdog cuts it (a cut repair publishes nothing and the next
pass retries). With it on, one table per pass is rebuilt
(`SIGLAKE_AGG_SHORT_REPAIR_MAX_TABLES`), because every table upgraded across the
prefix change is short at once. On a table that size the operator's
`rebuild-group-counts` remains the tool.

If the LOST log says the marker itself could not be written, or automatic
rebuild keeps failing, the operator fallback remains:

```
siglake rebuild-group-counts --namespace <ns> --table <table>
```

Both automatic and operator-triggered rebuilds use the same exact per-file
Tier-2 path as a query, record a `rebuilt_through` watermark so an old or late
delta is not folded twice, and are safe to re-run. A census rebuild rebuilds the
exact columns only, so it merges the sketch half of every delta that watermark
retires into the base first — the fold deletes those deltas rather than folding
them, and an approximate column's rows are not re-added by any later commit. They deliberately repair
only the incarnation's `siglake-agg-wide.json`, not the inline
`siglake-aggregates.json` object maintained by the commit path. A
repaired column therefore reports `served_by: "tier1_wide"` even when it is
below the 4096 inline cap, and a cold metadata cache may read and fold the wide
object instead of taking the inline shortcut. Keeping the repair wide-only
avoids a second CAS and a staleness protocol against concurrent commit-path
merges; answers remain exact and use Tier-1 once repaired.

By default the rebuild repairs only the columns the aggregate already carries.
A table created before typed columns joined the side aggregates has its typed
dimensions (`status`-like `long`/`double`/`bool` columns) in every file's footer
and in no aggregate, so `GROUP BY` on them reports `served_by: "materialized"`
for the life of the table; the plain rebuild names those columns as ones it
could add, and `--admit-typed-columns` adds them, computing each exact full-table
total from the files. **No rewrite is needed for that case.** A rewrite is
required only when some live file can serve the column from neither its footer
nor a raw-page decode — the column is missing from that file's schema, or the
file predates typed footers — and then the rebuild leaves the column absent and
says so rather than writing a partial total; compaction rewrites such files with
footers for the current column set. An admitted column is held to
`SIGLAKE_TYPED_GROUP_COUNT_CARDINALITY` (default `1024`) on its whole-table
distinct count and is reported, not written, when over it. The same knob caps
the exact group-count cardinality of typed columns admitted by inference at
write time; declared dimensions retain the table-level cap. Raising it admits
wider typed columns but also increases per-commit delta size and counting work.

The inline object's own repair is a separate command, for a separate failure —
an object whose coverage chain cannot be proven at all:

```
siglake rebuild-time-aggregates --table <table>
```

It recomputes the time buckets (one footer read per live file) and the 2-D
time×group rollup (a two-column decode of every live file whose time range
spans more than one bucket; a file contained in one bucket contributes its
group-count footer instead), replaces both, and publishes the scanned
snapshot's coverage edge at its normal form — the root of the re-cluster run
it sits on, so the next append's link joins it. A component short of
`total-records` is left absent
rather than written short, and the inline whole-table group counts are dropped
rather than certified — one coverage edge governs the object, and granting it to
maps from an unknown earlier snapshot is the unmarked overwrite the edge exists
to catch. Nothing readable is lost by that: those counts were already refused.
Re-running is a reported no-op. Unlike the wide rebuild it is NOT safe to run
against a table being ingested: a commit landing under the pass cannot be
merged, so the command retries and then exits without writing, asking for a
window with no ingest. Details in
`docs/DESIGN_inline_time_aggregate_rebuild.md`.

**Which table needs it.** The state that command repairs is reported by name.
Every 15 minutes (`SIGLAKE_INLINE_COVERAGE_SCAN_INTERVAL_SECS`, `off` to
disable) the maintenance compactor reads each maintained table's inline object
under the `agg_fold` lease and asks the read guard's own question — does its
coverage edge reach the current snapshot? — and sets
`siglake_inline_coverage_unproven{iceberg_namespace,table}` to 1 or 0 for every
table it reaches a verdict on. A repaired table clears on the next pass. The
census never rebuilds: it reads table metadata and one object per table, and
automating the repair is separate work. An object it cannot READ writes no
sample at all — a failed GET is not evidence about coverage in either direction
— and a publication still in flight (the edge does not reach current, but one of
the object's pending links does) is reported as covered, because the next commit
settles it. A table the pass no longer reaches at all — a dropped index — has its
reading zeroed, since a metric series is never removed from a live process and a
standing 1 would otherwise page until a restart.

The counter beside it, `siglake_inline_coverage_census_total`, is one increment
per completed pass. The gauge is a last observation, so a compactor that stops
censusing keeps serving its last reading; `SiglakeInlineCoverageUnproven` pairs
the two, and a pod that stopped looking leaves the alert rather than paging from
a reading nobody is refreshing. It is the one alert in this area at `critical`
severity: the two beside it name events that automatic maintenance or an
operator's `rebuild-group-counts` repairs, and this one names a state that
persists for the life of the table until a human runs a command. Answers stay
exact throughout — what is lost is Tier-1, not correctness.

**Residual attributes (WS-7).** OTLP resource/log attributes that aren't
promoted columns are preserved losslessly in a JSON-string `attributes`
column, queryable via the `attr_get(attributes, key)` UDF (values come back as
text so they `CAST` cleanly) and `LIKE`. Existing tables gain the column via
the additive `siglake migrate-schema` — see **Upgrades** below.

**Auto-promotion is the one schema mutation nobody requests.** Promoted columns
are normally declared: an operator lists `(attr_key, column, type)` triples and
the write path extracts them. The compactor can also choose them itself — it
samples a bounded slice of the newest live files every 300 s and promotes each
key that clears a frequency threshold — and that path calls the same
`declare_promotions_for`, so a sampling verdict records the promotion property
and widens the table's schema on its own. Additive widening is irreversible
(nothing in the product drops a column), which puts it at the opposite end of
the spectrum from the migration described under **Upgrades**: that one runs
because a chart upgrade or a `spec.schemaVersion` change asked for it, and the
operator's part is to observe and record the outcome, never to decide. So
auto-promotion ships off (`SIGLAKE_AUTO_PROMOTE_MIN_PCT=0`), and the whole of
what makes it safe is its bounds — a 1% threshold floor, a hard ceiling of 64
promoted columns per table, a sample bounded in files × rows, and a capped key
census. Those are specified, measured and tested in
[`DESIGN_auto_promotion_qualification.md`](DESIGN_auto_promotion_qualification.md),
with the evidence a default-on decision would need and the open items that
decision is still missing; nothing in this section changes until one is taken.

**Upgrades and schema versions.** A siglake binary declares the schema it
wants; a table records the schema it is at, under the
`siglake.schema_version.v1` table property. When the binary is newer than the
table, the columns the table lacks **cannot be written** — so rather than
accept the rows and drop the column, the write is refused, naming the column
and the remedy. Refusing is the safe direction: the rows are still in the WAL,
the client retries, and nothing is lost. Accepting would ack them, sweep the
segment, and lose the column permanently with no error anywhere.

So run the migration before the new binary serves writes:

```
siglake migrate-schema --all-tables --all-namespaces        # add the columns
siglake migrate-schema --all-tables --all-namespaces --dry-run   # or just look
```

It is additive only — it adds columns, never drops, retypes or rewrites them —
and idempotent, so re-running is free. A column it adds is queryable as soon as
the migration commits, reading null on every row already in the table; the
migration writes no data, so nothing waits on an append. That holds on
distributed reads as well: a fan-out is pinned to the coordinator's whole
serving generation — table UUID, snapshot and schema id — so a replica whose
metadata cache still predates the migration refreshes onto the pinned schema
instead of planning its shard against the narrow one (see "One generation per
fan-out" below). `--dry-run` prints
each table's recorded version alongside what the running binary declares. `--all-namespaces` matters
on any multi-tenant install: tenancy is header-based, so `events` exists once
per tenant namespace, and without it a migration reports success having
migrated exactly one of them.

Both control planes do this for you. The Helm chart runs it as a `pre-upgrade`
hook (`schemaMigration.enabled`, default on), so a failed migration stops the
rollout instead of shipping a binary whose writes will be refused. The operator
applies its migration Job and waits for it to complete **before** rolling the
workloads, and holds the rollout if it has not finished.

**Rolling back (additive changes only).** A rollback rolls back the CODE; it
never reverses a committed schema mutation. `migrate-schema` is additive —
nothing in the product drops a column — so after a rollback the table is still
wide and the older binary meets it there. That direction is supported and
regression-tested
([`crates/siglake-storage/tests/storage/schema_rollback.rs`](../crates/siglake-storage/tests/storage/schema_rollback.rs)):

- **Writes land, null-filled.** Alignment builds each write by walking the
  TABLE's field list, so a column the writer does not declare is written null
  rather than refused. Rows already carrying values keep them, and the columns
  the writer does declare are untouched. (The refusal in the other direction is
  about *losing* data; an omitted column carries none.)
- **The older binary's own migration is a no-op.** The additive diff is by
  name against the table, so a narrower declared schema adds nothing (`up to
  date`) and removes nothing. Re-running the Helm hook or the operator's Job on
  the old image does not narrow the table.
- **Reads are unaffected.** A column the reader's binary does not know about is
  simply not selected; one it does know about reads null on files written
  without it, exactly as it does for files older than the migration.
- **The recorded version does not go down.** `migrate-schema` stamps
  `siglake.schema_version.v1` to the higher of what the table already records
  and what the binary declares, so an old binary's run — which adds nothing —
  leaves the wider table reporting the shape it has, and `--dry-run
  --all-namespaces` answers "what is this table at?" the same on both sides of
  a rollback. The maximum is taken inside the Iceberg transaction, against the
  table state each commit attempt lands on, so two migration jobs running at
  once (a Helm hook and the operator's Job, or a retried Job overlapping its
  first attempt) cannot take each other's version away: a higher version
  published after this run read the table, or between a lost CAS and its retry,
  stands. That is behaviour of the binary running the migration: an image built
  before this fix (anything pre-0.1.0) still stamps its own lower constant,
  which changes no column and no write decision, and rolling forward restamps
  it.

What is out of scope of that guarantee: anything that is not an additive column
change. The pre-0.1.0 nanosecond `timestamp` contract is a refusal boundary —
`migrate-schema` refuses such a table rather than migrating it, and there is no
rollback path across it (see [`LIMITATIONS.md`](LIMITATIONS.md)). Reverting the
WS-7 `--promote-attr` *configuration* is safe: the promotion list lives on the
table and is what the write path extracts from, so a writer started without the
flag still populates the promoted column (also tested). An image predating
promoted-column writing altogether is not: it would write those columns
present-but-null, which both backfill predicates read as "already backfilled",
and once the `attr_get`→column rewrite is enabled those rows drop out of
results. Nothing in the tree qualifies a specific older image against a specific
table; that needs a live two-image run.

**Vendored Iceberg forks.** `third_party/iceberg` +
`third_party/iceberg-catalog-sql` are first-class forks: they carry the
`rewrite_files` (atomic replace) transaction action, count- and age-based
`expire_snapshots`, commit-reload elision + commit-attempt observability, S3
conditional-put primitives, and the incremental append scan Iceberg table
subscriptions need. Periodically rebased against upstream; feature
work does not block on upstream releases.

## Compaction

Compaction is continuous and coexists with sustained writes — validated
through 200 GB and 1 TB sustained-ingest rounds
(`docs/DESIGN_continuous_compaction_and_ingest.md`):

- **Leveled ladder.** Files are size-classed into levels (defaults
  L0 <128 MiB <L1 <1 GiB <L2 <8 GiB <L3); each compaction merges a bounded
  fan-in (≤64) of one level's files toward the next level's size through a
  gap-aware bin packer that never splits a time-overlapping cluster across
  bins (disjointness is the query-performance invariant). Optional per-table
  window sealing bounds output time spans.
- **Per-tier cadence.** L0 rescans every ~10 s, L1 every minute, colder levels
  hourly — the leading edge gets the attention; cold rescans don't pay
  needless manifest walks.
- **Graded backpressure.** When the sealed backlog exceeds a gate, maintenance
  yields to the drain — but doesn't stop: every Nth backlogged cycle runs ONE
  throttled merge, restricted to cheap L0 work and chosen by
  **file-count-reduction-per-byte** score, so the file count stays bounded
  under load while expensive level-ups wait for lulls. At 1 TB sustained this
  held live files to a linear ~35/hour crawl vs unbounded thousands before.
- **Write-amplification bound.** A file rewritten `max_merge_gen` times
  (default 4) retires from compaction unless it still overlaps a neighbor —
  bounding total compaction writes to ≈ gen × ingested bytes.
- **Bounded merges.** In-RAM merges are capped by decoded-size guards; larger
  bins stream through a fan-in-bounded k-way merge. Bins beyond the fan-in cap
  take the **page-bounded plan merge**: a timestamps-only read builds an RLE
  run plan, then execution walks it in row-bounded chunks (`RowSelection`
  reads against cached metadata), so decoded memory is bounded by the chunk —
  independent of fan-in — with zero intermediate write amplification, and
  near-disjoint inputs collapse to zero-copy slices (merges get cheaper as
  data ages). A streamed merge writes ONE output partition per call — its
  writer stamps the bin's first partition value on everything it writes — so a
  bin spanning two partitions is refused rather than committed under a partition
  value that hides its rows from a predicated query; the planners bin per
  partition, and an in-RAM merge splits its output by partition value and takes
  a mixed bin (see [`LIMITATIONS.md`](LIMITATIONS.md)).
  Merged output is where deferred indexes materialize, in one of
  two shapes: an in-RAM merge writes the full inline set, including the footer
  inverted indexes (with Puffin overflow) and the whole-file raw trigram
  bloom, while a streamed merge writes neither of those and leaves its output
  unindexed unless the opt-in post-commit rebuild pass
  (`SIGLAKE_INDEX_REBUILD=1`) registers Puffin sidecars for it.
  Group-count, time-bucket and row-group-bloom footers are written
  either way (see [`LIMITATIONS.md`](LIMITATIONS.md)).
  Row groups on merged output are sized in BYTES: the writer is built on the
  merge's first output batch and takes
  `SIGLAKE_PARQUET_TARGET_ROW_GROUP_BYTES` (or
  `IcebergTuning::target_row_group_bytes`, 256 MB uncompressed by default)
  divided by that batch's decoded row size, clamped to 128 Ki–4 Mi rows — the
  same sizing the flush path makes from the batch it is handed. The open row
  group is buffered decoded while its bloom accumulates, so that target is also
  what bounds a merge's, a re-cluster's and a delete rewrite's writer-side
  memory (see [`LIMITATIONS.md`](LIMITATIONS.md)).
- **Metadata hygiene on the same loop:** snapshot expiry (count + age),
  orphan-file GC with its own safety age (deliberately not the retention
  window — a file younger than the longest write-then-commit gap may be about
  to be committed), table-generic retention sweeps (file/day-granular), and
  delete-task execution — which runs by default (`SIGLAKE_DELETE_TASKS=0`, or
  `compactor.deleteTasks: false`, turns the sweep off and leaves submissions
  recorded for `siglake delete-sweep --apply`). A delete task is one warehouse
  object keyed by its
  own id (`_siglake/config/delete_tasks/<namespace>/<task_id>.json`), never a
  shared per-namespace ledger: an acknowledged deletion request cannot be
  erased by a concurrent replica's submission or by a sweep's status write.
  Ownership of a task is a second object: an executor create-only-writes
  `<task_id>.claim` beside the record before it runs the task, so two executors
  racing the same pending task produce exactly one execution and the loser
  reports it as already claimed. That claim is kept in every state, terminal
  included — a delayed executor's pending set predates its claim, so the claim
  is the only thing that stops it re-running a task that already finished (see
  [`LIMITATIONS.md`](LIMITATIONS.md)). `GET /api/v1/delete-tasks/{id}` adds a
  read-only `claim` observation for pending tasks: whether the object was seen,
  when it was observed, and — when its diagnostic body is readable — its
  claimant UUID, creation time and age. Ledgers written by earlier builds stay
  readable and are never rewritten.
  A candidate file is rewritten as "the rows the predicate is TRUE for, gone;
  everything else kept" — the survivor set is the predicate's NULL-safe
  complement (`IS NOT TRUE`), so a predicate on a nullable column keeps its
  NULL-valued rows instead of deleting or stranding them, and a row-count
  conservation check (survivors + deleted == input) fails the task rather than
  committing a rewrite that lost rows. A candidate past the in-RAM caps
  (16 MiB compressed or 128 Ki rows; `SIGLAKE_DELETE_REWRITE_INRAM_MAX_MB` /
  `SIGLAKE_DELETE_REWRITE_INRAM_MAX_ROWS`) is rewritten by a streaming pass
  instead — two bounded reads of the file, 8192 rows decoded at a time,
  survivors written straight through the rolling writer — so a cold-target
  file does not have to fit in the packaged compactor's 1Gi several times
  over. The caps are derived from that limit and are the same split
  re-clustering makes. Streamed output carries the per-file group-count,
  time-bucket and raw row-group-bloom footers; its inverted indexes come from
  the post-commit rebuild pass rather than inline, as streamed merge output's
  do, which means Puffin sidecars for the indexed columns and no whole-file
  raw trigram bloom. With the rebuild opted out the output carries no inverted
  index at all until a later in-RAM rewrite builds one; either way the effect
  is pruning, not correctness (see [`LIMITATIONS.md`](LIMITATIONS.md)).
  Each task's candidate rewrites land in
  ONE atomic commit for that task, so a failure before the commit leaves
  unreferenced output files for orphan GC, never a half-deleted snapshot — and
  a failed task is terminal: it is recovered by resubmitting the request, which
  answers with a new task id (see [`LIMITATIONS.md`](LIMITATIONS.md)).
  `siglake audit-rotate` composes them
  non-destructively. Managed indexes are swept by the same loop, which is why
  an external consumer's output tables should be ordinary indexes.

## Query

**SQL, with guardrails.** `/api/v1/sql` plans through DataFusion against the
Iceberg snapshot (+ the WAL buffer for events). Every request gets a
manifest-walk cost estimate pre-flight (bytes/rows rejection before any data
IO), per-tier ceilings clamp per-request limits, a mid-flight rows-scanned
breaker aborts runaway scans at batch boundaries, and a wall-clock timeout
backs everything.

An unknown key inside the request's `limits` object is **refused**, `422`, by
the JSON extractor — before planning, execution or batch enqueue. Every field
there is a safety constraint the caller asked for, so quietly dropping a
misspelt one and applying the tier default instead is the worst available
answer, and `{"limits":{"max_rows":5000}}` is the typo that happens: `max_rows`
is what the RESPONSE envelope calls the applied cap, while the request field is
`max_rows_returned`. There is deliberately no alias — one request spelling, and
a wrong one is loud. Only the `limits` object is strict; unknown keys at the top
level of the request body are still ignored, and a known limit above its tier
ceiling is still clamped rather than refused.

That wall-clock timeout is one budget per request, spent by preparation (WAL
delta load, result-cache probe, table registration, planning, estimation) as well as
execution, so a warm result-cache hit cannot answer a request whose clock has
already run out. A batch job's RUN gets the same one-budget contract, from
publishing `running` and table registration through the metadata fast paths to
the rendered body, with the clock starting when the run starts: the `202`
submission is deliberately outside it, so asking for a short execution budget
still gets a job id, and a job that spends that budget in preparation is failed
as `timeout` rather than starting its collect on a fresh one. The two writes a
run makes to the job store while it executes — `running` and the estimate —
draw on that budget too, so a store that has stopped answering cannot hold an
admitted job, and its admission reservation, past the budget it was given. The
terminal write is bounded as well, on a second clock started when execution
ends (three attempts within 30 s, so a failover blip costs a retry and keeps
the real result); a run that still cannot persist its verdict parks the job
with its executor and returns, because holding admitted work for the length of
an outage is not a way to end a job. Interactive requests and asynchronous
batch jobs share the same admission budget; each admitted batch job reserves a
quarter of the pod budget until it completes or is cancelled, and saturation
returns `429` with `Retry-After` before a job is created. Completed queries are
submitted whole to a best-effort `query_audit` Iceberg writer. The process
retains at most 10,000 submitted rows and 64 MiB charged across the channel,
flush buffer, owned strings and overlapping Arrow conversion; an oversized or
over-budget row is dropped without changing the query response and increments
`siglake_query_audit_dropped_total{reason}`. Each append the worker awaits is
bounded by a service deadline (30 s;
`SIGLAKE_QUERY_AUDIT_APPEND_DEADLINE_SECS`, `0` awaits without a bound), so a
storage append that stops answering costs its own batch instead of the audit
service: the deadline releases that batch's retained budget, counts its rows
under `reason="append_deadline"`, and the worker takes the rows behind it. The
abandoned batch is never re-appended — the deadline cuts the await, not the
commit that may already have landed — which is the `query_audit` table's one
source of silent row loss under a healthy process. For batch jobs,
`query_audit.duration_ms` measures the bounded run lifecycle from the moment the
queued future starts on the dedicated batch runtime; it excludes both
batch-runtime queue time and the HTTP `202` handoff. The jobs API exposes
`submitted_at`, `started_at`, and `ended_at` separately when clients need those
intervals.

**Batch jobs are owned by an executor, not by the tier.** The batch-job store
is the catalog Postgres — the database the query pods already connect to —
shared by every query replica, so each row records the replica *incarnation*
executing it. That is the default (`query.jobs.persistent: true`, and the
operator points an adopted query tier at its own `spec.catalogUri`), because
the tier it installs is two pods behind one Service with no session affinity:
a per-pod store answers `404` for the `job_id` a `202` just returned whenever
the read lands on the other pod, with nothing restarted. Setting it false, or
giving a query pod a blank `SIGLAKE_JOBS_POSTGRES_URI`, keeps the in-memory
store, which is correct at one replica and loses every in-flight job on a
restart. The chart refuses `query.jobs.persistent: false` when either its fixed
replica count or enabled KEDA ceiling exceeds one. The operator applies the
same `InvalidSpec` policy when its effective jobs-store URI is blank and
`spec.autoscaling.query.max` exceeds one; a non-blank `spec.extraEnv` override
remains a supported shared-store configuration. Owners heartbeat. Recovery
fails a `pending`/`running` row only once its owner's lease has expired. That
is the only evidence available that the executor is gone. Scaling the query
tier out, or rolling one pod, therefore
leaves the other replicas' in-flight jobs and their eventual results intact —
previously every startup failed every non-terminal row in the shared table.
The cost is latency, not correctness: a crashed replica's jobs reach `failed`
within one lease period (default 120 s) rather than the instant a pod starts,
and rows written before ownership existed are held for a day before recovery
touches them. A planned SIGINT/SIGTERM shutdown is different: after the HTTP
server drains, it abandons local batch futures, stops and joins owner upkeep,
then deletes this incarnation's registration. The next peer recovery pass can
therefore fail any remaining non-terminal rows immediately; if that final
DELETE fails, the stopped heartbeat preserves the ordinary lease-expiry
fallback. Execution ownership is separate from tenant authorization: the
`tenant` column still decides who may read a job.

**A job whose executor is alive is resolved by that executor.** Lease-scoped
recovery has one blind spot, and it is the mirror of what makes it correct: a
row this replica owns is never condemned, so a run whose execution *ended*
without a persisted terminal state — the store was unreachable for every
attempt the run could afford — left a `running` row that no sweep would ever
resolve (the TTL sweep only deletes rows carrying the `expires_at` a terminal
write sets). The executor is the one party holding positive evidence that such
a run finished, so it keeps a bounded, body-free note of those job ids — one
status each, no results and no error text — and retries the same conditional
write every 5 s (`siglake_query_jobs_reconciled_total{computed,outcome}`,
`siglake_query_jobs_unreconciled`). Nothing is re-executed and no terminal row
is ever reopened: a state that landed while the run was blind to it (an
ambiguously acknowledged write, a client cancellation, recovery) is preserved,
and a *success* whose body went with the failed write becomes an explicit
`failed` telling the client to resubmit, never a success with no answer behind
it. The note is capped at 1024 ids per replica and the cap is loud
(`siglake_query_jobs_unreconciled_dropped_total`): past it a row degrades to
the old behaviour and waits for its executor to exit, when lease-expiry
recovery reaches it. That degradation pages —
`SiglakeBatchRowStrandedNonTerminal`, on the dropped counter and on the run's
own `cause=write_abandoned` report of the same event, deduplicated to one
alert per pod. The backlog itself does not: a nonzero
`siglake_query_jobs_unreconciled` that drains again is the mechanism working.

**A batch job's row says what it is doing while it does it.** `running` and
`started_at` are published *before* the query executes, and the cost estimate
the moment planning has produced it — which is what `GET /api/v1/jobs/<id>`
documents. Both used to be written from the completed branches of the run: a
job that executed for an hour read `pending` for that hour, its `started_at`
recorded the moment execution *ended*, and a run that failed after planning —
including one refused by the pre-flight bytes ceiling, which reads that very
estimate — carried no cost at all. The two lifecycle writes are conditional on
`status IN ('pending', 'running')` like every terminal one, so publishing
progress cannot reopen a job a client cancelled or recovery condemned, and
`started_at` is only ever set, never moved.

**Cancelling a batch job crosses replicas.** `DELETE /api/v1/jobs/<id>`
persists `cancelled` on a row every replica can see, but the thing that
actually frees resources is dropping the executing future — it owns both the
admission reservation and the storage-scan cancel guard, and its abort handle
is process-local. With a shared store the replica serving the `DELETE` is
usually not the executor, so the `202` used to be a promise nobody kept: the
row went terminal while another pod kept scanning and kept a quarter of its
admission budget. Each replica now re-reads the status of its *own* in-flight
jobs every `--jobs-cancel-poll-secs` (default 2 s, a primary-key read over
this pod's in-flight ids) and aborts the ones that read `cancelled`
(`siglake_query_jobs_cancel_propagated_total`); `202` bounds when the work
stops rather than claiming it already has. Terminal transitions stay
conditional in both directions — whichever of cancellation and completion
lands first wins — and the loser is *told* it lost. Before execution, a
`running` publication that observes a remote cancellation, recovery verdict or
vanished row drops the query future unpolled, releases its admission reservation,
reports
`siglake_query_job_terminal_conflict_total{attempted,actual,cause}` and audits the
store's verdict with `attempted=running`, an execution-not-started message and
null cost. A job-store error is not proof of a terminal verdict, so that case
logs and executes as before. Every terminal write reports back which status is
now installed, so the same counter catches the other ways a run's verdict is
not the row's:
recovery having condemned it (`actual=failed`), the TTL sweep having deleted it
(`gone`), every attempt at the write failing (`actual=unknown`, with
`cause=write_deferred` when the executor is still reconciling the row and
`write_abandoned` when nothing is), and a result body too large for the
row, which `finish_succeeded` installs as `failed` and which therefore counts
as `failed` — never as the success the query in fact computed. The dashboard
graphs that conflict counter beside `siglake_query_jobs_total{outcome}` (which
counts only terminal states this executor actually installed, whether at the
end of the run or at the reconciliation pass that finally landed), and the
`SiglakeBatchCompletionRejectedByRecovery` alert selects only `cause=recovery`,
covering both a refused start and a refused completion:
client cancellation and TTL expiry are expected conflicts and do not page.
`write_abandoned` pages under `SiglakeBatchRowStrandedNonTerminal` instead —
that row has nobody retrying it — while `write_deferred` does not, because its
executor is still reconciling it.

**Read-only, enforced.** Every SQL entry point — `/api/v1/sql`, `/local`,
`/shard`, `/distributed`, `/explain`, `dry_run` and the batch tier — plans
client SQL with DDL, DML and session statements refused. `COPY … TO`,
`CREATE [EXTERNAL] TABLE`, `CREATE VIEW`, `DROP`, `INSERT` and `SET` answer
`400`, and the refusal lands during plan verification, before the statement
can take effect: DataFusion executes DDL eagerly inside its planning call, so
a permissive default would have run it before the cost estimate, before
admission and before `dry_run` returned. The same options cover every other
string built from request input: the compactor's persisted delete-task
predicate and the Jaeger trace routes, whose `WHERE` clauses are assembled
from `service`, `operation`, tag and duration parameters. Writes reach the
lake only through ingestion and the operator's own jobs.

**Aggregate fast paths (zero-scan).** Whole-table group-bys, date histograms,
windowed counts, windowed group-bys, and negation counts are served from the
side-object aggregates and per-file footers — `rows_scanned: 0`, typically
single-digit milliseconds warm — with core-plus-boundary decomposition for
arbitrary windows and strict validity guards before any fast path is trusted.

**Ordered early-stop (WS-3).** `ORDER BY timestamp … LIMIT n` — plain or
windowed — early-stops without a blocking sort: the scan advertises the
table's declared direction when partitions are single files or time-disjoint
runs, and k-way-merges overlapping partitions (bounded fan-in) otherwise.
Direction-aware for legacy DESC tables; observable via
`siglake_query_scan_output_ordering_total`.

**Implicit newest-first.** An interactive `SELECT` that names one table and
asks for no ordering of its own is given `ORDER BY timestamp DESC` — the
browse a log reader means when they write `SELECT timestamp, raw FROM t LIMIT
100` — which is also what puts the query on the ordered early-stop path above.
It applies to `events`, `query_audit` and to any managed index whose doc
mapping declares `timestamp` as its `timestamp_field`; an index that names
some other event-time field is left alone, since a column called `timestamp`
there need not be a time order (the same reason such an index gets no
`timestamp_ns` sort tiebreak). An explicit `ORDER BY`, a `GROUP BY`, an
aggregate, a CTE, a join, `DISTINCT`, `EXPLAIN` and the batch tier are all
left exactly as written. So is a projection that gives another column the
output name `timestamp` (`SELECT raw AS timestamp FROM t LIMIT 2`): SQL
resolves the injected bare identifier to that output name, which would order
the browse by the aliased column, and the source column cannot be named around
the alias — DataFusion rejects `ORDER BY t.timestamp` under such a projection
as an ambiguous reference. An explicit `LIMIT` is preserved, and a query with no
`LIMIT` gets `max_rows_returned + 1` so the truncation signal still fires.
`default_order: false` on the request turns it off. Counted by
`siglake_query_default_order_applied_total`.

**Distributed by default.** Query replicas form a StatefulSet; the classifier
splits eligible plans into per-shard scans (`ScanShard` file sharding),
workers execute `/api/v1/sql/shard` over Arrow IPC, and the coordinator
two-phase-merges — including distributed ordered scans (shards sort+limit,
coordinator merge-sorts). Mergeable aggregates (`count`, `sum`, `min`, `max`,
with or without `GROUP BY`) under a top-level `ORDER BY [LIMIT n]` also
distribute; only non-mergeable aggregates (`DISTINCT`, `avg`, and aggregates
over subqueries) and other classifier fallbacks stay single-pod. Whether the
one referenced table is a managed index at all — the gate a fan-out passes
before anything is dispatched — is answered from the catalog row plus the
bounded-staleness table-metadata cache, never an uncached `load_table`: that is
the per-query metadata.json read table registration was already changed to
avoid, and a gate holding it merely moved the same read one step earlier in the
request. A worker
fragment carries the same one-budget-per-request rule as the local path,
anchored when the shard request arrives: preparation (tenant resolution, table
registration, generation pin, planning) spends the same clock as the scan, so a
shard cannot outlive the coordinator waiting on it, and one that spends its
budget preparing is refused with `504` before the scan starts.

**One generation per fan-out.** File sharding only partitions the table when
every worker enumerates the same file generation, so the coordinator pins each
shard request to *its* serving generation and the worker time-travels to it.
The pin names the snapshot id, the Iceberg schema id the coordinator planned
against, and the table UUID. The schema id matters because an
additive `migrate-schema` commits no data snapshot — the schema id moves while
the snapshot id stands still — so a snapshot-only pin was satisfied by a
replica that predated the migration, which then planned its shard against the
narrow column set. The UUID distinguishes a dropped managed index from a new
snapshotless index under the same name; both can have no snapshot and schema id
0. A worker resolves the pinned generation against its own
metadata, refreshing once if it lags, and serves a *historical* schema out of
the metadata it retains, so a replica that is ahead of the coordinator answers
the generation asked for rather than refusing. A worker that cannot resolve
any part — a snapshot expired from the catalog, a schema unknown, a replaced
table incarnation, or one still not visible after a metadata refresh — refuses
the fragment with `503` +
`Retry-After` (`reason: "shard_pin_unresolved"`, with the pin in the body)
instead of answering from its own current generation, and the coordinator
forwards the refusal rather than re-running the fragment on itself. A
distributed answer is therefore either from one generation or absent; it is
never a merge across generations. Misses are visible on
`siglake_query_shard_pin_total{outcome="miss"}`.

The pin's schema id and table UUID are optional on the wire, so a mixed-version
rollout stays compatible in both directions. A newer worker enforces the fields
an older coordinator supplies. An older worker ignores unknown fields and
therefore cannot enforce the newer identity parts, so the cluster keeps each
pre-upgrade exposure until every worker supports that field.

**One membership per query.** Peers are discovered, not rendered: each pod
re-resolves the headless Service's `_http._tcp` SRV record (Ready endpoints
only) and publishes a normalized, ordered membership snapshot, so every replica
the autoscaler adds receives shard work once its readiness probe passes — no
rollout, and no ceiling tied to the replica count that was rendered. A query
captures ONE snapshot and uses it for the shard count `N`, the shard→peer map,
the WAL-partial branch, every primary and failover request, and its
attribution. Membership refreshes publish a new snapshot and never mutate a
captured one, so a pod that joins or leaves mid-query can neither duplicate nor
omit a shard: the file predicate stays `fnv1a(path) % N == i`, a partition of
the file set for any fixed `N`. A departed peer's shard is retried once, with
its original `(i, N)` and generation pin, on the snapshot's explicit coordinator
URL — never `peers[0]`, which is this pod only on ordinal zero. A resolver
error or an empty answer retains the last known good membership rather than
shrinking the cluster on a DNS blip; before the first usable answer the pod
answers locally. Observable on
`siglake_query_peer_discovery_{members,refresh_total,last_success_seconds}`,
with `SiglakeQueryPeerDiscoveryStalled` for a pod that never resolves one.
Static `--query-peers` remains the non-Kubernetes compatibility mode (its
contract is that the coordinator is peer zero); configuring both is refused.

**Caches.** Per-file footer cache, snapshot-keyed aggregate + windowed-result
caches, and a live-file cache holding both manifest-stat records and planned
Tier-2 scan tasks — all keyed by `(table, snapshot, …)` and invalidated on
commit. A text query's per-file inverted index is cached on both sides of its
decode: the Puffin blob bytes and the parsed index, under the write-once
identity each came from — `(statistics path, blob offset)` for a Puffin blob,
`(data file, column)` for one carried in the Parquet footer — so neither can go
stale and a repeated query over a warm file does not deserialize its whole term
dictionary again, whichever way the file stores its index. The decode is
measured at 33 ns per indexed row, 257 ms for a 7.3M-row file; over 2M rows in
28 footer-indexed files, dropping the repeat took a warm `LIKE` query from
73 ms to 12 ms. The parsed side is what a warm query is
served from, and both storage shapes share its budget and its LRU. It is
bounded by
`SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES` (1/16 of the pod's memory limit, capped
at 1 GiB; a parsed index costs about 40 bytes per indexed row) as
well as by the blob cache's
entry count, evicts least-recently-used, hands out a shared reference rather
than a copy, and takes no index-load permit on a hit, so warm files neither
re-decode nor queue behind `SIGLAKE_INDEX_LOAD_CONCURRENCY` (4). The blob side
is what a parsed eviction falls back on: holding the serialized form saves the
re-fetch but not the re-parse, so it is sized to cover about the same files as
the parsed budget — `SIGLAKE_PUFFIN_BLOB_CACHE_MAX_BYTES` (1/64 of the limit,
capped at 256 MiB, a blob being roughly a quarter of its parsed size) and
`SIGLAKE_PUFFIN_BLOB_CACHE_MAX_ENTRIES` (128), whichever binds first, with a
blob larger than the whole budget left uncached rather than evicting the
entries that fit. Both bounds apply to every entry, so a per-file index sized
by its row count cannot push the cache past the byte ceiling the way the entry
count alone allowed. Setting the entry count to `0` turns both caches off and
returns to fetching and deserializing per query; setting the blob byte bound to
`0` drops only the serialized copy. Both budgets are subtracted from the query
memory pool like every other read cache and published on
`siglake_cache_budget_bytes{kind="text_index"}`.

The experimental decoded-file cache
(`SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_{BYTES,ENTRIES}`, both `0` everywhere the
project packages) is the one read cache whose entry is a whole file's decoded
batches, which is why it fills only from a scan that reads a file to its end and
returns nothing to a log UI's browses (#4494, [`LIMITATIONS.md`](LIMITATIONS.md)).
An entry is keyed by file and projection and holds rows read under no predicate,
so that any later query can reuse it. A task that carries a converted predicate
therefore bypasses population rather than be read with its predicate stripped,
which cost 2.8x a cache-disabled read on a page-prunable browse (#4891); it can
still be served from an entry a predicate-free scan left, and the provider
declares exact-capable filters `Inexact` while the cache is on, so the residual
filter re-applies the predicate to those rows.
Per-row-group population, which a clipped read can leave behind, is qualified
against that policy and against no cache at all in
`docs/DESIGN_row_group_decoded_cache_qualification.md` — a local prototype
reachable only in-process, with the recorded disposition and what would change
it, so nothing in this section changes until it is adopted.

**Which budget a process gets is its role.** The query server derives both from
its pod's limit. The `siglake` binary resolves its own at startup, and for the
maintenance roles — the compactor pod, the ingest server, the sweeps and the
rebuilds — that is an explicit zero: the only site that fills either cache is
the scan's index-pruning path, reached from a plan carrying a text predicate
through the Iceberg table provider, and maintenance plans none. A Tier-2
aggregate rebuild counts from manifest stats, Parquet footers and raw pages; a
delete task evaluates its predicate over a `MemTable` of the candidate file's
decoded rows. The flat 1 GiB + 256 MiB pair those processes used to inherit from
the fork was 1.25 GiB of ceilings on caches that never take an entry, inside the
1Gi the chart gives the compactor. The subcommands that do run SQL in process —
`sql-direct`, `iceberg-demo`, `subscribe` — derive what the query server would.
The byte-range object cache stays the opt-in it has always been
(`SIGLAKE_OBJECT_CACHE_BYTES`) in every role, and is now recorded rather than
re-derived: a process that configured nothing left the pool subtracting a
quarter of the pod for a cache that process had switched off. Every override
survives the role, in both directions.

What the parsed side is doing
under those budgets is readable per query rather than inferred from latency:
`siglake_iceberg_parsed_index_cache_lookups_total{outcome,storage}` records one
`hit` or `miss` per file a text query acquires an index for, and
`siglake_iceberg_parsed_index_cache_evictions_total{reason}` names the bound
that dropped an entry — `byte_bound`, `entry_bound`, or `oversized` for an index
that alone exceeds the budget and is therefore never admitted at all. A hit
ratio cannot separate a first read from an entry this cache decoded and threw
away, which is the difference between a cold plan and a thrashing one; the
resident set is charted against its bound on
`siglake_iceberg_parsed_index_cache_bytes` and
`siglake_iceberg_parsed_index_cache_max_bytes`, both published where the bounds
are enforced and therefore absent until the pod's first indexed text query.
The startup cost itself is split by stage on
`siglake_iceberg_text_index_startup_seconds{stage,storage}`: `permit_wait` for
the load semaphore, `blob_fetch` for the Puffin read, `decode` for
`InvertedIndex::from_bytes` and `selection` for the postings lookup plus the
row-selection runs. `decode` is recorded only on a miss and `selection` on
every file the index prunes, so the two sample counts together say how much of
a plan started warm — run #73 could not tell those four apart from a round's
artifacts, which is what the split is for. The "Text-index startup" panels of
`deploy/grafana/siglake-overview.json` read all of it, and the two counters are
pre-registered at 0 on the query server so a tier serving no text query charts
zero rather than no data. They are also the last claim
on the limit: the derivation gives them only what is left once the pool can
still reserve one compacted file's decode working set, so the packaged 4Gi pod
— where that reservation is the whole remainder — caches no text indexes unless
the two knobs are set explicitly, a 5Gi pod holds 400 MiB of them, and a 16Gi
pod reaches the caps. Without a cgroup limit to read — bare metal, an unlimited
container — both fall back to the 1 GiB and 256 MiB constants. The
table-metadata cache underneath them is served stale-while-
revalidate (bounded by a hard staleness ceiling), but it only ever moves
FORWARD: reloading a busy table's metadata takes seconds, so a commit — or a
second reload — can finish inside that window, and a reload that publishes
afterwards would put pre-commit metadata back for a full TTL and let a
result-cache body built from the older generation be stored under the newer
snapshot's key. Publication is therefore fenced by a per-table epoch that
invalidation and every published reload advance; a reload that loses the race
serves what is cached instead of overwriting it. Recency is decided by that
epoch, never by comparing Iceberg snapshot ids, which are opaque. Losing the
race is counted as
`siglake_iceberg_table_cache_fenced_total{action="superseded"|"reload"}` and is
healthy contention; a reload that runs out of attempts is counted as
`action="unpublished"` and leaves that table's metadata cache EMPTY, so every
read pays a full `load_table` until one publishes. All three series are
pre-registered at 0 on the query server and the compactor, and a sustained
`unpublished` rate fires `SiglakeTableCacheUnpublished`. The counter carries no
`table` label — index-table names are only known at the increment, so the
series could not be pre-registered and the first fence would be invisible — so
the exhausted-attempts case also logs at WARN with `table=` and `attempts=`,
which is how an operator turns the per-pod alert into a table name. The healthy
arms stay at DEBUG. A refresh reuses the cached (expensive) provider only while
the table is the same incarnation: the table UUID is compared alongside the
snapshot id, schema id and retained-snapshot count. Deleting an index and
recreating it under the same name matches on the other three — both
incarnations are empty — and a replica that did not run the delete hears about
it only through an ordinary refresh, so without the UUID it would keep serving
the deleted table's columns and renew their TTL for the life of the process.
The table identity in a SQL result key is the whole serving **generation** —
the current snapshot id and the current schema id, read from one metadata
entry. An additive `migrate-schema` commits a schema update and no data
snapshot, so it widens the served column set while the snapshot stands still;
with the snapshot alone in the key, a warm filtered `SELECT *` replayed the
pre-migration columns until the next data commit. The scan resolves its
projection against that same current schema, so a column added by a migration
reads back — null on every row written before the widen — without waiting for
an append. The SQL result cache keys the **serving mode** alongside the
generation: request limits, exactness (`exact: true` is never served a warmed
approximate top-K) and shard scope (`shard` restricts the scan to a subset of
the file set), so an entry is only ever shared between requests owed the same
answer.
Which queries are eligible — filtered scans, aggregates and ordered `LIMIT`
browses, but not bare unordered scans — and whether a query is a pure function
of `(table, snapshot, query)` at all are both decided on the *parsed
statement*, never on the SQL text: cacheability does not depend on how the
client formats its SQL, and a pretty-printed query caches exactly like its
single-line twin. Purity is decided by the declared volatility of every
function call in that statement, read from the planner's own registry, and the
refusals are counted apart on
`siglake_query_sql_result_cache_requests_total{outcome}` because they are
different operational facts: `skip_time_dependent` is a registered STABLE or
VOLATILE function (`now()`, `random()`) and is ordinary dashboard traffic,
`skip_unclassifiable` is a function name the registry does not hold — the
conservative direction of that classifier, on a query that may plan and succeed
anyway, so a rising rate is this deployment losing cache coverage — and
`skip_unparseable` is a statement this classifier's own parser rejected and
should sit at ~zero. Uncacheable *shapes* are not counted at all. The **SQL
result cache by outcome** panel of `deploy/grafana/siglake-overview.json` is
where the three are read against the working `hit`/`miss`/`insert` arms.
It also serves and stores only while the WAL buffer is *proven* to contribute
nothing to the query's tables: buffered rows change results without changing
the snapshot, so rows in flight — or a buffer read that failed, or one refused
for being past the decode budget, and therefore proved nothing — take the
request off the cache entirely, both lookup and insert. A refusal in
particular is not emptiness: the fast paths price the whole buffer while a
scan prices only the segments the query's window keeps, so during backlog
recovery a windowed query is served a union the refusing gate never saw. Those
requests recompute; the fast paths keep serving their committed-only answers.
The proof is also *re-taken at insert time*, over the segment set it was
originally established on: the scan lists the WAL directory afresh (that is how
a `timestamp` predicate prunes segments by header), so a segment sealing between
the proof and the execution lands in the body. An insert whose witnessed segment
set has moved — or can no longer be read — is dropped rather than stored under a
key that asserts an empty WAL.
Its byte budget is charged on what an entry **retains**, not on what it
encodes. A stored row is a `serde_json::Map`, i.e. a `BTreeMap` node allocated
whole — eleven key/value slots, 632 bytes — however few columns fill it, so a
one-column row holds ~670 bytes of heap to carry ~26 bytes of JSON. Charging
the encoding, which is what the store did until it was measured, made
`siglake_query_sql_result_cache_bytes` under-report its own footprint by 29–35x
on that shape and let a 4 MiB budget retain 21–132 MiB, none of it inside the
query memory pool. The gauge and the 4 MiB cap now describe heap: the retained
figure is exact for any row of at most eleven columns and modelled — erring
high, calibrated against a tracking allocator — for wider ones. The practical
effect is that the byte budget binds where the 256-entry cap used to: about 48
entries of a 128-row one-column result, not 1,400 of them.
That figure counts the **key** as well as the body. A key carries the query's
normalized SQL, so it is the part of an entry the client sizes: 256 distinct
64 KiB queries hold 16 MiB of key text however small their answers are. The
store used to keep that text three times over — once in the map, once in the
recency queue, once more per hit until the queue was compacted — and charge
none of it. A key is now one `Arc<str>`, shared by the map entry and by every
recency marker, charged once with the entry and given back when that entry is
evicted; a marker is a pointer, charged as one, and compaction holds the queue
at twice the live entries, so the whole recency queue costs about 8 KiB at the
entry cap instead of a second copy of every key in it. The count of markers was
always bounded — the copies behind them were the unbilled part, and a hot key
hit ten thousand times now adds 16 bytes of pointer to the store, not 64 KiB of
text per compaction cycle. The per-entry allowance prices the key too, which
keeps the property the byte gate exists for: one entry never retains more than
one caller's allowance, so a megabyte of SQL under a hundred-byte answer is
refused entry rather than admitted and then evicting everything else. What is
left outside the figure is the hash table's own spare capacity and the queue's,
single-digit KiB at the entry cap. A hit hands out
the stored body behind an `Arc`, so what happens inside the process-wide cache
mutex is a hash lookup and a refcount bump rather than the 17–139 µs deep copy
of a `serde_json::Value` tree that every other probe on the pod used to queue
behind.
The two **Jaeger name lists** (`…/api/services` and
`…/api/services/{service}/operations`) reach that same store rather than a
second one, so there is one byte budget, one single-flight map and one
`outcome` counter for both surfaces. Only the *eligibility* and the *key* are
theirs: the routes serve two fixed `SELECT DISTINCT`s with nothing
time-dependent in them and register the Iceberg provider alone (never a union
with the WAL buffer), so a list is committed-data-only by construction. The key
cannot be the query text the way SQL's is — every index is registered under the
one `traces` alias, so two indexes in two namespaces issue byte-identical SQL —
and is instead the RESOLVED `namespace.index_id`, the snapshot the registered
provider serves (reported by the registration itself, so a background refresh
cannot file this snapshot's answer under the next one's key), which list, which
service, and the ceilings the answer was rendered under. Its *entry* is theirs
too: eligibility is a **byte** allowance of 512 KiB — an eighth of the shared
budget — over an entry that holds the names as one concatenated buffer plus a
`u32` offset each, rather than SQL's 128 rows over a rendered result. A row
count prices this shape wrong by two orders of magnitude: a one-column row of
an 8-byte name retains ~670 bytes as a `serde_json::Map`, so an 800-name list
is 530 KiB rendered against 9 KiB as an arena, and the 800 the row cap refused
were 0.4% of the byte budget that refused them while every poll re-ran a
17–41 ms full-table aggregate. Rows are bounded upstream instead, by the
`ceilings.names` already in the key (10,237 on a packaged pod, enforced
mid-flight), so 512 KiB admits the whole render ceiling whenever names average
under 47 characters. A list past the allowance is simply not stored — never
truncated to fit, and never a reason to raise caps that bound SQL too — and a
hit is a complete answer that already passed the same ceilings, so `413`,
`429`, `503` and `504` behave exactly as they do on a miss.
**Standing invariant: result caches are
snapshot-keyed, never TTL-expired** — a TTL'd result cache silently serves
stale leading-edge answers and is prohibited.

## Consuming segments (external pipelines)

siglake writes every accepted event to a write-ahead log before anything else
touches it. `siglake_wal::consumer::SegmentConsumer` is the supported way for a
**separate process** to read that stream — a detector, a router, a mirror into
another system — with a durable cursor, at-least-once delivery, reads that
survive the compactor's renames, and **retention that waits for you** until you
fall too far behind.

Four calls: `open`, `poll`, `read`, `commit` — committed after you process, which
is the whole contract. See **[docs/CONSUMING_SEGMENTS.md](CONSUMING_SEGMENTS.md)**.

This interface is not speculative. A four-tier semantic detection pipeline
(streaming detectors → episode correlation → webhook dispatch) shipped *inside*
siglake until 2026-08-29 and was moved out to run entirely on top of it,
consuming the WAL through these four calls and nothing else. That pipeline is
maintained as the reference consumer deliberately: if the interface cannot build
it, the interface is wrong.

`siglake subscribe` remains for tailing an Iceberg *table* (committed rows) — it
delivers only what append commits added, so continuous compaction never replays
rows a consumer already has — and `/api/v1/stream` is the SSE live tail on the
ingest path.

## Multi-tenancy

Tenants route to per-tenant WAL subdirectories and per-tenant Iceberg
namespaces (`tenant_<id>`); the default tenant maps to the main namespace, and
namespaces auto-create on first use. WAL consumers run per tenant directory
(`<wal>/<tenant>/`). User indexes are per-tenant tables with their own doc
mappings and retention. Rate budgets are per tenant only where
`trustScopeHeader` is on (optionally Redis-shared across replicas): the ingest
limiter runs ahead of authentication, so with `oidc.tenantClaim` alone the
budget is per bearer token, then per first `X-Forwarded-For` address, then one
`anonymous` bucket.

**Multi-tenant routing is something you turn on.** Both boundaries are
single-tenant by default: the query server routes every caller to the default
namespace, and ingest routes every request to the `default` tenant. An
`X-Scope-OrgID` naming another tenant is refused with `403` — on HTTP and on
OTLP/gRPC alike — rather than honoured or quietly ignored, because a client
that sends the header believes it is getting isolation and has to be told it is
not. Naming `default` is a no-op, so a client that always sends the header
keeps working.

Two settings route tenants, and you pick one:

- `ingester.oidc.tenantClaim` / `query.oidc.tenantClaim` — the tenant comes
  from the caller's verified JWT. This is the setting for a shared cluster: the
  claim is the authority on both transports, a header may only agree with it,
  and a token carrying no usable claim is refused.
- `ingester.trustScopeHeader` — the header selects the tenant, on the client's
  word. Defensible where a gateway in front of the ingester sets the header
  itself and strips the client's; on anything else it means any accepted
  credential can write as any tenant.

Ingest header tenancy was the DEFAULT until 2026-09-11, in every configuration
without a tenant claim, and the OTLP/gRPC exporters never ran the claim check
at all — so turning OIDC on hardened the HTTP port and left gRPC accepting
unauthenticated exports into any named tenant. Both are fixed: one boundary now
decides tenancy for every ingest transport.

**A tenant claim needs an issuer and an audience with it.** The tenant comes
from a verified token, so with no OIDC verifier configured there is nothing to
take it from. Both boundaries refuse to start on a claim set by itself — with
static tokens, with open auth, or with `trustScopeHeader` still routing on the
header — rather than accept the option and leave tenancy to whatever the
request carries. The chart refuses the same shapes a release earlier, at render
time: on each enabled tier `oidc.issuer` and `oidc.audience` come as a pair and
`oidc.tenantClaim` needs both, or `helm install` fails. It used to emit the
whole block only when the pair was complete, so an incomplete one vanished into
a tier running on static tokens or open — and, because the variables were never
rendered, the binaries' own refusals never saw it either.

A blank value is not a claim, on either boundary. `SIGLAKE_OIDC_TENANT_CLAIM=`
is how a shared `extraEnv` turns the option off against an entry the operator
already renders, so both binaries trim the value and read empty as unset: the
tier starts single-tenant instead of one refusing to start while the other
runs. A whitespace-only name never reaches the verifier as a claim name no
token can carry.

**Configuring a tenant claim makes it mandatory, on both boundaries.** Once
`--oidc-tenant-claim` is set, a verified token whose claim is missing, blank,
not a string, longer than 128 characters, or outside `[A-Za-z0-9_-]` is
refused with `403` before the request is routed anywhere — token validity is
not tenant authorization. Identifiers are validated, never repaired: `acme.corp`
is a refusal rather than a rewrite to `acmecorp`, so two claims cannot alias
onto one namespace and an all-invalid claim cannot fall back to the default
one. Refusals are counted by `siglake_query_tenant_denied_total` and
`siglake_ingest_tenant_denied_total` (`reason="claim_missing"` /
`"claim_invalid"` / `"header_mismatch"` / `"header_not_trusted"` /
`"not_allowed"` / `"at_capacity"`).

**Bounding what a header can create.** The tenant and index headers each mint a
backpressure lane (holding an open file), metric label values, and an Iceberg
namespace. `ingester.allowedTenants` bounds that when the tenant set is known —
checked against the tenant actually resolved, so it bounds a JWT claim as well
as a trusted header; `ingester.maxTenants` and `ingester.maxLanes` are the
backstop when it is not. All default to unbounded.

`ingester.maxTenants` counts distinct resolved tenants, not `(tenant, index)`
lanes — one tenant writing to twelve indexes is one tenant, and
`ingester.maxLanes` is the bound on the cross product. At the cap the tenants
already admitted keep writing; a novel one is refused with `403` (gRPC
`PermissionDenied`) before a writer opens, a directory is made or a row is
published, counted under `reason="at_capacity"`. The count is per ingester and
per process: it starts empty on restart, and each pod holds its own, so a
2-pod ingester with `maxTenants: 100` admits up to 100 tenants per pod. **This
cap was inert until #4240** — parsed, passed to the ingester and never read, so
an operator whose only bound was `maxTenants` had none.

## Observability (OpenTelemetry emission)

Metrics and emission are separate paths, on purpose.

**Metrics stay on Prometheus.** Every binary exports its counters, gauges and
histograms through the `metrics` crate to a `/metrics` endpoint
(`siglake_core::metrics::init`), which is what the chart's `ServiceMonitor`,
the `PrometheusRule` alerts, the KEDA scalers and the Grafana dashboard read.
No call site changed when OTel arrived. A collector with a Prometheus receiver
is how these reach an OTLP backend.

**Logs and traces leave as OTLP/HTTP, when configured.** `siglake_core::
telemetry::init` installs the process's `tracing` subscriber: the console
layer always, plus — when an endpoint is configured — an OTel logs bridge and
an OTel traces layer. The logs bridge forwards existing `tracing::info!` and
friends as OTLP log records, so no logging call site changed either. The
traces layer forwards the spans placed at the boundaries that cost something:
the ingest handlers and `ingest_batch`, the compactor drain, the query
server's per-request middleware and `distributed_inner`.

**A distributed query is one trace.** The coordinator injects W3C
`traceparent` into each shard request and the worker's middleware extracts it,
so a fan-out's worker spans are children of the coordinator's span rather than
unrelated roots. `crates/siglake-query-server/tests/otel_traceparent_propagation.rs`
pins that over a real socket.

**Configuration is the standard OTel environment, and it is off by default.**

| Variable | Effect |
| --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | The switch. Unset or empty ⇒ no OTel emission at all: console logs only, exactly as before. Set (e.g. `http://otel-collector:4318`) ⇒ logs and traces export over OTLP/HTTP. |
| `SIGLAKE_OTEL_DISABLED` | `1`/`true` turns emission off without removing the endpoint. |
| `OTEL_TRACES_EXPORTER`, `OTEL_LOGS_EXPORTER` | `none`/`off` drops that one signal; the other keeps exporting. |
| `OTEL_SERVICE_NAME` | Defaults to `siglake-<component>` (`ingest`, `compactor`, `query`, `operator`, `cli`), derived from the binary and its subcommand. |
| `OTEL_SERVICE_NAMESPACE`, `OTEL_RESOURCE_ATTRIBUTES`, `HOSTNAME`/`HOST`, `POD_NAME` | Resource attributes: `service.namespace`, free-form `k=v,k=v`, `host.name`, `service.instance.id`. |
| `OTEL_EXPORTER_OTLP_HEADERS` | `k=v,k=v` exporter headers (an API key for a hosted backend). |
| `RUST_LOG` | Unchanged: it drives the console layer and the log bridge alike. |

Every one of these is read once, at startup, through pure resolvers that the
tests drive directly; nothing mutates the process environment.

**Console output stays on stderr.** stdout belongs to the reports:
`siglake-operator --print-crd` (which CI diffs), `migrate-schema --dry-run`,
`gc-orphans`, the SQL client's rows.

**Shutdown is explicit, because nothing else flushes.** The batch processors
buffer, and the providers live in a `OnceLock` that never drops, so a
drop-at-exit guard would ship nothing. Each binary's `main` initializes
telemetry and then wraps a `run()`, so one `telemetry::shutdown()` covers the
graceful SIGTERM return, one-shot commands and errors after initialization.

**The disabled path is cheap, not free.** The per-request middleware runs
whatever the configuration: it allocates the request path, asks the global
propagator to extract, and creates a span no subscriber is listening to.
Measured with `cargo test --release -p siglake-query-server --lib
otel_disabled_path_cost -- --ignored --nocapture` (5 interleaved pairs of 2000
`/healthz`-shaped requests): **+393 ns/request against no layer at all, and
+104 ns/request against the `TraceLayer` it replaced**. `--release` is part of
the recipe — the default test profile reports the same residue as +6.6 us and
+2.6 us, which measures the profile. Against a SQL query's milliseconds this is
noise; against an empty request it is most of the cost.

## Deployment

- **Helm** (`deploy/helm/siglake`): all roles; query as a headless-service
  StatefulSet with `query.distributed.enabled` default-on; KEDA `ScaledObject`s (`keda.enabled`) scale ingester and query on saturation
  signals with anti-flap (since #967 `keda.query.maxReplicas` may exceed
  `query.replicas`: every ready replica joins the SRV membership and receives
  shard work); graceful scale-down via SIGTERM force-seal +
  `preStop` grace; opt-in PDBs, anti-affinity, NetworkPolicies, and
  External Secrets integration.
- **Operator** (`deploy/helm/siglake-operator` + the `siglake-operator`
  crate): `SiglakeCluster` CRD rendering the data-plane tiers with the
  same workload kinds as the chart (ingester and compactor as Deployments,
  query as a StatefulSet), plus the WAL PVC,
  Services, retention CronJobs and a one-shot schema-migration Job; health
  probes and lifecycle match the chart. **The chart remains the supported
  install surface**: the operator does not render Ingress, PDBs,
  NetworkPolicies, ServiceMonitors, HPA/KEDA objects or scheduling
  constraints (per-tier resources default to the chart's and are overridable
  through `spec.resources`), and it cannot express the query tier's bearer
  tokens, either tier's OIDC, the ingester's tenant routing and admission
  bounds, TLS or the WAL-buffer volume — `ingester.auth.existingSecret`
  (as `spec.authTokensSecretRef`) is the one authentication setting it
  carries. Catalog credentials are passed as
  a plain `spec.catalogUri` and appear in pod env and in CronJob argv; the
  chart composes them from a Secret instead. Adoption of an existing Helm
  release (`--adopt-values`) is **experimental** — it has no live test
  coverage and several values keys are not yet mapped. The preflight reports
  the ones that would change behaviour at cutover: a
  `wal.existingClaim` that differs from the `{cr-name}-wal` claim the operator
  mounts (the adopted pods would start on another, probably empty WAL while
  anything not yet committed to Iceberg stays on the release's claim), and
  each configured authentication or tenant control above, with the tier it
  belongs to and how to keep it (`deploy/helm/siglake-operator/README.md`).
  The report is a single applicable manifest — findings and runbook are
  comments — and `--adopt-namespace` (default: the release name) sets both
  `metadata.namespace` and the `-n` on every runbook command, so a release
  installed into a namespace that is not its name adopts into the right one.
- **Terraform/EKS BYOC** (`deploy/terraform`): EKS + EFS (RWX WAL) + RDS
  (catalog) + S3 (warehouse), validated across the AWS smoke rounds; Grafana
  overview dashboard in `deploy/grafana/`.
- **Local dev:** `scripts/up.sh` brings up Postgres + MinIO + ingester +
  compactor + Prometheus; after `cargo build --release -p siglake-loadgen`,
  `scripts/loadgen.sh` sustains 5 k+ EPS on a laptop;
  `scripts/smoke.sh` validates correctness; `scripts/kind-*.sh` runs
  the chart in kind. Single-process demo: `siglake ingest-server
  --with-compactor`, POST OTLP to `/v1/logs`, then `siglake sql`.

## Diagnostics

### The metrics port is node-local, and nothing on it is authenticated

Every role serves `--metrics-bind` (9100 for the ingester, 9101 for the
compactor, 9105 for the query tier): `/metrics` for Prometheus, `/` as a
one-line pointer to it, and nothing else in a release build. The compactor's
liveness probe is a `tcpSocket` against it. None of it checks a token — the
query tier's bearer tokens and OIDC guard 8089, not this — so **treat the
metrics port as an internal control surface and do not route it through an
Ingress or a LoadBalancer.**

The chart's `networkPolicy.enabled` writes an **egress** policy only; there is
no shipped ingress restriction on the metrics port, so the reachability you get
is whatever your cluster's default is. A cluster that allows pod-to-pod traffic
allows scrapes from anywhere in it. Restricting it further is an operator
decision: your own `NetworkPolicy` admitting only the Prometheus
ServiceAccount's pods, or no policy and `kubectl port-forward` for ad-hoc
reads. On the AWS bench stack the security group opens 8088, 8089 and 22 only,
which is why the port is reachable from the node and its peers and nowhere
else.

### On-demand profiling and the `PROFILING=1` image

`/debug/pprof/{profile,heap,runtime}` serve a CPU profile (gzipped pprof
protobuf, 99 Hz, `?seconds=` clamped to 1..=600), a jemalloc heap profile in
`jeprof` text, and tokio runtime counters as JSON measured over a real window
(`?seconds=` clamped to 1..=60). They mount on the metrics port because it is
the one HTTP surface every role shares, so one mount point profiles the
ingester, the compactor and the query tier. **They are never mounted on the
public API port:** a CPU profile is a stack-trace oracle and the heap route
names allocation sites, and neither should be reachable by a caller who is
merely authorized to query. Everything in the paragraph above about restricting
the metrics port applies with more force once these are armed.

A released image cannot serve them at all. Reaching them takes both opt-ins,
which are not redundant:

1. **A build.** `siglake-core`'s `profiling` cargo feature is off by default
   and no published image sets it, so the code is absent rather than disabled.
   `deploy/Dockerfile` with `--build-arg PROFILING=1` is the only build that
   turns it on; it also passes `--cfg tokio_unstable` (without which
   tokio-metrics reports a much smaller counter set) and
   `-C force-frame-pointers=yes`, and skips `strip --strip-debug` so profiles
   resolve to file:line. That DWARF is most of why the image is ~1.95 GB
   against ~115–123 MB per stripped binary, so it is built on request
   (`.github/workflows/profiling-image.yml`, `workflow_dispatch`) and tagged
   `prof-<sha>`, never published as a release image.
2. **An operator.** Even that image mounts nothing until
   `SIGLAKE_PPROF_ENABLED=1` is set in the process environment; any other
   value, including a misspelling, leaves the routes absent. A disarmed
   profiler answers `404`, which is what lets a profiling round refuse at its
   readback gate instead of capturing nothing.

A feature flag alone would be too easy to ship by accident; an env var alone
could not remove the code. The pair is the contract, and neither half is
scheduled to become a default.

One capture runs at a time, CPU or heap: a heap dump walks allocator state
while the CPU profiler's `SIGPROF` handler interrupts threads, so a second
request of either kind is refused with `409` rather than interleaved. The
admission ticket releases on drop, so a client that hangs up mid-window leaves
the endpoint usable.

The heap route needs one more thing the image cannot give it: the process must
have **started** with `_RJEM_MALLOC_CONF=prof:true,prof_active:true` — prefixed,
because `tikv-jemalloc-sys` builds jemalloc with a prefixed symbol namespace and
ignores the plain `MALLOC_CONF`. jemalloc samples only while `prof.active` is
true and a dump reports live sampled allocations, so arming it at dump time
would report a near-empty heap; the handler therefore only dumps, and answers
`412` naming what is missing when sampling was never on.

## Performance (measured on AWS, 3-node clusters)

Benchmarks against Quickwit, Elasticsearch, ClickHouse, and a
vanilla-Parquet DuckDB baseline use open data and methodology:
https://github.com/limnion-ai/siglake-benchmarks

At **200 GB / 394 M rows** (m6i-class nodes, S3 warehouse):

- **Ingest→queryable freshness:** ~5.5 s at full ingest rate (20/20 probes;
  seal-age floor — records serve from the WAL buffer before commit).
- **Fleet ingest:** ~415 K rows/s sustained end-to-end (4 ingesters +
  3 drain nodes + RDS catalog), exact row counts held through node crashes
  via at-least-once recommit with dedup-by-proof.
- **Zero-scan aggregates:** whole-table `GROUP BY` / windowed counts /
  histogram / negation ~3 ms warm, exact (manifest + rollup fast paths, no
  file reads).
- **Attribute queries:** hot OTLP attribute keys auto-promote to typed
  columns — opt-in, and this round ran with it on; attribute `GROUP BY` in
  ~3 ms over 394 M rows, attribute filters in the label-filter class (~9 ms).
- **Selective search:** keyword ~8–12 ms, label ~9 ms, substring via trigram
  blooms — bloom-pruned scans touching 10⁴–10⁵ of 394 M rows.
- **Ordered browse:** `ORDER BY timestamp DESC LIMIT 100` over the whole
  table in ~45 ms warm via early-stop (no sort); deep pagination ~52 ms;
  exact `count(distinct)` ~3 ms.
- **Under 32-way mixed query load:** metadata fast paths hold ~6 ms flat on
  a dedicated execution runtime while concurrent scans stream.

At **1 TB / 2.02 B rows** (converged layout): the same 21-shape suite runs
zero-error with exact counts — `match_all` browse 180 ms, windowed browse
100 ms, aggregates ~13 ms, freshness 20/20 at ~5.5 s.

Today's public performance record is this section, `docs/DESIGN_*.md`, and
`docs/PERF_OTEL_INGEST_2026-06-07.md`; full round-by-round benchmark results
will live in the benchmarks repository once it is public.


## History

The build was phased — each phase shipped compiling, clippy-clean, tested
code: phases 1–4 (storage engine, ingest, query, scale-out + multi-tenancy +
hardening: backpressure, catalog claims, audit, retention, GDPR deletes),
phase 5 (the detection tiers), and the 2026-06/07 performance arc (BIG-1..4,
time-ordered storage, distributed query, ordered early-stop, leveled
compaction + graded backpressure, defer-index, continuous-dispatch drain).
September WAL durability hardening added directory-boundary fsyncs and bounded
recovery of complete acknowledged batches ahead of a torn partial tail.
The S3 mirror follow-up pinned queued segments across local compaction and
retention, made crash catch-up scan those pins, and exposed uploader queue lag.
The operator's September WAL follow-up made its mirror-prefix override apply to
both upload and catalog-claim recovery, and rejects a disabled mirror when the
compactor autoscaling range can enter claim mode.
The September consumed-proof follow-up made terminal catalog acknowledgements
compact by segment ID, preventing a pinned time watermark from wedging later
drain commits at the durable-property cap. It then extended the same bounded
proof maintenance to the filesystem drain using terminal WAL directory state.
The September default-enablement pass also qualified footer inverted-index
write cost and post-rewrite Puffin coverage, and made the writer default on
with explicit binary, Helm, and operator opt-outs. The post-rewrite rebuild
went the other way for 0.1.0: it ships off, opt-in through the same three
surfaces, because the 50G text ceilings were measured on the scan path and the
sidecar path does not meet them at that layout's index sizes.
The design record lives in `docs/` (`DESIGN_*`).

> siglake was renamed from **knulps** on 2026-06-12; pre-rename documents in
> the internal history and older commits use the old name. Same system.
