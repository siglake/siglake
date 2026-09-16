# Changelog

## Unreleased

- **Observability (new, off by default)**: setting
  `OTEL_EXPORTER_OTLP_ENDPOINT` exports the log lines every binary already
  writes as OTLP log records, and the spans at the boundaries that cost
  something — the ingest handlers and `ingest_batch`, the compactor drain, the
  query server's per-request middleware and `distributed_inner` — as OTLP
  traces over HTTP. The coordinator injects W3C `traceparent` into each shard
  request and the worker's middleware extracts it, so a distributed query is
  one trace rather than a root span per replica. `/metrics` does not move:
  `siglake_*` stays Prometheus, which is what the alerts, the KEDA scalers and
  the dashboard read, so an OTLP-only backend still needs a collector with a
  Prometheus receiver (`docs/LIMITATIONS.md`). Unset or empty endpoint and no
  exporter, batch processor or OTel layer is constructed; what remains is the
  per-request middleware asking the global propagator to extract and creating a
  span nothing listens to, measured at +104 ns/request against the `TraceLayer`
  it replaced and +393 ns against no layer at all. Configuration is the
  standard OTel environment, read once at startup through pure resolvers —
  `OTEL_SERVICE_NAME` defaults to `siglake-<component>`,
  `SIGLAKE_OTEL_DISABLED=1` stops emission without unsetting the endpoint,
  `OTEL_TRACES_EXPORTER`/`OTEL_LOGS_EXPORTER=none` drops one signal. Neither
  the chart nor the operator renders any of it, so it reaches a pod through
  `<tier>.extraEnv`. Console output stays on stderr, where the CRD print and
  the `--dry-run` reports need it. Each binary's `main` flushes the providers
  explicitly on a graceful SIGTERM return, a one-shot subcommand's return and
  any error after initialization: the providers live in a `OnceLock` that never
  drops, so a drop-at-exit guard shipped nothing. The OTLP exporter uses the
  blocking `reqwest` client, because the SDK's batch processors export from
  their own threads and the async client panics with no reactor there. No HTTP
  surface, values key or default changes. Imported from Gianluca Arbezzano's
  PR #7. (#4546)

- **Diagnostics (new, off in every released binary)**: on-demand CPU, heap and
  tokio-runtime profiles at `/debug/pprof/{profile,heap,runtime}` on the
  `--metrics-bind` router, so one mount point covers the ingester, the
  compactor and the query tier. Reaching them takes two opt-ins, neither of
  which a release carries: `siglake-core`'s off-by-default `profiling` cargo
  feature, and `SIGLAKE_PPROF_ENABLED=1` in the process (any other value,
  including a misspelling, leaves the routes absent and answering `404`). The
  build that carries the feature is `deploy/Dockerfile --build-arg
  PROFILING=1`, which also sets `--cfg tokio_unstable` and frame pointers and
  keeps DWARF — ~1.95 GB against ~115–123 MB per stripped binary, so it is
  built on request by `.github/workflows/profiling-image.yml` and tagged
  `prof-<sha>`, never published. Nothing on the metrics port is authenticated
  and the chart restricts only egress, so the exposure model is now written
  down where an operator will find it (`docs/ARCHITECTURE.md` "Diagnostics").
  One capture runs at a time, CPU or heap — a heap dump walks allocator state
  while the CPU profiler's `SIGPROF` handler interrupts threads — and the
  ticket releases on drop, so a client that hangs up mid-window leaves the
  endpoint usable. The heap route needs the process STARTED with
  `_RJEM_MALLOC_CONF=prof:true,prof_active:true`, prefixed, and answers `412`
  naming what is missing otherwise. The runtime route measures over a real
  `?seconds=` window (default 2): tokio-metrics counters are deltas over a
  sampling interval, and sampling immediately reported a fully loaded ingester
  as idle. No release binary, HTTP surface, values key or default changes.
  Imported from Gianluca Arbezzano's PR #11. (#4547)

- **Metrics (series identity changes)**: the four aggregate-maintenance
  counters now carry `iceberg_namespace` alongside `table`:
  `siglake_group_count_short_aggregates_total`,
  `siglake_group_count_delta_write_failures_total`,
  `siglake_side_aggregate_publish_failures_total` and
  `siglake_group_count_auto_rebuilds_total`. One compactor maintains the base
  namespace and every `tenant_*` namespace, each with its own `events`, so a
  bare `table="events"` merged every tenant into one series and
  `SiglakeGroupCountAggregateShort` named a table an operator could not
  locate. The three alerts that read these counters now name
  `<namespace>.<table>` and pass `--namespace` to the `rebuild-group-counts`
  they suggest, and the dashboard's three panels group by the pair. The label
  is `iceberg_namespace`, not `namespace`, because Prometheus attaches the
  Kubernetes namespace under that name and renames a colliding metric label to
  `exported_namespace`. Existing recording rules, dashboards and silences that
  match these four counters by `table` alone keep working; anything that
  matches an exact label set does not. Pre-registration still lists the
  default namespace's `events` alone — a tenant namespace, an index table and
  a base namespace moved by `SIGLAKE_TENANT_NAMESPACE` are known only at the
  increment. (#4737)

## 0.1.1

Twelve changes on top of 0.1.0. Nothing about the on-disk format or the HTTP
surface moves, and a 0.1.0 warehouse is read and written unchanged: one values
key and six environment knobs are added, and no flag or values key is removed.
Two defaults move. The audit worker now gives each append 30 s instead of
awaiting it forever, and every process except the query server budgets zero for
the two text-index caches — a ceiling rather than a behaviour, since those
processes were already holding nothing in them. The
workspace version, both chart `version`/`appVersion` pairs, the pinned image
tags under `deploy/` and the two OpenAPI documents' `info.version` all read
`0.1.1`, and git tag `v0.1.1` publishes image tag `0.1.1`.

- **Query**: whether to *use* a text index a file already carries is now
  decided per execution. Loading one costs a deserialization proportional to
  the file's rows, and a text predicate under a bare `LIMIT` stops the scan
  after a sliver of the first file, so that shape stays on the scan path; the
  `ORDER BY timestamp` form of the same decline shipped in 0.1.0. An unclipped
  text scan keeps the index, which is the regime it wins in. On a 14 × 7.34M-row
  local fixture with every index resident, the four clipped shapes went from
  24.4 / 13.6 / 50.3 / 869.2 ms indexed to 7.5 / 10.1 / 10.8 / 5.4 ms, while
  two unclipped rare scans kept their indexed 126.3 and 39.4 ms against
  1,757.2 and 535.2 ms scanned. The rule is deliberately blunt and the same
  fixture shows what it costs: a rare term under a `LIMIT` is declined with
  the rest and scans in 531.2 ms where a resident index would have answered in
  45.8 ms. Document frequency lives inside the whole-file index being
  declined, so choosing by it would first pay the load the decline avoids.
  Answers are unaffected either way: the index only ever
  produced a superset row selection, blooms stay active, and the exact
  predicate is re-evaluated above the scan either way. A decline increments
  `siglake_query_inverted_index_declined_total{reason}` and is named in the
  scan's plan line (`text_index:[declined:clipped_limit]`). Puffin rebuild
  still ships off. (#4375)
- **Operator**: `siglake-operator --adopt-values` now prints one YAML
  document, with its findings and runbook as comments, so the saved file is
  what the runbook's own `kubectl apply -f cluster.yaml` can apply — the output
  used to stop being parseable YAML right after the synthesized
  `SiglakeCluster`. `--adopt-namespace <NS>` sets `metadata.namespace` and the
  `-n` on every runbook command, including the abort path and helm's
  release-namespace annotation, and defaults to the release name. The CRD is
  namespaced, so a release installed into a namespace that is not its name
  previously produced a report that applied wherever the current kube context
  pointed. (#4544)
- **AWS reference deployment**: `deploy/aws/up.sh` writes its own mode-0600
  kubeconfig, verifies the context it wrote names the cluster it just
  provisioned, and passes `--kubeconfig` / `--context` to every `kubectl` and
  `helm` call. It leaves the caller's default kubeconfig and current context
  alone, prints the `KUBECONFIG` export the follow-on scripts need, and removes
  the file on failure. `deploy/terraform/aws/README.md` documents the same
  by-hand recipe. `scripts/check-aws-up-kubeconfig.sh` holds the script to this
  in the `shell` job. (#4545)
- **Maintenance**: the compactor now censuses every maintained table for a
  group-count aggregate short of its row count with every commit's contribution
  accounted for, and reports it on
  `siglake_group_count_short_aggregates_total{table,outcome}` and the new
  `SiglakeGroupCountAggregateShort` alert (34 alerts in the chart, was 33). That
  state does not heal on its own — a commit killed between its commit and its
  delta PUT leaves it, a table whose aggregate prefix started empty mid-life
  starts in it, and a later delta adds its own rows while the total stays short
  — so until now it lasted until an operator ran `siglake rebuild-group-counts`.
  Answers were and remain exact: a short aggregate sends `GROUP BY` to the exact
  per-file path. The census runs every 15 minutes
  (`SIGLAKE_AGG_SHORT_SCAN_INTERVAL_SECS`), from the maintenance compactor only,
  and costs one number per column out of the base — 15 ms at 40k rows, 141 ms at
  400k. Rebuilding automatically is opt-in
  (`SIGLAKE_AGG_SHORT_REPAIR=1`, `compactor.shortAggregateRepair`, one table per
  pass via `SIGLAKE_AGG_SHORT_REPAIR_MAX_TABLES`), because it is one Tier-2
  query per maintained column: ~217 ms per 100k rows per column measured on a
  local filesystem, so a 250M-row column is ~9 minutes and a table that size is
  still the operator's to rebuild by hand. A rebuild records the columns it
  could not restore, so one unreadable column does not buy a full rebuild every
  pass. (#3000)
- **Maintenance**: new `siglake rebuild-time-aggregates --table <t>`, for a table
  whose inline aggregate object predates the snapshot-coverage chain. Such an
  object cannot prove which equal-row-count snapshot it describes, so every
  query refuses it and `date_histogram` and windowed `GROUP BY` fall to the
  exact per-file tier — and it does not heal, because a chain with no head
  cannot be rejoined by later appends or by a compaction. The command
  recomputes the time buckets (one footer read per live file) and the 2-D
  time×group rollup (a two-column decode per live file, or that file's
  group-count footer where its whole time range sits inside one bucket),
  replaces both, and publishes that snapshot's coverage edge at the root of the
  re-cluster run it sits on, after which ordinary commit-path maintenance
  carries the chain forward. Measured on
  a local fixture, the fallback it removes costs 23–59× the Tier-1 path warm
  (though under ~2ms) and 3.8–35× cold across 49–168 live files, growing with
  the file count. Three limits, all reported by the command: the inline
  whole-table group counts are dropped rather than certified, since one
  coverage edge governs the object and they cannot be proven (they were already
  refused, so nothing readable is lost); a component short of `total-records`
  is left absent rather than written short; and a commit landing under the pass
  cannot be merged, so on a table under live ingest it retries three times and
  exits without writing. Re-running after success is a reported no-op. Distinct
  from the entry above: a short aggregate has a provable chain and too few
  rows, and `rebuild-group-counts` (or the opt-in automatic repair) is its
  remedy; this one has the rows and cannot prove which snapshot they belong
  to. No existing behaviour, default or format changes. (#3082)
- **Maintenance**: a side-object aggregate's coverage chain now survives
  snapshot expiry. A reader walks from the current snapshot back to the
  object's coverage edge over row-conserving re-clusters, so it needs every
  snapshot in between: a compaction-only stretch longer than `retain_last`
  (default 100, swept every 60 s) dropped the edge's own snapshot and stranded
  the object for the life of the table, with every later append's edge
  stranded behind it. `expire_snapshots` now decides, against the metadata it
  is about to shrink, whether the edge it can still prove survives the commit,
  and re-roots it onto the deepest surviving snapshot when it would not: the
  same rows, no recompute, one object write, counted on
  `siglake_inline_coverage_reroots_total`. The write is fenced on the object
  still carrying the edge that was proven, so an append publishing in the
  window keeps its own edge
  (`siglake_inline_coverage_reroot_conflicts_total`), and an expiry that
  cannot walk to the edge leaves it alone — ancestry that is gone is never
  bridged and equal row totals are not evidence. Both rebuild commands had a
  related defect: they published the scanned snapshot's own edge, which on a
  compacted table is a re-cluster, and an append's link names the
  data-changing snapshot below the run — so a repair there lasted until the
  next commit. Every edge is now published at that normal form. Answers were
  exact throughout, by the per-file tiers; what was lost was the fast path.
  Retention, a delete task and a foreign overwrite still retire the object
  until `rebuild-time-aggregates` runs, which is
  `docs/LIMITATIONS.md`. (#3800)
- **Maintenance**: the tables in that state are now named. The triggers that
  remain after the re-root — retention, a delete task, a foreign overwrite and
  the two residual windows at expiry — all end in one state: an object the read
  guard refuses, so windowed `GROUP BY`, date histograms and windowed counts on
  that table answer exactly from the per-file tiers, for the life of the table,
  because no commit republishes a chain the reader cannot walk. Nothing said
  which table it was. `siglake_query_side_aggs_cache_total{result="unproven_coverage"}`
  needs a query to arrive and carries no table label, and the expiry path's warn
  fires only in the window where its own re-root failed. The maintenance
  compactor now censuses every maintained table's inline object every 15 minutes
  under the `agg_fold` lease
  (`SIGLAKE_INLINE_COVERAGE_SCAN_INTERVAL_SECS`, `off` to disable), asking the
  read guard's own question, and sets
  `siglake_inline_coverage_unproven{iceberg_namespace,table}` to 1 or 0 for
  every table it reaches a verdict on — so a table repaired by
  `siglake rebuild-time-aggregates` clears at the next pass. A publication still
  in flight reads as covered, an object the census could not read writes no
  sample at all (a failed GET is not evidence in either direction), and a table
  the pass stops reaching — a dropped index — has its reading zeroed rather than
  left standing until the process restarts.
  `SiglakeInlineCoverageUnproven` (critical, 35 alerts) fires after 30 minutes —
  two censuses — and renders the repair command with both labels filled in; it
  carries `increase(siglake_inline_coverage_census_total[1h]) > 0` on the same
  pod as a liveness arm, so a compactor that stopped censusing leaves the alert
  rather than paging from a reading nobody is refreshing. The census reads and
  never rebuilds. No format, default or query behaviour changes. (#4674)
- **Maintenance**: the compactor, the ingest server and the `siglake`
  maintenance subcommands resolve their own cache budgets at startup instead of
  inheriting the vendored reader's constants. The two text-index caches are an
  explicit zero there: the only site that fills either is the scan's
  index-pruning path, and maintenance plans no text predicate — a Tier-2
  aggregate rebuild counts from manifest stats, Parquet footers and raw pages,
  and a delete task evaluates its predicate over the candidate file's decoded
  rows — so what the packaged 1Gi compactor pod used to carry was 1.25 GiB of
  ceilings on caches that never take an entry. `sql-direct`, `iceberg-demo` and
  `subscribe` do run SQL in process, and derive what the query server derives at
  their own memory limit. Nothing is switched on to make the accounting agree:
  the byte-range object cache stays off until `SIGLAKE_OBJECT_CACHE_BYTES` is
  set, and what a process holds is now recorded rather than re-derived, so a
  process no longer subtracts a quarter of its pod from the query memory pool
  for a cache it has switched off. `SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES`,
  `SIGLAKE_PUFFIN_BLOB_CACHE_MAX_BYTES` and `SIGLAKE_OBJECT_CACHE_BYTES`
  override the role in either direction. (#4082)
- **Drain**: a WAL segment the local filesystem drain cannot read no longer
  takes every batch it joins down with it. The read phase now names the
  segments that failed instead of returning one error for the whole batch, and
  after three consecutive failed reads
  (`SIGLAKE_COMPACTOR_POISON_ATTEMPTS`; `0` restores the old behaviour) that
  file — and only that file — moves to `<wal>/poison/` with a `.poison.json`
  note holding the read error and the attempts spent. Its batch siblings commit
  on the next pass. Before this, a truncated restore, a bad sector or a frame
  version the build did not know left the segment cycling between `sealed/` and
  `processing/` with nothing naming the file, and the healthy segments behind it
  never became queryable. A transient failure still charges nothing and retries
  the whole batch, and a segment that commits has its charges dropped, so the
  budget counts consecutive failures of one file. `poison/` is excluded from the
  automatic `orphans/` disposition, survives restarts, and is never deleted or
  rewritten: the new `siglake wal-requeue --wal <wal-root>` command
  (`--segment`, `--dry-run`) is the way back, once the cause is fixed. `siglake_compactor_segments_poisoned_total` counts the set-asides,
  `siglake_compactor_segments_poisoned{tenant}` levels them, and
  `SiglakeSegmentsQuarantined` now fires on either drain's held-back segments
  (still 34 alerts). (#3143)
- **Drain**: a pass that keeps failing no longer re-claims the same segments
  until its cycle budget runs out. Each drain pass now spends at most three
  claims on a segment and then leaves it in `sealed/` for the next cycle; the
  segments a pass has not tried, including ones sealed while it ran, are
  claimed as before. A failed batch was released back to `sealed/` and handed
  straight back by the re-list, so a cause that fails fast and names no
  segment — a recurring catalog conflict, a store refusing writes, an
  unreadable segment on a build with `SIGLAKE_COMPACTOR_POISON_ATTEMPTS=0` —
  cost a rename and two fsyncs per segment each way, as fast as the failure
  returned, for the whole 30 s default budget: 17,502 claims over two segments
  in one measured pass, against the 3 it now makes. Same-cycle retry of a
  transient failure is unchanged, and so is the cross-cycle accounting behind
  the `poison/` set-aside — the per-pass bound is not a verdict on a segment
  and is forgotten at the end of the pass. A withheld segment is counted by
  `siglake_compactor_pass_claim_attempts_exhausted_total{tenant}`. No setting,
  default or durability boundary moves. (#4651)
- **Query audit**: an audit append now has 30 s to finish
  (`SIGLAKE_QUERY_AUDIT_APPEND_DEADLINE_SECS`; `0` restores the unbounded
  await). Query responses never waited on the audit worker and 0.1.0 bounded
  what it retains, but a single append that stopped answering still held that
  whole bounded budget: every later row was refused as `row_limit` or
  `byte_limit`, and nothing reached the `query_audit` table again until the
  process restarted. A batch that outlives the deadline is abandoned, which
  releases its rows and lets the worker take the ones behind it; the rows are
  counted by `siglake_query_audit_dropped_total{reason="append_deadline"}`
  beside one `siglake_query_audit_failures_total{reason="append_deadline"}`,
  and the dashboard's drop panel names them. Shutdown of a finite queue is
  bounded by the same deadline. The abandoned batch is never re-appended: the
  deadline cuts the worker's await rather than the append, so a catalog commit
  that had already gone out can land unseen, and a retry would duplicate the
  rows it did persist. Audit rows can therefore be lost whole at the deadline,
  which `docs/LIMITATIONS.md` now records. (#3438)
- **Naming**: the ingest handlers, their rate-limit middleware and the prose
  around ingest tokens no longer carry the name of the HTTP event-collector
  compatibility surface that was removed in 2026-06. The middleware is named in
  three shipped `429` descriptions, so `docs/api/openapi-ingest.yaml` is
  regenerated. No runtime behaviour changes. (#4569)
## 0.1.0 — initial public release

siglake: a horizontally-scalable, OTLP-native log analytics platform on
Parquet v2 + Apache Iceberg + DataFusion.

- **Ingest**: OTLP/HTTP logs and traces (`POST /v1/logs`, `/v1/traces`), a
  default-on OTLP/gRPC logs and traces listener on `0.0.0.0:4317`, and an
  Elasticsearch-compatible `_bulk` surface with per-index document mappings.
  Disable OTLP/gRPC with `--disable-otlp-grpc` or
  `ingester.otlpGrpc.enabled=false`; the WAL has graceful-shutdown force-seal,
  backpressure lanes and token-bucket rate budgets. Auth is `Authorization:
  Bearer <token>` or OIDC.
- **Durability**: a request is acked once its rows are in the WAL, after an
  `fsync(2)` by default, so acknowledged rows survive a node-level crash or
  power loss on a filesystem that honours `fsync(2)` — ext4 or xfs on a
  node-attached volume. The sync covers the directory entries that name a
  segment as well as its bytes, and a seal publishes the sealed name durably
  before unlinking the active copy. Send `?commit=auto` to be acked once the
  `write(2)` reaches the
  kernel page cache instead — that survives a process crash, an OOM kill and a
  pod restart, but not power loss in the window before the kernel flushes.
  The lifecycle moves after the ack are durable on the same terms: claim,
  release, finish, the orphan and stale-owner quarantines, the owner stamp and
  both halves of a secondary consumer's position — the watermark it publishes
  and the cursor in its own state directory — each sync the directories they
  changed before reporting the move, so a drain's progress is not something a
  power loss takes back.
  Sealed WAL segments are also mirrored to the object store by default, so the
  WAL volume is not the only copy of what has been acknowledged; the upload is
  asynchronous, so an ack is durable locally and not yet remotely.
  `siglake wal-recover` fsyncs each restored segment and the directory that
  names it before counting it, so a restore that reports 400 segments has 400
  whole ones. Neither mode
  waits for the rows to become *queryable*; that follows the drain's cadence
  (measured p50 ~5.5s).
- **Multi-tenancy**: single-tenant by default. Every ingest request routes to
  the `default` tenant — which chooses both the WAL subtree and the Iceberg
  namespace — and an `X-Scope-OrgID` naming another tenant is refused with
  `403` on HTTP and OTLP/gRPC alike, rather than honoured or ignored. Routing
  tenants is explicit, by one of two means. `--oidc-tenant-claim` takes the
  tenant from the caller's verified JWT on both transports: a header may only
  agree with it, and a token carrying no usable claim is refused. That is the
  setting for a shared cluster. `--trust-scope-header` takes the client's word,
  for a deployment whose gateway sets the header itself and strips the
  client's. Optional `--allowed-tenants` / `--max-tenants` bound what can be
  created, checked against the tenant actually resolved.
  The query server applies the same rule to its own `--oidc-tenant-claim`:
  a claim that is missing, blank, non-string, over 128 characters or outside
  `[A-Za-z0-9_-]` is a `403` before routing, not a fall back to the default
  namespace, and identifiers are validated rather than repaired so
  `acme.corp` cannot reach `acmecorp`'s data.
- **Storage**: Iceberg tables on any object store, physically time-ordered
  Parquet with per-file group-count footers (typed columns included),
  token/trigram bloom filters, and continuous leveled compaction with
  overlap-depth convergence. The compactor's `siglake_table_live_data_files`
  gauge is read from the snapshot summary each cycle, so it stays exact on
  tables too large to walk inside the sampler's per-table budget; the
  level/depth gauges can lag and `siglake_table_gauges_sampled_at_seconds`
  says when they were last walked. Group-count delta writes get four attempts with
  retry/failure metrics. An exhausted write leaves a durable marker for the
  maintenance compactor to rebuild automatically.
  `siglake_group_count_auto_rebuilds_total{table,outcome}` records the result;
  `SiglakeGroupCountDeltaLost` fires only when that rebuild fails or remains
  incomplete. As the operator fallback, `siglake rebuild-group-counts` repairs
  the aggregate from committed files without double-folding late deltas; its
  `--admit-typed-columns` adds the typed columns a table created before typed
  side aggregates never carried, so no rewrite is needed for those.
- **Query**: DataFusion SQL (`/api/v1/sql`), read-only on every entry point —
  DDL, DML and session statements (`COPY … TO`, `CREATE [EXTERNAL] TABLE`,
  `CREATE VIEW`, `DROP`, `INSERT`, `SET`) are refused with `400` during plan
  verification, before the statement can take effect, on `/local`, `/shard`,
  `/distributed`, `/explain`, `dry_run` and the batch tier alike, and the
  compactor plans a persisted delete task's `predicate_sql` under the same
  options rather than trusting the REST validator in the other process, as do
  the Jaeger trace routes, whose `WHERE` clauses are assembled from request
  parameters; with a transparent distributed
  coordinator; ordered-scan early stop in both time directions (reversed
  tail-chunk decode); zero-scan fast paths for counts, group-bys,
  histograms, distinct counts, and dimensional/typed filters served from
  footers and snapshot-keyed side aggregates; snapshot-keyed result and
  decoded-chunk caches (invalidated by commit, never by TTL; the result cache
  keys the serving mode too, so `exact: true` and a `shard` request never
  replay another mode's answer). The table-metadata cache they key off only
  moves forward: a reload that a commit — or a newer reload — overtakes while
  it is loading publishes nothing rather than restoring pre-commit metadata.
  Per-request
  scan/cost attribution (`stats.scan`). Bounded query spill uses
  `SIGLAKE_QUERY_SPILL_DIR` / `SIGLAKE_QUERY_SPILL_MAX_BYTES`, the chart's
  `query.spill.*` ladder, and a matching query-pod ephemeral-storage limit.
  Memory-pool or spill-cap refusals return `503` plus `Retry-After`, including
  refusals forwarded from workers, and increment
  `siglake_query_breaker_trips_total{breaker="pool_exhausted"}`. A fanned-out
  query is answered from ONE table generation or not at all: each shard request
  is pinned to the coordinator's serving snapshot **and** schema id — an
  additive `migrate-schema` moves the schema without committing a snapshot, so
  the snapshot alone is not that generation — and a worker resolves the pin
  against its own metadata, refreshing once if it lags and serving a retained
  historical schema if it is ahead. One it cannot resolve either way refuses
  with `503` + `Retry-After` (`reason: "shard_pin_unresolved"`) rather than
  substituting its own current generation; the coordinator forwards that
  refusal instead of re-running the fragment. The pin's schema id is optional
  on the wire, so a mixed-version rollout keeps the snapshot-only behaviour
  until every query replica carries the field. The single-pod
  wall-clock timeout covers cost estimation and the metadata fast-path battery
  as well as execution. Each request registers exactly the tables its SQL
  names, wherever they are named: a table reached only through a subquery in
  `WHERE`/`EXISTS`/`IN`, a JOIN condition, `HAVING` or a nested projection
  expression is registered like one in `FROM`, while a `WITH` alias still
  shadows a physical table for the scope that declares it. An unknown key
  inside the request's `limits` object is refused with `422` by the JSON
  extractor — before planning, execution or batch enqueue — rather than
  dropped: `{"limits":{"max_rows":5000}}` used to be answered `200` with the
  tier default silently applied, and `max_rows` is exactly the typo the
  response envelope's own spelling invites (the request field is
  `max_rows_returned`; there is no alias). Only the `limits` object is strict —
  unknown top-level keys are still ignored, and a known limit above its tier
  ceiling is still clamped, not refused.
  Asynchronous batch jobs (`priority: "batch"`) are stored in the catalog
  Postgres by default, one table shared by every query replica: the id a `202`
  hands back is readable, and cancellable, on whichever replica the Service
  routes the client to, and a pod restart surfaces in-flight jobs as `failed`
  once their owner's lease expires instead of losing them. Setting
  `query.jobs.persistent: false` keeps the former per-pod in-memory store,
  which is correct only at `query.replicas: 1`.
- **Freshness**: sealed-WAL buffer serving — records are queryable in
  seconds, before commit, exactly folded into every fast path.
- **Attributes**: lossless capture of OTLP resource/log attributes,
  queryable via `attr_get()`; hot keys auto-promote to typed columns with
  backfill and query rewrite (opt-in).
- **Streaming consumers**: `siglake_wal::consumer::SegmentConsumer` is a
  supported interface for reading WAL segments as they seal — a cursor
  `fsync(2)`ed before `commit` returns, at-least-once delivery, retention that
  waits for slow consumers (bounded, so a stuck consumer degrades to "you
  missed some" rather than filling the disk), and CRC integrity. See
  `docs/CONSUMING_SEGMENTS.md`.
  The four-tier semantic detection pipeline that shipped inside siglake
  through 2026-08-29 was moved out to run entirely on top of this interface,
  and is maintained as its reference consumer.
- **Schema evolution**: a table records the schema version it is at. When
  the running binary is newer, the columns the table lacks *cannot* be
  written, so the write is **refused** — naming the column and the remedy —
  rather than silently dropped. Run `siglake migrate-schema --all-tables
  --all-namespaces`; it is additive-only and idempotent, and
  `--all-namespaces` matters on any multi-tenant install because `events`
  exists once per tenant namespace. Both control planes do it for you: the
  chart as a `pre-upgrade` hook, the operator before rolling the workloads.
  The recorded version only moves up: an older binary's migration Job on an
  already-widened table (a roll-forward after a rollback, or an operator
  `spec.image` revert) adds no column and leaves the higher version in place,
  so `--dry-run --all-namespaces` reports the shape the table has.
- **Operations**: Helm charts, a Kubernetes operator (`SiglakeCluster`
  CRD) with per-tier `spec.resources.<tier>`, the chart's `4Gi` query-pod
  default, schema-migration jobs, and offline Helm-release adoption. The
  operator binary's built-in Prometheus URL now matches the charts' default
  `prometheus-community/prometheus` Service. Invalid
  configurations set `InvalidSpec` with one of ten reasons rather than being
  partly honored; the former `QueryAutoscalingIgnored` condition is removed.
  A compactor tier that can hold more than one pod (`compactor.replicas` or
  `autoscaling.compactor.maxReplicas` above 1) is refused by the chart unless
  `compactor.catalogClaim.enabled` divides the work, as is that claim without
  the WAL mirror it claims from; the operator enables both together.
  `ingester.extraArgs: [--with-compactor]` is refused outright — the embedded
  compactor takes no claim, the chart renders none for it, and a rolling
  update puts two of them on one table at any replica count.
  Terraform/EKS reference deployment, Prometheus metrics throughout,
  Prometheus alert rules for the silent-loss counters, audit logging,
  retention/GC/delete sweeps.
- **Interoperability**: the warehouse is plain Iceberg-on-Parquet at **format
  version 2**, with no v3-only type in any schema. Event time is `timestamp`,
  a microsecond `timestamptz` (Parquet INT64 TIMESTAMP(MICROS, UTC)) that every
  Iceberg reader maps; `events` also carries `timestamp_ns`, a required `long`
  holding the OTLP `time_unix_nano` value verbatim, so nanosecond exactness is
  available externally via `to_timestamp_nanos(timestamp_ns)` and siglake's own
  scans lose nothing. Removing the v3 nanosecond-timestamp type opened the
  reader matrix: the
  [local-fixture compatibility evidence](https://docs.siglake.dev/guides/external-engines/#compatibility-evidence)
  demonstrates Trino 483, Spark 3.5.9 (`iceberg-spark-runtime-3.5_2.12:1.11.0`),
  DuckDB 1.5.5 (core `iceberg` `45163a28`) and PyIceberg 0.12.0 + PyArrow 25.0.1
  each reading a format-version-2 warehouse without siglake in the path and
  agreeing on its exact `timestamp_ns` bounds. The fixture is on local disk with
  a SQLite catalog; the Spark measurement went through a wrapper because the
  bundled driver then selected a Hadoop catalog. Warehouses written before this
  change are format version 3 and must be recreated, not migrated
  (`docs/DESIGN_time_ordered_storage.md`, "Timestamp contract").

See the **Performance** section of `README.md` for measured comparisons against
Quickwit, Elasticsearch, ClickHouse, and a vanilla-Parquet DuckDB baseline.
