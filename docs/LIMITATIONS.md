# Things deliberately not yet done

The engineering record of what siglake 0.1.0 leaves out on purpose, why, and
what would change it — one entry per decision, kept current with the code. It
moved here from the README on 2026-09-15; the docs site's
[Limitations](https://docs.siglake.dev/about/limitations/) page is the
user-facing summary of the same list. The mechanisms named below are described
in [`ARCHITECTURE.md`](ARCHITECTURE.md).

- **The implicit newest-first ordering only knows the column name
  `timestamp`.** An index whose doc mapping names some other
  `timestamp_field` browses in file order unless the query writes its own
  `ORDER BY`, and so does not reach the ordered early-stop path. Reading the
  mapping's field name into the rewrite is the extension; it was left out
  because ordering by a column named `timestamp` on an index whose event time
  is something else would stamp an order that is not a time order, and the
  safe half (the shipped templates and every bulk-created index, all of which
  declare `timestamp`) covers what users actually have.
- **A pod at the 4Gi floor caches no text indexes by default.** Both caches now
  derive from the pod's memory limit and are subtracted from the query pool, so
  they are inside the budget rather than beside it — and what they get is what
  remains once the pool still holds one compacted file's decode estimate, which
  at the floor is nothing. The floor pod therefore deserializes an index per
  text query again unless `SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES` and
  `SIGLAKE_PUFFIN_BLOB_CACHE_MAX_BYTES` are set by hand, trading the decode
  reservation for warm indexes; 5Gi buys both. A pod in that state is now
  visible rather than inferred: every acquisition is a `miss` on
  `siglake_iceberg_parsed_index_cache_lookups_total` with no eviction beside it,
  which is the shape the "Text-index startup" panels were added for (#3969). The caps above 16Gi are policy
  rather than measurement, but the working set below them has now been sized,
  and it is larger than the caps assume: one 7.34M-row compacted file's parsed
  index is 526.0 MiB, so 1 GiB holds **one** of them, and a 14-file text plan
  wants 7.19 GiB (#4376, `DESIGN_segmented_inverted_index.md`). At the 1 GiB
  budget that plan takes zero cache hits and 41 evictions. Sizing cannot close
  that gap at any cap a query pod can afford, which is why the format itself is
  the open item rather than the budget. Whether the
  serialized copy earns its share at all is a separate open question: since a
  warm query reads only the parsed form, the blob is worth its bytes exactly
  when a refetch from the object store costs more than holding them, which no
  measurement against a real store has settled. Both are kept for now.
- **A maintenance process's cache budgets are readable at startup, not on
  `/metrics`.** The compactor, the ingest server and the `siglake` maintenance
  subcommands resolve their own budgets now — zero for the two text-index
  caches, whatever `SIGLAKE_OBJECT_CACHE_BYTES` says for the byte-range cache —
  and log the resolved numbers once. `siglake_cache_budget_bytes` and
  `siglake_query_memory_*` are sampled by the query server alone, so a
  compactor's budget is not scrapeable. Publishing them from the compactor's
  metrics loop is the extension, and it was left out because the one sampler
  that exists reads the query memory pool, and reading that pool BUILDS it: a
  process with no query engine would start publishing a pool it never uses. A
  cache-only sampler is the change; nothing measured yet needs it, since what a
  maintenance process holds is a derivation of its limit and its environment.
- **The operator does not convert WAL between filesystem and catalog-claim
  drains.** Changing `spec.autoscaling.compactor.max` across one changes the
  ownership protocol. If either existing workload template names the other
  mode, reconciliation stops before applying the PVC or workloads and reports
  `DrainModeHandoverRequired`. Restore the old maximum, or stop ingestion,
  finish and verify the current drain, account for retained local and mirrored
  segments, then delete both Deployments so the operator can create them in
  the selected mode. The handover does not automatically convert or delete
  local WAL, mirror objects, or catalog rows.
- **The Helm chart cannot scale the compactor on its backlog.** Backlog-driven
  compactor scaling is `siglake-operator`'s only. The chart's HPA renders
  `siglake_compactor_sealed_pending` as a `type: Pods` metric, and that
  algorithm averages the metric over the running pods before dividing the
  target into it. Under the catalog claim every compactor publishes the whole
  shared queue, so the same backlog asks for a pod count proportional to the
  count already running, and the combination
  (`autoscaling.compactor.customMetric.enabled` with
  `compactor.catalogClaim.enabled`) is refused at render time rather than
  installed. Claim-mode HPA scaling is CPU-only. The filesystem drain does
  publish one pod's own sealed count, but it is held at a single pod by the
  claim refusal, so the metric scales nothing there either. Dividing the target
  into the shared queue once would need one aggregated series behind an
  `Object` or `External` metric and the prometheus-adapter rule that exposes
  it; the chart does not own that rule and does not render it.
- **No live Prometheus has confirmed the ingester's per-pod scaling signal.**
  The operator reads ingest load as
  `avg(sum by (pod) (rate(siglake_ingest_requests_total{…}[1m])))`: a pod's
  endpoint/status/tenant/index series are summed before the pod totals are
  averaged, so the same traffic reads the same whether it arrives on one
  endpoint or spreads over logs and traces. The `pod` label that grouping needs
  is attached by Prometheus Operator's own target relabeling — the chart's
  `targetLabels` add only the instance and the component — so nothing rendered
  here proves it is on the series, and if it were missing `sum by (pod)` would
  return one group holding the fleet total, which is replicas times too high.
  `scripts/kind-round.sh` now holds the ingester at two pods for a bounded
  phase, drives OTLP logs and traces at both, and retains four Prometheus
  answers taken at one timestamp (`results/ingester-pod-labels*.json`): the raw
  series with their label sets, the per-series rate, that rate summed by `pod`,
  and the operator's own expression. The grader refuses a capture whose series
  carry no `pod`, one where fewer than two pods carried traffic, and one whose
  operator value is not the mean of the per-pod sums. No round has supplied
  that capture yet (#3647).
- **No tier can be scaled to zero.** Every `spec.autoscaling.<component>.min`
  must be 1 or more; `0` is refused with `InvalidSpec=True` /
  `AutoscalingZeroFloorUnsupported` before the operator touches a child
  resource, so the cluster keeps running unchanged until the floor is raised.
  The reason is that each component's scaling signal — ingest requests/sec,
  pending segments, in-flight queries — is exported by the pods of the
  component it scales. A tier parked at zero publishes nothing, so nothing can
  ask for it back, and because the decision needs all three readings, the
  stopped tier's missing series also holds the two healthy ones at their
  current size. An activation signal that outlives the stopped pods (a
  catalog-side backlog probe, a request-driven wake-up) is not implemented.
- **An audit batch can be lost at its append deadline.** Query responses never
  wait for the best-effort audit worker, its retained rows and conversion
  working set are bounded by count and charged bytes, and each append is
  bounded by a service deadline (30 s;
  `SIGLAKE_QUERY_AUDIT_APPEND_DEADLINE_SECS`, `0` restores an unbounded await).
  A storage append that outlives the deadline is abandoned so the worker can
  serve the rows behind it, and the abandoned batch's rows are gone: they are
  counted by `siglake_query_audit_dropped_total{reason="append_deadline"}`
  beside one `siglake_query_audit_failures_total{reason="append_deadline"}`,
  and nothing re-submits them. They are not retried on purpose. The deadline
  cuts the worker's await, not the append's effects — a commit whose catalog
  write had already gone out can land unseen — so a retry would duplicate the
  rows it did persist rather than recover the ones it did not. Rows submitted
  while an append is still running are dropped whole once the bounded capacity
  is full, as before. Per-row delivery is therefore best-effort in both
  directions: the `query_audit` table is an operational record, not an
  accounting one.
- **Dropping an index does not reclaim its committed storage.** `DELETE
  /api/v1/indexes/{id}` removes the catalog entry only. The retention and
  orphan-GC paths both need to load that entry, so neither can reclaim the
  dropped table's files afterward. In the local committed-data regression,
  recreating the same index id reuses the table location but creates a fresh
  table UUID: the replacement exposes only its own rows while the dropped
  incarnation's files remain alongside it. A future cleanup record must
  preserve the dropped table's immutable UUID and exact location plus an
  incarnation-specific file inventory (or equivalent retained metadata tree),
  so cleanup never resolves the same-name replacement or deletes its files.
  The index's **WAL** is keyed by tenant and index name, so it survives the
  deletion too. It is separated by a per-directory owner marker holding the
  table's UUID, written by the drain and compared by every reader: a directory
  whose marker names the dropped table contributes nothing to a WAL-buffered
  query (local, count fast paths, or a distributed fan-out), and the drain
  moves its segments to `stale/<dropped-uuid>/` instead of committing them into
  the replacement. Nothing is deleted there — a never-committed segment's rows
  may exist nowhere else — so reclaiming them is an operator decision on
  visible files. Each segment also names its own table, independently of the
  directory it sits in: the framed header carries the owning table's UUID
  (frame v2), written before the segment's first append rather than inferred at
  seal time, so a segment recovered straight out of `active/` or off the mirror
  carries it too. That is the gate that holds when an ingester keeps the
  dropped incarnation's writer open across the `DELETE`+`POST` and seals into a
  directory the drain has already re-stamped for the replacement: the buffer
  (local, count fast paths, and the distributed fan-out) skips those segments
  and the drain holds them under `stale/<dropped-uuid>/`. It is also what the
  re-stamp itself goes by. A lane that re-resolved the index before the drain
  arrived has acknowledged rows for the REPLACEMENT into a directory the marker
  still assigns to the dropped table, so the sweep moves only the segments
  whose own header disagrees with the incoming owner, plus the residents that
  name no table at all — for those the marker being displaced is the only thing
  that ever spoke, the same reading the mirror gives an unstamped object under
  a re-stamped prefix. The replacement's segments stay where their writer put
  them, with their `.crc` sidecars, including the partial it still holds open;
  the cycle that re-stamps still refuses the index, and the next one drains
  what was kept. Until that re-stamp the marker is the only identity a reader
  consults, so the replacement's own buffered rows are invisible for as long as
  the directory names the dropped table — they arrive whole once the drain
  moves the marker. Regressions:
  `a_recreated_index_serves_only_its_own_rows`,
  `a_recreated_index_serves_only_its_own_rows_in_the_fan_out` and
  `a_writer_held_open_across_recreation_cannot_reach_the_replacement`.
  The catalog-claim drain reads none of that filesystem — it claims rows and
  fetches bytes from the WAL mirror — so the marker there is one object per
  `<prefix>/<tenant>/<index>/` key prefix. Nothing is moved on that side: the
  dropped incarnation's objects are already addressable by that prefix, and
  copying them elsewhere would double the bytes an operator has to reason
  about. So the marker moves to the replacement, records the table it
  displaced, and each object is then admitted on the UUID in its own header.
  The dropped incarnation's objects are quarantined rather than committed —
  they stay in the mirror and go straight to the claim store's quarantine
  state on the cycle that refuses them, which `requeue_quarantined` is the way
  out of. The release backoff other drain failures take (twelve attempts, then
  quarantine) has nothing to retry here: a table UUID is never reused, so each
  attempt would pay a GET of the same bytes for the same answer. While the
  marker names a displaced table, an object carrying no UUID at all is refused
  too, since the prefix that would otherwise vouch for it has itself named the
  dropped table.
  Leaving the marker on the dropped table instead refused the whole prefix
  including the replacement's own objects, which cost a recreated index its
  ingest for good. Regressions:
  `a_recreated_index_does_not_claim_the_dropped_incarnations_mirror`,
  `a_mirrored_segment_from_a_dropped_incarnation_is_refused_after_the_prefix_is_restamped`
  and `an_unstamped_mirrored_segment_is_refused_under_a_re_stamped_prefix`.
  Both checks answer for the moment they run, and the append then resolved the
  index NAME a second time — so a `DELETE`+`POST` landing in between committed
  the dropped incarnation's rows into the replacement, together with the
  consumed proof that retires the ingester's copy of them. The transaction's own
  UUID fence cannot see that: the replacement was loaded before the transaction
  was constructed, so it is that transaction's valid base. The identity a cycle
  verified now travels with its commit target, and is compared against the exact
  table handle that writes the data files and bases the transaction, before a
  single file is written. A mismatch refuses the batch: nothing is uploaded,
  the replacement receives neither the rows nor the proof that would retire the
  ingester's copy of them, and it ends the cycle with the empty snapshot history
  it started with. The segments are released — back to `sealed/` on the
  filesystem, back to `sealed` in the claim store — for the next cycle's
  ownership check to quarantine.
  A recreation AFTER that load is still the transaction's to refuse, and it
  does. Regressions:
  `a_recreation_before_the_append_loads_its_table_commits_nothing`,
  `a_recreation_before_the_mirrored_append_loads_its_table_commits_nothing` and
  `a_recreation_after_the_append_loaded_its_table_commits_nothing`.
  Both sides count their refusals — `siglake_compactor_wal_owner_mismatch_total`
  for the directory or prefix finding, `siglake_compactor_wal_stale_segments_total`
  per object — labelled by tenant and index. These are refusal EVENTS, not a
  count of distinct objects:
  a mirrored object requeued out of quarantine and refused again counts again,
  and a filesystem segment whose move under `stale/` fails stays in `sealed/`
  and is counted on every cycle that re-examines it. The **WAL owner
  mismatches** panel of `deploy/grafana/siglake-overview.json` reads both as a
  rate and as a cumulative total. Neither has an alert of its own: a held-back
  mirrored object is a quarantined claim on the cycle that refuses it, and
  `SiglakeSegmentsQuarantined` pages for that.
  Three residues remain. A directory no drain has visited since the upgrade has
  no marker, and a segment sealed before it has no UUID in its header; both
  read as "no opinion" and serve as they always did, so an upgrade does not
  blank in-flight buffers. On the mirror path that leniency stops at a prefix
  that has named a dropped table: an object with no UUID under one is held for
  an operator rather than committed, which is also what happens to a segment
  from a lane that could not resolve its table's UUID at all. And an ingest
  lane learns of a recreation only when it
  re-resolves the index, once per `SIGLAKE_WAL_IDENTITY_REFRESH_SECS` (30s
  default): a write accepted in that window is stamped with the dropped table,
  so it is acked `200` and then held under `stale/` instead of committed. It is
  quarantined where an operator can see it rather than silently dropped, but
  the ack does not mean the row will be queryable. Shortening the interval
  costs one catalog metadata read per lane per interval.
  The table-level **aggregates** are separated the same way, by storage rather
  than by a check. Each incarnation's inline object, folded wide base,
  per-commit deltas and rebuild markers live under
  `metadata/siglake-agg/<table-uuid>/`, and a publisher addresses the prefix of
  the table handle it committed against — so a delta or rebuild marker written
  by a lane that stalled across the `DELETE`+`POST` lands in the dropped
  incarnation's directory, where the replacement's fold never lists it.
  Deleting a shared prefix at creation would not have covered that publisher.
  This mattered when aggregate admission used `column_total == record_count`
  alone: two incarnations can agree on a row count while describing different
  rows, and the dropped table's `rebuilt_through` watermark also told
  the replacement's fold to skip the deltas it had just written. Regressions:
  `a_recreated_index_does_not_inherit_the_dropped_incarnations_aggregates`,
  `a_delayed_publication_from_the_dropped_incarnation_cannot_reach_the_replacement`
  and `a_legacy_artifact_at_the_shared_path_is_never_adopted`.
  Two residues here. Artifacts written before the prefix existed sit at the old
  flat paths (`metadata/siglake-aggregates.json`,
  `metadata/siglake-agg-wide.json`, `metadata/siglake-agg-deltas/`) and nothing
  adopts them, because the name they share with the current table is exactly
  what proves nothing; nothing deletes them either — the orphan GC still counts
  them as siglake's. So an upgraded table starts a fresh aggregate at its first
  commit, which is short of `record_count` for every row that predates the
  upgrade: `GROUP BY` answers stay exact and fall to the per-file tiers. The
  maintenance census finds that state within 15 minutes and reports it
  (`siglake_group_count_short_aggregates_total`,
  `SiglakeGroupCountAggregateShort`), but repairing it automatically is opt-in
  (`SIGLAKE_AGG_SHORT_REPAIR=1`, one table per pass) because the rebuild is one
  Tier-2 query per maintained column; `siglake rebuild-group-counts --table
  <table>` recomputes the columns from committed files either way. And a table
  whose metadata carries no UUID publishes and reads no aggregate at all, on the
  same reasoning — so it is also invisible to the census, which has nothing to
  measure a shortfall against.
  The catalog-claim **acknowledgement watermark** is the third artefact keyed by
  a name. It is the boundary the maintenance compaction writes onto a table so
  one that receives no later append still retires its terminal consumed-proof
  entries, and the claim store held it under `(tenant, index name)` alone — so
  after a `DELETE`+`POST` the replacement received a boundary describing
  segments it never held, and `compact_through` then dropped the replacement's
  own retained entries at or below it, which are what reclaim reads before it
  requeues an abandoned claim. The watermark row now records the incarnation
  whose commit established it, written in the same transaction that makes the
  claim terminal, for a commit whose append was already fenced against that
  UUID. Maintenance passes that UUID to the compaction, which compares it
  against the table the name resolves to now, on the handle the transaction is
  based on. A mismatch — or a boundary with no recorded incarnation, which is
  what a pre-upgrade row reads as — leaves the table's proof property exactly as
  it was and counts a
  `siglake_compactor_proof_watermark_skipped_total{reason,index}`. Nothing is lost:
  the target's next terminal claim re-establishes the boundary under the live
  table, and the same comparison drops a mislabelled boundary from the drain's
  own append without stopping the rows. A different incarnation replaces the
  stored boundary rather than taking the maximum with it, since carrying a
  dropped table's acknowledgement forward under the replacement's UUID is the
  mislabelling the column exists to stop. The events table records no
  incarnation, on the same reasoning the append fence uses: its name has no
  recreate path to straddle. Regressions:
  `maintenance_does_not_acknowledge_a_replacement_for_the_dropped_incarnation`,
  `maintenance_refuses_a_recreation_between_the_watermark_read_and_the_commit`
  and `a_new_incarnation_replaces_the_boundary_rather_than_inheriting_it`.
- **A WAL segment that never decodes is set aside, and only an operator gets
  it back.** The local filesystem drain reads a claimed batch as a unit, so one
  unreadable segment — a torn restore, a bad sector, a frame version this build
  does not know — failed the batch, went back to `sealed/`, and failed the next
  batch it joined, forever. After `SIGLAKE_COMPACTOR_POISON_ATTEMPTS`
  consecutive failed reads (3; `0` restores the old behaviour) the drain now
  moves that one file to `<wal>/poison/` with a `.poison.json` note holding the
  read error and the attempts spent, and its batch siblings commit on the next
  pass. What is left out is any automatic way back. `poison/` is excluded from
  the `orphans/` disposition that runs every cycle, survives restarts, and is
  never deleted or rewritten: requeueing is an operator running `siglake
  wal-requeue --wal <wal-root>` once the cause is fixed, and a segment requeued
  unchanged simply spends its attempts again. Where the corruption is local and
  the WAL mirror holds a good copy, `siglake wal-recover` is the other way back
  — the set-aside left no file under `sealed/`, so recovery pulls that segment
  again and the drain commits it, with the unreadable bytes still under
  `poison/` to look at. Until then its rows are acknowledged,
  durable on the volume, and not queryable — which is the trade the set-aside
  makes, against a queue behind it that never drains.
  `siglake_compactor_segments_poisoned_total` counts the set-asides,
  `siglake_compactor_segments_poisoned` levels them per tenant, and
  `SiglakeSegmentsQuarantined` pages on either drain's held-back segments. The
  attempt counter itself is per-process, so a restart gives a segment its
  budget again; the bytes and the verdict are what survive. Regressions:
  `an_undecodable_segment_is_set_aside_and_its_siblings_commit`,
  `without_the_set_aside_one_unreadable_segment_blocks_its_siblings` and
  `only_the_segments_a_failure_names_are_charged_for_it`.
- **A concurrent index-mapping update is refused, not merged.** Two writers
  appending different fields to the same index are not combined: the loser's
  `field_mappings` are no longer an extension of what is stored, and only the
  caller knows what it meant to append, so the update is refused and the caller
  must re-read the index and re-send. The refusal is a `400` — there is no
  version or `If-Match` in the request to answer with a `409`, and no
  merge-on-append mode. Re-sending an update that is already stored stays a
  no-op, so an idempotent retry costs nothing.
- **Pre-0.1.0 warehouses are not migrated to the current timestamp contract.**
  Tables written before 2026-09-06 are Iceberg format version 3 with a
  nanosecond `timestamp` and no `timestamp_ns` sibling. siglake still reads
  them, but no external v2-only engine can, and there is no in-place upgrade:
  Iceberg cannot change a column's precision and cannot downgrade a v3 table.
  **Recreate such a warehouse** (delete and re-ingest). A read-old/write-new
  rewrite tool is not built — 0.1.0 is untagged and no data predates it. See
  `docs/DESIGN_time_ordered_storage.md` ("Timestamp contract").
- **No rollback has been qualified against an actual older image.** The
  additive-rollback mechanism is regression-tested
  (`crates/siglake-storage/tests/storage/schema_rollback.rs`), but every one of
  those tests runs the CURRENT binary against a table widened past what it
  declares — that is the mechanism, not evidence about a released image. No run
  has deployed image N, migrated, deployed N-1 and read the result back, and the
  chart's and operator's rollback paths ("Upgrades and schema versions") are
  reasoned from the templates and the reconciler, not from a live revert. An
  image that differs by more than its declared column set (the timestamp and
  file-format contracts, promoted-column writing) is out of the tested set
  regardless.
- **The external-reader demonstration is local-disk only.** Trino 483, Spark
  3.5.9 (`iceberg-spark-runtime-3.5_2.12:1.11.0`), DuckDB 1.5.5 (core `iceberg`
  `45163a28`) and PyIceberg 0.12.0 each read a fresh format-version-2 fixture
  and agreed on its exact `timestamp_ns` bounds, but that fixture is a `file://`
  warehouse with a SQLite catalog: object storage, Postgres as the catalog
  backend, reads concurrent with compaction, Spark 4.x, DuckDB's REST-catalog
  attachment and Trino versions other than 483 are covered by no measurement.
  The object-storage external read remains open (#1560).
  `scripts/check-external-timestamp-contract.sh` is the regression check, but
  only its siglake half (format version, Iceberg field types, the
  `timestamp_ns` round-trip, the total order) runs without those engines
  installed, and the script does not include Trino. A run that verified nothing
  external no longer reads as green: `scripts/ci-local.sh --all --strict` passes
  the checker `--require-engines`, so a reader that is not installed is a FAIL
  line naming it, and a permissive run says INCOMPLETE. Its Spark command selected
  a Hadoop catalog that could not resolve the fixture at the time of the
  measurement, which was taken through a wrapper pointing Spark at the
  fixture's own `JdbcCatalog`.
- **The strengthened three-reader timestamp check has run once.** The original
  2026-09-06 demonstration above, including Trino 483, compared only
  `timestamp_ns`. External run #46 on 2026-09-11 ran the strengthened assertions
  from pinned source `f54d07b`: DuckDB, Spark 3.5 with Iceberg 1.11, and
  PyIceberg each agreed on the decoded microsecond `timestamp` type and bounds,
  no nulls, and `floor(timestamp_ns / 1000)` row by row. The retained gate
  record does not contain the DuckDB or PyIceberg versions, and the check does
  not include Trino. Run #46 qualifies only its pinned snapshot and predates
  #3379. Rollback acceptance after #3379 and the final-SHA gates remain required
  (#3324 and #2346).
- **Elasticsearch compatibility is write-only, and stays that way.** The bulk
  ingest endpoint and the root/cluster-health probes are served; `_search`,
  `_msearch`, scroll, field capabilities and `_cat/*` return
  `501 not_implemented_exception`. This one is a decision rather than a
  deferral: no ES query API is planned, which is why those routes are the one
  part of the HTTP surface left out of the OpenAPI specs. Use
  `POST /api/v1/sql` for reads.
- **Query replicas can briefly disagree across a commit.** Each replica serves
  table metadata from its own cache: the default TTL is 5 seconds, and stale
  metadata can be served for up to 60 seconds while a background refresh runs
  (`SIGLAKE_ICEBERG_METADATA_CACHE_TTL_SECS` controls the TTL). Two replicas
  behind one Service can therefore see different snapshots during that window.
  Clients that need a consistent sequence of queries must send them to one pod
  or wait for every replica's cache to converge. This is a *cross-request*
  limit only: within one distributed query the fan-out is pinned to the
  coordinator's serving generation (table UUID, snapshot and schema id), and a
  worker that cannot resolve that generation refuses its shard (`503`,
  `reason: "shard_pin_unresolved"`) rather than contributing rows from another
  one — so a single answer is never merged across generations. A worker that
  only lags refreshes onto the pin; one that is ahead serves the pinned
  historical schema. The cost of that guarantee is availability: while a
  coordinator's cache is stale on a snapshot the catalog has already expired,
  its fanned-out queries return `503` until the cache converges
  (`/api/v1/sql/local` still answers). Raise
  `compactor.snapshotExpire.retainLast` to widen the margin, or lower
  `SIGLAKE_ICEBERG_METADATA_CACHE_TTL_SECS` so coordinators stop serving a
  snapshot before the catalog expires it.
- **Query spill is bounded node-local scratch space, not durable or globally
  observable storage.** The chart and operator provision a 10Gi `emptyDir` per
  query pod, and DataFusion stops at 8Gi so a query fails before kubelet
  eviction. PVC-backed spill for sorts larger than a node's ephemeral-storage
  budget is not implemented. DataFusion exposes current spill use internally,
  but Siglake does not publish a whole-runtime spill-bytes metric until it has a
  dashboard reader; query failures and pod ephemeral-storage remain the signals.
- **Interactive scans slow down during a large backfill, in proportion to how
  far compaction is behind.** A scan's cost tracks overlap depth, and depth
  climbs while ingest outruns compaction: measured at 1TB, the same windowed
  browse ran 23s at depth 254, 0.88s at depth 153, and 0.28s at depth 43.
  Compaction concurrency is derived from the compactor's memory limit and core
  count, so **a compactor sized at the packaged default (1Gi, 2 CPU) merges one
  bin at a time and will lag hardest**; give it memory and cores and it keeps
  up far better. Measured 2026-08-27 with the throttle lifted on an
  adequately-sized fleet: the windowed browse during full-rate ingest went from
  **40.9s to 0.13s**, depth at 1.0B rows from 86 to 57, and post-ingest
  convergence from ~100 minutes to ~30 — with ingest throughput unaffected
  (~1.11M rows/s) and no OOM kills. Narrow time-window queries are unaffected
  throughout (freshness p50 5.4s at 769K rows/s). If interactive search during
  a backfill matters to you, size the compactor rather than expecting the
  default to cope.
- **Below a 4Gi query pod, scan decode memory stops being accounted.** The query
  memory limit is a budget everything
  else is derived from: read caches take 25%, metadata caches 12.5%, and the
  DataFusion pool half the remainder — 0.3125 × the limit, or ~1.25Gi at the
  packaged 4Gi. A scan reserves one file's decode estimate from that pool before
  it opens a second file, and with the packaged compaction policy that estimate
  is 256Mi × 5 = 1.25Gi. The two meet exactly at 4Gi, so **a smaller limit can
  never reserve even the first file**: every scan runs one file at a time and
  holds that file's buffers outside the pool, where the one bound on this
  process cannot see them. That fallback is the safe state — making reservations
  cheaper took a 1TB round from 8.8GiB RSS to a 31.6GiB OOM-kill — so the
  operator renders the StatefulSet and reports the non-blocking
  `QueryMemoryUndersized=True` status condition with reason
  `QueryMemoryBelowDecodeFloor`; unlike `InvalidSpec`, the advisory does not set
  `Ready=False`. **It is not a speed floor.** Measured 2026-09-06 on a
  decode-bound scan (9.87GB decoded per
  query), sweeping the pool 80× around one file's estimate moved the median from
  1.78s to 1.95s — with the starved side *faster* — and pinning per-partition
  file concurrency to 1 under an unconstrained pool cost nothing. Dropping
  partition fan-out from 4 to 1 cost 3.6×, and the pool does not govern
  partition fan-out. So
  `siglake_query_scan_decode_reservation_total{outcome="unreserved"}` is a
  coverage signal, not a latency one — in that sweep the fastest arm had the
  highest unreserved rate. Local storage only; overlapping S3 first-byte
  latency is untested.
- **Query scales by REPLICATION, not by fan-out, for ordinary log search.**
  Adding query replicas multiplies throughput — measured 705 QPS on one
  replica and 2,269 on three (3.22x), with browse p50 flat at 13–21ms through
  16-way concurrency. But it does *not* make an individual search faster,
  because the shapes a log UI issues — small-`LIMIT` browses and the Tier-1
  metadata aggregates — classify as `Local` and are answered by whichever
  replica receives them. Measured 2026-08-18: a coordinator wired to four
  peers served **7,409 of 7,409** queries itself; the peers logged two
  requests each, both health checks. That is the classifier working as
  designed (`SIGLAKE_DIST_SCAN_LOCAL_MAX_LIMIT`, 100,000 — below that,
  answering locally beats paying for fan-out), but the consequence is worth
  stating plainly: **size the query tier for concurrency, not for single-query
  latency.** File-shard fan-out is real and does engage for large scans; the
  selectivity gate that would additionally route *highly selective* browses
  across peers ships default-off (`SIGLAKE_DIST_BROWSE_MIN_SCAN_ROWS`) because
  on a converged layout the distributed arm hung past 120s where a single node
  returned a bounded answer.
- **Admission is per-coordinator; shard work is not admitted.** A distributed
  query reserves one admission share (at most a quarter of the pod budget) on
  the pod that coordinates it, held from admission through the merge, and
  nothing on the workers: a shard is a fragment of a query its coordinator
  already admitted, and admitting it again deadlocked a pod against its own
  ordinal shard and made every distributed query need a slot on every pod.
  Measured 2026-09-05 on a coordinator-plus-peer pair with a four-slot budget:
  `/api/v1/sql` and `/api/v1/sql/local` both admit four concurrent heavy
  aggregates and refuse the fifth, and the peer never trips admission. The
  consequence is that a worker's concurrent heavy shard scans are bounded by
  its memory pool and the per-shard rows-scanned breaker, not by admission,
  and can reach `replicas × 4` at once. A cluster-scoped budget, or a worker
  reservation priced at the shard's own share, is not implemented.
- **Peer membership converges; it is not consistent between queries.**
  `keda.query.maxReplicas` may now exceed `query.replicas`, and the operator
  accepts any valid `autoscaling.query` range (a malformed one still reports
  `InvalidSpec=True` with `AutoscalingRangeInvalid`, a `min` of 0 with
  `AutoscalingZeroFloorUnsupported`, and a non-positive target with
  `AutoscalingTargetNotPositive`, changing no managed workload until the
  spec is fixed). What that costs: a new pod becomes eligible one readiness
  probe plus one DNS refresh (default 5s) after it starts, so scale-out shows
  up on the NEXT query rather than the one in flight; two coordinators may
  briefly hold different memberships, which is correct — each snapshot is a
  complete partition — but makes shard placement non-reproducible across
  queries; and a departed peer's shard is retried only on the coordinator,
  never rebalanced onto a joining peer. Discovery has hermetic coverage
  (normalization, retain-on-error, mid-query join, failover to the captured
  coordinator), and the kind round now drives the scale 2 → 4 → 2 *while the
  query mix runs* (`scripts/kind-round.sh`: a before/during/after sample of
  the cross-shard `GROUP BY` sum against the row count every pod agrees on,
  plus each pod's published membership and shard work, written to
  `results/scale-2-4-2.json` and `results/membership.log`). A recorded kind
  round on 2026-09-11 validated that path from merged commit `b58634b903db`
  with an installed KEDA range of 2–4 replicas: scale-out was requested at
  17:18:09Z and observed at 17:18:15Z; scale-in was requested at 17:18:28Z and
  observed at 17:18:30Z. Its three exact before/during/after samples used
  transparent `/api/v1/sql` aggregate fan-out, with cross-shard `GROUP BY`
  sums matching tenant row counts of 6,000, 6,000, and 6,320 across 2, 4 and
  2 peers. The newly joined `siglake-query-2` and `siglake-query-3` pods each
  answered a pinned shard. That qualifies the local kind gate (#968). The
  S3-backed AWS churn gate (#909) is still open, and the round exercised
  aggregate fan-out only, never the ordered-aggregate path the grader also
  accepts. Design:
  `docs/DESIGN_dynamic_query_peer_discovery_2026-09.md`.
- **The shared Postgres batch-ownership regression is not hermetic.** Recovery
  in the shared Postgres job store is scoped to execution ownership and uses an
  expired owner lease as crash evidence (default 120 s, three missed heartbeats).
  The condemning write now rechecks that evidence, so a heartbeat renewed
  after candidate selection cancels recovery. If recovery wins before a true
  owner's late completion, the terminal `failed` state remains monotonic; the
  client-visible error says the computed output was discarded. If the executor
  has not started, its refused `running` publication drops the query unpolled;
  if it has finished, metrics, warning logs and the audit row report the
  discarded completion instead of falsely claiming success. Planned shutdown
  instead stops the owner heartbeat and removes its
  registration, so the first peer sweep has immediate evidence. The two-store
  regression that pins the lease, late-completion and shutdown paths
  (`crates/siglake-query-server/tests/jobs_postgres_ownership.rs`) is `#[ignore]`d
  and needs a scratch Postgres. The container-image job in qualifying external
  run #46 executed it against the compose Postgres and passed from pinned source
  `f54d07b`, as part of that run's 16/16 green jobs. That is historical evidence
  for the two-store ownership paths. Reconciliation through a real Postgres
  outage remains unmeasured (#1975).
- **Cross-replica cancellation is a poll, not a push, and the poll shares a
  runtime with the work it is trying to stop.** The executing replica learns
  about a cancellation another replica persisted by re-reading its own
  in-flight rows every `--jobs-cancel-poll-secs` (default 2 s), so `202`
  bounds termination at that interval rather than making it immediate; there
  is no `LISTEN`/`NOTIFY` fast path. The watch loop also runs on the batch
  runtime, alongside the batch futures, so a job that occupies a worker
  without yielding delays its own cancellation — the same exposure the owner
  heartbeat has. The hermetic coverage
  (`crates/siglake-query-server/tests/batch_cancel_across_replicas.rs`,
  `batch_cancel_terminal_race.rs`) drives the sweep by hand; only the
  `#[ignore]`d Postgres regression exercises the timer, and neither observes a
  real storage scan stopping mid-flight — that link is
  `tests/pool_returns_to_zero.rs`, on a locally cancelled job. Two of the
  reporting cases the completion gate distinguishes are Postgres-only and
  covered by unit tests over the pure decision rather than by execution: a
  result body installed as `failed` because it exceeds the row's inline cap,
  and a terminal write that errors outright (`actual=unknown`) — the latter is
  additionally driven end to end against the in-memory store through an
  injected write fault, which is not the same as a real connection failure.
- **Reconciliation of finished-but-unpersisted jobs is per-process memory, and
  its live-Postgres half is unmeasured.** The ids a replica finished without
  persisting a verdict are held in that process only: a pod that is killed
  before its next pass loses the note, and the row is then resolved by
  lease-expiry recovery instead (bounded, but a lease later, and as
  `recovered` rather than as the verdict the run computed). The note is capped
  at 1024 ids per replica; past the cap ids are refused with
  `siglake_query_jobs_unreconciled_dropped_total` and an error log, which is
  the same fallback. A reconciled row carries a fixed error text, so a failed
  run's specific message is lost with the write that could not store it, and a
  reconciled *success* is a `failed` row asking for a resubmit — the result
  body is deliberately not retained across an outage. Every assertion is
  hermetic (in-memory store, injected write faults, a store wedged under
  virtual time); nothing has been observed against a real Postgres outage. The
  two ways a row is left with nobody retrying it do page
  (`SiglakeBatchRowStrandedNonTerminal`, on the dropped counter and
  `cause=write_abandoned`); `cause=write_deferred` and the
  `siglake_query_jobs_unreconciled` gauge are deliberately dashboard-only,
  since a rising and then falling backlog is reconciliation working. An
  opt-in, bounded local-kind probe now retains the per-pod outage/reconnect
  trace in `results/postgres-outage-reconnect.json` and grades missing or
  non-draining observations `unverified`; no live round has supplied the first
  measured trace yet (`POSTGRES_OUTAGE_PROBE=1 scripts/kind-round.sh`). Three
  rounds have run it, and none of them measured what it claimed: the retained
  traces carried no evidence that the paused process set stayed stopped, and no
  Prometheus scrape timestamp, so a counter that moved could not be placed
  against the pause. Run #76's trace is kept under `scripts/testdata/` as a
  fixture that has to stay red: its backlog emptied ten seconds before
  restoration while the samples were still labelled `outage`, and the grader
  called it `verified` with a 0.0s drain. The probe now reads every postgres
  process's state and start time on each sample, attempts one bounded write
  before, during and after the pause, and records the container's identity and
  restart count across the window; the grader rejects a trace missing any of
  that, and flags an outage sample with zero backlog and rising completions —
  separately when its scrape predates the pause, which makes it delayed
  observation of pre-pause work rather than a write that landed during it. The
  kind Postgres now starts with `track_commit_timestamp=on` (postmaster-only,
  off by default, set for that throwaway install alone), and the probe dates
  each job row by `pg_xact_commit_timestamp(xmin)` after the bounded recovery
  window; the grader correlates those rows with the accepted submissions,
  reports how many committed inside the pause window, and resolves the
  zero-backlog observation when every accepted job is dated outside it. That
  reading is bounded: the commit timestamp dates the row version visible at
  collection, not every status transition, so a recovered row — which the
  amendment path can rewrite after it went terminal — a missing row, a NULL
  timestamp, a nonterminal job, or a commit inside the second the probe's own
  stamps are truncated to all leave the observation unexplained. No live round
  has supplied a dated trace yet, so what happened in run #76 is still
  unexplained; nothing here establishes a persistence failure.
- **The query server's `/healthz` is a constant 200, so no probe acts on the
  one known query degradation.** `/healthz` answers `ok` for as long as the
  process is serving and `/readyz` only round-trips the catalog. The still-open
  degradation from the 2026-08-22 release gate — a single browse 504s at 60s
  while `count(*)` answers in 0.15s, cured only by a restart — passes both, so
  Kubernetes never restarts the pod, never pulls it from rotation, and a
  coordinator keeps fanning shards to it. Every honest probe is a behaviour
  change on an un-root-caused defect: a readiness trip that fires fleet-wide
  takes the API dark, and a liveness restart ships "restart the query server
  when it gets slow" as the product's answer and can kill a legitimate long
  query. What ships is the signal, not the action: the
  `SiglakeQueryWarmCycleStalled` alert fires when no cache-warm cycle has
  completed for three intervals, the candidate discriminator that the
  degradation has and a slow-but-healthy pod does not. (The ingester's
  `/readyz` does probe WAL writability, because "can I durably ack?" has an
  unambiguous answer.)
- **Mirror-to-catalog reconciliation is page-bounded, and committed mirror
  objects default to 24-hour retention.** The compactor's recovery sweep is the
  only path that repairs "object in the mirror, no catalog row" — the state
  behind the 2026-08-15 incident where 36,122 rows were accepted, durable, and
  permanently unqueryable with zero errors. One elected owner registers at
  most 1,024 sealed objects per pass and stores a shared `(last_key, rotation)`
  cursor in the catalog. Progress advances only after the whole page registers,
  survives owner handoff, and clears at end-of-prefix so a later rotation
  repairs keys inserted or rows lost behind it. S3 continues with OpenDAL
  `start_after`; backends without that capability use a correctness-first local
  filtering fallback. `siglake_compactor_mirror_sync_objects` continues to
  record how many objects each bounded pass examines. Whole-rotation telemetry
  is separate: `siglake_compactor_mirror_sync_rotation_duration_seconds` and
  `siglake_compactor_mirror_sync_rotation_objects` are recorded only when the
  cursor wraps, while durable completion-count and last-completion gauges
  survive an elected-owner handoff. The
  `siglake_compactor_mirror_sync_rotation_objects_examined` gauge publishes
  durable examined-to-date progress after every successful page, continues
  across owner handoff, and resets to the first page's count after a wrap; it
  is progress, not a remaining-object backlog estimate. The dashboard graphs
  that in-progress count alongside completion age and the objects seen by a
  completed rotation, so a flat 1,024-object page series cannot hide a walk
  that has stopped advancing. A panel is not a page, so
  `SiglakeMirrorReconciliationErrors` warns within 30 minutes when listing or
  registering objects fails, and points operators to the compactor error rather
  than making them wait for the whole-rotation window. Separately,
  `SiglakeMirrorReconciliationStalled` fires when bounded passes keep running
  (`siglake_compactor_mirror_sync_total`) while the durable rotation counter has
  not moved for `prometheusRule.mirrorRotationStallSecs` — 21,600s by default,
  above the ~3.5 hours a 218,569-object prefix takes at one 1,024-object page a
  minute; `0` does not render the alert. Both arms are required: pages running
  is normal, and no rotation completing is also what a cluster with
  reconciliation switched off looks like. Because the prefix would
  otherwise grow monotonically, `SIGLAKE_COMMITTED_RETENTION_SECS` (chart
  `compactor.committedRetentionSecs`) defaults to `86400` seconds (24 hours);
  `0` explicitly opts out of purging. Any non-zero value is floored at 901s so it
  outlives the 600s `SIGLAKE_WAL_LOCAL_SWEEP_SETTLE_SECS` delay plus one 300s
  `SIGLAKE_WAL_LOCAL_SWEEP_SECS` cadence. That lets the ingester observe the
  committed row and remove its local sealed copy before a catch-up sweep can
  re-upload and re-register it after mirror retention. When enabled, each
  retention run drains 512-object pages up to a 16,384-object bound, deleting
  objects concurrently before batch-deleting their catalog rows. Runs are paced
  start-to-start, so a backlog does not wait another interval after a long pass;
  even one full budget per default 600s watchdog window is more than twice the
  documented 50K-EPS segment-creation rate.
- **Nothing reclaims mirror objects unless the drain claims from them.** WAL
  mirroring is on by default; the retention pass above belongs to the
  catalog-claim drain, which knows an object was committed because it is the
  thing that claimed it. The default single-replica compactor drains local
  `sealed/` instead and never reads the mirror, so the prefix grows for as
  long as the cluster ingests. The DR copy retains every sealed segment: the
  asynchronous uploader holds a durable `mirror-pending/` link across local
  compaction and retention, and the catch-up sweep resumes pins left by an
  outage or process exit. Nothing expires remote objects. Give
  `<wal.mirror.prefix>/` an object-store lifecycle expiry longer
  than your worst-case drain backlog (the Terraform module adds one for the
  warehouse prefix, not for this one), or run the claim drain
  (`compactor.catalogClaim.enabled`), which purges what it commits. Turning
  the mirror off (`wal.mirror.enabled: false`, or an empty
  `SIGLAKE_WAL_MIRROR_PREFIX`) is the third option, and gives up the
  off-volume copy.
- **The embedded compactor is a single-process shape, and the chart refuses
  it.** `ingest-server --with-compactor` runs the drain and the maintenance
  loop inside the ingester; the dev quickstart and the bench scripts use it.
  It takes no catalog claim, and the chart renders no claim arguments on the
  ingester, so `ingester.extraArgs: [--with-compactor]` fails the render at
  any replica count — one replica included, because the ingester rolls with
  `maxSurge: 1` and the outgoing and incoming pods overlap long enough for two
  of them to run maintenance against one table. Supporting it would take a
  claim on the ingest binary plus a way to hold exactly one embedded compactor
  across a rollout; until then the dedicated compactor tier is the deployed
  path, and it is the default.
- **`?commit=force` is refused when a remote catalog-claim drain consumes the
  mirror.** Force proves a commit by watching the segment leave the local
  `sealed/`+`processing/` directories. A claim drain commits from object
  storage, where that movement is not observable, so the API returns `400`
  instead of waiting 30 seconds and returning a false `504`. The default
  single-replica local drain supports force even while WAL mirroring is on.
  With `compactor.catalogClaim.enabled`, use `?commit=wait_for` (the default ack
  mode) and query for visibility.
- **A claimed delete task is never un-claimed; a claim leaked by a crash
  strands the task.** Each task is its own warehouse object
  (`_siglake/config/delete_tasks/<namespace>/<task_id>.json`), so submissions
  and status writes never touch a key another writer owns — that is what makes
  two replicas' acknowledgements both survive. The record itself is still
  last-write-wins, so ownership of a task is a separate object: before executing
  a pending task an executor create-only-writes a sibling
  `<task_id>.claim` key (`if_not_exists`, which both the fs and S3 backends
  support), and only the winner runs it. A loser reports the task as
  `tasks_already_claimed` and touches nothing; a store that cannot do a
  create-only write fails the sweep rather than executing unclaimed. That covers
  every entry point, including `siglake delete-tasks execute` run by hand and a
  second control plane with no catalog configured, where the compactor's
  `delete_tasks` maintenance lease does not reach. What it does NOT do is
  release: the claim survives completion, failure and a crash mid-rewrite, on
  purpose — an ambiguous failure is exactly when a second executor must not
  start, and a takeover on claim age alone, with no fencing token the Iceberg
  commit could check, would let the evicted owner's rewrite land after its
  successor's. So a process that dies between claiming and executing leaves a
  task `pending` forever; it recovers by the same explicit resubmission a
  `failed` task takes (a new task id, and so a new claim key). Dry runs claim
  nothing. **Nor is a terminal task's claim collectable**, which is the version
  that looks free: a `done`/`failed` task is in no pending set, so its claim
  reads as litter. But a sweep works from a pending set read *before* it claims,
  so a delayed executor can still be holding a `Pending` copy of a task another
  executor has since finished, and it never re-reads the record — the surviving
  claim is the whole of its exclusion. Remove the claim and that executor claims
  the task again, re-runs it, and its terminal record replaces the first
  executor's `files_rewritten`/`rows_deleted` (or its `error`) with a second
  run's verdict: the audit trail of what the deletion did is exactly what a
  claim sweep costs. Observing a terminal record is therefore not a safe
  precondition for deletion; reclamation would need a protocol that excludes
  delayed executors — a fencing token or a generation the Iceberg commit checks
  — and a measured reason to pay for it. The price of keeping them is one extra
  LIST entry per task: the prefix holds two objects per task, and listing costs
  one LIST plus one GET per task (the reader takes `*.json` only, so claims
  inflate the LIST and not the GETs), which is fine at GDPR request volumes and
  is not a design for millions of tasks. Ledgers written by
  builds before this layout are read (and merged under the per-task records) but
  never rewritten, so nothing migrates itself.
  The tenant-scoped `GET /api/v1/delete-tasks/{id}` exposes these facts for a
  pending task under a `claim` object. `present` means only that the sibling was
  observed at `observed_at`; a readable body also supplies `claimant`,
  `claimed_at`, and `age_seconds` (wall time since claim creation, not execution
  duration). An empty, truncated, malformed, or task-id-mismatched body remains
  visibly present with those three diagnostics null, because presence is what
  excludes another executor. Absence means only “not observed during this
  read.” Neither presence, UUID nor age proves liveness, abandonment, commit
  outcome, or safe takeover, and the API does not release, retry, or expire a
  claim.
- **A queued delete task belongs to the index incarnation it was accepted
  against, not to the index name.** An index id is reusable: `DELETE
  /api/v1/indexes/logs` followed by a `POST` of the same id is a different
  Iceberg table under the same name. Submission therefore records the
  `table_uuid` the server resolved the id to — read from the same metadata load
  that supplied the config it validated — and the executor requires that table
  to be the one it is about to rewrite. It checks twice: when it loads the
  table, and again against a fresh catalog read before the commit, because the
  rewrite in between is not instantaneous. A task whose id now resolves to a
  different table is refused, goes `failed` with an `error` naming both tables,
  and recovers by resubmission against the index that exists; a deletion
  authorised for rows that were dropped with their index never touches the
  replacement's. Refused after the rewrite ran, the output files are
  unreferenced and orphan-file GC reclaims them — the `error` says so — and no
  snapshot was committed. A task recorded before this binding existed carries a
  null `table_uuid`: it stays readable, is never rewritten or migrated, and is
  refused with the same resubmission instruction rather than bound to whatever
  its index id means today. Nothing infers an incarnation from a name, which is
  the point.
- **A `failed` delete task is terminal; recovery is an explicit
  resubmission.** The executor takes `pending` tasks only and no surface returns
  a terminal task to `pending` — there is no retry API, no automatic retry and
  no `failed → pending` transition. The same holds for a task stranded in
  `running` by a crash or by a status write that failed after the rewrite
  committed, and for one left `pending` under a claim its executor never
  released: nothing sweeps either up, and both recover the same way. A warehouse
  that hit a bug which failed
  ordinary requests (the pre-0.1.0 NULL-complement defect failed any predicate
  on a nullable column) therefore holds acknowledged 201 deletions that will
  never execute. The recovery, per request: `GET /api/v1/delete-tasks/{id}` and
  read `error` for the failure cause; remedy that cause and make sure the
  executor runs the fixed build; POST `index_id`, `predicate_sql`, `start_ts`
  and `end_ts` — request fields only — to `/api/v1/delete-tasks` again under the
  same tenant identity, which answers with a NEW `task_id` in `pending` while
  the failed record and its `error` stay exactly as they were (record both ids
  in your audit trail); then confirm delete-task execution was not turned off
  for this deployment — it is on by default and runs on the compactor's
  maintenance sweep under the `delete_tasks` lease, so do not run a second,
  manual executor alongside it — and poll the new id to `done` or `failed`.
  This is a resubmission, not a retry: it is not idempotent at the HTTP layer,
  it promises nothing about exactly-once execution, and each task reports only
  what its own run rewrote, never the request's cumulative effect. On an
  unchanged dataset with the same predicate and the same fixed bounds, a repeat
  finishes with `rows_deleted: 0`, `files_rewritten: 0` and commits no snapshot;
  a time-dependent predicate, or rows that arrived since, makes it delete a
  different set. A task that failed BEFORE its commit may have left unreferenced
  output files behind — inert, and reclaimed by orphan-file GC — not a partly
  deleted snapshot, because the whole task commits once
  (`execute_one_delete_task`). What a terminal state cannot prove is the other
  side of that commit: a task recorded `failed` because the commit result was
  ambiguous, or because the status write itself failed afterwards, may have
  committed its rewrite. Check `rows_deleted` on the new task against what you
  expected before concluding the first run did nothing.
  You are told when a CLAIMED task is left non-terminal, and only that. Before
  it executes anything, each delete sweep observes every non-terminal task in
  each namespace once — read-only: it writes no record, no state and no claim —
  and reports the ones whose claim object is older than twice the sweep's own
  watchdog ceiling (`SIGLAKE_DRAIN_WATCHDOG_SECS`, so 20 minutes on defaults;
  `0` disables the ceiling and with it any notion of stalled). There are two
  such states and no others: `running`, a task claimed and started that never
  reached `done` or `failed`, and `pending_claimed`, one whose executor died
  between taking the claim and writing `running`. A fresh claim on either is an
  execution in progress — the shape two racing executors produce while both are
  alive — so only the bound separates contention from a stranding. A `pending`
  task with NO claim is outside the signal entirely: it is indistinguishable
  from one waiting for the next sweep, or from one no sweep will ever run
  because delete-task execution is off. Each reported task gets
  a WARN carrying its id, index, state, claim age and that bound — never
  the predicate — at most ten per namespace per sweep and at most once per task
  per bound interval; `siglake_compactor_delete_tasks_stalled_total{state}`
  counts them for `SiglakeDeleteTaskStalled`, and
  `siglake_compactor_delete_tasks_nonterminal{state}` is a dashboard-only count
  of the last complete observation. Read it honestly. The claim age is wall
  time since exclusive ownership was taken, which bounds execution duration
  from ABOVE and is not a running duration — the claim is never released, so
  the number keeps growing after the task strands. A stalled report proves
  neither that the executor died nor that its rewrite failed to commit: a
  crash, a status write that failed after the commit landed, and a watchdog
  cancellation leave byte-identical records, so the recovery above — and your
  audit trail — is still what decides whether the original rewrite happened.
  Nothing here retries, resets or reclaims anything. Coverage is the sweep's
  own: with delete-task execution disabled, with the `delete_tasks` lease held
  elsewhere, or for a tenant with tasks but no WAL directory, there is no
  observation and no signal — which is why the alert reads counter movement and
  no rule treats a quiet gauge as health.
- **Index templates are safe against concurrent WRITERS, not against two
  writers of the SAME template.** Each template is its own warehouse object
  (`_siglake/config/index_templates/<namespace>/<template_id>.json`), so a PUT
  or DELETE of one id never touches another id's key — that is what makes two
  replicas' acknowledged edits both survive. Two writers of the same id in one
  namespace are still last-write-wins: there is no CAS, no conditional PUT, and
  no version in the request. A DELETE leaves a tombstone record rather than
  removing the object, because an older record may remain read-only underneath;
  tombstones are never garbage-collected. Listing costs one LIST plus one GET
  per template, which is fine at the handful-of-templates scale this is for.
  Builds before 0.1.0 stored templates at the warehouse root. Those records are
  still read only by the configured default namespace and are never rewritten
  or deleted; named tenants must re-PUT their templates after upgrading and do
  not inherit the old shared list.
- **Table subscriptions do not support an external writer's overwrite
  semantics.** `siglake subscribe` / `IcebergSubscription` deliver only the
  files an *append* commit added; every non-append commit in a poll interval is
  skipped and the cursor advances past it, which is what keeps continuous
  compaction (re-cluster, retention, delete tasks — all Iceberg `overwrite`
  commits whose replacement files are `ADDED`) from re-delivering rows a
  consumer already has. siglake marks its own rewrites with the
  `siglake.rewrite` snapshot-summary property; a non-append commit written by
  another engine (Spark `INSERT OVERWRITE`, `MERGE`, a row-level delete) is
  skipped too, so any rows it genuinely added are never delivered. It is
  counted as `siglake_subscription_rewrite_commits_skipped_total{origin="foreign"}`
  and logged at WARN, and the recovery is a SQL query over the interval — the
  same answer a history gap gets. Nothing in a manifest distinguishes a
  replacement file from a new-row file inside one commit, so there is no
  delivery rule that serves both writers. See
  [docs/CONSUMING_SEGMENTS.md](CONSUMING_SEGMENTS.md).
- **Only the latency (`*_seconds`) histograms and four per-call count
  histograms are exported in Prometheus histogram form.** The exporter renders
  a `metrics::histogram!` as a summary (`quantile` label, no `_bucket` series)
  unless the recorder is handed buckets for that name; until 2026-09-03 no
  histogram had any, so the dashboard's p99 panels and the KEDA queue-wait
  trigger read nothing. `siglake-core`'s `metrics::builder` now sets buckets
  for every `*_seconds` name plus `siglake_group_count_deltas_folded`,
  `siglake_group_count_tier2_files_per_call`,
  `siglake_compactor_mirror_sync_objects` and
  `siglake_compactor_mirror_sync_rotation_objects`. The `_bytes` / `_rows` /
  ratio and other count families deliberately remain summaries: no shipped
  consumer needs cross-pod quantiles for them, and their ranges differ enough
  that one generic bucket layout would give poor resolution. If a future
  consumer needs an aggregatable quantile, choose buckets for that family in
  `crates/siglake-core/src/metrics.rs` before putting a `_bucket` query on it.
- **An `increase()` alert on a counter with a per-event label misses the first
  event.** The `metrics` crate creates a series on its first write, and
  Prometheus `increase()` needs two samples, so a counter's first increment on
  a fresh pod was invisible to every `increase(...) > 0` rule. Each binary now
  pre-registers the counters its alerts read at 0 on startup
  (`siglake_core::metrics::preregister`; the lists live in
  `crates/siglake-core/src/metrics.rs` and `check-chart.py` holds the
  PrometheusRule to them), so those series show 0 rather than no data and the
  first increment is a visible delta. Only counters, not gauges. What cannot
  be pre-registered is a series whose label value is only known at the
  increment: `siglake_storage_schema_drift_total{column=...}` (the alert fires
  on the second refusal, which the next drain cycle produces) and
  `siglake_group_count_delta_write_failures_total` for index tables (the
  events table is pre-registered).
- **Mixed-version claim-reclaim rollout needs temporary snapshot headroom.**
  New writers atomically maintain the bounded `siglake.consumed_proof.v1`
  table property, so committed-claim evidence survives snapshot expiry and
  reclustering. A new writer removes each proof entry once the matching
  catalog row is terminal, even when an older non-terminal row pins the time
  watermark. The filesystem drain applies the same bound from its owned WAL
  directory: entries under `committed/`, or absent after quarantine disposition
  and retention sweeps, are removed on the next append; entries still under
  `sealed/`, `processing/`, `orphans/` or `poison/` remain. The v1 property format is
  unchanged. Old writers know only the retained snapshot summaries. Before a
  rolling upgrade, set `compactor.snapshotExpire.retainLast >= 400`; keep it
  there until every old drain/maintenance writer is gone and for another 1,025
  seconds. The dual reader uses both sources during that interval. Corrupt or
  over-cap durable state refuses writes and makes uncovered reclaim decisions
  unprovable rather than silently discarding positive proof. During a refusal,
  `siglake_consumed_proof_current_watermark_lag_seconds` continues to report
  the boundary offered by each attempt, while
  `siglake_consumed_proof_watermark_lag_seconds` retains the lag recorded by
  the last successful property update.
- **Relevance scoring (BM25)** — log search results sort by time, not score,
  and term frequency degenerates on one-line templated log text; a full
  design (per-file df-sketch footers, snapshot-keyed corpus stats, scored
  TopK) is on file in `docs/DESIGN_bm25_scoring.md` if that calculus changes.
- **Aggregating merge kinds** (rollup / LWW dedup) — the exact rows-conserved
  commit guard is per-merge-kind-waivable by design, but no such kind exists
  yet.
- **WAL durability model** — acks are fsync-based by default, but remain local
  to the WAL. The mirror is on by default and asynchronous: a segment reaches
  the object store after it seals, so the ack→upload window is real and an
  acknowledgement is not remotely durable. `wal.mirror.activeIntervalSecs`
  snapshots the in-flight segment every N seconds, which narrows that window
  to N seconds on one recovery path: an operator runs `siglake wal-recover` to
  rebuild the WAL root from the mirror, and the filesystem drain commits the
  recovered segments. No restart consumes an active snapshot by itself, and
  the catalog-claim drain (`compactor.catalogClaim.enabled`) reconciles sealed
  objects only — it never reads `_active/`, so it has no N-second target. An
  object-store group-commit PUT-as-ack design is a flagged decision for a
  future cycle. A
  crash during the NEXT append leaves that final Arrow IPC message incomplete.
  A recovered PARTIAL frame serves and drains every complete fsynced batch
  before it, leaves the original bytes intact, and reports the discarded tail
  through a warning and `siglake_wal_partial_tail_dropped_total`. A partial
  without one complete batch still fails decoding; complete sealed frames and
  legacy segments retain their all-or-nothing integrity checks. The ack is also only as
  durable as the filesystem under the WAL: siglake syncs the segment's bytes
  and every directory entry that names it, and assumes those syncs reach the
  device. A network filesystem answers `fsync(2)` on its own terms and
  siglake measures none of them, so ext4 or xfs on a node-attached volume is
  the substrate the power-loss claim is made for.
- **Attribute auto-promotion is opt-in** — hot-key sampling and promotion of
  OTLP attributes to typed columns ships default-off
  (`SIGLAKE_AUTO_PROMOTE_MIN_PCT`); promoted keys can also be listed
  explicitly per table.
- **Tier-1 group counts serve only columns present since the table's first
  commit.** The serving guard is `column_total(column) == record_count`, so a
  column that starts accumulating mid-life — a typed column on a table created
  before typed columns joined the side aggregate is the common case — is
  permanently short and every `GROUP BY` on it falls to the exact per-file
  Tier-2 path: correct answers, no speedup, `served_by: "materialized"` for the
  life of the table. `siglake rebuild-group-counts --table <t>` (with
  `--admit-typed-columns` for the older-table case) backfills the total from
  the committed files. Nothing automatically admits a pre-existing typed
  column: the maintenance census measures a shortfall only against the columns
  the aggregate already carries, and neither it nor the lost-delta repair widens
  what a table maintains — that is an operator decision. A benchmark that
  recreates its table every round never sees the
  older-table case; a long-lived table does, so do not read a Tier-2 result on
  an old table as the fast path failing. Rebuilds repair only the wide Tier-1
  object: they do not backfill the inline object, so repaired columns remain
  `tier1_wide` even below its 4096-entry cap and may pay a wide-object fold on a
  cold metadata cache.
- **The short-aggregate census reports more than it repairs.** Every 15 minutes
  the maintenance pass finds a maintained column short of `total-records` with
  every commit's contribution accounted for and fires
  `siglake_group_count_short_aggregates_total` /
  `SiglakeGroupCountAggregateShort`; rebuilding it is opt-in
  (`SIGLAKE_AGG_SHORT_REPAIR=1`, `compactor.shortAggregateRepair`) and budgeted
  at one table per pass, because the rebuild is one Tier-2 query per maintained
  column — ~9 minutes per column per 250M rows measured on a local filesystem.
  Three gaps follow from that shape. A table wide or large enough for the
  rebuild to exceed the compactor's watchdog (600 s) has it cut, publishes
  nothing, and retries on the next pass with no durable backoff, so a
  persistently trippable table needs the knob off and
  `siglake rebuild-group-counts --table <t>` run once by hand;
  `siglake_compactor_watchdog_trips_total{stage="agg_short_repair"}` is the
  signal. A shortfall the coverage rules cannot bridge to the current snapshot —
  a foreign overwrite or a delete task as the newest commit — is never censused,
  because that state is indistinguishable from a contribution still in flight.
  And a table with no exact map at all (every column sketched, or no aggregate
  object yet) has nothing to measure a shortfall against.
- **Pre-coverage side aggregates are not adopted.** Inline group counts, time
  buckets and 2-D time×group counts written without a snapshot-coverage chain
  remain readable but cannot prove which equal-row-count snapshot they
  describe, so queries use the exact per-file tiers. `rebuild-group-counts`
  restores the folded wide group-count object; `rebuild-time-aggregates`
  restores the inline object's time buckets and 2-D time×group counts. Nothing
  repairs the condition automatically: further appends publish coverage edges
  that never join a chain with no head, and a row-conserving re-cluster has no
  edge to walk back to
  (`crates/siglake-storage/tests/storage/pre_coverage_time_agg.rs`). Measured
  on that file's report, the fallback costs 23–59× Tier-1 warm but stays under
  ~2ms, and 3.8–35× cold over 49–168 live files, growing with the file count —
  so a cold, large, rarely-queried table is where it is felt. Three limits on
  the repair: it drops the inline whole-table group counts rather than certify
  maps it cannot prove (they were already refused, so nothing readable is
  lost, but an unwindowed `GROUP BY` below the raised cardinality cap stays on
  Tier-2); it leaves absent any component short of `total-records`, which a
  table with delete files or NULL timestamps always is; and it cannot merge a
  commit that lands under it, so on a table under live ingest it retries three
  times and exits without writing. See
  `docs/DESIGN_inline_time_aggregate_rebuild.md`.
- **A row-removing commit retires the side object until it is rebuilt.**
  Retention and a delete task rewrite files without conserving rows, so the
  object's counts describe a generation that no longer exists: they exceed
  `total-records`, the read guard refuses them, and the chain cannot bridge the
  commit either. Nothing on the commit path recomputes them — the counts are
  cumulative, and a commit knows only its own delta — so the table answers from
  the exact per-file tiers until `rebuild-time-aggregates` runs. A foreign
  overwrite (a writer that is not Siglake) is the same state and deliberately
  unbridgeable, because an unmarked N-for-N overwrite preserves the row total
  while changing every answer. Snapshot expiry no longer joins this list: it
  re-roots the edge onto surviving ancestry rather than orphaning it. Two
  residual windows there, both costing acceleration and never an answer, and
  both repaired by the same command: a process that dies between the expire
  commit and the re-root write, and an append that publishes in that window on
  a store with no conditional write, where single-writer-per-table is the
  correctness story for every side-object write.
  `crates/siglake-storage/tests/storage/orphaned_coverage_repair.rs` pins the
  repair after a delete task, the re-root across an expiry, and the refusal to
  certify an object whose rows a delete task removed. The state is reported by
  name: the maintenance compactor's 15-minute inline-coverage census sets
  `siglake_inline_coverage_unproven{iceberg_namespace,table}` for every table it
  reaches a verdict on, and `SiglakeInlineCoverageUnproven` (critical) names the
  table and renders the `rebuild-time-aggregates` line for it. Three limits on
  the census: it reports, it never rebuilds (automating the repair is separate
  work); it says nothing about a table whose object it could not read, leaving
  the previous reading standing rather than writing one it did not observe
  (a table it stops reaching altogether, a dropped index, is zeroed instead);
  and the gauge is a last observation, so the alert carries
  `increase(siglake_inline_coverage_census_total[1h]) > 0` as a liveness arm to
  keep a compactor that stopped censusing from paging off a stale reading.
- **Streamed rewrite output carries no inline inverted index.** Every rewrite
  past the in-RAM caps — a leveled compaction merge, a re-clustering pass, or
  a delete task's large candidates (16 MiB compressed / 128 Ki rows) — is
  written by the streaming writer, which builds the group-count, time-bucket
  and raw row-group-bloom footers but not the footer inverted indexes and not
  the whole-file raw trigram bloom. Both are computed from the whole decoded
  batch, which is what the streaming arm declines to hold. The post-commit
  rebuild pass covers part of that and is **off by default**, so a
  default-shaped install queries a streamed merge's output by scan. Turned on
  (`SIGLAKE_INDEX_REBUILD=1`), it registers Puffin inverted-index sidecars for
  the columns the enabled index specifications name — not inline footers in
  place of the ones the streamed write skipped, and not the whole-file trigram
  bloom, so `LIKE '%substr%'` on `raw` prunes against per-row-group blooms
  alone until some later in-RAM rewrite covers the file. Under
  `SIGLAKE_INVERTED_INDEX=0` there are no specifications to rebuild from.
  Turning either switch back
  on does not backfill what is already committed: a rebuild only ever sees the
  files of the rewrite it follows, and only a rewrite of the file itself
  restores an inline footer index or the whole-file bloom. A table with
  `index_at_flush: false` also defers the in-RAM delete-rewrite arm's indexes,
  because a delete rewrite writes at generation 0 like an ingest flush. The
  consequence throughout is pruning: an unindexed or unbloomed file is scanned
  and row-evaluated instead of skipped, and returns exact rows.
- **A re-cluster bin may span partitions only while it fits in RAM.**
  `IcebergContext::recluster_files_with` takes a bin of data files and merges
  it into replacement output. The in-RAM merge splits its output by partition
  value, so a bin holding two days rewrites into one correctly-stamped file
  per day. The streaming executors — every rewrite past the in-RAM caps —
  write through a single writer stamped with one partition value and cannot;
  they now REFUSE a mixed bin before writing anything rather than commit rows
  under the wrong partition value, where a timestamp-predicated query prunes
  them away while `count(*)` still counts them (#4200). The dispatch is chosen
  by bin size and the `SIGLAKE_RECLUSTER_*` knobs, so a caller that cannot
  bound its bins must group by partition value and call once per group, as
  both shipped planners (`recluster_pass`,
  `recluster_all_indexes{,_leveled}`) do. Automatic regrouping is deliberately
  not done: bin budgets (`max_pass_bytes`, the rewrite-generation cap) are
  stated per output file.
  `crates/siglake-storage/tests/storage/recluster_cross_partition.rs` pins the
  refusal on each streaming dispatch, the in-RAM fan-out, and window
  visibility either way.
- **Search v1 limits:** FTS pruning engages only on columns with index blobs
  (others row-eval); the current metadata path retains Puffin statistics
  registration after its data snapshot expires; strict mapping mode enforces
  at commit time as lenient-plus-counter.
- **Operator adoption of existing Helm releases** is offline in v1: the
  operator synthesizes a `SiglakeCluster` from chart values and preflights
  name/selector parity, but the ownership handover (annotation flip + helm
  release-secret removal) is a documented manual runbook, not automated.
- **Jaeger** is the HTTP query subset (no gRPC SpanReader). Its render
  ceilings are an EMPIRICAL guardrail, not a proven bound on process memory:
  they bound the render's INPUT (span rows, accumulated Arrow bytes, distinct
  names, `?limit=`) using coefficients measured on one pod answering one
  request at a time — 8.5 KiB of peak per span row, 4x over Arrow, 1.4 KiB per
  name, 12 KiB per trace, all rounded conservatively out of the request's
  admission reservation. What
  sits outside them: allocator slack, concurrent requests (the ~80 admissible
  on a packaged pod are arithmetic, not a measurement), and any corpus whose
  per-row cost is worse than anything measured. The pool (`503`) and admission
  (`429`) remain the bounds that actually account for bytes. Two further gaps:
  the render itself is synchronous, so the request deadline cannot interrupt
  the `serde_json` walk once it starts (bounding its input bounds it in
  practice), and bounding the two list routes' OUTPUT does not bound their
  scan. They are unpredicated full-table aggregates, and the FIRST poll of
  every snapshot still pays one in full — over a local 2-million-span/100-file
  fixture that was 54–66 ms for services and 90–106 ms for operations. Repeats
  on a standing snapshot no longer do: the two lists reach the same
  snapshot-keyed result cache SQL uses (see *Caches* above). What is still
  uncached: a name list past the 512 KiB one entry may retain, which is
  executed and returned WHOLE on every poll rather than truncated to fit. That
  is ~43,700 8-byte names, ~10,000 at 48 bytes and ~3,970 at 128 — so it is
  wide names, not many, that keep paying the aggregate on every poll, and
  SQL's global caps are not raised to accommodate Jaeger. No time window,
  result limit, or TTL cache will change the all-history list semantics.
  A refusal also overshoots its bound, by up to the ROW ceiling's worth of wide
  rows: the plan carries a fetch of one row past the row bound, so the span
  query's sort is a bounded `TopK` rather than a blocking sort over the whole
  match, but 1,687 rows of an arbitrarily wide corpus is 26.77 MiB of Arrow at a
  16 KiB payload and unbounded in principle. Measured, a refused `?limit=200`
  over 16 KiB-per-span traces peaks at 56.77 MiB — against 322 MiB when only
  accumulation was bounded, and 643 MiB (with a 160 MiB body) with no ceiling at
  all — and a refusal whose rows fit under the fetch does not improve at all,
  because the producer bound is in rows and the ceiling that binds there is in
  bytes. Concurrently buffered batches are outside all of it.
- **No built-in UI.** There is no bundled query or alerting front-end; the
  query tier is reached through SQL over HTTP and the Elasticsearch- and
  Jaeger-compatible shims. What does ship for operations: a starter Grafana
  dashboard (`deploy/grafana/siglake-overview.json` — import it yourself, the
  chart does not render it) and a `PrometheusRule` with 34 alerts grouped by
  what an operator should do (data-loss, stalled, refusing, saturation),
  rendered when `prometheusRule.enabled` is set (default off). No metrics
  downsampling; retention is file/day-granular (no row-level retention).
