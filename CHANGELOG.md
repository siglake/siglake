# Changelog

## Unreleased

- **Query observability (feature)**: the Puffin blob cache reports what it is
  doing on `/metrics`. `siglake_iceberg_puffin_blob_fetches_total` counts the
  index blobs a process read from object storage,
  `siglake_iceberg_puffin_blob_cache_lookups_total{outcome}` the decodes handed
  bytes it still held against those that had to read, and
  `siglake_iceberg_puffin_blob_cache_evictions_total{reason}` which arm of the
  eviction rule chose each victim — `redundant` for a blob whose parsed twin is
  resident and which therefore cannot be read until that twin goes, `stale` for
  one nothing read while the cache turned over four times, `fifo` for the
  fallback the coupled rule replaced. All three were process-wide diagnostics
  readable only from a test, so a deployment could infer the blob cache's
  behaviour only from index-phase object-store bytes against the parsed cache's
  miss rate: that is how #4182's refetch regression — every execution re-reading
  every blob of a 14-file plan, 4.60 GB over 183 index-phase reads against 0.50
  GB over 73 — stayed invisible for a round, and why the miss rates in its
  report had to be inferred from `first_batch_ms`. Every series is
  pre-registered at 0 on the query server, so a tier that has served no text
  query charts zero rather than "No data", and the "Puffin blob cache fetches /
  hits / evictions" panel reads them beside the parsed cache's own. Eviction
  behaviour, both cache defaults and the diagnostics are unchanged; the counters
  are additive. (#4718)

- **AWS reference deployment (fix)**: `deploy/aws/down.sh` settles
  `SIGLAKE_DOWN_MODE` before it writes to the cluster. The check sat on the
  destroy's own `case`, after the `helm uninstall`, the Postgres Secret, PVC
  and namespace deletes and the optional warehouse sweep had run, so a typo'd
  mode took the workload out of the cluster, destroyed nothing in AWS and
  exited 1 — the smoke run gone and the billing resources still up. An
  unrecognised mode now exits 1 naming the value, having run no `helm`, no
  `kubectl delete`, no `aws s3` and no `terraform destroy`. `cluster` and `all`
  behave as before, including their resource selection and the destroy's exit
  status; `scripts/check-aws-down-destroy.sh` covers the rejection with the
  default warehouse handling and with `EMPTY_WAREHOUSE=1`, which is the arm
  that carried the sweep. (#5332)

- **AWS reference deployment (fix)**: `deploy/aws/down.sh` exits with
  terraform's status when the destroy fails. The script runs without errexit
  and ended with `log "down complete"`, so its status was that log call: a
  failed `terraform destroy` — targeted in the default keep-EKS mode, or the
  full stack under `SIGLAKE_DOWN_MODE=all` — exited 0 with RDS, the warehouse
  bucket and the IAM role still running and billing, and the caller read the
  teardown as finished. AWS rounds have already hit `VcpuLimitExceeded` from
  instances an earlier teardown left behind. The `helm uninstall` and
  `kubectl delete` steps before the destroy stay best-effort, and the
  resource selection in both modes is unchanged;
  `scripts/check-aws-down-destroy.sh` covers the six cases under stub
  binaries, including a run whose cleanup fails at every step and still
  reaches the destroy. (#5309)

- **Recovery (feature)**: `siglake wal-recover --catalog <uri>` settles
  whether `--from` is the mirror root from the catalog instead of from a
  marker. A mirror with no managed index and no active mirroring — the default
  install — carries neither of the markers #4973 reads, and its listing one
  component too high is indistinguishable from a legitimate mirror whose first
  tenant is named after a prefix; a restore of that listing invents a tenant
  named after the mirror prefix. The uploader recorded
  `(tenant, index_id, segment_url)` for every object it PUT and none of the
  `UPDATE`s in the claim path touch those three columns, so the listed segment
  ids can be looked up and the routing each KEY implies compared against the
  routing the ledger recorded. The lookup runs on the plan's own listing,
  including the ids of keys recovery refuses on their layout — which is the
  whole of a deep mirror listed one component up, and turns the generic
  "restored nothing" bail into a refusal naming the directory to pass instead.
  Only the TAIL of `segment_url` is compared with the listed key, never its
  head against `--from`, so a mirror copied into another bucket is a legitimate
  source; the leftover above the key is the mirror prefix, and an empty
  leftover is the signal that `--from` already carries it. One agreeing match
  confirms the root, since the root is a property of `--from` rather than of an
  object; a disagreement, or two matches claiming different prefixes, refuses
  the restore whole, reports both routings and reroutes nothing. Objects with
  no row — retention deletes a row as soon as its object is gone, so this is
  the ordinary case — keep the routing their key implies and are counted as
  uncertified in the plan. Read-only mechanically rather than by discipline:
  SQLite is opened `mode=ro` and Postgres runs its SELECTs inside
  `START TRANSACTION READ ONLY`, so the reader cannot reach the `ensure_schema`
  that connecting through `SqlSegmentClaim` would have run against the catalog
  a plan is inspecting. The flag adds evidence and removes no refusal: a
  contradicting marker still refuses whatever the catalog says, and a catalog
  that cannot be read fails the run rather than falling back to the marker
  verdict — the remedy is to drop the flag, and the message says so. No env
  default, no new requirement, and a mirror with no `--catalog` behaves exactly
  as before. `docs/LIMITATIONS.md` has what it does not certify, including the
  one known false refusal (a `wal.mirror.prefix` changed mid-life) and the
  absence of a live Postgres arm. (#4997)

- **Text indexes (feature)**: an opted-in streaming re-cluster builds the
  compressed segmented inverted index (`seg2`) as it emits Parquet row groups,
  then registers the completed Puffin statistics file in the same transaction
  as the data-file rewrite. The writer holds one row group's parsed postings at
  a time, validates the sidecar's group rows against the Parquet footer, and
  produces one blob per output file and indexed column when a rolling rewrite
  splits. A failed transaction leaves no discoverable index, and the existing
  post-commit rebuild recognizes seg2 coverage instead of decoding the output
  file again. Query discovery recognizes seg2 and retains existing whole-file
  v1 reads. The unreleased seg1 prototype is no longer discovered and no longer
  suppresses a rebuild; its pinned bytes remain a decode-only codec test.
  `SIGLAKE_SEGMENTED_INDEX_WRITES=1` and the
  separate `SIGLAKE_SEGMENTED_INDEX_READS=1` are both required to build and use
  the format; both remain off by default pending AWS qualification. On the
  14 x 7.34M-row acceptance corpus, seg2 averaged 282.77 seconds of rewrite
  time and 251.4 MiB peak tracked heap, 12.0% faster and 80.8% smaller than the
  post-commit v1 rebuild it replaces. If that rebuild finds an uncovered file
  on the rewrite's snapshot, it preserves the registered seg2 blobs and counts
  the deferred v1 registration instead of replacing them. The query-path report
  now builds its segmented fixture through the streaming seg2 rewrite and
  refuses retained seg1 fixtures. At 14 × 7.34M rows its two rare scans were
  0.14x and 0.06x the scan, with exact answers and matching Parquet layouts;
  the dated seg1 columns remain as history. A rewrite that rolls its output
  into many files holds every finished sidecar until its transaction publishes
  them: measured over one same-partition rewrite from 3 to 40 rolled outputs,
  against a matched control with the writer off, that retention moved the
  rewrite's peak live heap by under 300 bytes. The peak is the merge's own
  buffers plus one row group's parsed index, and the retained bytes track rows
  rather than files — 2.40 B per row per indexed column, whatever the rolling
  target. The heap figures above are peak tracked live bytes over append plus
  every rewrite in an arm.
  (#4377, #5228, #5230, #5233, #5234, #5299)

- **Text indexes (docs)**: the documented integrity gap in a v1 inverted-index
  blob is the **footer-KV** path only. An index stored as hex in a Parquet
  footer — the path taken per column while that column's serialized index fits
  `SIGLAKE_INDEX_FOOTER_MAX_BYTES` (1 MiB), so the small and freshly written
  files — has nothing covering its stored bytes, and a corruption that still
  decodes and still covers the file's rows prunes with it: the query succeeds
  and answers short. The Puffin sidecar above that threshold, which carries the
  large compacted files, is covered by the codec it is written with: none of
  19,888 single-bit flips of the stored frame produced a wrong answer, against
  5,883 of 19,856 with the frame's content checksum off, and its failure mode
  is a failed query rather than a fallback to a scan. Warm, neither path
  re-verifies: a cached parsed index answers from the parse until it is
  evicted. `docs/LIMITATIONS.md` now states the exposure per path, and
  `docs/DESIGN_inverted_index.md` carries the sweep, the reader's disposition
  per arm, the measured price of one whole-blob CRC-32 (4 bytes, +0.41% of the
  decode) and what a 0.1.x reader does with each placement. No format, API or
  default changed. (#4991)

- **Alerting (feature)**: `SiglakeCompactorOrphansHeld` (critical, `for: 15m`,
  `siglake.stalled`) pages on the one WAL orphan disposition that needs a
  person. A compactor killed mid-commit leaves its segment under
  `<wal>/orphans/`; the drain deletes it when the table's consumed-segment set
  proves the rows are committed and requeues it when the retained history
  proves they are not, but a name absent from that set while snapshot expiry
  may already have dropped the proving snapshot is neither, so the file is held
  and its rows stay uncommitted and unqueryable until an operator settles it.
  Nothing read the gauge, and the gauge was not worth reading: it was written
  once per directory, so a tenant's events pass and its index passes overwrote
  each other's value, and a directory with no orphans returned before writing
  anything, so a resolved hold kept its last non-zero reading for the life of
  the process. It is now the tenant's total over every directory the sweep
  visits, including the two that exit before disposition runs (an index name
  that resolves to no table, a WAL directory whose owner marker refuses the
  drain) and a disposition that fails partway, where what it never classified
  counts as held rather than as resolved. The alert preserves the tenant, pod
  and namespace labels; its action is to preserve the files and establish
  commit status from the operator's own evidence, and it says outright that
  raising `compactor.snapshotExpire.retainLast` protects the proof for future
  orphans without restoring history that has already expired. (#3267)

- **CLI (fix)**: `siglake wal-recover` refuses a mirror object that is not a
  WAL segment instead of restoring it as one. An `_active/` object is listable,
  and stat-able at zero bytes, before its body lands — opendal's `fs` writer
  creates the target in place with no `atomic_write_dir`, so a
  filesystem-backed mirror and any interrupted uploader leave the same state —
  and the apply published whatever it read under a sealed name and counted it
  pulled. The drain then failed to read a zero-byte sealed segment the restore
  had reported as done. Every candidate now has to decode to at least one row,
  sealed and active alike; the ones that do not are counted in `unreadable`
  rather than `pulled`, named with their reason in the plan an operator reads
  before `--apply` as well as in the report, charged to
  `siglake_wal_recover_unreadable_total`, and left in the mirror — a refused
  candidate creates no `.tmp`, no `sealed/` and no tenant discovery directory.
  A flushed prefix whose last Arrow IPC message is torn still restores, which
  is what the active mirror is for. The check costs the plan one GET per
  candidate it would write; an already-present destination is not read, so a
  re-run does not re-download the mirror. (#5077)

- **Ingest (fix)**: `wal.mirror.activeIntervalSecs` /
  `--wal-active-mirror-interval-secs` uploads the segments ingest is writing.
  The loop was handed the ingester's root `WalWriter`, and every request lands
  in a per-tenant writer or a backpressure lane's writer instead — the server
  installs one of those routers in every configuration. So each tick flushed an
  empty writer and sent nothing: a process that logged "WAL active-segment
  mirror enabled" wrote no `_active/` object, the N-second loss bound the flag
  advertises did not hold anywhere, and `wal-recover`'s `root confirmed`
  verdict, which keys off an `_active/` object, was unreachable. The loop now
  takes the writer sets the HTTP handlers resolve against and asks them on every
  tick, so tenants, managed indexes and write shards created by later traffic
  are covered as they appear. One object per open writer, keyed
  `_active/<tenant>[/<index>]/<segment>` as recovery expects, and a segment
  that has not grown since its last upload is skipped rather than re-PUT.
  Flushes stay under the writer's lock (or inside its lane task) and the upload
  outside it. Off by default, unchanged; sealed mirroring and shutdown sealing
  are untouched. (#5055)

- **Alerting (docs)**: `SiglakeTenantsDenied` now says what to do about
  `reason="header_not_trusted"`, the single-tenant default refusing a routing
  `X-Scope-OrgID`: bind the tenant to a verified identity with
  `ingester.oidc.tenantClaim`, and set `ingester.trustScopeHeader` only where a
  gateway in front of the ingester sets the header itself and strips the
  client's. The rule is otherwise untouched — same name, same
  `increase(siglake_ingest_tenant_denied_total[10m]) > 10` over every reason,
  same `for: 5m` — and the chart's promtool fixtures now pin that reason's
  behaviour: a sustained rate fires and then resolves on its own once the
  refusals leave the 10m window, while one client retrying a stale header four
  times and a half-hour trickle at half the threshold both stay quiet. (#3067)

- **Query (fix)**: a filtered browse written `WHERE TIMESTAMP >= …` gets the
  same scan-order hint and distribution estimate as the lowercase spelling. The
  two shape detectors behind the selectivity-aware ordered policy and the
  distribution gate compared an identifier's written value with `timestamp`,
  and an unquoted `TIMESTAMP` — the same column everywhere else in the planner
  — matched neither the pure-time-range test nor the exclusion on the
  dimensional arm, so the term was read as the browse's dimension or, with a
  real dimension already present, sank the shape. Both now apply SQL's own case
  rules: unquoted identifiers case-fold, and a quoted `"Timestamp"` remains the
  distinct ordinary column a managed mapping may declare. Results were never
  affected — a missed shape costs an early-stop drain and sends the query to
  the gate's limit-only fallback. (#4217)

- **Ingest (fix)**: the ingester's local WAL sweep reclaims a managed index's
  sealed segments. It listed the WAL root and its tenant directories one level
  deep, and a managed index's segments sit at
  `<root>/<tenant>/<index>/sealed/`, so in catalog-claim mode — where the drain
  reads the mirror and this sweep is the only thing that deletes an ingester's
  local copies — nothing was removed for a managed index and the PVC grew for
  the life of the pod. `siglake_wal_local_sealed_segments` counts only the
  directories the sweep visits, so it did not report the backlog either: a
  directory nothing lists contributes nothing to the gauge. The walk is now the
  drain's own — root, each tenant, each tenant's indexes. The deletion gates
  are unchanged: a local copy goes only once its catalog row says `committed`
  and has settled for `SIGLAKE_WAL_LOCAL_SWEEP_SETTLE_SECS`, and the row itself
  is left for remote retention. (#4915)

- **Recovery (breaking)**: `siglake wal-recover` plans by default and writes
  only under `--apply`. The plan is the listing the restore already did before
  its first GET: one line per `(tenant, index)` with the segment count, the
  byte total the listing reported, a sample key and the destination it
  reconstructs, plus the already-present and skipped counts. It creates
  nothing under `--to`, `--to` itself included. A script or runbook calling
  the old single-command form stops writing and prints a plan instead.

  The same listing settles whether `--from` is the mirror root. siglake writes
  two markers at a fixed depth under it — a first component `_active` with a
  `.arrow.partial` tail, and `<tenant>/<index>/owner` from the catalog-claim
  drain — so either at its own depth confirms the root, and either exactly one
  component deeper means `--from` is one component above it: the run exits
  nonzero naming the directory to pass instead, in both forms, and the apply
  before it creates anything. A marker at root depth does not excuse a
  misplaced one. A mirror with neither marker is reported `unverified` and
  restores under `--apply` on the operator's reading of the plan; that is the
  population no rule reading only the keys can separate from a legitimate
  mirror whose first tenant is named after a prefix, and it is why the plan
  exists. There is no `--force`.

  #4928's all-skipped nonzero exit is unchanged, with its counts on the plan's
  totals line; the `sealed/`-as-target refusal still runs before the listing;
  and the restore's durability, routing, ownership and #4972 discovery-dir
  repair are untouched. `siglake_wal::mirror` gains `plan_recovery` and
  `apply_plan`; `recover_from_object_store` keeps its signature and is now
  both halves in one call. (#4973)

- **Recovery (fix)**: `siglake wal-recover` rebuilds the tenant discovery
  directory, so a restore that holds only index segments for a tenant is
  drained. The compactor enumerates a tenant by its own `<tenant>/sealed/`,
  which the ingester creates before it opens any per-index lane; recovery
  rebuilt `<tenant>/<index>/sealed/` and not that, so a mirror for a tenant
  whose events lane never sealed a segment — Elasticsearch-bulk-only traffic —
  restored from the right `--from`, printed `pulled N segments`, and left the
  rows in a layout no drain cycle walks: no commit, no
  `siglake_compactor_index_unresolved_total`, no backlog gauge, nothing in
  `orphans/`. The directory is now created on the restore path with the same
  durability as the segments, before the already-present skip, so re-running
  the command repairs a WAL root restored by an earlier version. Segment
  contents, tenant and index routing, ownership checks and the report are
  unchanged. (#4972)

- **Recovery (behaviour change)**: `siglake wal-recover` reports what it
  understood, not only what it pulled. A `--from` naming an ancestor of the
  mirror root — `…/store` where the segments are at
  `…/store/warehouse/wal-mirror/<tenant>/` — recognises no key, and printed
  `pulled 0 segments` and exited 0: the same line and the same status as a
  re-run with nothing left to do, so "nothing understood" read as "nothing to
  do" on the path that is the reason WAL mirroring is on by default. The
  report now appends the count already present and the count of keys skipped
  for an unrecognised layout (`pulled 0 segments into <wal> (3 keys skipped:
  unrecognised layout)`), and the command exits nonzero, naming a refused key
  and what `--from` should point at, when nothing was pulled, nothing was
  already present and keys were skipped. A mixed mirror still succeeds — the
  recognised segments are restored, the skip count is on stdout and the skip
  is logged — and so does an idempotent re-run and an empty mirror.
  `recover_from_object_store` returns a `RecoverySummary` (pulled, skipped,
  already-present, one sample refused key) in place of the pulled `usize`;
  tenant and index routing, the preference for a sealed copy over its active
  prefix and the fsync-before-count durability are unchanged. (#4928)

- **WAL mirror reclamation for the filesystem drain (new, off by default)**:
  `compactor.mirrorLedgerReclaim` (`SIGLAKE_MIRROR_LEDGER_RECLAIM`) lets the
  default single-replica compactor delete the mirror objects it has committed.
  Committed retention belonged to the catalog-claim drain, which purges what it
  claimed; the filesystem drain commits out of local `sealed/` and never reads
  the mirror, so the prefix grew for as long as the cluster ingested — 421,632
  objects and 37.3 GB a day at 20K EPS — and so did the `wal_segments` row the
  ingester writes per upload, because retention only deletes `committed` rows
  and none reached that state. With the knob on, the compactor connects the
  catalog and the mirror store WITHOUT claiming — no `try_claim`, no
  mirror-to-catalog reconciliation, no abandoned-claim reclaim, so it never
  registers an object it did not commit — upserts the ingester's row to
  `committed` for each file in local `committed/`, and the unchanged retention
  pass deletes the object and then the row under `committedRetentionSecs`,
  whose `0` still means delete nothing. The mark is driven off the directory
  rather than the commit return, so it repairs a crash between the Iceberg
  append and the mark; it skips a segment whose `mirror-pending/` pin says an
  upload is still owed; and the local sweep waits for it, so local commit
  evidence outlives remote evidence. The 3600 s `committed/` ceiling still
  wins, charging `siglake_compactor_mirror_unreclaimed_total` (panel 163) when
  a catalog outage leaves an object beyond reach. Segments no local drain ever
  committed — a dropped index incarnation's, an ingester whose volume was lost
  — are still left to an object-store lifecycle rule, as `docs/LIMITATIONS.md`
  says. One related fix on the ingest side: the mirror catch-up sweep no longer
  uploads a candidate whose only remaining local name is `committed/`, which
  could recreate a reclaimed object. (#4913)

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

- **Release images report the commit they were built from**: `siglake
  --version` and the `siglake_build_info` metric read a revision stamped in at
  build time, and the publish workflow passed none. The builds copy no Git
  metadata, so the fallback `git rev-parse` had nothing to read and every
  published image — both the server and the operator — said `unknown`, leaving
  no way to tell a release apart from a rebuild carrying later fixes. Both
  builds now get the commit the release checkout resolved to, which for a
  manual dispatch is the commit named by its `tag` input rather than the
  revision the workflow itself ran from. Image repositories and the image tag
  are unchanged: the tag still says what the release is called, and the
  revision now says what is in it. (#4557)

## 0.1.1

Thirteen changes on top of 0.1.0. Nothing about the on-disk format or the HTTP
surface moves, and a 0.1.0 warehouse is read and written unchanged: one values
key and six environment knobs are added, and no flag or values key is removed.
Two defaults move. The audit worker now gives each append 30 s instead of
awaiting it forever, and every process except the query server budgets zero for
the two text-index caches — a ceiling rather than a behaviour, since those
processes were already holding nothing in them. The
workspace version, both chart `version`/`appVersion` pairs, the pinned image
tags under `deploy/` and the two OpenAPI documents' `info.version` all read
`0.1.1`, and git tag `v0.1.1` publishes image tag `0.1.1`.

- **Recovery**: `siglake wal-recover --from <url> --to <wal-root>` restores the
  segments under the URL's path. It built its object store rooted at the whole
  `--from` URL and then passed that same path again as the listing prefix, so
  the lister walked `<path>/<path>/`. Every URL carrying a path — including the
  `s3://<bucket>/<warehousePrefix>/<prefix>` the chart's DR recipe prints —
  printed `pulled 0 segments` and exited 0: a restore that reported clean and
  had recovered nothing, on the path that is the reason WAL mirroring is on by
  default. The prefix is now relative to the operator's root, which on that
  path means empty, and `recover_from_object_store` takes an empty prefix
  instead of listing `"/"` and then failing to strip `"/"` off every relative
  key. A nonempty prefix still selects only what sits under it, for a caller
  whose operator is rooted above the mirror. The tenant and index routing, the
  preference for a sealed copy over its active prefix, the fsync-before-count
  durability and the skip of what is already present are unchanged, and a
  re-run still pulls nothing twice. `wal-recover` now has a test that runs the
  binary against a `file://` mirror, with and without a trailing slash.
  (#4912)
- **Query (experimental cache, off by default)**: with the decoded-file cache
  switched on (`SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_{BYTES,ENTRIES}`, both `0`
  everywhere the project packages), a scan whose predicate converts to an
  Iceberg predicate no longer populates the cache. An entry is keyed by file and
  projection, so it had to be read with the predicate removed to stay reusable —
  and that read decodes the whole projection where the reader's page index would
  have skipped most of it. On a local two-row-group fixture a
  `host = '<label>' LIMIT 100` browse ran 2.8x slower with the cache on than
  with it off, for a population the browse's `LIMIT` then discarded. Such a scan
  now reads with its predicate intact, as it does with the cache off, and is
  within 0.3 ms of that control over four runs. It can still be served from an
  entry a predicate-free scan left behind; answers are unchanged either way,
  since an exact-capable filter is declared `Inexact` whenever the cache is on
  and re-applied above the scan. The cost of this is a narrower fill: an entry
  needs a drained, non-order-preserving scan carrying no convertible predicate,
  which on a log-UI workload is close to nothing (`docs/LIMITATIONS.md`).
  (#4891)
- **Query**: a text query over more indexed files than the text-index caches
  hold no longer re-reads every index blob from object storage on every
  execution. The two caches sit on either side of one decode, and only the
  parsed side serves a warm query, so a cached blob is read exactly when its
  parsed twin has been evicted — which is also the moment arrival-order
  eviction dropped it. A 14-file plan on the 50G benchmark round read 4.60 GB
  over 183 index-phase reads where the same plan had read 0.50 GB over 73.
  Eviction now drops the blobs the parsed cache still covers, which cannot be
  read at all, and keeps the ones it has dropped, for a bounded number of the
  cache's turnovers so that a compacted-away file's blob is not retained for
  the life of the process. What remains is arithmetic: a repeat text suite
  re-fetches the indexed files the blob budget cannot cover and nothing more —
  measured as exactly `files - blobs held` per pass over plans from 8 to 28
  files, against the whole plan before. No budget or default changes, and no
  answer changes: a blob-cache miss costs a fetch, never a row. (#4182)
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
