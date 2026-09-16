# DESIGN — Continuous leveled compaction + 1TB/hour ingest

Status: DESIGN (2026-06-30). Motivated by the 1TB runs. Owner: perf/compaction arc.

> **Forward plan:** the sequenced adoptions this arc feeds into — per-tier
> cadence, a write-amplification bound, the page-bounded merge, and a dedicated
> compaction service.

## Standing invariants (decided 2026-07-02, see the plan doc §5)

- **Level/maturity is computed at plan time, never persisted.** A file's level
  derives from its manifest byte size (`LevelPolicy::level_of`); its rewrite
  generation derives from its file name. No lifecycle state is stored that could
  disagree with policy (the reference series' "born mature, never merged" bug
  class is unrepresentable). If that ever changes, add a startup audit asserting
  no file is persisted-mature while policy-immature.
- **The rows-conserved commit guard is per-merge-kind.** Today every merge is a
  pure rewrite and the guard is exact (`in_rows == out_rows`, asserted before
  commit). Future aggregating merge kinds (rollup, LWW dedup) must declare their
  own invariant (`out_rows ≤ in_rows` + group-count cross-checks where available)
  and be sized on **decoded** bytes, not compressed file bytes — never silently
  waive the exact guard.
- **Result caches are snapshot-keyed, never TTL-expired.** The aggregate-result
  caches are pure functions of `(table, snapshot, query)` and invalidate on
  commit. A TTL'd whole-result cache silently serves stale leading-edge answers
  and is prohibited — freshness at the leading edge is a core product property.
  The SQL result cache's `snapshot` is the whole metadata **generation**
  (snapshot id + current schema id, from one loaded metadata): an additive
  `migrate-schema` changes the served column set and commits no snapshot, so
  snapshot-only identity would leave the pre-migration columns replaying until
  the next data commit (#2494). Invalidation is unchanged — both ids advance on
  their commit and the superseded key is never asked for again.

## Why

The 1TB runs exposed two coupled problems:

1. **Compaction starves under sustained writes.** The compactor's `run_loop`
   reclusters *only when ingest is idle* ("so it never contends with the commit
   hot path" — its own comment). Logging workloads are bursty **but always have a
   baseline write rate**, so the idle window never comes: in the 1TB runs recluster
   ran *zero* during ~8h of ingest and `live_files` grew unbounded (→4492). A
   degraded layout degrades queries (more files scanned, ordered-scan refuses →
   `match_all` full sorts, windowed boundary scans hit more files).

2. **Whole-partition merges are too big to run online.** Even after the
   bounded-fan-in tiered-merge fix removed the OOM, the latest run shows a single
   600-file / 22 GB day-bin taking **1–2h** to recluster (`merge_tiers_total`
   advancing, `removed_total` still 0 after 2.5h). A multi-hour merge cannot run
   concurrently with writes without either stalling ingest or never finishing.

The deploy intentionally uses giant single-bin merges (`MAX_FILES=600`,
`MAX_PASS=1TB`) to disjoin a whole overlapping cluster in one pass — the 200G bench
fix for "consolidates but doesn't disjoin". That trades *online-ability* for
*one-shot disjointness*. The right answer is the **LSM/leveled** model: many
*small, bounded, frequent* compactions that progressively disjoin, so work is
amortized and always-online.

**Goals:** (A) compaction runs continuously alongside a baseline write rate,
keeping `live_files` bounded; (B) 1TB ingest in ≤1 hour.

---

## Part A — Continuous leveled compaction

### A.1 Model: size-tiered levels (LSM-style)

Organize a table's data files into **levels** by role/size; each compaction merges
a *bounded* set of files from level `i` → level `i+1`:

| level | produced by | size | time layout |
|---|---|---|---|
| **L0** | WAL drain | ~WAL-batch (small, ~tens of MB) | arrival-order, **overlapping** |
| **L1** | merge of L0 | ~256 MB | sorted runs, locally disjoint |
| **L2** | merge of L1 | ~1–2 GB | larger disjoint runs |
| **L3** | merge of L2 | ~8–16 GB | fully time-disjoint |

Key properties:

- **Bounded per-compaction work.** A compaction merges at most `fanin` files of one
  level (≤64, the tiered-merge cap) → seconds–minutes, never the 1–2h giant merge.
- **Progressive disjointness.** L0 is overlapping; each level-up merge runs the
  existing gap-aware packer over a *bounded* input, emitting disjoint runs at the
  next level. Disjointness is reached by *iteration across levels*, not one giant
  pass — so we keep the 200G-round guarantee without the giant bin.
- **Bounded file count.** Per-level trigger: compact level `i` when it has
  `> max_files(i)` files (or `> max_bytes(i)`). With `max_files` small per level,
  L0 can never pile to 600 — it's drained up to L1 continuously.

**Level tracking.** Store a file's level as an Iceberg data-file property
(`siglake.level`) stamped at write (drain → L0, merge-to-`i+1` → that level). The
scheduler reads levels from the manifest (cheap; already loaded). No schema change.

### A.2 Concurrent scheduler (the starvation fix)

Replace "drain, then recluster-if-idle" with **two concurrent, budgeted tasks**:

- **Drain task** — WAL → Iceberg L0 commits. Keeps its freshness priority.
- **Compaction task** — a continuous loop: pick the highest-*pressure* level
  (most files over its `max_files`), do ONE bounded `Li → Li+1` compaction, repeat.
  Runs **always**, not only at idle.

Coexistence without starving the commit path:

- **Resource budget.** Compaction is capped: bounded fan-in (≤64 decoders),
  bounded merge concurrency (1–2 in flight), and an optional CPU/IO share. Peak
  memory is one bounded merge (the tiered cap), so it can't OOM or balloon.
  **Update (2026-07-07):** bins beyond the merge fan-in no longer tier through
  intermediate files (which re-wrote every byte once per tier). The default is
  the **page-bounded plan merge**: a timestamps-only read per input builds an
  RLE `MergeRun` plan (gallop-based; near-disjoint inputs collapse to one run
  each, so plans shrink as data ages), then execution walks the plan in
  `SIGLAKE_MERGE_CHUNK_ROWS`-row chunks (default 512 Ki), re-opening each
  contributing input with a `RowSelection` against cached metadata — the
  offset index prunes the body GET to covered pages, whole runs assemble as
  zero-copy slices, fragments interleave. Decoded memory is bounded by the
  chunk, **independent of fan-in** (600 overlapping inputs cost the same
  resident memory as 6), and no reader state persists between chunks. The
  tiered path remains behind `SIGLAKE_RECLUSTER_TIERED_MERGE=1`. Plan-phase
  cost is 8 bytes/row of timestamps; a pipelined next-chunk prefetch is the
  known follow-on if chunk-sequential fetch shows up in bench latency.
- **Backpressure-aware sharing.** A shared signal from the WAL queue depth
  (`siglake_ingest_backpressure_*`, already exported): if drain falls behind (WAL
  growing), compaction throttles (fewer concurrent merges) to yield IO; if L0 piles
  up (compaction behind), it gets more budget. The commit path always wins ties —
  freshness first — but compaction never drops to zero.
- **Commit coordination.** Drain and compaction both commit to one Iceberg table.
  They already serialize through the catalog's optimistic concurrency; the
  vendored `update_table_with_base` retries on conflict. Compaction commits are
  rare relative to drain and touch disjoint files, so conflicts are infrequent.

This is still inside the existing **separate compactor deployment**
(`deployment-compactor.yaml`); A.2 makes its *internal* scheduling concurrent.
Splitting compaction into its *own* service (independent scaling) is a later option
(A.5) once we measure whether one budgeted compactor keeps pace.

### A.3 Write amplification & cost

Leveling rewrites each row ~`O(levels)` ≈ 3–4× total (vs the giant scheme's ~1×
but un-runnable-online). That extra write IO is the explicit, bounded price of
always-online compaction — standard LSM economics, and far cheaper than an
unbounded un-compacted layout's *read* amplification at query time.

### A.4 Increments (each ships compiling + tested)

- **A.4.1 — Concurrent + bounded compaction (build first).** Decouple drain and
  recluster into concurrent tasks; remove idle-gating; cap each recluster pass to a
  small bounded bin (≤64 files / a few hundred MB) so it finishes in seconds–minutes
  and runs continuously. This alone keeps `live_files` bounded under sustained
  writes (the immediate fix), using the *existing* drain-to-quiescence iteration to
  reach disjointness over many small passes.
- **A.4.2 — Explicit levels** ✅ *(built; pending 1TB validation).* Level is
  **size-derived** (not a stored `siglake.level`: Iceberg `DataFile` has no
  arbitrary-property map, and the byte size is already in the manifest the
  scheduler loads) — `LevelPolicy::level_of(bytes)` counts the ascending
  `level_ceilings` a file meets (default `[128 MiB, 1 GiB, 8 GiB]` → L0/L1/L2/L3).
  `IcebergContext::recluster_pass_leveled` picks each partition's **most-pressured**
  level (most files ≥ `trigger_files`, ties → lowest level so L0 drains first) and
  merges a bounded `max_fanin` of its files toward the next level's size via the
  existing gap-aware packer — so each compaction is seconds–minutes, not the giant
  whole-partition merge. Wired through the compactor behind
  `SIGLAKE_COMPACTOR_LEVELED=1` (`ReclusterConfig::levels`); ladder/trigger/fan-in
  tunable via `SIGLAKE_COMPACTOR_LEVEL_{CEILINGS_MB,TRIGGER_FILES,MAX_FANIN}`.
  Observability: `siglake_table_level_files{level}` (per-level live count) +
  `siglake_compactor_level_compactions_total{level}`.
- **A.4.3 — Backpressure-aware budget** from WAL queue depth. ✅ *(first slice)*
  The 200G leveled run proved this is **required, not optional**: A.4.2 held
  `live_files` bounded but starved the drain (11 K-segment backlog, ~8 K rows/s)
  because the loop ran a leveled pass every cycle regardless of backlog. `run_loop`
  now skips **all** maintenance for a cycle when `pending_sealed_total()` exceeds
  `SIGLAKE_COMPACTOR_MAX_SEALED_FOR_RECLUSTER` (default 256) — the commit path wins
  under load, compaction runs in the lulls. Counter
  `siglake_compactor_maintenance_skipped_backpressure_total`.
  **Graded budget** ✅ (2nd slice): the gated 200G re-run showed the hard on/off
  gate lets files grow during *sustained* ingest (real count hit ~778 because
  compaction was fully suppressed until the lull). So under backlog the loop no
  longer skips outright — it runs ONE *throttled* pass (`max_bins_per_pass=1`, a
  single bounded merge) every `SIGLAKE_COMPACTOR_BACKPRESSURE_COMPACT_EVERY`
  (default 4) backlogged cycles and drains on the rest, keeping files bounded
  without re-starving the drain (`..._throttled_backpressure_total`). And
  `IcebergContext::sample_live_file_gauges()` runs on a 30 s timer so
  `siglake_table_live_data_files` stays fresh during ingest (it froze at 1 while
  the gate suppressed the passes that update it). The count is read from the
  current snapshot's summary (`total-data-files`, O(1)) each cycle, so it is
  exact even on a 2B-row table whose manifest tree the sampler cannot walk
  inside its per-table budget (`SIGLAKE_GAUGE_TABLE_TIMEOUT_SECS`, default 20 s;
  the 2026-08-03 1TB round froze it at 886 for a whole settle). The gauges that
  need per-file sizes and bounds — `siglake_table_level_files`,
  `siglake_table_leading_edge_small_{files,bytes}`, `siglake_table_overlap_depth`
  — still come from the walk and can lag: `siglake_table_gauges_sampled_at_seconds`
  is set only when a table's walk completes, and
  `siglake_table_gauge_table_timeouts_total` counts the walks that did not.
  Remaining: a catalog-claim-mode backlog signal (`peek_pending` instead of
  `sealed/` listing).
  Measured on the 2026-07-01 200G leveled-compaction round.
  - **Un-interruptible polls under `tokio::time::timeout` (#1165 audit,
    2026-09-06).** The mechanism #1002 hit is general: a `tokio::time::timeout`
    fires only between polls of its inner future, and an in-memory cache hit
    resolves *inside* the poll. The fork's `InputFile::read` and
    `CachingFileRead::read` served object-cache hits synchronously, so a loop of
    `load_manifest(..).await` over a warm cache, a `buffer_unordered` footer
    sweep over a warm `FooterCache` (every polled future `Ready`, the collect
    loops), or a fully cached page scan was one poll that no wrapping timeout
    could cut until it ended. Fix chosen: every cache hit that stands in for
    I/O spends one unit of tokio's cooperative budget
    (`tokio::task::coop::consume_budget`), in the fork's two object-cache hit
    paths and in siglake's two `FooterCache` wrappers. A task yields only once
    its per-poll budget (128) is gone, so an all-hit loop yields every 128 reads
    and every existing timeout above it becomes enforceable at that granularity,
    while a read path with a handful of hits pays a thread-local decrement on
    top of the `format!` key, `Mutex` lock, `HashMap` get and counter it already
    paid. Rejected: a per-site `Instant` deadline at each caller (a dozen
    sites, and it cannot reach the fork's page reads). Kept: #1002's per-manifest
    deadline check plus `yield_now` in `live_data_files_within` — finer than the
    128-hit budget, and it also bounds a cold walk with fast I/O. Not changed:
    the moka `ObjectCache` hits inside the fork's `plan_files`, which run on a
    spawned producer behind bounded channels (the caller's await is a channel
    receive).

    **Warm-path measurement (2026-09-06, #1187): within noise.** Native
    loopback, SQLite catalog + `file://` warehouse, exact release builds at
    `6a6fd80` (before) and `1d4d458` (after). The live table had 3,000 one-row
    files; each plan trial made 20 small append commits and measured the first
    exact Tier-2 query on the new snapshot. The footer arm made 30 same-snapshot
    windowed `GROUP BY host` requests per trial with result caching, the 2-D
    aggregate and prewarming off; 2,900 files/request hit `FooterCache` (100
    no-bound files took the boundary path). There were two independent fixture
    restores per arm: 40 plan samples and 60 footer requests per arm. Values are
    milliseconds; “delta” is after minus before, centered across the two trial
    quantiles, and “repeat noise” is the largest same-commit difference between
    trial 1 and trial 2.

    | Warm measurement | before T1 / T2 | after T1 / T2 | centered delta | repeat noise |
    |---|---:|---:|---:|---:|
    | `iceberg_scan_cost` `plan_files` p50 | 1131.25 / 1106.64 | 1072.99 / 1128.32 | -18.28 | 55.33 |
    | `iceberg_scan_cost` `plan_files` p95 | 1137.45 / 1253.68 | 1142.15 / 1144.72 | -52.13 | 116.23 |
    | file-list `plan_files` p50 | 1107.29 / 1097.42 | 1093.16 / 1228.06 | +58.26 | 134.90 |
    | file-list `plan_files` p95 | 1117.92 / 1226.60 | 1267.53 / 1280.66 | +101.84 | 108.68 |
    | footer request wall p50 | 68.57 / 79.97 | 78.31 / 75.14 | +2.46 | 11.40 |
    | footer request wall p95 | 77.17 / 86.61 | 93.89 / 81.25 | +5.68 | 12.64 |

    The server-reported footer latency agrees (centered p50/p95 deltas
    +2.48/+5.71 ms, repeat noise 11.26/12.60 ms). Each after-before delta is
    smaller than its observed repeat noise, and all four trials performed the
    same work (87,000 footer hits/trial and ~328,272 raw object-cache hits).
    Keep the per-hit spend; an every-Nth-hit counter is not justified by this
    measurement.

    Every production `tokio::time::timeout` / watchdog, with what its inner
    future iterates and the disposition:

    | Site | Inner future iterates | Can it return `Pending`? | Disposition |
    |---|---|---|---|
    | compactor `bounded(drain_watchdog, run_expire_once)` | `list_indexes`, per-table `load_table`, expire commit | yes: catalog and object-store I/O, no cached-object loop | not needed |
    | compactor `timeout(limit, run_once)` (drain cycle) | WAL listing and claims, segment reads, Parquet writes, commits | yes: all I/O; leveled planning is not inside the drain cycle (next row) | not needed |
    | compactor recluster watchdog (progress-guarded `JoinHandle::abort`) | `recluster_pass_leveled` → `live_data_files` → `live_data_files_within(None)`, then bin merges | yes: `yield_now` per manifest since #1002 (the abort lands there), merges are I/O | covered by #1002 |
    | compactor `timeout(30 s, sample_live_file_gauges)` | per table `load_table`, then the walk | yes: deadline + `yield_now` per manifest | covered by #1002 |
    | `iceberg.rs` `timeout(per_table, live_data_files_within(deadline))` | one snapshot's manifests | yes: deadline + `yield_now` per manifest | the #1002 template |
    | `iceberg.rs` `reachable_files` (no wrapper; CLI `gc-orphans` only) | every retained snapshot's manifest list and manifests via `load_manifest*` directly | before: not on a warm cache; now each hit spends budget | fork change; nothing above it to enforce |
    | `iceberg.rs` `live_file_scan_tasks_cached` miss → fork `plan_files` | manifests via the moka `ObjectCache` | yes: spawned producer behind bounded channels; the caller awaits a receive, the producer parks on a full channel | not needed |
    | `iceberg.rs` footer sweeps: `warm_query_caches_sharing`, `date_histogram` Tier-2, windowed `GROUP BY` Tier-2, `grouped_counts_from_files` | `buffer_unordered` over `cached_read_file_time_buckets` / `cached_read_file_group_counts_for_path` / `raw_page_load_metadata` | before: a fully warm `FooterCache` set (≤ its 4,096-entry cap) was one poll; a miss does a `metadata()` HEAD (I/O) | `FooterCache` hits spend budget; byte-range hits spend budget (fork) |
    | `iceberg.rs` `scan_cost` | `live_data_files_cached` (hit: one `Arc` clone; miss: the walk), then an in-memory prune over `DataFile`s | walk yields per manifest (#1002); the prune is µs per file, bounded by the live-file count | not needed |
    | query-server `timeout(cycle_cap, warm_all_query_caches)` | the footer sweep above per table, `cached_side_aggregates` (one read), `warm_group_counts` (`plan_files` + Tier-2) | yes, after this change | covered |
    | query-server `timeout(warm_probe_timeout, LIMIT 1 probe)` | DataFusion collect; footer and page reads through `CachingFileRead` | before: a fully cached single-partition probe was one poll; now byte-range hits spend budget | fork change |
    | `sql.rs` `timeout(resolved.timeout, distributed_inner)` | `estimate` → `scan_cost`; the Tier-1/Tier-2 battery (rows above); fan-out HTTP | yes, after this change | covered |
    | `sql.rs` `apply_timeout` (local estimate, battery, render), `timeout(.., collect_fut)` (batch), `timeout(collect_deadline, collect)` (shard) | local cost/metadata reads, then DataFusion collects with `CancelOnDrop`; multi-partition plans run behind DataFusion's spawned partition pumps, single-partition reads go through `CachingFileRead` | yes, after this change | local scope covered by #1185; cache hits covered by #1165 |
    | `sql.rs` `SQL_RESULT_CACHE_WAIT` and the 5 s waiter; `query_provider.rs` `settle_scan_partitions` | `Notify` waits | yes, by construction | not needed |

    The separate single-pod scope gap found by this audit is fixed by #1185:
    `estimate`, every asynchronous Tier-1/Tier-2 battery stage, and execution
    now share one absolute request deadline. `CancelOnDrop` remains armed while
    the battery runs, so a timed-out footer sweep cannot continue unobserved.
- **A.4.4 (optional)** — dedicated compaction service (own deployment + scaling).

### A.5 Validation

Add a **sustained-ingest** harness mode: ingest continuously while sampling
`live_data_files`, query p50/p99, and `recluster_files_removed_total`. Success =
`live_files` stays bounded (not monotonically growing) and query latency stays flat
*during* ingest — not just after a settle window.

---

## Part B — 1TB ingest in ≤1 hour

1TB = 2.02 B rows; 1h ⇒ **~560 K rows/s sustained** committed-to-queryable. Current
drain is ~28–90 K/s and degrades. Order: exhaust single-node levers, then scale.

### B.1 Single-node levers (measure each)

- **B.1.1 — Shrink `metadata.json`.** `group_counts` still lives in the snapshot
  summary (~120 MB at 1TB across snapshots); each commit's `load_table` re-reads it.
  Move it to the side object too (like the 2a/2b aggregates already are). Directly
  speeds every commit. *(Highest-ROI, smallest change.)*
- **B.1.2 — Defer heavy footer/index work off the drain write.** Drain writes
  *minimal* L0 Parquet (timestamp-sorted + group-count footer only — cheap); the
  inverted-index Puffin + raw-bloom are built by **compaction** at L1+ (where data
  is consolidated anyway). Removes the per-drain-write index cost from the hot path.
- **B.1.3 — Parallel drain.** Multiple concurrent WAL→Iceberg drain workers (the
  WAL is already per-tenant/segment-sharded). Iceberg optimistic concurrency +
  `update_table_with_base` handle parallel commits; UUID filenames avoid dup-check.
- **B.1.4 — Larger commits.** Raise the drain batch (bytes/segments) to amortize
  the per-commit `load_table` + manifest + catalog cost over more rows. *(Knob
  exists: `SIGLAKE_COMMIT_BATCH_TARGET_MB`, default-on at 32 MiB since round 60;
  the leveled 1TB run bumps it to amortize further.)*
- **B.1.5 — Commit-reload elision** ✅ *shipped as **lever-2*** (round 59,
  AWS-validated): the vendored `iceberg-catalog-sql`
  `Catalog::update_table_with_base` drops the redundant per-commit
  `metadata.json` re-read and tightens the optimistic lock. Nothing further
  needed here.

### B.2 Horizontal scale (if levers fall short)

KEDA already scales ingester + query on saturation. Scale the **drain** the same
way (request-rate / WAL-depth ScaledObject), and size the bench node up
(m6i.16xlarge needs a vCPU-quota bump). Target measured per-worker drain rate ×
workers ≥ 560 K/s.

### B.3 Note

B depends on A: a faster drain produces L0 files faster, which only stays
query-able if compaction keeps up — so continuous leveled compaction (A) is the
prerequisite for sustained high-rate ingest.

---

## Sequencing

1. **A.4.1** concurrent + bounded compaction → 1TB re-run, validate `live_files`
   stays bounded *during* ingest (not just at settle). **← building now.**
2. **B.1.1** move `group_counts` to the side object (commit speedup).
3. **A.4.2** explicit levels; **B.1.2–B.1.5** ingest levers; measure toward 1TB/h.
4. Scale (A.4.4 / B.2) only after single-node levers are exhausted.
