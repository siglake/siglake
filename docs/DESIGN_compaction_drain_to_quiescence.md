# Compaction: drain to quiescence (200G under-consolidation fix)

**Status:** implemented, pending 200G AWS validation (2026-06-19).
**Touches:** `siglake-compactor` run loop, `siglake-storage` recluster instrumentation,
the bench-harness settle probe.

## Symptom (200G round 1)

At the 200G tier (393.9M docs, 4× the 50G corpus) compaction "quiesced" after
**6 passes** leaving **448 live data files** — ~11× the 50G layout's 39 files,
not the ~4× a clean scale-up gives. That under-consolidated, time-**overlapping**
layout broke everything that depends on a time-disjoint read layout:

- `match_all` / `deep_pagination` (newest-first, no time filter) → the scan can't
  advertise a `timestamp` ordering (overlapping files exceed the k-way merge
  fan-in budget), DataFusion inserts a `SortExec: TopK` over all 393M rows → trips
  the 100M mid-flight row breaker → **413**.
- `histogram_hourly` regressed 27× (284ms → 7.7s): hundreds of files straddle
  every hour boundary → footer re-bucket reads hundreds of footers + falls to scan.
- FTS / filter + LIMIT went superlinear (~10× for 4× data): the LIMIT has to
  touch far more files to gather 100 rows.

Manifest-served aggregations (Tier-1 count_by_level, footer high-card group-by)
were unaffected — they don't read file data, so file count is irrelevant.

## Diagnosis (what it is NOT, then what it is)

Ruled out, each with a local repro/sim:

1. **Thin-partition gate** (the 751633d hypothesis). 200G is 2 *dense*
   day-partitions, not thinly-spread ones. The cold age-gate (`min_files=2`) is a
   real fix for thin cold partitions but does not move the 200G dense case.
2. **Pack/select count-churn** (b83bc0a). A fixed-point model of the pack→merge
   loop *converges* for small files — it does not oscillate.
3. **Merge algorithm can't produce disjoint output from overlapping input.**
   Disproved by `compaction_consolidation_tests::d_…` and `e_…`: a single k-way
   merge of N overlapping files produces one sorted stream that the rolling writer
   cuts into time-**disjoint** slices. Even under tight, production-scale per-pass
   bins (~3 files/merge), the layout converges to 0 overlapping pairs — just
   *geometrically*, over many passes (`e_…` measures 36→24→15→9→5→3→2 files,
   ~×0.65/pass; 448 files ⇒ ~13 passes).

The actual root cause is **scheduling**, in the compactor run loop:

```
loop {
  n = run_once();              // commit sealed segments
  if n > 0 { continue; }       // ingest busy → skip maintenance
  if recluster_interval_elapsed { run_recluster_once(); }  // ONE pass
  sleep(poll_interval);
}
```

A single bounded pass only heals a slice, and convergence is geometric — but the
loop ran **one pass per `interval`**, only on idle cycles. To take 448 files down
to target needs ~13 passes ⇒ ~13 idle intervals (minutes). Meanwhile the bench's
settle probe ("live file count stable for 5×60s") declared the layout settled in
the gaps *between* passes and measured an unconverged layout. Budgets were not the
constraint — the 200G deploy already used 4GB pass bytes / 45M rows / 64 files /
8 bins.

## Fix

### 1. Drain to quiescence (`siglake-compactor`, the primary fix)

When idle and reclustering is due, keep issuing passes **back-to-back** — bypassing
both the interval gate and the `poll_interval` sleep — as long as each pass removes
files (`draining`). `run_once` (commit) still runs at the top of every cycle, so
resumed ingest always preempts the drain. A pass that removes 0 files ends the
drain and the loop returns to the interval cadence. A `DRAIN_BURST_CAP` (256)
bounds a single burst defensively against a degenerate remove-then-re-add
oscillation; geometric convergence settles far below it. On completion the
compactor logs `tier-2 re-clustering drained to quiescence (passes=N)`.

This converges a fragmented layout in **one idle window** instead of ~13 intervals.

### 2. Convergence observability (`siglake-storage`)

- `siglake_table_live_data_files{table}` gauge, sampled at the start of every
  `recluster_pass` — the breaker-safe way to watch consolidation (the old bench
  probe forced a full-scan `LIKE` to read `files_scanned`, which itself trips the
  100M breaker at 200G).
- `recluster_pass` debug log: `partitions_total`, `partitions_acted`,
  `partitions_under_gate`, `files_removed`, `files_added` — why a pass did or
  didn't make progress.

### 3. Bench settle probe (the bench harness)

Switched from the breaker-tripping `LIKE` full-scan to scraping the new
`siglake_table_live_data_files` gauge from `/metrics` (9100), and wait for 8×30s
(4 min) of a flat value so a mid-drain plateau between passes can't be mistaken
for convergence.

## Regression tests

- `d_time_overlapping_input_yields_disjoint_output` — overlapping input compacts
  to a time-disjoint layout (multi-file output stays disjoint).
- `e_overlapping_converges_under_small_pass_bins` — converges to 0 overlapping
  pairs under tight per-pass bins, in >1 pass (proving the drain is necessary).
- Existing `a/b/c` consolidation tests + `recluster_*` bound/conservation tests
  still pass.

## AWS 200G validation (2026-06-20) — partial: drains, but does NOT yet disjoin

Ran the fix against the real 200G corpus (393,866,085 rows) on the bench node.
Salvaged a single ingest by hand-driving compaction after the WAL committed.
Headline: **the scheduling + budget changes reduce file COUNT but do not yet
produce a time-DISJOINT layout, so `match_all`/histogram/pagination remain
broken at 200G.** The investigation pinned why, in order:

1. **Ingest ≫ commit at 200G (separate bottleneck).** Clients POST all 394M rows
   into the WAL at ~168k rows/s (4×42k, 0 errors) in ~39 min, then exit. The
   compactor then commits WAL→Iceberg at only ~48k rows/s — a ~27k-segment /
   24GB backlog that took **~90 min** to drain to queryable. The 50G-tuned bench
   waits hit this mid-backlog and aborted (looked like "ingest didn't finish").
   Fixed in the bench (commit-lag-tolerant wait + env-configurable windows); the
   commit pipeline itself is a real 200G throughput item, independent of compaction.

2. **Scheduling drain — confirmed necessary.** Before the fix, one pass/interval
   left the layout unconsolidated for many idle minutes. With the drain, the
   compactor ran passes back-to-back once idle. ✅

3. **Per-pass budget — the default is far too small at 200G.** Peak memory is
   **one bin** (~`target`, processed sequentially), so the real fan-in lever is
   `max_files_per_pass`/`max_bins_per_pass`, NOT memory. The default 64 files /
   8 bins consolidated only ~13–108 files/pass and **plateaued ~480 live files**.
   Raising to **384 files / 48 bins / 12GB** (in-place, no rebuild) removed
   **393 files in one pass** and converged 703 → 478 → 175 → **86** live files
   in 2 passes. ✅ (Memory stayed ~2GB — peak is one 512MB bin.)

4. **THE CORE GAP — count reduction ≠ disjointness.** At 86 files,
   `ORDER BY timestamp … LIMIT 100` STILL planned a `SortExec: TopK` over the
   scan in **both** ASC and DESC (so it is not the known reverse-direction
   limitation) → full scan → breaker/slow. Root cause: multi-pass compaction of
   time-**overlapping** input produces target-sized outputs that overlap *each
   other* (each pass merges a different scattered subset spanning the whole
   range), and once an output reaches `target` the **undersized gate excludes it
   from further merging** — so the overlap is never resolved. The ordered-scan
   gate (`scan_output_ordering`) correctly refuses to advertise an ordering, and
   the early-stop can't engage.

   This is exactly **200G-round focus #1**: compaction must *merge toward
   time-disjoint runs*, not just "few enough files per partition." The local
   `d_`/`e_` tests pass because they run to FULL quiescence (2 files), where one
   final merge trivially sees the whole partition — they do **not** exercise the
   "target-sized but still overlapping" intermediate that occurs at scale. That
   missing test ships with the fix.

5. **Per-pass inverted-index rebuild dominates pass time** (~12–40 min/pass over
   tens of millions of rows). A throughput cost that makes cold re-compaction
   slow; orthogonal to correctness.

### The remaining fix: disjointness-aware compaction (next change)

`recluster_pass` must detect groups of time-**overlapping** files in a partition
(including target-sized ones) and merge each group into time-disjoint runs —
bounded by fan-in/memory. With a large per-pass budget this is one k-way merge
of a partition's overlapping set → a single sorted stream → rolled into disjoint
`target`-sized runs (precisely what the local repro does at small scale). The
"undersized < target" gate must be replaced/augmented by an "overlaps others"
predicate so already-large-but-overlapping files are still re-merged. Pair with
a raised default `max_files_per_pass`/`max_bins_per_pass` (cheap) so a partition
disjoins in 1–2 passes. Add the at-scale regression test (consolidate to target,
assert 0 overlapping pairs). Optionally raise the read-path k-way fan-in budget
as a complementary lever for moderately-overlapping layouts.

**Status of this change:** the scheduling drain + instrumentation + bench fixes
are correct and prerequisite, but DO NOT ship the consolidation as "fixed" — hold
until the disjointness-aware merge lands and a 200G round shows `match_all`
early-stopping (no `SortExec`).

## Disjointness-aware compaction + streaming merge (2026-06-20/21)

Two more layers, after the drain + budget work above still left 200G overlapping:

1. **Gap-aware bin-packing + overlap-eligibility** (`pack_recluster_bins` now takes
   per-file `(lo,hi)` bounds). Bins seal only at a *time gap* (or the hard memory
   cap), never merely for reaching `target`, so an overlapping cluster stays in one
   bin → one merge → time-disjoint runs. `recluster_pass` feeds ALL of a partition's
   files (time-sorted) and gates on *eligible* files (undersized OR overlapping a
   neighbour), so already-target-sized-but-overlapping files are re-merged (the old
   `< target` filter excluded them — `f_target_sized_overlapping_files_are_disjoined`).
   Result on 200G: consolidated 765→177 in one pass — but **still overlapping**.

2. **Why count-reduction still didn't disjoin** (`g_overlap_cluster_exceeding_bin_
   budget_stays_overlapping`, a free local repro): the old merge read each input
   file fully into RAM, so a bin was capped at `max_pass_bytes`. A partition's
   overlap cluster at 200G (~22–37GB) exceeds any sane budget → it splits across
   bins → split pieces of an all-overlapping cluster each span ~the whole range →
   outputs overlap. Download-first merging fundamentally can't disjoin a cluster
   bigger than RAM.

3. **Streaming merge** (the fix): `merge_files_streaming` now opens each input as an
   async `ParquetRecordBatchStream` over object store (`ArrowFileReader`, the bridge
   the query scan uses) instead of `input.read()`-ing the whole file. Peak memory is
   bounded by **fan-in × row-group buffer**, not total cluster bytes — so one merge
   consolidates an entire overlapping cluster of any size into disjoint runs.
   Deploy then sets `max_pass_bytes`/`rows` effectively unbounded (1TB/4B) with
   `max_files_per_pass` (600) as the real fan-in bound (≥ a partition's file count),
   so a whole cluster merges in one pass. Fan-in > a partition needs a multi-level
   external sort (follow-on). **Validation:** 200G round in progress (streaming +
   `--no-teardown`); success = `match_all` early-stops (no SortExec) + wide-window
   queries stop 413-ing.

## Follow-ons (not in this change)

- **Streaming-from-object-store merge.** `merge_files_streaming` reads each input
  file fully into RAM, so a bin's fan-in is bounded by `max_pass_bytes` (resident
  compressed bytes), not by stream count. An async parquet stream reader would
  decouple fan-in from memory and let one pass consolidate a whole partition →
  even fewer passes. Drain makes this an optimization, not a correctness need.
- **Scale the mid-flight row breaker** (200G-round focus #2) — a fixed 100M limit
  turns *slow* into *failed*. Independent safety net; with a disjoint layout the
  newest-first queries early-stop and never reach the breaker.
