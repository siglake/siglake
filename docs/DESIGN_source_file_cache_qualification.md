# Design — the opt-in source-file cache, at budgets a pod can afford (#3053)

Status (2026-09-17): **opt-in, unchanged.** The decoded-file cache stays off in
the chart, the compose file and the operator
(`SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_{BYTES,ENTRIES}` are `0` in all three), and
nothing here proposes a default. What this document adds is the sizing an
operator who turns it on has to make: which two numbers to set, what they buy,
and what they take from the query memory pool.

The measurements that exist ran at the bench fleet's 8 GiB / 16,384 entries
(#4494, #4846, #4891, #4847). No packaged pod has 8 GiB to spend on one cache —
the chart's query pod is 4Gi in TOTAL and the operator renders 2Gi — so at every
size that ships, the budget was the one thing never exercised: eviction, the
per-entry ceiling and the pool subtraction had no numbers at all.

## What the cache is, and where it is configured

One entry is one data file's DECODED batches under one projection, delete set
and direction, keyed `path:start:length:fields:deletes:reverse`. It fills only
from a scan that reads a task to end-of-stream and carries no predicate the
converter accepts (#4494, #4891): a browse whose `LIMIT` is satisfied early
drops its populate stream before the insert, and a task with a converted
predicate bypasses population so the reader can prune pages.

| surface | how it is set | default |
| --- | --- | --- |
| standalone | `--query-scan-file-cache-max-bytes` / `-max-entries`, or the two `SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_*` variables | unset — off |
| chart | `query.scan.fileCacheMaxBytes` / `fileCacheMaxEntries` | `0` / `0` |
| operator | `spec.extraEnv` override of both variables (no CRD field) | rendered `0` / `0` |

All three resolve through one function
(`siglake_storage::resolve_query_read_cache_config`), and it needs BOTH limits
positive: a positive byte budget with the entry limit left at `0` is off. Since
this card the query server says so at startup — a warning naming the pair its
container limit would size — because the chart and the operator both render both
variables explicitly, so "set one of them" is the likely way to ask for the
cache and not get it.

## The two rules that decide whether a budget buys anything

1. **Both limits positive**, or the cache is off and reserves nothing.
2. **An entry larger than a quarter of the byte budget is never cached**
   (`MAX_FILE_CACHE_ENTRY_FRACTION`, `QueryFileBatchCache::insert`). One
   oversized file would evict everything useful and then sit alone, so it is
   refused with `outcome="skip_oversized"` — and the budget's bytes are still
   subtracted from the query memory pool. A budget can be positive, reserved,
   and hold nothing, indefinitely.

Rule 2 is what makes the budget a statement about FILE SIZE. A compacted file is
`cold_target_file_bytes` (256 MiB of Parquet) at the scan's own decompression
estimate (`DEFAULT_SCAN_DECOMPRESSION_FACTOR = 5`) — about 1.25 GiB decoded — so
a cache that can hold ONE is 5 GiB. Against the recommendation below that is a
40 GiB container. Every pod this project packages is far under it: **enabled on
a packaged pod, this cache holds pre-compaction files and nothing else.**

## What one entry costs, measured

Local, release build, `file://` warehouse,
`crates/siglake-storage/tests/file_cache_budget_measurement.rs`. Fixture: 8
files x 65,536 rows of ~60-byte events, 8 scan partitions, batch size 8,192,
and the drained scan `SELECT raw FROM events` repeated five times per arm;
`warm` is the median of runs 2-5. Three runs on 2026-09-17; the entry figures
were identical in all three and the warm medians moved by at most 1.6 ms.

One entry, for one file's `raw` under this projection:

| currency | bytes | what it is |
| --- | --- | --- |
| priced | 5.5 MiB | what the cache charges its budget (`get_array_memory_size` per batch) |
| extent | 4.4 MiB | what the rows actually span (`get_slice_memory_size`) |
| retained | 5.5 MiB | the distinct backing allocations, deduplicated |

The same rows are 0.39 MiB of Parquet on disk. The cache holds the decoded form,
which is the number its budget buys — and `priced >= retained` here, so the
bytes subtracted from the pool are not an under-count of what the process cannot
give back. That direction is asserted, not assumed
(`file_cache_budget_bounds.rs`).

## What the budget buys

Five arms over the same fixture, each budget a multiple of the measured entry
so the bounds bite where the data puts them. Warm p50 in ms, three runs:

| arm | budget | holds | warm p50 (3 runs) | cold | counters per warm run |
| --- | --- | --- | --- | --- | --- |
| `off` | 0 | — | 9.3 / 7.6 / 8.2 | 9.4 / 9.1 / 8.9 | none |
| `fits` | 16 entries (88 MiB) | all 8 | **1.4 / 1.2 / 1.6** | 7.9 / 6.7 / 9.1 | `hit=7-8` |
| `bytes` | 4 entries (22 MiB) | 4 | 7.8 / 6.6 / 6.9 | 10.8 / 7.9 / 9.2 | `hit=4 miss=4 ins=3-4 evict=3-4` |
| `entries` | 88 MiB, `max_entries=2` | 2 | 8.1 / 7.1 / 6.5 | 9.7 / 7.4 / 8.9 | `hit=2 miss=6 ins=5-6 evict=5-6` |
| `oversize` | 3 entries (16 MiB) | 0 | 8.6 / 7.2 / 7.3 | 7.5 / 6.4 / 9.1 | `miss=8 oversize=8` |

Read it as one finding: **this cache is all-or-nothing on a repeated scan.** The
arm whose budget covers the working set is 5-7x faster warm. The arm holding
half of it is within noise of no cache at all — an LRU over a cyclic scan evicts
exactly what the next pass wants, so it inserts and evicts four entries per run
to serve four hits and still reads the other four files. The entry-bounded arm
is the same shape at a different bound. The oversized arm never caches anything
and pays the populate path on every pass.

So the sizing question is not "how much can I spare" but "does the working set
fit". A budget that half-covers it buys a few percent and costs its bytes.

Two costs the table does not show, both charged whether or not an entry sticks:

* **Population memory is off-pool and outside the budget.** The populate streams
  buffer decoded batches per partition until end-of-stream. Peak across the 8
  concurrent streams: 30-37 MiB retained against an 88 MiB budget — and 27-28
  MiB in the `oversize` arm, which inserts nothing. The bound is `budget / 4`
  per stream times the partition count, which can exceed the budget itself.
* **A predicate query loses its `LIMIT` pushdown** while the cache is on, since
  exact-capable filters are declared `Inexact` so a hit can be re-filtered
  (#4891 measured 0.1-0.6 ms on its fixture).

## The recommendation, and what accepting it costs

`siglake_storage::derive_file_cache_limits(memory_limit)` is the pair to set. It
recommends; nothing applies it. Bytes are `derive_read_cache_bytes`' second
value — an eighth of the container limit, 64 MiB floor, 8 GiB cap — which has
been documented as this cache's sizing recommendation since the F7 defaults
review and until now had no entry-limit twin. Entries are one per MiB of that
budget, at least `MAX_FILE_CACHE_ENTRY_FRACTION`: at the measured 5.5 MiB entry
the count cannot bind before the bytes do for a file of ordinary size, which is
what it is for — the entry limit is the guard on map growth over the small files
a sub-second WAL drain produces, and the BYTES are the bound that means
something to the pool.

From `file_cache_budget_table` in the measurement test (MiB, fraction 0.5):

| pod | recommended | entries | pool off | pool on | headroom off | headroom on | holds a compacted file |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2Gi | 256 | 256 | 640 | 512 | 640 | 512 | no |
| 4Gi | 512 | 512 | 1280 | 1024 | 1280 | 1024 | no |
| 8Gi | 1024 | 1024 | 2240 | 1728 | 2240 | 1728 | no |
| 16Gi | 2048 | 2048 | 4480 | 3456 | 4480 | 3456 | no |
| 32Gi | 4096 | 4096 | 10624 | 8576 | 10624 | 8576 | no |
| 64Gi | 8192 | 8192 | 22912 | 18816 | 22912 | 18816 | yes |

The 4Gi row is the one to read before enabling anything. That pod is the chart's
floor precisely because its 1280 MiB pool is one compacted file's decode
estimate, which is what a scan must reserve to open its FIRST file. Accepting
the recommendation there takes the pool to 1024 MiB — below the reservation the
floor exists to hold. **On the packaged pod, enabling this cache means raising
`query.resources.limits.memory` too.**

## What is tested, and where

* `crates/siglake-storage/tests/file_cache_budget_bounds.rs` — the byte bound,
  the entry bound, the quarter rule one byte either side, and the explicit zero,
  against a fixture whose entry size is measured first. It also asserts
  `priced >= retained`, which is what makes the pool subtraction honest.
* `crates/siglake-storage/tests/query_memory_bound.rs` — the budget with the
  cache on: its bytes join the read caches, the pool and the headroom both
  shrink, no pod over-commits, and the 4Gi pool drops below its decode
  reservation.
* `crates/siglake-storage/tests/exact_filter_file_cache.rs` — cold and warm
  answers equal the cache-disabled control for exact-capable predicates.
* `crates/siglake-storage/tests/file_cache_predicate_bypass.rs` — a converted
  predicate still prunes with the cache on.
* `crates/siglake-storage/tests/file_cache_population_shape.rs` — what does and
  does not populate (#4494's four readings).
* `crates/siglake-query-server/src/main.rs` tests — both flags are needed, a
  half-configuration is reported, and the resolved pair is what the pool
  subtracts.
* `scripts/check-chart.py` and `crates/siglake-operator/src/render.rs` tests —
  the packaged zeros, and the `extraEnv` override that turns it on.

## What a default-on proposal would still need

1. A populate path that survives cancellation, or the log-UI shapes leave
   nothing behind (#4494's option set; #4847's row-group prototype, disposition
   REVISE).
2. A fleet reading rather than a local one: warm hit rate and evictions on a
   real working set, which is #4691's and #4938's territory.
3. A working-set estimate the sizing can be checked against, since the
   measurement above says a budget that does not cover it buys nothing.
4. A pod size where the cache and the pool's decode reservation both fit — the
   4Gi floor has room for one of them.
5. A decision about the off-pool population memory, which today is neither
   budgeted nor bounded by the partition count.

## 2026-09-21: shared population-budget prototype (#5074)

The local prototype selects admission against the decoded-file cache's existing
process-wide byte budget. Completed entries and every admitted live population
batch use one conservative currency: `RecordBatch::get_array_memory_size`.
Admission uses a lock-free compare-and-swap before retaining a batch. EOF moves
the same charge into the completed entry, so handoff neither releases a gap nor
charges the bytes twice. Cancellation, read failure, an oversized candidate, a
duplicate entry and a contended insert keep the charge until their retained
batches are dropped, then release it. Refusal abandons optional population and
the scan continues with the same answer.

The gate is `QueryScanTuning::file_cache_population_bound_prototype`. It has no
environment variable, CLI flag, chart value or operator field. It adds no
cache-outcome label; local qualification reads `budget_refusals` and
`peak_accounted_bytes` from `decoded_file_cache_population_stats()`. Packaged
limits, cache defaults and memory-pool sizing are unchanged.

Two alternatives were not prototyped:

- Reserving population from the query pool charges the same bytes twice. The
  whole file-cache budget is already subtracted before that pool is sized. It
  would require changing the budget model as well as the population path.
- A per-query population count does not bound overlapping queries. A
  process-wide semaphore would put a count on streams rather than bytes and
  still needs a byte policy for differently sized candidates.

`file_cache_population_bound.rs` drives eight scan partitions and two
overlapping queries at a four-entry budget, then repeats with resident entries
and cancels a population through `LIMIT`. Every query returns all requested
rows, `peak_accounted_bytes <= max_bytes`, and in-flight charges return to zero
after insertion and cancellation. Unit coverage drives failed and contended
insertion; the existing oversize and population-shape fixtures cover the other
terminal paths. The row-group prototype uses the same `PopulationCharge` and
compiles through the same ownership rules.

The retained #3053 measurement was rerun in release mode on 2026-09-21 (8 files
x 65,536 rows, 8 partitions, 5 executions). One decoded entry priced 5.4 MiB;
the four-entry budget was 22 MiB. Times are cold / warm p50 / warm max:

| arm | times (ms) | warm cache work per run | installed / peak population |
| --- | --- | --- | --- |
| off | 9.8 / 9.1 / 11.1 | none | 0 / 0 MiB |
| fits (86 MiB) | 9.0 / 1.2 / 6.3 | 7 hits | 43.1 / 32.0 MiB retained |
| bytes (22 MiB) | 8.0 / 6.5 / 8.1 | 4 hits, 4 inserts, 4 evictions | 21.6 / 31.6 MiB retained |
| shared (22 MiB) | 8.9 / 6.5 / 6.6 | 4 hits, 4 refusals, no insertion or eviction | 21.6 / 21.1 MiB retained; 21.6 MiB accounted peak |
| oversize (16 MiB) | 7.6 / 8.4 / 8.7 | 8 oversized refusals | 0 / 27.0 MiB retained |

All arms returned the same 524,288 rows as the disabled control. The shared arm
enforced the configured bound, including populations that returned no cache
entry. It also exposed a policy defect: once residents fill the budget, no new
population can reach EOF and displace one. The existing LRU arm turns over four
entries per warm pass; the bounded arm freezes the first four and has the same
6.5 ms warm median. Which four files stay resident becomes scheduler order,
not recency.

## Disposition: REVISE

Do not adopt this admission rule in production. The hard bound and ownership
accounting are sound, but loss of resident turnover changes cache policy and
buys no time on the matched control. Production work must preserve the same
configured byte bound while making room for a completed candidate without
blocking on the cache mutex per batch. Task #5786 carries that implementation;
it must reuse this fixture and update `ARCHITECTURE.md`, `LIMITATIONS.md` and
the operator-facing docs if adopted. The in-process prototype remains as the
qualification record.
