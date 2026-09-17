# Design — row-group population for the decoded-file cache (#4847 qualification)

Status (2026-09-16): **local prototype, nothing wired, disposition REVISE.**
The shipped policy is unchanged and stays drained-scan-only: the packaged cache
is still off (`SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_{BYTES,ENTRIES}` default to 0),
the operator's 4Gi clamp is untouched, and the prototype is reachable only from
inside the process through `QueryScanTuning::file_cache_row_group_prototype`,
which has no environment variable, CLI flag, chart value or operator field.

#4494 established why the cache served 0 of 1,160 and 0 of 1,065 requests on the
2026-09-15 50G rounds: the populate stream inserts in its end-of-stream arm, and
a browse whose `LIMIT` is satisfied from the first batches drops it before that.
It left four options and stopped, because choosing between them is a policy
call. This document qualifies option (c) — make the cache unit a ROW GROUP, so a
read that stops early still leaves whole units behind — against the two controls
that matter (the shipped whole-file policy, and no cache at all), and records
what the measurement says.

## What the prototype does

Cache unit: one row group of one data file, under one projection, delete set and
direction. The whole-file entry key is
`path:start:length:fields:deletes:reverse`; the row-group key is
`path:rg=<index>:fields:deletes:reverse`. Dropping the byte range is deliberate
— the group's index pins its bytes, so two splits of one file address the same
group — and nothing else about identity moves.

Two facts make this implementable without touching the vendored fork:

* parquet-rs never lets a record batch straddle a row group, so a decoded prefix
  that reaches a boundary is a set of whole batches. The prototype does not
  assume it: each batch is checked against the group's footer row count, and a
  batch that would overshoot stops population for that task and charges
  `outcome="misaligned"`. Nothing is sliced, so no entry retains a buffer whose
  rows it does not own.
* the only row-group selector the reader exposes from outside is the task's byte
  range (`filter_row_groups_by_byte_range`), which accumulates synthetic offsets
  from 4 over `compressed_size()`. Recomputing those offsets from the footer
  addresses any contiguous span of groups exactly, so a task whose first `k`
  groups are cached is served as `k` cached groups followed by a reader over a
  derived `FileScanTask` covering the rest.

Population inserts at every boundary it reaches. A partial group left in the
buffer is dropped and charges `outcome="abandoned"`, in #4846's sense exactly
(it decoded batches, kept them, and never reached an insert). `hit` still means
the task needed no reader and `miss` that one was opened; the prototype's own
detail — `group_inserted`, `partial_serve`, `served_whole_task`, `misaligned`,
`layout_read`/`layout_hit`/`layout_error`, `refused` — goes to
`siglake_query_scan_file_cache_row_group_total`, which no dashboard or
pre-registration reads.

Refusals, all falling back to the shipped whole-file path: a reversed read (the
ordered path, which does not use this cache at all), a task carrying delete
files (its row counts no longer match the footer's, so no group could close), and
a file whose footer will not read. A task with a raw-text or promoted-column
prune still bypasses the cache before any of this, at either granularity.

Footer layouts are read once per data file and kept in a process-wide map keyed
by path (files are immutable); the read skips the page, column and offset
indexes.

## What it buys, and where

Measured 2026-09-16 on this box, release build, `file://` warehouse, from
`crates/siglake-storage/tests/row_group_cache_measurement.rs`. Fixture: 4 files
x 155,648 rows, each written at the row-group floor
(`IcebergTuning::target_row_group_bytes = 1` rounds down to
`MIN_ROW_GROUP_ROWS`), so every file holds group 0 = 131,072 rows and group 1 =
24,576. Cache 8 GiB / 16,384 entries — the bench rounds' tuning. 4 target
partitions, batch size 8,192, browse = `… WHERE <predicate> LIMIT 100`, 5
consecutive executions per arm; `warm` is the median of executions 2-5. The
table is one run; two further runs on 2026-09-17 reproduced every arm's warm p50
within 0.4 ms and every figure in the memory table exactly, so read the
milliseconds below as +/- 0.4 and the ratios as the ranges given in the text.

Regimes cross two axes. How deep the clip reads: `bound` puts the matching rows
at the END of a group, so the clip covers group 0 whole; `inside` puts them at
its head, so the clip ends in the first batch. Whether the predicate reaches the
reader: `push` is `host = '<label>'`, which converts to an Iceberg predicate and
therefore also gets page-index row selection; `resid` is
`lower(host) = '<label>'`, which does not convert, so every arm reads the same
rows. Both labels appear in both groups, so row-group statistics cannot drop
either group in either regime.

| regime | arm | cold ms | warm p50 ms | entries after the browses | abandoned populations |
| --- | --- | --- | --- | --- | --- |
| resid/bound | disabled | 6.1 | 6.2 | — | — |
| resid/bound | whole_file | 6.8 | 6.7 | 0 | 20 |
| resid/bound | row_group | 6.4 | **2.0** | 4 | 0 |
| resid/inside | disabled | 2.8 | 2.8 | — | — |
| resid/inside | whole_file | 2.6 | 2.7 | 0 | 20 |
| resid/inside | row_group | 2.8 | 2.7 | 0 | 20 |
| push/bound | disabled | 1.9 | 2.3 | — | — |
| push/bound | whole_file | 9.0 | 6.4 | 0 | 20 |
| push/bound | row_group | 6.1 | **1.8** | 4 | 0 |
| push/inside | disabled | 2.3 | 2.5 | — | — |
| push/inside | whole_file | 2.4 | 2.6 | 0 | 20 |
| push/inside | row_group | 2.4 | 2.5 | 0 | 20 |

Three readings.

**Where the clip covers a whole group, the prototype is the only arm that gets
faster on repetition.** `resid/bound` warm p50 falls from 6.7 ms (whole-file) and
6.2 ms (no cache) to 2.0 ms, a 3.2x improvement over the shipped policy
(3.1-3.4x across the three runs), and the
reader's fetched bytes on the warm executions fall from 0.5 MiB to under 0.05
MiB. The shipped policy in the same regime abandons 4 populations per execution,
20 over the 5, and leaves the cache empty.

**Where it does not, the prototype is the shipped policy plus a footer read.**
Both `inside` regimes have all three arms inside 0.3 ms of each other, the
prototype's browses leave 0 entries, and it charges the same 20 abandoned
populations. The clip covered no boundary, so there was nothing to insert.

**A cache-enabled install pays for stripping the predicate, at either
granularity.** `push/bound` is the case to read carefully: with the cache off the
browse runs in 2.3 ms because the reader's page index skips the pages the
predicate cannot match (the scan emits ~0 MiB). Both cache arms strip the
predicate from the read — which is what makes an entry reusable by another
query — and therefore decode the whole projection: 32.6 MiB emitted, 6.4 ms warm
for the whole-file policy, **2.8x slower than not caching at all**. The
prototype recovers to 1.8 ms once populated, slightly under the no-cache arm,
but only because it has the decoded rows in memory. This cost belongs to the
cache's design, not to its granularity, and it applies to every convertible
predicate.

## Memory

Two currencies were asked for and both are reported by
`siglake_storage::decoded_file_cache_footprint()`: `priced_bytes` is what the
cache charges against its budget (`get_array_memory_size`), `extent_bytes`
prices the rows the entries span (`ArrayData::get_slice_memory_size`), and
`retained_bytes` sums the DISTINCT backing allocations, deduplicated across the
whole cache by allocation base pointer.

Population memory is now measured rather than bounded on paper. #4494 recorded
the risk as "up to budget/4 per stream, concurrently per partition" with nothing
to read it from: the `siglake_query_scan_file_cache_bytes` gauge is written from
the insert, so a population that never inserts is invisible in it — which is
every population on a clipped browse. `PopulationMeter` charges each buffered
batch and releases on insert or drop, and
`decoded_file_cache_population_stats()` reports the in-flight totals, their peak
and the peak number of concurrent populate streams.

Across the 4-partition browse arms above, the peak population was 24.3-24.7 MiB
of extent (31.3-31.7 MiB retained) over 4 concurrent streams for BOTH policies.
That fixture cannot separate their bounds: its group 0 is 84% of the file, so
"buffer the file" and "buffer a group" are nearly the same quantity. One file of
four floor-sized groups, drained once, separates them:

| arm | cache entries | priced MiB | extent MiB | retained MiB | population peak extent MiB | retained MiB |
| --- | --- | --- | --- | --- | --- | --- |
| whole_file | 1 | 23.6 | 20.8 | 23.6 | 20.8 | 23.6 |
| row_group | 4 | 23.6 | 20.8 | 23.6 | **5.2** | **6.3** |

Same bytes cached, one quarter of the off-pool peak per stream — the population
bound becomes one row group instead of one file, and one row group is what the
write path targets at 256 MB uncompressed (`MIN_ROW_GROUP_ROWS` = 131,072 rows,
`MAX_ROW_GROUP_ROWS` = 4 Mi). The `budget / MAX_FILE_CACHE_ENTRY_FRACTION` entry
bound (2 GiB at 8 GiB) still applies per group and is now reached far less
often.

Retained equals priced in every arm measured, and extent is 0.88x of it. These
entries are whole reader-produced batches, so `get_array_memory_size` is not the
over-count it is on a merge-path slice (#4754): the allocation it prices is one
the entry alone holds.

## Exact answers

Checked against the cache-disabled control, not against expectations:

* `crates/siglake-storage/tests/row_group_cache_population_shape.rs` (a gate,
  ~7 s) compares five phases on a two-group file against the control's rows in
  the control's order — the clipped browse, its repeat served from a cached
  group 0 plus a read of group 1, a drained pass, a browse needing no reader,
  and the unclipped predicate answer over a half-populated file.
* the measurement asserts, for all 12 arm/regime pairs, that every clipped row
  appears in the control's unclipped answer, and that the unclipped answer over
  the partially populated cache equals the control's sorted row for row.

Not verified: a task that is itself a sub-file split (siglake plans one task per
file today, so the derived-range arithmetic for a split's remainder is exercised
only from index 0), the reversed/ordered path (refused by construction),
eviction of row-group entries under a budget too small to hold them, files with
positional deletes (refused), and any of this against S3 rather than a local
warehouse.

## Disposition: REVISE

The mechanism works and is exact, and it fixes the memory profile of population.
It does not, on this evidence, earn adoption:

1. **The win is conditional on a quantity nobody has measured in production.** A
   clipped browse populates only if it decodes a whole row group. At the
   shipped floor that is 131,072 rows, so a `LIMIT 100` browse qualifies only
   when its residual predicate matches fewer than about 1 row in 1,300. Whether
   the rounds' `label_filter`, `label_filter_last25` and `multi_label_and`
   shapes sit above or below that line is not in #4494's export — it counted
   misses and abandonments, not rows decoded per clipped browse. In the regime
   where they are above it, this change buys a footer read and nothing else.
2. **The larger effect for a convertible predicate is the stripped predicate,
   not the granularity.** Turning the shipped cache on made a page-prunable
   browse 2.8x slower than leaving it off. Any decision to adopt row-group
   population should be taken after that cost is either accepted with numbers or
   removed (predicate-carrying entries, or declining to populate when the reader
   would prune). *Removed on 2026-09-17 by #4891, by declining — which also
   removes the prototype's best regime. See "reason (2) is fixed" below.*
3. **Scan attribution goes partly blind.** A group served from cache builds no
   reader, so `row_groups_read`, `rows_pruned_selection` and fetched bytes
   under-report — already true of a whole-file hit, but a task can now be half
   read, which no counter distinguishes.

So: keep option (a) as shipped, keep this prototype behind its in-process gate
as the qualification record, and re-open the choice when (1) has a number.
Reason (1) is filed as #4890 (measure rows decoded per clipped browse on the
SHIPPED populate path, against the 131,072-row floor) and reason (2) as #4891
(the stripped predicate, which costs a cache-enabled install 2.8x on a
page-prunable browse at either granularity). Reason (3) is work only an
adoption would owe, so it is recorded here rather than filed.

## 2026-09-17: reason (2) is fixed, and it changes what `push/*` measures

#4891 took the first of the two shapes it offered: a task that carries a
converted predicate no longer reaches the populate path at all. It takes the
bypass the raw-text and promoted-column prunes already took
(`open_task_batch_stream_cached`, `crates/siglake-storage/src/query_provider.rs`)
and is read with its predicate intact, at either granularity — the bypass sits
above the prototype branch, so this is one policy and not two. Lookups are
untouched: a predicate query still HITS an entry a predicate-free scan left.

Same fixture, same box, same command, four runs on 2026-09-17 (the numbers above
are not rewritten; these are the post-fix arms, and the matched pairs within a
run are what to read — the box is shared and the absolute values drift between
runs):

| regime | arm | warm p50 ms, 4 runs | scan emitted, per warm execution |
| --- | --- | --- | --- |
| push/bound | disabled | 2.0 / 2.3 / 2.4 / 2.4 | 0.0 MiB |
| push/bound | whole_file | 2.0 / 2.5 / 2.7 / 2.5 | 0.0 MiB (was 32.6) |
| push/bound | row_group | 2.2 / 2.4 / 2.6 / 2.6 | 0.0 MiB |
| push/inside | disabled | 2.3 / 2.5 / 2.6 / 2.4 | 0.0 MiB |
| push/inside | whole_file | 2.5 / 2.6 / 3.2 / 3.0 | 0.5 MiB |
| push/inside | row_group | 2.2 / 2.6 / 2.6 / 2.9 | 0.5 MiB |

`push/bound`, the 2.8x regression, is within 0.3 ms of the cache-disabled arm in
every run, and the emitted bytes that caused it are gone: the pages the page
index skips are skipped again. The `resid/*` arms are unchanged, as they must
be — nothing there converts, so nothing bypasses.

Two things this does not claim. **The cache-on arm is not free on a predicate
browse.** `push/inside` stays 0.1-0.6 ms above its control and still emits 0.5
MiB where the control emits 0.0, because a cache-enabled provider declares
exact-capable filters `Inexact` (`filter_pushdown_with_file_cache`) so that a
hit's unfiltered batches are re-filtered. DataFusion then cannot push the
`LIMIT` into the scan, and the scan emits more rows before it stops. That is the
price of hits being reusable and is unrelated to the populate path. **And one
fixture is not a guarantee**: this is a local `file://` warehouse, two row
groups per file, one predicate shape.

For the prototype, reason (2) closing also removes its best regime. `push/bound`
was where per-row-group population beat no cache outright (1.8 ms against 2.3);
it now bypasses like everything else with a converted predicate, so the case for
adoption rests on `resid/bound` alone — 2.1 ms against 6.1, and only when the
clip covers a whole 131,072-row group, which is still the unmeasured quantity of
reason (1) (#4890). The gate
(`crates/siglake-storage/tests/row_group_cache_population_shape.rs`) browses
under `lower(host)` for the same reason: a converted predicate reaches no
population to assert on.

## 2026-09-17: reason (1) is measurable, and what the number will and will not say

#4890 instruments the SHIPPED whole-file path, not the prototype (whose
counters sit behind `file_cache_row_group_prototype`, which nothing outside the
process can set). `CachePopulateStream` now records one observation per
population in `Drop`:

```
siglake_query_scan_file_cache_populate_rows{outcome="completed|clipped|unpolled|error"}
```

The quantity is defined narrowly. It is the rows the inner reader HANDED the
population, summed over the stream, counted before the residual `FilterExec`
above the scan drops any of them. It is not the rows the query returned, not
the rows the cache kept (a candidate that crosses the entry bound is discarded
and the count keeps rising), and not total physical decoder work (row selection
the reader applies under it — positional deletes, time bounds — and decoded
pages that never became a batch are not in it). The populate read carries no
predicate and no prune spec, because a task with either bypasses population
entirely since #4891, so nothing page-prunes below it. On a `LIMIT 10` browse
over a 1,024-row file at 256-row batches the observation is 256: one batch, not
10 rows and not the file.

The four outcomes are the whole label set, and each has a different reading.
`completed` is a drained read. `clipped` is a population dropped before
end-of-stream — a `LIMIT` satisfied early, and equally a cancelled or
failed-elsewhere query, which the stream cannot tell apart, so a round reads it
against its own query outcomes. `unpolled` is a task the plan opened and never
polled (zero by construction, charged `miss` and `abandoned` like any other, and
excluded from the qualifying fraction). `error` is a failed read, also excluded.

Per request, `stats.scan.file_cache_populate_rows` carries the same depth and
`stats.scan.file_cache_bypasses` the tasks that declined population. The second
exists because after #4891 the rounds' label shapes produce NO population
samples, and "decoded nothing" and "was never eligible" are opposite readings of
the same absence.

Reading it: `scripts/read-file-cache-populate-depth.py --shape NAME=after.txt
[--baseline NAME=before.txt] [--geometry NAME=geom.json] [--stats
NAME=responses.json]`. The floor is exact — `POPULATE_ROW_BUCKETS` carries an
edge at 131,071, so `+Inf − le("131071")` is the population handed at least
`MIN_ROW_GROUP_ROWS` rows — and the reader refuses an export that was
re-bucketed away from that edge or rendered as a summary rather than reporting a
number it cannot support.

Two things the number will not settle, both stated by the reader rather than
left to the reader's reader. **Reaching the floor is necessary, not
sufficient**: `MAX_ROW_GROUP_ROWS` is 4 Mi, so a file whose groups are larger
closes none of them at 131,072 rows, and a read that does not start on a group
boundary closes none at any depth — hence `--geometry`, which downgrades the
claim to an upper bound when the measured files hold larger groups. **And
selectivity is not location**: a predicate matching 1 row in 1,300 says how many
rows a clip must cross, not where the matches sit, so a shape can clear the
floor on one file and not on the next.

Local evidence only. `crates/siglake-storage/tests/file_cache_populate_depth.rs`
pins the accounting (drained, clipped, hit, bypassed),
`crates/siglake-query-server/tests/file_cache_populate_depth_stats.rs` pins the
per-request fields through the router, and
`scripts/check-file-cache-populate-depth-reader.sh` pins the reader against
fixture exports. None of it is fleet evidence: the rounds' `label_filter`,
`label_filter_last25` and `multi_label_and` numbers still need a prepared normal
round, and until they exist reason (1) is measurable but unmeasured. That round
is #4938 (held for the next prepared normal round, isolated per shape, with the
cache overrides and footer geometry recorded); the round-collector work it needs
is siglake-benchmarks #4939.

## Reproduce

```sh
# the gates
cargo test -p siglake-storage --test row_group_cache_population_shape
cargo test -p siglake-storage --test file_cache_predicate_bypass
cargo test -p siglake-storage --test file_cache_populate_depth
cargo test -p siglake-query-server --test file_cache_populate_depth_stats
scripts/check-file-cache-populate-depth-reader.sh

# the numbers in this document
cargo test --release -p siglake-storage --test row_group_cache_measurement \
  -- --ignored --nocapture
```
