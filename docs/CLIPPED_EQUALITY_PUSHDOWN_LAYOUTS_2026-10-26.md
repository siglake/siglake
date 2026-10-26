# Clipped equality-pushdown layouts (#7500)

[#7457](CLIPPED_EQUALITY_PUSHDOWN_ATTRIBUTION_2026-10-26.md) measured one
layout — a label interleaved across every page at 25% selectivity — and found
the reader predicate cost 23.6 ms of paired-median collect time while saving no
page and no requested byte. Its line 101 left the opposite case open: a rare or
clustered label, where row-group statistics or page selection can drop data the
residual-only route has to decode.

Three more layouts answer it. The reader predicate's sign flips. It is a 20.8 ms
loss on the interleaved fixture, an 11.7 ms saving at 0.2% selectivity with
nothing pruned at all, a 41.1 ms saving when statistics leave one file planned
instead of fifteen, and neutral on the fourth layout. Page-index `RowSelection`
flips with it, costing 2 ms where it rejects nothing and saving 9 ms where it
rejects pages.

No AWS run was made. Production pushdown policy is unchanged and this adds no
public knob.

## Matched fixtures

Each layout is one Iceberg snapshot: 15 Parquet files, 40,000 rows per file,
production writer properties (20,000-row page limit, 1,048,576-row row-group
ceiling), one row group per file. Only the placement of `source = 'us-east-2'`
changes.

| layout | target rows | placement | what bounds can drop |
| --- | ---: | --- | --- |
| `interleaved` | 150,000 (25%) | four labels cycling row by row | nothing |
| `rare_scattered` | 1,200 (0.2%) | one row in every 500, in every page | nothing |
| `rare_clustered` | 1,200 (0.2%) | 300 contiguous rows in the first page of each of four files | 11 files, and the second page of each opened file |
| `rare_clustered_single_file` | 1,200 (0.2%) | all 1,200 contiguous in one page of one file | 14 files, and that file's second page |

The three non-target labels (`ap-south-1`, `eu-west-1`, `us-east-1`) all sort
below `us-east-2`, so a page or row group holding only those has an upper bound
strictly below the target.

Every layout runs the same query:

```sql
SELECT timestamp, raw
FROM events
WHERE source = 'us-east-2'
LIMIT 100
```

and #7457's three in-process arms, each keeping DataFusion's exact residual
filter and the `[timestamp, source, raw]` scan schema:

1. `pushed`: reader predicate and page-index `RowSelection` enabled.
2. `no_selection`: reader predicate enabled, `RowSelection` disabled.
3. `suppressed`: reader predicate omitted; the residual filter is authoritative.

Each arm returns 100 distinct rows satisfying the equality on every layout.

## Result

Seven triples per layout, rotating order, one process, diagnostic build. Times
are medians of root-stream collection and the differences are paired per triple.
Counters are medians; byte and read counters are per opened file, because
wave-2 admission sometimes opens one more file before the clipped fetch closes.

The lane is shared, so the test was run four times. Run A below is the headline;
all four are in [Reproduction](#reproduction). Run B hit contention during
`rare_clustered` — every arm of that layout came out about three times slower —
and its figure for that layout is not used.

| layout | pushed | no selection | suppressed | pushed − suppressed (A/B/C/D) |
| --- | ---: | ---: | ---: | --- |
| `interleaved` | 27.866 ms | 25.771 ms | 7.255 ms | **+20.8 / +21.5 / +21.0 / +21.8 ms** |
| `rare_scattered` | 40.882 ms | 40.321 ms | 52.432 ms | **−11.7 / −10.3 / −9.9 / −8.9 ms** |
| `rare_clustered` | 35.196 ms | 43.964 ms | 36.746 ms | **−2.4 / (+8.0) / −1.7 / −3.7 ms** |
| `rare_clustered_single_file` | 11.882 ms | 14.576 ms | 53.185 ms | **−41.1 / −43.1 / −41.3 / −41.4 ms** |

`interleaved` reproduces #7457 on this machine and build: 27.866 / 25.771 /
7.255 ms against its reported 30.878 / 26.982 / 7.077 ms, and +20.766 ms against
its +23.643 ms. The rest of the table can be read against it.

Only `rare_clustered` is close enough to call neutral. Its arms overlap pair by
pair (pushed 32.5–37.9 ms, suppressed 35.2–40.2 ms in run C) and the paired
difference is a couple of milliseconds in either direction. The other three
layouts separate with no overlap between the pushed and suppressed arms in any
pair of runs A and C (run B was recorded as summaries only).

What each arm read:

| layout | arm | planned partitions | files opened | rows pruned by selection | data bytes requested / file | decoded Arrow bytes / file |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `interleaved` | pushed | 16 | 1 | 0 | 29,413 | 1,229,144 |
| | no selection | 16 | 1 | 0 | 29,413 | 1,229,144 |
| | suppressed | 16 | 1 | 0 | 29,413 | 2,326,872 |
| `rare_scattered` | pushed | 16 | 3 | 0 | 65,968 | 77,125 |
| | no selection | 16 | 3 | 0 | 65,968 | 77,125 |
| | suppressed | 16 | 4 | 0 | 98,952 | 8,373,944 |
| `rare_clustered` | pushed | 4 | 4 | 63,680 | 38,125 | 166,548 |
| | no selection | 4 | 4 | 0 | 38,125 | 166,548 |
| | suppressed | 16 | 2 | 0 | 100,750 | 4,887,732 |
| `rare_clustered_single_file` | pushed | 1 | 1 | 19,520 | 36,310 | 370,248 |
| | no selection | 1 | 1 | 0 | 36,310 | 370,248 |
| | suppressed | 16 | 4 | 0 | 100,082 | 6,090,166 |

`row_groups_pruned_stats` is 0 everywhere. File bounds drop the target-free
files during planning, before a reader opens anything, so the pruning shows up
as a smaller planned-partition count rather than as a pruned row group.

## Selectivity alone reverses the result

`rare_scattered` prunes nothing. Every page's bounds span the target, every row
group is read, `rows_pruned_selection` is 0 in all three arms, and the planned
partition count is 16 in all three. The only difference from `interleaved` is
that the predicate rejects 99.8% of the rows it sees instead of 75%.

That is enough to reverse the sign. The reader predicate saves 11.734 ms paired
median, with 109 times less decoded Arrow per opened file (77,125 against
8,373,944) and a third fewer requested data bytes per file. #7457's finding that
the predicate "saves neither a page nor a requested data byte" was a property of
its fixture's selectivity, not of the clipped path.

The mechanism is the one #7457 described, running the other way. The residual
filter above the scan can only reject rows the reader already materialised, so
the suppressed arm decodes `raw` for every row it reads; at 0.2% selectivity it
must read roughly 50,000 rows to find 100 matches. The reader filter evaluates
`source` first and materialises `raw` only for rows that survive. The cost
#7457 attributed to the Parquet row filter is still there — it is simply much
smaller than the decode it avoids once the predicate is selective.

## Page selection flips sign too

`RowSelection` is the difference between `pushed` and `no_selection`:

| layout | pushed − no_selection (A/B/C/D) | rows pruned by selection |
| --- | --- | ---: |
| `interleaved` | +1.97 / +1.68 / +1.95 / +0.90 ms | 0 |
| `rare_scattered` | +0.78 / +2.04 / +0.51 / +0.71 ms | 0 |
| `rare_clustered` | **−9.21 / −13.40 / −8.75 / −9.33 ms** | 63,680 |
| `rare_clustered_single_file` | **−2.69 / −2.54 / −3.23 / −2.71 ms** | 19,520 |

This is the most reproducible result in the set — it holds on all four runs,
including the contended one. Where selection rejects nothing it costs 0.5–2.0 ms
on all four runs, below #7457's 4.587 ms on the same layout; that gap is the
one figure of #7457's that did not reproduce here, and it is small either way.
Where it rejects pages it pays for itself
several times over: on `rare_clustered` the pushed arm is faster than the
no-selection arm in every pair of runs A and C with no overlap (run A
32.3–39.3 ms against 41.3–46.7 ms).

Selection cost and selection benefit are therefore separable at run time by the
same counter: `rows_pruned_selection`. On `rare_clustered_single_file` a full
second-page prune is 19,520 of a file's 40,000 rows — the first page closes at
20,480 rows because the 20,000-row limit is checked at 1,024-row write-batch
boundaries. The four-file total of 63,680 is below four times that and is stable
to the row across all seven pairs; the shortfall is not attributed here.

## Pruning is a plan-time effect, so prunable arms are not plan-matched

On a layout where the predicate can prune, the arms cannot be held to identical
physical plans, because the pruning itself happens during planning:

- `rare_clustered`: file bounds leave 4 planned partitions against the
  suppressed arm's 16. DataFusion inserts a `RepartitionExec` above the scan to
  refill `target_partitions`. Everything else, including the clipped-limit
  coalescer, is identical, and the test asserts that the plans differ by nothing
  but repartitioning.
- `rare_clustered_single_file`: bounds leave **one** planned partition.
  `apply_clipped_limit_backpressure` requires two input partitions
  (`crates/siglake-storage/src/query_provider.rs:7012`), so the reader-predicate
  arms get a plain `CoalescePartitionsExec` and lose the clipped-limit shape
  entirely, while the suppressed arm keeps it. The test pins this rather than
  working around it.

The −41.143 ms on that layout is therefore a combined plan-and-read difference,
not a reader-predicate measurement. The −11.734 ms on `rare_scattered`, where
all three arms plan 16 partitions and run the same wrappers, is the clean one.

A second consequence matters for any production gate: pruning reduces the
partitions available to clipped admission. On `rare_clustered` the suppressed
arm spread 16 partitions over the snapshot, found a matching file in 2 opened
files, and came out within about 2 ms of the pushed arm despite decoding 29
times more Arrow per file and requesting 2.6 times more data bytes per file.
Pruning to 4 partitions gave back most of what it saved. That is why
`rare_clustered` is the one layout here where the reader predicate is neutral on
wall time while being plainly cheaper on every I/O counter.

## Pushdown decision boundary

#7457 proposed that the clipped path should not apply the reader predicate when
the exact residual already remains above the scan. That holds for its fixture
and does not generalise. Applying it unconditionally would cost 11.7 ms on a
rare label with no pruning available, and 41.1 ms on a clustered one.

What separates the cases is not whether the residual is retained but how much
the reader filter rejects relative to what the residual-only route would decode.
Both of the signals a gate would need are already counted per scan:
`rows_pruned_selection` for the selection term, and the gap between rows the
reader filter sees and rows it emits for the predicate term. Neither is
available at planning time, which is where the current policy is decided.

This investigation makes no production policy change and adds no knob.

## What this does not establish

- No AWS or distributed measurement. These are single-process, diagnostic-build
  local numbers on a shared lane whose load varies between runs.
- `rare_clustered`'s wall-time difference between the pushed and suppressed arms
  is within run-to-run noise. Its counters are not.
- Nothing about the field result in
  [#7204's residual attribution](CLIPPED_LIMIT_RESIDUAL_ATTRIBUTION_2026-10-26.md).
  The fixtures here are not the field layout.
- One equality predicate on one dictionary-encoded low-cardinality column. No
  range predicate, no multi-column predicate, no `raw` text predicate.
- The four-file `rows_pruned_selection` total of 63,680 is not decomposed to a
  per-file figure.
- No gate is implemented, so nothing here is qualified as a shipped policy.

## Reproduction

```sh
cargo test -p siglake-storage \
  --test clipped_equality_pushdown_layouts \
  clipped_equality_pushdown_layout_attribution \
  -- --ignored --nocapture
```

Roughly four minutes; run alone on an idle lane. The deterministic assertions
cover exact answers in every arm, fixed scan projections, the residual filter
and reader-predicate presence per arm, each layout's pruning floor, and the
plan-shape difference on the two clustered layouts. Wall time is printed and is
not a CI threshold.

Four runs, 2026-10-26, 32-core shared lane. Every counter in the tables above
was identical in all four; only wall time moved. Runs A–C predate a
clippy-driven rewrite of one fixture predicate into the identical
`is_multiple_of` form; run D is the committed source and reproduces them.

| layout | run | pushed | no selection | suppressed | p − ns | p − s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `interleaved` | A | 27.866 | 25.771 | 7.255 | +1.969 | +20.766 |
| | B | 29.048 | 27.762 | 7.046 | +1.681 | +21.453 |
| | C | 27.874 | 25.929 | 6.873 | +1.945 | +21.001 |
| `rare_scattered` | A | 40.882 | 40.321 | 52.432 | +0.780 | −11.734 |
| | B | 45.226 | 43.685 | 55.684 | +2.039 | −10.285 |
| | C | 40.670 | 40.537 | 50.703 | +0.510 | −9.921 |
| `rare_clustered` | A | 35.196 | 43.964 | 36.746 | −9.210 | −2.380 |
| | B *(contended)* | 99.300 | 119.138 | 106.988 | −13.400 | +8.016 |
| | C | 35.548 | 43.310 | 37.048 | −8.753 | −1.722 |
| `rare_clustered_single_file` | A | 11.882 | 14.576 | 53.185 | −2.694 | −41.143 |
| | B | 11.757 | 14.136 | 53.844 | −2.540 | −43.124 |
| | C | 12.064 | 15.394 | 54.497 | −3.234 | −41.277 |

Run D recorded paired differences only. Its `pushed − suppressed` figures are
+21.771, −8.861, −3.652 and −41.403 ms in the table's layout order, and its
`pushed − no_selection` figures are +0.896, +0.714, −9.332 and −2.705 ms.

Run B's `rare_clustered` block ran at a 1-minute load average of 18 on 32 cores
and every arm of that layout is about three times its A, C and D value. Its
paired differences for that layout are discarded; its other three layouts agree
with the rest and are kept.
