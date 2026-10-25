# Staged partition start for an unordered clipped `LIMIT` (#4865)

Local qualification, 2026-09-17. Implementation: `ScanAdmission` in
`crates/siglake-storage/src/query_provider.rs`; evidence
`crates/siglake-storage/tests/clipped_limit_admission.rs`.

## The shape

`SELECT … WHERE <residual> LIMIT n` with no `ORDER BY` keeps the parallel pruned
plan: one partition per data file, no ordering to advertise, and a residual
`FilterExec` above the scan that DataFusion cannot push a limit through. Each
partition therefore believes it owes its whole file, and nothing tells it the
query wants `n` rows in total.

On a fully compacted 15-file / 98.5M-row table (benchmarks #4840, capture
`results/20260916-aws.214836`, build `5348199`, 4Gi query pod, result cache off)
all 15 partitions started together, every one landed its first batch within
2.0–3.4 ms of the others, and each had decoded 1.3–4.0 MB by the time the global
limit cancelled it:

```
p=12 files=1 prows=7,056,853 first=4.50ms out=16,384 dec=1,293,184 el=10.13ms
p= 5 files=1 prows=7,067,100 first=3.16ms out=23,552 dec=1,810,440 el=10.33ms
... 15 partitions, all active ...
TOTAL dec=32,055,260 fet=76,682,163 out_rows=419,840   (100 rows wanted)
```

419,840 rows decoded to return 100. The same query on a layout that still had an
unmerged 22,228-row tail (run #90, `results/20260915-aws.052029`) decoded 597,696
bytes: the tiny partition answered the limit at 0.76 ms and the whole-file
partitions were cancelled having decoded nothing. That report compares two
builds with different topology and rebuild settings, so its latency attribution
is context, not a matched A/B; the numbers qualified below are local and
matched.

#4353 coalesced singleton file partitions for small *ordered* `LIMIT` scans.
This is the opposite case. The partitions are not small, so merging them is
wrong; what is wrong is that they all start.

## The fix

`ScanAdmission` stages the start. A partition takes a ticket when it is
executed and runs once the ramp passes its ticket. The ramp starts at one
partition and multiplies by `wave` (default 2) each time the cumulative credits
reach the admitted width. A partition end pays one credit immediately. Raw
source batches pay one credit per eight batches: the residual predicate lives
above the scan, so a raw batch does not mean the query needs another partition.

* A limit answered before the eighth source batch stays on the first partition.
* A sparse term, or a term with too few matches, reaches full fan-out after
  64 batches on the 15-file layout, still far short of one 7M-row file.

The eight-batch pace targets #6721's retained 50G failure. The earlier ramp
reached all 15 partitions after eight raw batches. The warm scans fetched zero
object-store bytes, yet decoded 11.8--31.4 MB and 154,624--445,440 rows for 100
results; collect alone took 6.2--20.4 ms. The batch credit was therefore a plausible bottleneck. The matched cloud round still must establish whether
this pacing restores the original latency ceilings. Eight batches per credit
keeps the common predicate on its first partition and bounds the sparse case
above.

The credit count is cumulative and is never reset. A per-wave counter deadlocks
the shape that pays nothing but its ends: with the ramp at 2 and its first
partition already finished — bloom-pruned, no batch — only one partition is left
to pay and the wave never completes. Against the cumulative count the `admitted`
partitions pay one credit each just by ending, which is exactly the threshold,
so the ramp widens even if not one batch is ever emitted. This is not
hypothetical: it hung `too_few_matches_return_every_qualifying_row` on the first
implementation.

### Why it cannot lose a row

The ramp decides WHEN a partition starts, never how much of it is read. A
partition that waits reads every row it would have read; the limit still lives
above the residual filter; no source-row cap is pushed anywhere. The scope is
the query shape, not the layout: the scan only builds a ramp when the session
carries `ClippedScanLimit`, which the SQL layer sets only where every operator
between the scan and the limit passes rows through unchanged (one plain table,
no join/CTE/`GROUP BY`/`DISTINCT`/aggregate/window/subquery, no `ORDER BY`), and
which already folds `OFFSET` into its value. Order-preserving scans are excluded
outright, which matters for more than symmetry: a `SortPreservingMerge` polls
every partition for a first batch before it can emit anything, so gating one
would deadlock rather than slow down.

Which requests reach the ramp at all: `clipping_scan_limit` and the implicit
newest-first rewrite read the same shape test (`default_order_target_table`),
and a rewritten query is ordered, so it leaves the ramp by the exclusion above.
What is left is the shape that qualifies for the hint and is not rewritten —
`default_order=false`, `Priority::Batch`, or a table whose index does not order
by canonical `timestamp` (`sql.rs::resolve_default_order_index`). The capture
this card was filed from is one of those: its per-partition profiles are an
unordered plan.

Liveness for the unordered case: if tickets are waiting then `admitted <
partitions`, the tickets below `admitted` are all registered, and each pays at
least the credit it owes by ending. Tickets are handed out modulo the partition
count, so a caller that executes a single partition takes ticket 0 and runs at
once, and a plan executed a second time finds the ramp already open — it
degrades to the ungated behavior rather than blocking.

## Which group starts first (#6979)

The ramp decides how many streams run; until #6979 nothing decided WHICH files
the first one read. Two races picked it:

* `CoalescePartitionsExec::execute` iterates partitions in index order, but
  `run_input` (datafusion-physical-plan 53.1.0, `stream.rs`) spawns a task and
  calls `execute` inside it. Tickets were therefore handed out in whatever
  order the runtime ran those tasks, so ticket 0 went to an arbitrary
  partition.
* `partition_file_tasks` sorted by `Reverse(record_count)` with a stable sort
  over the order `plan_files` finished its manifest entries in, and that
  planner fans its manifests out concurrently. On a compacted table every file
  has the same record count, so the tie was decided by the planner's race.

Five warmed runs of one `keyword` query opened three different first files in
the retained 2026-10-19 n10 reader capture (`latency-details.json`, held with
the round rather than in this tree): three fetched about 6.09 MB over three
object-store reads and collected 379–508 ms. The other two fetched nothing and
collected in 11.5 ms, but they still scanned 4,096 rows, decoded 251,760 and
310,128 bytes, and reported zero bloom/statistics pruning. The capture proves
variable fetched work, not that either file was prunable; cache state was not
controlled.

Both are now pinned:

* the task sort is a total order — weight, then data file path, then byte range
  — so the split is a pure function of the file set for a given snapshot and
  predicate;
* a stream reads the file group its admission TICKET names, not the one its
  partition index names. The first stream to register reads group 0 whatever
  the scheduler does, and the ramp then opens the groups in weight order.

Reading by ticket is safe here because the groups partition one file set and
this shape advertises no ordering: `admission` is only built where
`preserve_task_order` is false, so the union the parent coalesce and
`GlobalLimitExec` truncate is the same multiset of rows whichever stream reads
which group. No sort key is advertised and none is implied — group 0 is first
by weight, not by time. Ticket allocation still wraps, so the liveness
argument above is unchanged, and a caller that executes one partition still
takes ticket 0, runs at once, and now reads a known group.

The cost is that a partition INDEX no longer names a file set. Per-partition
scan counters describe whichever group ran there (their sums are unchanged),
and `partition_statistics(Some(p))` reports unknown while a ramp is active —
the table-wide answer, which is the one `AggregateStatistics` uses to answer
`count(*)` without IO, stays exact.

Determinism is all this buys. It does not make the first group the cheapest
one, and the retained spread above is not a latency claim. Pinning the choice
fixes which observed work profile the query gets; that capture does not
establish why the profiles differed.

Evidence: `the_group_opened_first_does_not_depend_on_which_partition_starts`
starts a different partition index first on four freshly planned scans and
requires the same file set each time, then starts every partition in reverse
order and requires the groups to still cover every file exactly once and every
source row once. `an_exhausted_first_group_still_fills_the_limit` picks a
target file the first group does not hold, so the head of the ramp ends having
emitted nothing and the limit fills from behind it.
`cancelling_a_clipped_scan_ends_the_streams_behind_the_ramp` drops the root
stream after one batch and requires every stream that began to end.
`partition_file_tasks_splits_the_same_way_whatever_order_the_planner_returns`
holds the sort. Both halves fail on the pre-#6979 code: the first-group
assertion reads `{15}` where it expects `{0}`.

## Cost-order candidate (#7000)

The measured rule was metadata-only: after the existing greedy split, rank
groups that contain at least the clipped source-row count ahead of groups that
do not, then sort by
`ceil(compressed_file_bytes * clipped_limit / record_count)`. File path and
byte range are the total tie-break. If any group lacks a record count, retain
the weight-first order for the whole scan. Group membership never changes.
The query server does not select this rule; `ClippedGroupOrder` is a
per-session test control.

This rule uses the file length and record count already present in Iceberg scan
tasks. It performs zero metadata or index probes. `raw_prune_spec` says which
predicate hints a reader may apply, not whether one file will prune, so it is
not a cost signal. Probing every Parquet footer would charge at least one read
per candidate before the scan knew which group to start; this candidate did not
hide that cost behind planning.

The local fixture holds one snapshot, predicate and four singleton groups. The
weight-first head is a 12,000-row readable file. A smaller, highly compressible
6,000-row file is not group 0 and its whole-file trigram bloom rejects the
predicate. The candidate's bytes-per-row estimate moves that prunable file to
the head. Both arms bypass reader caches and drain ticket-ordered groups
sequentially until 20 exact matches have been collected, so task scheduling
cannot add speculative readers. Shared planning caches are warmed before both
timed arms.

| order | planning probes | planning | files read | bloom-pruned files | object-store reads | fetched bytes | decoded bytes | drain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| weight first | 0 | 3.60 ms | 1 | 0 | 4 | 2,817,594 | 1,426,896 | 14.46 ms |
| estimated bytes to limit | 0 | 5.13 ms | 2 | 1 | 6 | 2,904,984 | 1,426,896 | 17.64 ms |

The candidate decoded the same bytes and added two object-store reads plus
87,390 fetched bytes. One local timing pair is descriptive; decoded bytes and
read counts are the acceptance evidence. The production choice therefore
remains deterministic weight-first. The fixture is retained as
`estimated_cost_order_measures_a_non_head_prunable_file`; a later policy needs
a per-file signal whose acquisition cost is part of its comparison.

## Residual request planning (#7100)

The matched #7076 candidate still missed the unchanged `keyword`,
`keyword_last25` and `substring_scan` ceilings at 23.12, 25.18 and 16.32 ms.
Its warm responses attribute about 1.1--1.3 ms to physical planning and
7.9--14.4 ms to collection for those three shapes; rendering stays below
0.1 ms. Across the retained measured interval, the server phase counters add
an average 0.61 ms for table registration and 1.86 ms for cost estimation.
The scan-cost cache refreshed only three times in that interval, so the
repeated estimate time was outside the snapshot-keyed file walk.

The remaining repeatable metadata work was `estimate()` calling
`list_indexes()` for every managed-index query. That operation enumerates the
namespace and performs an uncached `load_table` for every managed index, even
though registration already loaded the referenced table through the
bounded-staleness table cache. Cost estimation now reads only each referenced
index's cached configuration. The accessor retains a fresh catalog-row check,
so a dropped index cannot resolve through an old cache entry. `scan_cost`
remains snapshot-keyed and supplies the same exact byte, row and file totals.

`a_warm_managed_index_estimate_does_not_reload_table_metadata` removes the
target's `metadata.json` files after registration and a first exact estimate.
The uncached control then fails, while the repeated estimate returns the same
exact rows, bytes and file count. This is a bounded local cause proof, not an
AWS speedup claim; the unchanged matched qualification must be rerun against
the gated source.

## Coalesce backpressure (#7339)

The restored #7204 candidate removed repeated cost-mapping work, but its warm
collection still averaged 15.09 ms for `keyword`, 19.18 ms for
`keyword_last25`, and 8.83 ms for `substring_scan`. The five measured samples
scanned 386,188--400,524, 231,424--238,592, and 362,636--400,524 source rows,
respectively. Each `keyword` response opened all 15 cached readers and decoded
27.9--28.9 MB despite returning 100 rows. Cost estimation averaged only
0.126--0.158 ms, so it was no longer the material phase.

The remaining avoidable work was between admission and cancellation.
DataFusion's `CoalescePartitionsExec` sizes its shared channel to the input
partition count. A producer runs the complete residual filter in its partition
task, sends a filtered batch into that channel, and immediately polls the scan
again while the root has not yet observed the first batch. With 15 partitions,
one admitted producer could buffer many filtered batches. Its raw source
batches also paid paced admission credits, opening more readers before the
global limit dropped the plan.

The clipped shape now substitutes an equivalent coalescer whose shared channel
holds one batch. At most one batch is buffered and one is held by a blocked
sender; the root then observes the first filtered batch and cancellation closes
the senders. This changes neither the filter nor the `LIMIT`: every admitted
partition still runs the complete predicate, sparse predicates retain the same
batch and terminal admission credits, and unordered SQL continues to permit
any qualifying rows.

`one_batch_coalesce_bounds_dense_residual_producer_runahead` holds a 16-file
snapshot and dense residual predicate fixed. The bounded arm returned 100
distinct qualifying rows while opening one reader and decoding 138,364 bytes;
the ordinary-coalesce control returned the same cardinality and predicate
domain while opening 16 readers and decoding 691,820 bytes. Drain time was
7.65 ms versus 12.35 ms in that diagnostic run; the work counters, rather than
one local timing pair, are the regression oracle. This local result does not
qualify the AWS ceilings. The unchanged matched field qualification remains
required.

## Local measurement

`the_admission_ramp_stops_every_partition_decoding_for_a_clipped_limit`: 16
files × 3,000 rows, 64 matches in every file, `SELECT raw FROM events WHERE raw
LIKE '%queen%' LIMIT 20`, 16 partitions. One layout; the arms differ only in the
`ClippedAdmissionWave` session extension (`0` is the pre-#4865 scan). The
regression runs seven interleaved pairs and reads counters after
`settle_scan_partitions` so the cancelled partitions' folds are included. This
table is the original three-pair local capture:

| pair | gated files_read | gated decoded_bytes | gated drain | ungated files_read | ungated decoded_bytes | ungated drain |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 4 | 284,912 | 3.92 ms | 10 | 569,824 | 3.12 ms |
| 1 | 4 | 284,912 | 3.14 ms | 5 | 569,824 | 3.43 ms |
| 2 | 3 | 284,912 | 3.25 ms | 6 | 569,824 | 3.16 ms |

The gated arm decoded exactly two batches in every pair; the ungated arm
decoded four. `drain` is the wall time to drain the root stream, taken before
the settle so it covers only what a caller waits for. The two arms are
indistinguishable on it and the card's acceptance is not argued from it: at
three thousand rows a file a partition's whole read is one batch, so the drain
is dominated by planning. It is recorded because both arms' latency was asked
for. Decoded bytes are the metric the assertion uses, not files read: a
file counts as read the moment its footer lands, so `files_read` measures how
many partitions won a scheduling race with the root stream closing, and it
varied 10/5/6 in the run tabulated above and 7/4/5 in another on this box.
`bytes_scanned` is the vendored reader's fetched-byte counter over a `file://`
store and is reported for the pair comparison only — it is not S3 traffic.

Both arms return 20 rows and every row satisfies the predicate.

With #6721's paced credit, the same seven-pair test measured a gated median of
138,364 decoded bytes (one row group) against 553,456 ungated (four row groups).
Before the change, the matched run measured 276,728 against 415,092. Drain wall
times overlapped in both runs because this small fixture is planning-bound; the
assertion grades decoded work and exact results, not local latency.

The seven fixtures in this test binary run serially. Six own four-worker Tokio
runtimes; before serialization they created 24 runtime workers plus the
mechanism test on a four-core hosted runner. A process-wide fixture lock removed
that in-binary contention. The full workspace gate can still schedule other test
binaries beside this one. The old per-batch ramp could widen to full fan-out
under that contention. The eight-batch pace bounds this fixture at one decoded
group plus, at most, the successor admitted by the first partition's terminal
credit. The matched A/B is therefore a required test:

```shell
cargo test -p siglake-storage --test clipped_limit_admission \
  the_admission_ramp_stops_every_partition_decoding_for_a_clipped_limit \
  -- --nocapture
```

The regular test gate also retains the exact query-result controls below and
the scheduler-free mechanism assertion.

The mechanism is asserted separately and without a scheduler in
`a_partition_behind_the_ramp_reads_nothing`: the partition streams are executed
and polled by hand, the partition at the back of the ramp stays `Pending` across
repeated polls with nothing able to widen it, and after the plan is dropped and
settled the scan node reports `files_read`, `object_store_reads` and
`bytes_scanned` all zero. A partition held by the ramp has not started its read.

Controls, all against the gated arm: a match present only in the last file still
fills the limit; seven matches spread across three late files come back exactly,
compared as a set; `LIMIT 5 OFFSET 10` returns a full page of distinct
qualifying rows; an unfiltered `ORDER BY timestamp LIMIT n` and a `count(*)`
with the hint set both complete and answer exactly.

## Not measured here

The card's acceptance is a fully compacted 50G AWS round with no small-file tail
and `keyword` back under the 10 ms standing ceiling. Nothing local speaks to
that: this fixture's files are three thousand rows, so a partition's whole read
is one batch, and the latency the AWS capture attributes to the fan-out cannot
be reproduced at this size. The ramp factor default of 2 is chosen from the
local decode comparison above, not from a round.

## Knob

`SIGLAKE_SCAN_CLIPPED_ADMISSION_WAVE` — ramp factor, default 2. `0` turns the
ramp off, which is the pre-#4865 scan and the negative-control arm of the A/B.
Anything else is clamped to at least 2, since a factor of 1 would never widen
the ramp. Per-session override `ClippedAdmissionWave` exists so a measurement
can run both arms in one process without `set_var`. Packaged defaults, the chart
and the operator are unchanged: the knob has no chart value and no CRD field.
