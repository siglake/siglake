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
reach the admitted width — one credit per batch the scan emits, one per
partition that ends.

* A limit answered out of the first batch leaves the ramp at `wave`: one
  partition decoded, one admitted behind it.
* A sparse term, or a term with too few matches, reaches full fan-out after
  `partitions / wave` credits — eight batches on the 15-file layout, against the
  430 batches one of its files holds.

Charging the ramp the admitted width rather than one credit per batch is what
keeps the first wave narrow. Widening on every batch was measured on the local
fixture below and reached four files where the width rule reaches two, because
the ramp outruns the root stream closing.

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

## Local measurement

`the_admission_ramp_stops_every_partition_decoding_for_a_clipped_limit`: 16
files × 3,000 rows, 64 matches in every file, `SELECT raw FROM events WHERE raw
LIKE '%queen%' LIMIT 20`, 16 partitions. One layout; the arms differ only in the
`ClippedAdmissionWave` session extension (`0` is the pre-#4865 scan). Three
interleaved pairs, counters read after `settle_scan_partitions` so the cancelled
partitions' folds are included.

| pair | gated files_read | gated decoded_bytes | ungated files_read | ungated decoded_bytes |
| --- | --- | --- | --- | --- |
| 0 | 4 | 284,912 | 8 | 569,824 |
| 1 | 4 | 284,912 | 5 | 569,824 |
| 2 | 4 | 284,912 | 5 | 569,824 |

The gated arm decoded exactly two batches in every pair; the ungated arm
decoded four. Decoded bytes are the metric the assertion uses, not files read: a
file counts as read the moment its footer lands, so `files_read` measures how
many partitions won a scheduling race with the root stream closing, and it
varied 7/4/5 across three otherwise identical ungated pairs on this box.
`bytes_scanned` is the vendored reader's fetched-byte counter over a `file://`
store and is reported for the pair comparison only — it is not S3 traffic.

Both arms return 20 rows and every row satisfies the predicate.

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
