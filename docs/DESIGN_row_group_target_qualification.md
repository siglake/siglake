# Design — a smaller compactor row-group target (#4772 qualification)

Status (2026-09-17): **local qualification, no default changed.**
`TARGET_ROW_GROUP_UNCOMPRESSED_BYTES` is still 256 MiB, the packaged compactor
still has `memory: 1Gi` (`deploy/helm/siglake/values.yaml:325`), and nothing in
the chart, the operator or the CLI moved. What this document adds is a measured
candidate — **64 MiB, compactor-only, as a chart-level env default** — and the
matched-round evidence a 0.2.0 change would have to produce first.

#4754 made `SIGLAKE_PARQUET_TARGET_ROW_GROUP_BYTES` /
`IcebergTuning::target_row_group_bytes` reach the merge, re-cluster and
delete-rewrite writers and deliberately changed no default. The question it set
aside: at the packaged 1Gi limit, is 256 MiB the right thing for a compactor to
ask for? The only evidence was a reset-counter estimate of net heap growth over
one delete fixture of 262,144 survivors (`delete_task_size_gate.rs`), which is
three orders of magnitude below a cold-target file and was measured in a debug
build. That delete measurement was corrected on 2026-09-22; the separate merge
measurements below were not rerun.

## Where the target binds

Row groups on merged output are sized in rows, from bytes: the writer is built
on the merge's first output batch and takes `target_row_group_bytes` divided by
that batch's sampled row size, clamped to `MIN_ROW_GROUP_ROWS` (128 Ki) ..
`MAX_ROW_GROUP_ROWS` (4 Mi) — `row_group_rows_for_avg` in
`crates/siglake-storage/src/iceberg.rs`.

The memory it bounds is the writer's carry buffer. With a row-group bloom
column set — every merge writes one, `with_raw_rowgroup_bloom_column` — the fork
takes row-group formation away from the inner Parquet writer so each token bloom
covers exactly one row group, and holds the not-yet-sealed rows as decoded Arrow
batches in `pending`
(`third_party/iceberg/src/writer/file_writer/parquet_writer.rs:921`). So one
open row group of decoded rows is resident for the whole of its formation, and
the target is what sizes it.

Two consequences the measurement below is shaped around:

* **The target is priced in extent and paid in buffers.** The sizing divides by
  `sampled_row_bytes` — `ArrayData::get_slice_memory_size`, the extent the rows
  span. What the carry buffer holds is whole buffers. On this corpus that is 468
  B/row against 874 B/row, a factor of 1.87, so a "256 MiB" row group holds
  about 478 MiB of Arrow allocation. Neither number is resident memory, which
  also carries the input chunk prefetch, the encoder and the allocator's
  retained arenas. The ingest flush path divides the same target by
  `get_array_memory_size` instead, which is accurate on a batch it assembled
  itself; that split is kept (#4774, `docs/LIMITATIONS.md`), so a target read
  on the flush side asks for a different row count than the numbers here.
* **Below the floor the target does nothing.** 128 Ki rows is the floor
  whatever the target says. At this corpus's row size that floor is ~109 MiB of
  buffered allocation; at the ~1.2 KB/row of the delete fixtures it is ~157 MiB
  (`docs/LIMITATIONS.md`, the streamed-delete entry). A target below
  `floor x row_bytes` buys nothing at all, which is why the sweep stops where it
  does.

## The measurement

`crates/siglake-storage/tests/row_group_target_qualification.rs`, release build,
`file://` warehouse, on this box. One arm per process, because peak RSS is
process-wide and monotonic. Fixture: 8 input files x 250,000 corpus-shaped rows
(2 M rows, 125.2 MB compressed), disjoint hour spans, appended through the
DEFAULT tuning so every arm merges byte-identical inputs and only the
compactor's writer changes. One bin, slice-streaming merge — the compactor's
path for any bin at or under the fan-in cap.

Three repeats per arm. The geometry, byte and heap columns were identical to the
last printed digit on all three; only wall times moved, and the box carried
other lanes (load 2.8-8.2), so wall time is reported as a range and is the one
column not to read closely.

| target | row groups | rows/rg (max) | extent/rg | peak heap | peak RSS | file bytes | rg-bloom footer | needle read | full scan |
|---|---|---|---|---|---|---|---|---|---|
| **256 MiB** (default) | 4 | 574,808 | 223.1 MB | 543.0 MB | 625-659 MB | 125.0 MB | 12.8 KB | 0.80 MB / 5.1-6.0 ms | 165-174 ms |
| 128 MiB | 7 | 287,404 | 127.5 MB | 369.5 MB | 433-485 MB | 125.1 MB | 22.4 KB | 0.28 MB / 3.5-3.6 ms | 159-176 ms |
| **64 MiB** | 14 | 143,702 | 63.7 MB | 295.3 MB | 412-413 MB | 125.4 MB | 44.8 KB | 0.28 MB / 3.4-4.2 ms | 160-186 ms |
| 32 MiB (floor binds) | 16 | 131,072 | 55.8 MB | 284.6 MB | 403-414 MB | 125.5 MB | 51.1 KB | 1.13 MB / 5.3-6.3 ms | 161-196 ms |

*peak heap* is what the row-group qualification binary's historical reset
counter labeled net live-heap growth across the merge. Pre-window frees made it
a lower bound on window growth, not live heap or an exact delta above a
baseline. The 2026-09-22 correction to `delete_task_size_gate.rs` did not rerun
or reinterpret these separate merge measurements; this table retains their
original method and values. *peak RSS* is the process high-water mark sampled
every 10 ms during the merge, over four readings per arm; the merge starts from
a 349-413 MB baseline the corpus build leaves behind, so the 64 and
32 MiB arms' merges fit inside arenas the allocator already held and their RSS
delta reads as < 1 MB. Compare the absolute peaks, not the deltas. *needle read*
is the settled `bytes_data` and wall of `sum(length(raw)) WHERE host =
'host-needle'`, a host confined to one 4-second window of the ordered output.
`bytes_data` is fetched bytes at the reader's 1 MiB coalesce default, so this
column is not a row-group-geometry result on its own; see the #5133 section
below for where the needle arms' bytes actually go.

### What the target buys

Peak heap falls 543.0 -> 369.5 -> 295.3 -> 284.6 MB, and it tracks the carry
buffer: rows-per-row-group times 874 B of allocation, plus a fixed ~120-170 MB
of prefetch, encoder and runtime. Peak RSS falls with it, 625-659 MB down to
412-413 MB. Against a 1Gi limit, 650 MB of resident for ONE bin of 125 MB
compressed is the finding: the packaged default has about a third of its limit
of headroom while merging a bin far smaller than a production leading edge, and
the chart's own budget of ~3 GB per concurrent bin for 200G-class bins
(`deploy/helm/siglake/values.yaml:335`) is the same observation from the fleet
side.

Below 64 MiB the curve flattens: the floor takes over (the 32 MiB arm's row
groups are 131,072 rows, the clamp exactly) and 10.7 MB more heap is all that is
left to win.

### What it costs

* **File bytes: +0.4% at 64 MiB** (125.0 -> 125.4 MB), +0.4% at 32 MiB.
  Compression is flat at 5.35-5.36x encoded/compressed: smaller row groups did
  not cost dictionary efficiency on this corpus.
* **Footers: negligible as shipped.** The per-row-group raw trigram bloom is
  3.20 KB per row group, so 12.8 KB -> 44.8 KB across the sweep — 0.01% to
  0.03% of the file. Thrift footer 0.03 -> 0.07 MB. Time-bucket and group-count
  footers are per file, not per row group, and did not move.
* **Merge throughput: unmoved.** 321-350 K rows/s across every arm and both
  passes, with no ordering by target. Turning the tracking allocator off changed
  nothing, so the column is not an artifact of measuring.
* **Query: better where pruning has room; the needle column measures the
  reader.** The needle and narrow-range shapes read one row
  group at every target, so a smaller row group has less to offer them: 0.80 ->
  0.28 MB from 256 to 128 MiB. At 32 MiB the same shape read 1.13 MB, more than
  the default — reproducibly, three times. #5133 settled that ordering against
  the page accounting below: the bytes follow the page the needle lands on and
  the reader's 1 MiB range coalescer, and the row-group size only decides which
  page that is. The full predicate scan reads every row group by construction
  and cost 18.44 -> 18.82 MB (+2%) with wall times inside the noise of a loaded
  box.
* **If the native blooms ever come back, the target's cost changes class.**
  Parquet-native blooms are default-off (`native_blooms_enabled`, measured
  useless on this layout). Priced back on: 0.57 MB at 4 row groups, 2.00 MB at
  14 — ~146 KB per row group per file, so 0.45% of the file at 256 MiB against
  1.57% at 64 MiB. A decision to re-enable them and a decision to lower the
  target are not independent.

### Where the needle shape's bytes go (#5133)

`bytes_data` is the length of each MERGED fetch, not of what was asked for. The
fork's `get_byte_ranges` runs the requested ranges through `merge_ranges` and
charges each physical fetch
(`third_party/iceberg/src/arrow/reader/file_reader.rs:229`, :247). siglake sets
no range knobs, so `effective_reader_tuning` returns `None` for both and the
fork's own `DEFAULT_RANGE_COALESCE_BYTES` of 1 MiB applies
(`third_party/iceberg/src/arrow/reader/mod.rs:34`): two requested ranges less
than 1 MiB apart become one fetch, and every byte between them is read and
charged. At the time of the measurement the scan logged `range_enabled=false`,
which said siglake configured nothing, not that the reader stopped coalescing.
#5806 renamed that field to `range_override_applied` and made
`range_coalesce_bytes` / `range_fetch_concurrency` report the values the reader
will apply, so the same scan now logs the 1 MiB it really coalesces at.

`sum(length(raw)) WHERE host = 'host-needle'` takes two fetches on the row group
it selects — the predicate column under the page-index selection, widened to
batch boundaries because parquet caches predicate columns, then `raw` under the
selection the predicate produced. Each goes through the coalescer on its own.
The batch is parquet's own `DEFAULT_BATCH_SIZE` of 1,024, not DataFusion's
8,192: the fork calls `with_batch_size` only when siglake configured one
(`third_party/iceberg/src/arrow/reader/pipeline.rs`), and nothing here sets
`QueryScanTuning::batch_size`. On this layout the distinction does not move a
byte, but not because the expansion is a no-op: pages here run ~20,000 rows and
are not batch-aligned, so both 1,024 and 8,192 widen the request past both edges
of the selected page and both land on exactly one neighbouring page either side.
A geometry with smaller pages would separate them, which is why the model has to
name the right one. From the merged output's offset index, with the fetches
traced:

| arm | rows/rg | needle lands on | `host` asked / fetched | `raw` asked / fetched | `bytes_data` |
|---|---|---|---|---|---|
| 256 MiB | 574,808 | rg 1 row 300,192, `raw` page 15 of 30 | 89,442 / 456,579 | 384,473 / 384,473 | 841,052 |
| 128 MiB | 287,404 | rg 3 row 12,788, `raw` page 0 of 15 | 61,815 / 61,815 | 236,089 / 236,089 | 297,904 |
| 64 MiB | 143,702 | rg 6 row 12,788, `raw` page 0 of 8 | 61,815 / 61,815 | 236,089 / 236,089 | 297,904 |
| 32 MiB | 131,072 | rg 6 row 88,568, `raw` page 5 of 8 | 89,436 / 174,727 | 389,046 / 1,006,851 | 1,181,578 |

The fetched columns sum to the measured `bytes_data` to the byte in all four
arms, and in all four again under the control below — eight agreements, which is
what makes this an accounting and not a story. `audit_needle_pages` in the
fixture reconstructs the two fetches from the offset index and prints the
prediction next to the measurement. Two mechanisms produce the spread, larger
first:

* **The coalescer charges the gap.** A column chunk's dictionary page sits at
  the chunk start and the reader always asks for it, so a needle on page *k*
  leaves pages 0..k-1 between the dictionary request and the page request.
  Under 1 MiB of gap the two merge into one fetch. At 32 MiB the gaps are `raw`
  pages 0..4 (617,805 B) and `host` pages 0..2 (85,291 B): 703,096 B, 59% of
  everything the arm was charged. At 256 MiB the `raw` gap is 2.54 MB and stays
  split, while the `host` gap — pages 0..12, 367,137 B, 44% of the arm — merges.
  At 128 and 64 MiB the needle is on page 0 of both chunks and there is no gap
  at all. The `host` gap stops one page short of the needle's own page because
  the batch expansion above widens the predicate request past both edges of the
  page the index picked: page boundaries are not batch-aligned, so the needle's
  `host` page 4 (page 14 at 256 MiB) is requested as pages 3..5 (13..15), four
  ranges counting the dictionary. The control shows it directly — those three
  adjacent page ranges merge to one fetch and the dictionary stays its own,
  `4 ranges -> 2`.
* **`raw` page 0 is a fifth the size of a PLAIN page.** The corpus's `raw`
  values are near-unique, so the dictionary reaches the writer's 1 MiB
  uncompressed limit inside the first page and the column falls back to PLAIN:
  page 0 is 38.4 KB of dictionary indices, page 1 is a 1.8-1.9 KB remnant flushed
  at the fallback, and pages 2 and up are ~187 KB. The 128 and 64 MiB arms read
  page 0; the other two read a PLAIN page.

Which of these applies is decided by the needle's offset inside its row group,
and that offset is arithmetic rather than a property of the target. The needle
sits at merged row 875,000 in every arm, so the offset is `875,000 mod
rows_per_group`, and 287,404 is exactly 2 x 143,702 — which is why the 128 and
64 MiB arms land on the same offset (12,788) and agree to the byte. Reading the
sweep as "131,072-row groups cost 4x 143,702-row groups" reads a fixture
coincidence as a law about geometry.

`RG_COALESCE_BYTES=1` is the control: it is the smallest value that survives the
`.max(1)` in `query_provider.rs`, and it leaves only ranges that are adjacent or
one byte apart merged — immaterial for page ranges — so
`bytes_data` reports what the reader requested. With it, the arms read 473,915 /
297,904 / 297,904 / 478,482 B. The 32 MiB arm reads 1.6x the 64 MiB arm instead
of 4x, and lands within 1% of the 256 MiB default instead of 40% above it. What
remains is the PLAIN page, which any arm pays whenever its needle misses page 0.

The control also prices what the coalescer buys. The scan's `reads` counter goes
5 -> 6 on the 256 MiB arm and 4 -> 6 on the 32 MiB arm when coalescing is turned
off, and does not move on the two arms whose needle is on page 0 and which had
no gap to merge. So the two arms that were charged the gap are exactly the two
that saved a request for it: one and two fetches respectively, against 367 KB
and 703 KB.

Neither `merge_ranges` nor the page selection is wrong, so nothing is fixed here
and no default moves. On object storage the coalescer is trading those bytes for
request count, which is what it exists to do; this is a `file://` warehouse,
where the trade has no upside. What the sweep table cannot show is the split
itself — `bytes_data` reports fetched bytes and a reader of the column has no
way to see how much of it was gap. That is #5805.

## The candidate: 64 MiB, compactor-only, in the chart

64 MiB is where the memory curve has given up most of what it has (543 -> 295 MB
of heap, 650 -> 412 MB of resident) while the target still governs the geometry
rather than the clamp: 143,702 rows per row group on this corpus, 9% above the
floor. 32 MiB is the floor in disguise, and it is the one arm whose query
numbers got worse.

It should ship as a compactor-scoped chart default —
`compactor.extraEnv: SIGLAKE_PARQUET_TARGET_ROW_GROUP_BYTES=67108864` — not as a
change to `TARGET_ROW_GROUP_UNCOMPRESSED_BYTES`. The constant is shared with the
ingest flush path, and nothing here measured the flush side. The env route also
matches what the AWS round configs already do:
`deploy/aws/config/values.query-perf.yaml:42` sets 64 MiB and
`deploy/aws/config/values.smoke.yaml:41` sets 32 MiB, both from before #4754,
when the merge writer ignored the value entirely and took a flat 1,048,576 rows.

### Row width decides whether the candidate does anything

The target buys memory only above the floor, and the floor is in ROWS. At 468
B/row of extent, 64 MiB asks for 143,702 rows. At the ~1.2 KB/row the delete
fixtures carry, 64 MiB asks for 55,924 and gets 131,072 — the floor — and the
open row group holds ~157 MiB whatever the target says. So on a wide-row corpus
the candidate is a no-op and the floor is the thing to argue about. Any round
that qualifies this has to report the row size it measured at, or the number
means nothing.

## What a matched round would have to show

Bounded, and on an already-planned normal round rather than one raised for this:

1. **Two arms, one corpus.** Same generated corpus, same ingest, same round
   size, compactor `extraEnv` at 256 MiB and 64 MiB. Everything else identical,
   including `binConcurrency: 1` and the 1Gi limit.
2. **Geometry, first.** Merged-output row-group row counts and the sampled row
   size behind them, per arm. This is #4773's collection; without it the arms
   may differ only in a number nobody applied, which is exactly the state #4754
   found (the round configs set the env and the merge writer never read it).
3. **Compactor RSS, as the container sees it.** `container_memory_working_set_bytes`
   for the compactor pod across the pass, peak and p95, plus any OOMKill. The
   local heap column does not transfer; the local RSS reading is one process on
   a `file://` warehouse, without an S3 client, a WAL drain or index work.
4. **Write cost.** Merged bytes out per bin and merge rows/s per arm, and the
   per-file footer share, so the +0.4% local reading is checked at fleet row
   sizes.
5. **Query cost, matched.** The round's own suite, both arms, compared per shape
   — including at least one shape that reads every row group and one selective
   shape — through `/api/v1/sql` distributed, plus a `GROUP BY` cross-shard
   merge check whose per-key counts sum to the full row count.

A pass needs the RSS reduction to survive at fleet row sizes with no shape
regressing beyond the round's own noise band. Anything less leaves 256 MiB in
place, which costs nothing: the knob is already there for an operator who needs
it.

## What this does not claim

One corpus, one row width, one bin, one merge path, one `file://` warehouse, one
box. Nothing here exercised S3, concurrent bins, delete rewrites (the delete
arm's own numbers are in `delete_task_size_gate.rs`) or the ingest flush path.
The local RSS numbers say what a merge adds to one process on this machine; they
are not what a packaged compactor's container reports, and nothing here
establishes that 256 MiB is unsafe at 1Gi — only that it is measurably closer to
the limit than it needs to be, on a bin far smaller than the ones the fleet
merges.

The follow-ups this left open: #5132 is the chart change if a round supports it.
#5133 settled the 32 MiB arm's selective read against the page accounting above
and left #5805 (requested versus fetched bytes in scan attribution) and #5806
(the misleading `range_enabled` log field, since renamed) behind it.

## Reproduce

```sh
# the sweep in the table above (one arm per process, three repeats)
for rep in 1 2 3; do
  for mb in 0 128 64 32; do
    RG_TARGET_MB=$mb cargo test --release -p siglake-storage \
      --test row_group_target_qualification -- --ignored --nocapture
  done
done

# wall time and RSS without the tracking allocator on the merge's hot path
RG_HEAP_TRACKING=0 RG_TARGET_MB=64 cargo test --release -p siglake-storage \
  --test row_group_target_qualification -- --ignored --nocapture

# the native-bloom arms
RG_NATIVE_BLOOMS=1 RG_TARGET_MB=64 cargo test --release -p siglake-storage \
  --test row_group_target_qualification -- --ignored --nocapture

# #5133's control: `bytes_data` then reports requested bytes, not coalesced ones
for mb in 0 128 64 32; do
  RG_COALESCE_BYTES=1 RG_TARGET_MB=$mb cargo test --release -p siglake-storage \
    --test row_group_target_qualification -- --ignored --nocapture
done
```

Every arm prints the needle row group's page layout and the two fetches
`audit_needle_pages` reconstructs, ending in a `predict` line whose `fetched`
total must equal the `needle host` row's `bytes_data`.
