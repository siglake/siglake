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
ask for? The only evidence was net heap growth over one delete fixture of
262,144 survivors (`delete_task_size_gate.rs:991`), which is three orders of
magnitude below a cold-target file and was measured in a debug build.

## Where the target binds

Row groups on merged output are sized in rows, from bytes: the writer is built
on the merge's first output batch and takes `target_row_group_bytes` divided by
that batch's sampled row size, clamped to `MIN_ROW_GROUP_ROWS` (128 Ki) ..
`MAX_ROW_GROUP_ROWS` (4 Mi) — `row_group_rows_for_avg`,
`crates/siglake-storage/src/iceberg.rs:1920`.

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
  retained arenas.
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

*peak heap* is net live-heap growth across the merge, the same measure
`delete_task_size_gate.rs` reports. *peak RSS* is the process high-water mark
sampled every 10 ms during the merge, over four readings per arm; the merge
starts from a 349-413 MB baseline the corpus build leaves behind, so the 64 and
32 MiB arms' merges fit inside arenas the allocator already held and their RSS
delta reads as < 1 MB. Compare the absolute peaks, not the deltas. *needle read*
is the settled `bytes_data` and wall of `sum(length(raw)) WHERE host =
'host-needle'`, a host confined to one 4-second window of the ordered output.

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
* **Query: better where pruning has room, and one shape got worse.** The needle
  and narrow-range shapes read one row group at every target, so a smaller row
  group is strictly less data: 0.80 -> 0.28 MB from 256 to 128 MiB. At 32 MiB
  the same shape read 1.13 MB, more than the default — reproducibly, three times
  — which is a page-level effect inside the group the reader selects and not
  something this fixture explains (#5133). The full predicate scan reads every
  row group by construction and cost 18.44 -> 18.82 MB (+2%) with wall times
  inside the noise of a loaded box.
* **If the native blooms ever come back, the target's cost changes class.**
  Parquet-native blooms are default-off (`native_blooms_enabled`, measured
  useless on this layout). Priced back on: 0.57 MB at 4 row groups, 2.00 MB at
  14 — ~146 KB per row group per file, so 0.45% of the file at 256 MiB against
  1.57% at 64 MiB. A decision to re-enable them and a decision to lower the
  target are not independent.

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

The follow-ups this left open: #5132 is the chart change if a round supports it,
and #5133 is the 32 MiB arm's backwards selective read.

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
```
