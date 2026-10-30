# Footer tier against the cold wide fold it declines (#7790)

Measured 2026-10-30 at `c705a32` plus this card's probe changes.
Recommendation: **leave `predicate_count_tier1` as #7787 left it. No gate.**

## What was open

#7787 gave the positive `=` / `IN` count path `ColdWideFold::Forbidden`: on the
first touch of a table generation it declines to fold a wide group-count
aggregate and continues to the per-file footer sum. Both the design note
(`docs/DESIGN_incremental_group_count_aggregate.md`, "What is bounded, and what
is not") and `docs/LIMITATIONS.md` recorded the resulting cost comparison as
unmeasured, with the explicit caveat that on a table with many live files the
declined fold may have been the cheaper of the two.

Two things needed numbers:

1. how footer-tier cost and fold cost each scale, and where they cross;
2. whether any signal available **before** the listing could drive a gate.

## How it was measured

In-crate report tests, `#[ignore]`, in
`crates/siglake-storage/src/iceberg.rs`, module
`predicate_count_tier_cost_measurement`:

```text
cargo test --release -p siglake-storage --lib \
  predicate_count_tier_cost_measurement -- --ignored --nocapture --test-threads=1
```

`--test-threads=1` is required: the footer-byte counter is process-global.

Each row is a **first touch** of a generation — a freshly opened
`IcebergContext`, so the entry's wide memo, the per-file footer cache and the
snapshot-keyed aggregate cache are all cold, exactly as they are for the first
request after a commit. The table entry is warmed before the clock starts, so
neither arm is charged for catalog metadata the other would also pay. The two
arms are `predicate_count_tier1` (`bounded`) and
`grouped_counts_with_summary` (`generic`), run against the same warehouse in
separate contexts. Every row asserts both arms return the same exact counts.

Fixture: one column, `host`, with 5,120 distinct single-row values plus one
value carrying 5,120 rows, spread evenly over `files` commits. Total rows and
distinct values are held constant so only the file count moves. `deltas` is
extra delta names planted in the incarnation prefix beyond the one each commit
writes — the shape a compactor that has absorbed nothing leaves behind.

Instruments. Fold side: `delta_io_probe`, extended here with a byte counter,
because the fold reads through the aggregate `opendal::Operator` and never
reaches `iceberg::arrow::object_store_bytes_read()`. Footer side: a new
`footer_io_probe` on `RawPageAsyncReader::get_bytes`, the one choke point for
the per-file footer metadata reads and the raw-page fallback — those also go
through `FileIO` directly and were invisible to the existing byte counter
(verified: it read zero for every footer-tier row).

Local filesystem, release build. `d_gets` is delta object GETs, `f_reads` is
reads through `RawPageAsyncReader`, `agg_bytes` and `footer_B` are payload
bytes on each side.

## The wide aggregate covers the column

The generic arm's fold pays off here: it answers from `tier1_wide` and performs
no footer read at all. This is the shape where declining the fold costs
something.

```text
 files   deltas     tier        source  elapsed_ms  lists  d_gets  bases   agg_bytes  f_reads    footer_B
     8        0  bounded  materialized        6.04      0       0      0           0       16      213832
     8        0  generic    tier1_wide        3.28      1       8      1        3475        0           0
     8     1024  bounded  materialized        5.54      0       0      0           0       16      213832
     8     1024  generic    tier1_wide       30.23      1    1032      1       30105        0           0
     8     8192  bounded  materialized        6.15      0       0      0           0       16      213832
     8     8192  generic    tier1_wide      179.88      1    8200      1      216471        0           0
    32        0  bounded  materialized       17.68      0       0      0           0       64      268260
    32        0  generic    tier1_wide        5.52      1      32      1       13421        0           0
    32     1024  bounded  materialized       17.11      0       0      0           0       64      268260
    32     1024  generic    tier1_wide       29.55      1    1056      1       40045        0           0
   128        0  bounded  materialized       56.92      0       0      0           0      256      544352
   128        0  generic    tier1_wide        7.48      1     128      1       47247        0           0
   128     1024  bounded  materialized       60.58      0       0      0           0      256      544352
   128     1024  generic    tier1_wide       30.30      1    1152      1       73865        0           0
   256        0  bounded  materialized      106.62      0       0      0           0      512      946816
   256        0  generic    tier1_wide       10.28      1     256      1       92338        0           0
```

## The wide aggregate does not cover the column

The aged `query_audit` shape. `host` is demoted to a sketch, so the generic arm
folds the whole prefix, finds nothing usable, and pays the footer tier anyway:
its footer columns are identical to the bounded arm's, and the fold is pure
loss.

```text
 files   deltas     tier        source  elapsed_ms  lists  d_gets  bases   agg_bytes  f_reads    footer_B
     8        0  bounded  materialized        5.70      0       0      0           0       16      213832
     8        0  generic  materialized        7.79      1       8      1        3481       16      213832
     8     1024  bounded  materialized        5.48      0       0      0           0       16      213832
     8     1024  generic  materialized       32.15      1    1032      1       30105       16      213832
     8     8192  bounded  materialized        5.21      0       0      0           0       16      213832
     8     8192  generic  materialized      196.22      1    8200      1      216473       16      213832
    32     1024  bounded  materialized       15.28      0       0      0           0       64      268260
    32     1024  generic  materialized       43.05      1    1056      1       40036       64      268260
   128     1024  bounded  materialized       57.13      0       0      0           0      256      544355
   128     1024  generic  materialized       85.90      1    1152      1       73862      256      544355
```

## After a DELETE shortens a complete aggregate

A real delete commit on an index table, with the aggregate folded to completion
first so the delete is what shortens it. Same structure as the sketched case —
the generic arm folds, rejects the aggregate on the row-count guard, and pays
the footer tier — with the delete file pushing each live file to four reads
instead of two.

```text
 files   deltas     tier        source  elapsed_ms  f_reads    footer_B  d_gets   agg_bytes
     8        0  bounded  materialized       24.14       32       21188       0           0
     8        0  generic  materialized       33.42       32       21188       8        3266
     8     1024  bounded  materialized       20.00       32       21188       0           0
     8     1024  generic  materialized       61.05       32       21188    1032       29885
    32     1024  bounded  materialized       97.93      128       84774       0           0
    32     1024  generic  materialized      172.11      128       84774    1056       39935
```

## Reading

**Selectivity does not enter.** Both tiers compute the whole group-count map
and the predicate picks one key out of it. The selective key (1 row) and the
broad key (5,120 rows) come out of the same lookup at the same cost, in every
row above. There is no selective-versus-broad cost difference at this layer to
gate on, and `estimated_rows_processed` could not help even if it were
available for the table: the quantity it would estimate does not affect either
tier.

**The two costs scale in different dimensions, as predicted.** Footer tier: two
reads per live file, flat across 0 / 1,024 / 8,192 deltas (16 / 64 / 256 / 512
reads at 8 / 32 / 128 / 256 files). Fold: one LIST plus one GET per outstanding
delta plus the base, flat in file count (3.28 ms at 8 deltas, 179.88 ms at
8,200).

**The regression is real and bounded.** Worst measured case is 256 live files
with the compactor caught up: 106.62 ms against 10.28 ms, a factor of 10.4, or
about 96 ms once per generation per column. Against it, the case #7787 fixed:
8 files with 8,192 outstanding deltas, 5.21 ms against 196.22 ms, a factor of
38 — and production reached 43,042 outstanding names and 17.7 s / 21.97 s
against a 30 s deadline.

**Local filesystem overstates the fold's advantage.** Object counts are what
survive the move to S3: the footer tier is `2 × live_files` reads, the fold is
`2 + outstanding_deltas`. At 256 files with no backlog that is 512 against 258,
a factor of 2, not 10 — the local gap is mostly footer decode CPU, which is
paid either way. The crossover by object count is `outstanding_deltas ≈
2 × live_files`.

**Both results land in the snapshot-keyed cache**, and the per-file footer
cache makes a second same-process request on the same generation free. Every
figure above is a once-per-generation-per-column first touch, not a per-query
cost.

## Why no gate

The crossover needs both terms. Live file count is free — it is in the plan the
query already has. Outstanding delta count is knowable only by performing the
listing the gate is deciding about, which is the finding #7787 already
established structurally (a pinned generation's sequence number does not bound
the shared prefix; rebuild and short-repair markers are separate objects that
track no commit count).

A gate on live file count alone picks the wrong side exactly when it matters. A
table with 256 live files and 43,042 outstanding deltas is the compactor-behind
case, and a `fold if live_files > N` rule would fold it and reproduce the
incident. The quantity that is cheap to read is not the one that bounds the
risk.

So the choice is between a bounded regression and an unbounded one. The footer
tier's worst case is two reads per live file of the pinned generation — the
same file list the query would otherwise scan, so it is strictly below the scan
it replaces. The fold's worst case is bounded by nothing observable before
paying it. At 96 ms once per generation per column for the worst shape
measured, the bounded side is the right one to keep.

## If it ever does matter

One signal would close the gap without reopening the listing: a **bounded paged
listing**.
`opendal::Operator::lister_with` (0.57.0, already used in
`crates/siglake-wal/src/mirror.rs:2116` and
`crates/siglake-compactor/src/lib.rs:7735`) returns a stream, so reading at
most `2 × live_files + 1` entries and stopping answers "is the fold's input
small enough to be worth it?" in a bounded number of requests. That preserves
#7787's bound — the work is capped by a quantity known in advance — while
giving the tight answer a sequence-number heuristic could not.

It is not worth building yet. Revisit if a production table shows all three of:
more than ~256 live files, a wide aggregate that covers the equality column,
and first-touch counts visible in `query_audit`. Filed as a follow-up.

## Not measured

- Object storage. Every figure is local filesystem; the S3 reading above is
  arithmetic on object counts, not a measurement.
- The planner's filtered scan. It is no longer the alternative on this path
  (`None` means no aggregation path at all), so it was not timed. Exactness is
  pinned by the bounded arm agreeing with the generic arm on every row here,
  and by `crates/siglake-storage/tests/bounded_predicate_count_semantics.rs`.
- Concurrency. One request at a time; nothing about what either tier does to a
  loaded query server.
- Raw-page fallback at scale. The delete arm exercises it at 8 and 32 files
  only.
