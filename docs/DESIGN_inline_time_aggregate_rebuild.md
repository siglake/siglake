# Rebuilding a pre-coverage inline time aggregate

**Status:** implemented as `siglake rebuild-time-aggregates`
(`IcebergContext::rebuild_inline_time_aggregates`). Both open questions were
answered on 2026-09-16 and are recorded as decisions at the end. Task #3082.

## The condition

#2920 gave the inline side object (`siglake-aggregates.json`) a
snapshot-coverage chain. A reader admits the object only when
`aggregate_covers_current_snapshot` can walk from the table's current snapshot
back to the object's `coverage` edge through nothing but row-conserving
re-clusters. An object written before #2920 has no edge, so every consult is
refused and counted as
`siglake_query_side_aggs_cache_total{result="unproven_coverage"}`.

The refusal is correct. A row total alone cannot establish which snapshot an
aggregate describes: an unmarked N-for-N overwrite preserves `total-records`
while changing every answer. What the object loses is acceleration, not
correctness — the three consumers fall to the exact per-file tiers:

| consumer | with coverage | without |
| --- | --- | --- |
| `date_histogram_counts` | `time_buckets`, zero file reads | a footer read (or a scan) per live file |
| windowed `GROUP BY` | `time_group_counts`, zero file reads | a group-count footer per contained file, a window-restricted scan per boundary file |
| `windowed_count` | `time_buckets` | declines outright; the caller full-scans |

## It does not heal, and that is structural

`add_coverage_link` joins an append's edge `(parent, snapshot, seq)` onto the
chain's head. A legacy object has no head, so the first edge after the gap has
a parent that matches nothing and stays pending; every later edge chains onto
that pending run instead of joining it. The object goes on accumulating correct
counts it can never prove. A row-conserving re-cluster does not help either —
`aggregate_covers_current_snapshot` walks re-clusters back toward an edge, and
there is none to reach.

Asserted, not argued:
`crates/siglake-storage/tests/storage/pre_coverage_time_agg.rs`
(`later_appends_do_not_restore_coverage`,
`a_recluster_does_not_restore_coverage`). The same file's
`a_pre_coverage_object_is_refused_and_the_answers_stay_exact` pins the other
half: both tiers return the same rows.

This is why a repair has to be published deliberately. Nothing on the commit
path will ever do it.

## What the fallback costs, measured

`pre_coverage_time_agg::the_fallback_cost_report`, release, one shared
development box, 2026-09-16. Each row is the same table queried twice: once
with the side object as written, once with its `coverage` keys removed, arms
interleaved, one fresh `IcebergContext` per sample. Every sample asserts which
path served it, so the report cannot compare the per-file tier to itself.
Milliseconds.

| live files | shape | Tier-1 cold | Tier-1 warm | fallback cold | fallback warm |
| --- | --- | --- | --- | --- | --- |
| 49 | `date_histogram` | 2.06 | 0.08 | 7.87 | 2.10 |
| 49 | windowed `GROUP BY` | 2.02 | 0.04 | 7.61 | 2.00 |
| 73 | `date_histogram` | 1.89 | 0.07 | 20.80 | 1.84 |
| 73 | windowed `GROUP BY` | 1.83 | 0.04 | 19.94 | 1.71 |
| 168 | `date_histogram` | 2.04 | 0.08 | 72.58 | 1.80 |
| 168 | windowed `GROUP BY` | 1.98 | 0.03 | 69.35 | 1.51 |

Two readings, and they point in different directions.

**Cold, the fallback scales with live files and Tier-1 does not.** 7.9 → 72.6ms
across 49 → 168 files while Tier-1 holds at ~2ms: 3.8× → 35× over a 3.4× file
count. That is the footer read per file, and it is the shape that matches the
1TB board, where `count_by_level_last25` cost 1,065.9ms on an otherwise
42–102ms panel and `count_last25` cost 39.6s reading 507M rows.

**Warm, the fallback is flat at 1.5–2.1ms.** The per-file reads run
concurrently and the footer cache absorbs them, so a warm table's fallback
costs about 2ms regardless of file count — 23–59× Tier-1, but still 2ms. A
table whose footers stay warm does not urgently need this repair.

So the cost that justifies a rebuild is the cold one: a large table, a cold
footer cache, and a per-file tier whose price is set by the file count. That is
the state a restarted query pod is in, and the state a rarely-queried legacy
table is always in.

## What a rebuild can read, and what it cannot

The two components have completely different costs, and this is the fact that
shapes the whole design.

**`time_buckets` is a footer read per live file.** Every file carries its own
1-D time histogram in the Parquet footer
(`siglake_bloom::TIME_BUCKETS_KV_KEY`, read by `read_file_time_buckets`), with
the same validity guard the query path applies — absent footer, deletes, NULL
timestamps or a total that misses the file's row count all demote the file to a
timestamp decode. Summing those over the live files is one pass at the cost of
one cold fallback query.

**`time_group_counts` has no footer to read.** The 2-D map is built from the
in-memory batch at commit time (`file_time_group_counts`), never stamped into
the file. A file's group-count footer gives whole-file totals per value with no
time dimension, so it cannot be split across hourly buckets. The general case
is a two-column decode — `timestamp` and the grouped column — over every live
file.

The one case that avoids the decode: a file whose manifest `[min, max]`
timestamps lie inside a single aggregate bucket contributes its whole
group-count footer to that one bucket. Siglake writes time-clustered, so on a
compacted table this should cover a large fraction of files; on an
interleaved-arrival table it covers few. The rebuild should take the footer
where containment is provable and decode otherwise — the same classification
`grouped_counts_windowed` already makes, with the aggregate's bucket width in
place of the query window.

A rebuild therefore has two very different price tags, and an operator should
be told which one they are about to pay.

## Fencing

Three fences, all of which already exist and none of which are new mechanism.

**Incarnation (table UUID).** Every aggregate artifact is addressed under
`metadata/siglake-agg/<table-uuid>/` (#2919). The rebuild resolves its operator
through `aggregate_operator`, which returns `None` for a nil UUID; a table
whose ownership is unprovable has nothing safe to rebuild into and the command
refuses rather than writing to a path some earlier table at the same location
may own. This is what the wide rebuild already does.

**Snapshot.** The rebuilt maps are a pure function of the live files of one
snapshot `S`. The published `coverage` edge is exactly `S`'s `(snapshot_id,
sequence_number)` — never a later one, never a total-records match.

**CAS version.** The inline object is published under a conditional write where
the store has one (`OpendalSideCas`); the local filesystem and non-conforming
S3-compatible stores fall back to plain read-merge-write, where
single-writer-per-table is the correctness story. The rebuild inherits both
exactly as the commit path has them, and gains nothing the commit path does not
already assume.

## The publication protocol

```
1. op        = aggregate_operator(table)        -- else refuse: no incarnation
2. S         = table.current_snapshot()         -- else refuse: nothing committed
3. (obj, V)  = read the inline object with its CAS version
4. if obj.coverage already reaches S:           -- idempotent no-op
        report "already covered", exit 0
5. compute from the live files of S:
        time_buckets       (footer per file, decode on a footer miss)
        time_group_counts  (footer where one bucket contains the file,
                            two-column decode otherwise)
   a component whose total misses S's `total-records` is LEFT ABSENT,
   never published short
6. (obj', V') = re-read
   if V' != V, or obj'.coverage_links holds any edge above S:
        the table moved -> retry from 2 (bounded), else give up
7. write, conditional on V:
        time_buckets       := rebuilt      (replaced, not merged)
        time_group_counts  := rebuilt      (replaced, not merged)
        group_counts       := dropped      (open question 1)
        coverage           := S's edge
        coverage_links     := cleared
8. invalidate_cached_table(ident)               -- else readers keep the pin
```

**Why replacement and not a merge.** The delta path merges because each commit
contributes a disjoint set of rows, which is also why double-counting is its
central hazard. A rebuild carries the whole table at `S`, so merging it into a
base that may already contain some of those rows is the one way this can
produce an over-count — and an over-count is permanent where an under-count
self-heals. Replacement makes the operation a pure function of `(incarnation,
S, column set)` and therefore idempotent by construction: running it twice
writes the same object twice. That is the same reasoning that makes the wide
rebuild replace rather than merge.

**Why step 6 refuses instead of reconciling.** An append that publishes between
the file read and the write has merged its counts into the object we are about
to replace, and its rows are not in our maps. Keeping its pending link would
claim coverage over rows the aggregate never saw. Dropping the link would strand
the chain again, since the next append's parent is that snapshot and not `S`.
Neither is acceptable, so a moved table is a retry, and reconciliation by hand
is how a repair becomes a corruption.

**Why the pending links are cleared and not preserved.** After step 6 every
remaining link is at or below `S`, so its rows are in the files this pass read.
The links are redundant, and leaving them would let `add_coverage_link` advance
`coverage` past `S` on a chain the rebuild has already superseded.

**Bounding the retry.** Under continuous ingest step 6 can fail every time: the
rebuild races a commit cadence it cannot beat. Two shapes. The cheap one is to
bound the attempts and tell the operator to run it in a quiet window — what was
built, see the decisions below. The convergent one is a catch-up pass: on
conflict, do not rescan the table, fold in only the files added by the snapshots
between `S` and the new current snapshot, and rescan from scratch only if an
intervening commit was neither an append nor a row-conserving re-cluster. The
catch-up shrinks toward one commit's worth of work and converges; it is also a
second code path over the same maps, which is where a repair acquires its own
bugs.

**Why step 4 is a no-op and not an error.** Coverage already reaching `S` means
a previous pass did this work and nothing has invalidated it. Reporting that
rather than rewriting is what makes the command safe to put in a runbook. It
also falls out of the re-rooting property: after a successful pass, the next
append's edge has `S` as its parent and joins the chain, so a table that is
being maintained normally reports "already covered" rather than looking broken.

## What this does not touch

- **The wide object.** `siglake-agg-wide.json`, its deltas, its `absorbed` set
  and its `rebuilt_through` watermark are untouched. This rebuild has exactly
  one CAS target, the inline object, so the "wide-only repair, no second CAS
  target" decision of 2026-09-04
  (`DESIGN_incremental_group_count_aggregate.md`) is not reopened by it — that
  decision was about the wide rebuild also writing the inline object, which is
  the reverse direction.
- **`rebuild-group-counts`.** It repairs the folded wide group counts and is
  the remedy for a lost delta. This is a different object, a different failure
  and a different cost.
- **#3000's automatic short-aggregate repair.** That detects an aggregate short
  of `record_count` and schedules a Tier-2 rebuild. A pre-coverage object is not
  short; it is unprovable. Keeping them separate is deliberate.

  Worth checking once, because both landed in 0.1.1 and this pass drops
  `group_counts`: the census is unaffected. `short_group_count_census` takes its
  column set from the WIDE object and consults the inline one only to excuse a
  column the wide map leaves short, and that arm requires the inline object to
  prove coverage. Before a repair it never applied (no coverage edge); after
  one it still does not (coverage edge, no `group_counts`). So dropping the map
  can neither add a column to the census's verdict nor remove an exculpation
  that was in force. The same holds for the object the next commit rebuilds
  from empty: covered but short, which the arm already declines.

## The same machinery serves #3800

#3082 is one way an inline object ends up with an unprovable chain: it never
had one. #3800 is the other set — retention, a delete task, a foreign
overwrite, or a stretch of re-clusters longer than `retain_last` so the ancestor
walk runs off the end of retained metadata. The trigger differs; the repair does
not. #3800's stated acceptance is "a full recompute that sets `coverage` to the
snapshot it read, as the wide rebuild does", which is steps 5 through 8 above.

Two differences to carry into #3800 rather than assume away. Its triggers can
remove rows, so its rebuild has to run after the row-removing commit rather
than race it, and the object's totals are already short on their own terms
until it does. And a repair that fires automatically after every such commit
pays the 2-D decode cost each time, where an operator-invoked repair pays it
once — which is the argument for the containment optimization being settled
here first.

## Decisions, 2026-09-16

**The inline `group_counts` are dropped, not certified.** One `coverage` field
covers the whole object, so granting coverage to maps computed at an unknown
earlier snapshot is the unmarked-overwrite hazard the field exists to catch.
Dropping them costs nothing that was readable — a pre-coverage object's group
counts were already refused by every consult. What it does carry: from the next
commit onward the inline group counts hold that commit alone and stay short of
`total-records`, so on a table below the raised cardinality cap an unwindowed
`GROUP BY` stays on Tier-2. Accepted rather than recomputing them in the same
pass, which would have cost one more Tier-2 pass per column.

**The retry is bounded, not convergent.** Three attempts, then the command
exits non-zero asking for a window with no ingest to the table. The convergent
shape — fold in only the files the snapshots between `S` and the new current
snapshot added, rescanning in full when an intervening commit was neither an
append nor a row-conserving re-cluster — is specified above and can be added
without changing anything published here. It was not built because it is a
second code path over the same maps, and because the measurement puts the cost
this command addresses on cold, rarely-queried tables, which are the ones a
retry wins against: a warm table's per-file fallback measured ~2ms.

## What was built

- `IcebergContext::rebuild_inline_time_aggregates`, steps 1-8 above, with the
  two decisions applied. `siglake_inline_time_aggregate_rebuilds_total` counts
  a publication, `..._conflicts_total` an attempt lost to a commit, and
  `siglake_inline_time_{,group_}rebuild_files_total{source="footer"|"decode"}`
  says which arm each file took.
- `siglake rebuild-time-aggregates --table <t>`, reporting per component
  whether it was restored, because exiting 0 is not the same as the fast path
  being back.
- `crates/siglake-storage/tests/storage/pre_coverage_time_agg.rs`: the refusal
  and its permanence, the repair restoring Tier-1 with byte-identical answers,
  coverage advancing on the appends that follow, the second pass as a reported
  no-op, the footer and decode arms agreeing with what maintenance accumulated,
  and the refusal to invent an object that is not there.
- `crates/siglake-cli/tests/cli/rebuild_time_aggregates_cli.rs`: the report an
  operator reads, from the real binary.

One thing the measurement did not cover and the implementation therefore does
not claim: the 2-D decode's cost at scale. It is a two-column pass over the
live files, so it is bounded by what a Tier-2 query already costs on the same
table, but no at-scale figure has been taken.
