# Incremental group-count aggregate (base + deltas)

**Status:** stages 0–5 implemented. Stage 5 re-run 2026-07-31 with the overlap
fix: ingest is back to baseline (accept 7m51s vs 7m59s, tail 1 min) with the
knob ON, `top_hosts` 3,080.83 → 57.85ms exact, `count_distinct_host` → 16.19ms,
18/18 zero errors. The one criterion still unmet is the self-imposed
single-digit-ms bar; see stage 5. **The default is still 4096 — flipping it is a
deliberate decision, not implied by this round.** Pre-launch.

## The problem, measured

`top_hosts` (exact top-100 over ~1.1M distinct hosts, 247M docs) costs **3,081ms**
because high-cardinality columns get no precomputed aggregate: above
`TABLE_GROUP_COUNT_CARDINALITY` the column is dropped from the snapshot
aggregate, above the per-file cap (1024) it is dropped from every footer, and
the raw-page fallback refuses dictionaries over 65,536 values — so the query
lands on a full DataFusion `GROUP BY`.

Raising the cap to 2,000,000 was tried at scale on 2026-07-29 and **collapsed
ingest**: accept was fine (236M of 247M rows in ~16 min), then the run crawled
for two hours until the harness gave up. The aggregate was maintained
synchronously on the commit path by a CAS read-modify-write, so at 1.1M keys
*every commit* decoded, merged, re-encoded and zstd'd the whole map.

The query side is not the problem. Selection is already an O(n) quickselect
over borrowed keys, measured at **1.82ms** over 1.1M keys once the aggregate
exists. The problem is exclusively how the aggregate is *maintained*.

## Why "move it to the compactor" is not sufficient

The obvious fix — build the aggregate in the background — fails on the read-time
validity guard. Tier-1 is only used when `column_total == record_count`. A
background-built aggregate is by construction behind the newest commits, so it
fails that guard and falls through to the scan: all of the work, none of the
benefit.

The aggregate has to stay *current* while its maintenance cost stops being
proportional to total cardinality.

## Design: base + deltas, folded in the background

The same LSM shape Siglake already uses for data, applied to the aggregate.
Four object classes per table incarnation. Every one of them is addressed
under `metadata/siglake-agg/<table-uuid>/`, so an index recreated at the same
location reads only what its own incarnation wrote (#2919; the paths below are
relative to that prefix):

| Object | Written by | Holds |
| --- | --- | --- |
| `siglake-aggregates.json` | every commit, under CAS | columns within `BASE_GROUP_COUNT_CARDINALITY`, plus the time aggregates. **Unchanged.** |
| `siglake-agg-deltas/<seq>.json` | every commit, one unconditional PUT | that commit's contribution, all covered columns, at the full table cap |
| `siglake-agg-wide.json` | the compactor only | the folded aggregate + the set of delta ids already absorbed |
| `siglake-agg-deltas/<seq>.rebuild.json` | a committer after a delta exhausts its retries | durable automatic-rebuild request, including the lost exact columns and their admission caps plus sketched column names; shares the delta listing so the healthy path adds no object-store request |

- **Commit path** — one small PUT sized by the commit. No read, no merge, no
  CAS. Written after the commit, like the inline object, so it only ever
  describes rows that are in the table.
- **Read path** — per column, the inline object if it accounts for every row,
  else the wide base plus every delta it has not absorbed. A low-cardinality
  `GROUP BY` therefore reads exactly what it always did — one object, no
  listing — and only a miss pays for the fold. Memoized per snapshot on the
  cached table entry, alongside the existing side-aggregates memo.
- **Compactor** — folds outstanding deltas into the wide base, records their
  ids, deletes them a cycle later, then drops the ids once the objects are
  gone. Paced by `SIGLAKE_AGG_FOLD_INTERVAL_SECS` (default 60s).

### Three things that are not obvious, and cost the most to get wrong

**The wide base must be its own object.** Splitting *columns* between maps
inside one object does not help: the inline object is read, decoded, re-encoded
and written on every commit, so its cost is set by its size and not by what the
commit touched. A 1.1M-key map in there collapses ingest no matter who put it
there. Nothing on the commit path reads or writes `siglake-agg-wide.json`.

**Deltas are keyed by Iceberg sequence number, not snapshot id.** Snapshot ids
are `abs(uuid.hi ^ uuid.lo)` — random, carrying no order at all. Sequence
numbers are strictly increasing per commit for format v2 (which is what
`TableCreation` defaults to, and `add_snapshot` rejects a non-increasing one).
The first cut of this design used a high-water mark over snapshot ids, which
would have skipped roughly half of all future deltas — failing safe, and so
invisible to every hermetic test written with sequential ids.

**Absorption is recorded by membership, not by a high-water mark.** Even over
monotonic sequence numbers a scalar watermark is unsound, because deltas cannot
be totally ordered by *arrival*: a writer that stalls between its commit and its
delta PUT lands one below a mark the compactor has already advanced past, and
that delta is then skipped forever. A set makes no ordering assumption. It stays
small because a delta is deleted once absorbed and its id dropped once the
object is gone — steady state is roughly one cycle's worth.

**Deltas carry every covered column, not just the wide ones.** Classifying per
batch would put `host` inline while it still had few distinct values and then
drop it — and its early rows with it — the moment it outgrew the inline cap,
leaving the one column the feature exists for permanently incomplete. Carrying
everything costs a handful of keys per commit and makes the wide aggregate
complete from a table's first commit. The read path never sums the two maps, so
a column present in both is served by whichever is complete, never doubled.

### The correctness hazards

**Double counting.** A delta object outlives its absorption by a full cycle on
purpose, so there is a window where the base contains a delta whose object is
still listed. The absorbed set is what closes it; both must be read from the
same object, which they are.

**Missing deltas.** If a delta is absent (not yet written, lost, or GC'd early)
the folded total will not equal `record_count`, the existing guard fails, and
the query falls through to the per-file path. Safe by default — but note this
is also why an *answer-only* test proves nothing here: every way of corrupting
the aggregate degrades to a scan that returns the correct answer. The tests
assert on the aggregate. An exhausted write leaves a per-sequence rebuild
marker; the maintenance compactor consumes it after its normal fold and rebuilds
the exact maps and any affected bounded sketches from committed files.
Per-sequence objects avoid a lost wakeup: a rebuild only deletes markers at or
below the snapshot sequence it scanned, so a concurrent newer loss remains
pending.

**A delta deleted mid-read.** A reader can list a delta and find it gone before
reading it, against a base that predates its absorption. Detected (a read
returning `None`) and retried against a fresh base; three losses in a row give
up and fall back to the per-file path.

## Repair and limits

A delta PUT gets four total attempts, with 250/500/750ms delays between them.
`siglake_group_count_delta_write_retries_total{iceberg_namespace="<ns>",table="<table>"}`
counts the retries used when a later attempt succeeds;
`siglake_group_count_delta_write_failures_total{iceberg_namespace="<ns>",table="<table>"}`
counts a PUT that exhausted all four. The committer then writes a durable rebuild marker;
the maintenance compactor recomputes through the exact Tier-2 path — footer
where one exists, raw-page decode where it does not — and records
`rebuilt_through` so a late delta from the scanned prefix cannot be folded
twice. If the marker write or automatic rebuild fails, the LOST log names the
operator fallback:
`siglake rebuild-group-counts --namespace <ns> --table <table>`. It is safe
to re-run.

**A shortfall with no marker at all (#3000).** A marker is written by the
committer, so a committer that dies between its commit and its delta PUT leaves
none, and neither does the per-incarnation prefix change (#2919), which starts
every upgraded table's aggregate fresh at its first commit after the upgrade.
That state is stable: a later delta adds its own rows and the total stays short.
The maintenance pass therefore also censuses each maintained table on a slow
interval (`SIGLAKE_AGG_SHORT_SCAN_INTERVAL_SECS`, default 900s) for a maintained
column short of `total-records`, reading one total per column out of the compact
base (`decode_column_totals`) rather than decoding each column's values.

Two rules decide when it may rebuild rather than report, and both exist because
the alternative is worse than the deficit. **Landed, not merely absent:** a
commit publishes its delta after its commit, so "short" and "short for another
second" read identically in the totals; the census requires the current
generation's own contribution to be in the folded artifact, counting coverage
links still waiting on a missing predecessor and bridging a row-conserving
re-cluster the way the read guard does. **One attempt per condition:** a column
the rebuild cannot cover is dropped from the base by that rebuild and re-added
short by the next delta, so the rebuild records the columns it failed to restore
(`short_repair` in the base object, written in the same CAS that publishes the
rebuild) and the census skips them until another rebuild — a marker repair, or
the CLI — clears the record.

A census rebuild recomputes the **exact** columns and each existing sketch from
the files. Sketch restoration is per column: `None` from the Tier-2 path means
that column keeps the base-plus-delta state the rebuild was already required to
carry and is reported as unrestored; an object-store or decode error still
fails the rebuild. This distinction matters on the events table, where
`timestamp_ns` becomes a sketch after its per-row-unique values cross the cap,
but neither its omitted footer nor the UTF-8-only raw-page fallback can serve
it. One unavailable typed column no longer prevents a readable short sketch
beside it from being corrected. Exact-versus-sketch exclusivity is preserved:
any column selected for sketch restoration is removed from the exact result.

The carry remains necessary. `rebuilt_through` makes every delta at or below it
redundant, and both folds then delete rather than fold it — sketch half included,
which no later commit re-adds. The rebuild therefore merges the sketch half of
exactly those deltas into the base under the fold's own redundancy predicate,
then replaces each sketch it could recompute. Marker repair retains its
all-or-nothing sketch contract, because a durable marker names rows known to be
lost. The CLI still carries sketches without recomputing them, so its cost and
operator contract are unchanged.

Measured again 2026-09-20 (release, local filesystem, one dimension column):
census 14.6ms / repair 65.1ms at 40k rows, 145.4ms / 876.5ms at 400k. Both are
linear in the column's distinct values and the repair is ~6× the census per
column, so the census is unconditional and the repair is opt-in
(`SIGLAKE_AGG_SHORT_REPAIR=1`) and budgeted at one table per pass
(`SIGLAKE_AGG_SHORT_REPAIR_MAX_TABLES`). At the 1TB shape the extrapolation is
~9 minutes per column per 250M rows; no large-table timeout was measured. The
compactor's cooperative 600s watchdog can cut that repair safely because the
rebuild publishes in one write at the end. A local cancellation at a requested
286.3ms returned at the scan's next yield, 830.5ms, published nothing, and was
detected again after reopening the warehouse. The durable 0.2.0 retry decision,
including the 1.3ms local marker-path measurement, is in
[`DESIGN_short_aggregate_repair_backoff.md`](DESIGN_short_aggregate_repair_backoff.md).
Until it is implemented, a table that size stays the operator's to rebuild.

The sketch-restoration A/B uses matching 8-commit warehouses with two
high-cardinality dimensions and an unavailable carried `timestamp_ns` sketch.
At 40k rows, carrying the two sketches cost 51.2ms and restoring the readable
one cost 91.0ms (+39.7ms). At 400k rows the pair was 792.5ms and 1,432.5ms
(+640.0ms). This is the expected one extra Tier-2 query per readable sketch.
The extension is kept because it runs only inside the existing opt-in,
one-table-per-pass repair budget and is the only automatic path that can correct
a sketch already short before the repair. The census-only pass, marker repair
and CLI add no work.

The rebuild takes its column set from the aggregate, not the schema: it repairs
what a table was maintaining, and inventing columns would change what the table
serves. The one opt-in exception is `--admit-typed-columns`, for a table
created before typed columns joined the side aggregates (`2c597f8`): its typed
dimensions are in every file's footer and in no aggregate, so the plain rebuild
cannot help and the read path serves them from the per-file tier forever. The
flag unions the typed part of the write path's column set
(`group_count_columns_for` with no declared dims — a typed column is never a
bloom column) into the rebuild. Typed inference excludes the canonical
`timestamp_ns` twin when the schema also has the canonical `timestamp` field:
the twin is event-time storage, not a group dimension. A user field named
`timestamp_ns` beside another declared event-time field is still inferred, and
an explicitly declared dimension is still admitted. An admitted column is held
to the typed cap on its whole-table distinct count (counted, reported, not
written when over), and
is left absent — never partial — when `grouped_counts_from_files` returns
`None`, i.e. some live file has no footer for it and the raw-page decode cannot
read its physical type or the column is missing from that file's schema. That
last case is the only one a rewrite is truly required for, and the CLI's report
separates it from the case the flag fixes. Admitted columns land in the wide
object, the rebuild's only output, so they read back as `tier1_wide`.

A separate repair, for the separate failure of an inline object with no
coverage chain at all, is specified in
`DESIGN_inline_time_aggregate_rebuild.md`. It targets the inline object only
and leaves everything in this document untouched.

**The rebuild is deliberately wide-only (decision 2026-09-04).** It does not
also repair `siglake-aggregates.json`, even for a rebuilt column below
the 4096 inline cap. Such a column stays `tier1_wide`; on a cold metadata cache
the reader may load and fold the wide object instead of taking the inline
shortcut. Repairing both objects would add a second CAS target and require a
staleness rule against the commit path's concurrent inline merge. That extra
write protocol is not justified for an exact Tier-1 result that the wide object
already provides.

`SIGLAKE_TYPED_GROUP_COUNT_CARDINALITY` caps exact group counts for typed
columns admitted by inference. It defaults to 1024, matching the per-file
footer cap; declared dimensions still use `SIGLAKE_TABLE_GROUP_COUNT_CARDINALITY`.
The effective inferred-column cap is the smaller of the two.

## Stages

**0. Hermetic harness ✅** — exactness at every absorbed prefix, idempotent
absorb, the absorbed set proven load-bearing (a reader ignoring it over-counts
exactly 2×, asserted), a missing delta caught by the row-count guard. **Cadence
budget:** the fold is flat to ~200 outstanding deltas (~2.1ms, dominated by the
0.85ms base clone) and 11.5ms at 500 — so 60s of backlog is nowhere near the
budget, and there is nothing to gain by folding harder.

**1a. Format ✅** — superseded in place by the revision above (sequence numbers,
absorbed set, separate wide object). It was inert and unreferenced, so revising
it cost nothing.

**1b+2. Writer + read-path fold ✅** — landed together because they are one
behaviour change: capping the inline object at the ceiling is what makes the
fold necessary, so shipping the writer alone would leave a wide column covered
by nothing.

**3. Compactor fold + GC ✅** — fold, absorb, deferred delete, prune. Its own
cycle step, watchdog-bounded, `SIGLAKE_AGG_FOLD_INTERVAL_SECS` (60s); inert
unless the knob is raised. Deliberately not part of the re-clustering pass —
a deployment with re-clustering off would otherwise never fold. It does inherit
the loop's `draining` early-out, so folds pause during an active drain and the
backlog is absorbed once it goes idle (observed live: 42 deltas → 0 within a
minute of `sealed_pending=0`).

**4. Raising the cap is now safe ✅** — the knob is the sole gate and the whole
mechanism is behind it. Default stays 4096 until stage 5 passes.

**5. At-scale acceptance — RE-RUN 2026-07-31 WITH THE OVERLAP FIX.** http_logs,
single node, `SIGLAKE_TABLE_GROUP_COUNT_CARDINALITY=2000000`, caches off.

| criterion | 07-30 (serial) | 07-31 (overlapped) | |
| --- | --- | --- | --- |
| ingest wall within ~10% of baseline | accept +41%, tail 4.5 min | **accept 7m51s vs baseline 7m59s, tail 1 min** | ✓ |
| `top_hosts` p50 single-digit ms, exact | 64.48ms | **57.85ms**, exact | ✗ |
| exact ingest count | ✓ | **✓** (247,249,116 scanned = corpus + 20 markers) | ✓ |

18/18 zero errors, freshness 20/20 @ 5.49s p50.
**`top_hosts` 3,080.83 → 57.85ms (53×)** and **`count_distinct_host` 3,087.53 →
16.19ms (191×)**, both `rows_scanned: 0`, every other shape flat.

**Two of three criteria pass and the third is a judgement call.** 57.85ms is not
single digits. It also beats every engine that answers this shape exactly —
DuckDB 259ms, ClickHouse 708ms — while Quickwit's 7.7ms discards 96.4% of the
corpus (`sum_other_doc_count` 238,291,469). The single-digit bar was set in this
document without a stated justification; "beats the best exact answer" is the
one that reflects what the board actually claims.

**Where the remaining 58ms goes**, if it is ever worth closing: `count_distinct_
host` does the same fold and iteration for 16.19ms, so ~16ms is the aggregate
scan and ~42ms is `top_hosts` materializing a 1.1M-entry borrowed `Vec` (~26MB)
before selecting. The targeted fix is a bounded K-heap over the iterator instead
of a full materialization — O(n) time, O(K) memory, no large allocation —
which would help more than the `BTreeMap`→sorted-`Vec` change and is smaller.

What did work, cleanly: `top_hosts` 3,080.83 → **64.48ms (47.8×)** and
`count_distinct_host` 3,087.53 → **51.78ms (59.6×)**, both `rows_scanned: 0`,
18/18 zero errors, and **every other shape flat** — which is the evidence that
the inline path really is untouched. Exactness end-to-end: 1,149,520 distinct
hosts summing to 247,249,116 (corpus + the harness's 20 freshness markers).
The mechanism behaved as designed throughout: deltas accumulated during ingest,
the fold absorbed the backlog (42 → 0) within a minute of the drain going idle,
the wide object reached 3.6 MB while the per-commit object stayed at 90 KB.

**Why 64ms and not single digits.** Not the fold, and not the top-K — that
stage is already clone-free over borrowed keys. It is `column_rows()`, which
materializes the aggregate into an owned `Vec<(Option<String>, u64)>`, cloning
all 1.1M keys, on every query. Corroborated by the board itself:
`count_distinct_host` does trivial post-processing on the same upstream call and
still costs 51.78ms, while low-cardinality `count_by_status` costs 1.89ms — the
cost tracks key count, not query shape. With result caches ON this is memoized
per (table, snapshot, column) and invisible; with them off, as any honest board
must run them, it is the floor. The design's "1.82ms quickselect" figure was
measured over *borrowed* keys and is still right about the stage it describes.

**Fixed 2026-07-30, not yet re-measured at scale.**
`grouped_counts_with_summary` now returns a `GroupCounts` view that borrows from
the Arc'd aggregate instead of an owned `Vec`; every consumer already only
borrowed. Deliberately *not* fixed by reclassifying the per-column memo as a
"data cache" so it survives `SIGLAKE_QUERY_RESULT_CACHE=off` — that moves the
number without earning it, which is what made the 07-27 board meaningless.

Measured hermetically (`tests/group_count_view_cost.rs`, release):

| keys | materialize + scan | borrow + scan | flat-Vec scan |
| --- | --- | --- | --- |
| 1,000 | 0.08ms | 0.00ms | 0.00ms |
| 100,000 | 6.51ms | 0.81ms | 0.05ms |
| **1,100,000** | **89.72ms** | **21.26ms** | **1.35ms** |

So ~68ms comes off `top_hosts`, which should land it near 25–30ms rather than
64.5ms — **still not single digits, so stage 5 still would not pass on that
criterion.** The remaining ~21ms is not work the query needs: it is pointer
chasing through `BTreeMap` nodes, and the flat-Vec column measures the headroom
at 1.35ms. Closing that means `ColumnGroupCounts` storing its values as a sorted
`Vec` rather than a `BTreeMap` — a change to a type used across merge, lookup
and the compact codec, so it is a separate piece of work, not a tweak. The
design's "1.82ms quickselect" figure is consistent with the flat-Vec column: it
was measured on data already in contiguous form.

**The accept regression IS the mechanism, not infrastructure — controlled
2026-07-30.** Four runs of the same corpus, all reaching ~7 minutes together:

| run | knob | at 7 min | tail |
| --- | --- | --- | --- |
| 07-29 | off | 233.9M | **1 min** → done |
| 07-30 #1 | **on** | 239.7M | **4.5 min** |
| 07-30 #2 | **on** | 238.2M | **node wedged** |
| 07-30 #3 | off | 239.3M | **1 min** → done |

Run #3 used the *same image* as the wedged #2, differing only in the env var, so
this is not a code difference and not instance luck. Two knob-on runs faltered
in the accept tail; two knob-off runs did not. The first reading — "known infra
flakiness, retry" — was wrong, and worth recording as wrong: the wedge signature
(SSM unresponsive, CPU collapsing to ~7%, EC2 status still `ok`) matches a
documented infra failure mode exactly, which made the convenient explanation
also the plausible one.

**What it is not.** S3 request volume, the first hypothesis. The run's compactor
log shows 7 fold events folding 3–4 deltas each — ~25 delta PUTs for the whole
247M-row run. Nowhere near enough to exhaust anything.

**What it most likely is.** ~25 commits for 247M rows means ~10M rows per
commit, and at the measured 139.70ms per 500K rows the table-cap pass is **~2.8
seconds of synchronous work per commit** — building a map with a `String` per
distinct value, on the path the drain runs on. At the tail the drain is the
bottleneck and backpressure feeds straight back into accept, which is why the
stall is tail-shaped and why accept looks healthy until it suddenly isn't.
~70s across the run does not fully account for #1's +210s tail, so allocator
churn (~1M `String`s per commit) is a live second contributor; the round's
iostat captured CPU and disk but not memory, so that part is unclosed.

**The footer redesign is a dead end, and here is why — do not re-derive it.**
The obvious fix looks like: the Parquet writer already walks every row and
stamps per-file group counts at close, capped at 1024, so raise the cap and let
the compactor fold per-file footers with zero added commit work. It does not
work. At cap 1024 the writer *abandons* a wide column the moment it exceeds the
cap (`accumulate_group_counts` sets `over_cap` and drops it), so its cheapness on
`host` is precisely because it does nothing. Raising the cap moves the same
~2.8s into the writer — which is equally on the commit path. Counting 10M rows
by host costs what it costs, and the commit path is where the rows are. A
secondary problem, had the first not been fatal: complete footers run to
megabytes per file, and every metadata read would pay for them.

**What actually helps: overlap, not relocation.** The three aggregate passes ran
to completion BEFORE the write, so a commit paid CPU and then IO serially. They
now run on a blocking thread alongside the upload, so a commit pays
max(cpu, io). Nothing moved off the commit path; the path just stopped being a
straight line. Read `siglake_iceberg_aggregate_join_duration_seconds` to see
what did not overlap. The caveat is real — Parquet encode and compress are
themselves CPU-hungry, so the overlap only pays where cores are free (ingest ran
at 25–40% of 16 cores in these rounds).

Note also that the criterion's stated "34–37 min" baseline matches no artifact
(the 07-29 baseline is 29.6 min on-node); re-derive it before the next attempt.

**Remaining query-side headroom.** `top_hosts` lands ~25–30ms with the borrowed
view, which already beats every engine that answers it exactly (DuckDB 259ms,
ClickHouse 708ms) — so the single-digit criterion was polish, not a blocker, and
was self-imposed without a stated justification. Three follow-ups were measured
against it after the 2026-08-03 round; the measurements changed the plan twice.

- **Targeted per-column decode — done, 7.2×.** The 1TB wide base was 26.5MB
  across 22 columns and the read path decoded all of it to check one column's
  total. It now decodes one column, using a reader the compact-footer work
  already had. On-disk format unchanged.
- **Sorted `Vec` instead of `BTreeMap` — done, but only on the read path.** The
  original note proposed changing `ColumnGroupCounts` itself, touching merge,
  lookup and the codec. That is the wrong scope: the fold genuinely needs keyed
  updates, and the read path genuinely never does one. So the read path gets its
  own type (`SortedColumnCounts`) and the fold keeps the map. Measured: the
  codec already returns values sorted, so rebuilding the map was pure waste —
  **13.5ms per 400,000-value column**, ~35% of the decode. The invasive version
  buys nothing beyond this.
- **Bounded top-K — done, but the stated rationale was wrong.** The premise was
  that materializing a 1.1M-entry view cost ~3× on `top_hosts`. It does not:
  measured, bounded and unbounded are **1.0×** (20.13ms vs 19.22ms). The view is
  already borrowed, so the per-key `String` allocation that made materialization
  expensive was removed long ago, and what remains is the unavoidable O(n) walk.
  The bound was kept anyway, on the honest grounds it actually has: it drops a
  **26MB transient allocation per query to 4KB**, which is ~830MB of churn at
  32-way concurrency, not a latency win.

The ordering contract needed care in the second item: a `Vec`'s order is the
codec's promise where a `BTreeMap`'s was the container's guarantee. Both are
pinned by differential tests against the map-backed path, break-checked.

**6. Census sketch recovery ✅ (2026-09-20).** The opt-in census rebuild now
restores each readable sketch and carries an unavailable one independently.
The retained cost A/B and the `timestamp_ns` regression are in
`agg_short_repair`; the measured decision is recorded under "Repair and
limits" above.

## Risks

- **The commit path is the most correctness-sensitive code in the system.**
  Exact ingest is a core claim (11 consecutive). Below the ceiling the commit
  path is byte-identical to before; above it, the only addition is one PUT after
  a successful commit, and a failed PUT costs a fallback, never a wrong answer.
- **Read amplification if the fold lags.** Bounded by cadence; stage 0
  quantified it before any production code was written.
- **Object count.** One small object per commit until folded.
- **An over-count is permanent where an under-count self-heals.** A missing
  delta is fixed by the delta arriving; a doubled one is baked into the base and
  leaves the column falling back to a scan until something rebuilds it. Nothing
  in the design can produce one — this is the reason the absorbed set exists —
  but it is the asymmetry to keep in mind for anything added later.

## Rollback

Drop `SIGLAKE_TABLE_GROUP_COUNT_CARDINALITY` back to the default and the
mechanism disappears: no deltas written, no new objects read, and a wide column
uncovered exactly as before. Asserted, not assumed —
`crates/siglake-storage/tests/agg_delta_disabled.rs`.

## Alternative, if this proves too large before the tag

Remove the per-file footer cap so high-cardinality columns get *complete*
footers, and merge at query time. Measured ~0.5s for 12M entries — far short of
1.82ms, but roughly 6× better than today's 3,081ms, for a much smaller change
that never touches the commit path.

## Exact top-K candidate cutoff (0.3.0, 2026-10-09)

The bounded collector still partitioned every 100 candidates for a LIMIT 100,
including candidates already known to lose. After each partition it now retains
the Kth entry as a cutoff and rejects candidates that are no better, using the
same count/key/NULL comparator as the final sort. Between partitions the cutoff
can only be too permissive: retaining better candidates cannot make the true
Kth entry worse. Buffered counts are merged before comparison; buffered-only
groups pass through the same collector. Storage coverage checks and result-cache
behavior are unchanged. This does not cache a leaderboard or approximate it.

The collector measurement `report_bounded_top_k_vs_full_view` compares the
previous algorithm with the cutoff over 1,150,000 distinct, ascending host keys,
with K=100. Each arm checks its answer against a full sort. It alternates arm
order for 12 rounds, discards the first, and reports the median of 11. An
optimized (`rustc --test -O`) extraction of the actual comparator, collector and
tests on an AMD Ryzen 9 5950X measured:

| Count distribution | Previous collector | Cutoff collector | Speedup |
| --- | ---: | ---: | ---: |
| Pseudorandom uniform | 15.38 ms | 2.47 ms | 6.23x |
| Long tail | 21.46 ms | 2.39 ms | 8.97x |
| All counts tied | 28.74 ms | 5.86 ms | 4.91x |
| Strictly improving candidates | 13.86 ms | 15.54 ms | 0.89x |

These are synthetic collector timings, not HTTP latency or published benchmark
results. The improving-input case exposes the extra comparison cost when every
candidate survives; it is deliberately retained in the measurement. The Sep 2
published `top_hosts` p50 of 142.29 ms includes planning, storage and response
work this measurement does not cover. A matched HTTP-logs run with result caches
off is required before changing that published result.

## Wide-column streaming read (0.3.0, 2026-10-10)

The matched HTTP run for the candidate cutoff improved exact `top_hosts` warm
p50 from 149.65 to 124.47 ms and p95 from 173.93 to 148.61 ms, but collection
still averaged 119.9 ms and `count_distinct_host` measured 116.09 ms. Cold
`top_hosts` regressed from 745.03 to 840.38 ms, and the unchanged AVG control
slowed 10.5%. The rounds used independent fresh ingests, so they do not isolate
the collector as the cause. The retained system report is
`siglake-benchmarks/results/20261010-httop-execution/comparison-report.md`.

The next local profile isolated `WideGroupCounts::decode_column`: its targeted
decoder still allocated one `String` per group before the bounded collector
saw a key. Both ignored release profiles consume the same bytes in their two
arms, and timing and allocation passes are separate so allocator atomics do not
inflate CPU or wall time.

The primary input is the read-only HTTP-logs capture at
`siglake-benchmarks/results/20261010-httpavg-execution/wide-profile-fixture.json`
(SHA-256 `04ab46c13dc5537f6c9e3f249e9c4da4bc148d05445c1a3c78ad43a09887cde0`).
It carries 1,149,519 real host groups in a 3,515 KiB encoded blob. Set
`SIGLAKE_GROUP_COUNT_PROFILE_FIXTURE` to that file and run
`report_real_fixture_decode_materialization_cost`. Three release runs at
`765731d` measured:

| Real captured arm | Wall ms | Process CPU ms | Allocations | Allocated MiB | Peak heap growth MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Owned targeted decode | 101.00–102.85 | 100.94–102.83 | 1,149,552 | 158.26 | 83.68 |
| Streaming validation + top-100 selection | 48.27–51.55 | 48.27–51.55 | 1,462 | 18.72 | 10.70 |

The real input reduced decoder-plus-selection wall/CPU by 49–53%, allocation
count by 99.87%, allocated bytes by 88% and peak heap growth by 87%. The test
compares every streamed row with the full decoder and the streaming top 100
with full decode plus sort before it passes.

The raw real-fixture readings were:

| Run | Owned wall / CPU ms | Prepare wall / CPU ms | Select wall / CPU ms | Stream total wall / CPU ms |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 101.00 / 100.94 | 31.98 / 31.97 | 19.58 / 19.58 | 51.55 / 51.55 |
| 2 | 102.85 / 102.83 | 28.79 / 28.79 | 19.49 / 19.48 | 48.27 / 48.27 |
| 3 | 101.63 / 101.62 | 28.96 / 28.95 | 19.69 / 19.68 | 48.65 / 48.62 |

The synthetic input remains as a repository-local reproduction. It has a
1,453 KiB encoded payload with 1,149,520 non-NULL keys and one NULL group,
uniform counts and tightly front-coded dotted-decimal keys, so its cutoff and
compression are more favorable than the captured host distribution. The same
three-run invocation measured:

| Synthetic arm | Wall ms | Process CPU ms | Allocations | Allocated MiB | Peak heap growth MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Owned targeted decode | 64.00–65.98 | 63.97–65.98 | 1,149,553 | 157.62 | 84.56 |
| Streaming validation + top-100 selection | 39.93–40.50 | 39.93–40.49 | 732 | 17.20 | 9.19 |

That input reduced decoder-plus-selection wall/CPU by 37–39%, allocation count
by 99.94% and peak heap growth by 89%. Both profiles are local codec/selection
evidence, not a public HTTP result.

The raw synthetic readings were:

| Run | Owned wall / CPU ms | Prepare wall / CPU ms | Select wall / CPU ms | Stream total wall / CPU ms |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 64.00 / 63.97 | 23.51 / 23.51 | 16.99 / 16.99 | 40.50 / 40.49 |
| 2 | 65.98 / 65.98 | 23.16 / 23.16 | 16.77 / 16.77 | 39.93 / 39.93 |
| 3 | 65.02 / 65.02 | 23.27 / 23.27 | 16.78 / 16.78 | 40.06 / 40.06 |

The read path now retains the decoded compact body, reconstructs every column's
keys to validate the whole body before returning, and reconstructs the selected
column again into one reusable callback buffer. Count and predicate consumers
use the key only inside the callback. The bounded collector copies a key only
when it remains competitive; the synthetic profile owned 709 keys across the
entire 1.15M-key selection.

That whole-body validation is new work relative to `decode_column`, which
stopped after the requested column. CPU therefore scales with every key in the
blob even though retained allocations scale with the encoded bytes plus the
selected top K. At the recorded 1 TB extreme (26.5 MiB, about 20 million keys
across 22 columns) one cache-disabled read can reconstruct about 20 million
keys for validation and the requested column a second time. No retained copy of
that extreme exists to time, so the real one-column capture above does not bound
its wall time. The trade keeps the rule that corruption anywhere in an
aggregate refuses the aggregate before a callback can expose a partial answer.
Callers that need materialized values retain the full decoder. The on-disk
format, SQL shapes, coverage and row-total guards, cardinality budgets,
result-cache switch and aggregate-object cache are unchanged. A malformed,
truncated or foreign payload returns no aggregate and takes the existing exact
fallback; no callback runs until validation has succeeded.

Differential tests compare the streaming rows with the full decoder and the
owned bounded collector with a full sort. The latter covers string, integer and
float ordering; ascending and descending count/key sorts; dense ties; NULL;
forward and reverse input; LIMIT 1 through the full input; and a winner arriving
last. The existing full decoder remains covered independently.
