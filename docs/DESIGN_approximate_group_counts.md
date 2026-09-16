# Approximate group counts (bounded-size heavy-hitter sketch)

**Status:** stages 0–5 done, 2026-08-01, and stage 5 PASSES on the re-run with
preemption: `top_hosts` 3,080.83ms → **2.91ms** (~1,058×), labelled, top-100 set
exact, ingest unchanged. One issue open — ordering WITHIN the top-100 is
approximate; see stage 5b. Pre-launch. Companion
to `DESIGN_incremental_group_count_aggregate.md`, which this reuses wholesale.

Stage 0 changed two premises in this document — the top-K *set* turns out to be
exact even at 1,000 counters, and the CPU win is a ~3× constant rather than the
asymptotic one first claimed. Read stage 0 before stages 1–5; the justification
for building this is now bounded memory and cliff removal, not speed.

## The gap this fills

`TABLE_GROUP_COUNT_CARDINALITY` is a **cliff, not a dial**. In
`file_group_counts` the cap does exactly one thing — trigger a bail-out:

```rust
else if values.len() < cap { values.insert(v.to_string(), 1); }
else { over_cap = true; break; }
```

A column one distinct value over the cap contributes *nothing*. There is no
partial credit: `top_hosts` on such a column falls all the way back to a full
`GROUP BY` over 247M rows (~3s), or trips the mid-flight row ceiling and is
refused outright.

That is the whole gap. Under the cap we are exact and fast (57.85ms at 1.15M
hosts). Over it we are slow or we refuse. Nothing in between.

It is also invisible: `host` is 1,149,520 distinct today against a 2,000,000
cap. Growth past the cap silently reverts `top_hosts` from 58ms to 3s with no
signal — a 50× regression whose only symptom is latency.

## Policy: approximate replaces SLOW, and it is opt-OUT

**Revised 2026-08-01, after the first policy shipped and did nothing.** The
original rule was "approximate replaces refusal, not exactness". It was never
reachable: `host` misses Tier-1 and falls to the raw-page tier, which *succeeds*
exactly in ~3.1s, so there was no refusal to replace and `top_hosts` was no
faster than before the feature existed.

The rule is now:

- A **cheap** exact answer always wins — anything Tier-1 covers stays exact and
  is untouched.
- A **Tier-1 miss** on a whole-table top-K by descending count is served from
  the summary, labelled, instead of falling to a seconds-long footer-sum or
  raw-page decode.
- The caller can refuse it: `"exact": true` on the request takes the slow exact
  path however long it costs. Operators can disable approximation for a whole
  deployment with `SIGLAKE_APPROXIMATE_GROUP_COUNTS=off`.
- Every approximate answer carries its error bound and its "not counted"
  residual, in the response, always.

**Opt-out rather than opt-in, deliberately.** A high-cardinality `GROUP BY` that
silently costs seconds is a worse surprise than a labelled approximation — it
looks like the system is broken, and it appears only at scale, long after the
query was written. Defaulting to fast-and-labelled puts the choice in front of
the people who care about exactness, who are far better placed to know they care
than we are to guess.

This is deliberately narrower than the competition. Quickwit's terms aggregation
is *always* approximate: its `top_hosts` on this corpus reported
`sum_other_doc_count: 238,291,469` — it discarded **96.4%** of the corpus to
answer in 7.7ms, and you only learn that by reading the raw response. siglake
answering exactly where it can, approximately where it must, and saying which,
is a stronger position than either mode alone.

## The sketch: Misra-Gries

Misra-Gries with `m` counters, chosen over Count-Min Sketch (which needs a
separate heavy-hitter structure and only over-estimates) and over Space-Saving
(isomorphic in practice, but MG has the cleaner published merge proof — Agarwal
et al., *Mergeable Summaries*).

**Accumulate.** Keep ≤ `m` (value → count) counters. On an item: increment if
tracked; else insert with 1 if there is room; else decrement every counter by 1
and drop any that hit zero, accumulating the decrement into `error_floor`.

**Merge** (this is the property the whole pipeline depends on): union the
counters summing shared keys; if more than `m` remain, take the `(m+1)`-th
largest count `c`, subtract `c` from all, drop non-positive, and add `c` to
`error_floor`.

**Bound.** For any value `x`: `count(x) ≤ true(x) ≤ count(x) + error_floor`. Any
value whose true frequency exceeds `N/(m+1)` is guaranteed present. `error_floor`
is carried in the summary, so the bound is reportable rather than theoretical.

**Residual.** `record_count − Σ counts` is the exact analogue of Quickwit's
`sum_other_doc_count`, and we can report it because the snapshot knows
`record_count`.

### One pass, degrading in place

The neat part, and the reason this is cheap: a column does not need to be
classified in advance. The accumulator starts **exact** and, the moment it would
exceed the cap, converts its partial map into an MG summary and continues in
sketch mode — an exact map is a valid MG summary with `error_floor = 0`. So:

- under-cap columns end exact, byte-identical to today;
- over-cap columns end as a sketch, having cost one pass, not two;
- nothing needs to know a column's cardinality up front.

Per-commit *memory* stops scaling with distinct count: a fixed `m`-entry map
versus today's 1M-entry `BTreeMap`. Per-commit CPU does not — see stage 0, which
measured a ~3× constant-factor win rather than the asymptotic one this paragraph
originally claimed.

## Reuse: this is the same pipeline

Everything from the incremental aggregate applies unchanged. The sketch is a
different payload in the same objects:

| Machinery | Change |
| --- | --- |
| `<incarnation>/siglake-agg-deltas/<seq>.json` | carries sketches alongside exact maps |
| `<incarnation>/siglake-agg-wide.json` | holds merged sketches alongside exact maps |
| absorbed-set bookkeeping, fold cadence, deferred delete, GC | **none** |
| orphan-GC exclusion, CAS, watchdog | **none** |
| commit-path overlap with the write | **none** |

That is why this is a smaller project than it sounds: the hard parts —
correctness of the fold, double-count avoidance, GC, the commit-path cost
model — are built and validated.

## Stages

**0. Hermetic harness ✅** (`crates/siglake-storage/tests/mg_sketch.rs`). All
four properties pinned and each verified by breaking the code: dropping the
error a prune introduces fails 4 of 5 tests; dropping the *other* side's error
in `merge` fails exactly the two merge tests and correctly leaves the
single-sketch ones green.

A note on the harness itself: the first stream generator (min of two uniform
draws) was so weakly skewed that no value ever cleared the error floor, so the
heavy-hitter test could not observe a single heavy hitter. A generator too flat
to produce the phenomenon under test is worse than no test. It is now a proper
Zipf(s=1) by inverse transform.

**Measured, 10M rows over 1.15M distinct:**

| m | build ms | counters held | error_floor | top-100 correct | max count error |
| --- | --- | --- | --- | --- | --- |
| 1,000 | 778 | 1,159 | 3,441 | **100/100** | 48.25% |
| 10,000 | 608 | 7,806 | 250 | **100/100** | 3.51% |
| 100,000 | 970 | 136,162 | 14 | **100/100** | 0.20% |
| *exact* | *1,258* | *844,346* | — | — | — |

**Two findings that change the framing, both against my own initial claim:**

1. **The top-K *set* is exact at every `m` tested, down to 1,000 counters.**
   Only the counts are approximate. For a leaderboard that is the property that
   matters most, and it is far better than the worst-case bound suggests.
2. **The CPU win is ~3×, not asymptotic.** This document originally said
   per-commit cost "stops scaling with distinct count". It does not: MG still
   pays a hash lookup per row and an allocation per miss, and 608–970ms against
   the exact HashMap's 1,258ms (or ~2.8s for the production `BTreeMap` path) is
   a constant-factor win, not a change of order. Since the commit path now
   overlaps that work with the write anyway, **speed is not the reason to build
   this.**

The reasons that survive: it **removes the cliff** (a column over the cap gets
an answer instead of a 3s scan or a refusal), and it **bounds memory** — 7,806
counters against 844,346, ~108×, independent of how wide the column gets.

**Sizing `m`, from the measured corpus rather than simulation.** With
N = 247,249,096 and the top host at 277,634 (0.1123%), a value survives only
while its count exceeds the floor, and the floor is at most `N/(m+1)`:
`m ≥ 890` to retain the top host at all, `m ≥ 89,066` to bound its error under
1%. Note the real corpus is *flatter* than the Zipf used above (top 0.11% vs
~5%), so realised error at a given `m` will be worse than the table shows.

**Which points at a scoping correction worth stating plainly:** for `host`
specifically, exact is affordable — 3.6MB and 57.85ms — and a 100,000-counter
sketch would not even be smaller on disk. The sketch is not for `host`. It is
for the columns *beyond* the cap that we serve with nothing today. `m = 10,000`
(~400KB, ~8K counters) is the default I would start from, with the error bound
reported so a user can see when it is too coarse for their column.

**1. Format.** `GroupCountSketch` (counters + `error_floor` + `m`), versioned per
`DESIGN_file_formats.md` as an **accelerator** — an unreadable sketch degrades to
a scan, never to a wrong answer. Compact encoding, same shape as the group-count
codec.

**2. Accumulation.** The degrade-in-place pass in `file_group_counts`. Returns
exact maps and sketches from one traversal. Must leave under-cap behaviour
byte-identical — pinned by a differential test against the current function.

**3. Fold.** Sketch merge in the existing delta → wide-base fold. Small.

**4. Read path + labelling.** Serve top-K from the merged sketch **only** where
the exact path would have scanned or refused. Response carries `approximate`,
`error_upper_bound`, `not_counted`. A test must prove an approximate answer
cannot be returned unlabelled — that is the one failure that damages the
product, not just the query.

**5b. At-scale acceptance — RE-RUN 2026-08-01 with preemption. PASSES.**

| | before any of this | cap 262,144, refusal-only | cap 262,144, preemption |
| --- | --- | --- | --- |
| `top_hosts` p50 | 3,080.83ms | 3,122.48ms | **2.91ms** |
| ingest to `sealed_pending=0` | 7m51s | 7m53s | **8m00s** |

**`top_hosts` 3,080.83ms → 2.91ms, ~1,058×**, single-digit as the original
criterion asked. 18/18 zero errors, every other shape flat.

Accuracy against ground truth, taken by running the same query both ways on the
same snapshot:

- **top-100 SET: exact.** The right hundred hosts.
- **top-100 ORDER: NOT exact.** See below — this is the open issue.
- counts within **5.59% max / 3.86% mean**, every one inside the reported bound
  (max shortfall 2,995, bound 2,995 — the bound is tight, not decorative).
- `not_counted` 200,491,393 of 247,249,096. Large, and correctly so: 16,350
  counters cannot represent a 1.15M-value tail. Reported, which is the point —
  Quickwit's equivalent on this corpus is 238,291,469 and is not surfaced.

The opt-out works at scale: `"exact": true` returned 277,634 — the true count —
in 5,588ms.

**`count_distinct_host` stays at 2,994ms, correctly.** A summary with `m`
counters has no idea how many distinct values it evicted, so it must not answer
`count(DISTINCT)`. The scope restriction is doing its job.

### The open issue: order within the top-K

The bound is 2,995 while adjacent hosts in the top-100 are typically closer
together than that, so neighbours swap. For a leaderboard, ordering is much of
the point, and stage 0 checked set membership without ever checking order.

Order fidelity needs the bound well below the typical adjacent gap, i.e. a much
larger `m` — but at `m` = 100,000 the summary approaches the size of the exact
map it replaces (~2.8MB vs 3.6MB for `host`), and its memory advantage
evaporates for a column of this width. The honest options:

1. raise `m` and accept a bigger summary (helps order, erodes the memory case);
2. raise the CAP so columns of this width stay exact, and reserve the sketch for
   genuinely huge ones (10M+ distinct), where its advantage is real;
3. accept approximate ordering and document it.

This is the same cap-versus-sketch boundary question as before, now with the
data to answer it.

**5a. At-scale acceptance — FIRST RUN 2026-08-01. The sketch works; the POLICY
did not deliver.** Ingest 7m53s against a 7m51s baseline, so the sketch costs
nothing to build. The wide object carried `host` with 16,368 counters,
`error_floor` 3,006 (~1.1% of the top host) and `rows` 247,249,096 — exactly the
corpus count, so it accounts for every row at scale.

**And it was never consulted.** `top_hosts` came back at 3,122ms, against
3,080ms *before any of this work existed*:

| shape | cap 2,000,000 (07-31) | cap 262,144 (08-01) | pre-feature |
| --- | --- | --- | --- |
| `top_hosts` | **57.85ms** | 3,122.48ms | 3,080.83ms |
| `count_distinct_host` | **16.19ms** | 3,105.24ms | 3,087.53ms |

`host` (1,149,520 distinct) is above the 262,144 cap, so it misses Tier-1 and
falls to the raw-page tier — which SUCCEEDS, exactly, in ~3.1s. The policy says
approximate replaces refusal, not exactness, and the exact path did not refuse.
Both halves behaved exactly as specified and the result was worthless.

**The lesson, which is about the two decisions and not either one:** a
conservative cap and "exactness always wins" are individually defensible and
jointly the worst of both. A low cap is only worth having if the sketch may
preempt a *slow* exact path; otherwise a low cap is strictly worse than a
generous one for query latency, and the sketch is unreachable code. These were
chosen in separate conversations without anyone holding both at once.

The open choice: a generous cap (fast and exact for `host`, but the number comes
from our corpus and an unknown user's wide column falls off the same cliff), or
letting the sketch preempt slow-exact — ideally opt-in, so displacing an exact
answer is the caller's decision rather than ours.

Also found by this round, and fixed: the degrade-in-place was in the wrong
place. It fires when a COMMIT crosses the cap, but a commit sees ~5M rows and
~120K distinct hosts — under the cap — so `host` was counted exactly by every
commit and the overflow only happened in the fold, where `merge`'s `retain`
dropped the column entirely. No exact aggregate, no sketch, a 6s scan, and
`rows_scanned: 0` in the response because the raw-page path bypasses that
counter. Every hermetic test missed it because they all built columns that
crossed the cap within a single commit.

**Original criteria, for the record:** http_logs, and stateable in advance:
  - ingest wall unchanged versus the exact-path baseline (7m51s);
  - `top_hosts` on an over-cap column answered in single-digit ms;
  - measured error against the known exact answer within the reported bound;
  - the top-100 *set* matches the exact top-100 (the practical accuracy test —
    the bound is worst-case, Zipfian log data is not).

## Choosing `m` — unlike the cap, this one is a real dial

`m` trades memory and per-commit cost against accuracy, continuously, with no
cliff. Worst-case per-value error is `N/(m+1)`; on skewed data the realised
error is far smaller because the evicted minimum is small.

At N = 247M: `m` = 10,000 → worst case 24.7K (~9% of the top host's 277,634);
`m` = 100,000 → 2.5K (~0.9%). Memory is roughly `m × 40` bytes per summary, so
400KB and 4MB respectively. Stage 0 measures realised error on the actual
corpus distribution and picks the default from data — this is exactly the
experiment the cardinality cap did *not* deserve.

## Not doing: a compact encoding for the sketch

The sketch serialises as JSON inside the wide object, where every other
group-count payload uses the compact front-coded encoding. At the default
16,384 counters that is roughly 500KB per over-cap column against ~150KB
compact, so the change is a real 3× — and pre-launch is the only window in
which changing an encoding is free.

It is still not worth building, for two reasons that between them remove both
halves of the argument.

**It is inert at the shipped cap.** The sketch only exists for a column that
exceeds `SIGLAKE_TABLE_GROUP_COUNT_CARDINALITY`, now 4M. The corpus that
motivated all of this has 1.15M distinct hosts. No column in any measured
deployment produces a sketch, so the 3× applies to zero bytes today.

**The "free window" argument does not apply to this artifact.** It applies to
formats that would need a compatibility shim later. This one already carries
`version: u32`, and `GroupCountSketches::merge` *drops* an unrecognised version
rather than guessing — the exact behaviour `DESIGN_file_formats.md` prescribes
for an accelerator. So changing the encoding after launch costs one generation
of lost sketch on affected tables, which degrades to a scan and self-heals on
the next fold. That is what the version field was for.

Build it when a corpus actually produces sketches, and size it against that
corpus rather than against an assumed counter count.

## Risks

- **An approximate answer escaping unlabelled.** The one outcome that matters
  more than latency. Labelling belongs on the same struct as the counts, not
  alongside it, so it cannot be dropped by a refactor.
- **Merge correctness.** The `(m+1)`-th-largest subtraction is easy to get
  subtly wrong and would fail *silently* — counts would just drift. Differential
  testing against a brute-force exact counter over randomised streams.
- **Two paths through the read code.** Mitigated by the policy: the sketch is
  consulted only where the exact path has already given up, so it is a fallback
  branch rather than a parallel implementation.
- **Scope discipline.** HyperLogLog for `count_distinct` is the obvious sibling
  and rides the same pipeline — but `count_distinct_host` is already 16.19ms
  exact, so it is not in this plan. Add it later if a corpus makes it hurt.
