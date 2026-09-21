# Design — predicate-keyed decoded-cache qualification

Status (2026-09-21): **local prototype, nothing wired, disposition REJECT.**
The shipped policy and defaults are unchanged. The cache remains off by
default, predicate tasks still bypass population, and exact-capable filters are
still declared `Inexact` when the cache is enabled. The prototype is reachable
only through `QueryScanTuning::file_cache_predicate_key_prototype`; it has no
environment variable, CLI flag, chart value or operator field.

## Candidate

#4891 made whole-file entries reusable by declining to populate whenever a task
carries a converted predicate. #4905 tests the alternative: keep that predicate
on the reader and append its serialized bound form to the existing
`file:range:projection:deletes:direction` key. A predicate-free whole-task entry
remains a fallback. The residual `FilterExec` stays in every plan, so a hit,
miss and predicate-free fallback answer the same query.

Population still inserts only at end-of-stream. A clipped prefix is not a
complete entry and is dropped. The prototype does not change pushdown exactness
or `LIMIT` placement during planning: cache hits are not known then, and a
matching converted predicate does not prove that every residual expression was
enforced.

`crates/siglake-storage/tests/file_cache_predicate_key_prototype.rs` compares
hits, misses and fallback hits with a cache-disabled answer. It also proves that
a `LIMIT` which clips a task leaves no entry.

## Declared workload

The measurement is synthetic; none of its frequencies are production
observations. It writes four local Parquet files of 60,000 rows. Each predicate
returns 20 rows per file, so `LIMIT 100` exhausts all four tasks and makes every
miss eligible to populate. The 20-request trace contains:

- eight requests for one stable label;
- two requests each for three other labels;
- six unique moving 20-second windows, each paired with `host = 'bulk'` so the
  scan is non-order-preserving and reaches this cache path.

The trace therefore gives exact predicates a 50% request-repeat ceiling. The
four label values produce four keys per existing file/projection identity. The
six window literals produce six keys per identity under their projection. All
cache arms use 64 MiB and 16 entries, enough for four predicates across the
four files. Query-specific page and footer reads are warmed before the matched
arms.

Measured 2026-09-21 on this box, release build, `file://` warehouse. Times are
per-class p50 milliseconds. `post4891` is the shipped cache with predicate
population declined; `predicate` is the in-process prototype.

| arm | stable cold | stable repeat | varied cold | varied repeat | moving cold |
| --- | ---: | ---: | ---: | ---: | ---: |
| disabled | 6.63 | 5.41 | 5.51 | 5.41 | 6.01 |
| post4891 | 6.12 | 6.12 | 5.71 | 5.75 | 6.51 |
| predicate | 6.40 | **0.62** | 5.48 | **0.88** | 6.97 |

| arm | task hits / lookups | completed / inserted | evicted | resident entries | retained | reader fetched |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| disabled | — | — | — | 0 | 0 | 5.8 MiB |
| post4891 | 0 / 80 | 0 / 0 | 0 | 0 | 0 | 5.8 MiB |
| predicate | **40 / 80 (50%)** | 40 / 40 | **24** | 16 | 0.525 MiB | **2.9 MiB** |

The repeated stable predicate is 9.9x faster than `post4891`; repeats of the
three varied labels are 6.5x faster. Reader-fetched bytes fall in proportion to
the 50% task hit rate. Scan output stays 2.6 MiB in all arms because cached rows
still pass through the residual filter; the reduction is physical reader work,
not rows handed to DataFusion.

The entry budget, rather than bytes, binds first. Ten predicates over four file
identities create 40 complete entries. The 16 survivors retain 0.525 MiB, about
34 KiB each, while 24 entries (60% of all completed populations) are evicted.
The six moving windows have no repeat in the declared trace: they account for
24 inserts and all 24 evictions without producing a hit. The 40 candidates
would retain about 1.3 MiB at this selectivity, but consume 40 entry slots.

## Disposition: REJECT

Do not pursue unconditional predicate-keyed admission. Exact repeats are worth
serving once resident, but admitting every distinct predicate spends the scarce
entry currency on one-use window literals. Even this favorable trace groups
repeats before the moving windows and grants a 50% repeat ceiling; it still
evicts 60% of completed populations. Interleaving window movement with repeats
can only reduce that hit opportunity at the same entry cap.

A later proposal would need an admission rule that proves reuse before spending
an entry, such as second-touch admission, and would need its own measurement.
That is a different policy, not authorization to adopt this prototype. The
shipped #4891 bypass, filtering behavior and defaults stay as they are.

## Boundaries

This is one local synthetic fixture, two projection identities and highly
selective predicates. It covers no observed UI frequency, S3, distributed
execution, concurrent queries, positional deletes or raw/promoted prune paths.
It does not lift the `Inexact` downgrade or test an exact/LIMIT plan because
planning cannot distinguish a future hit from a miss or predicate-free fallback.

## Reproduce

```sh
cargo test -p siglake-storage --test file_cache_predicate_key_prototype
cargo test --release -p siglake-storage \
  --test predicate_keyed_cache_measurement -- --ignored --nocapture
```
