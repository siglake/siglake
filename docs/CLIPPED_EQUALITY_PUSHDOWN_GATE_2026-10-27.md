# Clipped equality-pushdown learned gate (#7513)

[#7500](CLIPPED_EQUALITY_PUSHDOWN_LAYOUTS_2026-10-26.md) established that
neither the exact residual nor file and page bounds alone decide whether a
clipped equality should reach the Parquet row filter. This experiment tests a
snapshot-local learned signal. It does not change production policy and adds
no setting or public metric.

The candidate keeps today's pushed predicate on a cold observation. That read
counts the rows presented to the Parquet predicate and the rows its Boolean
result accepts. A later execution with the same snapshot, predicate, projected
fields, clipped limit and reader batch size may omit the reader predicate when:

- page selection rejected zero rows; and
- at least one eighth of the predicate input survived.

The one-eighth threshold separates the four fixtures; it is not proposed as a
production constant. The experiment suppresses only the reader predicate.
DataFusion's exact `FilterExec` remains above the scan in every arm and returns
the authoritative answer.

## Signal source and cost

The predicate input is `RecordBatch::num_rows()` immediately before the
converted Iceberg predicate runs. Its output is `BooleanArray::true_count()`
immediately afterwards. The observer is installed in
`third_party/iceberg/src/arrow/reader/row_filter.rs`; it receives two `usize`
values per predicate evaluation. The fixture sums them with two relaxed atomic
increments. Production leaves the function pointer unset, so the reader does
no new accounting.

`rows_pruned_selection` remains the reader-setup counter in
`third_party/iceberg/src/arrow/reader/pipeline.rs`. The gate reads both signals
only after the cold scan settles. It never treats the scan leaf's emitted rows
as predicate output.

Signal acquisition is one useful current-policy query, not a separate sample
scan. The measured `observed` arm differs from the baseline only by the two
counter increments. Its paired median difference was -0.955 ms on
`interleaved`, +0.014 ms on `rare_scattered`, +0.903 ms on `rare_clustered`
and +1.205 ms on `rare_clustered_single_file`. The last fixture was noisy: the
seven baseline samples ranged from 11.790 to 18.424 ms and the observed samples
from 12.695 to 28.520 ms. These diagnostic-build timings do not resolve a
sub-millisecond counter cost.

## Cache and absent-signal rule

A production version would store the settled aggregate under all inputs that
can change either the observation or its value:

```text
(table UUID, snapshot ID, normalized bound predicate, projected field IDs,
 clipped limit, reader batch size)
```

An absent, incomplete or zero-input observation keeps today's predicate. A new
snapshot therefore starts cold. Cancellation before scan settlement must not
publish a cache entry. The entry has no time-to-live: snapshot identity is its
validity boundary.

Task #7525 added the missing internal counts and a local-only learned-cache
prototype. The reader already counted row groups rejected by statistics;
Iceberg planning now also counts live data files rejected by manifest metrics.
`rows_pruned_selection = 0` alone still says nothing about either earlier
stage, so the prototype suppresses only when all three pruning counts are zero.

## Matched local result

Seven rotating triples per layout used #7500's immutable fixtures, diagnostic
build, current factor-2 admission wave and cache bypass. `baseline` is today's
pushed predicate with the exact residual retained. `observed` adds only signal
counting. `gate` consumes an earlier observation. Missing-signal behavior is
the baseline itself and needs no separate arm.

| layout | predicate input | output | selection-pruned | decision | baseline | gate | paired gate - baseline |
| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| `interleaved` | 40,000 | 10,000 | 0 | suppress | 29.048 ms | 6.331 ms | -23.007 ms |
| `rare_scattered` | 80,000 | 160 | 0 | retain | 41.363 ms | 40.693 ms | -0.064 ms |
| `rare_clustered` | 81,920 | 1,200 | 63,680 | retain | 34.033 ms | 34.603 ms | +0.793 ms |
| `rare_clustered_single_file` | 20,480 | 1,200 | 19,520 | retain | 13.405 ms | 14.699 ms | +0.880 ms |

The common interleaved label crosses the threshold and saves 23.0 ms. The
other three decisions retain the baseline reader configuration and exact work;
their sub-millisecond paired differences are scheduling spread between
identical plans, not a gate-induced read change. Answers, projections, wrapper
shape, planned partitions and selection totals are asserted for every run.

No layout takes the harmful suppressed arm: `rare_scattered` retains the
predicate that saved 11.7 ms in #7500, and both clustered layouts retain file,
row-group and page pruning. A cold query also retains it. On these fixtures the
candidate therefore has no policy regression against the pinned baseline.

## Why the planning proxies were rejected

Manifest lower and upper bounds, page bounds and dictionary cardinality do not
separate `interleaved` from `rare_scattered`. Both have four dictionary values,
every file and page spans the target, and `rows_pruned_selection` is zero. Yet
their pushed-minus-suppressed signs differ by 32.5 ms. Iceberg `value_counts`
counts non-null values, not occurrences of the equality literal. Compressed
column size depends on encoding and placement and is not an exact frequency
signal.

Footer reads would add work before the clipped scan knows which file admission
will open. Page indexes still provide bounds rather than value frequencies.
None of these proxies can safely choose the suppressed arm for the two
non-prunable fixtures.

## Limits

- Local single-process diagnostic build only; no AWS or distributed run.
- One equality on one dictionary-encoded UTF-8 field and one projection.
- The learned result helps repeats only. First execution stays on current
  policy.
- The test threshold only brackets 5.86% and 25% survival. It does not locate a
  production break-even point.
- The bounded cache is an in-process qualification prototype. There is no
  production knob, public metric or shipped policy.

## #7525 cache qualification

The prototype holds at most 256 entries in insertion order. Entries have no
time-to-live. Its complete key is:

```text
(table UUID, snapshot ID, serialized normalized bound predicate,
 ordered projected Iceberg field IDs, clipped limit,
 resolved Parquet reader batch size)
```

The cold execution keeps the reader predicate and publishes only after every
scan partition settles. An error or request cancellation marks the observation
invalid; the last partition then discards it. A zero-input observation and a
scan whose key cannot be completed are also not published. Missing entries
keep current policy. The exact DataFusion `FilterExec` remains above the scan
on cold and warm executions.

The deterministic local test covers these cases:

| layout | manifest metrics pruned | row groups pruned | page rows pruned | repeat decision |
| --- | ---: | ---: | ---: | --- |
| common interleaved equality | 0 | 0 | 0 | suppress |
| one target-free file | >0 | 0 | 0 | retain |
| one target-free row group | 0 | >0 | 0 | retain |
| nullable UTF-8 equality | 0 | 0 | 0 | suppress, exact residual excludes nulls |

Appending creates a new snapshot-key miss for the otherwise identical query.
A separate unit check supplies nonzero predicate counts to an observation,
marks its partition cancelled, and verifies that no entry appears. The local
qualification passed in 5.78 seconds. It does not authorize production use;
the one-eighth threshold and deployment behavior remain unqualified.

## Reproduction

```sh
cargo test -p siglake-storage \
  --test clipped_equality_pushdown_layouts \
  clipped_equality_learned_gate_attribution \
  -- --ignored --nocapture

cargo test -p siglake-storage \
  --test clipped_equality_pushdown_layouts \
  learned_cache_requires_all_earlier_pruning_signals_and_snapshot_identity
```

The retained run was made on 2026-10-27 on the shared 32-core lane. It finished
in 223.54 seconds.
