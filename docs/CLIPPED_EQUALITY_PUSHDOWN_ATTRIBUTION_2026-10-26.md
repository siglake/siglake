# Clipped equality-pushdown attribution (#7457)

The [retained field result](CLIPPED_LIMIT_RESIDUAL_ATTRIBUTION_2026-10-26.md)
shows a collect-time difference, not a pushdown comparison.
`keyword_and_label` projects `region`; `keyword` and `substring_scan` do not.
This local measurement uses one equality query and one snapshot for every arm.

No AWS run was made. Production pushdown policy is unchanged.

## Matched fixture

The ignored `clipped_equality_reader_pushdown_attribution` test creates one
Iceberg snapshot with 15 Parquet files and 40,000 rows per file. Each file uses
the production writer properties: a 20,000-row page limit and a 1,048,576-row
row-group ceiling. `source` cycles through four values, so `us-east-2` selects
25% of the rows and every page contains matches.

Every arm runs:

```sql
SELECT timestamp, raw
FROM events
WHERE source = 'us-east-2'
LIMIT 100
```

The scan schema is `[timestamp, source, raw]` in all three arms. All three keep
the same `FilterExec`, clipped-limit coalescer, wave-2 admission, 8,192-row
batch size, range settings and cache bypass. Each result contains 100 distinct
rows satisfying the equality. The in-process-only control changes the reader:

1. `pushed`: reader predicate and page-index `RowSelection` enabled.
2. `no_selection`: reader predicate enabled, `RowSelection` disabled.
3. `suppressed`: reader predicate omitted; the unchanged residual filter is
   authoritative.

There is no setting, command-line flag or deployment value for these controls.

## Result

Seven triples ran in rotating order on the same snapshot. Times below are
medians of root-stream collection; differences are diagnostic-build local
measurements, not latency targets.

| arm | collect | files | leaf rows | decoded Arrow bytes | physical reads | index bytes | data bytes requested | rows pruned by selection |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| pushed | 30.878 ms | 1 | 8,192 | 1,229,144 | 4 | 1,264 | 29,413 | 0 |
| no selection | 26.982 ms | 1 | 8,192 | 1,229,144 | 4 | 1,264 | 29,413 | 0 |
| reader predicate suppressed | 7.077 ms | 1 | 8,192 | 2,326,872 | 3 | 1,264 | 29,413 | 0 |

The median paired differences are 4.587 ms for `pushed - no_selection`,
19.905 ms for `no_selection - suppressed`, and 23.643 ms for
`pushed - suppressed`.

The per-node `elapsed_compute` medians are small and do not contain the reader
wait:

| arm | clipped coalescer | residual filter | scan leaf |
| --- | ---: | ---: | ---: |
| pushed | 0.037 ms | 0.606 ms | 0.000016 ms |
| no selection | 0.035 ms | 0.601 ms | 0.000016 ms |
| reader predicate suppressed | 0.023 ms | 0.790 ms | 0.000016 ms |

The collection difference therefore sits inside the reader's asynchronous
open/decode path, outside DataFusion's node compute timers.

## Attribution

The third projected column is not the cause in this control. Every arm reads
`source` because the residual filter needs it; the suppressed arm keeps that
projection and is 4.4 times faster at the median.

Page-index `RowSelection` is a minority term. It rejects no rows on this
interleaved layout. Disabling only selection saves 4.587 ms paired-median while
leaving reads, index bytes, requested data bytes, emitted leaf rows and decoded
Arrow bytes unchanged. Column and offset indexes are preloaded by default in
both reader-predicate arms (`third_party/iceberg/src/arrow/reader/options.rs`).

Most of the cost is the Parquet row filter created from the pushed predicate.
The no-selection arm still spends 19.905 ms more than the predicate-suppressed
arm. At 25% selectivity, the reader must evaluate source rows until it can emit
an 8,192-row matching batch. The residual-only arm emits the first 8,192 source
rows and the filter above it can satisfy `LIMIT 100` from those rows. The pushed
arm reports fewer decoded Arrow bytes because the counter measures emitted
batches after the reader filter; it does not mean the predicate evaluated less
source data. Equal requested data bytes and zero selection pruning show no I/O
reduction on this layout.

This explains the local order-of-magnitude direction without assigning the
field result's exact 73.56 ms to the same cause. The field layout and this
fixture differ, and the retained field artifacts do not report reader-predicate
input rows.

## Pushdown decision boundary

For a common equality interleaved across pages, the clipped path should not
apply the reader predicate when the exact residual already remains above the
scan. It delays the first useful batch and saves neither a page nor a requested
data byte in this control.

This result does not justify disabling clipped equality pushdown for every
layout. A clustered or rare value can let row-group statistics or page
selection avoid data. A production policy needs a gate that preserves that
case, or a matched selective/clustered control showing that the residual-only
route also wins there. This investigation adds no public knob and makes no
production policy change.

## Reproduction

```sh
cargo test -p siglake-storage \
  --test clipped_equality_pushdown_attribution \
  clipped_equality_reader_pushdown_attribution \
  -- --ignored --nocapture
```

The deterministic assertions cover identical physical wrappers and
projections, retained residuals, exact answers, reader-predicate presence, and
the observed I/O/selection relationships. Wall time is printed and is not a CI
threshold.
