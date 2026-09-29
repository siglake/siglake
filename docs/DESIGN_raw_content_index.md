# Design: raw-content index for cheap `raw LIKE` wide scans

Status: **proposal** (the real query lever found in rounds 51–52). Not yet
implemented. Author: night session 2026-05-30.

## Problem (measured)

The remaining expensive query is the uncached, wide-window analytical scan
that filters on `raw` content, e.g. `count(*) WHERE <time-window> AND raw
LIKE '% 500 %'`. Cost decomposition on the 102M-row warehouse:

| Query shape | cols decoded | decoded/partition | latency |
|---|---|---:|---:|
| time-window count (no raw) | — (metadata fast path) | 0 | ~0 |
| time-window + `sourcetype=` | timestamp, sourcetype | ~81 MB | ~1.0 s |
| time-window + `raw LIKE` | timestamp, raw | ~840 MB | ~2.0 s |

So materializing `raw` adds ~1.0 s / ~5 GB decoded for a 1-day window. Round
52 proved this is **Arrow string materialization, not decompression** (LZ4 vs
ZSTD-3 were equal). `LIKE`/substring is not an Iceberg predicate, so it is
applied in DataFusion's FilterExec *above* the scan — every row's `raw` is
decoded even though the predicate is highly selective (status-500 ≈ 0.6%).

There is **no cheap codec/knob fix**. The only way to make this fast is to
**avoid decoding `raw`** for row groups that cannot match.

## Proposal: per-row-group token bloom filter on `raw`

### Write side (compactor, `siglake-storage`)
1. At Parquet write time, tokenize each row's `raw` (split on
   non-alphanumeric, lowercase, cap token length, cap tokens/row) and add the
   tokens to a **per-row-group bloom filter** stored on a derived hidden
   column (or as Parquet column bloom on a `raw_tokens` column). The compactor
   is S3-PUT wire-bound, so the extra tokenization CPU overlaps the upload at
   ~no wall-clock cost (rounds 18–20).
2. Tunable: tokens/row cap, min token length, bloom FPP — mirror the existing
   `bloom_ndv_for` / `BLOOM_FPP` machinery already in `siglake_writer_properties`.

### Query side (`siglake-query-server` / `query_provider`)
3. Detect `raw LIKE '%TOKEN%'` where `TOKEN` is a single whitespace-delimited
   term (the common log-search case). Lower it to a bloom probe: skip any row
   group whose token bloom does not contain `TOKEN`. Multi-term `AND` → require
   all; `OR` → union. Substrings that span token boundaries or contain
   wildcards fall back to the current full decode (correctness preserved — the
   bloom only *prunes provable non-matches*).
4. This rides the row-group pruning path that already works for the timestamp
   predicate (round 51), so a status-500 query would decode only the row
   groups that actually contain "500" instead of the whole window.

### Expected effect
For a 0.6%-selective term over a 1-day window, decode drops from ~5 GB to the
handful of row groups containing the term — i.e. the ~1.0 s raw-materialization
cost collapses toward the narrow-column (~1.0 s → ~0.1 s) regime, bounded by
bloom FPP. Correctness is exact (bloom false positives only cause extra decode,
never wrong results).

## Cost / risk
- Storage: +1 small bloom per row group per file (KBs). Modest.
- Write CPU: tokenization in the compactor — overlaps the wire-bound PUT.
- Scope: a real feature (write-side tokenization + bloom, query-side
  predicate lowering + row-group skip), ~1 focused phase. Bounded and testable.
- Limitation: only word-boundary terms prune; arbitrary substrings/regex fall
  back to full scan. An ngram index would generalize this at higher cost —
  out of scope for v1.

## Feasibility (investigated 2026-05-30) — iceberg-rust 0.9 constrains the read side

iceberg-rust 0.9.1's `ArrowReader` (registry source, `src/arrow/reader.rs`):
- `row_group_filtering_enabled` (default true) prunes row groups by column
  **min/max stats**; `row_selection_enabled` (default false) does page-index
  selection. **There is NO parquet bloom-filter support** — `grep bloom` in
  `iceberg-0.9.1/src/arrow/` is empty.
- Pruning is driven by the `FileScanTask` **Iceberg `Predicate`**, which
  supports `starts_with` (prefix `LIKE 'x%'`) but NOT substring/`contains`.

So a token bloom cannot ride iceberg-rust's reader, and substring `LIKE`
cannot be expressed as an Iceberg predicate. Two viable implementation paths,
both touching core paths and therefore **feature-flagged + heavily validated,
not an unattended change**:

1. **File-level token bloom (lower risk, coarser).** Write a per-file token
   bloom into the Parquet file's key-value metadata at compaction; in Siglake's
   custom scan (`SiglakeIcebergTableScan`), after `plan_files`, read each
   `FileScanTask` file's footer metadata bloom and DROP tasks whose bloom
   lacks the query term before handing the surviving tasks to iceberg-rust's
   reader. No schema change, no read-path rewrite. Open question: whether
   iceberg-rust 0.9's `ParquetWriter` exposes `append_key_value_metadata` at
   close (the bloom is only known after the data is written). If not, store
   the bloom as an Iceberg file property / sidecar.
2. **Row-group-level (finer, bigger).** Bypass iceberg-rust's `ArrowReader` in
   the custom scan and read Parquet directly via the `parquet` crate
   (`ParquetRecordBatchReaderBuilder` supports bloom filters +
   `with_row_groups(...)` selection). Siglake takes over the read path for
   raw-LIKE queries (behind a flag); reuse iceberg-rust for everything else.

Recommended: prototype path 1 first (verify the writer metadata hook), keep it
behind `query.rawTokenBloom.enabled=false`, validate exactness (bloom false
positives only cause extra reads, never wrong results), then consider path 2.

### Status this session
Foundational, path-independent piece implemented + unit-tested: the tokenizer
+ bloom build/probe helper (`raw_token_bloom`), off the hot path, with no
schema or read-path change yet. The core-path integration (1 or 2 above) is a
dedicated, feature-flagged follow-up — too risky to land unattended.

## Why this is THE lever
Rounds 33–49 exhausted scan-concurrency knobs; round 51 showed projection and
page-pruning already work; round 52 showed codec is irrelevant. The only
unexploited structural lever for uncached `raw`-content queries is to not
decode `raw` you don't need — which requires a content index. This is it.
