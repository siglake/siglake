# Compact group-count footer encoding

**Status:** shipped (2026-07-26); normalized to a single v1 encoding before
launch (2026-07-28). See `DESIGN_file_formats.md` for the versioning contract.

## What the footer is

Every data file carries a **group-count footer**: for each designated column,
that file's exact row count per distinct value, plus its NULL count. It is
stamped into the Parquet footer KV at write time by the vendored
`ParquetWriter` (`third_party/iceberg/src/writer/file_writer/parquet_writer.rs`),
accumulated per output file so `Σ values + nulls == the file's row count` — the
invariant the read path's validity guard checks before trusting a summary.

It is the basis of the whole dimensional-count tier: `GROUP BY <col> COUNT(*)`,
`count(*) WHERE col = 'v'` (and `!=`, `IN`, integer ranges — the typed-count
fast path), and the windowed variants all sum footers instead of scanning rows.
A file with no usable footer is simply scanned, so the footer is a pure
accelerator: losing it costs latency, never correctness.

## Why not JSON

The obvious encoding is one JSON blob per file:

```json
{"columns":{"host":{"values":{"web-0001.prod.example.com":42,…},"nulls":0}}}
```

Per column that is capped at `MAX_GROUP_COUNT_CARDINALITY` (1024) distinct
values, but a file summarizes every promoted/dimensional column at once, and on
high-cardinality columns (hosts, URLs, paths) each entry is a long string plus a
decimal count plus punctuation. Real footers reached multiple MB.

Two costs follow, and both land on the *cold* merge — the once-per-snapshot pass
that merges every live file's footer into the answer:

- **Fetch.** The blob rides inside the Parquet footer, so it is not an optional
  read: every footer load pulls the whole thing over the object store.
- **Parse.** Every footer read materialized **all** columns into
  `BTreeMap<String, u64>`s and then cloned the one column the query wanted into
  a `Vec` — so a query for `host` paid for `url_path`, `trace_id`, `level` and
  the rest, twice over.

Measured at the top of the launch benchmark, cold `top_hosts` was ~6.8s
once-per-snapshot (hidden operationally by the 30s `warm_group_counts` cycle,
which pays it in the background — but it was still real work, and it bounded how
fast a fresh snapshot became cheap).

One thing worth recording, because it redirected the design: **JSON tokenizing
was never the parse cost.** Swapping the encoding alone, keeping the
decode-everything-into-maps shape, measured *1.0×* — `serde_json` is fast, and
the time was going into map construction and string allocation. The parse win
had to come from decoding less, not from decoding faster.

## The format

Same information, compact binary form, under `siglake.group_counts.v1`. Spec and
implementation: `crates/siglake-bloom/src/group_counts.rs`. The versioning
contract it follows is `DESIGN_file_formats.md`.

```text
"LGCF" | version:u8 | codec:u8 | payload   (base64'd — Parquet KV values are Strings)

body := uvarint n_columns
        repeat: uvarint name_len, name bytes
                uvarint nulls
                uvarint n_values
                  repeat: uvarint shared_prefix_len   (with the previous value)
                          uvarint suffix_len, suffix bytes
                          uvarint count
```

Three things shrink it:

1. **Front coding.** Values are stored in sorted order (they come out of a
   `BTreeMap`), so each value keeps only its delta from its predecessor. This
   targets exactly the columns that got big — hostnames, paths, URLs and IPs
   share long prefixes with their neighbours once sorted.
2. **Varints.** A count costs one byte in the common case instead of a decimal
   rendering plus quotes, colon and comma.
3. **zstd** over the result (level 3), and only when it actually shrinks the
   body — the header byte records which, so a small footer never pays a
   decompression step it didn't need.

And the read API is what makes it *faster*, not just smaller. The format is a
single forward pass, so `decode_column(blob, "host")` walks past every other
column's bytes without allocating — no map, no strings for columns nobody
asked about — and returns values in their stored (sorted) order, which is
already the shape the read path wants. `decode_column_names` skips values
entirely for the warm cycle's census.

## Measured

A representative wide footer — 8 columns, three of them at the per-file cap of
1024 distinct values (hostnames, URL paths, trace ids), the rest
low-cardinality — decoding the `host` column, release build:

| | JSON | compact |
|---|---|---|
| footer bytes | 133,506 | 14,548 (**9.2× smaller**) |
| decode one column | 553 µs | 104 µs (**5.3× faster**) |

Reproduce with the (ignored) scaffold:
`cargo test --release -p siglake-storage --lib report_footer_encoding -- --ignored --nocapture`.

Note the *whole-blob* decode (`FileGroupCounts::from_compact`, ~600 µs here) is
not faster than JSON — building the maps is the cost either way. That path is
only the general-purpose seam; the read path uses the targeted decoders.

## Compatibility

- There is exactly **one on-disk encoding**. An earlier JSON encoding existed
  during development and was deleted before v0.1.0 along with its fallback
  path — it never shipped, so nothing had to stay compatible with it.
- Both readers in `crates/siglake-storage/src/iceberg.rs` —
  `footer_column_counts` (one column, the hot path) and
  `footer_group_count_columns` (the warm cycle's census) — decode this format
  only. A future encoding is a version bump per `DESIGN_file_formats.md`.
- Anything undecodable — corrupt blob, foreign blob, truncation, trailing
  bytes, or a payload from a FUTURE version — returns `None`, which every caller
  already handles as "scan this file instead". The format fails closed by
  construction.
- The **snapshot-side** aggregates object (`metadata/siglake-agg/<table-uuid>/siglake-aggregates.json`,
  holding the table-level `FileGroupCounts` + time aggregates) is unchanged and
  still JSON. It is one object per table, capped at
  `TABLE_GROUP_COUNT_CARDINALITY` (4096) values per column — orders of magnitude
  smaller than the per-file footers summed across a snapshot, and not on the
  cold path this change targets. Same for the per-file time-bucket footer
  (`siglake.time_buckets.v1`), whose size is a file's span in minutes.

## Tests

- `crates/siglake-bloom/src/group_counts.rs` — round-trip (incl. empty values,
  unicode, prefix-of-successor, `u64::MAX` counts), rejection of garbage /
  wrong magic / unknown codec / truncation / trailing bytes, a proptest
  round-trip, a proptest that arbitrary input never panics, and a size
  regression guard pinning the compact blob under ⅛ of an equivalent JSON
  rendering on realistic hostnames. The targeted decoders are pinned against the full
  decode, including the case that matters most — reading a column that sits
  *after* a fat one, which is only correct if the skip is byte-exact.
- `crates/siglake-storage/src/iceberg.rs` (`footer_group_counts_tests`) — the
  encoding seam against real Parquet footers, for both read paths: the footer
  reads, an unreadable one (garbage, empty, or a FUTURE version) degrades to a
  scan rather than to a wrong count, an uncovered column is no summary, and no
  footer is no summary.
- The existing recluster/fast-path suites exercise it end-to-end: they assert
  the group-count tier still serves after a re-cluster, which only holds if the
  writer stamps and the reader parses.
