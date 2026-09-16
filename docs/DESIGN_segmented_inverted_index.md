# Design — row-group-addressable inverted-index sidecars (#4376 prototype)

Status (2026-09-16): **prototype, nothing wired.** The codec and its reader are
`siglake_index::segmented`; the writer, the scan path and every default are
untouched. This document is the format decision #4377 needs ahead of building
postings during a streaming merge, and the specification the remaining slices
implement: #4560 (the codec and its fixtures), #4561 (reader integration and
bounded partial reads), #4562 (the measured proceed/revise/reject disposition).

It exists because the shipped format has one property that cannot be fixed by
sizing a cache: **it is only readable whole.**

## What the shipped format costs

`InvertedIndex::from_bytes` builds a `BTreeMap<String, Vec<u32>>` over the
file's whole dictionary before the first lookup. On the measurement corpus one
token per row is unique to that row — the row ordinal in the text, which stands
for the request ids, trace ids and timestamps real logs carry — so the
dictionary has as many entries as the file has rows. Measured on one 7,340,000
row file (see [Measurements](#measurements)): 116.5 MiB serialized, **526.0 MiB
parsed**, 3.31 s to parse.

A 14-file text plan therefore wants 7.83 GB of parsed index. #4329 measured what
a 1 GiB budget does with that: one resident index, 388 evictions, and the
0.001%-density rare scan the index exists for running 19x slower than the scan
it was supposed to beat. With the whole 7.83 GB resident it wins by 13x. Both
facts are the same fact — the win needs residency, and residency needs a 4 GiB
query pod to hold 7.83 GB.

Cache sizing cannot settle that. Reading in part can.

## The layout

```text
offset 0   [magic "KIDS"][version u8]     header — for offline identification
           group 0 postings section        per-term delta-varint lists, group-relative
           group 0 dictionary blocks       sorted, prefix-compressed, ~4 KiB each
           group 1 postings section
           group 1 dictionary blocks
           ...
           directory                       one range read
EOF-25     trailer                         fixed 25 bytes
```

**Trailer** (fixed width, read first, at a known offset from EOF):

| field | width |
|---|---|
| `dir_offset` | `u64` LE |
| `dir_len` | `u64` LE |
| `dir_crc` | `u32` LE |
| `version` | `u8` |
| magic `KIDS` | 4 bytes |

**Directory** (`n_rows`, `n_groups`, then per group):

| field | encoding | why |
|---|---|---|
| `first_row`, `n_rows` | varint | the group's slice of the file's ordinal space |
| `dict_offset`, `dict_len` | varint | the group's dictionary, absolute |
| `postings_offset`, `postings_len` | varint | the group's postings, absolute |
| `n_blocks` | varint | |
| per block: `first_term` | varint length + bytes | selects one block without reading any |
| per block: `offset`, `len` | varint, relative to `dict_offset` | the block's byte range |
| per block: `postings_base` | varint, relative to `postings_offset` | the block's first term's postings |
| per block: `crc` | `u32` LE | see [Integrity](#integrity) |

**Dictionary block**: `n_terms`, then per term `shared_prefix_len`,
`suffix_len`, suffix bytes, `df`, `postings_len`. Terms ascend, so the shared
prefix is with the previous term in the block and the first term is written in
full. A term's postings start at `postings_base` plus the lengths of the terms
before it in the block — so **one block read gives an exact byte range for any
term the block holds**, with no second directory hop.

**Postings**: per term, LEB128 deltas over group-relative ordinals, the first
delta from 0. Identical to v1's posting encoding with a group-relative origin.

### What one lookup costs

A point lookup (`match_terms`, a normalizable `LIKE '%term%'`) is, per row group
the caller did not already prune: one dictionary-block read, plus one postings
read where the group has the term. Binary search over the directory's
`first_term` values picks the block; a term sorting before a group's first term
costs no read at all. Measured: **14 range reads and 9.8 KiB for a rare term
over a 7-row-group, 85.8 MiB blob** — 0.011% of it.

A substring sweep (`rows_containing`, the non-tokenizable `LIKE` case) is the
opposite: finding every dictionary term that *contains* a substring means
reading every block. That is bounded by the dictionary (35.9 MiB of an 85.8 MiB
blob here), not the postings, and it is not a partial read in any useful sense.
The format supports it; a reader should treat it as a distinct regime and is
free to decline it, exactly as #4375's policy declines clipped shapes.

### Sizing the blocks

The block target trades the resident directory against the bytes one lookup
fetches, and both directions are pinned by a test
(`block_size_trades_resident_directory_against_bytes_per_lookup`). At the 4 KiB
default, one 7.34M-row file's directory is 474.9 KiB — against 526.0 MiB parsed
for the same index.

## Versioning and discovery

Four separate discriminators, so no reader ever has to guess:

1. **Magic + version at both ends.** `KIDS` (v1 is `KIDX`), version byte in the
   trailer and in the header. A reader that does not know the version refuses
   the blob and the caller scans. Each decoder refuses the other's bytes:
   `a_v1_blob_and_a_segmented_blob_are_not_confusable`.
2. **Its own Puffin blob type**, `siglake-inverted-seg-v1`. The shipped reader
   matches `blob_type == "siglake-inverted-v1"` exactly
   (`third_party/iceberg/src/arrow/reader.rs`), so it skips a segmented sidecar
   and takes the scan path — the same path it takes for an unindexed file.
3. **Its own footer-KV key**, `siglake.inverted_index.seg1`, with the same
   per-column suffixing rule `inverted_index_kv_key` uses.
4. **Its own `format` property** on the registered blob, `seg1`, beside the `v1`
   the shipped writer stamps.

A table may carry both kinds at once, per file, with no migration and no
in-place conversion: the two keys and two blob types do not collide, and a file
with neither is scanned. That is the compatibility rule — **the format is
per-file metadata, never table state.**

Nothing here touches the Parquet or Iceberg v2 contract. A segmented sidecar is
a Puffin blob registered as a statistics file exactly as the v1 sidecar is, or a
footer-KV value on a data file; row ordinals remain file-physical and the
`RowSelection` bridge is unchanged.

## Row-group addressing and ordinals

Group `i` of the sidecar **is** Parquet row group `i`. Two things follow.

**The reader can reject.** `postings_in_groups(term, Some(&groups))` restricts a
lookup to the row groups a scan kept after partition, statistics and bloom
pruning — the reject path costs no read. On the corpus above, the
`rare_scan_last25` shape (a window predicate leaving the last quarter of the
file) drops from 14 reads to 4.

**The row domain is checkable per group.** The directory states every group's
row count, so `matches_row_groups(&parquet_row_counts)` compares the sidecar
against the file's actual row groups before it prunes anything. The shipped path
can only compare one stamped `row_group_size` against the file's groups
(`stamped_row_group_size_matches`), which accepts a sidecar written for a
different layout as long as the sizes happen to agree. This is the same class of
check #4558 is adding for the v1 reader, available here from the directory
rather than from a property.

Ordinals are stored group-relative and returned file-physical (`first_row +
delta`), so callers see exactly the ordinal space `InvertedIndex` returns today.
Nothing downstream re-maps, and a posting list is still decodable on its own.

## Integrity

Partial reads change the failure mode, and this is the part of the design that
is not a performance question.

A whole-file decoder either parses a blob or it does not. A partial reader asks
a question per row group, so a corrupt dictionary block answers **"this group
does not have the term"** — which is not an error, it is a wrong answer that
silently drops rows. The index only ever produces a superset row selection, so
extra rows are harmless (the exact predicate is re-evaluated above the scan) but
missing rows are lost matches.

Three mechanisms, in the order they fire:

- **A CRC per dictionary block**, in the directory. A lookup reads the whole
  block anyway, so verifying costs no extra bytes; a mismatch makes the lookup
  *unanswerable*, not *absent*.
- **The directory's own CRC**, in the trailer — a flipped `first_term` would
  otherwise send a lookup to the wrong block.
- **The document frequency**, cross-checked against the postings decoded for a
  term, beside strict ascent and the group's row domain.

And a third outcome in the API: `Lookup::{Rows, Absent, Unanswerable}`, where
the v1 decoder returns `None` for both "absent" and "unparseable". Only
`Unanswerable` may fall back to a scan; only `Absent` licenses skipping rows.
The AND and substring entry points return `None` for "cannot answer" and
`Some(vec![])` only for a definitive no-match.

Exhaustive single-bit sweep, every bit of every byte, asking one present term
for its rows (`report_single_bit_corruption_rates`):

| format | blob | flips | refused | unchanged answer | wrong rows | outside row domain | reported absent |
|---|---:|---:|---:|---:|---:|---:|---:|
| whole-file v1 | 14,038 | 112,304 | 28,050 | 83,977 | 142 | 22 | 135 |
| segmented | 11,186 | 89,488 | 45,000 | 44,368 | 120 | 0 | 0 |

The residual is the 120 (0.13% of flips): a bit flip inside a posting delta that
leaves the varint count intact yields a different, structurally valid row set.
The v1 format has the same exposure at the same rate, so this is not a
regression — but a segmented reader could close it with a CRC per posting
section, at 4 bytes per term, which on this corpus is ~4 bytes per row and about
a third of the blob. That is not worth it for a superset selection whose rows
are re-checked above the scan; it is the trade to revisit if postings ever feed
an answer directly. The `outside row domain` and `reported absent` columns are
the two classes this design removes outright.

## Publication semantics (what #4377 needs)

The whole layout is arranged so a merge can write it in **one forward pass**:

- Postings and dictionary for group `i` are written when group `i` closes, and
  nothing already written is patched. The directory and trailer come last.
- Peak writer memory is one row group's postings plus its dictionary entries —
  not the file's. `SegmentedWriter::push_group_index` takes an index built over
  exactly one group's rows, which is what a merge already has in hand when it
  flushes a row group.
- The blob is complete only once the trailer lands, so a partial upload is not
  mistakable for an index: no trailer, no magic, no reader.

For registration, a segmented sidecar should carry the properties the v1 one
does (`data_file`, `column`, `tokenizer`) plus `format: seg1`, and should **not**
carry `row_group_size`: the directory states every group's row count, so the
reader validates against the Parquet metadata itself. A merge that emits row
groups of unequal size is then representable, which the stamped-size check
cannot express.

One consequence for Puffin: the shipped sidecar is added with
`PuffinCompressionCodec::Zstd` (`crates/siglake-storage/src/iceberg.rs`), and
`PuffinReader::blob` reads `offset..offset+length` and decompresses the whole
thing. **A segmented blob must be registered uncompressed**, and the reader
needs a sub-range read against the statistics file rather than
`PuffinReader::blob`. The cost of that is visible in the measurement: 85.8 MiB
uncompressed against the ~16 MiB per file the Zstd'd v1 sidecar occupies on disk
(#4329: 229 MB of statistics increment over 14 files). The prototype leaves
sections uncompressed and states the trade rather than hiding it; per-section
compression is a later version's concern, and the byte ranges the directory
addresses do not change shape when a codec byte arrives.

## Measurements

Release build, this box, local instrumentation only. The corpus is
`puffin_rebuild.rs`'s measurement text (`ab_shaped_event`: shared tokens, a
2%-density `queen`, a `checkout` on every twentieth row, one unique token per
row) with `rareneedle` on one row in 100,000 — the same generator the #4329 and
#4375 tables were taken on. One file is 7,340,000 rows in 7 row groups of
1,048,576, which is the compacted layout's default row-group target
(`target_row_group_rows`). Nine executions per shape for the per-file section,
three for the plan section, medians reported.

Every arm's rows are asserted equal to the whole-file index's answer, restricted
to the row groups the shape kept, before any timing is reported.

### One file

MEASUREMENT_ONE_FILE

### Per shape, one file, index work only

The `v1 warm` column is a lookup on an **already parsed** index — the state a
14-file plan cannot hold. It is the cost segmented reads have to be compared
against *when the cache wins*, and it is where v1 is better: a `BTreeMap` hit is
hundreds of nanoseconds, a block read plus a posting read is tens of
microseconds.

MEASUREMENT_SHAPES

### A 14-file plan under the deployed 1 GiB parsed-index budget

Fourteen logical files behind one byte-bounded LRU, which is the working set
#4329 measured (7.83 GB against 1 GiB, 388 evictions, one resident index). Every
logical file carries the same blob bytes and differs only in its cache key: this
sizes the cache pressure without building fourteen distinct corpora, and the
parse on every miss is real work.

The first execution of each shape opens fourteen readers — a trailer and a
directory read each — and the two after it reuse them, so the `seg fetched`
column is a three-execution average with the cold directory reads amortized
into it. The 474.9 KiB directory dominates it for every shape except the
substring sweep.

Only one of the two deployed budgets appears here. The 1 GiB parsed-index cache
is what holds a decoded index, and the 256 MiB Puffin-blob cache holds the
compressed blob bytes a decode was fed. **A segmented reader caches neither**:
it holds a directory and range-reads the rest, so the blob budget has no role on
this path. That is a difference in kind between the arms and #4562's
configuration should say so rather than applying both budgets to all three.

MEASUREMENT_PLAN

### What these numbers are, and are not

- **Are:** the index-side cost of a text predicate — dictionary and posting
  bytes, range reads, parse and lookup time, cache behaviour at the deployed
  budget, and exact-row equality with the shipped index.
- **Are not:** end-to-end query latency. The scan-side term (opening Parquet,
  decoding the selected rows, `FilterExec` above it) is absent, and so is the
  query server, distribution and object storage. #4329's OFF column is the
  comparison those need: 1,671.8 ms cold / 1,801.2 ms p50 for the 14-file
  `rare_scan`. This harness says the shipped index spends **far more than that
  budget on the index alone**, and that the segmented reader spends a fraction
  of a millisecond; it does not say what the whole query costs.
- **Are not** an object-store measurement. `SliceSource` counts what the reader
  *asks for* — 14 reads of 9.8 KiB is 14 GETs against S3, where the shipped path
  issues one GET of ~16 MiB and decompresses it. Whether many small ranges beat
  one large one depends on per-request latency and is a question for a prepared
  round, after #4561 has code to validate. Nothing here qualifies an AWS result.
- **Are not** a writer-cost benchmark. The build columns are sequential local
  fixture construction, one arm after the other.

## What the acceptance still needs

#4376's acceptance — "the rare full scan beats OFF under a 1 GiB parsed /
256 MiB blob budget while the four LIMIT shapes are not regressed" — is an
end-to-end statement about the query path and cannot be settled by a codec. What
this prototype settles is that the format can be read in part, what one lookup
costs in reads and bytes, and that the resident working set falls by ~510x.
What remains, in order:

1. **#4561** — a sub-range read against a Puffin statistics file (not
   `PuffinReader::blob`), the reader's `RawPruneSpec` path taking a segmented
   sidecar when one is registered, the row-domain check against Parquet
   metadata, and legacy/segmented/absent mixtures all landing on exact results.
2. **#4562** — the six-shape harness with a third arm, cold and warm separately,
   under 1 GiB parsed / 256 MiB blob, plus the OFF control; that is where a
   proceed/revise/reject disposition for #4377 comes from.
3. **An open question for #4561**: the substring sweep reads the whole
   dictionary, and `keyword`-class terms with millions of postings read megabytes
   of posting bytes. Both are regimes where partial reads buy little, and #4375's
   per-execution policy is the place to decline them. The document frequency a
   policy would want is now in the directory — a 474.9 KiB read per file — which
   is the cheapest selectivity estimate this design makes available and did not
   exist before it.

## Reproduce

```
cargo test -p siglake-index --release --test segmented_measure \
  report_segmented_vs_whole_file -- --ignored --nocapture
cargo test -p siglake-index --release --test segmented_measure \
  report_single_bit_corruption_rates -- --ignored --nocapture
```

Sized by `SIGLAKE_SEG_ROWS_PER_FILE` (7,340,000 above), `SIGLAKE_SEG_GROUP_ROWS`
(1,048,576), `SIGLAKE_SEG_FILES` (14), `SIGLAKE_SEG_RUNS` (9),
`SIGLAKE_SEG_PLAN_RUNS` (3), `SIGLAKE_SEG_RARE_EVERY` (100,000),
`SIGLAKE_SEG_PARSED_BYTES` (1 GiB) and `SIGLAKE_SEG_BLOCK_BYTES` (4,096). The
defaults in the file are a tenth of the size, so an unparameterized run finishes
in a minute.

The codec's own fixtures run in the crate's normal test pass
(`cargo test -p siglake-index`): v1 equivalence term by term, group-straddling
ordinals, empty and partial groups, the reject path's read count, malformed
blocks, truncated and corrupt blobs, the two formats' mutual refusal, unknown
versions, the block-size trade, and per-group construction from an existing
index.
