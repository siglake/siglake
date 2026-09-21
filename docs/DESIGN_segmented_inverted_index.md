# Design — row-group-addressable inverted-index sidecars (#4376 prototype)

Status (2026-09-18): **seg2 streaming writer landed on its hermetic suite and
its 14 x 7.34M-row build cost is measured; production read/write adoption
remains off by default.** The same-snapshot registration and concurrent-commit
sequence boundaries are closed (#5228, #5260, #5298; see
[Writer integration](#writer-integration-4377)).
The query-path report now builds its segmented arm through that writer and
retains the older seg1 tables as dated history (#5230). Production discovery
recognizes seg2 only; seg1 remains a codec fixture.
The codec and its reader are `siglake_index::segmented`, and the reader
integration (#4561) is behind `SIGLAKE_SEGMENTED_INDEX_READS` — see
[Reader integration](#reader-integration-4561). A streaming re-cluster emits
seg2 only under `SIGLAKE_SEGMENTED_INDEX_WRITES=1`, registering the sidecar in
the rewrite transaction; with both opt-ins unset every default behaves exactly
as it did. This document records the format decision and implementation:
#4560 (the codec, its fixtures and the format's
open questions — settled below), #4561 (reader integration and bounded partial
reads — below), #5006 (the directory held between lookups — below), #4562 (the
measured proceed/revise/reject disposition), and #4988 (block compression plus
posting-span integrity), followed by #4377's writer and atomic publication.

It exists because the shipped format has one property that cannot be fixed by
sizing a cache: **it is only readable whole.**

## What the shipped format costs

`InvertedIndex::from_bytes` builds a `BTreeMap<String, Vec<u32>>` over the
file's whole dictionary before the first lookup. On the measurement corpus one
token per row is unique to that row — the row ordinal in the text, which stands
for the request ids, trace ids and timestamps real logs carry — so the
dictionary has as many entries as the file has rows. Measured on one 7,340,000
row file (see [Measurements](#measurements)): 116.5 MiB serialized, **526.0 MiB
parsed**, 3.27 s to parse.

A 14-file text plan therefore wants **7.19 GiB** of parsed index on this corpus
— #4329 measured 7,830,305,508 bytes on its own fourteen files, and the figure
below is the same quantity re-derived from this one. #4329 measured what a 1 GiB
budget does with that: one resident index, 388 evictions, and the
0.001%-density rare scan the index exists for running 19x slower than the scan
it was supposed to beat. With the whole working set resident it wins by 13x.
Both facts are the same fact — the win needs residency, and residency needs a
query pod sized to hold gigabytes of index.

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

The directory is not just a set of offsets a reader trusts. `open` refuses one
whose sections do not **tile the body exactly** — group `i`'s postings, then its
dictionary, in group order, from the end of the header to the start of the
directory — and one whose block `postings_base` values do not start at 0,
strictly ascend and end before their section does. Both checks exist because a
range bounded only against `dir_offset` can overlap another section, and a
block is bounded only by its own group's `dict_len`: an inflated one would let a
lookup read a neighbouring group's postings as a dictionary block. Twenty
fixtures cover the class — fifteen structural mutations and five hand-written
directory bodies — each re-encoded with a recomputed `dir_crc` so it reaches the
parser rather than stopping at the checksum
(`a_directory_that_is_consistent_and_lies_about_the_structure_is_refused`).

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
default, one 7.34M-row file's directory is 474.9 KiB on disk and 1.0 MiB once
parsed — against 526.0 MiB parsed for the same index. Those two directory
figures are the same structure at rest and in RAM; the resident comparison
throughout this document uses the 1.0 MiB one, and the fetched-bytes columns use
the 474.9 KiB one, because that is what a cold open reads.

## Versioning and discovery

Four separate discriminators, so no reader ever has to guess:

1. **Magic + version at both ends.** `KIDS` (v1 is `KIDX`), version byte in the
   trailer and in the header. A reader that does not know the version refuses
   the blob and the caller scans. Each decoder refuses the other's bytes:
   `a_v1_blob_and_a_segmented_blob_are_not_confusable`.
2. **Its own Puffin blob type**, `siglake-inverted-seg-v1`. This was the
   prototype discovery name.
3. **Its own footer-KV key**, `siglake.inverted_index.seg1`, with the same
   per-column suffixing rule `inverted_index_kv_key` uses.
4. **Its own `format` property** on the registered blob, `seg1`, beside the `v1`
   the shipped writer stamps.

Seg2 repeats the external discriminators as
`siglake-inverted-seg-v2`, `siglake.inverted_index.seg2` and `format: seg2`,
with version 2 in its header and trailer. `SegmentedReader` decodes both byte
layouts once handed a range source. Production discovery selects only the seg2
blob type. Seg1 discovery was retired before 0.2.0 (#5230): no released reader
promised it and no production writer emitted it. Its pinned codec fixture still
decodes the historical bytes, while a registered seg1 blob is ignored by query
selection and does not suppress a whole-file v1 rebuild.

A table may carry v1, retired seg1 and seg2 metadata at once because their keys
and blob types do not collide. The production reader uses v1 or seg2 and scans
a file with neither; seg1 metadata alone is treated as unindexed. The format is
per-file metadata, never table state.

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

And a third outcome in the API, `Lookup::{Rows, Absent, Unanswerable}`, whose
contract #4561 is written against:

- **`Rows`** is strictly ascending, file-physical, and covers every row group
  the lookup was allowed to read. The caller may skip every row not in it.
- **`Absent`** is a definitive no-match over the groups the lookup covered. The
  term normalizes, every kept group was read, none has it.
- **`Unanswerable`** is "this index concluded nothing; scan". It is never
  partial: a lookup that found rows in one group and could not read another
  returns `Unanswerable`, not the rows it managed to get.

Three things land in `Unanswerable` that a reader has to plan for. A term that
does not normalize, where v1's `postings` returns `None` and its
`matching_rows_all` then reads that as "no rows match" — a contract hazard the
shipped path does not reach, because `extract_match_udf_prune` fills
`RawPruneSpec` from tokenizer output and every token it emits is at least
`MIN_TOKEN_LEN` and already folded, so it normalizes by construction. A
malformed section or a failed range read. And a **row-group selection this
sidecar cannot serve**: the
indices must be strictly ascending and in range, because an out-of-range index
means the caller's row-group map and the sidecar disagree, and a repeated one
would return a group's postings twice — and both `intersect_sorted` and
`row_selection_runs` drop rows from a list that is not ascending. An *empty*
selection is not malformed: the caller pruned every group, so nothing in the
kept set matches, which is `Absent`.

Exhaustive single-bit sweep, every bit of every byte, asking one present term
for its rows (`report_single_bit_corruption_rates`):

| format | blob | flips | refused | unchanged answer | wrong rows | outside row domain | reported absent |
|---|---:|---:|---:|---:|---:|---:|---:|
| whole-file v1 | 14,038 | 112,304 | 99,019 | 13,136 | 120 | 0 | 29 |
| segmented | 11,186 | 89,488 | 45,000 | 44,368 | 120 | 0 | 0 |

The v1 row is the **post-#4558** decoder, re-measured 2026-09-17. Before that
task hardened `from_bytes` it read 28,050 refused / 83,977 unchanged / 142 wrong
/ **22 outside the row domain** / 135 reported absent — those 22 were the case
this design was partly arguing against, and validating the serialized lengths
and the postings' ascent and domain closed them in v1 too. The comparison stands
on the remaining columns: v1 still conflates "absent" with "unparseable" 29
times where the segmented reader never does, and only the segmented format can
be read in part.

The residual, identical in both formats at the codec level, is the 120 (0.13% of
flips): a bit flip inside a posting delta that leaves the varint count intact,
the ordinals ascending and every one of them inside the row domain yields a
different, structurally valid row set. The result is a wrong row set, and a row
the scan never decodes is not recovered by re-checking the predicate above it.
Nothing short of a checksum over the postings sees it.

A mis-addressed posting range is the same hole reached from the other side, and
a fixture pins what it does: a block's `postings_base` moved by one byte answers
row 23 for a term whose row is 22
(`a_directory_that_misaddresses_a_block_is_unanswerable_not_absent`). That one is
not corruption — the directory is checksummed, so it can only arrive from a
writer that computed the offset wrong — but it decodes the stated document
frequency, ascending and in domain, and is therefore indistinguishable from a
correct answer by everything the format checks.

### Do posting sections need their own checksum?

Seg1 keeps its original bytes without one. Seg2 adopts a CRC per block's
posting span, coupled to block compression. Measured at the per-file scale above
(`report_posting_checksum_and_compression_options`):

| option | cost on disk | median bytes a point lookup fetches per row group | catches the residual |
|---|---:|---:|---|
| nothing (`seg1`) | — | 1,621 B | no |
| CRC per term | 28.0 MiB, **+32.6%** of the blob | 1,621 B | yes |
| CRC per block's posting span | 90.0 KiB, **+0.103%** | 2,563 B (**1.58x**) | yes |

A point lookup reads one term's postings — a median of 3 bytes on this corpus —
so a checksum it can verify without extra reads has to be per term, and 4 bytes
per term against a median 3-byte posting list is where the 32.6% comes from. The
alternative moves the unit to the one the reader already fetches whole: a CRC
over each *block's* posting span costs 90.0 KiB and is verifiable only if the
lookup fetches that span (a median 945 B) instead of the term's slice. Against
the 1,618 B dictionary block it fetches anyway, that is 1.58x the bytes per
group and still thousandths of a percent of the blob.

The seg2 reader fetches one compressed posting-span frame, bounds decompression
by the directory's raw length, verifies the decoded length and CRC, and only
then slices out the named term. A frame error, length mismatch or CRC mismatch
is `Unanswerable`. This closes both the single-bit residual and a directory
whose otherwise-valid offsets address the wrong span.

The decision was taken because #4562 retained the format and measured the
uncompressed sidecar at 5.59x the shipped sidecar's on-disk bytes:

- **Registering uncompressed removes a checksum that exists today.** The v1
  sidecar travels inside a Zstd frame written with `include_checksum(true)`
  (`third_party/iceberg/src/compression.rs`), so in deployment a flipped bit
  anywhere in it fails decompression. What follows is a **failed query**, not a
  scan: the error propagates out of `PuffinReader::blob` through the reader
  (#4991, measured in
  [`DESIGN_inverted_index.md`](DESIGN_inverted_index.md), "Which storage path
  carries that residual"). A v1 index in a Parquet footer has no cover at all
  and answers short. An uncompressed segmented blob has no cover either, and
  its posting sections are then the only part of it no checksum spans.
- **Per-block compression uses the same granularity** (see
  [Publication semantics](#publication-semantics-what-4377-needs)), so a
  compressed version gets the checksum without another read — the span is
  already the fetch unit.

The checksum itself is IEEE CRC-32 from `crc32fast`, which the tree already
carried behind flate2. The prototype's bytewise table ran at 0.531 GB/s against
12.68 GB/s, which is 71.0 ms against 3.0 ms of writer time per file for the
dictionary blocks, and 0.92 ms against 0.04 ms for the directory — a quarter of
a 3.65 ms cold open spent checksumming it. `crc32_reference` in the module's
tests writes the algorithm out and pins `crc32` against it and against the
standard check vector, so which implementation computes a blob's checksums
cannot change the bytes on disk.

## Publication semantics (what #4377 needs)

The whole layout is arranged so a merge can write it in **one forward pass**:

- Postings and dictionary for group `i` are written when group `i` closes, and
  nothing already written is patched. The directory and trailer come last.
- Peak **parsed** index state is one row group's postings plus its dictionary
  entries — not the file's. `SegmentedWriter::push_group_index` takes an index
  built over exactly one group's rows, which is what a merge already has in hand
  when it flushes a row group. That bound is about the parsed state alone; the
  encoded blob accumulates behind it (`SegmentedWriter::out`), and a rewrite
  holds every finished blob until it publishes them. The three quantities are
  measured apart in [Many rolled outputs in one
  rewrite](#many-rolled-outputs-in-one-rewrite-5299).
- The blob is complete only once the trailer lands, so a partial upload is not
  mistakable for an index: the reader looks for the trailer's magic at a fixed
  offset from the end, and a truncated blob does not have it.

For registration, a segmented sidecar should carry the properties the v1 one
does (`data_file`, `column`, `tokenizer`) plus its versioned `format` (`seg1` or
`seg2`), and should **not**
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
(#4329: 229 MB of statistics increment over 14 files).

### Writer integration (#4377)

The streaming rewrite now follows those semantics behind
`SIGLAKE_SEGMENTED_INDEX_WRITES=1`. The Parquet writer closes the index group
beside each row group, compares the finished sidecar's group rows with the
footer it actually wrote, and hands one seg2 blob per `(data file, column)` to
the rewrite. The rolling writer shares one sink across its output files;
intermediate tier files receive no sink because no snapshot will contain them.

Before committing, the rewrite reserves its snapshot id, then a transaction
action writes one Puffin statistics file containing the completed blobs and
applies its registration with the data-file swap. The action runs after the
rewrite action on every commit attempt, so it stamps the snapshot id and
sequence number from the refreshed base into the table statistics metadata and
physical Puffin footer (#5260). A stale first base or a failed CAS writes
another Puffin container from the already-built seg2 bytes; the Parquet output
is neither decoded nor rewritten. A refused transaction can leave an
unreferenced object for orphan GC, but no table metadata names it. The
post-commit v1 rebuild treats v1 and seg2 registrations as equivalent coverage
for `(data file, column)`, so it performs no full-file decode for the new output
and a repeated rebuild is a no-op. Retired seg1 metadata does not suppress that
rebuild.

`crates/siglake-storage/tests/segmented_index_writer.rs` holds the single-file,
rolled-output, multi-column, two-partition, failed-transaction, idempotence,
exact-query and row-group-memory cases. Reads discover seg2 only; the knobs
for writing and reading are separate and both remain off by default. The
writer's build-time and peak-heap report is an `#[ignore]`d release test in
that file because its acceptance corpus is 14 x 7.34M rows.

The #5234 run used 14 files of 7,340,000 rows (102,760,000 rows total), with
append indexing on, `target_row_group_bytes=1` (clamped to 128 Ki-row groups)
and streaming rewrites in every arm. Only `segmented_index_writes` and
`index_rebuild` changed. The allocator
figures are peak tracked live bytes during append plus rewrite, not process RSS.
`/proc/loadavg` was read immediately before each arm.

Its scope is one arm's **append plus all fourteen of its rewrites**, one file
per rewrite, so at any moment the sink held one output file's sidecar. It says
nothing about a rewrite that rolls many outputs, which is the measurement in
[Many rolled outputs in one
rewrite](#many-rolled-outputs-in-one-rewrite-5299). Its heap column is also
the pre-#5299 tracker's: a counter reset to zero when the window opened, which
allocations already live were invisible to while their frees still decremented
it. That is a lower bound on the window's growth, not the process's live heap
and not an exact delta above a baseline. The arms are paired and each opens its
window on the same empty-warehouse state, so the comparison between them
stands.

| arm | load average (1/5/15m) | segmented writes / v1 rebuild | append | rewrite | total | peak live heap | registered sidecar / data | live files |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| off-1 | 5.63 / 4.82 / 4.87 | off / off | 451.81 s | 142.61 s | 594.41 s | 251.4 MiB | 0 / 697,883,148 B (0.00%) | 14 |
| on-1 | 4.20 / 3.66 / 4.09 | on / off | 454.52 s | 283.44 s | 737.96 s | 251.2 MiB | 247,359,225 / 697,883,148 B (35.44%) | 14 |
| off-2 | 9.08 / 5.34 / 5.28 | off / off | 448.18 s | 142.28 s | 590.46 s | 251.5 MiB | 0 / 697,883,148 B (0.00%) | 14 |
| on-2 | 4.20 / 3.39 / 4.16 | on / off | 446.41 s | 282.09 s | 728.50 s | 251.6 MiB | 247,359,226 / 697,883,148 B (35.44%) | 14 |
| v1-rebuild | 4.21 / 2.99 / 3.39 | off / on | 438.21 s | 321.35 s | 759.56 s | 1,309.4 MiB | 224,141,786 / 697,883,148 B (32.12%) | 14 |

Seg2 added 140.83 and 139.81 seconds to the rewrite arms over no index
(98.8% and 98.3%), or 24.2% and 23.4% to append plus rewrite. Peak tracked
heap stayed within 0.2 MiB of each paired control. Against the post-commit v1
rebuild it replaces, seg2 averaged 282.77 seconds of rewrite time and 733.23
seconds total: 12.0% and 3.5% below the v1 arm. It used 80.8% less peak tracked
heap and 10.36% more registered sidecar bytes. The v1 comparison is one arm,
while the seg2/no-index result is the requested repeated pair; the load readings
above bound what this local timing says.

The first run was interrupted after off-1, on-1 and off-2 had completed. Its
stdout and stderr were retained under the run-owned TMPDIR. A release
continuation reran off-2 immediately before on-2, then ran v1-rebuild; all
coverage assertions passed, including zero registered blobs for each off arm,
one live seg2 blob per file for each on arm and one live v1 blob per file for
the rebuild arm.

#### Many rolled outputs in one rewrite (#5299)

The #5234 arms rewrite one day at a time, so the shared `SegmentedIndexSink`
never holds more than one output file's sidecars. `report_rolled_rewrite_sink_heap`
asks the other question: one streaming rewrite of **one partition** whose output
rolls into many files, so every finished blob is retained together until the
transaction publishes them. It separates the three quantities the bound above
is easy to conflate — the parsed per-group index, the serialized bytes the sink
accumulates, and the rewrite's total peak heap.

Each arm: a managed index with one or two indexed text columns carrying the
same text, all rows in one day partition, appended with no index, then one
forced streaming rewrite of the whole bin. `target_row_group_bytes=1` (the
128 Ki-row floor) and a 64 KiB rolling target are fixed across every arm, so a
file closes at each row-group boundary and the output volume alone sets how
many sidecars the sink holds. Every shape runs with `segmented_index_writes`
on and off, identical otherwise. Heap is peak tracked live bytes **above the
live heap the rewrite started from**, measured over the rewrite only; the
appends are outside the window.

```text
SIGLAKE_SEG_ROLL_ROWS=327680,1310720,2621440,5242880 SIGLAKE_SEG_ROLL_COLUMNS=1,2 \
  cargo test -p siglake-storage --release --test segmented_index_writer \
  report_rolled_rewrite_sink_heap -- --ignored --nocapture
```

The 1-minute load average read before each arm fell from 7.27 to 3.00 over the
run, which bounds the wall times below but not the allocator figures.

| rows | outputs | row groups | indexed columns | rewrite off | rewrite on | peak above baseline, off | on | seg2 delta | sink retained at publish | largest parsed group index |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 327,680 | 3 | 4 | 1 | 0.36 s | 0.81 s | 19,779,511 B | 33,000,197 B | **13,220,686 B** | 784,001 B | 9,972,574 B |
| 1,310,720 | 10 | 14 | 1 | 1.54 s | 3.28 s | 27,855,750 B | 41,080,916 B | **13,225,166 B** | 3,148,010 B | 9,972,574 B |
| 2,621,440 | 20 | 30 | 1 | 2.94 s | 6.70 s | 38,022,297 B | 51,247,455 B | **13,225,158 B** | 6,293,316 B | 9,972,574 B |
| 5,242,880 | 40 | 60 | 1 | 5.86 s | 13.20 s | 58,282,520 B | 71,507,657 B | **13,225,137 B** | 12,586,628 B | 9,972,574 B |
| 327,680 | 3 | 4 | 2 | 0.42 s | 1.35 s | 33,312,294 B | 41,935,190 B | **8,622,896 B** | 1,568,002 B | 9,972,574 B |
| 1,310,720 | 10 | 14 | 2 | 1.59 s | 5.31 s | 48,746,822 B | 57,369,709 B | **8,622,887 B** | 6,296,020 B | 9,972,574 B |
| 2,621,440 | 20 | 30 | 2 | 3.30 s | 10.76 s | 68,153,074 B | 76,775,952 B | **8,622,878 B** | 12,586,632 B | 9,972,574 B |
| 5,242,880 | 40 | 60 | 2 | 6.46 s | 21.47 s | 106,816,746 B | 115,439,602 B | **8,622,856 B** | 25,173,256 B | 9,972,574 B |

**Sixteen times the output volume does not move the rewrite's index cost.**
From 3 to 40 rolled outputs the seg2 delta over its matched control changes by
under 300 bytes in either column count, while what the sink holds at publish
grows 16x with it. The peak is set elsewhere: the merge's own decode buffers
(which grow with the output, in both arms) plus one parsed row-group index. A
repeat of both arms of all eight shapes reproduced every `above baseline`
figure to within 6 bytes, so these are not noisy readings.

The three quantities, apart:

- **Parsed per-group index.** 9,972,574 B for a 131,072-row group of this
  corpus, identical in all eight seg2 arms — a pure function of the group's
  rows, as
  `the_index_state_a_rewrite_holds_is_one_row_groups` asserts. The *sum* over
  groups reaches 797,826,276 B in the largest arm and is never live at once.
- **Serialized sidecars the sink retains.** 2.40 B per row per indexed column,
  and that is per *row*, not per file: the rolling target decides how the bytes
  are split, not how many there are. One rewrite of N rows over C indexed
  columns ends holding about 2.4·N·C bytes until its transaction publishes them.
- **Total rewrite peak.** Dominated by the merge, in both arms, and growing
  with the output volume in both.

So the boundary is a row count per transaction, not a file count. Retention
passes one group's parsed index at about 4.2M rows per indexed column, and
reaches the merge's own working set on this box (50–110 MiB) at roughly
20–48M rows per column. At the production 512 MiB rolling target the same rows
produce far fewer, larger files and exactly the same retention: a 100M-row
rewrite would hold ~240 MB per indexed column at publish. That is the figure a
spilling decision would be about. #5299 measures the boundary and leaves the
behaviour where it found it: spilling and the writer's defaults were out of its
scope.

What the numbers do not settle: the corpus's sidecar is larger than its Parquet
output (25,205,726 B registered against 11,132,269 B of data in the largest
arm) because almost every term is unique to one row while the columns
themselves dictionary-compress nearly to nothing — the ratio on real text is
#5234's 35.44%. The seg2 delta is also not additive per column at the peak
(13.2 MB for one column, 8.6 MB for two): each arm peaks at a different
instant, and the two-column control is already carrying a second decoded text
column. And a rewrite of 40 outputs on this box is not one against S3, where a
slower publish holds the same bytes for longer.

#### Registration beside the refusals

`publish_segmented_sidecars` publishes nothing for a file whose finished
sidecar disagrees with the footer it wrote, counting the reason on
`siglake_iceberg_segmented_index_writes_total{outcome="refused"}`:

| reason | what disagreed |
|---|---|
| `column` | a row group whose text column could not be indexed, so the sidecar has no group for it |
| `file_rows` | the sidecar's total rows are not the data file's |
| `row_domain` | the sidecar's per-group rows are not the footer's row groups |

Silence is safe on its own — an unregistered file is read the way an unindexed
one is. Before #5228 it was unsafe for the file's *siblings*:

A rewrite registers every output blob in **one** `StatisticsFile` under the
snapshot id it reserved, and `set_statistics` inserts by snapshot id
(`third_party/iceberg/src/spec/table_metadata_builder.rs:589`), so a second
statistics file written against that snapshot replaces the first rather than
merging into it. With `SIGLAKE_INDEX_REBUILD=1` as well, a refused file is
uncovered, so the post-commit `rebuild_inverted_indexes_for_files` over the
rewrite's output built a v1 blob for it and registered it under the same
snapshot — dropping every seg2 blob the rewrite had just published. Their
Puffin path left `reachable_files` and orphan GC deleted the object; the query
path fell back to a scan and answered correctly, so nothing reported the loss.

Both opt-ins and a refusal were needed to reach it. Registration now refuses a
second statistics file for the same snapshot, counts the deferral with the
bounded `reason="snapshot_has_statistics"` label and logs the uncovered data
files. It reports none of those files or bytes as rebuilt. The first statistics
file remains discoverable and reachable to orphan GC; uncovered files stay on
the exact scan path until a later snapshot can carry their rebuilt index.
`a_v1_rebuild_against_the_rewrites_snapshot_keeps_its_seg2_blobs` drives that
mixed-coverage boundary and checks the retained seg2 reader's exact answers.

#### Which base the refusal is decided on

#5228 decided it on the caller's own table handle, which settles the sequential
case and no other. `Transaction::do_commit` loads the table at the top of every
attempt and re-applies each action against that base, so a check made before
the transaction says nothing about the base the commit lands on. Two
registrants starting from a statistics-free snapshot both passed it, and the
Iceberg `UpdateStatisticsAction` they used emits an unconditional replacement
(`third_party/iceberg/src/transaction/update_statistics.rs:79`) — so the second
one to commit dropped the first's blobs anyway, exactly as before the guard.

Since #5298 the Siglake registration path owns the action.
`RegisterFirstStatisticsAction` re-reads `statistics_for_snapshot` on every
attempt and emits its `SetStatistics` only while the snapshot still carries
none; a base that already has one yields no updates and no requirements, which
ends the transaction without a catalog write. The general Iceberg replacement
contract is unchanged — the fork's own action still replaces, and the rewrite
path's `PublishSegmentedStatisticsAction` still registers against the snapshot
it reserved. The deferral is counted once per registration call rather than
once per attempt: a retry is the same deferral seen again. The Puffin object
the deferred caller had already written is unreferenced and `gc_orphans`
reclaims it; the warning names it. The two deterministic boundaries are
`a_competing_registration_before_the_refresh_keeps_the_first_statistics_file`
(the rival commits between the caller's load and the first attempt's refresh)
and `a_competing_registration_in_the_cas_window_keeps_the_first_statistics_file`
(the rival commits inside the first attempt's CAS window, so the decision is
made on the retry's base). Both register disjoint files, and both assert the
winner's entry, its reachability, one deferral and the deferred caller's zero
rebuilt files and bytes.

### What per-section compression would recover

`seg1` stores every section uncompressed. Seg2 compresses every dictionary
block and every block's posting span as independent Zstd-3 frames. Its
directory carries each frame's stored and raw lengths; posting entries also
carry the CRC of the raw span. Measured on the same file:

| | bytes | ratio |
|---|---:|---:|
| `seg1` as written | 85.8 MiB | 1.00x |
| zstd over the whole blob (what a v1 sidecar pays, and what a range reader cannot use) | 16.3 MiB | 0.19x |
| zstd per dictionary block | 2.5 MiB of 35.9 | 0.07x |
| zstd per block posting span | 13.4 MiB of 49.4 | 0.27x |
| projected per-block total, directory uncompressed | **16.4 MiB** | **0.19x** |
| seg2 codec output, including its larger directory | **16.7 MiB** | **0.19x** |

Compressing at block granularity recovers the storage gap while every section
stays reachable by a range read. The measured codec is 0.3 MiB above the
projection because seg2 records four lengths and two CRCs per block and each
section is its own frame. On the six query shapes, one warm file fetched 5.7
KiB for full-file point lookups, 1.6 KiB for last-quarter lookups, 638 B for a
unique token, and 2.7 MiB for the whole-dictionary substring sweep. Those are
stored bytes after compression; the earlier 1.58x figure prices the raw span
the codec has to decode and checksum.

## Reader integration (#4561)

The scan path can now answer a text predicate from a segmented sidecar,
reading byte ranges of it. It is off unless `SIGLAKE_SEGMENTED_INDEX_READS` is
set (`1`/`true`/`yes`/`on`), and nothing writes the format, so with the knob
unset the reader behaves exactly as it did: a file carrying only a segmented
sidecar is scanned, and a file carrying a v1 one takes the v1 path.

**A sub-range entry point.** `PuffinReader::blob` reads a blob's whole
`offset..offset + length` and decompresses it, which is the cost this format
exists to avoid. `PuffinReader::blob_range_reader` returns a `BlobRangeReader`
instead: one opened file reader, ranges bounded by the *blob* rather than the
file, and a refusal for any codec but `None` — a compressed blob has no
addressable interior. Each range is charged to
`ObjectStoreReadPhase::Index`, the phase the whole-blob fetch charges, so the
fetched bytes land in the accounting the sidecar read they replace used.

**Discovery is per file, by blob type.** The reader looks through the scan
task's statistics blobs for `siglake-inverted-seg-v1` with a matching `column`
property, then resolves the blob's offset and length from the Puffin footer
(through the same cached footer read the v1 path uses). The v1 path looks for
`siglake-inverted-v1` and does not see a segmented blob; the segmented path
runs first and falls through to it, so a file carrying both is answered by the
segmented reader and a file carrying either is answered by that one. The
footer-KV key (`siglake.inverted_index.seg1`) is not wired: a footer index
arrives with the Parquet metadata the scan already read, so there is nothing to
read in part.

**The sync/async seam.** `RangeSource` is synchronous and the object store is
not. A range the async side cannot serve comes back as `None`, which the reader
turns into `Unanswerable`. Below the directory nothing is cached: the reader
asks for what a term needs, per lookup. #4561 bridged the seam by running each
lookup on a blocking thread and serving its ranges over a channel, one at a
time, which held one thread of tokio's blocking pool for the whole of a
lookup, IO waits included; #5007 replaced that with the staged reader below.

### Holding the directory between lookups (#5006)

`SegmentedDirectory` is the parsed directory on its own, split out of
`SegmentedReader` so a second reader on the same blob can be built from one
parsed earlier (`SegmentedReader::open_with_directory`). A blob at a Puffin
offset is written once, so the directory is a pure function of its bytes and
an entry can no more go stale than a parsed v1 index can; the reader holds
them in a cache keyed by `(statistics file, blob offset)`, the same write-once
identity `ParsedIndexKey::Puffin` uses. A repeat lookup then reads neither the
trailer nor the directory — the two reads and 55,660 bytes the table below
charges every shape at 1M rows, 474.9 KiB per file at 7.34M.

Four things this deliberately does not do:

- **It holds no per-lookup state.** The staged source, the counters and the
  `BlobRangeReader` are built per lookup and the cost reported
  (`_range_reads`, `_fetched_bytes`, and since #5007 `_stages` and
  `_reader_reads`) is that lookup's alone, warm or cold. The
  resident-byte histogram is recorded on a warm lookup too: the memory a warm
  arm spends is the thing #4562 is comparing, and it must not vanish from the
  report because it was paid once.
- **It does not skip the row-domain check.** `matches_row_groups` runs per
  lookup against the caller's Parquet metadata. The directory describes the
  blob; which data file the blob is being applied to is the caller's question,
  and a held directory must not answer it.
- **It does not turn a mis-keyed entry into a decline.** A held directory
  whose blob length is not this blob's is refused by
  `open_with_directory`, and the lookup reads the blob's own directory as if
  nothing had been held.
- **It does not touch the two shipped budgets.**
  `SIGLAKE_SEGMENTED_INDEX_DIRECTORY_CACHE_MAX_BYTES` (default 64 MiB, `0`
  disables retention) is its own, enforced by bytes with no entry bound, and
  it is not part of `text_index_cache_max_bytes_in_force()` — the query pool
  subtracts what it subtracted before. An experimental budget, not a packaged
  one: nothing consults this cache unless `SIGLAKE_SEGMENTED_INDEX_READS` is
  set, so a packaged process retains nothing in it whatever the knob says. Its
  own numbers are
  `siglake_iceberg_segmented_index_directory_cache_bytes` /
  `_max_bytes` and
  `_lookups_total{outcome}` / `_evictions_total{reason}`, and
  `segmented_directory_cache_footprint()` for a harness. What an entry costs
  is the directory plus its key: 135,316 bytes for the 1M-row file below,
  against the 135,222 the resident-byte histogram reports. At the 7.34M-row
  scale that is ~1.0 MiB per file, so the 64 MiB default holds a 14-file text
  plan's directories sixty times over and the byte bound does not bind at any
  scale this prototype has been measured at.

**The prototype is instrumented apart from the v1 path.** A file answered by a
segmented sidecar increments
`siglake_iceberg_segmented_index_used_total{source}` and does *not* touch
`siglake_iceberg_inverted_index_used_total`, the parsed-index cache counters,
the `text_index_startup_seconds` stages or the scan's `EXPLAIN`
`text_index:[…]` attribution — all of which describe loading a whole-file
index, which this path never does. #4562's harness has to read the segmented
counters; reading the v1 ones would show a plan that used no index at all.

**What the policy does with the three outcomes.** Only a definitive answer
prunes. `Declined` is recorded with a reason
(`siglake_iceberg_segmented_index_declined_total{reason}`) and leaves the
caller where it would have been anyway — the v1 index if the file has one, an
exact scan otherwise:

| reason | when |
|---|---|
| `compressed` | the sidecar was registered with a codec, so it has no addressable interior |
| `open` | trailer, directory CRC or structural tiling refused the blob |
| `row_domain` | `matches_row_groups` disagrees with the file's Parquet row groups |
| `row_group_order` | the scan's kept-group list is not strictly ascending, so the sidecar and the selection would cover different groups |
| `unanswerable` | a term that does not normalize, a malformed section, or a failed range read |
| `no_hints` | the prune spec carries nothing this index can answer |
| `clipped_document_frequency` | point terms' summed df exceeds the clipped query's row limit |
| `clipped_estimate_unavailable` | a clipped substring would require a full dictionary sweep rather than a point estimate |
| `stages` | the staged rounds did not converge inside their budget (#5007) — unreachable for this reader, and a bound rather than an expected outcome |

Two entry points changed in `siglake_index::segmented` for this, both
reader-side policy the codec deliberately left open:

- **AND resolves rarest first, per group.** Every term is located in the
  group's dictionary before any postings are fetched — the block read a point
  lookup pays anyway — and the postings are then read in ascending document
  frequency, stopping as soon as the running intersection empties. A group
  missing one of the terms reads no posting section at all. The df is in that
  dictionary block, beside the term's posting length; the directory carries
  the block's location and first term. A point estimate therefore costs one
  dictionary-block range read per group after the cold trailer and directory
  reads. The v1 index has no useful equivalent, since it has already decoded
  everything by the time it can read a df.
- **OR has an entry point at all.** `RawPruneSpec::any_terms` had none: the v1
  path unions `postings` per term and skips a term it cannot answer, which is
  safe only because `extract_match_udf_prune` fills the spec from tokenizer
  output. Under the three-outcome contract a skipped term would license
  skipping rows it might have matched, so `matching_rows_any_in_groups`
  declines the whole disjunction instead.

The substring sweep is answered exactly and costs what it costs (below);
declining it is a per-execution decision and belongs with #4375's policy, which
already gates this path along with the v1 one.

### Reading in stages, not on a blocking thread (#5007)

Two shapes were on the table for the seam.

An **async trait on the reader** makes every entry point `async` and awaits
each range where the lookup needs it. It holds no thread, and it keeps the
reads strictly serial: one round trip per read, which is the `reads` column
below running from 4 to 2,938 per file. It also reopens the codec API #4560
had just settled, and issuing a group's reads concurrently from inside the
reader would mean the codec depending on a runtime.

A **staged reader** runs the synchronous lookup over the ranges it already
holds. A range it does not hold is recorded as a miss and reads as a failed
read, so that run declines; the caller then fetches every miss the run
recorded — together — and runs the same lookup again. A lookup is a pure
function of the blob's bytes, so the replay asks for the same ranges and gets
one stage further each round. Nothing waits on the store inside a run, so
nothing holds a thread across a wait.

**The staged reader is what the reader does now**
(`siglake_index::segmented::StagedSource`, `segmented_matching_rows` in
`third_party/iceberg/src/arrow/reader/pruning.rs`). The codec's API is
untouched. What
changed inside it is that a failed range read no longer returns from the middle
of a multi-group walk: the verdict is the same, `Unanswerable` the moment any
range fails, but the walk finishes, so one run records every range the lookup
needs rather than one group's. Without that the rounds would be the groups
(`2 + 2G` for a single term over `G` kept groups); with it they are the
stages — the trailer, the directory, every dictionary block the terms name
across every kept group, and their posting sections. Four rounds for any
shape, two of them gone when the directory is held (#5006), plus one round per
additional term of a conjunction, whose postings are fetched rarest-first and
stop as soon as the intersection empties. That ordering is worth more than the
round trip it costs (the `and_rare_keyword` row below reads 26 ranges where an
unordered conjunction reads both terms' postings in every group).

The ranges of one stage go out together, bounded by
`SIGLAKE_SEGMENTED_INDEX_RANGE_CONCURRENCY` — default 10, which is
`DEFAULT_RANGE_FETCH_CONCURRENCY`, the bound the scan's own merged-range reader
uses. A range the store refuses is filled as unreadable rather than left
missing, which is both what the reader reads as a failed read and what ends the
rounds: every round fills every range it asked for, so the held set only grows
and a blob has finitely many ranges. A stage budget
(`Declined("stages")`) bounds the loop anyway, for a future reader that asked
for its ranges some other way.

**What it costs.** Two things, both of them per lookup and both measured
below:

- every range a lookup fetched is held until the lookup ends, so its peak is
  the lookup's fetched bytes rather than one range — 65,481 bytes for the
  `rare` shape, 5,207,615 for the substring sweep;
- a stage re-decodes the ranges it already holds on its way past them, which
  the `reader reads` column prices: 45 reads against 18 fetched for `rare`, and
  8,805 against 2,938 for the sweep, whose cost *is* dictionary decoding. The
  directory itself is parsed once per lookup, not once per stage: the first
  stage that parses it hands it to the next, the way a held directory is handed
  in.

**Where the waits went**, measured hermetically: eight files' lookups run
concurrently against an in-memory store that delays every range read by 10 ms,
on a runtime with two workers and a blocking pool of one thread that a parked
task holds for the duration
(`a_segmented_lookup_waits_on_the_store_without_a_blocking_thread`). All eight
answer in **51.3 ms** — four stages each, 10 store reads, 25 reader reads, 32
range reads in flight at the peak. With #4561's driver restored (the same test,
the blocking-thread lookup patched back in), one lookup does not finish, because
the pool it needs a thread from is held: 60 seconds on the first measurement,
and 10 on the control re-run after this work was carried onto the 0.10.1 fork.

### What a lookup costs through the reader

`report_segmented_reader_read_cost`
(`third_party/iceberg/src/arrow/reader/pruning.rs`, `#[ignore]`d) measures the same quantities the tables below do, through the
Puffin container. Release build, 1,000,000 rows in 8 row groups, a 12,127,530-byte
segmented blob against a 74,284,630-byte parsed v1 index:

`cold` is a lookup that opens the sidecar; `warm` is the same lookup with the
directory held (#5006), which is what a file costs after its first query.

| shape | rows | cold reads | cold fetched | ÷ blob | warm reads | warm fetched | ÷ blob |
|---|---:|---:|---:|---:|---:|---:|---:|
| rare | 1,004 | 18 | 65,481 | 0.540% | 16 | 9,821 | 0.081% |
| rare_last25 | 251 | 6 | 58,108 | 0.479% | 4 | 2,448 | 0.020% |
| keyword | 20,000 | 18 | 83,475 | 0.688% | 16 | 27,815 | 0.229% |
| unique_token | 1 | 4 | 57,415 | 0.473% | 2 | 1,755 | 0.014% |
| and_rare_keyword | 21 | 26 | 85,481 | 0.705% | 24 | 29,821 | 0.246% |
| or_rare_unique | 1,005 | 20 | 67,236 | 0.554% | 18 | 11,576 | 0.095% |
| substring_sweep | 20,000 | 2,938 | 5,207,615 | 42.940% | 2,936 | 5,151,955 | 42.481% |

Resident state is 158,646 bytes for every row — the directory, 468x smaller
than the parsed v1 index of the same file. (#4561 and #5006 reported 135,238
and 135,222 for the same file: the difference is seg2's per-block entry, which
#4988 added to every directory whatever format the blob is. The 16 bytes
between those two are the reader's own `size_of`, which moved to
`SegmentedDirectory` when #5006 split them.) Every row's answer is asserted
equal to the whole-file index's, restricted to the groups the shape kept,
before any cost is reported, cold and warm alike.

`and_rare_keyword` is the one row #5007 moved: 26 reads and 85,481 bytes where
the blocking-thread reader charged 34 and 93,296. Both terms sit in the same
dictionary block in each group; a staged lookup fetches that block once, and
the counters report what the store served rather than what the reader asked
for.

**The cold columns are the open**: two reads and 55,660 bytes of trailer and
directory, which is most of what every point shape fetches, and the whole
difference between the two halves of the table. At the 7.34M-row scale that
read is 474.9 KiB. `unique_token` is where it dominates — 57,415 bytes cold
against 1,755 warm, 33x — and `substring_sweep` is where it disappears into
the dictionary sweep the format does not help.

`and_rare_keyword` reads both terms' postings here — `rareneedle` and `queen`
are both in every row group, so the intersection never empties early — and its
26 reads are one dictionary block plus two posting reads per group. The
saving the df ordering buys shows where a term is absent from a group or the
intersection empties, which the codec's own fixtures pin.

#### Stages against reads (#5007)

The same run, same fixture, with the store waits counted. `stages` is the
rounds of waits the lookup took; `reader reads` is what those rounds decode
between them, against the `reads` the store served; `µs` is the lookup's wall
time through the Puffin container on a local file, two samples per arm, against
the blocking-thread driver on the same box and build.

| shape | reads | stages | reader reads | staged µs | #4561 µs |
|---|---:|---:|---:|---:|---:|
| rare | 18 | 4 | 45 | 376 / 444 | 771 / 784 |
| rare_last25 | 6 | 4 | 15 | 425 / 504 | 551 / 534 |
| keyword | 18 | 4 | 45 | 498 / 571 | 966 / 857 |
| unique_token | 4 | 4 | 10 | 278 / 330 | 550 / 501 |
| and_rare_keyword | 26 | 5 | 109 | 631 / 720 | 848 / 1,096 |
| or_rare_unique | 20 | 4 | 50 | 381 / 440 | 523 / 741 |
| substring_sweep | 2,938 | 4 | 8,805 | 89,445 / 99,620 | 75,577 / 78,173 |

Warm (directory held), the same two arms: two stages for every point shape and
three for the conjunction, 113-208 µs against 280-352 for the point shapes,
373 against 504-618 for the conjunction, and 85,125-86,210 against
70,518-73,034 for the sweep.

A local file's read is microseconds, so these columns are not the latency
argument — they are the *decode* argument, and they bound what the replay
costs: every point shape is faster staged (its four ranges go out together
where the blocking-thread reader waited for them in turn), and the substring
sweep is 18-27% slower, which is its 8,805 reader reads against 2,938 fetched
ranges. The latency argument is the delayed-store measurement above: at 10 ms a
read, one `rare` lookup is 4 stages against 18 sequential reads, and eight
files' lookups finish in the time one stage takes.

## Measurements

Release build, this box, local instrumentation only. The corpus is
`puffin_rebuild.rs`'s measurement text (`ab_shaped_event`: shared tokens, a
2%-density `queen`, a `checkout` on every twentieth row, one unique token per
row) with `rareneedle` on one row in 100,000 — the same generator the #4329 and
#4375 tables were taken on. One file is 7,340,000 rows in 7 row groups of
1,048,576, which is the compacted layout's default row-group target
(`target_row_group_rows`). Nine executions per shape for the per-file section,
medians reported; three for the plan section, where the first is reported as
`cold` and the median of the other two as `warm`.

Every arm's rows are asserted equal to the whole-file index's answer, restricted
to the row groups the shape kept, before any timing is reported.

### One file

7,340,000 rows, 7,340,011 distinct terms, 7 row groups of 1,048,576.

| format | build | serialized | parse/open | resident |
|---|---:|---:|---:|---:|
| whole-file v1 | 13.55 s | 116.5 MiB | 3.27 s | **526.0 MiB** |
| segmented | 11.87 s | 85.8 MiB | 3.65 ms | **1.0 MiB** |

Resident bytes fall **510x**; the blob is 0.74x the size of the v1 one, split
35.9 MiB dictionary, 49.4 MiB postings, 474.9 KiB directory. The blob shrinking
while gaining a directory and a per-term document frequency is the dictionary
blocks: v1 writes every term in full
(`crates/siglake-index/src/lib.rs:250`), and on this corpus almost
every term is `row-NNNNNN`, so a block's shared prefix covers most of it.

"parse/open" is the asymmetry the format exists for. v1 must decode 116.5 MiB
into a `BTreeMap` before answering anything; the segmented reader reads a
trailer and a directory and is ready in 3.65 ms, ~900x sooner.

Every byte count in this document reproduced exactly across two full runs. The
timings are one run's, and they jitter a few percent between runs on this box
(the v1 parse measured 3.27 s and 3.54 s); nothing here turns on a difference
that small.

### Per shape, one file, index work only

The `v1 warm` column is a lookup on an **already parsed** index — the state a
14-file plan cannot hold. It is the cost segmented reads have to be compared
against *when the cache wins*, and it is where v1 is better: a `BTreeMap` hit is
hundreds of nanoseconds, a block read plus a posting read is tens of
microseconds.

| shape | rows | v1 warm | seg warm | seg reads | seg fetched | ÷ blob |
|---|---:|---:|---:|---:|---:|---:|
| rare_scan | 74 | 380 ns | 66.2 µs | 14 | 9.8 KiB | 0.011% |
| rare_scan_last25 | 21 | 330 ns | 17.7 µs | 4 | 2.7 KiB | 0.003% |
| keyword | 146,800 | 12.8 µs | 522.2 µs | 14 | 152.9 KiB | 0.174% |
| keyword_last25 | 41,942 | 12.4 µs | 148.1 µs | 4 | 43.6 KiB | 0.050% |
| unique_token | 1 | 100 ns | 4.6 µs | 2 | 1.7 KiB | 0.002% |
| substring_scan | 367,000 | 289.9 ms | 328.2 ms | 23,059 | 36.3 MiB | 42.3% |

Read the first five rows as the partial-read case and the last as its limit. A
point lookup touches two to fourteen ranges and thousandths of a percent of the
blob, at tens of microseconds against v1's hundreds of nanoseconds — 40x to
170x, on an operation that is already negligible beside the 3.27 s decode that
has to precede it. Pruning compounds: keeping the last quarter of the row groups
takes `rare_scan` from 14 reads to 4, because a rejected group costs no read.

`substring_scan` is the regime where the format buys nothing: 23,059 reads and
42.3% of the blob, because finding the dictionary terms that *contain* a
substring means reading every block. Its cost is the dictionary (35.9 MiB), and
it lands within 15% of v1's warm sweep while giving up the residency win. This
is the shape a reader should decline, and #4375's per-execution policy is where
that decision belongs.

### A 14-file plan under the deployed 1 GiB parsed-index budget

Fourteen logical files behind one byte-bounded LRU, which is the working set
#4329 measured (7.83 GB against 1 GiB, 388 evictions, one resident index). Every
logical file carries the same blob bytes and differs only in its cache key: this
sizes the cache pressure without building fourteen distinct corpora, and the
parse on every miss is real work.

Cold and warm are reported apart rather than medianed together, because the
first execution is the one that opens fourteen readers — a trailer and a
directory read each — and the two after it reuse them. For the v1 arm the
distinction is empty, which is the point: it never hits, so its warm column is
its cold one.

Only one of the two deployed budgets appears here. The 1 GiB parsed-index cache
is what holds a decoded index, and the 256 MiB Puffin-blob cache holds the
compressed blob bytes a decode was fed. **A segmented reader caches neither**:
it holds a directory and range-reads the rest, so the blob budget has no role on
this path. That is a difference in kind between the arms and #4562's
configuration should say so rather than applying both budgets to all three.

| shape | v1 cold | v1 warm | v1 h/m/e | seg cold | seg warm | seg h/m/e | seg cold fetched | seg warm fetched |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| rare_scan | 44.93 s | 44.13 s | 0/42/41 | 35.6 ms | 1.07 ms | 28/14/0 | 6.6 MiB | 137.2 KiB |
| rare_scan_last25 | 43.58 s | 44.05 s | 0/42/41 | 30.5 ms | 273 µs | 28/14/0 | 6.5 MiB | 37.2 KiB |
| keyword | 43.00 s | 43.68 s | 0/42/41 | 37.3 ms | 6.75 ms | 28/14/0 | 8.6 MiB | 2.1 MiB |
| keyword_last25 | 43.30 s | 43.80 s | 0/42/41 | 32.7 ms | 1.97 ms | 28/14/0 | 7.1 MiB | 609.8 KiB |
| unique_token | 43.01 s | 43.47 s | 0/42/41 | 33.1 ms | 215 µs | 28/14/0 | 6.5 MiB | 24.0 KiB |
| substring_scan | 47.28 s | 48.26 s | 0/42/41 | 4.68 s | 4.83 s | 28/14/0 | 514.6 MiB | 508.1 MiB |

v1 resident working set for 14 files: **7.19 GiB**. Segmented: **14.4 MiB**.

The v1 arm is #4329's result reproduced at the codec level, and the cache
counters are why: **0 hits, 42 misses, 41 evictions.** Two 526.0 MiB indexes do
not fit 1 GiB, so every file in the plan evicts the previous one and every
execution re-decodes all fourteen — which is why warming changes nothing. That
is ~43 s of index work for a plan whose OFF control (#4329) is 1.67 s cold /
1.80 s p50: a 24x loss on the index term alone, at a layer where nothing but the
format is varying. Sizing cannot reach it, because the working set is 7.19 GiB
against a 1 GiB budget.

The segmented arm never evicts, because fourteen directories are 14.4 MiB. Its
cold column is the honest cost of the format's one bulk read: ~6.5 MiB of
directory over fourteen files, 30-37 ms. Warm, the four point shapes answer the
whole plan in 215 µs to 6.75 ms, fetching 24 KiB to 2.1 MiB. Both columns sit
far under the 1.67 s OFF control, and the cold one does not need a cache to get
there — which is the property the whole-file format cannot have.

`substring_scan` marks the boundary again: 4.68 s cold, 4.83 s warm, and over
500 MiB fetched either way. It is the one shape where partial reads buy nothing,
and warming does not help it because the sweep re-reads every dictionary block
regardless.

### What these numbers are, and are not

- **Are:** the index-side cost of a text predicate — dictionary and posting
  bytes, range reads, parse and lookup time, cache behaviour at the deployed
  budget, and exact-row equality with the shipped index.
- **Are not:** end-to-end query latency. The scan-side term (opening Parquet,
  decoding the selected rows, `FilterExec` above it) is absent, and so is the
  query server, distribution and object storage. #4329's OFF column is the
  comparison those need: 1,671.8 ms cold / 1,801.2 ms p50 for the 14-file
  `rare_scan`. What this measures is that the shipped index spends 44 s on the
  index term alone — 24x the entire OFF budget — where the segmented reader
  spends 35.6 ms cold and 1.07 ms warm. That made the acceptance plausible and
  did not establish it; the section below runs the same three formats through
  the query path, where the scan-side term is present (#4562).
- **Are not** an object-store measurement. `SliceSource` counts what the reader
  *asks for* — 14 reads of 9.8 KiB is 14 GETs against S3, where the shipped path
  issues one GET of ~16 MiB and decompresses it. Whether many small ranges beat
  one large one depends on per-request latency and is a question for a prepared
  round, after #4561 has code to validate. Nothing here qualifies an AWS result.
- **Are not** a writer-cost benchmark. The build columns are sequential local
  fixture construction, one arm after the other.

## Through the query path: scan, whole-file, segmented (#4562)

The sections above measure the index term on its own. This one runs the same
three formats under DataFusion, where the scan-side term is present, using
`crates/siglake-storage/tests/puffin_rebuild.rs`'s
`report_rebuild_on_off_text_shapes` and the protocol at
[`DESIGN_inverted_index.md`](DESIGN_inverted_index.md) (line 102). Five arms per
shape, interleaved per execution:

| arm | what it is |
|---|---|
| `off` | the same corpus with no text sidecar of any kind — the scan control |
| `on` | one whole-file v1 Puffin sidecar per file |
| `policy` | the `on` warehouse queried the way the query server queries it, so #4375's per-execution rule declines the index for a clipped `LIMIT` |
| `seg` | no v1 sidecar; one segmented sidecar per file, groups identical to the file's Parquet row groups |
| `seg_policy` | the `seg` warehouse under the same #4375 rule |

The 2026-09-16 measurement below predates the writer and is labeled seg1. The
harness wrote those sidecars
(`write_segmented_sidecars`): one uncompressed blob per live data file,
registered as one Puffin statistics file on the current snapshot. The arms exist
only under `SIGLAKE_SEGMENTED_INDEX_READS`, which the reader resolves once per
process.

14 files × 7,340,000 rows = **102,760,000 rows**, one day per file, 42 row groups
(3 per file, byte-targeted by the writer, not the codec harness's 1,048,576-row
target). Identical Parquet layout across arms — 92,325,000 bytes and the same
`file_rows` vector — asserted before anything is timed. Five executions per
shape; the first is reported as `cold` and the median of the rest as `p50`.

**Exact answers first.** Every arm's rows are compared against the `off` arm's
before any cost is reported: an unclipped shape row for row and against the
generator's own count of corpus matches, a clipped one on row count plus a
per-row check that the row carries the term and falls inside the shape's window.
A segmented arm that answered no file from a sidecar fails the test rather than
reporting the scan's numbers under its label. All three runs below passed every
one of those assertions, in every arm, on every shape. The format returned no
wrong row and no missing row anywhere in this measurement.

### Historical seg1 latency, under the deployed 1 GiB parsed / 256 MiB blob budgets

The segmented arm's directory cache is at its own 64 MiB default, which is not
one of those two budgets and is not derived from a pod's memory (#5006).
`÷ off` is the arm's p50 over the `off` p50.

| shape | clip | selectivity | off cold | off p50 | v1 cold | v1 p50 | v1 ÷ off | policy p50 | seg cold | seg p50 | seg ÷ off | seg_policy p50 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| keyword | 100 | 2% | 41.2 ms | 8.4 ms | 12,566.1 ms | 6,030.9 ms | 722x | 10.2 ms | 45.1 ms | 37.3 ms | 4.46x | 15.1 ms |
| keyword_last25 | 100 | 0.5% | 21.6 ms | 18.5 ms | 5,012.3 ms | 10,447.1 ms | 564x | 16.0 ms | 34.0 ms | 29.7 ms | 1.60x | 22.5 ms |
| keyword_last5 | 100 | 0.1% | 13.9 ms | 11.1 ms | 1,233.7 ms | 4,773.0 ms | 431x | 11.6 ms | 83.6 ms | 66.8 ms | 6.03x | 10.2 ms |
| substring_scan | 100 | 5% | 6.6 ms | 5.6 ms | 4,639.9 ms | 15,011.8 ms | 2,682x | 6.5 ms | 1,017.0 ms | 972.4 ms | 174x | 6.6 ms |
| rare_scan | none | 0.001% | 1,656.6 ms | 1,838.9 ms | 40,780.6 ms | 41,114.6 ms | 22.4x | 22,009.1 ms | 119.9 ms | **160.2 ms** | **0.09x** | 137.4 ms |
| rare_scan_last25 | none | 0.00025% | 627.5 ms | 692.1 ms | 4,756.2 ms | 5,070.8 ms | 7.33x | 4,438.9 ms | 50.7 ms | **39.8 ms** | **0.06x** | 37.4 ms |
| rare_keyword | 100 | 0.001% | 540.4 ms | 530.7 ms | 17,889.2 ms | 16,646.5 ms | 31.4x | 538.0 ms | 45.7 ms | **47.6 ms** | **0.09x** | 550.4 ms |

Four results, in the order they bear on #4377.

**The rare shapes cross from loss to win.** `rare_scan` is 22.4x slower than the
scan with the shipped format and **11.5x faster** with the segmented one
(1,838.9 → 160.2 ms), `rare_scan_last25` 7.33x slower against 17.4x faster. This
is #4376's acceptance, under the budgets it names, end to end: the shipped
format cannot reach it at any cache size a query pod can afford (its own winning
regime needs the whole 7.3 GiB parsed working set resident), and the segmented
one reaches it with 15.95 MiB of resident directory and no eviction.

**It also removes #4375's one known loss.** `rare_keyword` — a clipped `LIMIT`
over a term rare enough that an index would have won — is the shape the
per-execution decline gives up on: 530.7 ms of scan, against 16,646.5 ms if it
had kept the whole-file index. The segmented format answers it in 47.6 ms,
**11.1x faster than the scan**, and `seg_policy` declines it anyway and pays
550.4 ms. The decline exists because loading an index is a whole-file cost. That
premise does not hold for this format, so #4375's rule has to become
document-frequency aware before #4377's format can pay off on clipped shapes.
The directory narrows the lookup to one dictionary block per group; reading
that block supplies the df and posting length before any posting span is
fetched.

**The clipped high-df shapes still need the decline.** `keyword` (2% density) is
4.46x the scan and `keyword_last5` 6.03x: a scan that stops at 100 rows reads a
sliver of one file, while the index reads megabytes of postings for millions of
documents. `seg_policy` declines them and lands at 0.92-1.81x. Partial reads
narrow the loss from 431-722x to 1.6-6.0x and do not close it.

**`substring_scan` marks the format's boundary.** 972.4 ms against the
scan's 5.6 ms, 174x, because a non-tokenizable `LIKE '%…%'` means finding every
dictionary term containing the substring and that reads every block. The policy
declines it (1.17x). #4375's per-execution rule is where this belongs and the
answer is to keep declining it.

### Cost, apart from latency

Range reads and fetched bytes are drained per execution from a debugging
recorder and reported separately, because the two questions differ: a local
file's 8 MiB read is cheap and an object store's is not. `cold` is a shape's
first execution — in a process where earlier shapes have already opened some
directories — and warm is the mean of the four after it.

| shape | seg files answered / exec | cold reads | cold fetched | warm reads / exec | warm fetched / exec |
|---|---:|---:|---:|---:|---:|
| keyword | 8.8 | 40 | 3,525,113 | 58.5 | 1,453,611 |
| keyword_last25 | 4.0 | 28 | 2,276,680 | 22.0 | 540,007 |
| keyword_last5 | 1.0 | 6 | 149,038 | 6.0 | 149,038 |
| substring_scan | 1.4 | 23,041 | 37,864,577 | 35,251.5 | 56,910,821 |
| rare_scan | 14.0 | 94 | 2,894,562 | 84.0 | **35,592** |
| rare_scan_last25 | 4.0 | 22 | 8,387 | 22.0 | 8,387 |
| rare_keyword | 10.2 | 54 | 22,785 | 63.0 | 27,403 |

A warm 14-file `rare_scan` fetches **35,592 bytes** — 2,542 bytes per file, six
range reads each — to answer a predicate over 102.76M rows, against the `off`
arm's 1,838.9 ms of Parquet decode and the shipped format's 7.3 GiB of parsed
index for the same answer.

The clipped shapes' file coverage is not fixed: a clipped `LIMIT` cancels the
remaining partitions once it has its rows, and how many files answered before
that varies per execution (`keyword`: 8.8 files per execution here, 3.3 in the
cold-control run below). Their byte columns are therefore not comparable across
passes; the unclipped shapes' are.

**Resident memory**, at the end of the pass:

| | entries | bytes | evictions | hits |
|---|---:|---:|---:|---:|
| parsed v1 index cache (1 GiB) | 1 | 559,896,106 | 314 | 0 |
| segmented directory cache (64 MiB default) | 14 | 16,722,773 | 0 | 388 |

One 534.0 MiB parsed index resident and 314 evictions with zero hits, against
fourteen directories in 15.95 MiB (1.14 MiB each) with zero evictions. The
14-file v1 working set is 7.30 GiB; the segmented one is 0.2% of it.

### Construction cost, apart from both

The harness builds each file's sidecar per row group from that group's rows:
145.6 s of Parquet decode plus term-dictionary construction across 42 groups,
and **11.9 s of codec encode** (`push_group_index` + `finish`) for all fourteen
files — 0.85 s per file, 8.6M rows/s. The decode half is an artifact of building
sidecars for files that already exist; #4377 builds the postings during the
streaming merge, where the rows are in hand. The encode half is what #4377 adds
to a merge. For scale: #4329's fixture build was 574.7 s with the rebuild off
and 729.0 s with the v1 rebuild on, so the v1 sidecars cost ~154 s for the same
fourteen files.

**On-disk bytes go the wrong way.** Measured as the statistics-file bytes each
arm adds over the no-text-index baseline (224,474,617 B), the v1 sidecars add
15.59 MiB per file and the segmented ones **87.19 MiB**, **5.59x**. The gap is
compression, not layout: the v1 sidecar's 116.5 MiB of serialized index travels
inside a Zstd frame and occupies 15.59 MiB, while a segmented blob has to stay
uncompressed for its interior to be addressable — 87.19 MiB here, against the
85.8 MiB the per-file section measured on a file cut into seven row groups
instead of this fixture's three. #4988's per-block compression projects 16.4 MiB
per file, which is where parity is. This is the one number in this measurement
that argues against shipping the format as prototyped.

### The packaged 4Gi configuration (both shipped budgets off)

Re-timed on the same corpus with `parsed=0, blob=0`, which is what
`derive_text_index_cache_bytes` returns on the packaged 4Gi query pod once the
pool's first-file decode reservation has taken the remainder:

| shape | v1 cold | v1 p50 | v1 ÷ off | seg cold | seg p50 | seg ÷ off |
|---|---:|---:|---:|---:|---:|---:|
| keyword | 3,792.1 ms | 3,433.3 ms | 411x | 23.2 ms | 28.1 ms | 3.37x |
| keyword_last25 | 10,575.7 ms | 10,705.1 ms | 578x | 31.0 ms | 32.1 ms | 1.73x |
| keyword_last5 | 3,843.1 ms | 3,448.3 ms | 311x | 70.8 ms | 70.9 ms | 6.40x |
| substring_scan | 11,351.5 ms | 11,171.0 ms | 1,996x | 968.6 ms | 851.4 ms | 152x |
| rare_scan | 21,400.7 ms | 27,699.2 ms | 15.1x | 123.3 ms | 119.6 ms | **0.07x** |
| rare_scan_last25 | 3,808.1 ms | 4,560.7 ms | 6.59x | 34.6 ms | 39.7 ms | **0.06x** |
| rare_keyword | 10,385.7 ms | 10,589.2 ms | 20.0x | 40.6 ms | 44.1 ms | **0.08x** |

The segmented column is the deployed-budget column again, within run-to-run
jitter: the format never touched either budget, so turning them off costs it
nothing. That is the structural result — the packaged pod's inability to hold a
parsed index stops being a text-query problem. The v1 column stays in the same
regime it was in at 1 GiB, because one resident index out of fourteen and none
out of fourteen differ only in which executions re-decode.

Two cautions on that table. The parsed-cache footprint printed after a
zero-budget pass is process-cumulative — a zero budget refuses admissions and
evicts nothing, so the 534.0 MiB entry from the earlier pass is still counted;
the per-shape `cache_hits=0` beside nonzero `decodes` is what shows the cache
was off. And a packaged pod today enables no segmented reads at all, so this
arm's seg column is what such a pod would cost if it did, at the 64 MiB
directory default.

### The segmented format's own budget, swept

`SIGLAKE_SEGMENTED_INDEX_DIRECTORY_CACHE_MAX_BYTES` is the only retention this
path has, and the eviction pressure that broke the v1 format has to be put on it
too. Three passes over the same fixture at the same two shipped budgets, 14
files of 1.14 MiB of directory each:

| directory budget | dir entries / hits / evictions | rare_scan p50 | reads / exec | fetched / exec |
|---|---:|---:|---:|---:|
| 64 MiB (default) | 14 / 388 / **0** | 160.2 ms | 84.0 | 35,592 |
| 4 MiB (3 of 14 fit) | 3 / 0 / **125** | 135.2 ms | 106.0 | 6,292,130 |
| 0 (no retention) | 0 / 0 / n/a | 138.9 ms | 112.0 | 7,954,450 |

The 4 MiB pass is the v1 arm's failure mode reproduced on the new cache: a
14-file plan round-robins through a budget that holds three, so the entry is
gone before it is reused — **0 hits, 125 evictions**, the same shape as the
parsed cache's 0/42/41. What it costs is the difference between the two formats.
Re-reading a 565 KiB directory costs milliseconds, so `rare_scan` stays at
135.2 ms and 0.08x the scan; re-decoding a 534 MiB parsed index costs seconds,
which is how the same pressure puts v1 at 22.1x. A thrashing directory cache
degrades to the unretained cost and no further, and unretained is still 12.7x
better than that pass's own scan control (138.9 ms against 1,761.1 ms).

Retention shows up in bytes rather than latency: 35,592 bytes per execution held
against 7,954,450 unretained, **223x**, for a 33% difference in read count and
none in wall time, because 7.95 MiB of local-file range reads costs less than
the jitter between runs on this box. That is the quantity an object store prices
and this measurement cannot (see below). The unretained pass is also the honest
per-shape cold column, since no earlier shape can have opened a directory for
it: `rare_scan` 112 reads and 7.95 MiB, `rare_keyword` 80 reads and 5.73 MiB,
`keyword_last5` 8 reads and 742 KiB.

The 4 MiB and 0 passes ran at a load average of 3.5 against the main pass's 1.8,
so read the counts and bytes from them rather than the milliseconds — their
`off` control moved too (`rare_scan` cold 3,372.5 ms against 1,656.6 ms).

### What this measurement is, and is not

- **Is:** the end-to-end query-path comparison #4376's acceptance asked for, at
  the budgets it named and with both of them off, cold and warm apart and under
  eviction pressure on each format's own cache, with exact-answer equality
  asserted per arm per shape, and with reads, bytes, resident memory and codec
  construction reported apart from latency.
- **Is not** a distributed, object-store or HTTP measurement. One process, a
  `file://` warehouse, no query server, no shards. The reads column is what the
  reader asked for, not what S3 would charge for it; a warm `rare_scan`'s 84
  reads of ~2.5 KiB would be 84 GETs, against the shipped path's fourteen large
  ones. Whether that trade holds at per-request latency is a prepared round's
  question and nothing here qualifies an AWS result.
- **Was not** a defaults change. The 2026-09-16 measurement harness produced
  its own seg1 sidecars, and neither shipped budget moved.
- **Is not** a writer benchmark. The construction numbers are sequential local
  fixture building, and the Parquet-decode half of them is an artifact of
  building sidecars after the fact.
- **The 2026-09-16 evidence carried one attribution defect; #5041 closes it.**
  A clipped `LIMIT` returned before its cancelled partitions finished loading
  indexes, so the original `on` samples included `keyword` at 12,566 / 6,222 /
  6,031 / 10.5 / 16.1 ms, and a later counter window could include their
  decodes. The original cold column, summed `on` decodes and disposition remain
  the dated evidence above. The claim that `substring_scan,seg`'s eight decodes
  came from the adjacent `policy` arm was too strong: that policy arm declines
  v1 loads, and adjacency does not identify which earlier execution started
  process-global work. Draining the segmented recorder per execution did not
  isolate its window either.

  `time_ab_shape` now retains the physical plan, stops the latency clock after
  result collection, and requires every scan partition to settle before it
  samples either the decode counters or the segmented recorder. The wait is
  outside query latency; a 30-second timeout aborts the measurement before the
  next arm starts.

  A 2026-09-18 rerun used the same retained 14 x 7.34M-row fixture with both v1
  caches at zero, so prior cache state could not make an execution warm. The
  five clipped `on` samples were:

  | shape | milliseconds |
  |---|---|
  | `keyword` | 8,272.0 / 10,365.1 / 10,835.9 / 10,759.2 / 10,568.6 |
  | `keyword_last25` | 3,481.9 / 3,502.7 / 3,560.6 / 3,589.2 / 3,522.1 |
  | `keyword_last5` | 3,696.4 / 3,471.1 / 3,382.0 / 3,805.2 / 3,444.9 |
  | `substring_scan` | 11,684.4 / 11,127.6 / 11,219.8 / 11,222.4 / 11,291.6 |
  | `rare_keyword` | 10,488.6 / 10,464.2 / 10,536.0 / 10,393.8 / 10,462.7 |

  Each row's maximum is less than 1.4x its minimum. Every `off`, `policy`,
  `seg` and `seg_policy` row reported `decodes=0`, including
  `substring_scan,seg`. A companion rerun at the deployed 1 GiB / 256 MiB
  budgets also gave every one of those arms zero decodes. Its two narrow-window
  `on` shapes each retained one fast sample and reported one cache hit; that is
  the mixed warm/cold state the cache is meant to create, separate from work
  arriving after settlement.

### 2026-09-18 writer-produced seg2 rerun (#5230)

The same 14 × 7.34M corpus was rerun after replacing the report-built seg1
arm with the streaming rewrite's seg2 output. Each day was appended in bounded
chunks and rewritten on its own with segmented writes enabled. The fixture
asserted one `siglake-inverted-seg-v2` blob for `raw` on every live file, no
whole-file v1 blob in that arm, and no live seg1 registration. All three arms
again had fourteen 7,340,000-row files and 92,325,000 Parquet bytes. Every
exact-answer and per-row clipped-answer check passed, every segmented lookup
reported no decline, and #5041's partition settlement ran before each counter
sample.

Five executions per shape used the deployed 1 GiB parsed / 256 MiB blob
budgets and the 64 MiB segmented-directory default. `seg2 ÷ off` compares the
two p50 columns from this run; the 2026-09-16 seg1 columns above remain the
prototype history.

| shape | off p50 | v1 p50 | policy p50 | seg2 p50 | seg2 ÷ off | seg2 policy p50 |
|---|---:|---:|---:|---:|---:|---:|
| `keyword` | 6.8 ms | 16,102.3 ms | 6.0 ms | 42.7 ms | 6.26x | 6.8 ms |
| `keyword_last25` | 15.8 ms | 39.0 ms | 17.8 ms | 32.9 ms | 2.08x | 18.8 ms |
| `keyword_last5` | 9.6 ms | 3,658.7 ms | 10.1 ms | 65.3 ms | 6.80x | 9.6 ms |
| `substring_scan` | 5.4 ms | 15,559.5 ms | 4.5 ms | 831.0 ms | 154.77x | 4.7 ms |
| `rare_scan` | 1,619.0 ms | 22,614.9 ms | 22,377.5 ms | 220.7 ms | **0.14x** | 161.2 ms |
| `rare_scan_last25` | 680.5 ms | 4,877.6 ms | 4,506.2 ms | 44.0 ms | **0.06x** | 41.0 ms |
| `rare_keyword` | 531.2 ms | 17,170.9 ms | 538.0 ms | 69.9 ms | **0.13x** | 545.9 ms |

The writer built the fresh seg2 arm in 725.7 s, including append, Parquet
rewrite and sidecar construction. Its registered statistics totaled
482,548,759 bytes against the off arm's 224,474,617 bytes: 17.58 MiB per file,
1.13x the whole-file v1 arm's 15.59 MiB per file. The segmented-directory cache
held all fourteen parsed directories in 27,883,741 bytes with zero evictions.
A warm `rare_scan` fetched 33,854 bytes in 84 reads; its cold execution fetched
5,409,847 bytes while opening the directories.

The format conclusion holds with the writer's bytes: both unclipped rare scans
beat the scan, and the clipped rare term would beat it if policy retained the
index. The three high-document-frequency clipped shapes and the substring
sweep still require the decline. These are local `file://` results, not an
object-store, distributed or HTTP qualification.

### 2026-09-18 document-frequency policy rerun (#5040)

The retained writer-produced seg2 fixture was queried again after carrying the
bare clip into the reader and setting the per-file point-term budget to
`summed df <= clip`. The whole-file v1 and ordered-limit declines did not
change. Five executions per shape used the same deployed cache budgets; every
exact-answer and clipped membership check passed.

| shape | scan p50 | seg2 p50 | df policy p50 | policy / scan | policy disposition |
|---|---:|---:|---:|---:|---|
| `keyword` | 7.1 ms | 49.3 ms | 6.8 ms | 0.95x | `clipped_document_frequency` |
| `keyword_last25` | 16.2 ms | 28.7 ms | 17.8 ms | 1.10x | `clipped_document_frequency` |
| `keyword_last5` | 10.4 ms | 78.5 ms | 10.6 ms | 1.01x | `clipped_document_frequency` |
| `substring_scan` | 5.9 ms | 915.4 ms | 4.0 ms | 0.67x | `clipped_estimate_unavailable` |
| `rare_scan` | 1,714.2 ms | 165.3 ms | 162.9 ms | 0.10x | admitted, unclipped |
| `rare_scan_last25` | 646.9 ms | 46.0 ms | 45.0 ms | 0.07x | admitted, unclipped |
| `rare_keyword` | 558.7 ms | 79.9 ms | **56.6 ms** | **0.10x** | admitted, summed df at or below 100 |

The decision's own reads were retained, including cold setup. `keyword`'s
first policy execution opened directories and stopped after 17 reads / 2.27
MiB; its four warm executions averaged 10.5 reads / 2.1 KiB. The windowed
common terms each proved over budget with one 200-205 byte dictionary-block
read per answered file once their directories were warm. The substring arm
read nothing below the held directory. `rare_keyword` paid 66 reads / 26,989
bytes cold and averaged 63 reads / 25,303 bytes warm, including its posting
spans. Thus the cold directory and dictionary work is visible in the same
range-read/fetched-byte accounting as admitted postings; a policy decline no
longer reports that estimate as free.

The clipped scan controls are small enough that scheduler variation is visible:
the two closest rows landed at 1.01x and 1.10x in this five-run pass, while the
ordinary whole-file policy controls landed at 1.04x and 1.27x. The categorical
result is unchanged: all four high-cost shapes declined, and the rare clipped
shape retained seg2 and recovered the measured scan loss. This remains a local
`file://` measurement, with no HTTP, distributed or object-store latency.

## Disposition for #4377: proceed, with two revisions

**Proceed.** The format does the thing it was designed for, measured through the
query path: the rare unclipped shapes go from 7.3-22.4x slower than a scan to
11.5-17.4x faster, the clipped rare shape #4375 has to decline goes to 11.1x
faster than the scan, the resident working set falls from 7.30 GiB to 15.95 MiB,
every answer is exact, and none of it depends on the two cache budgets a 4Gi
query pod cannot fund. Starved of its own budget it degrades to 0.08x the scan
instead of 22x it (0 hits and 125 evictions at 4 MiB, the parsed cache's failure
mode on a cache whose miss costs milliseconds). No cache sizing reaches that
result with the shipped format.

Both revisions identified by #4562 are now in code:

1. **#4988's per-block compression prerequisite is complete.** As prototyped
   the sidecar is 5.59x the on-disk bytes of the v1 one it replaces (87.19 MiB
   against 15.59 MiB per file). Seg2 writes 16.7 MiB at the codec fixture's
   7.34M-row scale and settles the posting-span checksum at the same block
   granularity. #4377 can build the versioned format without carrying seg1's
   storage regression into every compacted file.
2. **#5040 makes #4375's decline document-frequency aware for seg2.** A clipped
   whole-file v1 index still declines. Seg2 locates point terms in the dictionary
   blocks named by the directory, sums their df across selected groups, and
   fetches postings only when that sum is no larger than the clip. This moves
   `rare_keyword` to 56.6 ms while the high-df and substring shapes keep their
   scan fallbacks. The table and cost accounting above are the retained result.

**Not blocking, and still open:** the substring sweep reads the whole dictionary
and stays a decline; and the directory cache's value is measured in bytes and
requests, not local latency, so what it is worth depends on an object-store
round. The sync/async seam no longer holds a blocking thread for a lookup
(#5007, [staged reading](#reading-in-stages-not-on-a-blocking-thread-5007)),
and what it now leaves open is smaller: a stage re-decodes what the lookup
already holds, which only the substring sweep pays enough of to see.

## Acceptance sequence

#4376's acceptance — "the rare full scan beats OFF under a 1 GiB parsed /
256 MiB blob budget while the four LIMIT shapes are not regressed" — is an
end-to-end statement about the query path and cannot be settled by a codec. What
this prototype settles is that the format can be read in part, what one lookup
costs in reads and bytes, and that the resident working set falls by ~510x.
What remains, in order:

1. ~~**#4561**~~ — done, see [Reader integration](#reader-integration-4561):
   the sub-range read, the `RawPruneSpec` path, the row-domain check and the
   mixtures.
2. ~~**#5006**~~ — done, see [Holding the directory between
   lookups](#holding-the-directory-between-lookups-5006): a repeat lookup on a
   blob reads no trailer and no directory, under a byte budget of its own.
   At that point no writer produced a sidecar for a real table, so #4562's
   harness built one.
3. ~~**#4562**~~ — done, see [Through the query
   path](#through-the-query-path-scan-whole-file-segmented-4562) and the
   [disposition](#disposition-for-4377-proceed-with-two-revisions): the
   seven-shape harness with the third format, cold and warm apart, under 1 GiB
   parsed / 256 MiB blob and again with both off, plus the OFF control.
   **Proceed, with two revisions** — #4988 first, and a df-aware decline in
   #4375's rule.
4. ~~**#4988**~~ — done: seg2 compresses the dictionary block and its posting
   span independently, records stored and raw lengths, and verifies a CRC over
   the decoded posting span before slicing a term. Seg1 bytes are pinned by a
   fixture and remain readable. The 7.34M-row report writes 16.7 MiB and records
   fetched bytes for all six shapes.
5. ~~**#4377 / #5233 / #5234 / #5228 / #5260 / #5298**~~ — done: the
   streaming Parquet writer builds one seg2 group per row group, registers all
   completed output blobs in the rewrite transaction, and leaves the
   post-commit v1 rebuild no file to decode. The hermetic suite covers rolling output, separate partition
   rewrites, failed transactions, repeated rebuild, exact answers and
   row-group-bounded parsed index state, and passes. Reads and writes remain
   separate opt-ins. The 14 x 7.34M build time and peak heap are recorded in
   [Writer integration](#writer-integration-4377). #5228 preserves the rewrite's
   statistics entry when a later v1 rebuild finds an uncovered sibling, for a
   registration that follows the first one; #5298 moves that decision into the
   registration's own transaction action, so it is re-made against the base of
   every attempt and holds for two concurrent registrants — see [Which base the
   refusal is decided on](#which-base-the-refusal-is-decided-on). #5260
   derives the Puffin sequence from the rewrite snapshot on each refreshed
   transaction attempt; deterministic stale-base and failed-CAS regressions
   compare the snapshot, table metadata and physical footer. Both defaults stay
   off pending AWS qualification.
6. ~~**#5040**~~ — done: the bare clipped limit is carried into the reader.
   Point terms are located across the selected groups before any posting span
   is fetched and are admitted when their summed df is no larger than the
   clip. The cold directory and dictionary-block reads are charged to the
   estimate. Common terms decline as `clipped_document_frequency`; a substring
   declines as `clipped_estimate_unavailable` without sweeping the dictionary.
   Whole-file v1 and ordered-limit declines are unchanged.
7. ~~**#5007**~~ — done, see [Reading in
   stages](#reading-in-stages-not-on-a-blocking-thread-5007): a lookup's store
   waits happen between runs of the synchronous reader, so no lookup holds a
   blocking-pool thread, and a shape's rounds of waits are its stages rather
   than its reads. The staged shape was chosen over an async codec API, and the
   evidence for both the choice and its cost is in that section.
8. **An open question for #4561**: the substring sweep reads the whole
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
cargo test -p siglake-index --release --lib \
  report_posting_checksum_and_compression_options -- --ignored --nocapture
cargo test -p siglake-storage --test segmented_index_writer -- --test-threads=1
SIGLAKE_SEG_WRITER_FILES=14 SIGLAKE_SEG_WRITER_ROWS_PER_FILE=7340000 \
  cargo test -p siglake-storage --release --test segmented_index_writer \
  report_segmented_writer_build_cost -- --ignored --nocapture
```

The first two are sized by `SIGLAKE_SEG_ROWS_PER_FILE` (7,340,000 above), `SIGLAKE_SEG_GROUP_ROWS`
(1,048,576), `SIGLAKE_SEG_FILES` (14), `SIGLAKE_SEG_RUNS` (9),
`SIGLAKE_SEG_PLAN_RUNS` (3), `SIGLAKE_SEG_RARE_EVERY` (100,000),
`SIGLAKE_SEG_PARSED_BYTES` (1 GiB) and `SIGLAKE_SEG_BLOCK_BYTES` (4,096). Those
are the values every table above was taken at; the run takes 14 minutes, almost
all of it the v1 arm's 42 re-decodes per shape.

The third is the seg1 checksum and compression projection retained as the
decision's baseline: 14 s, sized by
`SIGLAKE_SEG_ROWS` (7,340,000), `SIGLAKE_SEG_GROUP_ROWS` (1,048,576),
`SIGLAKE_SEG_RARE_EVERY` (100,000) and `SIGLAKE_SEG_BLOCK_BYTES` (4,096). Its
byte counts reproduce the per-file section's exactly, which is what makes the
two sets of figures comparable.

The file's default `ROWS_PER_FILE` is 1,000,000, which finishes in about a
minute and is a useful negative control rather than a smaller version of the
result: a 70.8 MiB index means all fourteen fit the 1 GiB budget (28/14/0, zero
evictions) and **v1 wins every point shape** — 66 µs against the segmented
arm's 160 µs for the 14-file `rare_scan`, because a warm `BTreeMap` hit beats a
pair of range reads. One million rows is also a single row group, so it does not
exercise the reject path either. The measured claim is specifically about a
working set that exceeds the budget; where residency is free, this format costs.

The reader integration's fixtures are the iceberg fork's own unit tests
(`scripts/check-fork-tests.sh --fork iceberg`), and its measurement runs from a
kept mirror:

```
scripts/check-fork-tests.sh --fork iceberg --keep
cargo test --release --lib report_segmented_reader_read_cost -- --ignored --nocapture
```

sized by `SIGLAKE_SEG_READER_ROWS` (1,000,000) and `SIGLAKE_SEG_READER_GROUPS`
(8), 5.3 s at those values. It prints the cold and warm columns, the stages
table and the directory cache's footprint; running it under
`SIGLAKE_SEGMENTED_INDEX_DIRECTORY_CACHE_MAX_BYTES=0` is the negative control,
where the warm columns come back equal to the cold ones.

The blocking-pool claim is a unit test rather than a measurement
(`a_segmented_lookup_waits_on_the_store_without_a_blocking_thread`, 0.5 s in
the same fork run). Its arm against #4561's driver is not retained: it was
taken by copying `third_party/iceberg/src` aside, patching the
blocking-thread lookup back into the copy and running
`scripts/check-fork-tests.sh --fork iceberg --fork-src iceberg=<copy>/src`,
where the test times out at 60 s and the other 1,198 pass.

The query-path comparison (#4562) is `report_rebuild_on_off_text_shapes` in
`crates/siglake-storage/tests/puffin_rebuild.rs`, run three times over one kept
fixture. `SIGLAKE_SEGMENTED_INDEX_READS` has to be in the environment the
process starts in — the reader resolves it once — and
`SIGLAKE_REBUILD_AB_REUSE_DIR` is what keeps the 5 GB fixture across the three:

```
export SIGLAKE_SEGMENTED_INDEX_READS=1
export SIGLAKE_SEGMENTED_INDEX_WRITES=1
export SIGLAKE_REBUILD_AB_FILES=14 SIGLAKE_REBUILD_AB_ROWS_PER_FILE=7340000
export SIGLAKE_REBUILD_AB_RARE_EVERY=100000
export SIGLAKE_REBUILD_AB_PARSED_BYTES=1073741824
export SIGLAKE_REBUILD_AB_BLOB_BYTES=268435456
export SIGLAKE_REBUILD_AB_REUSE_DIR=$TMPDIR/4562/fixture

# deployed budgets + the packaged cache-off pass (a fresh writer-produced seg2
# arm took 12.1 min to append and rewrite on the 2026-09-18 run)
SIGLAKE_REBUILD_AB_RUNS=5 SIGLAKE_REBUILD_AB_PASSES=packaged=0:0 \
  cargo test -p siglake-storage --release --test puffin_rebuild \
  report_rebuild_on_off_text_shapes -- --ignored --nocapture

# the same, with the segmented format's own retention off and then under
# eviction pressure, 6 min each
SIGLAKE_REBUILD_AB_RUNS=3 SIGLAKE_SEGMENTED_INDEX_DIRECTORY_CACHE_MAX_BYTES=0 \
  cargo test …
SIGLAKE_REBUILD_AB_RUNS=3 SIGLAKE_SEGMENTED_INDEX_DIRECTORY_CACHE_MAX_BYTES=4194304 \
  cargo test …
```

The `seg` arm is accepted only when every live file has exactly one seg2 blob
for `raw`. A retained seg1 arm is refused with a request to rebuild it, so an
old #4562 fixture cannot be reported under the seg2 label. The controls set the
per-context writer override off while the segmented arm sets it on; the
`SIGLAKE_SEGMENTED_INDEX_WRITES=1` export documents the production opt-in whose
path the arm exercises.

Without `SIGLAKE_SEGMENTED_INDEX_READS` the run is #4375's four-arm one, which
is the negative control for the arm's existence: the `seg` arms disappear rather
than silently becoming scans. A run that keeps them but declines every file
fails on `seg.used > 0`.

The codec's own fixtures run in the crate's normal test pass
(`cargo test -p siglake-index`): v1 equivalence term by term, group-straddling
ordinals, empty and partial groups, the reject path's read count, the row-group
selection contract, malformed blocks, truncated and corrupt blobs, twenty
malformed directories re-encoded with recomputed CRCs, the two formats' mutual
refusal, unknown versions, the checksum against a written-out reference, the
block-size trade, and per-group construction from an existing index.
