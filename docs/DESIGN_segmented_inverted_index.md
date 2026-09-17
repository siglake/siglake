# Design — row-group-addressable inverted-index sidecars (#4376 prototype)

Status (2026-09-17): **prototype, nothing wired.** The codec and its reader are
`siglake_index::segmented`; the writer, the scan path and every default are
untouched. This document is the format decision #4377 needs ahead of building
postings during a streaming merge, and the specification the remaining slices
implement: #4560 (the codec, its fixtures and the format's open questions —
settled below), #4561 (reader integration and bounded partial reads), #4562
(the measured proceed/revise/reject disposition).

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
different, structurally valid row set. It is a wrong answer, not a wider one, and
a row the scan never decodes is not recovered by re-checking the predicate above
it. Nothing short of a checksum over the postings sees it.

A mis-addressed posting range is the same hole reached from the other side, and
a fixture pins what it does: a block's `postings_base` moved by one byte answers
row 23 for a term whose row is 22
(`a_directory_that_misaddresses_a_block_is_unanswerable_not_absent`). That one is
not corruption — the directory is checksummed, so it can only arrive from a
writer that computed the offset wrong — but it decodes the stated document
frequency, ascending and in domain, and is therefore indistinguishable from a
correct answer by everything the format checks.

### Do posting sections need their own checksum?

Not in `seg1`, and the reason is a number rather than a principle. Measured at
the per-file scale above (`report_posting_checksum_and_compression_options`):

| option | cost on disk | what a point lookup fetches per group | catches the residual |
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

The prototype takes neither, because nothing reads it yet and the exposure
equals the shipped format's at the codec level. Two things should reopen it,
and both belong to #4562's disposition rather than here:

- **Registering uncompressed removes a checksum that exists today.** The v1
  sidecar travels inside a Zstd frame written with `include_checksum(true)`
  (`third_party/iceberg/src/compression.rs`), so in deployment a flipped bit
  anywhere in it fails decompression and the file is scanned. An uncompressed
  segmented blob has no such cover, and its posting sections are then the only
  part of it no checksum spans.
- **Per-block compression wants the same granularity** (see
  [Publication semantics](#publication-semantics-what-4377-needs)), so a
  version that compresses per block gets the checksum at no additional read
  cost — the span is already the fetch unit.

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
- Peak writer memory is one row group's postings plus its dictionary entries —
  not the file's. `SegmentedWriter::push_group_index` takes an index built over
  exactly one group's rows, which is what a merge already has in hand when it
  flushes a row group.
- The blob is complete only once the trailer lands, so a partial upload is not
  mistakable for an index: the reader looks for the trailer's magic at a fixed
  offset from the end, and a truncated blob does not have it.

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
(#4329: 229 MB of statistics increment over 14 files).

### What per-section compression would recover

`seg1` stores every section uncompressed, and that is a prototype decision, not
a property of the layout. Measured on the same file, at the zstd level Puffin
uses (3):

| | bytes | ratio |
|---|---:|---:|
| `seg1` as written | 85.8 MiB | 1.00x |
| zstd over the whole blob (what a v1 sidecar pays, and what a range reader cannot use) | 16.3 MiB | 0.19x |
| zstd per dictionary block | 2.5 MiB of 35.9 | 0.07x |
| zstd per block posting span | 13.4 MiB of 49.4 | 0.27x |
| per-block total, directory uncompressed | **16.4 MiB** | **0.19x** |

Compressing at block granularity recovers the whole storage gap — 16.4 MiB
against 16.3 MiB for whole-blob zstd, within 0.6% — while every section stays
reachable by a range read. It is the same trade the per-block posting checksum
asks for and for the same reason: the block's span becomes the unit the reader
fetches whole, since nothing can be sliced out of a compressed block. A
directory field per block would carry the compressed and raw lengths; the byte
ranges the directory addresses do not change shape.

That is a `seg2` question, deliberately left to #4562's disposition: it costs a
decompression per lookup, the 1.58x fetched bytes above, and a format field that
`seg1` has no reader for. What the measurement settles is that "85.8 MiB against
16 MiB" is not an argument against the layout — it is the cost of this
prototype's simplest choice.

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
  spends 35.6 ms cold and 1.07 ms warm. That makes the acceptance plausible and
  does not establish it: the scan-side term could dominate both.
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
3. **A `seg2` question for #4562's disposition**: per-block compression and a
   per-block posting checksum, which are one decision — both need the block's
   posting span to be the unit the reader fetches whole, and the measured price
   of that is 1.58x the bytes a point lookup fetches per group. What they buy is
   16.4 MiB per file instead of 85.8, and the end of the residual above.
4. **An open question for #4561**: the substring sweep reads the whole
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
```

The first two are sized by `SIGLAKE_SEG_ROWS_PER_FILE` (7,340,000 above), `SIGLAKE_SEG_GROUP_ROWS`
(1,048,576), `SIGLAKE_SEG_FILES` (14), `SIGLAKE_SEG_RUNS` (9),
`SIGLAKE_SEG_PLAN_RUNS` (3), `SIGLAKE_SEG_RARE_EVERY` (100,000),
`SIGLAKE_SEG_PARSED_BYTES` (1 GiB) and `SIGLAKE_SEG_BLOCK_BYTES` (4,096). Those
are the values every table above was taken at; the run takes 14 minutes, almost
all of it the v1 arm's 42 re-decodes per shape.

The third is the checksum and compression table: 14 s, sized by
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

The codec's own fixtures run in the crate's normal test pass
(`cargo test -p siglake-index`): v1 equivalence term by term, group-straddling
ordinals, empty and partial groups, the reject path's read count, the row-group
selection contract, malformed blocks, truncated and corrupt blobs, twenty
malformed directories re-encoded with recomputed CRCs, the two formats' mutual
refusal, unknown versions, the checksum against a written-out reference, the
block-size trade, and per-group construction from an existing index.
