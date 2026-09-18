# Design — per-file inverted index (WS-5 search arc)

Status (2026-09-14): **slices A + B shipped + tested** (build at write time,
consume at query time). Footer indexes now default on after a bounded local
write-cost and byte-size qualification. Post-rewrite Puffin rebuild ships
**off** for 0.1.0; turned on, it skips files that already carry a footer or
Puffin index. **Slice C = AWS validation** remains: run
`scripts/ws5-validate.sh` against a cluster with the defaults enabled.

## What exists (shipped)

`siglake-index::InvertedIndex` — per-file `term → ascending row-ordinal postings`,
`doc_id == file-local row ordinal`:

- **Build:** `IndexBuilder::push_row(raw)` / `InvertedIndex::from_rows(..)`.
  Tokenization is shared with the blooms (`siglake_bloom::tokenize` /
  `normalize_query_term`), so a term a bloom admits is looked up here under the
  same normalization — no index/bloom skew.
- **Query:** `postings(term)`, `matching_rows_all(terms)` (AND = sorted-merge
  intersection; any absent term ⇒ definitive no-match).
- **RowSelection bridge:** `matching_row_selection(terms) -> Vec<(bool, u32)>` —
  `(selected, length)` runs over the whole file, a Parquet-agnostic
  `RowSelection`.
- **Serialization:** `to_bytes`/`from_bytes` — magic/version + delta-varint
  postings, for a Puffin-style side blob.

## Integration (shipped)

### Slice A — build at write time ✅

The writer builds the index unless `SIGLAKE_INVERTED_INDEX=0` (Helm
`compactor.invertedIndex.enabled: false`; operator
`spec.extraEnv` with the same env opt-out). It indexes the events table's `raw`
column and stamps the hex blob into the footer KV
(`siglake_index::INVERTED_INDEX_KV_KEY`) at the single-writer + partition-split
commit paths. Metrics `siglake_index_build_{seconds,bytes}`. The streaming
re-cluster writer rebuilds the index for its committed output files, so both
flush and re-cluster paths retain exact query results and indexed output.

### Post-rewrite Puffin rebuild ✅

`SIGLAKE_INDEX_REBUILD` defaults **off** (2026-09-14, #4162). The Helm opt-in
is `compactor.indexRebuild: true`; operator-managed clusters use
`spec.extraEnv: [{name: SIGLAKE_INDEX_REBUILD, value: "1"}]`. Chart and
operator render the variable in both states, so a pod's env says which way it
runs rather than leaving the reader to know the binary's default.

Why off: a parsed index costs about 40 bytes per indexed row (≈294 MB for a
7.3M-row compacted file, `third_party/iceberg/src/arrow/reader.rs`), so a
50G-class text plan over 14 such files needs ~4 GB of parsed index against a
1 GiB cache and a 256 MiB blob cache. AWS rounds 78, 80 and 81 put `keyword`,
`keyword_last25`, `keyword_last5` and `substring_scan` 2-45x over ceilings that
were measured on the scan path on 2026-09-03. Nothing about reads changes:
indexes already registered are still discovered and used, and the flush path's
footer index is still written. Only new sidecars after a rewrite stop.
Index-path performance is 0.1.1 work.

Streaming rewrite outputs carry row-group blooms and aggregate footers but no
whole-file inverted index, so they require a Puffin sidecar. In-memory rewrite
outputs use the normal writer: an index at or below
`SIGLAKE_INDEX_FOOTER_MAX_BYTES` is already in the Parquet footer, while a
larger one still requires Puffin registration. Before decoding a file, the
rebuild checks each configured column against registered Puffin blobs and the
file footer. A second pass therefore registers no duplicate statistics file.
Iceberg permits only one statistics file per snapshot. If a rebuild finds
missing coverage on a snapshot that already has one, Siglake preserves the
registered file and defers the new blobs; it logs their data-file paths and
increments
`siglake_index_registration_deferred_total{reason="snapshot_has_statistics"}`.
The uncovered files remain scan-readable and can gain an index from a later
rewrite or a CLI rebuild after a later snapshot.

The registration is first-writer-wins under concurrency too. The absence check
is made by `RegisterFirstStatisticsAction` inside the registration
transaction, so it is re-made against the base of every commit attempt: a
competitor that arrives between the caller's load and the transaction's
refresh, or inside a lost attempt's CAS window, defers the caller instead of
replacing what the competitor registered. A deferral is counted once per
registration call, not once per attempt, and the sidecar the deferred caller
had already uploaded is left unreferenced for `gc_orphans`.

#### The local on/off measurement (2026-09-14)

`report_rebuild_on_off_text_shapes` in `tests/puffin_rebuild.rs` writes the
same corpus twice — once with the rebuild on, once off — and times the four
shapes the 50G gate fails. Both arms are freshly written and end with the same
Parquet layout (4 files × 7,340,000 rows, 39,825,917 bytes, one day partition
per file); the only difference is the sidecars. Release build, this box, nine
executions per shape per arm, arms interleaved, text-index caches at their
deployed defaults:

| shape | rows | off cold | off p50 | on cold | on p50 | on ÷ off |
|---|---|---|---|---|---|---|
| keyword | 100 | 27.9 ms | **3.7 ms** | 4365.0 ms | 9.2 ms | 2.5x |
| keyword_last25 | 100 | 14.6 ms | **10.2 ms** | 5575.1 ms | 69.4 ms | 6.8x |
| keyword_last5 | 100 | 10.5 ms | **8.6 ms** | 47.8 ms | 57.6 ms | 6.7x |
| substring_scan | 100 | 2.2 ms | **3.4 ms** | 284.5 ms | 271.7 ms | 79x |

The indexed arm's cold executions are the whole-index decode: 4.4 s and 5.6 s
for the first two shapes. Its warm p50 is not stable either — the run ends with
one 913 ms `keyword` and one 4186 ms `keyword_last25` sample, and the process
finishes holding **one** cached parsed index for four files. That is the
deployed 1 GiB budget against four ~294 MB indexes, which is the same eviction
the 50G rounds hit at 14 files. Reproduce with:

```
cargo test -p siglake-storage --release --test puffin_rebuild \
  report_rebuild_on_off_text_shapes -- --ignored --nocapture
```

`SIGLAKE_REBUILD_AB_{FILES,ROWS_PER_FILE,RUNS}` size it (defaults 4 /
1,000,000 / 9); `SIGLAKE_REBUILD_AB_{PARSED,BLOB}_BYTES` scale the caches down
with the corpus when a smaller one has to stand in for a deployed
working-set-to-cache ratio. What this does NOT measure: the HTTP query server,
object storage (the warehouse is local files), distribution across shards, and
a corpus of the round's width — 29.4M rows here against the round's ~98M.

#### The 0.2.0 regime-boundary measurement (2026-09-15, #4329)

The extended comparison adds `rareneedle` to one row in 100,000 and times two
unclipped `match_terms` shapes beside the four release shapes. The full-width
run used 14 files × 7,340,000 rows = 102,760,000 rows per arm. Both arms had
the same 14-file Parquet layout and 138,704,723 Parquet bytes. The OFF fixture
built in 574.7 s; the ON fixture built in 729.0 s and registered all 14
sidecars. The statistics files occupied 224,473,769 bytes OFF and 453,404,387
bytes ON. Build times are observations from sequential local fixture creation,
not a controlled writer-cost benchmark.

Release build, nine executions per shape, arms interleaved. The first pass used
the deployed 1 GiB parsed-index and 256 MiB Puffin-blob budgets:

| shape | corpus matches | selectivity | OFF cold | OFF p50 | ON cold | ON p50 | ON ÷ OFF | ON decodes / hits |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| keyword | 2,055,200 | 2% | 33.9 ms | 5.9 ms | 11,222.6 ms | 16,509.8 ms | 2783x | 100 / 1 |
| keyword_last25 | 513,799 | 0.5% | 8.2 ms | 9.0 ms | 6,118.8 ms | 4,345.2 ms | 480x | 19 / 4 |
| keyword_last5 | 102,759 | 0.1% | 12.4 ms | 14.0 ms | 1,536.1 ms | 8,568.8 ms | 613x | 21 / 2 |
| substring_scan | 5,138,000 | 5% | 7.6 ms | 6.8 ms | 16,640.3 ms | 14,710.5 ms | 2172x | 84 / 1 |
| rare_scan | 1,028 | 0.001% | 1,671.8 ms | 1,801.2 ms | 50,486.7 ms | 34,211.1 ms | 19.0x | 121 / 10 |
| rare_scan_last25 | 257 | 0.00025% of corpus | 583.9 ms | 583.1 ms | 6,547.6 ms | 4,826.1 ms | 8.28x | 31 / 5 |

The pass ended with one 559,896,106-byte parsed index resident, 388 evictions,
and no oversized-entry skips. Its optional 12 GiB follow-on pass was killed by
the local memory limit before printing a sample. The completed fixture was
then reopened in a fresh process with an 8 GiB parsed budget and a 2 GiB blob
budget. All 14 parsed indexes occupied 7,830,305,508 bytes; there were zero
evictions:

| shape | OFF cold | OFF p50 | ON cold | ON p50 | ON ÷ OFF |
|---|---:|---:|---:|---:|---:|
| keyword | 38.8 ms | 6.1 ms | 9,016.1 ms | 20.3 ms | 3.31x |
| keyword_last25 | 8.2 ms | 9.7 ms | 4,303.9 ms | 13.1 ms | 1.36x |
| keyword_last5 | 12.1 ms | 11.6 ms | 4,057.6 ms | 50.1 ms | 4.33x |
| substring_scan | 4.4 ms | 4.5 ms | 868.0 ms | 813.1 ms | 182x |
| rare_scan | 1,659.8 ms | 1,733.5 ms | 3,637.6 ms | **129.3 ms** | **0.075x** |
| rare_scan_last25 | 581.9 ms | 529.3 ms | 37.9 ms | **38.1 ms** | **0.072x** |

The current format therefore has a measured winning regime: after its entire
7.83 GiB parsed working set is warm, a 0.001%-density term over an unclipped
scan is 13.4-13.9x faster than the scan path. Its first full-corpus execution
still loses because it decodes the indexes. Under the deployed cap, eviction
removes the win and makes the same rare shapes 8.3-19.0x slower.

Residency does not protect the release shapes. Their resident ON p50 values
(20.3, 13.1, 50.1 and 813.1 ms) all exceed the 0.1.0 ceilings (3.7, 10.2, 8.6
and 3.4 ms). `substring_scan` is especially expensive because selecting 5% of
the rows costs more than its early `LIMIT 100` scan. A redesign needs both a
per-execution decline for clipped/high-document-frequency shapes and a layout
that can reach sparse postings without materializing every file's index. Cache
sizing alone asks a 4 GiB query pod to retain a 7.83 GiB parsed set and cannot
be the long-term fix.

Three 0.2.0 slices carry those two requirements and the build cost behind them:
#4375 declines the whole-file index for a clipped `LIMIT` shape per execution,
#4376 prototypes a row-group-addressable sidecar the reader can touch in part,
and #4377 builds the postings during the streaming merge so the post-commit
decode pass disappears. None of them changes a 0.1.0 or 0.1.x default.

#4376's format decision, its prototype codec and its measurements live in
[`DESIGN_segmented_inverted_index.md`](DESIGN_segmented_inverted_index.md):
per-row-group postings and dictionary blocks addressed by byte range, with the
directory and trailer written last so a merge can emit the blob in one forward
pass. The format has its own magic, footer-KV key and Puffin blob type, so this
reader does not see one — and #4561 gave the scan path a second reader that
does, behind `SIGLAKE_SEGMENTED_INDEX_READS` and with no writer producing the
format, so every default is unchanged. Measured on one 7,340,000-row file
from the same corpus: 526.0 MiB parsed for the whole-file index against
474.9 KiB of resident directory, and a rare term answered from 14 range reads
of 9.8 KiB rather than a 3.27 s whole-file decode.

#4560 settled the format's own questions on top of that prototype, and the
answers are recorded in that document rather than here. Version identification
is four separate discriminators (magic, a version byte at both ends, its own
Puffin blob type and footer-KV key), so a 0.1.x reader never sees a segmented
sidecar and a segmented reader refuses v1 bytes. Row-group boundaries and
ordinals: group `i` **is** Parquet row group `i`, postings are stored
group-relative and returned file-physical, and the directory states every
group's row count so the sidecar can be checked against the file's actual row
groups rather than against one stamped `row_group_size`. Directory offsets are
checked against an exact tiling of the blob body, so every section's range is
pinned by its neighbours. Compatibility is per-file metadata and never table
state: a table may
carry both kinds at once, with no migration, and a file with neither is
scanned. The lookup API is three-valued —
`Lookup::{Rows, Absent, Unanswerable}` — because a partial reader fails per
lookup, where this format's decoder can only fail at `from_bytes`; only
`Unanswerable` may fall back to a scan and only `Absent` licenses skipping
rows. Posting sections carry no checksum in `seg1` and sections are stored
uncompressed; both were priced rather than assumed (4 bytes per term is 32.6%
of the blob, while block-granularity checksums and compression together cost
0.1% and take the blob from 85.8 MiB to 16.4 MiB at 1.58x the bytes a point
lookup fetches), and both are `seg2` questions for #4562's disposition (#4988).

#4561 wired the reading half: a sub-range read against the Puffin statistics
file (`PuffinReader::blob_range_reader`, uncompressed blobs only), discovery by
blob type per file, the directory checked against the file's Parquet row
groups, and AND/OR/substring answered over the row groups the scan kept — with
every outcome the sidecar cannot conclude falling back to this reader or to an
exact scan. #5006 then gave the parsed directory a byte budget of its own so a
repeat lookup reads neither trailer nor directory.

#4562 ran the comparison that decides #4377 — the same seven shapes, five arms,
through the query path on this corpus, at the deployed budgets and again with
both of them zero. Its disposition is **proceed with two revisions**: #4988's
per-block compression first, because the prototype costs 5.59x the v1 sidecar's
bytes on disk, and a document-frequency-aware decline in #4375's rule, because
declining every clipped `LIMIT` costs `rare_keyword` an 11.1x win the segmented
format can now deliver. The tables are in
[`DESIGN_segmented_inverted_index.md`](DESIGN_segmented_inverted_index.md).

#### The per-execution policy arm (2026-09-16, #4375)

The same completed 14-file fixture was reopened with an 8 GiB parsed-index
budget and a 2 GiB blob budget. A third `policy` arm queried the indexed
warehouse through the per-execution session hint: the four clipped shapes
declined the whole-file index, while the two unclipped rare scans kept it.
Nine executions per shape and arm, interleaved, produced:

| shape | OFF p50 | indexed p50 | policy p50 | registered 50G ceiling | policy path |
|---|---:|---:|---:|---:|---|
| keyword | 6.0 ms | 24.4 ms | **7.5 ms** | 10 ms | declined |
| keyword_last25 | 9.4 ms | 13.6 ms | **10.1 ms** | 25 ms | declined |
| keyword_last5 | 11.3 ms | 50.3 ms | **10.8 ms** | 30 ms | declined |
| substring_scan | 4.9 ms | 869.2 ms | **5.4 ms** | 10 ms | declined |
| rare_scan | 1,757.2 ms | 134.8 ms | **126.3 ms** | — | allowed |
| rare_scan_last25 | 535.2 ms | 38.6 ms | **39.4 ms** | — | allowed |
| rare_keyword | 532.5 ms | 45.8 ms | **531.2 ms** | — | declined |

The ceiling column states the AWS guardrails registered in the benchmark
project at `docs/predictions/50g-regression.json`; it is separate from the
four-file local OFF medians above. All four clipped policy results are below
those guardrails, but this local-filesystem run does not qualify an AWS result.

All 14 parsed indexes occupied 7,830,305,508 bytes with no eviction. The
unclipped `rare_scan` policy arm recorded 126 parsed-cache hits and returned all
1,028 matches, row for row equal to the OFF arm. The isolated policy test also
checks each clipped `match_terms`, `LIKE`, and time-window plan for
`text_index:[declined:clipped_limit]`, then proves that the indexed warehouse
performs neither a lookup nor a decode and returns the same rows as its
unindexed control. The query-server request test covers the implicit
newest-first rewrite separately: it takes the existing `ordered_limit` refusal
and records one reason rather than both.

`rare_keyword` states the conservative rule's cost. Its limit clips the scan,
so the policy declines it even though this fixture's 0.001%-density term would
have won from a resident index. Document frequency lives inside the whole-file
index being declined; choosing by it would first pay the load this policy
avoids. #4376's segmented reader is the planned way to reduce that cost.

Reproduce the deployed pass with the command above plus
`SIGLAKE_REBUILD_AB_FILES=14`, `SIGLAKE_REBUILD_AB_ROWS_PER_FILE=7340000`,
`SIGLAKE_REBUILD_AB_RARE_EVERY=100000`,
`SIGLAKE_REBUILD_AB_PARSED_BYTES=1073741824`, and
`SIGLAKE_REBUILD_AB_BLOB_BYTES=268435456`. A completed fixture can be re-timed
by setting `SIGLAKE_REBUILD_AB_REUSE_DIR` to its root containing `off/` and
`on/`. This remains local evidence: it does not cover HTTP, object storage,
distributed execution or AWS, and it does not qualify a new default.

#### The parsed-index ceiling A/B (2026-09-17, #4102)

`derive_text_index_cache_bytes` caps the derived parsed-index budget at 1 GiB
(`crates/siglake-storage/src/iceberg.rs`). #4102 asked whether a pod with room
should be allowed more. The same completed 14-file fixture was re-timed at two
budgets — the deployed 1 GiB / 256 MiB pair and an 8 GiB / 2 GiB pair that
holds the whole set — one process per budget, so the difference between their
resident sets is the budget's rather than a high-water mark both passes share.
Three passes each, alternating, so a drifting box could not favour one. Release
build, five executions per shape and arm.

The sizing reproduced #4329's exactly. At 8 GiB all fourteen parsed indexes
were resident at 7,830,305,508 bytes in every pass, with zero evictions. At
1 GiB the cache held **one** index, 559,896,106 bytes, and took 325, 337 and
349 evictions. Two of those entries are 1,067.9 MiB, so the 1 GiB cap misses
holding a second by about 44 MiB, and a fourteen-file plan takes every lookup
as a miss.

| shape | OFF p50 | indexed p50 @1 GiB | indexed p50 @8 GiB | policy p50 @1 GiB | policy p50 @8 GiB | policy decodes 1 / 8 |
|---|---:|---:|---:|---:|---:|---|
| keyword | 7.5 ms | 7,108.9 ms | 26.8 ms | 9.9 ms | 9.9 ms | 0 / 0 |
| keyword_last25 | 20.9 ms | 7,066.4 ms | 25.3 ms | 19.7 ms | 19.2 ms | 0 / 0 |
| keyword_last5 | 10.9 ms | 8,605.3 ms | 63.5 ms | 10.8 ms | 9.4 ms | 0 / 0 |
| substring_scan | 5.2 ms | 15,225.4 ms | 807.4 ms | 7.0 ms | 6.6 ms | 0 / 0 |
| rare_scan | 1,711.7 ms | 50,393.7 ms | 125.8 ms | 22,798.9 ms | 120.2 ms | 69 / 0 |
| rare_scan_last25 | 673.1 ms | 6,079.5 ms | 34.5 ms | 4,843.4 ms | 38.2 ms | 15 / 0 |
| rare_keyword | 542.2 ms | 16,238.8 ms | 43.8 ms | 549.5 ms | 529.6 ms | 0 / 0 |

Each cell is the median over the three passes at that budget; the OFF column is
the median over all six, since it reads no index and both budgets should give
it the same number.

The policy columns are the path the query server takes, and they answer the
card. #4375's per-execution decline fires the same way at both budgets — zero
decodes on all five clipped shapes either way — so the four shapes the 50G gate
measures sit within noise of each other at 1 GiB and at 8 GiB. Raising the
ceiling buys them nothing, because they never load an index for a cache to
hold. What it buys is the unclipped regime, where the decline keeps the index
and the 1 GiB budget then evicts it: `rare_scan` goes from 22,798.9 ms to
120.2 ms (190x) and `rare_scan_last25` from 4,843.4 ms to 38.2 ms (127x).

The 8 GiB budget is not free of the pod either. Its passes held 20.2-21.3 GiB
resident against 14.3-15.6 GiB at 1 GiB on the same fixture, and #4329's
attempt at a 12 GiB budget was killed by the box before it printed. Reaching
7.83 GB through the 1/16 derivation would take a pod of about 117 GiB even with
the cap removed.

The ceiling therefore stays at 1 GiB, which is now a measured decision rather
than an unmeasured one: it costs the shipped shapes nothing, the whole-file
format is what a query pod cannot afford (#4376), and a deployment whose text
queries are unclipped rare-term scans has
`SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES` to raise it by hand. Reproduce either
budget with the command above, `SIGLAKE_REBUILD_AB_REUSE_DIR` pointed at a
completed fixture, and `SIGLAKE_REBUILD_AB_PARSED_BYTES` /
`SIGLAKE_REBUILD_AB_BLOB_BYTES` set to the pair being measured; the run prints
`rss_<pass> bytes=… peak_bytes=…` beside each footprint. This is local evidence
again: it does not cover HTTP, object storage, distributed execution or AWS.

`tests/puffin_rebuild.rs` covers both rewrite paths under the opt-in, the
default-off path (a rewrite leaves its output unindexed, and a table indexed
before the rewrite still serves index reads), exact filtered and unfiltered
counts, and snapshot expiry. Iceberg
snapshot expiry retains the registered statistics metadata in the current
implementation, so the Puffin sidecar stays discoverable and a later rebuild
remains a no-op.

### Slice B — consume at query time ✅

`Reader::inverted_index_row_selection` (in the vendored `arrow/reader.rs`):
for a normalizable `raw LIKE '%substr%'`, loads the blob, `rows_containing`, and
builds a `RowSelection` over the matching file-physical ordinals — one run per
matching stretch within each selected row group (`row_selection_runs`), in the
same shape `build_deletes_row_selection` produces for a delete vector, which is
what this reused with the complement until #3896. The selection is a superset
(the engine's `FilterExec` re-checks the exact `LIKE` above the scan),
intersected with any predicate/delete selection.
Metric `siglake_iceberg_inverted_index_used_total` + selected/file-row
histograms, and — since #3969 —
`siglake_iceberg_text_index_startup_seconds{stage,storage}` around the four
sections of that work separately (`permit_wait`, `blob_fetch`, `decode`,
`selection`), with the parsed-index cache's lookup outcomes and eviction
reasons beside it. One total could not say which section a regression was in:
run #73 measured about 30 ns per file row before a first batch and the round's
artifacts could not attribute it. Differential storage test
(`tests/inverted_index.rs`) asserts ground-truth-correct counts across
answerable substrings, fragments, absent terms, a delimiter-bearing fallback,
and a dimensional-predicate intersection, with many row groups + bloom-skip
active.

### What a v1 blob has to prove before it prunes (2026-09-17, #4558)

A footer KV and a Puffin blob are both bytes off storage, so every length in
them is untrusted input. Two layers check them, and a blob that fails either is
not an error — the file falls back to an exact scan, which is slower and right.

**`InvertedIndex::from_bytes`** refuses, before allocating anything from a
serialized count:

- a count no remaining payload could cover (`n_terms` against
  `remaining / 3` — the cheapest entry is a zero-length term, a posting count
  and one delta byte; `plen` and `tlen` against the bytes left), so a nine-byte
  blob claiming 2^32 postings costs nine bytes of work rather than a 16 GiB
  `Vec::with_capacity`;
- a value that does not fit the field it is read into (`n_rows`, a posting
  delta and the running ordinal are all `u32`; they were narrowed with `as`);
- a varint that does not round-trip a `u64` — more than ten bytes, or a tenth
  byte carrying more than the one payload bit that fits (the shift dropped
  those bits, so `2^64 + 1` decoded as `1`);
- a repeated or descending term (the encoder walks a `BTreeMap`, so terms are
  strictly ascending; `insert` used to collapse a duplicate silently);
- an empty postings list, a posting at or past `n_rows`, or a non-first delta
  of zero, all of which break "postings are strictly ascending file-local row
  ordinals";
- trailing payload after the last dictionary entry.

Valid v1 bytes are unaffected — the encoder has always held every one of these.
An empty term stays legal: the `raw` tokenizer emits one for a null value.

**The reader** then matches the index's row domain against the Parquet file
(`ArrowReader::index_covers_file`) before either a freshly decoded or a warm
cached index reaches `inverted_index_row_selection`, on both the footer-KV and
the Puffin path, counting `siglake_index_row_domain_mismatch_total{storage}`.
`from_bytes` only proves the postings are inside the *index's* own `n_rows`,
which says nothing about the file they are about to prune, and
`index_matches_row_selection` drops ordinals it cannot place — so an index over
20 rows stamped onto a 30-row file skips rows 20..30 before decode and **loses
the matches there**, rather than merely admitting extra rows. Both regressions
(`siglake-storage/tests/inverted_index_row_domain.rs`,
`tests/puffin_index_row_domain.rs`) put a match in the file's final row group
and past the index's domain, and fail against the pre-fix reader by returning
two matching rows of four and one of two respectively.

What neither layer catches is a flipped bit inside a posting delta that leaves
the ordinals ascending and in range: v1 carries no checksum. The exhaustive
single-bit sweep in `docs/DESIGN_segmented_inverted_index.md` under "Integrity"
prices what changed — over 112,304 flips of a 14,038-byte blob, refusals went
from 28,050 to 99,019, answers that came back wrong from 142 to 120, ordinals
outside the row domain from 22 to **0**, and a present term reported absent
from 135 to 29. The 120 are the residual.

### Which storage path carries that residual, and what closing it costs (2026-09-18, #4991)

The residual above is a property of the *format*. What a query does with a
corrupt blob depends on how the blob is stored, and the two paths differ:

- **Footer KV** — hex under `siglake.inverted_index.v1[.column]`, taken per
  column while that column's serialized index fits
  `SIGLAKE_INDEX_FOOTER_MAX_BYTES` (1 MiB by default), so it carries the small
  and freshly written files. Parquet
  checksums data pages, not footer metadata, and hex is an encoding rather
  than a check. Nothing covers these bytes.
- **Puffin sidecar** — the spillover above that threshold, so it carries the
  large compacted files (and nothing at all for a rolled write, whose
  spillover `sidecar_blob_specs_for_data_files` declines). `crates/siglake-storage/src/iceberg.rs` registers the
  blob with `PuffinCompressionCodec::Zstd`, and the fork's encoder sets
  `include_checksum(true)` (`third_party/iceberg/src/compression.rs`). A
  corrupt stored byte hits the frame's content checksum first.

Single-bit sweep of each stored form, 1,000 rows / 1,010 terms, classified
against both of #4558's layers
(`siglake-index/tests/v1_integrity_measure.rs::report_stored_byte_corruption_by_path`).
"Lost a match" is any `(term, row)` the sound blob named and the corrupt one
does not; the last column is the narrower per-query view #4560 reported, for
one probe term:

| stored form | bytes | flips | refused decoding | refused row domain | absorbed | lost a match | lost the probe term's rows |
|---|---:|---:|---:|---:|---:|---:|---:|
| v1 payload, uncompressed | 14,023 | 112,184 | 98,969 | 8 | 0 | 13,207 | 149 |
| footer KV, hex as stored | 28,046 | 224,368 | 212,897 | 4 | 1,035 | 10,432 | 125 |
| Puffin sidecar, zstd-3 frame as stored | 2,486 | 19,888 | 19,878 | 0 | 10 | **0** | **0** |
| control: the same frame, content checksum off | 2,482 | 19,856 | 13,958 | 5 | 10 | 5,883 | 20 |

The report prints one more column, dropped here because it was zero on all
four forms: a corruption that decoded to a *superset* — a different index that
still names every `(term, row)` the sound one did. The encoding is dense enough
that a flip surviving the decoder always costs something.

The control row is what the four checksum bytes buy: zstd framing alone lets
29.6% of flips through as a different index. The hex form refuses 94.9%
(212,897 of 224,368), most of them by alphabet — a flipped high bit usually
leaves `[0-9a-f]` — and the rest by the decoder, as the payload row's are. The
1,035 absorbed flips can only be letter case: `from_hex` reads a digit with
`to_digit(16)`, which accepts `A-F` though `to_hex` writes lowercase, and any
flip that changes a *nibble* changes the blob. So the doubling is an encoding,
not a check — the flips that stay in the alphabet are nibble corruptions and
reach the decoder.

"Lost a match" covers the whole dictionary and the last column one term of
1,010, which is most of the gap between them. The worst of the class is a flip
inside a term's *characters*: `matching_rows_all` reads an absent term as a
definitive "no rows match" (`crates/siglake-index/src/lib.rs`), so a renamed
dictionary entry costs a query for that term **every** row it has in the file,
not one ordinal.

The reader's disposition, per path, on a 400-row / 4-row-group file whose index
names four matching rows
(`siglake-storage/tests/inverted_index_integrity.rs`):

| arm | query | rows decoded | matches lost |
|---|---|---:|---:|
| no index (control) | succeeds | 400 | 0 |
| footer KV, sound | succeeds | 4 | 0 |
| footer KV, the needle's term flipped | **succeeds, answers empty** | 0 | 4 of 4 |
| footer KV, the needle's postings flipped | **succeeds, answers short** | 4 | 3 of 4 |
| Puffin sidecar (Zstd), one flipped stored bit | **fails**: `Restored data doesn't match checksum` | — | — |
| Puffin sidecar with `CompressionCodec::None` | succeeds, answers short | 4 | 3 of 4 |

So the Puffin path's failure mode is a failed query, not a fallback and not a
short answer: `PuffinReader::blob` returns the decompression error and
`ArrowReader` propagates it (`third_party/iceberg/src/arrow/reader.rs`). No
writer selects `CompressionCodec::None` for an inverted-index blob, so that arm
prices the cover rather than describing a shipped state — and it is the reason
the cover is worth a guard if the codec is ever made configurable.

Warm, neither path re-verifies anything: the parsed-index cache is keyed by
`(data file, column)` or `(sidecar path, blob offset)` on the write-once
identity of the file, so a blob that rots after a query warmed it keeps
answering from the parse until the entry is evicted, and the cold read of the
same file then behaves as the table above
(`a_warm_parsed_index_does_not_see_a_corruption_that_lands_after_it`).

**What one CRC-32 over the whole blob would cost.** Four bytes raw, eight hex
characters in a footer value, plus whatever names it. Against a 29.68 MiB
payload (2M rows, release, this box,
`report_whole_blob_checksum_cost`): `crc32fast` runs at 10.3-11.1 GiB/s, so the
sum is **2.881 ms against a 703.4 ms `from_bytes` (+0.41%)** and 76.0 ms of
`to_bytes` (+3.79%); `to_hex` alone costs 180.4 ms. Extrapolated to the
compacted 85.8 MiB blob `DESIGN_segmented_inverted_index.md` measures, at
these rates: about 8 ms of checksum against about two seconds of decode. The decoder already
walks every byte the sum would cover, which is what makes the granularity
question moot here — 4 bytes per *term* is a third of a segmented blob only
because a segmented point lookup fetches a median of 3 bytes and can verify
nothing else.

**Placement, and what a 0.1.x reader does with each.** `from_bytes` consumes a
well-formed blob exactly and refuses trailing payload, so nothing can be
appended invisibly (`the_shipped_decoder_refuses_both_checksum_placements`):

1. **Trailing CRC, version byte still 1** — refused by the shipped decoder at
   the trailing-payload check.
2. **Trailing CRC under version byte 2** — refused at the version check. Both
   refusals are safe (no index, exact scan, right answer) and both cost a
   mixed-version fleet all index pruning on newly written files until every
   reader is upgraded. Old blobs keep reading either way: the version byte is
   what selects the layout.
3. **A sibling footer-KV key** (`siglake.inverted_index.v1.crc32[.column]`,
   eight hex characters) — leaves the blob bytes and the version byte alone. An
   old reader ignores an unknown footer key and keeps pruning; a new reader
   verifies when the key is present and falls back to an exact scan when the
   sum disagrees.

**Measured recommendation**: (3), and leave the Puffin path on its codec. It
covers the only path the sweep found exposed, at eight hex characters per file
per column, with no format version, no compatibility cliff and no change to any
blob already on disk — and it is checked in the one place that already reads
the whole blob. It leaves two things to carry with it: a guard that an
inverted-index blob is never registered with `CompressionCodec::None` (the
cover is the codec, and nothing asserts the choice today), and the fact that a
sibling key shares the footer it checks, so a corruption large enough to take
both is undetected. Options (1) and (2) buy one thing (3) does not: a blob that
carries its own integrity wherever it is stored, including a future path that
is neither of these two. Any of the three is a production format change and
belongs to its own 0.2.0 card; this one changed no format, API or default.

What these numbers do not cover: object storage, AWS, or any incidence rate.
The sweep is a uniform single-bit model of what a corrupt byte *does*, not how
often storage delivers one, and every timing here is one local box.

### Slice C — AWS validation (remaining)

`scripts/ws5-validate.sh`: with `compactor.invertedIndex.enabled=true`, ingest a
rare-token batch, confirm `siglake_index_build_bytes` advanced (built), the
`LIKE '%token%'` count is exact (correct), and
`siglake_iceberg_inverted_index_used_total` advanced (pruned at scan time).

## Default qualification (2026-09-11)

`inverted_index_default::report_footer_inverted_index_write_cost` writes the
same 20,000 corpus-shaped events five times per arm on local storage, with arm
order alternating, and reports medians from a release build. This run measured
77.02 ms with indexes off and 79.73 ms with them on (**+3.51%**). The serialized
index was 96,103 bytes; hex footer storage raised the Parquet file from 123,467
to 315,705 bytes (**+192,238 bytes**). The fixture is small and compressible, so
the byte ratio is not a production-storage projection; it bounds and attributes
the default's local write work while the AWS validation remains separate. The
post-rewrite qualification establishes that this cost is not repeated for
files the default writer already indexed: only streaming outputs and indexes
above the footer threshold enter the Puffin builder.

## Original plan (for reference)

### Slice A — build at write time

In `iceberg.rs::write_batch_to_data_files`, alongside `raw_trigram_bloom_hex`,
build the index from the batch's `raw` column and stash the `to_bytes()` blob in
the Parquet **footer KV** under a new `SIGLAKE_INVERTED_INDEX_KV_KEY` (mirror the
raw-bloom KV path through `siglake_writer_properties`). The ordinal space is the
**file's physical row order** — which, post time-ordering, is `timestamp ASC`, so
ordinals line up with the SortingColumn footer.

- **Gate it.** A full inverted index is heavier than a bloom (CPU + blob bytes).
  This original default-off gate was superseded after local qualification; the
  opt-out is `SIGLAKE_INVERTED_INDEX=0` or Helm
  `compactor.invertedIndex.enabled: false`. Emit `siglake_index_build_bytes` /
  `_build_seconds` so the cost remains observable.
- **Per-file vs per-row-group.** Start per-file (one blob/file). The query bridge
  maps file-local ordinals to row-group-relative `RowSelection` using the
  Parquet metadata's per-row-group row counts.

### Slice B — consume at query time (the WS-3 RowSelection bridge)

When a query has an indexable predicate (`raw LIKE '%term%'` with a term of
length ≥ the tokenizer minimum, or a token-equality), and the file carries the
index blob:

1. Load the blob from footer KV, `from_bytes`.
2. `matching_row_selection(terms)`; if all-skip ⇒ skip the file (stronger than
   the bloom's maybe-contains).
3. Otherwise translate the `(selected,len)` runs to `parquet::arrow::arrow_reader::RowSelection`
   (`RowSelector::select/skip`) and hand it to the `ParquetRecordBatchReaderBuilder`
   `.with_row_selection(..)` so non-matching rows are never decoded.

This composes with the existing trigram blooms (bloom prunes the file/row-group;
the index prunes rows within a surviving block) and with the WS-3
`output_ordering` (selection preserves row order, so the ASC ordering still
holds).

### Slice C — validation

AWS round: write a warehouse with the index enabled, run `raw LIKE '%rare-term%'`
and confirm (a) correctness vs the un-indexed result, (b) `rows_decoded`
drops via the selection, (c) blob-size + build-time overhead are acceptable.

## Why bespoke postings (not Tantivy yet)

The first slice needs exact term→row matching + a RowSelection, which a sorted
postings list does in ~250 dependency-free lines with full test control. Tantivy
(BM25, phrase, fuzzy) is a later evolution if richer relevance/phrase search
becomes a product requirement; the `InvertedIndex` API (postings + selection)
can be re-backed by it without changing the query bridge.
