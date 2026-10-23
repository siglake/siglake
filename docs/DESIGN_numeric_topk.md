# Snapshot-bound numeric TopK (#6978)

This candidate targets the retained Big5 value-only numeric sorts from investigation
#6748. The October 17 preparation found a full typed-column scan, rather than raw
JSON extraction: 69,223,950 rows, 319 row groups and 32 files. Its historical DESC
p50 was 1106.55 ms versus Quickwit's 47.45 ms; ASC was 876.41 ms versus 38.78 ms.
Those are cross-system historical observations, not current performance results
or controlled estimates of this implementation's speedup.

## Eligibility and result semantics

Only the local Records route without configured peers can select this path.
It requires one Int64 value, the identical explicit `IS NOT NULL` predicate,
matching ASC/DESC order and literal LIMIT 1–1000 (no nonzero offset). Extra outputs,
filters, joins, aggregates, DISTINCT, float/string conversions and unsupported
logical shapes retain the existing planner. Output aliases and quoted physical
identifiers are resolved by DataFusion. The existing promotion rewrite's proven
Int64 → string → Int64 round trip is recognized; standalone string ordering is
not treated as numeric ordering. #6986 separately quotes injected promoted physical
identifiers so spaces, case, SQL keywords and embedded quotes retain their exact
column identity on the ordinary rewrite as well as this route.

The route consumes the immutable `QualifiedAttrRewrite` captured before provider
registration: table UUID/snapshot/schema, carrier/key/extraction path/type,
extractor version and complete live-file coverage. A physical column name alone
is insufficient. It never refreshes that binding by table name. Full Parquet scan
tasks must have no deletes or residual task predicates, exact file sizes and
record totals matching the captured manifest. NDJSON, batch, shards, coordinator
fallbacks, WAL-enabled serving routes and missing provenance use their ordinary
paths. #6982 separately prevents committed promotion rewrites from turning raw
WAL projection values into NULL; it does not broaden existing bounded WAL filter
or carrier admission. Some complex buffered queries therefore still intentionally
serve the committed snapshot, exactly as the existing residual route does.

Footer decoding is narrowed to what the route reads: page-encoding and size
statistics are skipped for every column, and once a file in an attempt proves
the target leaf's ordinal, later files decode column statistics for that ordinal
alone. This weakens no check — a skipped statistic is absent, and absent
statistics already refuse. A file that places the target column at a different
ordinal is refused (`column_position`) rather than read against a neighbour's
bounds; heterogeneous column orders within one snapshot therefore take the
ordinary plan.

Decoded footers are retained across requests in a bounded LRU keyed by
`(object path, manifest length, statistics scope)`. An entry is admitted only
after the physical object length matched the manifest length that keys it, and
siglake data objects are immutable and UUID-named, so an entry is a pure
function of its key. Nothing is keyed by table or column name and nothing
expires on time: a new snapshot's new files miss, retained files hit. The
statistics scope is part of the key because a narrowed entry carries one leaf's
bounds and must never answer for another column. Every per-request check — row
counts, field id, column path, Int64 type, exact min/max/null counts, chunk and
group budgets, and the serialized-footer budget — runs against the reused
metadata, so reuse saves the parse and never a check. A reused footer issues no
read and reports no footer bytes. The cache takes 0.03 of the derived
metadata-cache budget, which `footer_group_counts` gives up (0.30 → 0.27); the
shares still sum to 1.0. Under byte or entry pressure it evicts, counted on the
existing `siglake_cache_evictions_total{cache="numeric_topk_metadata"}` and
`siglake_cache_admission_refused_total{cache="numeric_topk_metadata"}` series —
the signal an operator reads when a small pod sees no repeat improvement. No new
metric name is introduced.

All row-group metadata is validated before any value decoding. The candidate
requires the exact physical field ID/type, exact Int64 min/max and null counts.
It schedules groups by their best possible numeric bound and keeps at most K
actual decoded values. It stops only when no remaining bound can improve the
current kth value. Equality is safe because the query returns only the value;
there is no hidden row identity or second projection to break ties. Signed MIN is
never negated. All-null groups and fewer-than-K inputs retain SQL cardinality.
Timestamp-sorted files are never advertised as physically sorted by this number.

## Bounds and accounting

Execution follows normal admission and the original request deadline. A 128 MiB
reservation comes from the query's existing DataFusion pool and drops with the
future. Limits are 256 files, 4096 groups, 8 MiB serialized footers in total,
512 KiB per serialized footer, 32 MiB retained decoded Parquet metadata, and
32 MiB per compressed/uncompressed target chunk. Decoding uses 8192-row batches
and yields between files/batches. These are conservative admitted workspace
limits, not a hard process RSS guarantee for parser/allocator overhead.

Before Parquet-file IO, twice the manifest file sizes plus eight trailer bytes
per file must fit the request byte allowance. Existing snapshot manifest planning
precedes this range budget. Object lengths are checked against that manifest.
A shared range-reader budget then charges every requested range before metadata
prefetch or data reads, including overlaps and coalesced ranges. Budget exhaustion
is an error, never partial success or an uncharged retry. Backend-internal retries
are not individually observable at this interface. Existing generic row-limited
query byte-limit behavior remains unchanged; it is not a new generic mid-flight
byte breaker.

`served_by: numeric_topk` identifies the route. Rows and decoded bytes come from
actual batches; footer/data/fetched bytes and read counts come from actual reader
outcomes. `files_read` counts every file the attempt validated — including files
whose data groups are pruned, and files whose footer was reused rather than
opened. Physical work is `object_store_reads`, `bytes_footer` and `bytes_data`,
all of which a reused footer leaves at zero. Group pruning includes all-null
groups. A metadata refusal returns no
partial output and runs the original plan once; its probe bytes/reads are added
to the fallback Records statistics and its cost warnings, including row-breaker
errors. Debug diagnostics record the bounded refusal reason and the probe bytes
and reads; no public metric series is emitted without a named reader.

`SIGLAKE_NUMERIC_TOPK=0` disables the candidate at process startup. A fresh process
also avoids carrying candidate result-cache bodies into a disabled control.
No other scan, source ordering or promotion setting is changed by this switch.

## Validation and remaining qualification

Focused native tests use real persisted Parquet and the actual HTTP router:
NULL/all-null/fewer-K, ties, signed extremes, overlapping bounds, missing late
statistics, field identity, memory refusal, decoded-metadata limits, cumulative
range accounting, cancellation, captured snapshot after a competing commit,
late unprovenanced files, matching/wrong carrier, quoted names, typed cast/order
refusals, request limits, NDJSON parity, shard/coordinator refusal and disabled
process fallback. The original registered CAST SQL must actually report
`numeric_topk`; answer equality alone does not qualify the optimization.
Existing attr-rewrite and WAL-union regressions remain relevant to the shared
proof and serving guard. Direct delete association is a source guard; no new
catalog-backed positional-delete fixture is claimed by this focused suite.

Before performance acceptance, use one qualified immutable binary/image and an
identical immutable table snapshot with TopK on/off in fresh processes. Disable
result caches (`SIGLAKE_QUERY_RESULT_CACHE=off`), preserve data-cache/cold/warm
boundaries and the registered resources/SQL. First compare all 30 original Big5
answers on the canonical first-1000 fixture, then require original ASC/DESC TopK
route/counter proof on a qualified full snapshot. Retain raw HTTP results,
physical rows/groups/bytes, source/image/provenance and cleanup evidence. Existing
historical footer audits are not new measurements or current snapshot proof.
No cloud timing or speedup is established by these source tests.

### Canonical footer admission follow-up (#7079)

The original 32-file snapshot has 15 serialized footers above the initial 256 KiB
per-file limit (maximum 498,994 bytes). The exact local Parquet 58.4.0 probe
measured 8,296,285 serialized bytes total and 10,985,491 bytes of retained decoded
metadata. The single-footer cap is therefore 512 KiB; the 8 MiB aggregate,
32 MiB decoded-metadata cap and 128 MiB reservation are unchanged. The aggregate
margin on that snapshot is only 92,323 bytes; other layouts may still refuse.
Persisted regression fixtures cover the observed maximum, the exact per-file
boundary and its one-byte overflow, plus cumulative footer overflow before data
reads. This is admission compatibility, not a performance result or a production
RSS guarantee. Exact original ASC/DESC SQL must still prove the numeric route
on the restored catalog/snapshot/current provenance before field timing.

### Repeated metadata work on warm requests (#7099)

The matched #7076 field result is the starting point, not the original
full-scan diagnosis: on the canonical snapshot the route already pruned to
2 (ASC) and 6 (DESC) decoded row groups of 319, and warm medians were still
453.06 ms and 521.74 ms. All 32 files stayed footer-opened and cold footer
bytes were 12,800,919, with warm footer counters absent in the body.

A local `#[ignore]`d measurement
(`numeric_topk_repeat_request_metadata_cost_measurement`) builds a bounded
32-file fixture matched to that metadata shape — 8,285,216 serialized footer
bytes and 12,379,712 decoded, against the canonical 8,296,285 / 10,985,491 —
over a trivial amount of data, so per-request metadata work is separated from
decode work. Steady state over six attempts on a local filesystem, release and
debug profiles:

| | release ms | debug ms | object-store reads | footer bytes |
|---|---:|---:|---:|---:|
| repeat without reuse | 5.9 | 59.2 | 98 | 16,777,472 |
| repeat with reuse | 0.15 | 1.70 | 2 | 0 |

Both rows come from the committed code: the test runs the same request against a
cache the previous request populated and against a fresh one. The narrowed
decode was measured separately before the reuse landed, on the same fixture in
the same session: it took the debug repeat from 68.4 ms to 56.3 ms, about 18%.
Retained decoded metadata for the whole 32-file fixture is 8,094,944 bytes, which
is less than the 10,985,491 the canonical snapshot decoded with full statistics.

A phase breakdown puts almost all of the local repeat in the footer decode: of
the 5.9 ms release repeat, 0.05 ms opens the 32 objects and reads their lengths,
5.4 ms fetches and decodes their footers, and the rest measures retained metadata
and derives the Arrow schema.

What that does NOT establish is how much of the canonical snapshot's 453 ms warm
median this removes. Six milliseconds of release-profile decode is not 453 ms,
and the local filesystem makes the other repeated work invisible: the 32
sequential object-length requests the metadata loop issued per attempt are
microseconds here and uncached S3 HEAD round trips there, which is a term this
fixture cannot weigh. Reuse removes both, but the field split between them, and
whatever else the warm median contains, is unmeasured. These are local source
measurements on one filesystem — not field timing, not an AWS acceptance, and not
a claim about the canonical warm medians.

The next matched qualification is the existing #7076 protocol, unchanged, with
one addition. Same immutable image and snapshot, result caches off, original
f48e35f6 harness/SQL and seed, 58 GiB query container, three cold samples with
five warm responses each per canonical ASC/DESC shape. Record, per warm response,
`stats.scan.bytes_footer`, `object_store_reads` and `files_read` alongside the
median: the claim this change makes is that a warm response reads no footers,
and a warm `bytes_footer` that is present and nonzero refutes it regardless of
timing. Report the query pod's `siglake_cache_evictions_total` and
`siglake_cache_admission_refused_total` for
`cache="numeric_topk_metadata"`; both must be zero for a 32-file snapshot on a
58 GiB container, and a nonzero count means the retained footers did not fit and
the timing is measuring eviction. Exact answers must still match the independently
accepted original full-source oracle.

That round settles the route's own counters. It does not say where the rest of a
warm request goes, and the local numbers say the remainder is most of it. The
same round can answer that for free by retaining each warm response's
`stats.phases` (`plan_micros`, `collect_micros`, `render_micros`) next to the
median, which is what decides whether the next card belongs in this route at all.

### Canonical request attribution (#7131)

The #7076 round did retain phases. The evidence is the six retained
`mode1-canonical` `queries.json` files. Their SHA-256 hashes, in sample order
0000 through 0005, are
`2b56ff8426f4fbe08d3f58bac405622e3d62b598475403da45612c47d14f7f1a`,
`6fa9e1bb74a40837d818903cb725fe9a73f7aea3b643411bd55678cb3d676c5d`,
`e886b360d2efca89172a219d9e4d5ca29f47a9d8c6cdd6980f30da8431724ab2`,
`99b326070d27d788411f9e6f407961ff14f11be13b6f0ef8172306958861243f`,
`ccb7263f647029e1330d781792fc5db360b60c5f5052d4a5fa39e9b6e08c299f`
and `387ae4914e7d9a59c3e126d5e9df8d68cdbd90fc988fa6f9908804d9d9b4b16c`.
`summary.json`
(`d7c3beb3979e69d1963d9f1b4b3a2a8b5d85ed4823e0f732c14319c057e2a685`)
says 36 responses passed; `protocol.json`
(`e7085684254b72400895f94f976471069a4f3ddc44a546f0276e486a65ddc46b`)
records three cold samples with five warm runs apiece and an uncontrolled
remote-storage cache. The image was built from `21a53d5`, before #7099.

The following rows use the individual response at the warm wall-time median,
not independently computed component medians. `Server remainder` is
`server_micros - plan_micros - collect_micros`; it includes request work outside
the two reported phases and JSON response construction. `Client gap` is wall
time minus the server header. Each row therefore sums exactly to its wall time.

| canonical warm response | plan | collect | server remainder | client gap | wall |
|---|---:|---:|---:|---:|---:|
| ASC, sample 0004 warm 2 | 3.583 ms | 432.398 ms | 16.438 ms | 0.641 ms | 453.060 ms |
| DESC, sample 0002 warm 2 | 3.552 ms | 501.465 ms | 16.056 ms | 0.667 ms | 521.740 ms |

`render_micros` is zero in these bodies, but zero is assigned by
`attach_fast_path_phases`; it is not a rendering measurement. `collect` starts
before the fast-path battery and ends once the numeric response body has been
built. It therefore combines metadata validation, footer decode, row-group
selection, page decode and candidate selection. The median ASC response decoded
one row group (234,441 rows and 1,903,328 decoded bytes); DESC decoded eight
(1,721,327 rows and 13,979,616 decoded bytes). All 30 warm responses report zero
fetched bytes and zero range reads. The old path's per-file `metadata()` calls
are outside those scan counters, so the retained responses cannot separate 32
object-length round trips from CPU work inside `collect`.

The retained cold medians use the same samples and decoded group counts as the
warm medians:

| canonical cold response | plan | collect | server remainder | client gap | wall | range reads / fetched bytes |
|---|---:|---:|---:|---:|---:|---:|
| ASC, sample 0004 | 11.156 ms | 2,176.226 ms | 41.569 ms | 0.679 ms | 2,229.630 ms | 51 / 21,971,178 |
| DESC, sample 0002 | 11.355 ms | 2,936.772 ms | 41.427 ms | 0.666 ms | 2,990.220 ms | 58 / 24,566,036 |

Both cold responses fetched 12,800,919 footer bytes. Against their warm response
in the same reset sample, cold `collect` adds 1,743.828 ms ASC and 2,435.307 ms
DESC while decoded groups and rows are unchanged. That delta contains footer and
data object reads, page-cache state and footer decode; the retained counters do
not assign it among them. It supports #7132's bounded cold-metadata concurrency
experiment, but its result must time the metadata batch separately from data
reads before claiming a cause.

The ignored
`numeric_topk_whole_request_cost_measurement` test supplies the local half. It
sends the registered SQL through the in-process `/api/v1/sql` router against 32
one-row files whose serialized footers total 8,285,216 bytes. This deliberately
does not model the canonical data volume. In a release build, the individual
median warm responses were:

| local warm response | plan | collect | other router/body work | wall |
|---|---:|---:|---:|---:|
| ASC | 0.202 ms | 0.282 ms | 13.550 ms | 14.034 ms |
| DESC | 0.197 ms | 0.275 ms | 13.529 ms | 14.001 ms |

Each response reused all footers, read ten trivial data groups and decoded 1,040
bytes. The first cold local request took 52.812 ms: 14.185 ms plan, 23.431 ms
collect and 15.196 ms other work. The storage-only matched-metadata measurement
above puts a fresh repeat at 5.9 ms, including 5.4 ms of footer fetch and decode.
These local numbers bound fixed route and local metadata cost; they are not
components to subtract from an AWS response.

The attribution selects no new warm optimization. Planning and response work
are small, and the 432--501 ms field `collect` term remains a combination of
unmeasured object-length latency and canonical data decode/selection. #7099
removes the former plus repeated footer decode on a cache hit, but this pre-#7099
round cannot measure its field effect. For cold requests, #7132 is the next
implementation candidate because the same-sample cold delta is large and the
metadata loop is serial; its acceptance must retain the finer timing that this
round lacks.

### Concurrent cold metadata requests (#7132)

A cold attempt used to validate files in one serial loop: per file an object
length request, an 8-byte trailer read and a footer fetch, each awaited before
the next file started. On the canonical 32-file snapshot that is 32 sequential
request chains before any data is decoded. #7099 removed that cost on a warm
repeat; a first request, a new snapshot or an attempt after eviction still paid
all of it.

The route now runs the first file exactly as before and overlaps the rest in
two bounded passes, `METADATA_CONCURRENCY` = 8 requests in flight:

1. open, match the physical object length against the manifest length and read
   the trailer, concurrently;
2. walk the known serialized lengths in FILE order and clear a prefix for
   fetching, charging the 512 KiB per-file cap, the 8 MiB running aggregate and
   the object's own length;
3. fetch the cleared prefix's footers concurrently.

The ordered validation walk is unchanged and still decides everything. Nothing
reorders because:

- The first file is serial, so the leaf ordinal it proves is known before any
  later file is probed. Every file after it decodes with the same
  `metadata_options(Some(leaf))` and looks up the same `StatsScope::Leaf(leaf)`
  cache entry it did when the loop was serial, and a file that places the target
  column at another ordinal still refuses `column_position`.
- The aggregate serialized budget is charged in file order against lengths read
  before any footer fetch, so the footer that would overflow 8 MiB is still
  never fetched, and neither is any footer after it. A reused footer charges the
  same length it charged when it was admitted.
- Refusals and errors from the prefetch are carried per file, not raised. The
  walk stops at the lowest-index file that refuses and returns that file's
  reason, so a refusal at file 3 is never replaced by file 7's.
- Everything is validated before any data read, and the memory reservation and
  cancellation behaviour are untouched: the prefetch futures live inside the
  request future and drop with it.

What a refusing attempt now does that a serial one did not: files after the
refusal may have had their object length and 8 bytes read. Decoded metadata is
also held for the whole cleared prefix before the 32 MiB decoded-metadata bound
is charged against it, where the serial loop charged it file by file. The prefix
is capped at 8 MiB of serialized footers, which decoded to 10,985,491 bytes on
the canonical snapshot and 12,379,712 on the local fixture, both inside the
128 MiB reservation.

`numeric_topk_concurrency_does_not_move_ordered_decisions` runs the same
fixtures at concurrency 1, 2, 8 and 64 — an answered attempt ASC and DESC, and a
mid-list refusal for absent statistics, for the target leaf at another ordinal
and for a single footer over the per-file cap — and requires identical values,
identical `files_read` and `groups_considered`, identical bytes asked of storage
and identical read counts. The over-cap case additionally requires the attempt
to read fewer bytes than that one footer. The pinned
`..._still_refuse_aggregate_overflow_before_data` and
`..._refuses_decoded_metadata_and_actual_size_before_data` tests are unchanged
and still pass.

The ignored `numeric_topk_cold_metadata_concurrency_measurement` is the local
half, on the same 32-file fixture: 8,285,216 serialized footer bytes,
12,379,712 decoded. At every concurrency from 1 to 32 the attempt reported 98
object-store reads, 16,777,472 footer bytes and 16,935,032 fetched bytes —
byte-identical to the serial code at `5082877` run against the same fixture.
Debug-profile wall time was ~62 ms at every concurrency, including 1. A local
filesystem has no round-trip latency to overlap — #7131 measured an object open
at 0.05 ms for all 32 files — so flat wall time is what this fixture can show.
These numbers establish that overlapping the requests changes no decision and no
byte; they establish nothing about AWS.

The saving this change is for is unmeasured. It needs a matched round against
the same image and snapshot as #7076 — result caches off, original harness, SQL
and seed, 58 GiB query container — recording cold ASC/DESC medians against the
2,229.63 ms and 2,990.22 ms this card started from, and separating the metadata
batch from the data reads in each cold response rather than reporting the
`collect` total. Until that exists, the cold medians are not improved, only the
request pattern is changed.
