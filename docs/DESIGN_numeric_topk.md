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
256 KiB per serialized footer, 32 MiB retained decoded Parquet metadata, and
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
outcomes. `files_read` includes footer-opened files, even where data groups are
pruned. Group pruning includes all-null groups. A metadata refusal returns no
partial output and runs the original plan once; its probe bytes/reads are added
to the fallback Records statistics and its cost warnings, including row-breaker
errors. Metadata refusal reasons and bytes have bounded-label counters
`siglake_numeric_topk_fallback_total{reason}` and
`siglake_numeric_topk_fallback_bytes_total`.

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
