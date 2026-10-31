# Hybrid WAL WARN freshness investigation

Task #7923 investigated an October 30 SigNoz query that returned 100 rows with
timestamps exactly 20 seconds older than its oracle. This record separates the
behavior reproduced in source from the historical facts that were not captured.

## Retained evidence and source path

The failed engine was
`1c69e5c1edc0bfd140f8eccc33c1269dd52e5f13`; the source under investigation is
`cf3527d6adcc25f54bf0097601ef4867d492d9d8`. Their
`crates/siglake-query-server/src/wal_buffer.rs` files have the same SHA-256,
`dbc1b3a8a996778d62cf09d1dd281167e589ee6f1099e02ba6295dbdc21e64e8`.
The retained SigNoz shim identifies source
`1205d6cdc4cb0e4a470d0ba210b37ec24cddf6b7`.
The independent raw discrepancy and peer review files rechecked as
`c2b45bf776ddb57436c447ae9a3d4b8a89e77c941e8464fd615d635f3a3645e8`
and `6c3e81cfddb82f6fd34e5230fccc5ad1c4b5af0adde9ccc9ac96781ad46082fa`.

The retained API body has SHA-256
`48ae9dc3d7004a564fb09a80f5536ba92ba40e4df95786e9e7aa8499d51f7de7`.
It requests `log.severity_text = 'WARN'`, projects six log attributes and
`service.name`, then orders by `customer.segment ASC` and timestamp descending.
The pinned shim maps the severity predicate to
`attr_get(attributes, 'sl_signoz_v1.severity_text') = 'WARN'`. It maps each
selected log attribute through the JSON stored at
`sl_signoz_v1.attributes_string`, and `service.name` through
`sl_signoz_v1.resources_string`.

The wire capture proves the API request and response, not the SQL text emitted
for that call. It also has no snapshot identifier, consumed-segment set, WAL
segment census, decoded generation identity, memory reservation reading, or
fallback reason. The observed 20-second offset therefore does not prove an
80,000-row commit plus 20,000-row WAL split.

## Source contract

`UnionEventsProvider::scan` obtains the base snapshot plan first, then tries to
load the selected WAL generation. A load refusal returns the base plan. After a
successful load, the filter pre-pass can return three distinct outcomes:

- bounded evaluation succeeds, and matching WAL rows enter the union;
- evaluation fails, and the scan unions the unfiltered WAL rows so the answer
  stays exact;
- workspace is unsupported or its reservation is refused, and the scan returns
  the committed base plan.

A charged WAL sort refusal also returns the base plan. These fallbacks are
intentional and bounded. The committed rows remain exact while newer rows wait
for a later admission or an Iceberg commit. The count and filtered query in the
October run were separate requests. They did not pin one shared Iceberg
snapshot or WAL generation.

## Deterministic fixture

The regression uses one fixed dataset: 16 committed rows and 16 newer sealed
WAL rows. WARN and INFO rows occur in both halves. Five cases reuse that data:

1. adequate memory with the retained WARN shape;
2. the retained projection and ordering without a filter;
3. the WARN shape with filter headroom fixed one byte below its bound;
4. `upper(severity) = 'WARN'`, which has unsupported filter workspace;
5. the committed provider without the WAL union.

Every case has a 30-second timeout and asserts the complete ordered JSON rows.
The first two plans contain the WAL union. The memory-refusal and unsupported
controls contain no WAL union and equal the committed-only control. The forced
memory case proves the generation was already loaded and hit in the cache before
the filter reservation was refused. It releases the pressure reservation and
the cached generation before asserting zero charged bytes.

The passing fixture used 32 rows, 22,320 logical payload bytes, one 1,310-byte
WAL segment, and 1,310 fixture filesystem bytes. Its loaded generation charged
19,118 bytes. The severity-only filter bound was 37,740 bytes; the refusal case
left 37,739 bytes free under the 16 MiB test budget and reserved the remaining
16,720,359 bytes without allocating that payload.

## Disposition

The fixture reproduces the documented contract behavior. It finds no new result
correctness defect: an admitted generation returns every matching committed and
WAL row, while an unsupported or refused filter workspace intentionally returns
the exact committed view. It also confirms an observability gap. The existing
filter-workspace counter combines unsupported expression with checked
arithmetic, while the general memory-refusal counter does not identify the scan
stage. A successful SQL response does not say that its WAL generation was
omitted.

Historical cause remains unresolved. No performance claim, live-tail claim, or
candidate comparison follows from this fixture.

A production change should first specify a bounded reason signal. The smallest
proposal is a stage-and-reason counter for `unsupported_expression`,
`bound_overflow`, and `memory_refused`, plus request diagnostics that identify
a committed-only WAL fallback without exposing segment names. Tests should
cover load, filter, and charged-sort refusal independently and assert that every
path releases its reservation. This investigation does not change the fallback
or restore an unbounded union.

## Investigation plan completion

- Phase 1: source and retained-artifact identity checks — complete.
- Phase 2: request-to-predicate mapping and contract trace — complete, with the
  historical emitted SQL explicitly unproved.
- Phase 3: bounded five-case scan regression — complete.
- Phase 4: disposition and remaining uncertainty — complete.
