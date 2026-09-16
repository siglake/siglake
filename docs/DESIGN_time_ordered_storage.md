# Design: time-ordered Parquet storage

Status: **shipped 2026-06-05.** siglake is fundamentally a time-series store, so
data is physically ordered by its event-time column when written to Parquet —
as a single, centralized, declared invariant of the storage layer.

## Timestamp contract (2026-09-06)

Event time is **two** columns, and every ordering rule below is stated in terms
of the pair:

| column | Iceberg type | Parquet physical type | meaning |
|---|---|---|---|
| `timestamp` | `timestamptz` (microsecond), required | INT64 `TIMESTAMP(MICROS, isAdjustedToUTC=true)` | the event instant, truncated to the microsecond (floored, so the map stays monotone across the epoch) |
| `timestamp_ns` | `long`, required | INT64 | the OTLP `time_unix_nano` value **verbatim** |

`timestamp` remains the partition column (`day(timestamp)`) and the lead sort
key; `timestamp_ns` is the second sort key.

**Why microsecond and not nanosecond.** Iceberg's nanosecond timestamp types
(`timestamp_ns`/`timestamptz_ns`) are **format-version-3 only**, so a table that
uses one is closed to every v2-only reader. Spark cannot map them at all
(Iceberg 1.11 has no nanosecond case for Spark 3.5/4.0/4.1 — Spark's
`TimestampType` is microseconds), DuckDB's iceberg extension rejects
`timestamptz_ns`, and PyIceberg refuses the type. Microsecond `timestamptz` and
`day()` are both v2 constructs, so `minimum_format_version` now stamps every
schema siglake ships at **format version 2** and the whole reader matrix opens.
Nothing is lost: `timestamp_ns` carries the exact nanosecond as a plain `long`,
which every engine reads, and siglake's own scans filter and order on it.
External SQL uses `to_timestamp_nanos(timestamp_ns)` when it needs exactness.

**Migration: recreate, do not upgrade.** Warehouses written before 2026-09-06
are format version 3 with a nanosecond `timestamp` and no `timestamp_ns`
sibling. Iceberg can neither change a column's precision by schema evolution nor
downgrade a v3 table in place, so there is no upgrade path — delete and
re-ingest. 0.1.0 is untagged, so no released data predates the contract.

`scripts/check-external-timestamp-contract.sh` is the regression check: it
writes the `siglake iceberg-demo` fixture, asserts the contract in-process
(format version, Iceberg field types, the nanosecond round-trip, no nulls, the
total order), and then has each installed external engine agree on the row
count, the `timestamp_ns` bounds **and the decoded `timestamp`**: its type
(DuckDB `TIMESTAMP WITH TIME ZONE`, Spark `timestamp`, PyIceberg
`timestamp[us, tz=UTC]`), its microsecond bounds, that neither column has a
null, and that every row's decoded microsecond is `floor(timestamp_ns / 1000)`
(`epoch_us`, `unix_micros`, an int64 cast of the Arrow column). Each engine
answers all of that from one query, so an engine that cannot decode `timestamp`
— the original nanosecond-type failure — fails rather than passing on its
nanosecond sibling. The fixture spaces rows 1 ns apart, so a thousand rows share
each microsecond and truncating to any coarser unit is visible.
`scripts/check-external-readers-report.sh` pins that hermetically with stub
engines: every arm there answers the nanosecond triple correctly and fails only
on the timestamp.

## Order: event time ascending, plus the exact-nanosecond tiebreak

Every table sorts by its event-time column **ascending**, with no *dimensional*
secondary key. Tables that carry the exact-nanosecond sibling `timestamp_ns`
(the 2026-09-06 timestamp contract) add it as a second ascending key:

| table | sort columns |
|---|---|
| `events` | `timestamp`, `timestamp_ns` |
| `candidates` | `bucket_start` |
| `detector_runs` | `started_at` |
| `episodes` | `started_at` |
| `episode_events` | `added_at` |
| `query_audit` | `timestamp` |

**Why ASC:** matches the WAL append order (so the write-time sort is near-identity
on the common in-order case), matches external-tool / TSDB convention
(parquet-tools, DuckDB, Trino read top-to-bottom as time-increasing), and is the
conventional Iceberg sort direction. Min/max pruning is direction-agnostic, so
there's no performance reason to prefer DESC.

**Why the `timestamp_ns` tiebreak:** under the timestamp contract above,
`timestamp` is a **microsecond** `timestamptz`, so it *does* tie — a thousand nanoseconds
map to one value. `timestamp_ns` is the OTLP `time_unix_nano` verbatim, so
`(timestamp ASC, timestamp_ns ASC)` is a **total** order that refines
`timestamp` and agrees with `timestamp_ns` alone. That is what the merge and
early-stop reasoning below assume, and it is why the k-way merge compares
`timestamp_ns` rather than the microsecond lead key: breaking microsecond ties
by source index would emit files that do not satisfy the order they are stamped
with.

**Why no dimensional secondary key:** a dimensional key (the old `host`) yields
no compression or pruning benefit behind a near-unique time lead — a column only
clusters when it *leads* the sort. Dimensional pruning is handled by the
per-row-group **bloom filters** (`host`/`source`/`sourcetype`/`index`), which are
the right tool for a non-leading column and work regardless of sort. This also
keeps the sort minimal and schema-agnostic ahead of an OTel transition (where the
high-value lookup keys — `trace_id`, `service.name` — are random or belong in an
index/partition, not the sort).

## Mechanism: one invariant, every write path

The order is driven by each table's declared Iceberg `SortOrder` — the single
source of truth — and enforced centrally in `IcebergContext::write_batch_to_data_files`
(`crates/siglake-storage/src/iceberg.rs`):

1. `table_sort_columns(table, schema)` maps the table's `default_sort_order()`
   (identity-transform fields only) to `(arrow_col_idx, descending, nulls_first)`.
2. `sort_batch_to_table_order` lexsorts the batch into that order **before** the
   partition split, so each per-day partition file is internally ordered.
3. `table_sorting_columns` writes the same order into the Parquet row-group footer
   as `SortingColumn` metadata, so **declared order == on-disk order** by
   construction, and readers (Iceberg, DataFusion, DuckDB) can trust it.

Because this lives in the shared write path, **every** writer inherits it:
- the compactor's WAL→Parquet append (its old ad-hoc `sort_by_time_host` —
  `timestamp DESC, host ASC` — is removed; storage owns the guarantee),
- direct `append_events` / `append_*` (previously arrival-order),
- all detection-table writes (candidates/episodes/… by the detection tiers),
- tier-2 re-clustering (`recluster_files` now just reads+concats via
  `read_files_concatenated`; the write path re-orders — fixing the old
  inconsistency where re-clustering wrote ASC while the compactor wrote DESC).

## Converging a legacy table

Commit `4842f3d` shipped the transition from a legacy `DESC` sort order.
`IcebergContext::converge_sort_order_to_asc` records the outgoing order id in
the `siglake.legacy_sort_order_id` table property and replaces the default with
the same lead column in `ASC` order as a metadata-only transaction. New writes
use the new order; existing files remain physically `DESC`.

While both directions are live, the query gate attributes each file using its
manifest-stamped `sort_order_id`; pre-stamp files fall back to the recorded
legacy order. A direction-mixed scan refuses ordered advertisement and falls
back to TopK. Re-clustering rewrites the legacy files through the shared writer,
and the gate advertises `ASC` after the last `DESC` file is gone. The mechanism
has no CLI or operator entry point yet, so invoking it and running it on legacy
tables remain open.

## Scope / non-goals

- **The WAL stays arrival-order.** Arrow-IPC WAL segments are not sorted; ordering
  happens only at the WAL→Parquet boundary (and all other Parquet writes).
- **Within-file** ordering is now a contract; **cross-file** global ordering
  across a partition (overlapping time ranges in separate files from out-of-order
  ingest) is still healed by BIG-1 re-clustering — this change makes re-clustering
  *consistent* with the initial write, not redundant.
- **Time-ordered query *results*** still require the coordinator/scan merge (the
  deferred `ORDER BY` follow-on / Splunk reverse-time default). This is the
  storage foundation that makes an ordered scan cheap (sorted files + tight
  row-group bounds + declared `SortingColumn`), but parallel scans still need a
  merge to present globally-ordered output.

## Gates

- `siglake-compactor/tests/wal_to_iceberg::rows_in_output_parquet_are_time_ordered_ascending`
- `siglake-storage/tests/iceberg_round_trip::direct_append_is_time_ordered_and_declares_sorting_columns`
  (out-of-order direct append → ascending on disk + `SortingColumn` footer present)
- the existing re-clustering tests (now ASC) — 20 in `iceberg_round_trip`.
