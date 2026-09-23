# Per-index event-time ordering qualification (task #4073)

**Status:** design and local qualification only, 2026-09-23. **Verdict:
REVISE.** Mapping-aware newest-first ordering is sound, but the proposed
rewrite-only extension is incomplete. Implementation is scoped in #6020 for
0.3.0. This record changes no query rewrite, storage order, schema or API, and
the limitation in `LIMITATIONS.md` stays in force until that card passes.

## Question

Can a managed index whose mapping declares `timestamp_field: "ts"` receive the
same implicit newest-first browse and ordered early-stop as an index whose event
time is named `timestamp`?

Yes, if `ts` is carried as part of the order contract through both planning and
storage. Replacing the literal in the injected `ORDER BY` is not enough. The
query server currently derives a direction-only `PreferredScanOrder` from the
literal name `timestamp` (`crates/siglake-query-server/src/sql.rs`,
`preferred_scan_order_from_query` and `sql_order_expr_is_timestamp`). The scan
then uses that literal for the projected column, filter classification,
per-file bounds, reverse reading, overlap merge and frontier pruning
(`crates/siglake-storage/src/query_provider.rs`, `scan_output_ordering`,
`frontier_pruned_merge_inputs` and `OrderedMergeStream`). Advertising `ts`
before those consumers agree would let a plan omit its blocking sort while the
reader merges a different column.

## Field identity and type

The mapping is sufficient authority for field identity and type:

- `IndexConfig::validate` rejects a `timestamp_field` that is undeclared, is
  not `FieldType::Datetime`, or is optional
  (`crates/siglake-core/src/index_config.rs:393-404`).
- `IndexConfig::to_arrow_schema` compiles that field to a required Arrow
  timestamp at microsecond precision. No query-time inference is needed.
- `create_index` declares an identity sort led by that exact field and a day
  partition on it (`crates/siglake-storage/src/index_manager.rs:330-346`).
- Updating an index cannot change `timestamp_field`
  (`crates/siglake-storage/src/index_manager.rs`,
  `validate_index_update`). The field identity is stable for the table
  incarnation.

The planner should therefore read the cached `IndexConfig` and carry the exact
field name, not a boolean. It must construct a quoted SQL identifier. Mapping
field names preserve case and may collide with SQL keywords; injecting an
unquoted name could bind a different column or fail to parse. The same exact
identifier is used by the alias-shadow checks. For example, on a mapping whose
field is `Ts`, `SELECT raw AS "Ts" ...` must decline the implicit rewrite, while
an alias named `timestamp` is unrelated to that order.

The storage hint must name the field as well as the direction. A direction-only
hint has no way to prove that `ORDER BY ts` and the table's sort lead describe
the same value. The scan accepts the hint only when its field matches the
current Iceberg schema field selected by the identity sort lead. A missing,
renamed, non-timestamp or non-identity field keeps DataFusion's blocking sort.

## Declared and per-file direction

Fresh custom-field indexes already declare `ts ASC`; the write path reads the
table's declared `SortOrder`, sorts each batch before partitioning, and stamps
the same order in the Parquet footer. That path is field-generic
(`docs/DESIGN_time_ordered_storage.md`, "Mechanism: one invariant, every write
path").

The scan must still prove the files it reads agree with the declared order.
The existing mixed-direction rule remains:

1. Resolve the declared identity lead to its schema field id and name.
2. For a table with more than one sort order, attribute each live file by its
   manifest `sort_order_id`.
3. Accept a file order only when its first field has the same source id and
   identity transform; refuse missing or mixed directions.
4. Reverse the stream only when the requested direction differs from the one
   proven for every file.

Most of this check already compares source ids rather than names. The
field-specific work is the projected column, bounds and execution machinery
around it. A custom table with one sort order takes the same no-manifest-walk
path as a fresh canonical table.

## Duplicate event times

Custom event-time indexes deliberately have no `timestamp_ns` tiebreak. The
canonical sibling is valid only when `timestamp_ns` is the exact nanosecond
twin of `timestamp`; another mapping may carry an unrelated column with that
name (`crates/siglake-storage/src/index_manager.rs:325-338`). This
qualification does not authorize a schema addition or a change to that rule.

The missing tiebreak does not make `ORDER BY ts DESC LIMIT n` incorrect. SQL
does not specify the relative order of rows whose sole order key is equal, and
the table is physically ordered on that sole key. The ordered merge may return
any members of the equal-time group at the LIMIT boundary. Repeated executions
need not choose the same members.

The source-level pruning rule remains strict: for DESC, a later input is skipped
only when its upper bound is less than the exact nth value; for ASC, only when
its lower bound is greater. An equal bound is admitted
(`bound_is_strictly_behind`). This keeps every equal-time candidate in the
merge. Deterministic ordering among ties would need a declared second key and
an explicit SQL second key; that is separate schema and product work.

## Safe ordered early-stop

The implementation may advertise custom-field ordering only after these
literal-`timestamp` dependencies take the proven field:

- the query server's implicit rewrite, explicit-order recognition,
  projection-shadow guard, time-range recognition, residual-browse classifier
  and `OrderedScanLimit` derivation;
- `PreferredScanOrder`, including the field identity sent to local and shard
  planning;
- scan projection and physical output-order expressions;
- the time-only pushed-filter check;
- manifest lower/upper-bound lookup and time-contiguous file arrangement;
- reverse chunking, k-way merge expressions, frontier-value extraction and
  source-level LIMIT pruning;
- ordered-plan cache reconstruction. A cache hit must rebuild the physical
  expression for the same field whose proof was cached.

The order proof remains conservative. A field mismatch, unsupported Arrow
timestamp representation, missing file bounds, unknown file sort order, mixed
direction, excessive merge fan-in or residual filter without its existing hint
falls back to `SortExec`. The fallback returns the right rows and gives up the
early-stop saving.

## Local qualification

`query_provider::tests::custom_event_time_sort_is_stored_but_not_advertised`
creates a valid `ts`-mapped index that also carries an unrelated `timestamp`
column and compares it with a canonical index. It establishes these current
facts:

1. The table's declared identity sort lead is `ts`, with no second key, and
   both appended Parquet files are physically `ts ASC`.
2. The canonical table advertises its order under the same scan setup.
3. `scan_output_ordering` refuses the custom table specifically as
   `non_timestamp_sort`.
4. An explicit `ORDER BY ts DESC LIMIT 5` over two files, with the LIMIT cutting
   through three equal `ts` values, keeps `SortExec` and returns
   `[40, 35, 30, 20, 20]`.

The planner-side current contract is pinned by
`sql::tests::query_rewrites_order_a_bare_index_select_newest_first`: the
canonical index gets `ORDER BY timestamp DESC` and a descending
`PreferredScanOrder`; the `ts`-mapped index is unchanged and gets no preferred
order. Validation tests in `siglake-core` cover missing, non-datetime and
optional mapping fields.

These are correctness controls, not implementation acceptance. They show that
the shipped fallback is safe and identify the exact boundary #6020 must move.

## Verdict and implementation scope

**REVISE the extension, then proceed in #6020.** The implementation should:

1. Replace `index_orders_by_canonical_timestamp -> bool` with a cached mapping
   lookup that returns the exact event-time field for a managed index.
2. Quote that field in the implicit `ORDER BY`, and parameterize the planner's
   order, alias and time-range predicates by the resolved table field.
3. Carry the field in `PreferredScanOrder`; have storage verify it against the
   identity sort lead and use it in every ordered-read dependency listed
   above.
4. Add comparison fixtures for canonical `timestamp` and custom `ts` tables,
   overlapping files, duplicate times, explicit ordering, `default_order:
   false`, batch priority and alias shadowing.
5. Remove the matching `LIMITATIONS.md` entry and update `ARCHITECTURE.md` only
   when the end-to-end implementation passes.

The implementation does not add `timestamp_ns` to custom mappings, migrate a
table, promise stable ordering among equal times, or change a query that already
has `ORDER BY`. Those would be separate decisions.
