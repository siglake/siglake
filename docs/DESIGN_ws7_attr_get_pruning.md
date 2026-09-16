# WS-7 next slice: `attr_get` predicates ride the promoted columns (design)

*2026-07-14. Status: designed, not implemented. Prerequisite reading:
round 70 (WS-7 dense extraction validated on EKS),
`crates/siglake-core/src/promote.rs`.*

## What already shipped (don't re-implement)

The BIG_TRACKS Track-4 description lagged reality. Landed and
AWS-validated in round 70 (`ef7b22e`, `149cd4d`, `dca30ef`,
`de546f0`-era arc):

- **Declared promotion**: `PromotedColumn { attr_key, name, ty }`
  (CLI spec `attr_key:type[:column]`), `promote_attributes()` widens
  every events batch at write time — ingest AND the compactor's
  re-cluster rewrites (old files backfill progressively as they are
  re-clustered).
- **Schema**: `events_schema_with(&promoted)` + additive
  `ensure_promoted_columns()` widening (migrate-schema wired).
- **Stats**: every promoted column gets Parquet column stats, preserved by
  re-clustering. Parquet-native SBBF blooms for Utf8 promotions are now an
  opt-in, write-only knob (`SIGLAKE_PARQUET_NATIVE_BLOOMS=on`), off by default
  since 2026-08-06 because the time-sorted layout makes them unselective.
- **Reads tolerate mixed widths** (`dca30ef`); the WAL-buffer union
  null-fills missing promoted columns.

So: a query that filters **on the promoted column by name**
(`WHERE k8s_pod = 'x'`) already prunes via manifest bounds + row-group
stats + page index. What does NOT prune is the form users and our own
Jaeger route emit: `WHERE attr_get(attributes, 'k8s.pod') = 'x'` — the
UDF is opaque to every pruning layer, so it full-scans and JSON-parses
per row.

## Why the obvious rewrite is unsound

Rewriting `attr_get(attributes,'k')` → column `k8s_pod` at plan time
changes answers for rows written BEFORE the promotion was declared:
those files lack the column entirely (read as NULL) while their
`attributes` JSON still carries the key. Backfill via re-clustering is
progressive, not guaranteed complete. `COALESCE(col, attr_get(...))`
is sound but defeats pruning (the fallback branch is opaque), which
was the whole point.

## The sound design: a promoted prune spec (reader-level)

Same pattern as `RawPruneSpec` — pruning hints that never change
correctness; execution still evaluates the original predicate:

1. **Key→column mapping as a table property.**
   `ensure_promoted_columns()` additionally records
   `siglake.promoted.v1 = JSON [{key,name,ty}]` on the table, so the
   query provider can map attr keys → column names without plumbing
   CLI state (providers only hold `Table`).
2. **Extraction** (query_provider, next to `extract_raw_prune_spec`):
   recognize `attr_get(attributes, 'key') = 'lit'` (+ `IN` lists) in
   pushed filters; when `key` maps to a promoted column, emit
   `PromotedPruneSpec { column, values }`.
3. **Reader** (fork, `process_file_scan_task`): for each file whose
   schema carries `column`, drop row groups whose min/max stats prove
   every value in `values` absent (and optionally consult the Parquet
   SBBF bloom for equality). Files written pre-promotion lack the
   column → no bounds → kept (conservative, correct). A file whose
   row groups all drop is skipped after the footer read — the same
   shape as the trigram file skip, and it lands in the new
   `stats.scan` counters (`row_groups_pruned_stats`).

Soundness argument: within any post-promotion file, the promoted
column *is* the extraction of the key (written by
`promote_attributes` from the same JSON), so column stats proving
`'lit'` absent prove no row's `attr_get` can equal `'lit'`.
Pre-promotion files are never pruned. No backfill requirement, no
gate, no semantic change.

## Later slices (in order of value)

- **Full column rewrite behind a backfill gate**: an explicit
  `backfill-promotions` op (compactor full rewrite) that sets
  `siglake.promotion_backfill_complete=true`; only then rewrite
  `attr_get` → column reference so GROUP BY/aggregates get typed-column
  speed and the group-count fast paths can pick promoted dims up.
- **Frequency-driven promotion** (auto-promote hot keys from a per-file
  key-frequency sketch — the same footer-sketch machinery BM25 wants).
- **Jaeger route**: once the prune spec lands, its generated
  `attr_get(attributes, k) = v` filters prune with zero changes.
