# Attribute auto-promotion: qualification of the opt-in path

*2026-09-17 (#3052), observability added 2026-09-21 (#5060). Status: qualified
as opt-in, at its bounds. No default change proposed, and this document does
not authorize one.*

Prerequisite reading: `DESIGN_ws7_attr_get_pruning.md` (what promotion is for),
`crates/siglake-core/src/promote.rs` (extraction), `LIMITATIONS.md`.

## The path, end to end

Auto-promotion is the frequency-driven half of WS-7. The compactor's
re-clustering pass calls it at the end of a cycle, behind an env gate and a
fixed 300 s cadence (`crates/siglake-compactor/src/lib.rs`,
`run_recluster_once_shaped`), and it composes with everything the declared path
already does:

1. **Sample.** `IcebergContext::auto_promote_hot_keys_for` lists the table's
   live data files, takes the newest `sample_files` of them, and reads the
   `attributes` column of the first `sample_rows` rows of each.
2. **Census.** Each row's residual JSON contributes one hit per scalar key,
   with nested objects addressed one level down by dotted key. A key's sampled
   values merge through a type lattice: one kind promotes, `Int` widens to
   `Float`, every other mix is `Mixed` and stays residual.
3. **Select.** `select_promotions` — pure, no IO — keeps keys above the
   threshold that are not already promoted and whose sanitized name collides
   with nothing in the schema, ranks them hottest-first with a name tie-break,
   and cuts the list at the column ceiling.
4. **Declare.** `declare_promotions_for` records `siglake.promoted.v1` and then
   widens the schema additively. Two catalog transactions, and the order
   decides correctness: property first leaves an in-window writer seeing a promotion
   whose column is absent, so its file visibly LACKS the column and both
   backfill predicates select it for repair. Widening first would leave files
   with the columns present and all-NULL, which those predicates read as
   already backfilled — and once the gate flips, those rows vanish from
   results.
5. **Materialize, backfill, gate.** The write path extracts the new columns on
   the next commit; re-clustering rewrites the older files, re-extracting from
   their JSON; `ensure_promotion_backfill_property` flips the completion
   property once every live file carries every promoted column; only then does
   the query server rewrite `attr_get(attributes, k)` to the column.

Step 4 is why this card exists. Nothing in steps 1–3 asks an operator
anything, and step 5 is where a wrong answer would become visible — after the
schema has already been widened, which nothing in the product undoes.

## Why this is not the migration runner

Both widen schemas additively, and they share nothing else.

| | auto-promotion | schema migration |
|---|---|---|
| Trigger | a sampling verdict, every 300 s | a chart upgrade or `spec.schemaVersion` change |
| Decides | the compactor, from a bounded sample | the operator of the cluster, by editing a value |
| Shape | whichever keys were hot in that sample | the columns the declared schema names |
| Operator's part | reads a counter afterwards, if it looks | asks for it, and the runner reports the outcome |
| Refusal path | none: the pass either promotes or does not | `migrate-schema` refuses a table it cannot migrate additively, naming the column |
| Reversal | none | none — but nothing happened that was not requested |

The operator's migration runner is report-only by design (standing invariant:
"the operator's migration runner is report-only; migrations run on request").
Auto-promotion is the one path that mutates a schema on its own initiative, so
it ships off and its bounds carry the whole argument.

## The bounds

Every knob has a pure resolver twin, and the tests drive the resolvers.

| Knob | Default | Bound | Why that bound |
|---|---|---|---|
| `SIGLAKE_AUTO_PROMOTE_MIN_PCT` | `0` (off) | positive values clamp to `[1, 100]` | 1% of the default sample is 164 rows of evidence; 0.1% is 16, and below that the ranking is noise choosing irreversible schema changes. A finer request is RAISED to the floor — stricter than asked. |
| `SIGLAKE_AUTO_PROMOTE_MAX_COLUMNS` | `16` | ceiling `64`; `0` is a second off switch | Each promoted column is extracted on every write, re-extracted by every backfill rewrite, and never removed. This is the blast radius of a mistyped knob. |
| `SIGLAKE_AUTO_PROMOTE_SAMPLE_FILES` | `4` | `1..=64` | Files read per pass. Zero is not an off switch here (the threshold and the ceiling are), so it resolves to one. |
| `SIGLAKE_AUTO_PROMOTE_SAMPLE_ROWS` | `4096` | `256..=65536` | Rows per file. The floor keeps the 1% threshold meaningful (256 rows is 2 hits); the product bounds one pass at ~4.2 M parsed documents in the worst configured case. |
| key census (not configurable) | — | 4096 distinct keys | The sample is bounded in rows, not in the key space those rows carry: a tenant whose keys embed an id presents a fresh key per row. Keys already tracked keep accruing; new ones past the cap are dropped and counted (`siglake_auto_promotion_sampled_keys_dropped_total`). A dropped key was seen too rarely to promote anyway. |
| cadence (not configurable) | 300 s | — | One pass per 300 s per process. |

The sample takes the NEWEST files, by timestamp upper bound, with size as the
tie-break for a table whose manifests carry no time bound. Until 2026-09-17 the
code sorted by file size while its comment claimed newest: it sampled whichever
files compaction had last merged, so on a table whose attribute shape had
changed it answered about the wrong era. The ordering is now a pure function
with its own test.

## What it costs

`cargo test --release -p siglake-storage --test auto_promotion_cost --
--ignored --nocapture`, 2026-09-17, 24-core dev box, load ~2, local
filesystem warehouse. Fixture: 8 files × 10 000 rows, 11 attribute keys per row
(two nested one level, one unique per row), 3.4 MB of Parquet. Repeat runs vary
about ±10%, so read these as one significant figure.

**One sampling pass**, by bound. The fixture holds 80 000 rows, so the last two
arms are capped by the data rather than by the bound — which is the point: the
bound is a cap, not a target.

| `files × rows` | rows sampled | wall | documents/s |
|---|---|---|---|
| 1 × 256 | 256 | 14 ms | 18 K |
| 4 × 4096 (default) | 16 384 | 75 ms | 219 K |
| 8 × 16 384 | 80 000 | 292 ms | 274 K |
| 64 × 65 536 | 80 000 | 290 ms | 276 K |

So the default bound is 75 ms of one core per table, and a pass visits the
events table plus each managed index — so `(1 + indexes) × 75 ms` every 300 s,
a 0.025% duty cycle per table. Sustained throughput is about 275 K JSON
documents per second once the per-file overhead amortizes, which puts the worst
configured bound at ~15 s of one core per table per pass: inside the cadence
for a handful of indexes, and the reason the row ceiling exists at all.

**Each promoted column**, on the write path. Extraction of 80 000 rows, with
every column populated (the synthetic columns re-extract a real key under a new
name, so an absent key's `append_null` shortcut does not flatter the number):

| promoted columns | extraction | rows/s | Parquet bytes | vs none | append wall |
|---|---|---|---|---|---|
| 0 | — (identity) | — | 3 383 552 | 1.00× | 319 ms |
| 1 | 85 ms | 944 K | — | — | — |
| 8 | 115 ms | 694 K | 3 524 432 | 1.04× | 437 ms |
| 16 (default cap) | 143 ms | 561 K | 3 667 558 | 1.08× | 497 ms |
| 32 | 199 ms | 403 K | — | — | — |
| 64 (ceiling) | 328 ms | 244 K | 4 489 719 | 1.33× | 1001 ms |

The first column is the expensive one: it pays the per-row JSON parse (~1 µs a
row), and each column after it adds ~50 ns a row. Storage is the constraint at
the ceiling, not CPU — 64 promoted columns is a third more Parquet and 3× the
append wall on this fixture, because a promoted value is stored twice (once
typed, once in the residual JSON, which promotion does not prune).

## What is tested

- `crates/siglake-storage/src/iceberg.rs`, `auto_promotion_sampling_tests`:
  the type lattice (including that `Mixed` never recovers), the census
  denominator, dotted nested keys, the census cap and its drop count, the
  column ceiling counting existing promotions, hottest-first ranking with the
  name tie-break, already-promoted and name-colliding candidates, the empty
  sample, and newest-first file ordering.
- `crates/siglake-compactor/src/lib.rs`, `auto_promotion_knob_tests`: every
  spelling of "not asked for" resolves to off, the threshold clamps at both
  ends, the column cap defaults/caps/off-switches, the sample bound clamps.
- `crates/siglake-storage/tests/auto_promotion_bounds.rs`: a zero threshold and
  a zero ceiling each leave schema and properties identical (with a negative
  control that the same fixture does promote when the feature is on); the
  ceiling bounds this pass and the next.
- `crates/siglake-query-server/tests/query_server/auto_promotion_bounds.rs`:
  mixed-type and explicit-null keys stay residual and stay queryable in both
  spellings; a promoted key's answer is exact before the promotion, mid-backfill
  (where the column is short by the pre-promotion files) and after the gate
  flips, when both spellings agree per group and in total.
- Already covered before this card, and unchanged by it:
  `promoted_prune.rs` (pruning, the pre-promotion soundness case, the
  autonomous loop on the events table and on a user index, nested leaves, the
  footer-served equality count) and `typed_promotion.rs` (numeric comparison
  semantics after the rewrite, zero-scan typed aggregates).

## Pinned, not endorsed

- **An explicit JSON null poisons a key.** `Value::Null` is not a scalar, so a
  key that is a string in most rows and `null` in the rest merges to `Mixed`
  and never promotes — even though a promoted cell for such a row would be
  null anyway, which is exactly what the extractor writes. Treating null as
  "no type information" instead would promote strictly more keys, so the
  current behaviour is the conservative one and the tests pin it. Changing it
  is a promotion-policy decision, not a bug fix.
- **A promotion is forever, and nothing watches whether it still earns its
  keep.** A key that was hot for one hour is a column for the life of the
  table. The pass never revisits a promotion, and a promoted column that has
  gone cold reports nothing about it.

## What a default-on decision would still need

This qualification says the bounds hold and the behaviour is what the code
claims. It does not say the feature should be on. Missing:

1. **Fleet-scale evidence.** Every number here is one local process on a
   filesystem warehouse. Sampling requests and bytes are now attributable per
   pass, and backfill files, input/output bytes and duration are attributable
   per bin. Their cost against S3 remains unmeasured until #5065's round.
2. **The backfill wave.** Each promotion makes every live file eligible for a
   re-clustering rewrite. On a large table that is a whole-table rewrite per
   promotion wave, competing with the drain for the same compactor. The cost is
   now attributable per committed backfill bin, but nothing paces promotions
   against it and #5065 has not measured a wave against S3.
3. **Reach and scope.** The pass runs on the compactor's own namespace and its
   managed indexes. On a multi-tenant install nothing extends it to the
   `tenant_*` namespaces the drain commits into, and the 300 s cadence gate is
   process-global — harmless while the re-clustering pass is single-namespace,
   latent the moment it is not.
4. **A per-tenant override.** The knobs are process-wide. One tenant's
   attribute shape decides promotions for every table the compactor's namespace
   holds.

The former observability item closed in #5060. Every discovered table now
pre-registers `siglake_auto_promotion_passes_total` for `promoted`,
`nothing_cleared`, `at_ceiling` and `failed`, so a zero is a known table whose
pass has not completed rather than an absent series. The enabled gauge separates
that state from a disabled feature. `siglake_auto_promotion_candidates` is the
last sampled count of unpromoted keys that cleared the frequency bar, including
keys later refused for mixed type, name collision or the configured cap; its
availability sibling is zero on disabled and already-at-cap paths, which still
perform no data-file reads. `siglake_auto_promotion_columns{kind="used|limit"}`
publishes usage against the effective configured cap per Iceberg namespace and
table. The packaged dashboard reads all of these and
`SiglakeAutoPromotionNearCeiling` warns at 80%. One INFO line per sampled pass
retains bounded key names by refusal reason and an exact truncation count.
`siglake_auto_promotion_sample_reads_total` and
`siglake_auto_promotion_sample_bytes_total{phase="footer|index|data"}` attribute
the sampling reads per Iceberg namespace and table, while
`siglake_auto_promotion_pass_duration_seconds` records one duration per sampled
pass. A round prices the rewrite wave from
`siglake_compactor_promotion_backfill_files_total`, the paired
`siglake_compactor_promotion_backfill_bytes_in_total` and
`siglake_compactor_promotion_backfill_bytes_out_total`, and
`siglake_compactor_promotion_backfill_duration_seconds`; those series are
recorded per committed backfill bin and retain the table label.
