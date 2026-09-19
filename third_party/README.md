# Vendored forks

This directory contains first-class forks of three Apache-licensed crates:

- `iceberg` adds scan instrumentation, pruning, ordered writes, and maintenance actions.
- `iceberg-catalog-sql` avoids redundant metadata reloads during optimistic commits.
- `iceberg-storage-opendal` makes multipart uploads configurable and observable.

They are **forks, not pinned copies**: siglake depends on behavior that does
not exist upstream, so we maintain the divergences here and periodically
rebase against upstream. All retain their original `LICENSE` and `NOTICE`
files (Apache-2.0).

The divergence surface is git history since the 0.10.1 adoption plus this
file; the pristine upstream is the crates.io package under
`~/.cargo/registry/src/index.crates.io-*/`, which is what a rebase diffs
against. The crates.io packaging
artifacts `Cargo.toml.orig` and `DEPENDENCIES.rust.tsv` were dropped from all
three forks: nothing built or read them, and they described upstream's
manifest rather than the fork's, so `iceberg/Cargo.toml.orig` asked tokio for `sync`
alone long after the fork's manifest needed `rt` and `time`. `.cargo-ok` and
`.cargo_vcs_info.json` stay — the latter records the upstream commit each fork
came from.

The 0.10.1 rebase was adopted atomically on 2026-09-19 after six isolated
port-and-equivalence slices. The shipping graph is Arrow/Parquet 58,
DataFusion 53.1, OpenDAL 0.57 and reqsign 3. The temporary candidate forks were
removed after their sources and regression tests moved into these forks and
the permanent workspace suites.

## `iceberg/` — fork of `apache/iceberg-rust` (`iceberg` crate)

Upstream base: 0.10.1. Key divergences:

- **Reader-level scan instrumentation** (`ScanCounters`): files
  planned/read/bloom-pruned, row groups considered/pruned/read, rows pruned
  by selections, byte and S3-GET accounting — surfaced per-request as
  `stats.scan` in query responses.
- **Reversed (tail-first) row-group reads** for newest-first scans on
  ascending-ordered files: per-chunk decode from the file tail with
  progressive chunk growth, correct interaction with row selections and
  row filters.
- **Row-group token/trigram bloom filters** written as Parquet metadata and
  consulted at planning time (file- and row-group-level pruning for keyword
  and substring predicates).
- **Group-count footers**: per-file per-column value-count sketches written
  at flush time, serving zero-scan `GROUP BY`/`count(*)` fast paths,
  including typed (Int64/Float64/Boolean) columns via cast-at-tally.
- **Ordered write path**: Parquet `SortingColumn` footers stamped from the
  table's declared `SortOrder`; per-file `sort_order_id` stamping.
- **Scan against the table's current schema** (`TableScanBuilder::
  with_current_schema`, opt-in): upstream resolves a scan's columns and
  predicate against the schema the scanned snapshot was written under, so an
  additive schema evolution that commits no data leaves the new columns
  unscannable until some later append. siglake's query provider exposes the
  current schema to DataFusion, so it plans and scans against the same one.
- **Rewrite (overwrite) action** (`Transaction::rewrite_files`,
  `transaction/rewrite.rs`): one `Overwrite` snapshot whose live file set is
  `(previous live files − removed) ∪ added`, with removed entries kept as
  `Deleted` tombstones so a later snapshot expiry can delete them physically.
  It refuses to commit unless every file it was asked to remove was live. All
  three shapes are supported and the contract is written at the top of that
  file: both sides populated (compaction), delete-only (retention — and, since
  #2838, with no snapshot properties needed, which cost a matching relaxation of
  the guard in `SnapshotProducer::manifest_file`), and add-only, which carries
  every live manifest forward the way an append does. Both sides empty is
  refused.
- **Transaction identity pinning across retries**: a transaction retains its
  original table UUID and refuses to reapply actions if the catalog identifier
  has been dropped and recreated. The mismatch is non-retryable and is checked
  before actions can write manifest files for the replacement table.
- Snapshot expiry uses upstream's transaction action plus the public
  `planned_removals` preview needed for exact dry-run counts, no-op commit
  suppression and Siglake's coverage re-rooting. Upstream closed
  `incremental_append_scan` as not-planned, so that table behavior remains
  local.
- `Cargo.toml`: tokio with `rt` and `time` where upstream asks only for
  `sync` — `rt` for `tokio::task::coop::consume_budget` in `io/file_io.rs`,
  `time` for the `tokio::time::timeout` around the reversed-chunk in-flight
  wait in `arrow/reader.rs` and the `tokio::time::sleep` handed to backon in
  `transaction/mod.rs`. Upstream's manifest under-declares the same two:
  reqwest turns `time` on and parquet turns `macros` on, so a workspace build
  compiles either way and only a standalone build of the fork can tell.
  `macros` and `rt-multi-thread`, which only the tests and the five
  `#[tokio::main]` doctests need, are a `[dev-dependencies.tokio]` entry
  instead. `scripts/check-fork-tests.sh` runs this fork's doctests, so a doc
  example that needs a dev-dependency the manifest does not declare fails the
  gate.

### Running the fork's own unit tests

The forks are `[patch.crates-io]` path dependencies, not workspace members, so
`cargo test --workspace` builds them without `--cfg test` and never compiles
their `#[cfg(test)]` modules. `scripts/check-fork-tests.sh` compiles and runs
them, and `scripts/ci-local.sh` runs it as its `fork-tests` job:

```
scripts/check-fork-tests.sh                 # all three forks
scripts/check-fork-tests.sh --fork iceberg  # one of them
scripts/check-fork-tests.sh --keep          # leave the mirror behind, named on stderr
```

What the script sets up, and why, for whoever next has to change it:

1. A mirror directory outside any cargo workspace — anywhere inside this
   checkout, `target/` included, makes cargo resolve `crates/siglake-bloom`
   twice and the run dies with a package collision in the lockfile. In it,
   symlinks to this checkout's `Cargo.toml`, `crates/`, `third_party/` and
   `rust-toolchain.toml`, so the forks' relative path dependencies on
   `crates/siglake-{bloom,index}` and their `workspace.package` inheritance
   resolve.
2. Per fork, two directory levels down so `../../crates/...` still points at
   those symlinks: a symlinked `src/` (and `testdata/`) beside a copy of the
   fork's `Cargo.toml` with `[workspace]` appended. That copy then has to carry
   back what the enclosing workspace supplied:
   - `iceberg-catalog-sql` and `iceberg-storage-opendal` both depend on
     `iceberg = "0.10.0"` from crates.io, and the root manifest's patch does not
     reach a standalone mirror, so their copies carry a `[patch.crates-io]`
     entry pointing `iceberg` at the fork; the script then checks the resolved
     lockfile, because a run against upstream 0.10 would pass while proving
     nothing.
   - `iceberg-storage-opendal` takes `metrics` with `workspace = true`, so its
     copy also gets a `[workspace.dependencies]` block holding the root
     manifest's line for `metrics`. The script lifts that line rather than
     pinning its own, and fails if the root stops pinning it.
3. `cargo test --lib` there, with the fork's default features — what siglake
   ships — and `cargo test --doc` for the two forks that have doc examples:
   - `iceberg`, which since #2671's `[dev-dependencies.tokio]` builds its five
     `#[tokio::main]` doctests standalone (85 pass, 9 ignored, ~4 s of the job:
     rustdoc rebuilds the merged doctest binary every run).
   - `iceberg-catalog-sql`, whose one crate-root example is `#[tokio::main]`
     too and failed the same way until #2784 gave its dev-dependency on tokio
     the `macros` and `rt-multi-thread` features (1 passes, ~1 s).
   - `iceberg-storage-opendal` has no doc examples — #2784 found no fenced
     block anywhere under its `src/` — so it is `--lib` only, with no
     `DOC_MIN_TESTS` entry rather than a floor of 0 that would assert nothing.
     Adding a doc example to it means adding an entry with it.

   The gate keys the doctest arm off `DOC_MIN_TESTS` in the script, one entry
   per fork, rather than a global flag. Non-default features are out, so
   `iceberg-storage-opendal`'s `src/azdls.rs` test module (behind
   `opendal-azdls`) is not compiled by this gate.

Each fork carries a floor on the number of tests that must run (`MIN_TESTS` in
the script, `DOC_MIN_TESTS` for the doctests), because a filter, feature or
mirror mistake that selects nothing exits 0 and prints "0 passed".

The mirror is removed on exit, but its path is derived from the checkout and
the sources under test rather than randomised: a path dependency's absolute
path is part of cargo's unit hash, so a fresh path would mean recompiling the
fork and everything below it on every run (~46 s against ~3 s).

Do not run `rustfmt` over the forks: they carry upstream's formatting
settings, which are not vendored, so a default-config run rewrites hundreds of
untouched lines.

## `iceberg-catalog-sql/` — fork of the SQL catalog

- `Catalog::update_table_with_base`: commit re-load elision — drops the
  redundant per-commit `metadata.json` read and tightens the optimistic
  lock (measured as a major commit-throughput lever).
- `Cargo.toml`: `[dev-dependencies.tokio]` asks for `macros` and
  `rt-multi-thread`; upstream's entry names no features at all. Both the
  `#[tokio::test]`s in `src/catalog.rs` and the `#[tokio::main]` in the
  crate-root example need `macros`, which the graph supplies anyway (parquet,
  through the iceberg fork), and the example's default runtime flavour needs
  `rt-multi-thread`, which nothing else turns on — so it compiled inside the
  workspace and failed standalone until #2784.

## Rebase policy

Rebase against upstream opportunistically (not on a schedule), keeping the
divergence surface documented in this file. Feature work is **not** gated on
upstream releases. When upstream ships an equivalent facility, prefer
migrating to it and shrinking the fork.

## `iceberg-storage-opendal`

Originally vendored 2026-08-08 for object-store multipart-upload concurrency;
rebased to 0.10.1 on 2026-09-19.

Upstream builds every writer as `op.writer(path)` with no options, and opendal's
`WriteOptions` derives `Default` with `concurrent: 0`, which its `MultipartWrite`
passes into `ConcurrentTasks::new(executor, concurrent, ..)`. So every Parquet
file siglake writes to object storage sends its parts **one at a time** — the
2026-08-08 1TB round measured flush (S3 PUT) at 30.2% of append time, the
largest single stage, with ~534 MB files going up in ~133 MB parts serially.

The fork adds `SIGLAKE_OBJECT_STORE_WRITE_CONCURRENCY`, **default `0` = upstream
behaviour unchanged**. It is a knob rather than a new default so a round can
isolate the change instead of confounding it with whatever else shipped in the
same image — the mistake made with the throttled-slot A/B on 2026-08-05.

Since then the fork has grown three more divergences. A
rebase onto a newer upstream must carry every item below. The upstream
external-service tests remain named but are feature-gated because their
published package omits the workspace-only test utility crate and the tests
require live GCS/S3/Azure credentials.

- **Multipart part size** (2026-08-09): `SIGLAKE_OBJECT_STORE_WRITE_CHUNK_MB`,
  default `0` = opendal's service default (~128 MiB on S3). Only consulted when
  concurrency is enabled, because writer memory is roughly `concurrent x chunk`.
  `writer()` publishes the effective values as the gauges
  `siglake_object_store_write_concurrency` and
  `siglake_object_store_write_chunk_bytes`, so a round can prove the knob was
  actually in effect.
- **Jittered retry backoff** (2026-08-10), in `create_operator`: upstream wraps
  the operator in `RetryLayer::new()` (backon defaults: 1 s minimum delay,
  factor 2, 3 attempts, no jitter). The fork uses
  `.with_jitter().with_min_delay(100 ms).with_max_times(5)` so concurrent
  retriers stop colliding in lockstep.
- **Per-class upload permits** (2026-08-15): the drain path and compaction share
  the object-store write path, and the 2026-08-15 1TB round showed drain uploads
  starving compaction (52 recluster-watchdog trips, all during ingest, all on
  `--role combined` nodes; Quickwit fixed the same problem with two semaphores
  in quickwit#6376). The fork adds:
  - `pub enum UploadClass { Drain, Compaction }`, the `UPLOAD_CLASS` tokio
    task-local, and `pub async fn with_upload_class(class, fut)`, which accounts
    every upload inside `fut` to `class`. The compactor wraps its rewrites in
    `with_upload_class(UploadClass::Compaction, ..)`; no task-local means Drain.
  - Two process-wide `tokio::sync::Semaphore`s behind `upload_permits(class)`,
    sized once on first use (`OnceLock`) from `SIGLAKE_S3_WRITE_PERMITS_DRAIN`
    (default `64`) and `SIGLAKE_S3_WRITE_PERMITS_COMPACTION` (default `32`).
    `0` or an unparsable value falls back to the default: the pools exist to
    stop one class starving the other, not to throttle either.
    `pub fn available_upload_permits(class)` exposes the free count for tests
    and observability.
  - `writer()` acquires one permit of the caller's class and holds it for the
    writer's whole lifetime, not just its creation. `OpenDalWriter` is no
    longer upstream's `OpenDalWriter(opendal::Writer)` newtype but a struct
    carrying a `ClassPermit`, whose `Drop` returns the permit via
    `add_permits(1)`.
  - The counter `siglake_object_store_writer_opened_total{class="drain"|"compaction"}`,
    incremented once per writer opened.
  - `Cargo.toml`: dependencies on `metrics` (workspace) and `tokio` with the
    `sync` feature, neither of which upstream has.
