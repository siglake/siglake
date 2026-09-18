# Design: BIG-3 hand-rolled orphan-file GC

Status: **implemented** (2026-06-02), local-FS validated; AWS smoke pending.
Reclaims object-storage space left behind by tier-2 re-clustering overwrites
and `#4c` snapshot expiry, which are now **default-on** (round 60) so every
cluster accrues orphans over time.

Shipped: `IcebergContext::{reachable_files,gc_orphans}` + `GcOptions`/
`GcReport` (`crates/siglake-storage/src/iceberg.rs`); CLI `siglake gc-orphans
--table T [--apply] [--min-age-secs N]` (dry-run default); metrics
`siglake_gc_orphans_{found,deleted}_total` + `siglake_gc_bytes_reclaimed_total`.
Listing/deletion go through a warehouse-scoped `opendal::Operator` — the Fs
*lister* doesn't populate mtime/size, so each orphan **candidate** is
`stat`'d (only candidates pay it). Gates:
`reachable_files_tracks_live_set_and_excludes_orphans`,
`gc_orphans_dry_run_then_apply_conserves_rows`,
`gc_orphans_respects_min_age_safety_window`. Scheduling is operator-side
(CronJob running the CLI), matching `audit-rotate`.

Statistics entries are retired before the physical sweep. The elected snapshot
expiry transaction evaluates them after applying snapshot removal, and a direct
`gc-orphans --apply` run performs the same maintenance step. An entry is
eligible only when every blob is a Siglake-owned inverted-index type, every
blob names `data_file`, and no named file is alive in any retained snapshot.
Entries with one live reference stay whole; foreign or incomplete entries are
left untouched. Once `RemoveStatistics` commits, the Puffin object leaves the
reachable set and the existing `min_age` rule controls deletion. The GC report
includes the retirement classification, and
`siglake_iceberg_statistics_removed_total` counts committed removals.

## Why hand-rolled

We own the vendored `iceberg` fork and do **not** wait on an upstream
`expire_snapshots` (no apache `iceberg` 0.10 exists; see `third_party/README.md` for the fork policy). The
vendored `ExpireSnapshotsAction` (`#4c`) is **metadata-only** — it drops
snapshots from `metadata.json` but leaves their manifest-list / manifest /
data files in object storage as orphans. BIG-3 adds the **physical
reclamation**: delete files unreachable from any *retained* snapshot.

## The invariant (get this wrong → data loss)

> A file may be deleted **only if** it is not in the reachable set of the
> current table metadata **and** it is older than a safety window.

### Reachable set
After `#4c` runs, `metadata.snapshots()` is exactly the *retained* set
(expired snapshots are removed from metadata). The reachable set is the union
over every retained snapshot of:

1. the snapshot's **manifest-list** avro (`Snapshot::manifest_list()`),
2. every **manifest** avro in that list (`ManifestFile::manifest_path`),
3. every **alive data file** in those manifests — entries with
   `ManifestStatus::{Added,Existing}` (`ManifestEntry::is_alive()`),
   via `entry.data_file().file_path()`.
4. every statistics-file path whose metadata entry remains registered after
   the conservative retirement rule above.

**`is_alive()` filtering is required, not optional.** A re-cluster rewrite
marks the old file `Deleted` in the new snapshot's manifest while a *pre-
rewrite* snapshot still has it `Added`. Once that older snapshot is expired
(`#4c`) and removed from metadata, the old file appears only as `Deleted` in
the retained manifests → no retained snapshot scans it → it is exactly the
orphan we want to reclaim. Conversely, any file a retained snapshot actually
reads is `Added`/`Existing` in that snapshot's manifest, so the alive-union
never drops a live file.

Traversal uses the public fork APIs (no fork change):
`Snapshot::load_manifest_list(file_io, metadata)` → `ManifestList::entries()`
→ `ManifestFile::load_manifest(file_io)` → `Manifest::entries()` →
`ManifestEntry::is_alive()` + `data_file().file_path()`.

### Safety window (the in-flight-write race)
A concurrent compactor commit may have *just* written a data file +
manifest that isn't yet referenced by the metadata snapshot we loaded.
Deleting it = data loss. So we **only delete files older than
`min_age`** (default 24 h, like Iceberg's `RemoveOrphanFiles` 3-day-ish
conservatism; configurable, 0 in tests). File mtime comes from the
object-store listing.

### Scope
- Only files under the table's `data/` and `metadata/` prefixes (derived
  from `TableMetadata::location()`).
- **`*.metadata.json` is excluded** from v1 — `#4c` already bounds the
  snapshots array, deleting metadata.json risks breaking rollback /
  concurrent readers, and it's a separate concern. Bulk storage is data +
  manifest + manifest-list files.
- Never the catalog `_catalog.db`.

## Listing — where it lives

`FileIO`/`Storage` (vendored) exposes `delete`/`exists`/`delete_prefix`/
`metadata` but **no list**; the concrete opendal-backed `Storage` lives in
the *non-vendored* `iceberg-storage-opendal`. Rather than vendor a third
crate or add a trait method that ripples across forks, BIG-3 builds a
**warehouse-scoped `opendal::Operator` inside siglake-storage** (the WAL
mirror already uses opendal this way) for listing + deletion:
- `file://` → `opendal::services::Fs` rooted at the warehouse path.
- `s3://`   → `opendal::services::S3` (bucket from URL, region/creds from
  the native cred chain — same as `OpenDalStorageFactory::S3`).
- `memory://` is **not supported** for GC (a separate operator wouldn't see
  iceberg's in-memory files) — fine: integration tests use `file://`, prod
  uses S3.

Path normalization between the iceberg reachable paths (full URIs) and the
opendal listing (operator-root-relative) is handled centrally and tested —
a mismatch would make every file look orphaned, so it gets explicit
coverage.

## API + CLI

- `IcebergContext::reachable_files(table_ident) -> HashSet<String>` — the
  live set (implemented + tested first; fork-free).
- `IcebergContext::gc_orphans(table_ident, GcOptions { min_age, apply }) ->
  GcReport { scanned, orphans, bytes, deleted }` — list, diff, age-gate,
  and (only when `apply`) delete. **Dry-run by default.**
- CLI `siglake gc-orphans --table events [--apply] [--min-age-secs N]` —
  dry-run unless `--apply`. Metrics: `siglake_gc_orphans_{found,deleted}_total`,
  `_bytes_reclaimed_total`.

## Test plan (correctness-first)

1. **Reachable set exactness** (this commit): build a table, append +
   re-cluster + expire, then assert the reachable set (a) contains every
   data/manifest/manifest-list file the current snapshots reference, and
   (b) excludes the files a re-cluster overwrote + expired (which are now
   orphans). No deletion.
2. **Dry-run diff**: orphans = listed − reachable; assert the count/bytes
   match the known re-clustered-away set; assert nothing is deleted.
3. **Apply conserves rows**: with `min_age=0`, apply the GC, then
   `count(*)` is unchanged and a fresh scan reads every live file (no
   "file not found"). Re-running is a clean no-op.
4. **Safety window**: a freshly-written-but-unreferenced file younger than
   `min_age` is **not** deleted.
