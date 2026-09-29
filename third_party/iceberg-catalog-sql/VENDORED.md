# Vendored apache/iceberg-rust `iceberg-catalog-sql` 0.10.1

A fork of the `iceberg-catalog-sql` crate 0.10.1, kept in-tree so Siglake can
own and modify the **commit path**. The published crate's `update_table`
re-loads the table (one S3 GET + full `metadata.json` parse) on every commit,
even though `Transaction::do_commit` in the (also-vendored) `iceberg` core has
*just* loaded that same table to compute the diff. BIG-4 lever-2 removes that
redundant read by threading the already-loaded base table through a new
`Catalog::update_table_with_base` hook (default-implemented in the core crate,
overridden here). Wired via `[patch.crates-io]` in the workspace `Cargo.toml`.

The `iceberg = "0.10.0"` version dependency below is redirected to the vendored
core fork (`third_party/iceberg`) by the same workspace patch, so both forks
build against each other.

Keep API surface backward-compatible so unmodified call sites keep compiling.
Local modifications are tracked in git history against the 0.10.1 base.

## Upstream base

crates.io `iceberg-catalog-sql` 0.10.1, upstream commit
`04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`, `path_in_vcs` `crates/catalog/sql`
— the same commit `third_party/iceberg` and `third_party/iceberg-storage-opendal`
record. The sha comes from the pristine 0.10.1 package's own
`.cargo_vcs_info.json` in the cargo registry
(`~/.cargo/registry/src/index.crates.io-*/iceberg-catalog-sql-0.10.1/`); it is not
derived from a Siglake commit. Git history cannot supply an earlier vendoring
commit — the tree was squashed at 614c6cd (2026-06-12), the first commit that
touches this directory.

All three forks carry the machine-readable provenance used for package diffs.
