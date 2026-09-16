# Vendored apache/iceberg-rust `iceberg-catalog-sql` 0.9.1

A copy of the `iceberg-catalog-sql` crate 0.9.1, pulled in-tree so siglake can
own and modify the **commit path**. The published crate's `update_table`
re-loads the table (one S3 GET + full `metadata.json` parse) on every commit,
even though `Transaction::do_commit` in the (also-vendored) `iceberg` core has
*just* loaded that same table to compute the diff. BIG-4 lever-2 removes that
redundant read by threading the already-loaded base table through a new
`Catalog::update_table_with_base` hook (default-implemented in the core crate,
overridden here). Wired via `[patch.crates-io]` in the workspace `Cargo.toml`.

The `iceberg = "0.9.1"` version dependency below is redirected to the vendored
core fork (`third_party/iceberg`) by the same workspace patch, so both forks
build against each other.

Keep API surface backward-compatible so unmodified call sites keep compiling.
Local modifications are tracked in git history vs this initial 0.9.1 import.

## Upstream base

crates.io `iceberg-catalog-sql` 0.9.1, upstream commit
`a78bd0dd02734bf53bec7d47624a5adb4409f1d3`, `path_in_vcs` `crates/catalog/sql`
— the same commit `third_party/iceberg` and `third_party/iceberg-storage-opendal`
record. The sha comes from the pristine 0.9.1 package's own
`.cargo_vcs_info.json` in the cargo registry
(`~/.cargo/registry/src/index.crates.io-*/iceberg-catalog-sql-0.9.1/`); it is not
derived from a siglake commit. Git history cannot supply an earlier vendoring
commit — the tree was squashed at 614c6cd (2026-06-12), the first commit that
touches this directory.

That file was missing from this fork alone until 2026-09-10, when it was copied
in verbatim from that package, so all three forks now carry the machine-readable
provenance a rebase diffs against.
