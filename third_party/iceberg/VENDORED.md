# Vendored apache/iceberg-rust 0.10.1

This fork is based on the published `iceberg` crate 0.10.1. It is in-tree so Siglake
can own and modify the Parquet **read path** (the scan/arrow reader), which the
published crate exposes only as a closed box. Wired via `[patch.crates-io]` in
the workspace Cargo.toml so Siglake + the iceberg-catalog-sql / -datafusion /
-storage-opendal companions all build against this copy.

Keep API additions backward-compatible so the companion crates keep compiling.
The 0.10.1 base is upstream commit
`04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`, `path_in_vcs`
`crates/iceberg`. Local modifications are tracked in git history and in
`third_party/README.md`.
