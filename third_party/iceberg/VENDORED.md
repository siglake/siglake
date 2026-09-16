# Vendored apache/iceberg-rust 0.9.1

This is a verbatim copy of the `iceberg` crate 0.9.1, pulled in-tree so siglake
can own and modify the Parquet **read path** (the scan/arrow reader), which the
published crate exposes only as a closed box. Wired via `[patch.crates-io]` in
the workspace Cargo.toml so siglake + the iceberg-catalog-sql / -datafusion /
-storage-opendal companions all build against this copy.

Keep API additions backward-compatible so the companion crates keep compiling.
Local modifications are tracked in git history vs this initial 0.9.1 import.
