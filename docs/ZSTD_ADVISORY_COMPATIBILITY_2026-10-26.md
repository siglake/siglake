# Zstd advisory compatibility, 2026-10-26

This investigation covers RUSTSEC-2026-0391, RUSTSEC-2026-0398,
RUSTSEC-2026-0403 and RUSTSEC-2026-0410 against the workspace lockfile. It
does not qualify an Arrow, DataFusion or Iceberg rebase.

## Result

The smallest compatible repair is a lockfile-only update from
`zstd-sys 2.0.16+zstd.1.5.7` to `2.1.2+zstd.1.5.7`. `zstd-safe 7.2.4`
requires `zstd-sys ^2.0.15`, so the patched binding is accepted without an
API-layer or parent upgrade. The bundled C library remains at 1.5.7. With the
new lockfile, cargo-deny no longer detects RUSTSEC-2026-0410 and its exception
has been removed.

The other three exceptions remain. No compatible lockfile selection crosses
the required major-version boundaries: `zstd 0.13.3` requires `zstd-safe
^7.1.0`, while the patched minima are `zstd 0.14.3` and `zstd-safe 8.2.0`.

## Resolved constraints and features

The active normal dependency graph has these direct parents. Requirements are
from `cargo metadata --locked`; optional dependencies which are not activated
are omitted.

| Parent | Requirement | Default features |
| --- | --- | --- |
| `apache-avro 0.21.0` | `zstd ^0.13.3` | off |
| `arrow-ipc 58.4.0` | `zstd ^0.13.0` | off |
| `compression-codecs 0.4.38` | `zstd ^0.13.1`, `zstd-safe ^7` | off |
| `datafusion 53.1.0` | `zstd ^0.13` | off |
| `datafusion-datasource 53.1.0` | `zstd ^0.13` | off |
| owned `iceberg 0.10.1` fork | `zstd ^0.13.3` | off |
| `parquet 58.4.0` | `zstd ^0.13` | off |
| `siglake-bloom`, `siglake-index`, `siglake-storage`, `siglake-wal` | workspace `zstd ^0.13` | off |

The selected chain is `zstd 0.13.3 -> zstd-safe 7.2.4 -> zstd-sys
2.1.2+zstd.1.5.7`. `cargo tree -e features` reports only `std` on each member
of that chain. In particular, neither `seekable` nor `legacy` is enabled.

## Affected call paths

- RUSTSEC-2026-0391 affects only
  `zstd::stream::raw::{Encoder,Decoder}::with_context`. Siglake uses bulk
  compression, `encode_all`, `decode_all` and fresh read decoders. Arrow IPC
  and Parquet use the bulk API. `compression-codecs` uses raw encoders and
  decoders, but constructs them with `new` or `with_dictionary` and resets a
  decoder with `reinit`; it does not call `with_context`. Searches of the
  workspace, the owned Iceberg fork and the locked parent sources found no
  affected constructor call.
- RUSTSEC-2026-0398 affects `zstd_safe::seekable` decompression. The resolved
  graph has no `seekable` feature and no seekable call site.
- RUSTSEC-2026-0403 affects `DCtx::decompress_stream` only while decoding a
  corrupt legacy-format frame. The stream operation is reachable through
  `compression-codecs`, but `zstd-sys` is built without `legacy`, so the
  affected legacy decoder is absent. Siglake's own stream reads use fresh read
  decoders, which the advisory also identifies as unaffected.
- RUSTSEC-2026-0410 affects the wasm32 allocator shim before `zstd-sys 2.1.2`.
  The lockfile now selects the patched minimum independently of target.

## Parent release boundary

The newest crates.io parents checked on 2026-10-26 do not provide a complete
upgrade path. `arrow-ipc 60.0.0` and `parquet 60.0.0` accept zstd 0.14, and
`compression-codecs 0.4.46` accepts zstd-safe 8. `datafusion 55.2.0` still
requires zstd 0.13, and `apache-avro 0.22.0` still requires zstd 0.13.3. The
workspace and owned Iceberg constraint can move once those two remaining
parents publish compatible releases. Updating the available parents now would
still retain the vulnerable 0.13/7 chain while taking the wider stack-rebase
risk.

Manager task #7492 owns the upstream release recheck. Its gate is exact:
recheck released DataFusion and Apache Avro manifests for `zstd >=0.14.3`,
then update the mutually pinned Arrow,
DataFusion and Iceberg stack, require `zstd >=0.14.3`, confirm `zstd-safe
>=8.2.0`, and remove the remaining three exceptions only after cargo-deny sees
none of them.

## Validation

The repair is checked by:

```text
cargo tree -p zstd-sys -e features
cargo test -p siglake-wal
cargo test -p siglake-bloom
cargo test -p siglake-index
cargo test -p siglake-storage
cargo fmt --all -- --check
cargo clippy -p siglake-wal -p siglake-bloom -p siglake-index -p siglake-storage --all-targets -- -D warnings
cargo deny --offline --locked check advisories
```

The cargo-deny command needs a writable copy of the advisory database in this
managed lane because the shared cargo home is read-only. Against database
revision `a78282a`, it reports the three documented exceptions and no
RUSTSEC-2026-0410 match.
