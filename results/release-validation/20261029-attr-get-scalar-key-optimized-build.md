# `attr_get` scalar-key optimized build — 2026-10-29

The compilation source was commit
`03ffa7f901ecaf438941d1405460232364114cd4`, tree
`568f866d5a6a8cd041a3ae76da4764b107e88a7d`. The worktree was clean before
the build. Rust 1.95.0 (`59807616e1fa2540724bfbac14d7976d7e4a3860`, LLVM
22.1.2) and Cargo 1.95.0 used the checked-in toolchain and `Cargo.lock`.

The library test target was compiled once with the repository release profile:
thin LTO, one codegen unit, and debug level 1. No optimization flags or target
directory overrides were added. Cargo exited 0 and emitted one test executable
for `siglake_query_server`; its `build-finished` record reported success.

```text
cargo test --release -p siglake-query-server --lib --no-run --message-format=json # 0
```

Compiler-reported artifact identity:

```text
label: release/deps/siglake_query_server-209b3cb0caca731e
size: 743152776 bytes
SHA-256: 39f6c65f88d894eb269001648c15fe264a9dd42a5b7f4195069f3936fc6d5930
ELF build ID: 9654ab42c09850b826a7b523b905f2ee6a0b3523
```

The source hashes recorded before and after compilation were identical:

```text
Cargo.lock: 801330c0f5d1083ece13b2682842f989880cb578483d3c5c8503592b03d0c4ac
crates/siglake-query-server/src/udfs.rs: 7b7091b443a640fc062009eea34d2a3910519b8e33b355951a2f0f1aef0fa9bf
crates/siglake-query-server/src/testdata/attr_get_semantic_cases.json: 13cee743c5e74202dfedb8cb6323c093848ffcbf0263d91c730c91ce54aa314e
```

This report-only change follows the build and is not the built source. The
manager commit that records it changes no production code, candidate test code,
or semantic gold. The optimized executable remains in the shared target for
the separately owned bounded semantic replay.

This worker did not execute the test binary or the ignored microbenchmark. The
build supplies no timing or allocation result and supports no native, HTTP,
image, cloud, API, or production-speedup conclusion.
