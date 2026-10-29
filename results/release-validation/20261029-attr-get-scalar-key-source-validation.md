# `attr_get` scalar-key source validation — 2026-10-29

This source-only validation used base commit
`15a4804e67ac5fd643224500a7d2e5087ad42435`, Rust 1.95.0
(`59807616e1fa2540724bfbac14d7976d7e4a3860`, LLVM 22.1.2), and Cargo
1.95.0. The prepared patch SHA-256 matched
`63112a70785bce2ff6f529cfdcc6228c4700d776b39c87d5ba64b1d8119c08d3`.

The candidate exists only in the existing `cfg(test)` module. It handles a
Utf8 scalar key without expanding the key into an Arrow array, while calling
the production `json_attr_lookup` and using `StringBuilder`. Array keys and
non-Utf8 scalar keys call the actual `AttrGetUdf::invoke_with_args`. The diff
does not modify production code.

All 19 prepared semantic cases retained their original attributes, key, and
expected value. They passed against the real UDF and the candidate at batch
sizes 0, 1, 128, and 4096 with literal, null, and dynamic keys. The controls
also passed scalar attributes, null rows and dynamic keys, and the production
errors for non-Utf8 attributes and keys. No prepared gold required correction.

The ignored benchmark covers narrow/early, wide/early, wide/late,
wide/missing, and literal-dotted collision shapes. Each timed sample contains
at least 32 UDF invocations and at least 16,384 rows, alternates arm order, and
records ten pairs after two warmups with the existing thread-local allocation
counter. Output equality is checked before timing. The production UDF object
and owned call arguments are constructed outside each timed region.

Commands and exits:

The managed run set `PREPARATION_DIR` to the task's immutable input directory;
its machine-local value is excluded from this public report.

```text
sha256sum "$PREPARATION_DIR"/*                                             # 0
jq -S '.cases' "$PREPARATION_DIR/semantic-cases.json" > "$TMPDIR/7763-prepared-cases.json"
jq -S '.cases' crates/siglake-query-server/src/testdata/attr_get_semantic_cases.json > "$TMPDIR/7763-rebased-cases.json"
cmp "$TMPDIR/7763-prepared-cases.json" "$TMPDIR/7763-rebased-cases.json"      # 0
cargo test -p siglake-query-server attr_scalar_key_candidate -- --nocapture # 0
cargo test -p siglake-query-server                                          # 0
cargo fmt --all -- --check                                                  # 0
cargo clippy -p siglake-query-server --all-targets -- -D warnings           # 0
```

The focused run passed three tests and ignored the benchmark. The crate run
passed all normal targets; ignored tests remained ignored. The timed benchmark
was not run, so this report makes no latency, allocation-reduction, native,
API, or production-speedup claim.
