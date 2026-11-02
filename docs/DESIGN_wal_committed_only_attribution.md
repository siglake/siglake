# WAL committed-only fallback attribution

`UnionEventsProvider::scan` has three resource-refusal branches that answer
from the committed Iceberg snapshot alone. Until #7927 nothing said which
bound refused: the global memory and workspace counters move for reasons that
do not select the committed-only plan and cannot safely attribute one request
under concurrency. Empty WAL and zero matching WAL rows also return the base
plan, but they are ordinary fast paths rather than resource fallbacks.

This note records what was added, what the labels mean, what the numbers
cannot tell you, and which branches the bounded valid fixture did not exercise.

Source: `crates/siglake-query-server/src/wal_buffer.rs`, baseline
`471ae0c`.

## What was added

One counter family and one tracing event, emitted together at each of the
three existing resource-refusal `scan` branches that return the committed
base plan:

```
siglake_query_wal_committed_fallback_total{stage, reason}
```

```
INFO outcome=committed_only stage=<stage> reason=<reason>
     wal-buffer scan served the committed-only base plan
```

A private `FallbackReason` is carried from the decision site to the branch
that acts on it: `BufferReadOutcome::Refused` gained a `reason` field,
`filter_batches_charged` returns `Result<DFResult<_>, FallbackReason>`
instead of `Option<_>`, and `ChargedSort::Refused` carries one. Nothing
else changed: the same plans are returned, the same errors propagate, the
same reservations are taken and released, and cancellation behaves as
before.

`FallbackStage` and `FallbackReason` are closed enums whose `as_str` is the
only source of label values. A tenant, index, path, SQL string or query id
cannot reach the metric, because there is no constructor that accepts one.

## Label inventory

Nine pairs, and no others.

| stage | reason | Producer |
|---|---|---|
| load | compressed_limit | Cached or selected generation over the compressed budget (`wal_buffer.rs:1250`, `:1277`, `:1311`) |
| load | memory_reservation | Decode or expansion reservation refused (`:1317`, `:1348`) |
| load | actual_payload_bound | Decoded or transformed payload over its admitted bound (`:1333`, `:1364`) |
| load | transform_unbounded | `transformation_expansion_bound` returned `None` (`:1342`) |
| filter | workspace_unbounded | `filter_workspace_bound` returned `None` (`:2040`) |
| filter | memory_reservation | Filter workspace reservation refused (`:2044`) |
| sort | memory_reservation | Charged-sort reservation refused (`:2320`) |
| sort | actual_payload_bound | Sorted payload over its admitted bound (`:2326`) |
| sort | ownership_attach | Sorted-array reservation attachment failed (`:2332`) |

`filter`/`workspace_unbounded` covers both an unsupported expression and an
arithmetic-unrepresentable bound. That is one `else` arm in the source and
one reason here, because the statement "the workspace is unbounded" is true
of both and the answer contract is identical. It is not an out-of-memory
condition and is not reported as one.

### What is deliberately NOT labelled

- A listing, sizing or decode failure. Those stay query errors; answering
  them committed-only would turn corruption into silent row loss.
- `Some(Err(..))` from the filter pre-pass. The scan unions the buffer
  unfiltered, so the WAL rows are still in the answer.
- `ChargedSort::Unsupported`. The union is retained, only unsorted.
- An empty WAL directory, and a filter that matches no buffered rows. Both
  return the committed base plan, but they are fast paths rather than
  resource refusals; `siglake_query_wal_buffer_skipped_no_match_total`
  already names the second one.

## Reading it

The named reader is the logs-only shadow pilot's query-freshness check. The
metric is a cumulative counter; the pilot reads a scoped before/after delta to
answer "did a resource refusal serve committed-only during this window, and
which bound caused it?" The pilot's per-request attribution mechanism is the
tracing event, filtered to `outcome=committed_only`.

Attribution limits, which the pilot has to respect:

- **The counter is cumulative; the pilot reads a delta, never an absolute.**
  An absolute value is a process total over an unknown window.
- **A delta does not attribute a decision to one query.** Under concurrency
  the increment could be any in-flight request's. The tracing event's parent
  span is the only thing that attributes it, and it does so because the
  emission is in `scan` after the load await — on the request's task, not
  on the `spawn_blocking` thread, which carries no request context.
- **Increments are decisions, not queries.** One query may scan several
  tables, and each scan can decide independently.

Case 21 proves source-level association with an explicitly supplied parent
span across the actual async scan boundary. Native HTTP `traceparent`
extraction and export of that event through OpenTelemetry have not been
qualified by this work.

## Validation

The authoritative inventory remains exactly 24 semantic cases. Nineteen named
tests ran. The scan cases use one immutable 32-row fixture shape (16 committed
rows in a `MemTable` base and 16 WAL rows in one sealed segment, fixture bytes
≤ 1 MiB). Six of the nine stage/reason pairs were exercised through the
real `UnionEventsProvider::scan`; omitted subpaths are named below. Case 24 is
a helper-level check and does not drive `scan`.

| Case | Test | Result |
|---|---|---|
| 1 load/compressed_limit | `case_01_load_compressed_limit_selects_committed_only` | pass |
| 2 load/memory_reservation | `case_02_load_memory_reservation_selects_committed_only` | pass |
| 3 load/actual_payload_bound | — | **not exercised** |
| 4 load/transform_unbounded | `case_04_load_transform_unbounded_selects_committed_only` | pass |
| 5 filter/workspace_unbounded | `case_05_filter_workspace_unbounded_selects_committed_only` | pass |
| 6 filter/memory_reservation | `case_06_filter_memory_reservation_selects_committed_only` | pass |
| 7 sort/memory_reservation | `case_07_sort_memory_reservation_selects_committed_only` | pass |
| 8 sort/actual_payload_bound | — | **not exercised** |
| 9 sort/ownership_attach | — | **not exercised** |
| 10 healthy union | `case_10_healthy_visible_union_records_no_decision` | pass |
| 11 empty WAL | `case_11_empty_wal_records_no_decision` | pass |
| 12 filtered zero match | `case_12_filtered_zero_match_records_no_decision` | pass |
| 13 filter evaluation error | `case_13_filter_evaluation_error_unions_unfiltered_without_a_decision` | pass |
| 14 unsupported charged sort | `case_14_unsupported_charged_sort_unions_unsorted_without_a_decision` | pass |
| 15 cancellation | `case_15_cancelled_scan_releases_its_charge_without_a_decision` | pass |
| 16 listing failure | `case_16_listing_failure_is_a_query_error_without_a_decision` | pass in retained non-root run; may return unexercised under root |
| 17 decode failure | `case_17_decode_failure_is_a_query_error_without_a_decision` | pass |
| 18 load ownership-attach failure | — | **not exercised** |
| 19 filter actual-bound error | `case_19_filter_actual_bound_unions_unfiltered_without_a_decision` | named test passed an ordinary filtered union; **actual-bound error not exercised** |
| 20 filter ownership-attach error | — | **not exercised** |
| 21 span attribution | `case_21_decision_event_stays_under_the_supplied_parent_span` | pass at source level; native HTTP/OTLP capture not run |
| 22 finite label inventory, scoped delta | `case_22_counter_is_a_scoped_delta_over_the_finite_inventory` | pass |
| 23 lease clone/drop | `case_23_lease_clone_holds_the_charge_until_the_last_owner_drops` | pass |
| 24 one truthful unbounded reason | `case_24_unbounded_workspace_has_one_truthful_reason` | helper-level unsupported-workspace check passed; **arithmetic overflow and real scan not exercised** |

Case 22's label-inventory half is enforced for every recorded scan decision:
the shared `decisions_in` reader panics on a label name other than `stage` or
`reason`, and on any observed `(stage, reason)` outside the table above.

Case 21 installs its subscriber globally behind a `OnceLock` and keys on a
unique parent-span name. A scoped subscriber passes single-threaded and
fails under `cargo test`'s default parallelism: the other cases emit the
same callsite from other threads, and the interest and max-level state
`tracing` keys off is process-global, not per-thread.

Case 16 chmods the sealed directory to `0o000`. A process that can still read
it (root) prints `case 16 NOT EXERCISED` and returns; the Rust test runner can
still mark that early return `ok`. The retained non-root `--nocapture` run
contains no such banner and exercised the query-error path.

### Coverage limitations

Five requested positions were not exercised by the bounded valid fixture:
cases 3, 8, 9, 18 and 20. The current bounds and ownership structure motivate
the coverage limit described below, but the review did not prove these paths
universally unreachable. Their production sites received source review rather
than observed execution.

- **`load`/`actual_payload_bound`** would require
  `decoded_payload_bytes(raw) > physical + 2 × uncompressed_ipc`. The decode
  charge is deliberately twice the IPC body, and decoded Arrow buffers are
  the IPC body. The existing test at `wal_buffer.rs:3386` already pins the
  margin from the other side.
- **`sort`/`actual_payload_bound`** would require a sorted payload over
  `2 × input + 8 × rows`. `concat_batches` then `take_record_batch`
  produces about one input payload.
- **`sort`/`ownership_attach`**, and cases 18 and 20, would require
  `attach_reservation_to_array_data` to fail. Its only failure is
  `ArrayData::builder(..).build()` rejecting data that came out of a
  validated `ArrayData`.
- **Case 19's error sub-path** was not exercised. It would require a filtered
  subset larger than `filter_workspace_bound`, which starts from the full
  input payload
  (`wal_buffer.rs:2093`). The case instead pins the ordinary filtered union
  and holds `siglake_query_wal_buffer_refused_actual_bytes_total` at zero,
  so the two outcomes stay distinguishable.
- **Case 24's arithmetic half** was not exercised. Reaching checked-arithmetic
  overflow appears to require about 2^48 nested `attr_get` calls or 2^63 bytes
  of literal key. The named test instead checks at helper level that an
  unsupported workspace maps to `WorkspaceUnbounded`, never
  `MemoryReservation`; case 5 covers that reason through a real scan.

The coordinator's scoped disposition permits review of the additive production
mapping with these rare paths explicitly unexercised. No new test-only hot-path
seams or user decision are required for this correction. Automatically filed
task #7992 tracks the separate coverage disposition; it is not a prerequisite,
runtime admission or request for Todd to respond.

### Commands

```
cargo test -p siglake-query-server --lib wal_buffer::buffer_prefilter_tests::case_ -- --nocapture
cargo test -p siglake-query-server --lib
cargo fmt --all -- --check
cargo clippy -p siglake-query-server --all-targets -- -D warnings
```

An earlier focused attempt completed in 0.04 s with 15 passed and 4 failed
(cases 16, 19, 21 and 23); that failed output remains in
`$TMPDIR/7927/focused-1.log`. After the test corrections, the retained final
`--nocapture` run compiled incrementally in 9.39 s and completed in 0.04 s with
19 passed and 0 failed, within the 180 s focused cap. The initial test-binary
compile was 121.8 s, measured separately. Final `cargo fmt --all -- --check`
and `cargo clippy -p siglake-query-server --all-targets -- -D warnings` exited
0; clippy completed in 11.19 s. The full crate lib suite was 500 passed,
8 ignored, 7.94 s. The pre-correction manager gate `scripts/ci-local.sh`
passed in 1179 s; it was not rerun for this documentation/comment correction.
Raw final focused, format and clippy evidence remains in
`$TMPDIR/7927/evidence.log`.
