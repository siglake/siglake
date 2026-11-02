# WAL committed-only fallback attribution

`UnionEventsProvider::scan` can answer from the committed Iceberg snapshot
alone. It does that on three branches, each reached because one existing
bound refused. Until #7927 nothing said which bound: the global memory and
workspace counters move for reasons that do not select the committed-only
plan, and none of them is reachable from a single request.

This note records what was added, what the labels mean, what the numbers
cannot tell you, and which branches no fixture can reach.

Source: `crates/siglake-query-server/src/wal_buffer.rs`, baseline
`471ae0c`.

## What was added

One counter family and one tracing event, emitted together at each of the
three `scan` branches that return the committed base plan:

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
- An empty WAL directory, and a filter that matches no buffered rows.
  Those are fast paths; `siglake_query_wal_buffer_skipped_no_match_total`
  already names the second one.

## Reading it

The named reader is the logs-only shadow pilot's query-freshness check: a
scoped delta on `siglake_query_wal_committed_fallback_total` answers "did
anything serve committed-only during this window, and which bound caused
it", which the existing counters cannot. The pilot's follow-up step is the
tracing event, filtered to `outcome=committed_only`.

Attribution limits, which the pilot has to respect:

- **The counter is a delta, never an absolute.** An absolute value is a
  process total over an unknown window.
- **A delta does not attribute a decision to one query.** Under concurrency
  the increment could be any in-flight request's. The tracing event's parent
  span is the only thing that attributes it, and it does so because the
  emission is in `scan` after the load await — on the request's task, not
  on the `spawn_blocking` thread, which carries no request context.
- **Increments are decisions, not queries.** One query may scan several
  tables, and each scan can decide independently.

## Validation

Frozen test-to-case mapping. Every case drives the real
`UnionEventsProvider::scan` over one immutable 32-row fixture (16 committed
rows in a `MemTable` base, 16 WAL rows in one sealed segment, fixture bytes
≤ 1 MiB) and reads only the decisions that scan emitted.

| Case | Test | Result |
|---|---|---|
| 1 load/compressed_limit | `case_01_load_compressed_limit_selects_committed_only` | pass |
| 2 load/memory_reservation | `case_02_load_memory_reservation_selects_committed_only` | pass |
| 3 load/actual_payload_bound | — | **not exercisable** |
| 4 load/transform_unbounded | `case_04_load_transform_unbounded_selects_committed_only` | pass |
| 5 filter/workspace_unbounded | `case_05_filter_workspace_unbounded_selects_committed_only` | pass |
| 6 filter/memory_reservation | `case_06_filter_memory_reservation_selects_committed_only` | pass |
| 7 sort/memory_reservation | `case_07_sort_memory_reservation_selects_committed_only` | pass |
| 8 sort/actual_payload_bound | — | **not exercisable** |
| 9 sort/ownership_attach | — | **not exercisable** |
| 10 healthy union | `case_10_healthy_visible_union_records_no_decision` | pass |
| 11 empty WAL | `case_11_empty_wal_records_no_decision` | pass |
| 12 filtered zero match | `case_12_filtered_zero_match_records_no_decision` | pass |
| 13 filter evaluation error | `case_13_filter_evaluation_error_unions_unfiltered_without_a_decision` | pass |
| 14 unsupported charged sort | `case_14_unsupported_charged_sort_unions_unsorted_without_a_decision` | pass |
| 15 cancellation | `case_15_cancelled_scan_releases_its_charge_without_a_decision` | pass |
| 16 listing failure | `case_16_listing_failure_is_a_query_error_without_a_decision` | pass (skips as root) |
| 17 decode failure | `case_17_decode_failure_is_a_query_error_without_a_decision` | pass |
| 18 load ownership-attach failure | — | **not exercisable** |
| 19 filter actual-bound error | `case_19_filter_actual_bound_unions_unfiltered_without_a_decision` | pass, **error sub-path not exercisable** |
| 20 filter ownership-attach error | — | **not exercisable** |
| 21 span attribution | `case_21_decision_event_stays_under_the_supplied_parent_span` | pass |
| 22 finite label inventory, scoped delta | `case_22_counter_is_a_scoped_delta_over_the_finite_inventory` | pass |
| 23 lease clone/drop | `case_23_lease_clone_holds_the_charge_until_the_last_owner_drops` | pass |
| 24 one truthful unbounded reason | `case_24_unbounded_workspace_has_one_truthful_reason` | pass, **arithmetic half not constructible** |

Case 22's label-inventory half is enforced for every case at once: the
shared `decisions_in` reader panics on a label key other than `stage` or
`reason`, and on any `(stage, reason)` outside the table above.

Case 21 installs its subscriber globally behind a `OnceLock` and keys on a
unique parent-span name. A scoped subscriber passes single-threaded and
fails under `cargo test`'s default parallelism: the other cases emit the
same callsite from other threads, and the interest and max-level state
`tracing` keys off is process-global, not per-thread.

Case 16 chmods the sealed directory to `0o000`. A process that can still
read it (root) prints `case 16 NOT EXERCISED` and returns rather than
reporting a pass it did not earn.

### Coverage limitations

Five positions cannot be reached from any fixture without adding
fault-injection seams to the production path. Each is a property of the
existing bounds, not of the fixture:

- **`load`/`actual_payload_bound`** needs
  `decoded_payload_bytes(raw) > physical + 2 × uncompressed_ipc`. The decode
  charge is deliberately twice the IPC body, and decoded Arrow buffers are
  the IPC body. The existing test at `wal_buffer.rs:3386` already pins the
  margin from the other side.
- **`sort`/`actual_payload_bound`** needs a sorted payload over
  `2 × input + 8 × rows`. `concat_batches` then `take_record_batch`
  produces about one input payload.
- **`sort`/`ownership_attach`**, and cases 18 and 20, need
  `attach_reservation_to_array_data` to fail. Its only failure is
  `ArrayData::builder(..).build()` rejecting data that came out of a
  validated `ArrayData`.
- **Case 19's error sub-path** needs a filtered subset larger than
  `filter_workspace_bound`, which starts from the full input payload
  (`wal_buffer.rs:2093`). The case instead pins the ordinary filtered union
  and holds `siglake_query_wal_buffer_refused_actual_bytes_total` at zero,
  so the two outcomes stay distinguishable.
- **Case 24's arithmetic half** needs about 2^48 nested `attr_get` calls or
  2^63 bytes of literal key to overflow the checked arithmetic. The case
  instead pins that the single `else` arm maps every unbounded workspace to
  `WorkspaceUnbounded` and never to `MemoryReservation`.

Adding `#[cfg(test)]` fault-injection seams for the first three would cost a
branch in three hot paths' source and is a source-shape decision, not a test
decision. It is left for review.

### Commands

```
cargo test -p siglake-query-server --lib wal_buffer::buffer_prefilter_tests::case_
cargo fmt --all -- --check
cargo clippy -p siglake-query-server --all-targets -- -D warnings
```

19 cases, 0 failed. Focused execution 0.03 s against the 180 s cap; the
test binary's compile was 121.8 s, measured separately. The full crate lib
suite is 500 passed, 8 ignored, 7.94 s.
