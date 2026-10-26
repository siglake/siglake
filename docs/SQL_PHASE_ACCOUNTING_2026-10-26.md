# Where a `/api/v1/sql` records request spends its time (#7456)

#7446 left 1.3–1.7 ms per request unnamed on the two clipped shapes that failed
their 10 ms ceiling, and read it as time spent after the handler's own
`total_ms`. That reading was wrong in both directions. The gap is two separate
terms with different owners, and the larger one is inside `total_ms`, in
handler phases that had no timer.

Measured locally at this branch; no run was made and none is authorised by this
document. Numbers are a release build, 6 files × 3,000 rows, one pod, result
cache cold per request (`cargo test --release -p siglake-query-server --test
sql_phase_accounting -- --nocapture`). The AWS magnitudes are not reproduced
here — the table is four orders of magnitude smaller — but the boundaries are
the same code.

## The four clocks, and what sits between them

```
client wall
└── x-siglake-server-micros          (lib.rs:783, stamp_server_micros)
    ├── request body receipt, Json<SqlRequest> extractor, auth, in-flight guard
    ├── total_ms                      (sql.rs: RequestClock::new → the terminal line)
    │   └── resolve, rewrite, buffer delta, cache probe, session, register,
    │       logical, estimate, battery, admission, probe, execute
    │           └── physical plan, collect, plan stats, render, cache fill,
    │               plan text, serialize
    └── terminal audit, request metrics, response unwinding through the layers
```

Two boundary facts the earlier reading had backwards:

1. **Response serialization is inside `total_ms`.** `Json(..).into_response()`
   encodes the body eagerly, and on the records path it runs at
   `crates/siglake-query-server/src/sql.rs:11738`, before the handler logs
   `total_ms`. It cannot be the term that sits above it.
2. **`server_micros − total_ms` is not all server compute.** The header's clock
   starts when the request HEAD has been parsed; the handler's starts after the
   `Json` extractor has read the BODY off the socket. A client that writes its
   head and its body separately charges the interval between them to the header
   and to nothing else.
   `crates/siglake-query-server/tests/sql_server_micros_boundary.rs` drives the
   server over a real loopback socket with a 250 ms split and measures
   `server_micros − total_ms = 250.1 ms` against `total_ms = 15.2 ms`.

## What the handler now names

Every interval between the request clock's start and the terminal line belongs
to exactly one field of `sql query profile` or, for `execute_ms`, to one field
of `sql execution profile`. The terminal line publishes `unattributed_ms` —
total minus the sum of its phases — so a reader does not have to add twelve
numbers to find out whether they add up.

New on `sql query profile`: `rewrite_ms`, `buffer_delta_ms`, `cache_probe_ms`,
`session_ms`, `battery_ms`, `admission_ms`, `probe_ms`, `execute_ms`,
`unattributed_ms`. New on `sql execution profile`: `plan_stats_ms`,
`cache_fill_ms`, `plan_text_ms`, `serialize_ms`.

`resolve_ms` changes meaning on `/api/v1/sql`. That endpoint resolves the
tenant context and rewrites the query in `handle_scoped` and hands both to the
local path, whose own resolve and rewrite are then no-ops: the prelude was
inside `total_ms` and reported as nothing. `RequestClock` now carries it
(`PreludePhases`), so `resolve_ms` and `rewrite_ms` are about the request
rather than about which function did the work.

Measured, warm (ms), one request's own numbers — not medians of separate
aggregations:

| phase | | phase | |
| --- | --- | --- | --- |
| resolve | 0.002 | battery | 0.008 |
| rewrite | 0.118 | admission | 0.001 |
| buffer delta | 0.000 | probe | 0.001 |
| cache probe | 0.067 | **execute** | **2.771** |
| session | 0.088 | — physical plan | 0.895 |
| register | 0.038 | — collect | 1.653 |
| logical plan | 0.096 | — plan stats | 0.058 |
| estimate | 0.006 | — render | 0.042 |
| | | — cache fill | 0.004 |
| | | — plan text | 0.003 |
| | | — serialize | 0.018 |
| **accounted** | **3.196** | execution phases | 2.671 |
| unattributed | 0.003 | | |
| **total_ms** | **3.199** | | |
| server header | 3.233 | | |
| client wall | 3.244 | | |

`unattributed_ms` is 3 µs on a warm request and 9 µs on the cold first one.
The ~0.10 ms between `execute_ms` and the sum of the execution phases is the
timeout wrapper, the task context and the handoff around `render_records`.

## Reading the round's numbers again

From `docs/CLIPPED_LIMIT_RESIDUAL_ATTRIBUTION_2026-10-26.md`, candidate phase:

| | keyword | substring |
| --- | --- | --- |
| harness-visible phases | 16.27 | 9.21 |
| + register, logical, estimate | 17.12 | 10.05 |
| `total_ms` | 17.99 | 10.50 |
| unnamed INSIDE `total_ms` | **0.87** | **0.45** |
| server p50 (client's header) | 18.85 | 11.40 |
| outside `total_ms` | **0.86** | **0.90** |

The first residual is the one this change names: rewrite, result-cache probe,
session build, plan-stats summarize, cache fill, plan text and serialization,
plus the execute wrapper. Locally those sum to ~0.45 ms on a 6-file plan, and
they scale with the plan (`summarize_plan_runtime` walks every node's metric
set) and with the SQL (the rewrite and `referenced_tables` parse it), so a
15-partition `match_terms` plan landing at 0.87 ms is the same shape.

The second residual is the one the socket test reclassifies: request-body
receipt plus the extractor, auth, the terminal audit, the request metrics and
the response's walk back out through four layers. It is not available to a
scan-side repair, and on a per-request basis it is not even all server work.

Neither term was measured on the round's own requests — these are local
figures against the round's medians, and medians of separately aggregated
columns do not locate a single request's time. What the round's captures
support is the split itself: `total_ms` covers more than the harness's four
phases, and the header covers more than `total_ms`.

## `register_ms`

#7456 asked why registering one already-registered table costs 0.6 ms. It is
not the registration. Sub-timers inside the block, debug build, where
`register_ms` totals 0.20 ms:

| | ms |
| --- | --- |
| `referenced_tables(query)` — parses the SQL to collect table names | 0.117–0.136 |
| provider registration (`cached_table_entry` + `register_table`) | 0.049–0.081 |
| `register_udfs` — seven scalar UDFs into the fresh session | 0.007–0.012 |

The provider comes from the snapshot-keyed table cache and clones only `Arc`s
(`Table` holds `TableMetadataRef`), which is why it is the small term. The
large one is a third SQL parse of the same query — after the serving rewrite
and before logical planning. Each parse is independent of the others and none
of them keeps its AST.

## Not established

- Any AWS latency. The per-request accounting is local; the round's figures
  above are medians re-read, not re-measured.
- That the 10 ms ceilings are reachable. A named 3.7 ms of non-scan time is
  still 3.7 ms. What changes is that most of it now has an owner: the largest
  single piece of it is `physical_plan` + `estimate` + `logical`, which is
  planning, and planning is not what the clipped coalescer work was aimed at.
- Whether the response body should carry the closing accounting. The harness
  reads `stats.phases` (plan, buffer delta, collect, render), which is the
  under-accounting that produced the original 2.58 / 2.19 ms "unattributed"
  rows. Closing it in the response is an API-visible change and is left
  unmade here.
