# What a `/api/v1/sql` response can say about its own time (#7471)

`RecordsResponse.stats.phases` carries four numbers — physical plan, buffer
delta, collect, render. A reader that sums them and compares the sum with what
the server reports is short by 16–21% of the handler's own total locally and by
2.58 / 2.19 ms on the two clipped shapes of the 2026-10-26 matched round
(`docs/CLIPPED_LIMIT_RESIDUAL_ATTRIBUTION_2026-10-26.md`). #7456 named every
one of the missing intervals in the query-server log
(`docs/SQL_PHASE_ACCOUNTING_2026-10-26.md`). This document settles what the
RESPONSE may carry, and proposes one shape for it. No schema change is made
here; the implementation is a follow-up.

Three questions had to be answered before any field could be chosen:

1. Where in the handler can a response body still be written, and what is left
   after that point?
2. A body can be replayed — from the result cache and from a batch job row.
   Whose time would a timing field in it describe?
3. What does the reader that actually consumes these numbers need?

## The body seals before the request ends

On the records path the body is wrapped in an `Arc` at
`crates/siglake-query-server/src/sql.rs:11779` and handed to the result cache
from there. Nothing may be written into it after that point without
deep-copying the entry the cache now shares (#2304). Three timed intervals and
the terminal line follow the seal: the cache fill, the plan-text render, and
serialization — `Json::into_response` encodes eagerly, so serialization is
inside the handler's `total_ms` but after the body is closed.

Measured on this branch, release build, 6 files x 3,000 rows, one pod, result
cache cold per request
(`cargo test --release -p siglake-query-server --test sql_phase_accounting --
--nocapture`), ms:

| req | `total_ms` | the four body phases | response-invisible | after the seal |
| --- | --- | --- | --- | --- |
| 0 (cold) | 12.379 | 7.728 | 4.651 | 0.031 |
| 1 | 3.893 | 3.080 | 0.813 | 0.031 |
| 2 | 3.775 | 3.036 | 0.739 | 0.030 |
| 3 | 3.860 | 3.155 | 0.705 | 0.022 |
| 4 | 4.128 | 3.439 | 0.689 | 0.027 |

("after the seal" is `cache_fill_ms + plan_text_ms + serialize_ms`.)

So a body field measured at the seal can account for 99.2% or more of
`total_ms` on these requests, and the remainder is already named in the log.
What it cannot do is equal `total_ms` minus serialization: the cache fill and
the plan text sit in that remainder too, and the untimed slack of the execute
wrapper straddles the seal. The card's "a `total_micros` up to the render would
be exact for everything else" does not hold.

A response HEADER has no such boundary. Headers are set on the response after
`into_response()`, so a header can carry the handler's total including
serialization, and it is stamped per response rather than stored in one.
`x-siglake-server-micros` already works this way (`src/lib.rs:783`), measuring
a wider span: it starts when the request head is parsed, which includes the
body receipt the handler's clock does not (#7456).

## A body is a stored artifact; a header is not

Two paths return a body built by an earlier request:

- **Result-cache hit** (`src/sql.rs`, the `Hit` arm of the cache probe): the
  stored `Arc` is serialized by reference. The phases come with it.
  `crates/siglake-query-server/tests/sql_phase_provenance.rs` pins this: the
  hit is proven by the cache's `outcome` counter, and the two responses' phase
  blocks compare equal.
- **Batch job result** (`src/jobs.rs:529`, `finish_succeeded`): a
  `RecordsResponse` is persisted and served later, possibly by a different
  binary after an upgrade.

So `stats.phases` is provenance of the execution that produced the body. That
is a usable thing to be — it is what a reader asking "what did this answer cost
to compute" wants — but a request-timing field placed beside it would report
the first request's time to every later one, and a round that did not disable
the result cache would grade replays as executions without saying so.

The two frames are therefore separated rather than merged:

- **execution provenance** goes in the body, is replayed verbatim, and is
  internally consistent (its parts and its total come from the same clock).
- **request timing** goes in a header, is stamped by whichever handler answered
  this request, and cannot be replayed.

## What the reader needs

The named reader is the separately maintained `siglake-benchmarks` tooling. At
revision `b708051`:

- `run_queries.py:1412-1413` archives `stats.phases` verbatim into the record,
  and `:1557` reads `x-siglake-server-micros` off the response per run.
- `plan_phase_attribution.py:551,625` reads `phases.plan_micros` and checks it
  against the log's `physical_plan_ms`, joined by `query_execution_id`; it gets
  `register`, `logical_plan` and `estimate` from the log alone. A log the round
  did not capture, or one whose ids are ambiguous, reads NOT ASSESSED.
- `degrade.sh:222` prints the four phase fields from the body without headers
  or log data.

What none of them can do today is say, from one request's artifacts, how much
of the server's time the four phases fail to name. That subtraction is what
produced the 2.58 / 2.19 ms rows, and it was done across independently
aggregated medians, which locates nothing.

## Options

**A. Carry every phase in the body.** Ten more fields (rewrite, cache probe,
session, register, logical, estimate, battery, admission, probe, plan stats) to
mirror the log. It duplicates a source the reader already parses, so the two
can disagree — `plan_phase_attribution.py` already checks one such pair and
refuses the decomposition when they differ. Every field is permanent API
surface, is replayed on a hit, is persisted in job rows, and is charged per
cached entry by `retained_heap_bytes` (`src/format.rs:533`). It still cannot
reach past the seal.

**B. One remainder in the body.** `execution_micros` at the seal plus
`other_micros`, the part of it the named phases do not cover, computed
server-side. Two fields, no duplication of the log, closed arithmetic within
the body. It says nothing about the request that received the body.

**C. B, plus the request's own total in a header.** `x-siglake-handler-micros`
stamped after serialization, and `x-siglake-result-cache` naming the outcome so
a reader can tell a replay from an execution. The header is per-request by
construction, and the reader already reads one header per run.

## Recommendation: C

Body, on `PhaseStats`, both `Option<u64>` with
`skip_serializing_if = "Option::is_none"` — absent where it was not measured,
never zero-filled (the #4290 rule: a counter the server did not report must not
arrive as the value a reader would read as a measurement):

- `execution_micros` — the executing request's handler clock at the moment this
  body was sealed. Threading it in is one parameter: `execute_with_limits`
  already holds `started` (`src/sql.rs:11477`) and `render_records`
  (`:11607`) does not.
- `other_micros` — `execution_micros` minus the phases named in the same block,
  by saturating subtraction on the server. The reader never subtracts, and the
  value is never negative.

Per path, stated rather than inferred:

| path | `execution_micros` | `other_micros` |
| --- | --- | --- |
| single-pod records | at the seal | minus plan + buffer + collect + render |
| coordinator records (`distributed`) | at the seal | minus plan + buffer + render + `max(shard_wall_micros)` + `merge_micros` |
| Tier-1 fast paths (`attach_fast_path_phases`, `:7213`) | absent | absent |
| NDJSON | no envelope exists | — |
| 413 truncated (`:11696`) | absent | absent |
| replayed body (cache hit, job result) | the filling execution's | the filling execution's |

The coordinator's rule is the one that needs saying out loud: shard walls are
concurrent, so the fan-out costs the maximum and not the sum, and
`collect_micros` is 0 there by design (`:19327`). The distributed block keeps
its current meaning.

The fast paths are left absent rather than guessed. `attach_fast_path_phases`
packs register + logical + estimate into `plan_micros` and reports
`render_micros: 0`, so a remainder computed against those fields would be a
different quantity under the same name. Giving them real values is its own
piece of work.

Headers:

- `x-siglake-handler-micros` — the handler's clock at the moment the response
  is complete, stamped after serialization, on buffered responses only. A
  streamed NDJSON body is not finished when its headers are sent, so the header
  is omitted there rather than carrying a number that means something else.
- `x-siglake-result-cache: hit|miss|insert|off` — set on the response, which
  leaves the shared body untouched. Without it, a body's phases cannot be told
  from a replay of them.

Neither header appears in the generated OpenAPI document today
(`docs/api/openapi-query.yaml` documents no response headers at all, including
the existing `x-siglake-server-micros`). The follow-up declares both in the
`#[utoipa::path]` responses or states in `docs/api/README.md` why they stay
out, and does not leave the question unasked.

What C gives up: a reader with the body and the headers alone still cannot see
WHICH phase the remainder is in. That decomposition stays in the query-server
log, where the reader already reads it. The remainder's size is what tells a
reader whether they need the log at all — 0.7 ms of 3.9 locally, 2.58 ms of
17.99 on the round.

## Compatibility

`PhaseStats` crosses no internal wire. Shard fan-out carries `ScanDetail` in
`x-siglake-scan` and a separate `ShardScanStats` in `x-siglake-scan-stats`
(`src/coordinator.rs:59`, `:118`); partials are Arrow IPC. A mixed-version
fleet during a rolling upgrade therefore never deserializes one binary's
`PhaseStats` into another's.

One cross-version deserialization does exist: batch job rows persist a
`RecordsResponse`, so a newer binary reads bodies an older one wrote. Both new
fields must be `#[serde(default)]`, which `Option<u64>` gives.

Every existing field keeps its name, type and meaning. `docs/api` is
regenerated (`cargo run -q -p siglake-openapi -- --out docs/api`) and CI diffs
it.

## How the implementation is verified

Relations between the clocks of ONE request, never an absolute duration and
never a median of one column minus a median of another:

1. `sum(named phases) <= execution_micros <= handler header <= server header <=
   client wall`, per request, in the shape of
   `tests/sql_phase_accounting.rs`'s existing chain.
2. `other_micros + sum(named phases) == execution_micros`, allowing k
   microseconds of slack where k is the number of summed fields: each `as u64`
   truncates under a microsecond, so a sum of k fields can undershoot its total
   by up to k.
3. The cache-hit control (`tests/sql_phase_provenance.rs`): a replayed body
   carries the filling execution's `execution_micros` and `other_micros`
   unchanged, and the replay is proven by the cache's `outcome` counter. The
   hit's header is asserted to EXIST and to say `hit`; nothing asserts it is
   smaller, which would be a wall-clock claim.
4. A two-shard coordinator control: `other_micros` is present and
   non-negative, `collect_micros` is still 0, and the `distributed` block is
   byte-identical to what it is today.
5. A fast-path response and an NDJSON response: the new fields are ABSENT, not
   zero.

## Not established

- Any AWS number. Everything measured here is local, on a table four orders of
  magnitude smaller than the round's. The round's figures are re-read from
  retained artifacts.
- That the remainder is worth reducing. This settles how to see it. On the
  round, most of the named non-scan time is planning, which no clipped-coalescer
  work touches (#7456).
- Whether `x-siglake-server-micros` should be corrected or retired. It measures
  a span that includes the client's request-side transport, which the reader
  reads as engine time (#7456). A handler-total header gives that reader
  something better, but the existing header's fate is not decided here.
