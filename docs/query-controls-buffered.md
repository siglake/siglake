# Query controls: injection-only buffered/body Slices 2A–2B

This is the historical 2A–2B slice boundary. Later accepted jobs/Jaeger work
supersedes its corresponding temporary guards; ordinary direct shard coverage
and the remaining coordinator refusal are documented in
[query-controls-shard-arrow.md](query-controls-shard-arrow.md).

**Not production enablement or complete enforcement.** There is no env/CLI/main
activation, customer UI, effective endpoint or successful applied/enforced response
metadata. `AppState::new` has no authority and preserves legacy behavior, including
optional timeout-breaker behavior and existing cache keys. No source numeric default,
policy/history/job schema, role grant, authorization scope or shard wire changed.

## Trusted local injection

An already explicitly connected dedicated `PolicyReader` can be injected with
`AppState::with_query_controls(reader, operator_generation)` for local/test servers.
Configure operator limits **before** injection: the authority captures immutable
interactive and batch timeout settings and a positive trusted numeric generation.
Both defaults/ceilings must be positive, fit signed-64 milliseconds and monotonic
instant addition, and default must not exceed ceiling. Sub-millisecond bounds are
rejected. No production values were inspected or new product defaults selected.
Later changes to other AppState limits do not replace the captured timeout authority;
rebuild the authority explicitly to change its operator timeout configuration.

After authentication and tenant authorization, middleware captures one original
monotonic ingress instant. SQL, local, explain and explicit-distributed wrappers
resolve before history/catalog/rewrite/classification/cache work. Keys use only the
verified provider principal's exact issuer/client and the verified tenant. Request
JSON/headers, sub/audience, history receipts and effective telemetry cannot select
policy identity. Metadata constructors are not authentication proof. A configured
handler without the authenticated ingress scope refuses; it cannot mint a fresh
clock from manually supplied identity metadata.

Every permitted admission, including a cache hit/follower, makes a fresh primary
read. Successful absence inherits; a retained all-NULL row inherits with its known
revision; any override (including warning-only) is applied with known revision.
Generic/static/open callers have no policy tuple/revision: each performs fresh
bounded configured-primary preflight (role/schema/primary/closed-state checkout),
without policy enumeration or invented identity, then inherits operator controls.
Neither successful absence nor healthy preflight is cached. Closed/unsafe/unavailable
configured authority is fixed `503 query_policy_unavailable`, never absent authority.
An already expired original clock instead returns fixed `504 query deadline exceeded`.
The completely absent-authority path does not acquire/preflight any policy connection.

Timeout is `min(operator ceiling, policy ceiling or infinity, caller timeout or
operator default)`, for the selected priority. Caller zero is immediate expiry;
`circuit_breakers:false` cannot disable the configured clock. The immutable snapshot
records that ignored timeout opt-out internally; no public warning/controls transport
is emitted while coverage is partial. Existing optional scan-row breakers, row/byte
semantics and estimate-based operator rejection/reservation are unchanged. Scan
warning is configuration only, never a hard admission cap, actual-byte budget or tariff.

The initial safe operator/caller budget includes primary lookup. Successful lookup
can only tighten from the original anchor, not renew time. A task-scoped snapshot
survives observation wrappers, classification and local/scan_local delegation.
The outer absolute deadline includes history begin/finish waits, tenant/catalog,
rewrites, registration/planning/estimate/admission, cache hit/follower, collection
and buffered rendering. Selected-result checks precede history completion; history
success is never admission authority. No post-INSERT history ownership await was added.
Pre-poll and post-completion checks reject late-ready/empty/cache successes as well
as await-driven expiry. The result-cache discriminator adds fixed/numeric trusted
schema/state/revision/generation/effective timeout and row/scan/byte/breaker settings,
preserving tenant/snapshot/schema/exactness/shard isolation. Cached payloads do not
contain controls or history correlation headers.

## Temporary fail-closed coverage guards

Only when authority is configured:

- Actual durable batch submission refuses before INSERT (direct local selection
  also precedes catalog/admission). Local NDJSON, selected local delegation and
  nonfanout metadata NDJSON bodies are covered by the producer below. No fanout
  NDJSON/Arrow coverage is inferred. Dry-run NDJSON preferences that actually
  return buffered planning JSON remain supported; explain likewise uses JSON.
- Explicit/transparent distributed selection can still serve bounded buffered
  tier-1 results or delegate locally. Actual fanout refuses after classification
  but **before** fragment partial work, admission or dispatch. Nothing reroutes to
  evade the guard. Explicit requested dry-run may really execute; batch priority
  may be a synchronous tier. Neither flag establishes a job or lifetime scope.
- Both ordinary external and coordinator-authenticated shard requests refuse before
  history/catalog/scan; Arrow/wire compatibility remains future work.
- All four Jaeger handlers refuse in `TraceRequest::begin`, before reservation,
  catalog or name cache. The hostile find-traces limit validation remains earlier.

These fixed `503 query_controls_path_unavailable` guards are temporary coverage
refusals, not new capabilities, new authorization or claims of complete controls.
No guard is installed when authority is absent. Jobs already stored/status/result/
DELETE do not acquire a new query budget or authorization capability.

## Cooperative production and explicit task ownership (Slice 2B)

Every configured successful SQL/local/explain/selected-nonfanout response carries
its ORIGINAL authenticated ingress deadline into server body production. There
is no new timeout at render, delegation, body start or fallback. The authority
reserves a global technical task slot before handler/source work, separate from
existing admission policy. There are at most **128 reserved/registered body tasks
per authority**; full/closed generation returns fixed `503 query_controls_busy`,
not an unbounded queue or a per-agent concurrency product. Existing query admission
sizing and breaker/row clipping choices are unchanged. Source guards remain owned
through outer body EOF, body abandonment or expiry, including a tiny source whose
EOF fits entirely in the channel. EOF of the producer is not delivered-body success.

The independent Tokio producer selects original expiry and body cancellation
against both source polling and channel sends. The channel holds at most **two
items of <=16KiB logical bytes**, plus at most one <=16KiB copied pending send;
EOF is a typed item. Chunk copies avoid retaining an entire source allocation in
small queued byte slices. The retained source may separately hold a complete
upstream Arrow batch/backing allocation, an encoded row/native field or an already
buffered records result. These are **not** channel bytes or a total-memory bound.
There is no new hard value/output/actual-scan cap and no full-query encoding copy.

Buffered Arrow-to-records conversion yields/checks once per row; streaming Arrow
NDJSON renders one row per step while preserving batch-boundary clipping/scan
breaker decisions. Metadata NDJSON and records-envelope body rendering serialize
one row/field at a time. Checked sinks test deadline/cancellation before/after each
<=16KiB output fragment; the producer yields/checks between outgoing chunks/rows.
A single native parser/Arrow/serde conversion or escaping step, row allocation,
field/row size, upstream batch/backing allocation, scheduling delay, scan-pump
unwind and OS wall latency are NOT bounded by those numbers. Work is cooperative,
not OS-preempted. A native step can overrun before its next check; then late success
is rejected and owned work drops at that boundary. Strict native-phase/total-memory
bounds remain unproved; no guaranteed hard elapsed-time ceiling is claimed.

At post-header expiry the source is dropped/cancelled and guards released; status
cannot become 504. NDJSON may emit the fixed `{"_meta":"timeout"}` line only if the
outer body is at a record boundary (including no bytes consumed). Already-queued
data is discarded. If a record is partially emitted, the body returns a transport
error/end instead: it MUST NOT append a valid-looking marker to a malformed line.
Other body formats terminate transport without JSON suffix. Marker delivery, TCP
receipt and client ACK are never guaranteed. Typed history candidates/outcomes,
not query-controlled JSON strings, distinguish source success, execution error,
timeout and body abandonment. Original receipts/expiry/privacy remain unchanged.

One shared deadline-aware terminal arbiter decides producer errors, timer expiry,
outer delivery and abandonment. Its cooperative commit checkpoint gives the
original deadline precedence over a late-ready synchronous error; an already
committed legitimate terminal stays immutable. Source-produced NDJSON error
markers, like source EOF, retain only a bounded typed history **candidate**. A
marker delivered before the deadline records query `failed` (transport delivery
is not query success); expiry before outer delivery records `timed_out`, and
abandonment records its truthful terminal. Candidates contain no marker/error
payload and cannot preempt the body arbiter. Default absent-authority source
history is unchanged. After dequeue, data is checked again against the selected
terminal; queued EOF is clean only if delivery actually wins arbitration. These
are cooperative checkpoints, not atomic nanosecond wall-clock preemption.

### Lifecycle ownership and confirmation

The authority retains every producer JoinHandle in a bounded registry until a join
(including error/cancellation) is actually observed. There is no detached reaper,
second runtime/thread, return task, process-global retention or history-owned work.
Body Drop requests cancellation only; it is NOT termination proof. While service
is live, the lifecycle owner must stay alive. Call
`AppState::shutdown_query_controls().await` (or the retained authority's `shutdown`)
at local/test service teardown. Shutdown closes generation admission, cancels and
observes every producer join. Cancelling a shutdown wait leaves handles in the
closed registry for a later await; concurrent waiters are supported. No await runs
under the registry mutex, and a reserved slot cannot spawn/reinsert after closure.

Every producer JoinHandle is polled with a stable registry-owned broadcast waker,
not a shutdown caller's task waker. The waker owns only its notification primitive,
not the registry/tasks. Shutdown waiters register before reaping, so cancellation
of the newest independently spawned waiter cannot steal the surviving waiter's
join notification. Handles remain in place until joins are observed.

**Final synchronous authority Drop is a catastrophic/final teardown fallback:** it
closes admission, requests cancellation/abort and cannot await joins. Termination
and cleanup are **unconfirmed**, not reported as joined or fully cleaned. It does
not block the executor or start a reaper. The still-running-executor test verifies
eventual guard release for cooperative synthetic work, not proof after runtime
termination. Normal accepted cleanup requires explicit shutdown/join observation.

### Tests and remaining gates

`check-query-policy.sh` runs the isolated policy/runtime/body suite and a real
signed local-JWKS/PG/warehouse routed test. It covers selected HTTP NDJSON plus a
signed Axum response body deliberately NEVER polled: 700ms primary preparation
spends the original 1s caller budget. Reacquisition of the complete real admission
budget proves guard release before a reset body timer could fire; metrics snapshots
(which RESET gauges) are not mistaken for live resource counters. HTTP clients use
a longer 10s timeout. This is real local engine execution, not mocked resource or
provider evidence. Tiny source EOF cannot release its reservation early.

Pure lifecycle tests retain real admission guards, semaphore permits and cancellation
flags across idle/trickling/full-channel/tiny sources, EOF/error/timeout/drop races,
capacity, cancelled/concurrent shutdown and final Drop. Paused-clock cleanup is
asserted after 12 explicit executor yields; the real synthetic route asserts release
by original ingress +1.5s (not a universal wall bound). Rendering tests cover
compatible values/order/null omission/clipping, fragments/inter-row expiry and a
late-ready native serde step; PG history tests prove source EOF stays nonterminal
until outer EOF or a typed failure/timeout/abandonment. Regression tests separately
spawn and order shutdown waiters, cancel the newest while native work is held,
and require the survivor to join and release real guards. Integrated producer/PG
history tests cross the original deadline inside one synchronous error poll and
verify timeout transport plus `timed_out`, with and without a source failure
candidate. Timely delivered error markers record `failed`; queued never-consumed
markers time out; dropped markers record abandonment. Deterministic dequeue-race
tests require winning timeout to discard queued data/EOF at both complete and
partial record boundaries. No native-phase preemption,
large-value allocation limit or remote pump termination is inferred.

Actual batch handoff/submission-anchored queue/run/dequeue/recovery, all-four Jaeger
resolution, Arrow/internal worker context, fanout/fallback/retry/skew and public
presentation remain separately reviewed gates. Guards for those paths remain. No
policy/history/job schema, manager grants, wire, OAuth/query-selection semantics,
source product defaults or main/env/CLI activation are added.

Production primary/TLS/admin/schema/logging trust, audit access/retention/erasure,
operator values, grants/migrations and rollout remain explicit approval gates.
Configuration isolation is not a sandbox. The historical terminal-wait transient
remains unexplained; later green selections do not supply its root cause.
