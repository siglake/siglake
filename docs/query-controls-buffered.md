# Query controls: injection-only buffered Slice 2A

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

- Actual local NDJSON/body execution refuses before execution/admission/body
  production. Delegated SQL may already have performed deadline-covered catalog
  and rewrite preparation before selecting that local refusal. Actual durable
  batch submission refuses before INSERT (direct local selection also precedes
  catalog/admission). Dry-run NDJSON preferences that actually return buffered
  planning JSON are still supported; explicit explain likewise uses JSON.
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

## Cooperative cancellation and remaining gates

Native CPU parser/serialization work is cooperative. Post-work checks prevent late
success but do **not** guarantee OS preemption, zero overshoot or bounded CPU chunks.
The existing local cancellation/admission ownership is retained; no new background
query/body producer is spawned. Cancellation-aware chunked rendering and independently
expiring body production (idle/trickling/never-polled streams), actual batch handoff
plus submission-anchored queue/run/dequeue/recovery, all-four Jaeger resolution,
Arrow/internal worker context, fanout/fallback/retry/skew and public presentation
remain separately reviewed gates. The current request anchor is not a guessed job
handoff/run clock, and unknown distributed classification never renews it.

The local fixture runner is `scripts/check-query-policy.sh`. New unit checks cover
formula/state/reset/warning-only, both priorities, invalid operator values, huge/zero
caller values, false breakers, exact key derivation, original-clock delegation,
late-ready checks and discriminator separation. Its additional synthetic signed JWT
router test covers real primary SQL/local/explain/planning/cache hit/follower,
primary delay/failure/unsafe/closed states, ignored forged hints, unsupported route
refusals, no durable INSERT/fanout and absent-authority compatibility. It uses only
new socket-only PG16.14 plus a loopback synthetic issuer/warehouse; it is not real
Cognito/OAuth/TLS or production authority. Catalog/collect delay instrumentation is
not exposed by this test API: common phase and cancellation tests cover the wrapper,
but per-native-phase CPU/chunk/resource overshoot is not claimed proven here.

Production primary/TLS/admin/schema/logging trust, audit access/retention/erasure,
operator values, grants/migrations and rollout remain explicit approval gates.
Configuration isolation is not a sandbox. The historical terminal-wait transient
remains unexplained; later green selections do not supply its root cause.
