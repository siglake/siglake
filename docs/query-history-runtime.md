# Optional buffered / local NDJSON query-history integration (PARTIAL coverage)

`AppState::with_history(HistoryService::observer())` is an explicit production
state/router seam. The default is **None**, and no standalone binary startup,
CLI flag, URL/environment resolver or migration enables collection. The caller
must explicitly supply a trusted, preflighted writer and retain the sole service
owner. Tests inject that application state; this is not deployed collection.
See [the unchanged storage/role contract](query-history-store.md). Reader schema,
RLS, column ACL, literal `pg_` contract and original 30-day expiry are unchanged.

## Buffered route boundary and attribution

The external handler owns exactly one observation. Observation begins after
production authentication and JSON extraction and the pure authoritative
`AppState::execution_tenant` routing decision, before namespace-open, empty SQL,
rewrite, cache, planning, admission and execution can fail. `resolve_ice` and job
ownership share that same `(TenantRegistry present, authenticated tenant present)`
decision; catalog opening remains inside its existing execution deadline. A
selected tenant does not prove that the catalog opened or work was admitted.

There is **no accepted canonical tenant for shared/default storage**. A signed
tenant without a configured registry is not used for routing and is not used for
history. Missing/ambiguous selected tenant, including current open/static/default
routes, is explicitly unobserved; query behavior is unchanged. For a selected
named tenant, generic verified callers without a supported provider principal
still produce tenant-scoped **unknown** principal rows. The engine does not know
console credential mappings. Only existing authenticated `CallerIdentity` can
supply verified issuer/client; no headers, body provider fields, subjects, emails,
user-supplied correlation IDs or namespace guesses supply attribution. Principal
validation failure disables observation, not the query.

### Supported/unsupported matrix

| Path | Observation in this slice |
| --- | --- |
| `/api/v1/sql`, records, no configured peer source, interactive | Buffered planning/data/cache/typed errors; one external parent |
| `/api/v1/sql/local`, records, interactive | Same, independent of configured peers |
| Supported SQL records `dry_run` (including batch priority dry-run) | Planning only; no returned/scanned measurement fabricated |
| `/api/v1/sql/explain` | Independent planning observation; actual JSON format regardless of ignored request format/priority |
| `/api/v1/sql/distributed` with no captured membership, eligible records | Not-admitted validation rejection; no helper/duplicate parent |
| Buffered records metadata fast-paths | Numeric completion boundary at selecting a real fast-path result; count path exercised by new routed proof |
| `/api/v1/sql` with **any configured peer source**, even discovery without a current membership | Deferred entirely, including local fallbacks |
| Configured explicit coordinator/fallback and direct/coordinator shard routes | Deferred; no parent/child rows or fabricated terminal success |
| `/api/v1/sql` NDJSON with no configured peer source, nonbatch | Same external parent transferred into the actual typed source/body lifetime |
| `/api/v1/sql/local` NDJSON, nonbatch | Same, independent of configured peers; real EOF/error/truncation or server-observed body abandonment |
| Explicit distributed NDJSON, configured transparent coordinator/fallback, shard, other formats | Deferred; no stream/job lifecycle inferred |
| Batch submit/run/cancel/restart (202) | Deferred; submission is not terminal success |
| Auth/tenant middleware or JSON extractor rejection | No execution observation |
| Missing/ambiguous effective tenant, no observer, stopped service/no DB receipt | No durable execution row promised; never a fabricated correlation ID |
| Jaeger/search/warmers, direct engine APIs, MCP subprocess, console chain | Not observed/proved by this slice |

Records cache acceptance, a selected real metadata result, accepted planning-only
work, and the real reservation/disabled-admission boundary explicitly provide
admission evidence; **HTTP200 alone never does**. Pre-admission bad SQL,
validation/planning/admission rejection stays not admitted. Execution errors and
504 timeouts use fixed safe codes without inspecting/copying error strings.
Buffered completion means engine execution/render handler completion, **not
network delivery**. Transparent local helpers do not open another observation.

A request-scoped task-local safe evidence object carries only the existing
handle, fixed enums, nullable numeric counters and booleans through internal
helpers. It never owns SQL, results, tokens, arbitrary errors, plans or names.
Renderer hooks copy only numeric row counts, truncation and existing measured
scan counters, never read/clone rows or HTTP bodies. Cache hits do not reuse the
original run's scan counters as current measurements. Estimates are distinct
from measured counters; absent/unestablished estimates and unavailable measured
counters stay NULL. Estimator completeness is internal provenance independent of
exactness: successful heuristics and established zero estimates remain available,
but failed or partially accumulated estimates do not. Scan evidence comes from
authoritative execution branches and complete instrumented snapshots, never the
presence of default response stats. Uninstrumented fast paths (including windowed
boundary counts and footer/page fallbacks), snapshots with unsettled partitions,
and residual retries whose final snapshot omits the first attempt retain NULL scan measurements while preserving returned-row/truncation evidence.
Whole-table metadata count has positive no-data-scan evidence and can report zero.
This does not change execution, settle deadlines, scheduling or limit enforcement.
Success requires explicit completion and admission evidence;
an unrecognized successful branch would retain uncertainty, not synthesize it.
All original SQL remains parser-free NULL. Existing raw engine parser/audit/job
logging remains **UNSANITIZED and outside the new-store guarantee**; nothing is
deleted, backfilled or silently cut over.

`x-siglake-history-id` is added to success/error responses **only for a real DB
receipt** and matches its execution UUID. It is correlation, not acknowledgement
that the row is durable: initial/terminal failures may queue or lose evidence.
Absent receipt means no header. Clients cannot choose/override the UUID.

The monotonic `duration_ms` is elapsed **after receipt/initial write and before
terminal selection/write**, including catalog I/O/planning/serialization. For
buffered work it ends at handler completion; for NDJSON it continues through
the observed source lifetime, including downstream polling/backpressure, until
typed completion/error or body abandonment. It is not pure scan time, a proven
execution/admission timestamp, or full client HTTP latency. The existing query deadline starts
where it did before. History begin/finish each retain their configured 100ms
budget outside that execution budget; unavailable history can add bounded
telemetry overhead but cannot alter result/auth/admission policy. Original event
wall times remain unknown; `received_at` is DB receipt time and
`admission_observed_at`/`finished_observed_at` are DB persistence-observation times,
not exact original event/completion times. Retries retain original receipt/expiry.

## API and ownership

* Explicitly connect a `HistoryWriter` using reviewed options; construct
  `HistoryService::new(writer)` inside a Tokio runtime. It owns exactly one
  retry/maintenance task. No URI/environment lookup, migrations or backend
  fallback are performed. The supplied `PgConnectOptions` is a trusted provenance
  boundary: SQLx constructors/URI parsing can already have consulted PG environment
  defaults and passfiles before this API sees it. `new_without_pgpass` skips only
  passfile loading, not PG defaults or default-host probing. Callers must construct
  options from deliberately trusted configuration; this service neither resolves
  URIs/environment nor retroactively guarantees how options were created. Pool
  creation/preflight is separate from request observation; request availability
  must never depend on history readiness.
* `HistoryService::observer()` clones a non-owning `HistoryObserver`; it never
  owns/spawns/joins the maintenance task. Service shutdown/drop closes surviving
  observers and remains cancellation-safe.
* `begin(Principal, RequestMetadata, RedactedSql)` returns a handle, not a query
  admission decision. A **single 100ms budget** includes DB receipt acquisition
  and initial persistence. If receipt acquisition fails, the handle has no
  receipt and `Unobserved` delivery. Query work can continue; no local timestamp,
  fabricated UUID/receipt, backdating or later replacement execution is created.
* If a receipt exists but initial persistence fails, retain its original UUID,
  DB `received_at` and 30-day `expires_at` in the handle and safe queued DTO.
  Missing initial persistence can recover with genuine admitted success. Every
  replay first idempotently persists that same pending DTO, then admission,
  uncertainty or immutable terminal evidence. Expired receipts never reinsert.
* `receipt()` returns an optional clone of that opaque original receipt;
  `initial_delivery()` describes the initial attempt only, not current durability.
* `admit()` records **trusted backend lifecycle evidence**, not a request flag,
  HTTP status or persistence acknowledgement. It queues a bounded owned DTO and
  does not await. Its return value describes volatile delivery, not execution
  admission policy. A genuine terminal may itself supply admission evidence
  after an absent admission write; contradictory local evidence is rejected.
* `finish(TerminalExecution)` selects one immutable logical terminal under a
  short synchronous lock, queues a cancellation-safe copy **before awaiting**,
  and attempts a bounded write. Identical repeated choices return
  `AlreadyFinalized` (not a fresh durable acknowledgement); different choices
  conflict. `Persisted` acknowledges that attempt, `Queued` is **not durable**,
  and `Unobserved` reports local loss. Queue loss can also lose selected terminal
  evidence; repeating an already-selected terminal is not an implicit replay API.
  Concurrent cancellation/finish/drop never invents another terminal outcome.
* The caller supplies exact typed outcome and separately measured monotonic
  duration/counters. Start the monotonic clock at the eventual outer wrapper
  boundary, not on a retry. Unknown measurements remain NULL. Original lifecycle
  wall-clock `admitted_at`/`finished_at` remain NULL; DB
  `admission_observed_at`/`finished_observed_at` are **persistence observation**
  times and must never be displayed as exact event times. Retries preserve them.
* Transfer the same handle (or an `Arc` of it) into the eventual owner of the
  last authoritative completion. Do not create a second parent for nested
  local/distributed execution, cache paths or job handoff. A dropped unfinished
  handle queues `indeterminate`, without a fabricated outcome/duration or a
  claim that execution stopped. Drop uses a nonblocking queue try-lock and atomic loss signals; it never waits
  for a contended state lock, awaits, blocks on DB or spawns tasks.

### Requirements for subsequent integration passes

Records wrappers own early `?` exits and finalize typed execution/render results
before returning; this is not proof of network delivery. Local NDJSON transfers
the same owner through source completion/error/truncation and body abandonment
as described below. Other formats and configured distributed sources remain
unobserved. Batch wrappers must persist/transfer the
same execution UUID through job submission, run, cancellation and recovery;
202/queued is not terminal success. Coordinator fanout must not produce duplicate
public parents or trust client-provided parent IDs. These changes require branch
and cancellation tests, and an explicitly reviewed legacy raw-audit/log cutover.

Coverage remains PARTIAL. The deferred matrix above is not inferred from seeded
history events, and no full SQL-history, production activation, erasure,
complete-roadmap, MCP-subprocess or console-chain claim is made.

## Local NDJSON ownership and typed evidence

Only positively selected named-tenant, nonbatch, nonshard requests are included.
Transparent `/sql` requires **no configured peer source**; explicit `/sql/local`
remains local even with peers configured. Auth/extractor/default/shared paths,
configured coordinator/fallback, explicit distributed NDJSON and jobs remain
deferred/unobserved. No startup activation or URI resolution was added.

The existing admission hook and verified principal create one external parent;
`render_ndjson` transfers that same handle and original monotonic start into
`NdjsonStream`. HTTP200 and stream construction select no terminal. The source
calls typed numeric hooks at actual upstream EOF, existing encoded execution
error/render failure, or the existing row/scan-cap truncation decision. The
source's error marker is still **Ok(JSON bytes)**; history never parses that
marker or mistakes the later EOF for success. No requests, Arrow batches,
results, marker bytes or arbitrary errors enter the history DTO. Legacy response
error text and raw audit/engine logging remain unchanged and unsanitized outside
this new-store guarantee.

Returned rows count successfully rendered Arrow rows at typed source completion,
not newlines, trailers or client receipt. Truncation comes only from existing
source cap evidence. Complete scan counters additionally require positive
instrumentation and zero unsettled partitions at natural EOF; early truncation,
errors and abandonment keep them NULL. No scan/cost zeros are inferred from a
cache or default runtime stats. Metadata NDJSON fast paths transfer ownership
into their already-rendered payload's body: numeric row counts come from the
selected typed result, and only whole-table metadata count has positive
no-data-scan zero evidence. Other metadata reads retain NULL scan counts.
Unobserved metadata paths retain their original buffered body representation.
Observed metadata bodies use an owned stream over the existing payload (without
copying/parsing it); transport framing may therefore be chunked.

Dropping an unfinished source/body selects `disconnected` with nullable unknown
returned/truncation/scan measurements. This means **server-observed delivery
abandonment**, including in-process Body drop: not proof of TCP disconnect,
engine cancellation, or client receipt. Existing `GuardedStream` admission and
CancelOnDrop guards are unchanged. A genuine engine error/completion already
selected before drop remains immutable. Poll/drop terminal selection uses a
try-lock and the bounded volatile queue, never awaits/spawns or waits on DB/state
locks. Contention/full/closed queue retains loss signals and uncertainty; queued
evidence and the correlation header are not durability acknowledgements.

The **existing** NDJSON timeout covers catalog/planning/admission/stream
construction before the handler returns. There is no existing mid-body
wall-clock timeout, and none was added. A pre-body typed 504 is observed by the
outer wrapper; this must not be presented as proof of mid-stream timeout or
cancellation. Original event wall times remain NULL; DB observation times retain
their existing meaning and original expiry is unchanged.

## Bounded writes, retries and loss provenance

All request/terminal/retry/maintenance attempts use the production **100ms**
budget. The one in-memory queue holds at most **1000 entries / 8MiB serialized
safe DTO payload**, including the one in-flight retry reservation. It contains
only bounded owned principal/request classification, opaque receipt, fixed
enums, measurements and tenant-scoped loss-point evidence. Never raw requests,
SQL, results, parameters, tokens, arbitrary errors or fingerprints. SQL remains
NULL for all inputs without invoking any parser/tokenizer. The service emits no
logs. Existing engine audit/job/SQL logging is explicitly unsanitized and outside
this new-store guarantee.

Retries run on a one-second cadence (also woken by new work), at most 32 per
worker turn, stopping a batch on an unavailable write. Payload age is at most
30 seconds for retry admission; the remaining age also limits an in-flight
retry's deadline. A bounded scan discards expired waiting payloads, including
those behind a failing head. Duplicate queued execution snapshots coalesce
without resetting their first retry time; terminal evidence supersedes earlier
snapshots. A foreground terminal copy may race the queued copy safely.

A Rust future timeout/cancellation **does not prove PostgreSQL did not commit**.
Retries use original identity/expiry and accepted conditional immutable writes;
acknowledgement replay is idempotent, conflicting terminal payloads never
overwrite. This is not exactly-once SQL execution/delivery. Neither the queue
nor a handle is a durable outbox; kills can lose whole observations/outcomes.

Fixed `LossReason` counters are process-local **signals**, not exact missing-row
counts across crashes. `NoReceipt`, unavailable, timeout, queue-full, retry-age,
queue contention, conflict, expired and shutdown are distinguished. Coverage is always partial.
When bounded capacity permits, known tenant loss signals are queued and persisted
on recovery as **DB-time point observations**: `started_at = ended_at =
received_at`, reason `store_unavailable` or `queue_full`, fresh 30-day coverage
expiry and an idempotent gap UUID. These timestamps mean the signal was recorded
at recovery, NOT the outage's original bounds. No crash/outage boundaries,
unknown caller activity, exact loss totals or original execution receipts are
invented. Retry age alone is not evidence of store unavailability: an expired
execution payload creates a store-loss point only when an actual unavailable/
timeout write was observed; coalescing preserves that evidence. Queue contention
is a local loss signal, not fabricated queue-capacity or outage evidence.
Loss-point payloads can themselves overflow/expire/crash; a local
counter does not guarantee a durable tenant coverage row.

## Heartbeat, recovery, primary cleanup and health

The task immediately heartbeats and performs startup catch-up, then heartbeats
about every 40 seconds and purges about every 60 seconds. Failed maintenance or
remaining backlog retries about every second. Each heartbeat pass recovers at
most 1000 other-owner pending rows under a 120-second lease, with a receipt-age
grace for absent owner registration. The current owner is excluded. Stale or
absent heartbeat proves only uncertainty: rows become `indeterminate`, never
failed/succeeded. Execution may still be running through a DB partition; genuine
late terminal evidence can reconcile before original expiry.

Cleanup calls the accepted 1..1000 batch primary DELETE, including coverage and
unreferenced old-owner cleanup and its advisory-lock/SKIP LOCKED coordination.
A separate index-oriented backlog query checks all three purge domains. A zero
execution-delete count (including lock contention) is **not** catch-up evidence.
Startup stays degraded until backlog is absent; competing workers/restarts retry.
`health()` exposes startup catch-up, current maintenance/writer reachability,
oldest primary overdue age when known, last cleanup/heartbeat instants, due/stale
flags, queue reservations, recovered-uncertain count, fixed loss signals and
closed/partial status. Readiness also checks task liveness, cleanup freshness,
heartbeat age and absent backlog. It is not SQL availability or complete history.

`shutdown(&mut self)` closes admission to new observations and uses one 100ms
asynchronous waiting budget for volatile drain and abort/join, not a hard
real-time scheduler guarantee. On deadline expiry it requests abort; this is
not proof termination has already been observed. The task handle stays owned
across cancellation and is removed only after completion is consumed. If the
same deadline prevents joining, shutdown returns false with the aborted handle
retained for later shutdown/Drop. True requires observed completion and an
empty queue, not durable delivery or absence of prior observation loss.
False means uncertainty/loss may remain. Service Drop closes and aborts without
awaiting or starting more tasks; handles surviving shutdown report unobserved
writes. Persisted pending rows remain eligible for later owner recovery. No
shutdown path manufactures successful terminals, and no process-local count can
reconstruct a crash.

Only primary live rows are purged: **not physical secure erasure**, WAL, MVCC,
replicas, backups, snapshots or existing legacy audit/job copies. No production
sweeper is running. Complete 30-day erasure across those domains remains a hard
rollout gate with **no exception**. Exact expiry minus/at/plus one-microsecond
behavior remains unverified, not a known predicate defect.

## Focused local verification

`scripts/check-query-history.sh` clears inherited environment, uses only pinned
Rust 1.95 and cached PostgreSQL 16.14, and owns its disposable socket-only
fixture. Its test-only isolation helper creates fresh private HOME/XDG/provider
paths, empty 0600 explicit PGPASSFILE **and** private HOME/.pgpass (SQLx falls
back to the latter even when the former is empty), empty AWS/provider files and
disabled AWS metadata discovery. Only fixed synthetic PG defaults are installed
after clearing inherited PG variables; fixture options use `new_without_pgpass`,
explicit socket/role/database/empty password/SSL-disabled settings, and assert
isolation before every construction. Locked offline builds use an isolated
CARGO_HOME linking only known registry source/index/archive caches, not original
Cargo configuration/credentials or HOME; compiler/target/jobs remain explicit.
Cargo launches from a fresh private temporary cwd with an explicit workspace
manifest, removing the real HOME/worktree ancestor chain from configuration
discovery. Root-/tmp-level build configuration remains a trusted host assumption;
this is not a filesystem sandbox. Selected tests do not spawn Cargo, and their
SQLx fixture construction checks its own runtime isolation before each call.
Prior runs retaining real HOME are trust-limited due possible ambient-file
consultation, not proof credentials were read/exposed; no actual passfile or
provider configuration was inspected. Corrected complete selected reruns are
required rather than accepting those prior results. The added source-named runtime tests include paused-clock budget/queue
age/capacity checks and **real-clock** PostgreSQL initial/terminal timeouts,
missing receipt/initial recovery, identical committed-evidence replay, concurrent
terminal/drop, cancellation-safe finish, expiry during outage, lease uncertainty
and late evidence, 1000-row recovery limit, cleanup locks/batches/startup/failure/
restart, a real approximately 60-second periodic purge/heartbeat, bounded
shutdown and task abort. Replay after a known committed write models lost
acknowledgement; it is not network-fault injection at the commit packet. Separate
existing privacy/logger/ACL/identity regressions remain required. These are
internal core tests, not routed SQL collection, full CI or source acceptance.

### Buffered integration proof

`scripts/check-query-history-buffered.sh` uses the same corrected isolation
helper and disposable real PostgreSQL setup as the prior core runner. The new
ignored `oidc::buffered_history::real_router_buffered_history_identity_lifecycle_privacy_and_outage`
test reuses the existing public test-only local RS256 issuer/JWKS fixture and
executes the **production HTTP router** against a temporary real engine warehouse
(one synthetic engine event supplies the real snapshot needed for cache; no
history execution is seeded). It checks renewed same-client identity, distinct
client/tenant/header tampering, reader tenant isolation, real cache miss/hit,
planning-only dry-run/explain, parse/planning/catalog/execution/admission/timeout
failures, count metadata completion, one UUID/owner, DB-admin privacy/NULL SQL/
original expiry/counter inspection, auth/extractor/deferred/default non-observation,
real PG table-lock write deadline/retry with original UUID, a history delay longer
than the query budget without consuming it, and fixture-role no-receipt outage
without changing real query results. It also checks observer clones surviving
shutdown and service Drop. Only this fixture's CONNECT privilege/sessions are
changed for fault injection; all fixture state is destroyed on exit. This is not
production startup, actual MCP subprocess or console/browser integration.

Trust remains **trusted-host root-/tmp-level build configuration and local
filesystem/processes**, not a sandbox. No real HOME, passfile or provider config
is inspected. The runner uses private Cargo cwd, explicit workspace manifest,
registry-only private CARGO_HOME, pinned 1.95, `--offline --locked`, bounded build
jobs, disposable private HOME/XDG/provider files and metadata discovery disabled.

### NDJSON local lifecycle proof and limits

`scripts/check-query-history-ndjson.sh` uses the same isolated socket-only PG16.14,
private registry-only Cargo home/target, native pinned Rust1.95, two build jobs,
private HOME/XDG/empty passfiles/provider files and local signed issuer as the
buffered runner. Its source-named production-router test covers natural EOF,
late real-engine execution error encoded as JSON, actual row-cap truncation,
existing pre-body timeout, and unpolled / partially-polled in-process Body drop.
It also exercises renewed/two-tenant identities and spoofed headers, real PG lock
outage with bounded start/drop and original-UUID recovery, no-receipt outage,
configured-peer/default/explicit-distributed exclusions and owner shutdown with
an outstanding body. Body-drop assertions are **not listening TCP disconnect
proof**; the listening loopback server is used for completed stream/error cases.
No deterministic listening-client backpressure/disconnect fault is claimed.

Separate internal PG tests use a bounded synthetic channel source (not a real
engine execution) to control pending polls, late typed failure, source EOF,
returned-row and scanned-row caps, abandonment before a queued trailer, and
marker-shaped / newline-containing row values without arbitrary sleeps. They
check one immutable terminal, thread-barrier terminal/drop contention and
shutdown races, and NULL uninstrumented/incomplete scans. Existing GuardedStream
cancellation and formatter/pool-marker regressions remain separate checks. There
is no new mid-body timeout/cancellation acknowledgement, real provider test,
full CI, complete metrics/coverage, full erasure, console/MCP-chain or production
activation claim.
