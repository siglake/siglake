# Internal query-history runtime core (not routed collection)

`siglake_query_server::history::{HistoryService, ObservationHandle}` is an
internal lifecycle API, currently called **only by tests**. No application
startup constructs it, no CLI/environment flag enables collection, and no SQL,
HTTP, stream, job or legacy-audit path uses it. See
[the unchanged storage/role contract](query-history-store.md). Schema, reader
RLS/column-ACL preflight, fixed roles and parser-free SQL omission are unchanged.

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

### Requirements for the next integration pass

Records wrappers must own all early `?` exits and finalize typed execution/render
results before returning; this is not proof of network delivery. NDJSON/other
stream wrappers must transfer ownership through EOF/error/timeout/truncation and
body abandonment, supplying genuine admission/disconnection evidence rather than
letting a temporary handle drop early. Batch wrappers must persist/transfer the
same execution UUID through job submission, run, cancellation and recovery;
202/queued is not terminal success. Coordinator fanout must not produce duplicate
public parents or trust client-provided parent IDs. These changes require branch
and cancellation tests, and an explicitly reviewed legacy raw-audit/log cutover.

**NOT YET OBSERVED:** public SQL records/cache/dry-run/fast paths, explain,
explicit local/distributed SQL, externally called shard routes, authenticated
coordinator children, NDJSON/CSV/Arrow/Parquet streaming completion/abandonment,
batch submit/run/poll/cancel/recovery, and all early handler/extractor failures.
Jaeger/search/warmers are not implicitly observed either. No public read/event
endpoint, console integration or executed-query E2E coverage is added here.

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
