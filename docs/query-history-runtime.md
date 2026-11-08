# Optional SQL / coordinator / shard query-history integration (PARTIAL coverage)

`AppState::with_history(HistoryService::observer())` remains the explicit
injected-router seam. The standalone binary now also supports **local opt-in
startup**, described below. Default configuration has no runtime observer and
makes **no history connections**. This is not production activation or approval
of provisioning, migration, deployment, erasure or consumer compatibility.
See [the storage/role contract](query-history-store.md). Reader fields, RLS,
column ACL, literal `pg_` contract and original 30-day expiry are unchanged;
v1/v2/v3 migration files are untouched.

## Standalone collector configuration and ownership

Only `SIGLAKE_QUERY_HISTORY_DATABASE_URL` enables the standalone collector.
There is deliberately no URI CLI argument (and no secret-valued clap field).
Absent means disabled even when catalog/jobs/DATABASE_URL/PG configuration exists.
An explicitly empty, non-Unicode or syntactically invalid value rejects startup
before catalog/OIDC/listener work. Activation also **requires the operator to
explicitly set `--audit-disabled` or `SIGLAKE_QUERY_AUDIT_DISABLED=true`**; the
collector never rewrites that setting. Existing raw parser/job/other engine
logging and historic audit/job copies remain **unsanitized**, outside this
new-store guarantee. Disabling the old audit does not sanitize those paths.

The restricted URI must explicitly supply scheme `postgres`/`postgresql`, fixed
username `qh_writer`, password colon, host, nonzero port, database and exactly one
`sslmode` query parameter. TCP requires a nonempty password and **verify-full**
using the currently selected SQLx **bundled WebPKI roots**, not OS-installed
private CAs; no TLS downgrade is permitted. Installing a private CA in the OS
trust store does not configure this backend. URI certificate/key/root,
options, passfile, target override, duplicate or unknown parameters are refused.
Inherited `PGOPTIONS`, `PGSSLROOTCERT`, `PGSSLCERT` or `PGSSLKEY` (even empty or
non-Unicode) are refused before SQLx construction. Other effective PG defaults
are overwritten; `new_without_pgpass` is used, never SQLx URI parsing or pgpass.
No catalog/jobs pool/URI or PG credential/target fallback is used. Config Debug,
help and errors never include the URI or SQLx upstream error details. TCP custom
trust/client certificates are unsupported, not silently ignored. Owned local
Unix fixtures use an absolute percent-encoded socket-directory host, explicit
empty password, explicit port/database and `sslmode=disable`; this exception is
not a TCP trust downgrade. Do not put real secrets into shell history or examples.

`HistoryCollector` owns one cancellable startup task. Each of at most **8**
preflight attempts builds a **fresh** pool with one overall **2-second** budget
covering connection, fixed role and schema checks. Backoff is 1,2,4,8,8,8,8
seconds (39 seconds total), with cancellation during connect/backoff. No observer
is published before complete preflight. Missing/unsafe role/schema is `Rejected`,
connection/operational failure (including query cancellation while executing role
or schema checks) is `Unavailable`. Only a completed false contract is rejection;
exhausted states remain
`ExhaustedRejected`/`ExhaustedUnavailable` **until process restart**. They are not
healthy disablement and there is no control/restart API. Pool drops/cancelled
attempts cannot escape the single owner's finite attempt budget.

Standalone preflight requires the separately owner-applied **v3** schema, checking
its validated exact constraint, nullable field types, enabled immutable trigger,
function body/search path and owner. Every fresh pool connection repeats v3 and
fixed least-privilege role checks. An unsafe fresh connection is refused with a
fixed safe error, not a schema downgrade. Existing role/policy drift on
already-open connections and administrator trust remain external responsibilities.
No runtime DDL/grants, role switching, application-schema/job migration or reader
API is added. Apply unchanged v1, v2 where jobs are supported, and v3 through a
separately reviewed owner process; **recreate all reader/service pools** after
migration. Consumer compatibility and complete erasure remain external hard gates.

The dedicated writer uses a small owned-connection backend, **not SQLx PgPool**.
At most four local sockets (idle, checked out or connecting) are admitted by its
permit/idle-slot bookkeeping. Acquisition, connect, full preflight, SQL and final
protocol drain all remain inside the caller's original 100ms operation budget
(or startup's overall 2s budget). No await holds the idle-slot lock. A lease is
non-reusable by default: only completely successful, fully drained protocol work,
with no unfinished transaction, may return idle. Error/cancel/timeout/uncertainty
synchronously drops the raw socket, without a server-error-reading release ping,
SQLx pool callback, graceful-close future or detached return/maintenance task.
Healthy work reuses sockets rather than reconnecting/preflighting every operation.
Purge commits/rolls back atomically before dedicated reuse. Legacy explicitly
injected PgPool writer/reader behavior is separate and unchanged.

Service-owner shutdown (also when its wait is cancelled) and Drop close the
dedicated backend: idle sockets drop, new leases are refused, and in-flight bounded
leases cannot reinsert after closure even when non-owning observers survive.
Socket discard is **not acknowledgement that PostgreSQL stopped the operation or
did not commit**; original receipt/idempotence/expiry and uncertainty rules remain.

**Narrow diagnostic boundary, not universal server-message confidentiality:**
dedicated operation ERROR/error-return/release/close paths discard upstream text
and have only fixed diagnostics. SQLx0.8.6
`sqlx-postgres/src/connection/stream.rs` NoticeResponse handling separately emits
`notice.message()` under `sqlx::postgres::notice` when enabled. Statement logging
disablement does **not** cover arbitrary NOTICE/INFO, and PostgreSQL
`client_min_messages` cannot suppress all INFO. This local slice trusts the DB,
administrator and reviewed fixed history schema; arbitrary notices remain outside
the guarantee. No dependency patch/global logging filter/protocol change was made.
Production logging/trust/cutover remains an unresolved rollout gate, alongside
existing unsanitized raw parser/job/audit copies. History still never sends
original SQL/parameters/results/tokens to its store.

After successful preflight the task retains the **same single HistoryService**,
owner UUID, bounded queue and runtime/pool for its lifetime. It does not replace
receipts, reset expiry, backfill unobserved work or create another recovery cycle
on transient failure. Existing 100ms writes, 1000-entry/8MiB/30s retry queue,
maintenance and bounded fresh checked connections remain the recovery mechanism. Failed
writes/maintenance make aggregate status `Degraded`; `Active` requires actual
`HistoryService::health().history_ready`, sampled about once per second, not a
latched preflight success. After activation the observer stays attached during
outage: bounded writes may fail open or lose coverage as before. Startup exhaustion
requires restart; post-activation transient recovery does not. History status is
**never SQL readiness/liveness, authorization or admission authority**. No public
history reader/status endpoint is added; status logs contain fixed enums only.

The managed facade is non-owning and resolves to no observer while unavailable.
Normal listener completion explicitly shuts down collection after existing job
drain. Collector shutdown clears the facade, signals its owner, and uses one
100ms wait/abort/join budget, retaining its task handle across cancellation or
an unconsumed abort. Drop closes and aborts without awaiting or spawning; the
owned service retains its established task Drop behavior and closes its dedicated
backend even if observers survive. A true drain result is not
a durability/no-loss guarantee; process kill cannot drain volatile history.
Early startup errors also Drop the owner. Isolation in the local runner is **not
a sandbox**.

`scripts/check-query-history-startup.sh` uses native pinned Rust1.95/PG16.14,
private HOME/XDG, both empty passfiles, empty provider files/disabled metadata,
registry-only private Cargo home, private cwd/explicit manifest and locked offline
two-job builds. It runs collector cancellation/Drop/normal ownership tests and
subprocess config/passfile defenses. Its **actual executable** smoke uses the
existing explicitly trusted loopback RS256 issuer/discovery/JWKS seam, signed
named-tenant requests and socket-only PG: default-off/no collector sessions;
safe-role collection/NULL SQL/original expiry; bounded lock outage/original UUID
recovery, fresh post-Active degraded status and a fresh Active recovery transition
(using byte offsets, not cumulative startup log matches); unsafe role/disabled-v3-trigger/missing DB without
SQL regression; fresh-connection v3 refusal; valid TCP connection-refusal secret
sentinel; invalid-config/help redaction; graceful SIGTERM and SIGKILL while
unavailable. Binary traces/results/tokens stay memory-only, never evidence files.
Three separate owned-PG backend regressions verify four-socket cap, healthy
reuse, cancelled wait/discard/unfinished-transaction/owner close; real role and
v3 check query cancellations versus completed false rejection; and a gated real
late server ERROR after the 100ms client timeout with SQLx tracing/logger enabled.
The latter raw-connection control confirms the delayed synthetic ERROR in memory;
the dedicated timed-out lease closes without release/close logs or sentinel escape.
These are internal backend tests, not additional listening-binary claims.
A separate **constructor** test proves finite real-clock startup exhaustion;
unit ownership tests use an injected pending task. Those are not listening-binary
cancellation-of-a-Rust-future or production TLS/credential/rollout proofs. The
unconfigured local route retains accepted NULL effective metadata; requiring v3
does not invent effective fields or expand execution semantics.

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
| `/api/v1/sql/distributed` with no captured membership, extractor-accepted requests | Not-admitted validation rejection; effective semantics unresolved; no helper/duplicate parent |
| Buffered records metadata fast-paths | Numeric completion boundary at selecting a real fast-path result; count path exercised by new routed proof |
| `/api/v1/sql` with configured peers, including discovery without membership | One external parent over actual coordinator, metadata/cache, or local fallback branch; v3 effective metadata required |
| Configured `/api/v1/sql/distributed`, records | One external parent over actual selected execution; no changes to existing routing/fallback/admission/timeouts |
| `/api/v1/sql/shard`, externally authenticated named tenant | Separate truthful shard/Arrow buffered observation; signed tenant resolution, not body/header tenant; no job inferred |
| Authenticated coordinator shard work | No public parent/child history row or fabricated provider identity; existing service-token trust unchanged |
| `/api/v1/sql` NDJSON with no configured peer source, nonbatch | Same external parent transferred into the actual typed source/body lifetime |
| `/api/v1/sql/local` NDJSON, nonbatch | Same, independent of configured peers; real EOF/error/truncation or server-observed body abandonment |
| Configured coordinated NDJSON | Existing fanout is buffered; original payload body owns observation through EOF/drop; returned rows known, complete remote scan counters unavailable |
| Configured local NDJSON delegation | Same existing typed local source/body owner, EOF/error/truncation/drop semantics |
| SQL request formats other than records/NDJSON | Still rejected by existing JSON extractor; not implemented or observed |
| Local non-coordinator batch submit/run/cancel/controller recovery | One external UUID; 202 is queued, not execution admission or terminal success; v2 association required for verified durable resume |
| Configured transparent batch | Existing local job path; same original UUID transferred, v2 association plus v3 effective metadata |
| Explicit distributed batch/dry-run hints | Branch-dependent legacy behavior, NOT a planning/job guarantee; see v3 warning below |
| SQL body-shard variants on configured routes | Existing local path, one external parent; no new shard capability |
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

## Configured coordination and v3 effective semantics (manual OWNER dependency)

**Pre-existing API limitation, not fixed here:** explicit `/sql/distributed`
can execute a distributable query even with `dry_run=true`. It can use
`priority=batch` only as a coordinator resource tier and return HTTP200
records/NDJSON, not a queued job. Its nondistributable/scan-local branches instead
delegate to local planning/job behavior. The chosen branch is known only after
catalog/planning; moving it before history begin or enforcing new validation
would change execution policy. Transparent `/sql` continues to honor its existing
local dry-run/batch routing. Query-controls must be revalidated against actual
execution paths in a separate pass, never against these request hints.

Apply unchanged v1 `schema.sql`, optionally/where jobs are supported v2
`schema-v2-batch.sql`, then separately reviewed manual OWNER
`schema-v3-effective.sql`. v3 works on v1 or v2 without replacing their checks.
It adds ONLY nullable `effective_format` and `effective_dry_run`. Both NULL means
unresolved/unestablished; old rows remain NULL, with no backfill. Typed route
selection supplies a coherent tuple once (SQL records, SQL NDJSON non-dry-run,
explain records planning, or shard Arrow non-dry-run); it cannot be rebound or
cleared. Receipt `format`/`dry_run` retain their original immutable legacy
request-classification meaning (already normalized for local batch/dry-run),
**not a guarantee of effective coordinator execution**. Priority retains the
actual requested coordinator/local resource tier; worker HTTP priority defaults
remain as before. No generalized metadata blob, parent ID, principal authority,
reader grant, retention change or automatic/runtime DDL was introduced.

Effective selection queues only fixed enums/boolean through the existing
100ms/1000-entry/8MiB/30s owned snapshot path; it adds no blocking await inside
query deadlines or between durable job INSERT and owned-task spawn. Every later
snapshot retains the tuple; an older in-flight retry cannot erase it. Outage,
contention or missing-v3 writes can lose effective evidence or leave the original
row pending/indeterminate, never invent another receipt or change query/job
availability. Buffered terminals still mean handler completion; direct shard
Arrow is collected and encoded before response construction, NOT a live Arrow
source. Actual coordinated NDJSON is also already buffered: its original
payload moves, without copying/parsing, into the existing owned body source for
EOF/abandonment evidence. No new distributed/format/job capability is added.

Internal shard requests authenticate with the existing service credential and
remain unobserved publicly, including self-shard failover. No unsigned parent or
coordinator indicator, forwarded header, JWT bytes or body tenant supplies
principal authority. No new correlation wire field is needed. Direct external
shards use their existing signed effective tenant and produce their own
shard/Arrow observation, never a fake records label. Their disabled-reservation
execution boundary is actual collect polling. Coordinated parents are admitted
at the original successful reservation/disabled-admission boundary; subsequent
worker refusals/errors are accepted failures, not pre-admission rejections.

Remote `x-siglake-scan` is existing partial diagnostic attribution, not complete
row/byte scan evidence across workers, retries and coordinator metadata/WAL work.
Consequently new coordinated history scan measurements stay NULL, even when
remote fields are present/nonzero. Typed returned rows/truncation and complete
heuristic estimates remain separately available. Existing diagnostic aggregation,
response costs, admission estimates, fallback, cancellation and timeout policy
are unchanged. Direct shard settled instrumented runtime counters and genuinely
local delegated measurements retain the accepted provenance requirements.

**Hard consumer/rollout gate:** manual migration requires recreating reader/
service pools (cached PostgreSQL `SELECT *` result shapes cannot change in place).
The engine safe reader handles v1/v2 as effective NULL and projects v3 fields on
new connections, under unchanged RLS/grants. The current console reader/JSON/UI
still needs a separately reviewed compatibility adapter before presenting new
variants: it must NOT show receipt hints as effective execution. No console edit,
production startup activation, production role/DDL/TLS change or deployment
is included; local standalone configuration is documented above. Earlier immutable buffered E2E artifacts remain their original
proof, not v3/coordination proof.

### Configured integration proof and exact limits

The expanded `scripts/check-query-history-batch.sh` owns real PG16.14 v1/v2/v3
fixtures and runs the production-router `distributed_history` tests together
with all retained routed batch/local tests. Two production coordinator/worker
routers use independent real engine contexts over one synthetic temporary
warehouse, a local RS256 issuer and the existing service credential trust. Tests
check fanout success, one public UUID/no internal duplicates, renewed client
stability, distinct clients/tenants/reader isolation, unsigned parent/header/body
spoofs, direct external Arrow and internal non-observation, actual explicit
dry-run execution versus delegated planning, batch-tier NDJSON versus actual
records jobs, configured local cache/metadata/discovery fallback, real worker
403/500, pre-body transparent timeout, actual owned-worker listener shutdown
and self-shard fallback, NDJSON EOF and unpolled/partly-polled in-process body
drop, queued delegated-job cancellation and real history-table-lock recovery
with original UUID/effective tuple. The listener shutdown is intentionally
induced connection refusal, not an external network-outage claim. Body drop is
server-observed abandonment, NOT controlled TCP disconnect/cancel acknowledgement.

v3 tests migrate real original observations on both v1 and v2, preserve every
prior constraint/receipt/outcome and NULL old effective fields, recreate the
reader, reject repeated migration and conflicting/cleared selection, and retain
tenant RLS. A queue unit regression separately checks older retry coalescing
and exact serialized-byte accounting. Retained local stream/job suites cover
their detailed lifecycle races; this is not proof of remote per-worker metrics,
mid-body timeout, true process-kill/commit-packet faults, arbitrary peer versions,
DNS discovery operation, production roles/TLS/erasure, full CI, console/MCP chain
or complete distributed/history coverage. Existing raw audit/job/parser logging
remains unsanitized outside the new-store guarantee.

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
as described below. Configured sources now use the same original owner and
separately selected v3 effective semantics. Actual local batch transfers the same execution UUID through the separate lifecycle below;
202/queued is not terminal success. Coordinator fanout must not produce duplicate
public parents or trust client-provided parent IDs. These changes require branch
and cancellation tests, and an explicitly reviewed legacy raw-audit/log cutover.

Coverage remains PARTIAL. The deferred matrix above is not inferred from seeded
history events, and no full SQL-history, production activation, erasure,
complete-roadmap, MCP-subprocess or console-chain claim is made.

## Local NDJSON ownership and typed evidence

Only positively selected named-tenant external requests are included. Explicit
`/sql/local` remains local even with peers configured. Configured delegation uses
this same source owner; actual coordinator NDJSON uses the already-rendered
payload body described below. Auth/extractor/default/shared paths remain
unobserved. Actual local jobs render records and have separate ownership below;
explicit coordinated batch-tier work is not necessarily a job. That earlier local-stream integration did not add startup configuration; the
separate local standalone owner is documented above.

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
NULL for all inputs without invoking any parser/tokenizer. The standalone owner
and dedicated fresh-connection boundary emit only fixed lifecycle diagnostics;
DTO/SQL/error bodies are not logged there. The separate SQLx notice limitation
is documented above. Existing engine audit/job/SQL logging is explicitly unsanitized and outside
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
original expiry/counter inspection, auth/extractor/default non-observation,
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
configured-peer local delegation, default exclusions, explicit no-membership rejection and owner shutdown with
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


## Local non-coordinator batch/job ownership (v2, explicit opt-in only)

The external observer starts before existing validation/admission and transfers
its sole handle after successful job INSERT. The batch reservation remains in
its existing place before enqueue and is **not** execution-admission evidence.
Admission is recorded only when the actual run future is first polled, after
its existing running-publication decision. A zero-budget run or a queued cancel
that never polls SQL stays not admitted. Existing syntax/read-only validation
may reject before queue; semantic planning remains on the dedicated runtime and
can fail after actual run admission. No planning or job behavior was moved.
Requested batch NDJSON still executes/renders records, as it always did; history
records the actual local-job format. Configured transparent `/sql` still takes
this same local batch path. Explicit distributed requests transfer here ONLY
when the existing routing decision actually delegates to local execution; a
coordinated batch-tier request can instead return buffered records/NDJSON with
no job. External direct shards return buffered Arrow even with batch priority;
internal coordinator identities and ambiguous/default routing remain unobserved.
Auth/extractor rejection remains outside observation.

The job store adds only nullable internal `history_execution_id UUID` through
its existing additive job migration seam. Old jobs remain NULL and unobserved,
with no backfill or recovery-time receipt. The field is absent from all public
request/response DTOs and no receipt type gains Deserialize. Only the controller
passes the newly minted observation UUID; headers/extra request fields never
supply it. Existing job raw query/result/error payloads and raw logging remain
**unsanitized and outside the new-history guarantee**; none are deleted, copied
into new history, or claimed sanitized. Existing job tenant authorization,
execution-owner leases, conditional writes, limits, scope and idempotence stay
unchanged. The job store's existing auto-migration privilege assumptions are not
history-role isolation proof or production rollout authorization.

**Separately reviewed OWNER provisioning dependency:** apply the unchanged v1
`history/schema.sql`, then `history/schema-v2-batch.sql` once. This manual atomic
v2 migration requires the exact validated v1 nonterminal-evidence constraint,
relaxes ONLY its `job_id IS NULL` clause, and preserves every numeric/terminal
invariant. A non-NULL job association requires SQL/records/batch/non-dry-run,
is unique per job, and cannot be rebound or cleared. Incompatible data/schema
or a repeated migration fails atomically without backfill/deletion/relabeling.
No runtime writer DDL, new history table, reader field or role/grant widening
was added. No production migration was executed. Existing console/buffered
immutable evidence remains **v1** proof, not retroactive v2 acceptance.
Missing-v2 association failure does not change 202 or local execution; durable
resume cannot guess an association that never bound.

Association is two trusted, non-atomic stores: job INSERT with its pointer,
then owned-task spawn before the handler's next await. Bounded history association
runs inside that task before engine polling. A 202 does not guarantee a history
binding while the owned task is still unpolled. A crash/outage between the two
stores can leave a partial link; it is not a durable outbox or exactly-once delivery.
Internal resume uses one bounded original-row SELECT and verifies both UUIDs,
exact original job tenant, SQL/records/batch semantics, original owner/receipt,
original 720-hour expiry and omission metadata. It never reconstructs context
from raw job SQL/results/errors or the current canceller, never resets expiry,
and never heartbeats an old history owner. Missing, expired, mismatched or
already-terminal rows do not mint replacements. Unknown admission after restart
stays unknown, not inferred from `started_at`, 202, or a reservation.

History terminals are selected only **after** authoritative persisted job
transitions, using the winning state (including an inline-body-cap failure),
never the executor's wished-for result. A local queued cancellation waits for
Abortable to drop the execution before claiming non-admission, closing the
admit/cancel race without changing abort/resource ownership. Repeated cancellation
retains its existing 409. Genuine remote cancellation can reconcile known original
admission; unknown original admission remains indeterminate. Failed job-store
terminal persistence drops into uncertainty; existing owner-local reconciliation
can later report the state it actually installed, not a discarded success.
Job-owner lease recovery may call the job failed but is **only indeterminate**
history evidence. Notifications share a bounded 100ms best-effort pass (at most
32 selected recovered links); omitted/lost evidence retains independent history
owner lease uncertainty. Startup job recovery before observer injection likewise
relies on that independent lease, not a fabricated failure. Genuine late terminal
evidence can reconcile before the original expiry.

Returned rows/truncation are copied only from the actual successful records
result; estimates require `estimate_complete`. Batch collection has no complete
settled scan-instrumentation snapshot, so measured rows/bytes stay NULL. Cache is
bypass. Original monotonic duration includes validation/queue/run to terminal
selection; resumed durations stay NULL (no recovery-time clock). Original event
wall times stay NULL. History terminal queuing never awaits DB inside the job's
conditional terminal transition; remote verification and association retain the
100ms budget. Existing 1000-entry/8MiB/30s volatile retry limits and safe Drop,
shutdown and observer/service split remain unchanged.

### Batch verification and explicit limits

`scripts/check-query-history-batch.sh` uses the same corrected private environment,
pinned native Rust1.95/PG16.14, offline locked two-job cache, socket-only disposable
PG and local RS256 issuer. The extra `qh_jobs` role and advisory/trigger gates
exist only in that disposable fixture, with no production test knobs. Actual
router tests cover pending 202, original UUID/pointer/header injection, renewed
and cross-tenant identity/access, original rather than canceller attribution,
prequeue syntax/admission rejection, actual run success/semantic-plan failure,
pre-run and in-run timeout, queued/running/repeated cancellation and persisted
cancel/completion ordering, history-lock/no-receipt faults, job-store privilege
refusal with unchanged response policy, original-UUID controller recreation,
lease uncertainty, legacy NULL pointers and genuine late terminal reconciliation.
The fixture gates use PostgreSQL advisory/row-lock queue evidence and explicit
worker release, not arbitrary sleeps. Existing memory and PostgreSQL ownership,
conditional-cancel/race/resource/finished-reconciliation regressions remain
separate required selections. v1->v2 migration is tested on real router-created
rows, including missing-v2 safe behavior and immutable/unique binding.

The additional internal PG resume/expiry test begins a real observation (no
seeded execution row) and uses the established privileged fixture-only clock
rewrite of its original row **and opaque DTO**, then purge/retry/recreation to
check no resurrection. This is not a real 30-day wait or exact microsecond-boundary
proof. Wrong job/tenant/pointer resume fails closed. Controller/service recreation
and lease recovery are **not SIGKILL/process-restart or commit-packet fault proof**.
The job-store privilege-refusal test is not a transport outage; the real history
lock and disconnected history-login tests are separately identified. No complete
history, production startup activation, complete distributed coverage, legacy logging
cutover, physical erasure, full CI, deploy, console/MCP chain or roadmap completion
is claimed. All prior rollout/TLS/backup/erasure gates remain unchanged.
