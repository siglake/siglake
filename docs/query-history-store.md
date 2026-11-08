# Query history storage/privacy foundations (explicit opt-in only)

The [internal runtime core](query-history-runtime.md) now wraps these primitives
when explicitly constructed or explicitly enabled through the local standalone
collector; the storage/reader contract
below remains the foundation, with separately versioned v2/v3 dependencies.
Runtime scheduling, deadlines, retries and health are defined there, not in the
bare store methods. Optional application-state injection supports only the
explicitly documented route matrix; the dedicated standalone startup contract
is documented in the runtime document. Default remains disabled.

`siglake_query_server::history` exports internal Rust primitives. The standalone collector has a dedicated restricted environment resolver,
not an application/catalog/jobs pool resolver. No runtime migration, silent
legacy audit replacement or authentication change is introduced. No console/public event-ingestion API.
The existing raw Iceberg audit and persistent jobs are untouched and are **not**
privacy-compliant history projections.

## Manual migration and production provisioning gate

`crates/siglake-query-server/src/history/schema.sql` is a one-time, reviewed
administrator migration on the existing UTF-8 catalog PostgreSQL database. It
explicitly `SET LOCAL ROLE qh_owner`, creates the engine-owned `query_history`
schema, and grants the documented privileges. It does not create logins, change
production grants automatically, or run in `HistoryWriter::connect`. It has only
been applied to fresh disposable local fixtures.

### Separately versioned metadata dependencies

v1 is unchanged. Local jobs require the separate manual OWNER
`schema-v2-batch.sql`. Configured peer/distributed/direct external shard
observation additionally requires manual OWNER `schema-v3-effective.sql`, on v1
or v2. Standalone activation always requires verified v3, even without peers;
it does not change accepted local-route effective-NULL semantics. v3 adds only nullable `effective_format`/`effective_dry_run`, a validated
coherent-tuple check and immutable-once-selected trigger. It does not replace
v1/v2 constraints, widen grants, backfill old rows or modify original receipts,
terminals, RLS, row bounds or expiry. Both NULL is unresolved, not records/false.
Legacy immutable receipt `format`/`dry_run` are request classification/hints,
not effective execution guarantees for the existing explicit coordinator API.
See the runtime document's prominent pre-existing dry-run/batch discrepancy.

Migration requires reader/service pool recreation because existing PostgreSQL
cached `SELECT *` plans cannot change result shape. A new engine reader projects
optional v3 fields (NULL on old schema/rows). The console reader/JSON/UI needs a
separately reviewed compatibility adapter before displaying these new variants;
it must not label request hints as effective execution. This is a hard consumer/
rollout gate, not authorization to alter production roles or apply DDL.

Before any production integration, separately provision these fixed, distinct
roles (names are a contract, not proof of isolation):

* `qh_owner`: non-runtime, NOLOGIN migration owner, granted schema-creation rights
  by the reviewed administrator as needed. Runtime roles cannot assume it.
* `qh_writer`: non-owner LOGIN; SELECT/INSERT/UPDATE/DELETE on executions,
  coverage, owners; USAGE on query_history; no DDL.
* `qh_reader`: separate non-owner LOGIN/pool; SELECT only on executions/coverage;
  USAGE on query_history; no owners/catalog/secrets table access or writes.

Writer/reader must be NOSUPERUSER, NOBYPASSRLS, NOCREATEDB, NOCREATEROLE,
NOREPLICATION, own no objects, have **no role memberships**, including NOINHERIT
memberships that allow SET ROLE, and have no CREATE/TEMP database or schema
creation rights. No executable application functions or access to other
application relations may be available through direct or PUBLIC privileges,
including column-only SELECT/INSERT/UPDATE/REFERENCES grants. Only literal `pg_`
system-schema prefixes and `information_schema` are excluded from these audits;
ordinary application schemas such as `pgapp` are checked.
The preflight rejects completed false contracts on every new connection;
dedicated operational failures executing role/schema checks are unavailable, not
contract rejection. The standalone writer uses the bounded owned-connection
backend documented in the runtime guide, without SQLx pool release/close futures; reader operations
also recheck their transaction. System catalog introspection/built-in functions
remain PostgreSQL capabilities; "no catalog access" here means application
catalog data, not an impossible claim to hide PostgreSQL system catalogs.

This deliberately conservative fixed-role contract can reject a normal catalog
DB's default PUBLIC TEMP/function privileges. A reviewed production provisioning
plan must resolve those grants and compatibility with existing clients; code
never revokes them on a running service. Cross-database privileges, server
configuration, administrator trust and subsequent grant/policy drift remain
operator security responsibilities. Do not point a normal console/catalog pool
at these APIs. No role switching is used by the reader.

Both tenant-readable tables ENABLE and FORCE RLS. Reader policies require
`tenant = nullif(current_setting('query_history.tenant', true), '')` and unexpired
rows. `list`/`detail` validate the existing shared tenant semantics, begin a
transaction, parameterize transaction-local `set_config(..., true)`, issue fixed
parameterized SQL (including explicit tenant/expiry predicates), and commit.
Drop/cancellation rolls back before pooled reuse. Missing context returns no
rows even without WHERE. Global owners have no reader grant.

**RLS is not session authorization**: a reader can choose tenant context. Future
console code must first authorize the organization under its session and only
then supply that tenant; no caller SQL/cursor/header may establish authority.

Dedicated error-return/release/close safety is **not** a guarantee for arbitrary
upstream messages. SQLx0.8.6 `connection/stream.rs` logs `NoticeResponse.message()`
under `sqlx::postgres::notice`, independently of statement logging. The local
standalone slice trusts the DB/admin and reviewed fixed schema; arbitrary
NOTICE/INFO and production logging/cutover remain outside its confidentiality
claim. No global suppression, dependency patch or new protocol layer is used.

## Event and store contract

* `HistoryWriter::receipt` obtains trusted DB `clock_timestamp()` and exactly
  `interval '720 hours'` expiry, with a fresh UUIDv4. Keep this **original** opaque
  receipt across every retry. No public UUID/timestamp re-admission constructor
  or Deserialize exists. `PendingExecution::new` accepts only a redactor-produced
  `RedactedSql`, bounded `Principal::from_caller`, typed request metadata and an
  owner UUID. It never copies subject, email, bearer, parameters or results.
* The existing verified `CallerIdentity` is a trusted in-process boundary, not
  external event input. No new auth behavior is introduced. Provider issuer and
  client form a nullable pair; coordinator/unknown identities are not fabricated.
* SQL is always NULL; issuer <=2048 UTF-8 bytes, client <=128, shared tenant
  validator, nonnil UUIDs, and bounded 8 KiB serialized payload are checked. SQL checks,
  immutable-receipt/terminal triggers and whole-row JSON-size checks defend the
  schema too. Counters are optional checked i64-range nonnegative `Counter`s;
  unknown is NULL. Estimates are never copied into measured counters. No false
  cross-counter inequalities (joins/aggregations can change row counts).
* Safe error/reason/outcome/cache/format/operation/priority values are fixed enums.
  `HistoryError` intentionally discards SQL/parser/connection upstream text and
  sources. The history SQL omission path never invokes parser/tokenizer code
  (whose dependency diagnostics can expose SQL/AST/tokens), and emits no logs.
  This is not a platform-wide logging/privacy claim: existing engine parsing and
  raw audit remain untouched. DTOs containing SQL/caller input are neither
  Deserialize nor Debug. Stored rows serialize through a typed, read-only projection, not an
  arbitrary JSON bag.
* `persist_pending` is UUID-idempotent and compares immutable metadata. Identical
  initial retries do not revert a terminal. `finish` conditionally accepts one
  terminal payload, permits identical retries, rejects conflicts (including
  concurrent success/cancel), and can reconcile indeterminate rows with genuine
  terminal evidence. Admission is not derived from HTTP status or a DB ack.
* `TerminalExecution` requires explicit trusted `AdmissionEvidence::Admitted` or
  `NotAdmitted`; `Unknown` cannot construct a terminal event. Success requires
  Admitted, rejection requires NotAdmitted. Pending/indeterminate admission is
  nullable, so unknown is not silently false. `admit` records explicit backend
  evidence; `finish_pending` can insert a previously missing initial observation
  and terminal with the **same original receipt**, including admitted success
  after a persistence outage. Neither helper creates a replacement receipt.
* Event time is distinct from persistence time. This slice does not accept
  original lifecycle wall-clock times: `admitted_at`/`finished_at` remain NULL
  when unknown; DB `admission_observed_at`/`finished_observed_at` record evidence
  persistence, **not original execution times**. Measured monotonic duration is
  separately nullable. Retries preserve observed timestamps and expiry.
* `heartbeat`/`mark_indeterminate` are explicit primitives only; the internal
  runtime core now owns bounded lease recovery and scheduling when constructed.
  Routing ownership remains later integration work. Recovery does not
  pretend that a dead lease means execution failed/stopped. Coverage contains
  only tenant/bounded gap reason/window/expiry columns; there is no general
  telemetry service or collector here.

If the history DB is down **before any trusted receipt exists**, the internal
runtime core preserves query availability with a disabled/unobserved handle,
reports known loss signals when possible, and never invents/backdates a recovery
receipt or silently resets the 30-day clock. This slice provides no local-time fallback. Store calls themselves
are not execution-availability policy: per-write 100ms deadlines, bounded retry
queues/loss metrics and cancellation/stream/batch lifecycle ownership are not
wired or claimed here. An unavailable store is an error, not an empty list.

## Metadata-only SQL availability, version 1

**All history SQL is unavailable.** `RedactedSql::parse` retains its API name but
never invokes a parser or tokenizer and never stores input. For all inputs up to
64 KiB it returns SQL NULL and fixed status `omitted`; larger inputs return SQL
NULL and the existing fixed status `input_limit`. Version remains 1. Valid,
unsupported, malformed and empty inputs have the same omission behavior. The
manual schema permits only these statuses and requires SQL NULL.

This is the conservative approved omit-on-uncertainty fallback, **not a claim
that canonical SQL rendering works**. The locked parser can emit original SQL,
ASTs and tokens through global `log` diagnostics at DEBUG, so even parsing input
that would later be omitted is unsafe for this history path. The previous
renderer has been removed. There is no partial/raw SQL, format, fingerprint or
regex fallback; no arbitrary-string constructor, Deserialize or input Debug.
Typed metadata and safe serialization remain available. A genuinely silent
parsing path and any future SQL availability require separate privacy review.
No logging bridge is disabled and no application-wide log suppression is used.
Existing engine/audit parsing is outside this narrow history API guarantee.

## Expiry, primary purge, and unapproved rollout work

`expires_at = received_at + 30*24h` is immutable across retries/recovery. Every
read/detail primitive and reader RLS hides expiry at `expires_at <=
clock_timestamp()` before cleanup. Already-expired original receipts cannot be
reinserted even after primary DELETE. These are in-process opaque receipts, not
a new import/replay protocol; do not add deserialization without re-review.

`purge_expired(batch)` accepts 1..1000, uses a transaction advisory lock plus
ordered `FOR UPDATE SKIP LOCKED`, expiry indexes and a stable DB statement-time
cutoff usable by the index. It deletes bounded batches of expired executions,
coverage and stale unreferenced owners atomically. Failure rolls back, later
explicit calls catch up. Return count is executions deleted, not total metadata
or a guarantee that cleanup finished. **No application/production sweeper is running today.** The internal runtime
core provides startup catch-up, ~60s scheduling, health/readiness/overdue-age
reporting and bounded deadlines only when explicitly constructed, currently in
local tests or an explicitly enabled standalone collector. No production
activation is claimed.

This deletes primary rows only, not secure disk erasure, WAL, long-lived MVCC
snapshots, replicas, backups, final snapshots or historical raw audit/job copies.
Backup/snapshot/legacy retention policy and verified restore/deletion behavior
remain a hard rollout/product-claim gate, with no approved exception. No historic
raw audit data was deleted. Console issuer binding/mapping, org lifecycle,
authorization/API/cursors/UI, all execution-route integration, future raw-audit
cutover and raw logging inventory, and signed-issuer/HTTP/MCP/browser E2E remain
unimplemented. Auth profile hardening has not been deployed.

## Local verification

Run `scripts/check-query-history.sh` with **no arguments**. It clears ambient env,
uses privately pinned cargo/rustc/clippy 1.95, the already-cached PostgreSQL 16.14
binaries, creates its own fresh socket-only cluster/database/distinct roles,
applies the migration only there, and trap-cleans only its own fixture. No
DATABASE_URL, production target argument, Docker, downloads of images or cloud
calls. The fixture marker and fixed database/role names guard ignored DB tests.

The runner uses fresh private HOME/XDG/provider paths, explicit empty 0600
PGPASSFILE and private HOME/.pgpass (including SQLx's fallback), cleared inherited
PG defaults followed only by fixed synthetic socket/database settings, empty
provider files and disabled AWS metadata discovery. Fixture construction uses
`new_without_pgpass` plus explicit settings and checks isolation first; that
constructor alone would NOT isolate PG environment defaults. The internal API
accepts trusted already-constructed options and does not vouch for their
provenance. Isolated Cargo state links only known registry caches for locked
offline builds, never original Cargo config/credentials or HOME. Cargo launches
from a private temporary cwd with explicit manifest; real HOME/worktree ancestor
configuration is avoided, but root-/tmp-level configuration remains a trusted host
assumption, not a filesystem sandbox. Earlier runs
that retained real HOME are trust-limited; corrected full selected reruns are
required. No actual passfile/provider configuration was inspected.

The runner also verifies a persisted synthetic row across an actual PostgreSQL
server restart with fsync enabled (not a power-loss/backup-restore test).
With the same pinned environment, `cargo test --locked -p siglake-query-server
--doc history::redaction` also checks the compile-fail API boundaries.
Tests cover durable store-restart reads, initial/terminal ambiguous-ack retries,
conflicting and concurrent finish, uncertain recovery, explicit admitted-success
recovery after write outage, honest unknown times, expiry during outage,
physical primary deletion, DELETE failure/rollback/catch-up, bounded competing
sweepers, owner/coverage cleanup, FORCE RLS, missing WHERE/context, pooled A/B
concurrency, cancellation/rollback cleanup,
reader denial of writes/catalog/secrets/owners/role assumption and privileged
role/membership/grant rejection, column-only ACLs and ordinary `pgapp` schema
privileges, including already-connected reader transaction rechecks. Expiry
tests exercise live/expired rows and physical purge, but the exact cutoff
minus/at/plus one-microsecond behavior of reader/RLS/purge is **unverified**; the
old arithmetic self-comparison was removed rather than advertised as coverage.
Production predicates remain unchanged; no caller-controlled clock was added.
Privacy tests include generated metadata noninterference, original identifier/
value/dollar-string/comment sentinels, malformed/deep/oversized/unsupported SQL,
DB/event/read serialization and fixed errors. A separate test binary captures
real global `log` and tracing output at maximal TRACE (including DEBUG), proves
intentional control logs are captured, then requires zero omission-path logs
for previously supported input, unsupported valid input and tokenizable parse
errors. Compile-fail tests protect constructor/Deserialize/Debug boundaries.
These are storage tests, not executed-query lifecycle/E2E coverage or production
provider validation.

## Optional v2 queued-job association (manual owner migration)

`src/history/schema.sql` remains the immutable v1 baseline. The separately
reviewed `schema-v2-batch.sql` owner migration permits a nonterminal `job_id`,
while preserving all other nonterminal evidence restrictions. It constrains
non-NULL associations to SQL/records/batch/non-dry-run, makes each job association
unique and immutable, and fails atomically on incompatible v1 constraints/data.
It changes no reader fields, RLS or grants. No history runtime runs this DDL and
no production provisioning/rollout is authorized. See the runtime document for
two-store partial-link, original-receipt recovery and verification limitations.
