# Injection-only controlled jobs (LOCAL source, manual artifact)

`jobs.sql` is **manual, reviewed-owner, LOCAL-only**. It is deliberately absent
from `jobs.rs::SCHEMA_SQL_STATEMENTS`: no application migration, grants, startup
activation, credential provisioning or production rollout is supplied. Disabled
controls retain the existing store and route behavior. An injected controlled
submission with missing compatibility schema fails unavailable, never legacy.
The primary policy pool remains separate from the job pool.

## Composite compatibility invariant

At baseline `bf40676424f036fa7be1dedc258e2660b9c2ccd8`,
`submit_observed` assigns a unique submitting incarnation owner and `submit_batch`
spawns its process-local future. **There is no cross-process queue claim, SQL
handoff or replay scanner.** Recovery only terminal-fails orphaned rows. New
controlled submission atomically creates an immutable job discriminator and
private versioned context. Only that submitting owner spawns the controlled
future; its dequeue validates owner/context/version and reads the primary afresh.
No legacy worker obtains that future. Additions to this ownership topology MUST
re-evaluate the invariant.

The trigger rejects the EXACT baseline `SET_RUNNING_SQL`, including repeat
running writes, against controlled rows. This is a lifecycle compatibility
fence, NOT a sandbox or authorization against a compromised DB role/DDL/admin.
The transaction-local start GUC is not a secret. The new controlled start gate
fails closed on every store/publication error. In contrast, the baseline legacy
driver deliberately executes after a running-publication error: **trigger denial
alone cannot stop an arbitrary legacy execution future**. Do not describe it so.
No mixed-version enabled fleet is supported: old ingress could still admit its
OWN uncontrolled requests. Applicable ingress/worker upgrades and remaining
Jaeger/wire proofs precede any enablement.

## Lifetime and ownership

Only actual main/local durable selection uses operator-default handoff rather
than the run budget. Controlled explicit `/api/v1/sql/distributed` batch execution
(`priority=batch`, `dry_run=false`) is temporarily unavailable BEFORE work,
including Tier1 metadata execution and nondistributable fallback. That endpoint
never selects handoff from request flags: interactive/dry-run work retains its
original request clock. Actual selected-path support awaits the distributed/wire
slice; absent-authority explicit behavior is unchanged.

Request policy/history/rewrite/admission/store waits remain within that
original ingress handoff clock. Accepted queue+run timestamps, expiry and snapshot
are immutable; private binding contains only trusted tenant/issuer/client, never
sub/token. Public accepted/execution metadata has no binding or owner.

Queue/run anchors at final acceptance staging AFTER pool/INSERT lock waits,
before context+COMMIT, and clamps against original wall expiry. This is a
conservative pre-commit acceptance anchor, NOT an exact PostgreSQL commit
timestamp; commit/response latency cannot extend it. Zero can durably return 202 and immediately time out. Dequeue
includes history association, durable context validation, fresh primary read and
running publication. The accepted/current-policy/operator minimum can only
shorten expiry. Running work retains that admitted snapshot, independent of
caller `circuit_breakers:false`; optional scan breakers are unchanged.

A bounded authority registry (128 technical lifecycle slots, not an agent rate
or concurrency policy) retains server-runtime owners and their batch children.
Expiry can persist a terminal timeout before the execution future is polled.
Abort and timeout persistence run concurrently for queued unpolled children;
ownership is retained until their real joins are observed. Terminal CAS decides
cancel/timeout/success races, and bounded owner-local reconciliation never
replays SQL. Cleanup uses the existing 30-second terminal-write budget, mandatory
for controlled work. History remains partial/SQL-NULL/best effort, original
720-hour expiry and receipt/effective selection unchanged.

A body/SQL-free uncertainty ledger reserves one of 1024 technical slots BEFORE
writes. The server-runtime owner retains submission through bounded COMMIT/ACK
handling even if HTTP disappears. Known durable acceptance may execute despite
loss of its HTTP response; unknown/failed/timed-out acknowledgement MUST NOT
execute. The existing reconciliation cadence only conditionally fails such IDs
with the matching owner/discriminator. Unlike finished-result reconciliation,
ABSENCE never proves rollback: an uncertain ID stays reserved until an
authoritative terminal result or owner shutdown. Outage/late visibility has no
invented universal time bound; full capacity refuses before another write.
No INSERT, COMMIT or SQL is replayed. The existing reconciliation task is now
retained, cancellable and joined, without changing its runtime or cadence.

Async shutdown closes reservations, cancels owners, and observes retained joins;
cancelling a waiter leaves handles in place. Final synchronous Drop only requests
abort and remains **UNCONFIRMED**, not termination proof. Native CPU/memory work
and batch scheduling remain cooperative. No new runtime/thread/loop is added.
No claim of immediate OS termination, total-memory bound, production TLS/RDS/
Cognito proof, complete Controls, Jaeger/wire/UI coverage, or Investigations.
