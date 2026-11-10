# Controls5A: local policy-edit management contract

This is an **offline source/manual-schema contract**, not a customer route,
production migration, grant, purge execution, retention service, startup hook,
or UI/effective endpoint. No Investigations activation. The owner approved
**720 hours** of policy-edit retention and **same-org authorized human operator**
access. Historical design text marking those two choices unresolved is superseded;
it does not authorize broader erasure or production operations.

## Schema provenance and upgrade gate

`schema.sql` describes fresh manual provisioning of **DB layout v2**.
`schema-layout-v2-contract.json` is its reviewed, OID-independent PostgreSQL 16
catalog projection, produced with `integrity.sql` on an owned PG16.13 fixture.
Every reader/manager checkout compares the **entire exact projection**, including
all function bodies/owners/ACLs, columns/column ACLs, checks, indexes, RLS, triggers
and schema/table grants. It is never generated or accepted dynamically at runtime.
The former `schema-v1-contract.json` is replaced, not an alternate allowlist.
Per-policy `schema_version=1`, response DTO v1 and existing shard wire versions
are **unchanged**. Layout v2 only adds audit expiry/guards/read/purge contract.

Deployment is a separate gate: stop/coordinate old and new consumers; prove
primary routing, separate bounded manager/reader credentials, exact existing role
attributes/no memberships, TLS verify-full plus additive approved CA trust, no
unrelated/PUBLIC/default privileges, and backup/rollback/data ownership review.
`schema.sql` is **fresh provisioning only**, not idempotent, not an upgrade script.
It must not be applied over an existing schema, dropped/recreated to force a match,
or invoked by an application. Existing layout-v1 installations fail the new exact
contract until a separately reviewed manual upgrade is prepared. Existing edits
and their original timestamps require an explicit data disposition review before
that upgrade: no automatic expiry backfill, deletion, timestamp reset, inferred
binding, issuer lookup, or destructive migration is supplied/authorized here.
Never extend an old edit's lifetime to make an upgrade pass. No current production
data was inspected or changed. Pin source/schema/manifest together during that
future review; evidence manifests bind their Git objects and SHA256 hashes.

## Next Console consumer: portable small contract

Copy/port the small typed manager contract into the customer Console repository;
there is no local-path/Git dependency, published package or new service. Source
seams: `audit.rs`, `dto.rs`, `mod.rs`, `store.rs`, `config.rs`, `owned.rs`, `role.rs`,
`integrity.sql`, the layout manifest and this document. Do not copy runtime/job/
history authority. Keep the explicit primary configuration, redacted errors,
strict checkout verification and owned connection cancellation semantics.

Before **each** read/save/page, the Console must verify its current human session
and current DB same-org authorization, exact org-owned credential UUID and
provisioning metadata. For saves also require ready state and exact same-origin
Origin; revoked credentials may be retained read-only. Resolve canonical tenant,
exact verified issuer and exact client from trusted metadata only. Do not infer
from actor/attributes/hosted UI domains, normalize issuer, backfill legacy metadata,
look up a provider, exchange a token, retrieve hidden credentials or execute a
query. Missing trusted binding is unavailable, not inherited. UUIDs and a manager
role/GUC are not authorization; compromised manager isolation is not claimed.

Signatures (Rust public API):

- `ManagementBinding::from_trusted_metadata(RuntimeKey, org: Uuid, credential: Uuid)
  -> Result<ManagementBinding, PolicyError>`; runtime key uses trusted canonical
  tenant/exact issuer/exact client, never browser fields.
- `VerifiedHumanActor::from_trusted_metadata(subject: &str)
  -> Result<VerifiedHumanActor, PolicyError>`; verified human subject, 1..256
  UTF-8 bytes, no control characters. Not an email/display name/token.
- `AuthorizedHumanManagementContext::from_trusted_metadata(org: Uuid,
  actor: VerifiedHumanActor) -> Result<..., PolicyError>`; constructor validates
  shape only. It does not verify JWT/session/current DB authorization.
- `AuditPageRequest::new(limit: u16, before_revision: Option<i64>)
  -> Result<AuditPageRequest, PolicyError>`; limit **1..100**, cursor null or
  positive signed-64 revision. No offset, cutoff or caller clock.
- `PolicyManager::audit_page(&self, &ManagementBinding,
  &AuthorizedHumanManagementContext, AuditPageRequest)
  -> Result<AuditPage, PolicyError>`; mismatched context org is `Binding` before I/O.
- Existing `lookup(&ManagementBinding)` and atomic
  `save(&ManagementBinding, &VerifiedHumanActor, SaveRequest)` remain unchanged.

`AuditPage` serializes `{edits: [PolicyEdit], next_before_revision: null|i64}`.
Each `PolicyEdit` has `schema_version: i16=1`, `revision: positive i64`,
`actor_sub: String`, `edited_at`/`expires_at` UTC timestamps and
`old_settings`/`new_settings`, each `{timeout_seconds: null|positive i64,
scan_bytes_warning: null|positive i64}`. Timeout max is `i64::MAX/1000` seconds;
warning max is `i64::MAX`. There are no extension maps/free-text event fields.
The response is scoped by the separately authorized binding, not a user-selected
principal map. Human subjects/pages must not be logged; Debug is redacted.
Use private/no-store responses and a bounded HTTP rendering/body contract later.
No management audit endpoint for an agent bearer or engine reader is supplied.

SQL interface, on the **same** dedicated bounded-primary manager connection and
transaction: `BEGIN ... READ ONLY`; transaction-local
`set_config('query_policy.tenant', canonical_tenant, true)` plus bounded statement
timeout; then `query_policy.read_edits(text tenant, text issuer, text client,
uuid org, uuid credential, bigint before_revision, integer limit)`; commit or
rollback before reuse. It returns exactly the nine scalar edit columns named in
`schema.sql`/`store.rs`. Never set tenant on another socket or at session scope.
Cancellation/error discards incomplete owned connections; no app-owner/history
pool injection, cache, read-replica or fallback is permitted.

The SECURITY DEFINER read function has fixed `search_path=pg_catalog`, qualified
objects, no dynamic SQL, explicit tenant/key/UUID checks and FORCE RLS. Only
`qp_manager` (and trusted owner) can execute; manager has **no direct edits SELECT
or INSERT/UPDATE/DELETE, no DDL, ownership, memberships or bypass RLS**. Reader
can only SELECT active policy; history roles have no new privileges. Manager's
exact two executable functions are save and read_edits, not trigger/purge helpers.

Order is revision DESC, exclusive `< before_revision`. Full pages return the last
revision as next cursor; a later page may be empty. New successful saves land above
the cursor, never shift/duplicate older pages. Each page freshly excludes
`expires_at <= statement_timestamp()` by **DB time**; this is not a snapshot/export
or a completeness promise across concurrent expiry/purge. Bad arguments are
`Invalid`; wrong extant binding is `Binding`; schema/role rejection is `Unsafe`;
other database errors are redacted `Unavailable`, never raw SQL/errors. A save
whose COMMIT acknowledgement is lost remains outcome-unknown: don't auto-retry or
claim rollback/success. Existing CAS conflict is `Conflict`.

## Retention and active policy

Only successful saves create edits. INSERT's DB trigger assigns original
`clock_timestamp()` and `expires_at = edited_at + interval '720 hours'`; the exact
720-hour difference is constrained. UPDATE of **any** edit field is forbidden,
including timestamp/key/schema/actor/settings. Reads, reset, CAS failure and purge
do not renew an original timestamp or expiry. Reset is another all-NULL policy
revision/edit, not deletion. Save and audit INSERT remain a single transaction:
audit failure rolls back the settings/revision, including first save.

The immutable `(tenant, org UUID, credential UUID, revision)` edit is bound to the
immutable active policy's exact issuer/client via read join; no duplicated guessed
issuer/client or foreign-store FK is needed. Policy binding remains immutable.
Active policies have **no TTL** and are never deleted by audit cleanup. After all
edits disappear a numeric policy is still readable/enforceable; an all-NULL policy
still inherits operator limits with its known retained revision. Neither is an
absent-policy lookup. No query SQL/parameters/results/tokens/secrets/raw errors,
form bodies, emails/display names, Activity events or usage ledger are recorded.

## Manual owner-only operation (NOT executed in production)

Only a reviewed trusted-owner operator outside the application may use the
existing `qp_owner NOLOGIN`, with no membership/bypass RLS/app credential added.
How the deployment's trusted admin assumes that role is a future operational
authorization gate, not a new application role. Do not grant the function to
PUBLIC, manager, reader or history roles.

After validating the deployed exact contract and selecting one authorized tenant,
within one bounded maintenance transaction/connection:

```sql
BEGIN;
SET LOCAL ROLE qp_owner;
SET LOCAL statement_timeout = '5s'; -- example maintenance bound, not policy TTL
SELECT pg_catalog.set_config('query_policy.tenant', '<reviewed-tenant>', true);
SELECT query_policy.purge_expired_edits('<reviewed-tenant>', 100);
COMMIT;
```

The invoker-only function accepts only tenant plus **1..1000** row batch limit;
there is no cutoff argument. It requires current_user qp_owner, exact transaction
tenant and FORCE RLS; selects expired edits by DB statement time with stable expiry/
UUID/revision ordering, `FOR UPDATE SKIP LOCKED`, and deletes at most that batch.
Only edits are touched, never policies/history. Guard permits owner DELETE only
for already-expired edits in that tenant and rejects unexpired DELETE or every
UPDATE; expiry cannot be changed to force purge. Trusted DB owner can alter DDL
(including triggers/TRUNCATE): no defense against compromised owner is claimed.
Concurrent locks can yield a short/empty batch without proving all expired rows
are gone. Roll back on error; finish before any pooled reuse. No scheduler,
startup purge, service, provisioner or application owner credential is provided.
Production retention-job/provisioning/monitoring remain rollout gates.

Logical read exclusion/DELETE is **not secure physical erasure**. No claim covers
backups, WAL, MVCC, snapshots, existing exported pages, replicas or OS termination.
Owned local fixtures use PG16.13 (socket calls generally non-TLS); synthetic native
TLS tests are additive trust regressions, not RDS/provider/two-version qualification.
History 30-day raw-SQL/receipt TTLs/roles/checker and actual routing/jobs remain
unchanged. Historical 61/62 terminal cause stays UNKNOWN; corrected accepted A/B
oracles are not retrospective cause proof. Later UI labels must preserve the
receipt-format normalization caveat (batch/dry-run already normalize to records;
raw HTTP format is not losslessly retained).
