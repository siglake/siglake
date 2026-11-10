# Query policy v1: OFFLINE authority only

Controls5A adds the [management edit-audit/read/purge contract](../crates/siglake-query-server/src/query_controls/audit-contract.md).
It supersedes the original ledger's retention/access and DB-layout statements;
policy/wire schema_version remains 1. This is local source/manual-schema only.

This document describes the accepted offline store contract. It does **not**
authorize runtime activation or production migration. Subsequent injection-only
buffered request work is documented in [query-controls-buffered.md](query-controls-buffered.md);
body/jobs/Jaeger/Arrow/wire/skew and presentation gates remain. Saved settings are
not effective limits. Nothing here claims a deployed numerical default.

## Reviewed manual owner artifacts

`crates/siglake-query-server/src/query_controls/schema.sql` is a manual schema,
never run by the library. Provision distinct `qp_owner` (NOLOGIN), `qp_reader` and
`qp_manager` (LOGIN) identities, all NOINHERIT, NOSUPERUSER, NOBYPASSRLS,
NOCREATEDB, NOCREATEROLE, NOREPLICATION, with **no inbound/outbound memberships**. Runtime roles
own nothing and cannot CREATE/TEMP, write tables directly, or execute unrelated
application functions (including PUBLIC grants). The owner needs CREATE only for
the reviewed manual schema installation; revoke it afterward. No policy grants
are added to history, application, catalog or job roles. Database setup and grants
are production approval gates, not engine startup behavior.

FORCE RLS protects both tables; tenant GUCs are transaction-local. Every read uses
exact tenant/verified issuer/verified client predicates with byte-exact `C`
collation (also checked in the manifest). Reader selects policy only;
manager selects policy and executes atomic save plus bounded exact-binding
`read_edits`, without direct audit-table access. Owner-only manual expired-edit
purge is not a manager capability.
The definer has a fixed `pg_catalog` search path, qualified policy objects, no
dynamic SQL, explicit tenant/binding/value validation, and revoked PUBLIC EXECUTE.
A trigger prevents key/binding/schema remapping and nonsequential revisions.

Expected revision zero inserts once. Existing saves lock/check/increment exactly;
concurrent or stale revisions conflict. NULL inherits, never means unlimited;
all-NULL reset saves a new revision, never deletes. Immutable runtime and
org/credential UUID bindings have separate uniqueness; there is no cross-database
FK, cascade, rebinding or credential lifecycle automation. Edits contain only safe
numeric old/new values, tenant/org/credential references, version/revision,
original DB timestamp, coherent immutable expiry and verified human actor
(256-byte storage bound).

**Approved edit retention is exactly 720 hours, with same-org authorized human
operator reads. Active policy survives audit expiry/purge.** Query history's 30
days is NOT authority for this audit. The bounded store page is not an HTTP API
or human-auth implementation. Production migration/grants/job/purge authorization
and broader erasure remain separate gates; no automatic migration/backfill or
physical erasure is supplied. See the linked Controls5A contract for prerequisites.

## Explicit configuration and owned lifecycle

`PolicyConfig::reader` and `::manager` accept explicit role-specific URIs only.
There is no environment activation/help flag or pool injection. No history,
catalog, jobs, app, libpq/PG, passfile or provider credential fallback is accepted.
Constructors overwrite inherited PG fields and reject inherited options/TLS file
paths even when empty or non-Unicode; SQLx URI parsing and passfile IO are avoided.
TCP requires nonempty explicit password, explicit port/database, and `verify-full`.
TLS defaults to bundled **WebPKI** roots; arbitrary custom/private-CA inputs remain unsupported.
An explicit named reviewed public RDS root addition is available as described below.
The existing SQLx dependency/features are unchanged. Empty-password, SSL-disabled
Unix sockets are supported for explicitly targeted local fixtures only.

Caller-supplied connection bounds are technical pool limits, **not agent concurrency
policy**: 1–16 sockets and a nonzero total operation budget at most 30 seconds.
No product timeout/default is selected. Each reader/manager owns separate raw
connections and a semaphore, independently of history's owned backend. Connect
and every checkout check current **and session** identity, role flags/memberships,
ownership, database/schema/table/**column**/function effective grants, and exact
schema/function/RLS integrity. Recovery-mode standby connections and disabled
row-security/replica-trigger session settings are rejected; the operator must
still supply a trusted primary target (no discovery or logical-replica attestation). Only literal `pg_` system schemas and
`information_schema` are excluded from unrelated-object privilege checks, not an
SQL wildcard prefix. Unsafe roles/schema fail closed.

`integrity.sql` projects an OID-independent fixed policy-object manifest;
`schema-layout-v2-contract.json` is the fixed DB-layout-v2 reference generated on
owned PostgreSQL 16.13 for Controls5A, not a learned runtime baseline. Per-policy
and response schema_version remains 1; existing layouts require a separate
reviewed manual upgrade, not runtime migration. It covers owner/ACLs, columns/defaults, constraints,
indexes, triggers, rules, RLS policies and full function definitions/configuration.
Compatibility with different server catalog rendering requires review; a mismatch
is unavailable/unsafe, never a reason to skip checking or inherit. The manifest
can be independently reproduced by applying the manual schema in a **new owned
disposable** PG fixture and comparing `psql -XAt -f integrity.sql` JSON to the file.
Do not regenerate it from a live runtime database or automatically accept tampering.

All checkout/read/save/commit/flush work stays within the caller's operation
budget. Operations use borrowed SQLx `begin_with` transactions with explicit
READ COMMITTED isolation and commit/rollback, not untracked raw SQL BEGIN.
Transaction drop only queues rollback on the owned connection; cancelled leases
discard that socket before it can be reused. Transactions also set a local server
statement timeout as a cancellation cleanup backstop; it is not an agent limit. Completed operations flush, ensure no
unfinished transaction, and reuse sockets. Cancellation, protocol uncertainty,
failed cleanup, and unfinished transactions synchronously discard sockets before
releasing permits. Close/Drop stops new admissions and drops idle sockets;
outstanding leases finish/discard within their owned operation budget. There are
no detached SQLx pool-return/close-error tasks, retries or forced per-query churn.
Drop does not claim instantaneous server execution termination; the server timeout
is a backstop. There is no background task owned by this library.

Errors and Debug expose fixed text, never SQLx upstream bodies or URI credentials.
This is **not universal PostgreSQL diagnostic confidentiality**: arbitrary SQLx
NOTICE/INFO remains excluded under the trusted DB/admin/reviewed-schema assumption.
No global logging filter, dependency patch or feature change is made.

## API trust and failure semantics

`RuntimeKey`, `ManagementBinding` and `VerifiedHumanActor` are deliberately not
request-deserializable. Their `from_trusted_metadata` constructors validate storage
bounds, not authentication. Offline values are fixtures/already-trusted metadata.
Injected runtime resolution derives keys from verified `CallerIdentity`/principal
exact tenant, issuer and client, never request headers/JSON, `sub`, display name,
JWT bytes or history. Customer saves still require session, exact Origin, org/credential UUID,
ready lifecycle and trusted provisioning binding authorization. Manager access to
a tenant GUC is **not isolation from a compromised manager application**.

Reader lookup is independent on every call; no policy/negative cache or replica
fallback. Only `Ok(Absent)` means successful primary absence. `Found` retains
revision even for all-NULL settings. Transport, timeout, missing/unsafe schema or
role errors never inherit. Manager returns saved configuration, not effective
controls.

The approved finite metadata matrix separates configuration revision from override
presence; `PolicyLookup::state()`/`revision()` classify successful reads only:

| Primary outcome | Policy state | Per-agent revision |
|---|---|---|
| Successful Absent | inherited | NULL |
| Successful Found, all overrides NULL (explicit inherit/reset) | inherited | stored positive revision |
| Successful Found, any configured override, including warning-only | applied | stored positive revision |
| Unresolved/unavailable | unavailable | NULL; no fabricated effective policy claim |

Operator clamping does not change the configured-override classification even if
the resulting effective bound is unchanged. Generic/unbound operator-only
enforcement has no per-agent revision. The encoder accepts inherited plus a known
positive revision, requires a revision for applied, and forbids one for unavailable.
No all-NULL row is labelled applied merely because it was Found. This is pure
metadata classification, not a runtime resolver or enforcement activation.

CAS/binding/invalid errors are fixed enums; generic failures are
`Unavailable`. Save and edit audit are one transaction. Audit INSERT failure
rolls back its policy mutation. Network/COMMIT acknowledgement loss can mean
**unknown outcome**: never display success or promise rollback, and never retry
blindly. Re-read the primary under the already-authorized binding to reconcile.

`SaveRequest::parse` enforces ≤4096 bytes, exactly the three fields
`expected_revision`, `timeout_seconds`, `scan_bytes_warning`, no unknown/duplicate
keys, and checked positive nullable settings. All three keys are required; NULL
is explicit inheritance. Expected revision fits the next signed-64 revision;
timeout seconds fit signed-64 milliseconds. No browser binding/actor/operator
fields or arbitrary strings/extensions are supported.

The bounded `EffectiveQueryControls` encoder is a future transport primitive,
not a resolver/enforcement claim. It has fixed enums, safe signed-64 numeric
values (a future caller's zero timeout remains immediate expiry, not disablement),
stable deduplicated six-code warnings, ≤4096-byte JSON and fixed-order
ASCII response values (`x-siglake-query-controls` ≤1024, warnings ≤512, total added
headers including framing ≤2048). Unavailable cannot claim resolved timeout,
revision or output bounds. No principal/token/query/results/parameters fields,
free text, extension bag or request-header authority exists. No route emits it.

## Local checks

`scripts/check-query-policy.sh` accepts no target arguments. It resolves native
Rust 1.95 and PG16.14 before private HOME/XDG isolation, creates/marks a **new**
socket-only disposable cluster before roles/DDL, uses empty explicit and HOME
passfiles, empty provider files/disabled metadata and registry-only private Cargo,
and runs the complete policy suite plus the injection-only signed-JWT buffered
router suite, including fixture-gated tests. Missing setup
fails; ignored tests are run explicitly, not counted as passes. Isolation is not a
sandbox. No existing cluster, real provider/auth credential or production role is
used. The fixture tests include a local synthetic wire relay dropping COMMIT
acknowledgement before/after server commit; both return unavailable, with different
actual primary outcomes. A separate bounded in-memory global tracing/log capture
proves the late-ERROR owned-cancellation/close path does not leak an upstream
sentinel; arbitrary NOTICE/INFO is still excluded. No wire payloads are logged.

Later slices must close SQL/body/job, all four Jaeger, external Arrow/internal
wire/skew, customer authorization and full end-to-end gates before advertising
enforcement. Production schema/security/audit/TLS/operator and deployment approval
remain separate. Offline Slice 1 was independently accepted; each subsequent
bounded runtime slice requires its own implementation review.

## Explicit RDS trust addition (no policy activation)

`PolicyConfig::reader_with_ca_profile(uri, bounds, Option<&str>)` and
`::manager_with_ca_profile(uri, bounds, Option<&str>)` independently select
`aws-rds-us-east-1-rsa2048-g1`. Existing constructors keep bundled WebPKI unchanged.
Profiles must be exact and TCP verify-full; unknown/empty selectors and selected
socket/disable configurations fail closed. Bounds, separate owned connection budgets,
fixed `qp_reader`/`qp_manager` roles, primary reads and integrity checks are unchanged.

Future deployment mapping is `SIGLAKE_QUERY_POLICY_READER_CA_PROFILE` for the
`qp_reader` connector and `SIGLAKE_QUERY_POLICY_MANAGER_CA_PROFILE` for `qp_manager`.
These names are **documentation only**: no environment/main/runtime/UI policy
activation was added. The caller must explicitly supply each constructor argument.
No history/app/catalog/jobs pools or credentials are reused.
See [public root provenance, additive SQLx semantics and rotation](../crates/siglake-query-server/certs/README.md).
Pinned addition bytes do not mean exclusive trust: WebPKI roots remain accepted.
Actual RDS TLS/role/schema compatibility and coordinated rollout remain gates.

### Historical owned PostgreSQL16.13 compatibility check (pre-Controls5A)

This subsection records earlier evidence, not the current layout-v2 manifest or
this slice's toolchain claims. Controls5A uses only the approved cached SSL-capable
PG16.13 prefix; it does not rerun 16.14, download tools or qualify two versions.

The then-unchanged reviewed PG16.14 integrity manifest (`schema-v1-contract.json`,
SHA256 `16fe62ee8fddbe6453e68754d3feec2697fa12e8eab3d830d6e5e66b292c6fd6`) was accepted by the owned
socket-only PostgreSQL16.13 fixture: the full policy role/schema/tamper/CAS/primary/
transaction suite plus signed buffered-controls selection passed. No catalog output
was learned from live RDS, no new manifest was generated, and no comparison relaxed.
This establishes local16.13 rendering compatibility, not production role/schema proof.

No verified16.13 installation was cached. With explicit supervisor approval, official
`https://ftp.postgresql.org/pub/source/v16.13/postgresql-16.13.tar.bz2` was verified
against its official `.sha256` over HTTPS (archive SHA256
`dc2ddbbd245c0265a689408e3d2f2f3f9ba2da96bd19318214b313cdd9797287`), built with existing
GCC15.3.0/make4.4.1 into an isolated private prefix using `--without-readline
--without-zlib --without-icu` (no SSL enabled), `make -j2`, no global install or Nix
hash override. Source/archive were removed after installation; public source/tool/
binary hashes and sanitized command results were retained in private local evidence.
This no-SSL16.13 build is **only** socket/catalog compatibility proof. Synthetic TLS
uses cached PostgreSQL16.14 + OpenSSL3.6.2; an actual RDS handshake remains unproved.
