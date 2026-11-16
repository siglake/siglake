# Optional Console query gateway: engine boundary

This draft supersedes the combined engine history/control-plane proposal in
PR7. It introduces **no engine runtime, dependency, CLI, authentication,
configuration, schema, collector, query/job or MCP changes**. Siglake remains
independently usable without Console or its metadata database.

Discussion: https://github.com/siglake/siglake/issues/11
Console replacement: https://github.com/limnion-ai/console/pull/12
Original source, tests and evidence: commit
`c3fc39a712470963d47a08664fe6bc789cd2d176`, preserved in
`gianarb/siglake:archive/query-history-controls-c3fc39a` and its frozen worktree.
The previous validation is historical evidence of that source, not this rescope
or of a newly deployed gateway store. No prior failure is erased or promoted.

## Scope withdrawn from the engine

The earlier combined diff included history persistence/configuration, SQL
redaction/collection, retention workers, database roles, policy management,
audit, private signed coordinator controls and extensive execution/job/fanout
instrumentation. Those changes are no longer proposed here.

Console owns the optional independently deployed records query gateway and its
new addon metadata schema/credentials, writer/reader, bounded collector and
maintenance, policy revisions/audit, resolver and source-labelled web adapters.
It must not import Siglake/DataFusion crates or share the engine/application
metadata pool. This is not a requirement for stock Siglake to add a database,
trust a tenant header, accept a Console administrator bearer or expose a generic
management proxy.

The new Console schema/roles are NOT automatically adopted from the old engine
six-role activation packet. No runtime DDL or implicit migration/production
activation is authorized by either the architecture or this docs-only PR.

## No speculative engine hooks

For the currently supported same-tenant interactive records mode, no missing
engine mechanism has been demonstrated. Reuse public query APIs and native
credential authorization; do not add hooks merely to retain an engine diff.

Existing upstream limits, SQL stats, tenant-scoped batch status/results/cancel
are mechanisms to assess, not evidence every deployed image has their current
semantics. In particular an estimated scan ceiling is not exact physical I/O
accounting or mid-flight enforcement. Qualify an exact deployment and every
relevant path before advertising an engine guarantee.

If a future required mechanism is missing, open a focused issue/PR containing
its reproduction, minimal engine-owned behavior and path-specific tests. Do
not reintroduce Console history/policy database or management plumbing with it.

## Security and guarantee separation

* Native engine authorization/scopes/tenant binding stay authoritative. A
  selected Console organization is context, not a credential. Gateway-wide
  admin tokens plus arbitrary trusted tenant headers are not a substitute.
* Gateway admission, original client wait, decoded/output bounds and rendered
  rows concern the gateway boundary, not global engine compute, physical scan
  accounting, cooperative cancellation completion or hard kill.
* Policy lookup errors fail before dispatch. A policy timestamp does not renew
  the original request clock. Store policy and gateway observations separately
  from semantically qualified native evidence.
* Client disconnect, timeout, output refusal, HTTP completion or gateway restart
  alone do not prove an engine terminal state. No automatic SQL/job replay
  after an ambiguous submission, transport error or gateway restart.
* Direct Siglake traffic remains outside optional gateway history/controls.
  Durable gateway SQL stays NULL; no raw results/tokens/provider errors in
  metadata. Original720h expiry, bounded best-effort maintenance and no
  resurrection/backup-erasure promises belong to the addon contract.

## Explicitly unsupported rather than falsely transferred

The current gateway supports interactive records only. Original private
streaming/batch/job/coordinator/distributed controls are withdrawn, NOT claimed
mechanically migrated or equivalent. Required unknown engine guarantees and
unsupported modes must refuse before upstream dispatch. The original custom
native identity/MCP/CLI changes are also withdrawn; stock interfaces remain.

Console's replacement uses explicitly gateway-only wait/output/row settings,
not a silent rename of native execution timeout or scan budgets. Its new JSON
adapters do not relabel legacy engine HTML controls as gateway guarantees.
Investigations and app migration0007 remain outside this work.

## Maintainer feedback on the superseded source

Reference: https://github.com/siglake/siglake/pull/7#issuecomment-6816413946

1. **Forbidden process environment mutation:** the history config fixtures and
   their five offending calls are removed from the current engine proposal.
   The existing source gate is unchanged, not exempted. Replacement config
   takes pure inputs; process fixtures supply child environments from their
   parent without Rust `set_var`/`remove_var`.
2. **Portable scripts:** contributor-local qualification scripts are removed
   from this proposal. Console replacements take explicit reviewed compiler,
   formatter, PostgreSQL, OpenSSL, Cargo and new private output directory
   inputs. No hardcoded contributor HOME/cache/Nix-store path is transferred.
3. **Current source/runtime qualification:** this rescope starts from current
   main `f12168e77e77789c7f5f127fc4b8af4db661f2e5`, including the public CI
   runner fix, and changes only this document. It does not claim the old69
   ignored fixtures, old historical integrations or cancelled CI as current
   passes. There is no new engine runtime feature to qualify in this diff.
   New addon code requires its own tests: pure models, real LOCAL PG/TLS and
   synthetic verifier/upstream tests are distinguished from actual stock-engine,
   provider and RDS evidence. They do not establish unsupported query paths.

Each original changed path has a disposition in Console's
`docs/query-gateway-original-paths.json`. Removed engine-feature tests remain
available in the archived source; new gateway tests are additive, not asserted
one-for-one replacements or coverage of all old native guarantees.

## Review and release gates

Keep both proposals draft pending upstream scope review and exact-source
checks. Reducing this PR withdraws the combined engine feature; it is NOT
acceptance of the new Console metadata implementation for production.
Actual metadata DB roles/credentials/TLS, source-specific UI, provider/RDS/
stock-engine qualification, immutable images, scoped saved plans and paired
migration-aware recovery require separate Console release approval. Do not
replay any completed Cognito security/branding or combined-engine activation
plan. No Siglake production deployment accompanies this change.
