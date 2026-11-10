# Query controls slice 4A: ordinary direct shard / Arrow

**Source-only, local injection only; not complete Controls or production enablement.**
The existing `AppState::with_query_controls` injection now covers ordinary externally
authenticated `/api/v1/sql/shard`. No env/CLI/main activation, provider integration,
new operation scope, estimator, query-admission reservation or wire field was added.

After verified authentication and tenant authorization, the existing middleware
captures the original monotonic ingress. The shard resolves its **own** exact
verified tenant/issuer/client primary policy before history/catalog work. Successful
absence inherits without revision; an explicit all-NULL row inherits with its known
revision; overrides are applied. Unavailable/unsafe primary refuses with
`503 query_policy_unavailable`, never inheritance. Open/static/human ID callers
without a verified provider principal use bounded primary preflight and inherited
operator limits, not agent attribution. Request JSON, tenant/controls headers,
subject/history and forwarded-looking fields cannot select policy identity.
Existing unknown-field parsing and coordinator tenant/auth rules are unchanged.

The original clock includes primary I/O, history, preferred scan order, registration,
snapshot/schema pin lookup, planning, collection, Arrow IPC writing, response assembly
and server body production. Resolution tightens from ingress, never worker entry.
Time is mandatory independently of optional circuit breakers. The shard wire still
has no caller `limits` field: unknown top-level hints do not introduce new timeout
or breaker controls. Expired/pre-poll/late-ready/empty results cannot commit success.
The existing native timeout remains the bare `504` body with
`shard exceeded its wall-clock budget of Ns`; no SQL/Jaeger cost is fabricated.
Initial unresolved timeout uses the initial operator budget; resolved timeouts use
the truthful effective budget. Policy failure is a distinct resolution failure.

## Safe metadata and compatibility

Resolved responses/errors add the existing bounded typed response-only headers:

- `x-siglake-query-controls`: fixed ASCII key order/enums, at most 1024 value bytes;
- `x-siglake-query-warnings`: deduplicated fixed allowlisted codes, at most 512
  value bytes and six codes; combined added name/separator/CRLF bytes at most 2048;
- serialized DTO is checked at at most 4096 bytes before work/headers. Encoding
  failure safely refuses rather than omitting required warnings.

Arrow always reports `estimate=unavailable;exact=na;estimated_bytes=na`, output rows
`na`, output cap false, and `scan_estimate_unavailable`. A configured scan warning
threshold also emits `scan_threshold_not_evaluated`, **not** a hard byte rejection,
zero estimate or within-budget verdict. The independently bounded `x-siglake-scan`
retains actual scan-attribution meaning; it is not a policy estimate or byte cap.
Arrow batches, IPC bytes/schema/values and `application/vnd.apache.arrow.stream`
remain unchanged. No JSON envelope, suffix or terminal marker is added to IPC.

Existing Arrow batch boundaries are preserved. Deadline/cancellation checks surround
each batch, each at-most-16KiB sink fragment and pre-success. Native Arrow conversions,
allocations and individual library calls are not OS-preempted. Cooperative overshoot,
scheduler latency, scan-pump unwind, total RSS and stalled-peer delivery are not
universally bounded. The existing authority-owned body registry/channel is reused:
128 reserved/retained tasks, two queued at-most-16KiB logical chunks, one pending
copy, with upstream/native backing separately retained. This is **not** DataFusion
query admission. Never-polled/slow bodies expire independently; dropped bodies
cancel. Post-header expiry terminates transport without status/body rewriting.
Explicit `shutdown_query_controls().await` observes retained joins. Synchronous
Drop is abort-only **UNCONFIRMED**, not termination proof.

## Deliberate 4B refusal and remaining gates

With controls configured, coordinator-authenticated shard work refuses
`503 query_controls_path_unavailable` before primary/history/catalog/execution:
there is no accepted implemented controlled forwarding context yet. Absent-authority
legacy direct/coordinator behavior and tenant/pin rules remain available unchanged.
Controlled fanout and explicit-distributed batch pre-work refusal remain intact.
There is no new future-wire scaffold or partially enabled forwarding mode.

Trusted coordinator wire, remaining-budget HTTP fanout/fallback/retry, worker clamps,
cross-host expiry/skew and two-node controlled proofs are **next slice 4B**, not
implemented here. Loopback membership is not a two-node execution proof. UI/API,
effective endpoint, startup enablement, full Controls, Investigations, real provider/
RDS/TLS/proxy qualification and production/manual-schema/grant/rollout approval
remain separate gates. Owned local PG16.14/16.13 and synthetic signed/TLS fixtures
are local evidence only; configuration isolation is not sandboxing.
