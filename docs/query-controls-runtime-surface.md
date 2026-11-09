# Optional query-controls runtime surface (source wiring only)

This is **not production activation**, a role/schema installation, fleet/TLS/clock
qualification, Console rollout, or Investigations implementation. The manual
layout-v2 catalog/role contract and policy/wire schema version 1 are unchanged.
Existing internal shard wire v2 remains coordinator-only, without redirects.

## Explicit startup

Only `siglake-query-server` owns this configuration. With no controls options it
opens no controls connection and preserves existing auth/token-profile defaults.
Supplying controls options without the dedicated DSN is an error, not a partial
or silently disabled configuration. The DSN is opaque to Clap, redacted in Debug
and hidden from environment-value help; validation errors do not echo it.

New optional flags and their exact environment equivalents:

| Flag (`--` prefix) | Environment (`SIGLAKE_QUERY_CONTROLS_` prefix) |
|---|---|
| query-controls-database-url | DATABASE_URL |
| query-controls-operation-timeout-ms | OPERATION_TIMEOUT_MS |
| query-controls-max-connections | MAX_CONNECTIONS |
| query-controls-operator-generation | OPERATOR_GENERATION |
| query-controls-ca-profile | CA_PROFILE |
| query-controls-interactive-default-seconds | INTERACTIVE_DEFAULT_SECONDS |
| query-controls-interactive-ceiling-seconds | INTERACTIVE_CEILING_SECONDS |
| query-controls-batch-default-seconds | BATCH_DEFAULT_SECONDS |
| query-controls-batch-ceiling-seconds | BATCH_CEILING_SECONDS |
| query-controls-clock-skew-ms | CLOCK_SKEW_MS |

Enabling requires the DSN, explicit positive reader operation budget (at most
30000 ms), reader capacity (1..16), and positive operator generation. Existing
source timeout defaults are used unless explicitly overridden: interactive
60/300 seconds and batch 3600/21600 seconds (default/ceiling). Defaults must be
positive, representable, and no greater than their finite ceilings. These are
source values, not observed deployment settings or new product timeouts.

The DSN must identify a dedicated **PRIMARY `qp_reader`**, not an application,
job, history, or manager pool. The existing strict connector requires explicit
host/port/database/password and `sslmode=verify-full` for TCP. Only owned local
socket fixtures may use its existing empty-password/disabled-TLS exception. The
existing named RDS trust selector is additive; it cannot downgrade verification.
There is no passfile/credential fallback, schema autoload, or automatic grant.
Role, primary, ownership, membership, ACL, RLS and exact layout checks run at
startup and on checkout. Invalid static controls configuration fails before
telemetry/catalog/discovery; unsafe/unavailable reader startup fails closed.

Controlled fanout (including worker-only wire-v2 configuration) needs an explicit
finite relative clock-skew bound and a valid nonempty existing coordinator token.
There is no default skew or secret provisioning. This is an operator clock
assumption, not a measurement. Worker clamps/original expiry, self-shard
no-double-admission, REQUEST-to-HANDOFF transfer and accept-anchored durable RUN
semantics are retained. Readiness performs fresh bounded reader preflight and
returns a fixed safe unavailable outcome on controls failure, without a policy
cache. Shutdown stops admissions, drains HTTP, cancels/joins owned job/body work,
then closes the dedicated reader; cancelled join waits retain ownership. Final
Drop is still abort-only, not termination confirmation. Native work remains
cooperative and delivery to a stalled/disconnected peer is not guaranteed.

## Caller-only effective defaults

Authenticated `GET /api/v1/query-controls/effective` uses the existing bearer and
tenant guard. **No query parameters**, principal/org/credential selector, mutation
or impersonation are supported. The existing coordinator token allowlist is not
expanded. The bounded primary read/preflight starts from authenticated ingress
and is charged to the smaller local default-request budget. It does not open a
query tenant catalog or scan data.

The <=4096-byte typed version-1 response is `private, no-store`, with explicit
`enabled`, `disabled` or `unavailable` availability. Enabled summaries include
local operator generation and interactive/batch operator default/ceiling,
policy state/revision, default-request effective timeout and warning threshold.
Absent rows inherit with null revision; all-NULL rows inherit with known revision;
applied rows retain their revision. Store/role/schema failure is unavailable
(503), not absence. Budget expiry is 504. Disabled does not invent inheritance
or effective zeros. The fixed scope `local_node_default_requests` is deliberate:
these are defaults for this caller on this node/configuration, not a preview of
future caller overlays, worker clamps or another credential. Static/generic/
human-without-provider-provenance callers receive operator inheritance after
preflight, never an invented agent binding from audience/sub/request metadata.

## Response presentation and clients

Records add a bounded `controls` envelope without renaming columns, rows, cost,
stats or result fields. Explain and actual local dry-run retain native top-level
cost meanings and add planning metadata. Cached payloads contain no current
controls: fresh primary admission and per-response snapshots still apply on hits.

NDJSON starts with `{"_meta":"controls","controls":...}`. Native truncation/error
markers remain; normal controlled EOF adds `_meta: complete`. Timeout after
headers stops production and may emit the existing bounded timeout marker only
when writable, never success after expiry or a guarantee of delivery. Initial
headers describe pre-header knowledge; terminal evidence is not retroactive.
Selected local clipping/records conversion alone can prove an output cap. In
particular, distributed NDJSON has no universal cap claim. Jaeger and Arrow
remain **headers only**, with native JSON/IPC/error/scan-counter semantics intact.

The two fixed ASCII headers retain their independent 1024/512-byte value and
2048-byte aggregate limits; each serialized controls snapshot is <=4096 bytes.
Encoding failure refuses safely before headers, rather than dropping a required
warning and reporting success or restarting work. Warning codes are the existing
six-entry allowlist. Only complete **trusted live cost inventories** evaluate a
scan warning threshold. Exactness is separate from completeness. Incomplete,
unavailable and deserialized/untrusted cached costs do not mean zero/within
budget. Above-threshold is warning-only, never a new admission reservation,
hard agent byte cap, measurement, tariff or billing claim.

Job 202/status/result accepted/execution snapshots and original expiry remain
explanatory, not authorization, permission to replay SQL, or evidence that the
job succeeded. Retrieval does not resolve another policy or extend RUN expiry.
History has no new fields and is never an authority. Its existing batch/dry-run
receipt normalization is not a lossless raw-request transcript.

The supported MCP adapter is Python **STDIO tools with an HTTP engine client**,
not an MCP HTTP server. It validates/passes the bounded typed controls and fixed
warnings through existing query execution metadata and explain result framing,
including safe error-header metadata. Invalid/oversize/duplicate metadata fails
safely, never silently drops warnings and reports success. Python integer values
are retained without float/JavaScript Number conversion. Existing native caller
output and client timeout/row/byte safeguards remain; those are client safeguards,
not engine policy. No new capabilities, token exchange or dependencies are added.

`siglake sql` is also an HTTP client: raw JSON/NDJSON output preserves engine
metadata; its table view is not an effective-policy preview. Offline `siglake
query` directly uses SessionContext, has no authenticated engine identity or
server dependency, and is outside this authenticated engine-API enforcement
claim. It loads no controls DSN or credentials. Ingest-only startup does not
initialize the policy store. No client/offline mode is made a replacement
resolver.

Remaining gates include reviewer/parent exact-head acceptance and integration,
Console copy/polish, manual schema/roles/grants, operator bounds and fleet clock
validation, actual deployment/TLS/provider/RDS/proxy qualification and customer
rollout. Local owned PostgreSQL/synthetic JWT/CA/loopback proofs do not satisfy
those gates. Historical 61/62 terminal observation cause remains UNKNOWN; current
A/B history oracles are not a production history fix or guaranteed delivery.
