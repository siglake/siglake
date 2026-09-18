# Architecture changes — June 2026 (SQL-only query · OTel-only ingest · autoscaling)

Three coordinated architectural changes, approved 2026-06-06. They reinforce
each other: one query language + one ingest protocol = a smaller surface and a
cleaner autoscaling story, all feeding the WS-7 OTel/dynamic-schema direction.

## 1. Remove SPL — full removal, SQL-only query <!-- vendor-name-ok: dated removal record, see #4601 -->

The detection pipeline does **not** use SPL. It survives only in the query layer <!-- vendor-name-ok: dated removal record, see #4601 -->
(`/api/v1/spl`) and as a boolean `search`-expression filter in two spots: the SSE <!-- vendor-name-ok: dated removal record, see #4601 -->
stream `?filter=` and the dispatcher webhook filter. Standardizing on DataFusion
SQL, we remove it entirely, sequenced so nothing breaks:

1. **Query layer** — remove the `/api/v1/spl` endpoint, `query-server/src/spl.rs`, <!-- vendor-name-ok: dated removal record, see #4601 -->
   the SQL/SPL split, the CLI `spl` command, and the `splReverseTimeDefault` <!-- vendor-name-ok: dated removal record, see #4601 -->
   plumbing.
2. **Filter sites** — replace the SSE `?filter=` + dispatcher webhook filter with
   a small SQL boolean-predicate evaluator (compile a SQL `WHERE`-style expr,
   evaluate against a single-row `RecordBatch`), or drop the SSE filter (debug
   convenience).
3. **Crate** — delete `siglake-spl` and its workspace/dep references. <!-- vendor-name-ok: dated removal record, see #4601 -->

## 2. OTel-only ingest (transport swap first)

OTLP logs (`POST /v1/logs`) is already live, but the HTTP event collector
compatibility surface is the hardened primary: tenancy is keyed off its tokens
and the rate-limiter + backpressure router sit in front of it. We make OTLP the
**sole** ingest path:

- **Tenancy: `X-Scope-OrgID` header** (Grafana/Mimir/Loki convention) replaces
  token→tenant routing. Absent ⇒ the default tenant (single-tenant mode).
- **Reuse, don't delete, the hardening** — the rate-limiter + backpressure router
  are generic; rename their collector-specific names to `ingest` and front the
  OTLP path with them.
- **Remove** the collector routes (`/services/collector/*`), the collector token
  type, and the collector token Secrets in the operator/Helm.
- **Event mapping unchanged for now** — keep host/source/sourcetype/index +
  body→raw; dense-attribute extraction / dynamic schema is the later WS-7 phase.
- Update smoke tests + loadgen to OTLP.

## 3. Autoscaling — KEDA, on the right primitives

Mechanism: **KEDA + Prometheus scalers** (per-pool `ScaledObject`s; lowest
maintenance, scale-to-zero capable). Native CPU HPA is the wrong signal; the
operator's bespoke scaler is reserved for future cross-pool policy (or retired).
The mechanism is secondary to the **primitives**, which must land first:

1. **Graceful ingest scale-down** — the ingester does HTTP graceful-shutdown on
   SIGTERM; harden it to **seal the active WAL segment on shutdown** (so buffered
   events aren't stranded) + add a **preStop hook + `terminationGracePeriodSeconds`**
   (neither in the Helm template today). *Critical for safe scale-down.*
2. **Fungible pods** — query already stateless; ingest scale-up safe; scale-down
   depends on (1).
3. **Clean per-pod saturation gauges** — ingest: accept-rate + backpressure-queue
   depth + 503-reject rate; query: in-flight + p95 latency. These are the KEDA
   triggers (mostly exported already; OTel-only narrows them to one protocol).
4. **Anti-flap** — KEDA stabilization/cooldown windows.
5. **Query cold-cache on scale-up** — HRW cache-affinity is a later optimization,
   not a blocker.

Then: KEDA `ScaledObject`s for ingest + query.

## Sequence

SPL removal (1) → OTel-only (2) → autoscaling primitives + KEDA (3). Small, <!-- vendor-name-ok: dated removal record, see #4601 -->
tested, committed increments throughout (the Phase A/B cadence).
