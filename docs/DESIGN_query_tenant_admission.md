# Query tenant admission qualification

Status: qualified on 2026-09-19. Proceed with the query-specific, opt-in
allow-list in #5489. This document records the contract; it does not add the
setting.

## Finding

A verified tenant claim is enough to reach `TenantRegistry::resolve`. The first
request for a claim creates the tenant namespace, creates empty `events` and
`query_audit` tables, and retains an `IcebergContext` in the process registry.
Sibling contexts share the catalog handle, connection pool, warehouse handle
and caches. Each distinct claim still adds persistent catalog/object-store
metadata and a retained registry entry.

The growth is small per tenant and linear. It is still an operator-controlled
resource boundary: an identity provider able to mint arbitrary valid claims
can make the query tier perform catalog writes and retain names and contexts
for the process lifetime. The writer already has a bound for this class of
input, while the reader has none.

## Bounded local measurement

`tenant_registry_cost_measurement` opens one filesystem-backed SQLite context,
takes a baseline, then resolves 100 distinct valid tenant names in sequence. A
tracking allocator counts live requested heap bytes. Warehouse bytes and file
counts include the SQLite catalog and Iceberg metadata. The ordinary test gate
ignores this characterization because it creates 100 namespaces; reproduce it
with:

```sh
cargo test -p siglake-query-server \
  --test tenant_registry_cost_measurement -- --ignored --nocapture
```

Three runs after compilation gave:

| tenants | elapsed (ms) | retained heap (bytes) | catalog growth (bytes) | warehouse growth (bytes) | new files |
|---:|---:|---:|---:|---:|---:|
| 1 | 7–8 | 3,133 | 0 | 2,763 | 2 |
| 10 | 76–88 | 9,325 | 8,192 | 35,822 | 20 |
| 25 | 196–215 | 19,513–20,569 | 12,288 | 81,363 | 50 |
| 50 | 396–953 | 36,669–43,288 | 32,768 | 170,918 | 100 |
| 100 | 796–1,480 | 70,981–78,048 | 65,536 | 341,836 | 200 |

At 100 tenants the process retained 69–76 KiB above baseline and the local
warehouse grew 334 KiB. Each tenant added two metadata files. This does not
measure Postgres rows, object-store request latency or storage billing, and a
filesystem-backed catalog understates those remote costs. It does establish
the local shape: the shared pool avoids a connection multiplier, while tenant
metadata and retained process state keep growing with novel claims.

## Decision

Proceed with a separate query allow-list, default unrestricted.

Reusing `ingester.allowedTenants` would couple read and write lifecycle. A
tenant may stop accepting new logs while its retention window, investigations
or export obligations still require reads. Operators need to remove that
tenant from write admission without removing query access. A query-specific
set can be a superset of the ingest set during that interval and can follow its
own change schedule.

Leaving the reader unbounded preserves one less setting but leaves verified
claims able to create catalog state forever. The measured cost does not justify
a default cap or a release hold; it does justify an opt-in exact set for
deployments whose tenants are known.

## Contract for #5489

- Add `query.allowedTenants` and the matching query-server CLI/environment
  input. An empty value emits no restriction and preserves current behaviour.
  Do not read `ingester.allowedTenants` in the query workload.
- A non-empty query set is meaningful only with `query.oidc.tenantClaim`.
  Refuse a chart or binary configuration that supplies the set without claim
  routing instead of accepting a setting that cannot match a named tenant.
- Check the normalized, verified claim after identifier validation and before
  any handler can call `TenantRegistry::resolve`. A claim outside the set gets
  HTTP `403`; no namespace, table or registry entry is created.
- Count the refusal in
  `siglake_query_tenant_denied_total{reason="not_allowed"}` and pre-register
  that label set at zero. Update `SiglakeQueryTenantsDenied` so its description
  distinguishes an absent/unusable claim from an allow-list refusal.
- Apply the same set on a worker when an authenticated coordinator forwards a
  tenant in `/api/v1/sql/shard`. The worker checks before `resolve_ice`; a
  restrictive or mismatched worker returns `403`, and the coordinator forwards
  that deliberate answer. A direct request and every shard therefore use the
  same admission rule.
- The default namespace remains unchanged. The set bounds named tenants from a
  verified claim; it does not turn static-token or open authentication into a
  new tenant-routing mode.
- Cover unrestricted compatibility, allowed and denied claims, absence of
  namespace creation after denial, worker refusal propagation, Helm rendering,
  the OpenAPI `403` descriptions and generated artifacts.

No query-side count cap is proposed. An exact set solves the qualified case,
and selecting a process-local count limit would need a separate lifecycle
contract for which tenants win after restart.
