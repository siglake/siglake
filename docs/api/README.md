# OpenAPI specifications

Machine-readable OpenAPI 3.1 descriptions of siglake's HTTP APIs:

- **`openapi-ingest.yaml`** — the ingest server (`siglake ingest-server`, default
  `:8088`): OTLP/HTTP logs and traces, plus an Elasticsearch 7.10-shaped
  bulk-ingest surface for existing shippers.
- **`openapi-query.yaml`** — the query server (`siglake-query-server`, default
  `:8089`): DataFusion SQL, index and index-template management, batch jobs,
  GDPR delete tasks, authenticated process diagnostics, and a Jaeger-compatible
  trace read API.

They are two separate documents on purpose. The servers are deployed and scaled
independently, listen on different ports with different auth token pools, and
have disjoint path sets — a single merged document with two `servers` entries
would falsely assert that every path is reachable on both base URLs, and
`/healthz` exists on both with different response bodies.

## How they are generated

The specs are generated from the `#[utoipa::path]` annotations that sit next to
the axum handlers. `utoipa-axum`'s `OpenApiRouter` registers a route and its
documentation in the same call, so a route that exists in the server cannot be
missing from the spec — with one deliberate exception, below.

## The one undocumented route family

The Elasticsearch read paths — `_search`, `_msearch`, `_search/scroll`,
`_field_caps` and `_cat/*`, with and without an index segment — are registered
with a plain `route` call (`es_read_stubs` in `siglake-ingest`) and answer
`501` with a pointer to `POST /api/v1/sql`. They exist so a stray ES client
gets a definitive answer instead of a `404` it might retry; no ES query API is
planned, and fifteen documented `not implemented` operations read as a surface
under construction. The refusal is stated once, in the `elasticsearch-compat`
tag description. `es_read_routes_stay_undocumented` in
`crates/siglake-openapi/tests/invariants.rs` pins both halves — the paths out
of the document, the sentence into the tag — so this cannot quietly become two
exceptions.

Regenerate after changing any handler, request/response type, or route:

```sh
cargo run -p siglake-openapi -- --out docs/api
```

`--check` verifies the on-disk files match without writing them. CI runs the
generator and fails on any `git diff`, so these files cannot drift from the
code. `crates/siglake-openapi/tests/invariants.rs` additionally pins the exact
route table and checks that every `$ref` resolves and that only the health
probes are unauthenticated.

## No runtime endpoint

There is deliberately **no** `/openapi.json` (or Swagger UI) served at runtime.
The spec is a build artifact, checked into the repo; not serving it keeps the
running servers' attack surface to the documented API only.

## Not covered here

The Prometheus metrics surface — `GET /metrics` (text exposition) and `GET /`
on each component's `--metrics-bind` port — is intentionally omitted. It is
standard Prometheus text exposition, and an OpenAPI description would add
nothing over that contract.
