# siglake

A horizontally-scalable, OTLP-native log analytics platform built on
**Parquet v2 + Apache Iceberg + DataFusion**, in Rust. Logs and traces go in
over OTLP or an Elasticsearch-compatible bulk API; everything lands as open
Parquet in an Iceberg catalog on object storage, queryable by siglake's own
distributed SQL tier and, without siglake in the path, by any Iceberg reader.
Tables are format version 2 with a microsecond `timestamptz` event time and the
exact OTLP nanosecond beside it in `timestamp_ns`, so they carry no v3-only
type; Trino 483, Spark 3.5.9, DuckDB 1.5.5 and PyIceberg 0.12.0 are
[demonstrated end to end](https://docs.siglake.dev/guides/external-engines/#compatibility-evidence)
against that contract.

**Status (2026-09):** 0.1.0 is the first public release. The platform is
complete and AWS-validated end to end — ingestion, WAL, Iceberg storage,
distributed SQL, a supported interface for external WAL consumers, Helm charts,
an operator and Terraform/EKS deployment — at 200 GB (394 M rows) and 1 TB
(2.0 B rows) across ~100 benchmark rounds.

- **Documentation:** https://docs.siglake.dev — quickstart, concepts, guides,
  operations and reference.
- **How it is built:** [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) is the
  long-form record of every subsystem; focused design notes are the
  [`docs/DESIGN_*.md`](docs/) files.
- **What it leaves out, on purpose:** [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md).
- **Changes:** [`CHANGELOG.md`](CHANGELOG.md).

## Quickstart

The full stack — Postgres catalog, MinIO warehouse, ingester, compactor and
query server — runs locally with one command:

```sh
scripts/up.sh
```

Send a log record over OTLP/HTTP (port 8088):

```sh
curl -s http://localhost:8088/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"quickstart"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"'"$(date +%s)000000000"'","severityText":"INFO","body":{"stringValue":"hello siglake"}}]}]}]}'
```

Query it with SQL (port 8089); fresh rows serve from the WAL buffer within
seconds, before compaction commits them to Iceberg:

```sh
curl -s http://localhost:8089/api/v1/sql \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT timestamp, host, raw FROM events ORDER BY timestamp DESC LIMIT 10"}'
```

The same binary is an interactive SQL client (`siglake sql`, one-shot or a
REPL, with per-query scan stats and `--dry-run` cost estimates), and OTLP/gRPC
listens on 4317 by default. The
[docs quickstart](https://docs.siglake.dev/getting-started/quickstart/) continues
from here: sending real data, first queries and local development.

## Architecture at a glance

```
                    ┌──────────────────────────── ingest tier ───────────────────────────┐
OTLP /v1/logs ──►   │ ingester ─► WAL (Arrow IPC segments: active/ → sealed/ → committed/)│
Elastic _bulk ──►   │            └ backpressure lanes, token-bucket rate budgets          │
OTLP /v1/traces ─►  │                                                                     │
OTLP/gRPC :4317 ─►  └──────────────┬────────────────────────────┬─────────────────────────┘
                                   │ drain (continuous,         │ SegmentConsumer
                                   │ N commits in flight)       │ (durable cursor,
                                   ▼                            ▼  retention waits)
                    Iceberg on object storage            EXTERNAL consumers
                    ┌───────────────────────────┐        e.g. a detection
                    │ events + user indexes      │◄───── pipeline, a router,
                    │ (Parquet v2, time-ordered, │        a mirror, your own
                    │  day-partitioned, blooms + │
                    │  inverted-index sidecars)  │        docs/CONSUMING_SEGMENTS.md
                    │ leveled compaction         │
                    └────────────┬──────────────┘
                                 │
                    ┌────────────▼──────────────┐
                    │ query tier (2+ replicas)   │  DataFusion SQL, transparent distributed
                    │ /api/v1/sql coordinator    │  coordination, aggregate fast paths,
                    │ + WAL real-time buffer     │  ordered early-stop, cost guardrails
                    └───────────────────────────┘
```

No Kafka, no Flink: the WAL is the streaming substrate. External consumers
read sealed segments through `SegmentConsumer` with their own durable cursors,
exactly like the drain does, sharing one Arrow `RecordBatch` representation end
to end. siglake ships no consumer of its own; a detector, a router or a mirror
is a separate process ([docs/CONSUMING_SEGMENTS.md](docs/CONSUMING_SEGMENTS.md)).

## Design decisions

The choices that shape everything else, each with where its full contract is
written down.

- **Open table formats, no private storage engine.** Everything is ZSTD
  Parquet v2 in Iceberg format-version-2 tables, day-partitioned by
  `timestamp`, in your object store and your catalog (SQLite for development,
  Postgres in production). No siglake schema uses a v3-only type, so any
  current Iceberg reader can query the warehouse directly. The timestamp
  contract — microsecond `timestamptz` for `timestamp`, the exact OTLP
  nanosecond in `timestamp_ns` — exists for that reason
  ([`docs/DESIGN_time_ordered_storage.md`](docs/DESIGN_time_ordered_storage.md)).
- **The WAL is the streaming substrate.** There is no Kafka and no Flink. Every
  accepted event is appended to a write-ahead log first; the drain reads
  sealed segments into Iceberg commits, and any other consumer reads the same
  segments through `SegmentConsumer` with its own durable cursor,
  at-least-once delivery and retention that waits for it. siglake ships no
  consumer of its own: a detector, a router or a mirror is a separate process
  ([`docs/CONSUMING_SEGMENTS.md`](docs/CONSUMING_SEGMENTS.md)).
- **An acknowledgement means the WAL is durable.** Rows are acked after the
  append and the directory entries that name it are `fsync(2)`ed; a client
  sends `?commit=auto` to be acked from the page cache instead. Neither waits
  for the rows to be queryable: the compactor commits asynchronously, and the
  query tier's WAL buffer serves uncommitted rows in the meantime (~5.5 s
  ingest→queryable measured). Sealed segments mirror to object storage by
  default wherever a warehouse URL is set, so the WAL volume is not the only
  copy of what was acknowledged. Holding a segment for its upload costs the
  writer one directory fsync per seal, and that cost is irreducible: a lane
  never has a second pin to share the fsync with, and the only actor that could
  close a deferred one — the compactor — reaches the WAL over a shared volume
  from another node ([Ingest path](docs/ARCHITECTURE.md#ingest-path)).
- **Time order is a storage invariant, and compaction never stops.** Every
  write physically sorts rows by the table's declared sort order and stamps
  Parquet `SortingColumn` footers, and no compaction bin splits a
  time-overlapping cluster. Compaction is a leveled ladder that runs beside
  sustained ingest with graded backpressure, and a file retires after
  `max_merge_gen` rewrites, so total compaction writes are bounded by roughly
  that generation count times the ingested bytes
  ([`docs/DESIGN_continuous_compaction_and_ingest.md`](docs/DESIGN_continuous_compaction_and_ingest.md)).
- **Search acceleration is self-describing in the files.** Trigram and token
  blooms, inverted indexes in the Parquet footer with Puffin sidecars for
  large ones, and per-file group-count and time-bucket footers ride with the
  data, and compaction policy is computable from file names and the manifest
  alone. Footer inverted indexes are written by default. The post-rewrite
  Puffin rebuild ships **off** in 0.1.0: a compacted file's parsed index costs
  about 40 bytes per indexed row, and at 50 GB-class layouts the sidecar path
  landed above the text-search ceilings that were measured on the scan path.
  Whether a query uses an index it finds is decided per execution: a text
  predicate under a `LIMIT` — ordered or bare — reads a sliver of the first
  file and would pay a whole file's postings to do it, so it stays on the scan
  path, while an unclipped text scan keeps the index. A missing or declined
  index costs pruning, never correctness. What a text query spends before its
  first batch is attributable per stage rather than as one number:
  `siglake_iceberg_text_index_startup_seconds{stage}` separates the load queue
  from the blob read, the decode and the selection, and the parsed-index
  cache reports its lookup outcomes beside the bound that dropped an entry
  ([`docs/DESIGN_inverted_index.md`](docs/DESIGN_inverted_index.md),
  [`docs/DESIGN_raw_content_index.md`](docs/DESIGN_raw_content_index.md)).
- **Exact aggregates without scans, or the exact scan.** Whole-table and
  windowed group counts, histograms and negations are served from side-object
  aggregates and per-file footers with `rows_scanned: 0`, but only behind
  strict validity guards — the total matches the record count and the
  snapshot-coverage chain is unbroken — and otherwise the query takes the
  exact per-file path. Result caches are keyed by table snapshot and never
  expire on a timer; a cache that could serve a stale leading edge is
  prohibited
  ([`docs/DESIGN_incremental_group_count_aggregate.md`](docs/DESIGN_incremental_group_count_aggregate.md),
  [`docs/DESIGN_group_count_footer_encoding.md`](docs/DESIGN_group_count_footer_encoding.md)).
- **SQL is the query surface, read-only and guarded.** `/api/v1/sql` plans
  through DataFusion, and DDL, DML and session statements are refused at plan
  verification. Every request gets a pre-flight cost estimate from the
  manifests, per-tier ceilings, a mid-flight rows-scanned breaker, one
  wall-clock budget that preparation and execution share, and a process-wide
  memory pool that refuses with `503` + `Retry-After` rather than spilling.
  Elasticsearch compatibility is write-only: `_bulk` ingests, the read APIs
  answer `501` with a pointer to SQL, and no ES query API is planned. Jaeger's
  HTTP query subset is served for Grafana on the same budgets
  ([API surface](docs/ARCHITECTURE.md#api-surface),
  [HTTP API reference](https://docs.siglake.dev/reference/http-api/)).
- **Distributed by replication, with one generation and one membership per
  query.** Query replicas sit behind a headless Service and any replica
  coordinates a fan-out, pinning every shard to its own serving generation
  (snapshot, schema id, table UUID) and to one membership snapshot, so an
  answer comes from one generation or not at all, never from a merge across
  two. Small `LIMIT` browses and metadata-served aggregates are answered
  locally by design; fan-out is for large scans
  ([`docs/DESIGN_distributed_query.md`](docs/DESIGN_distributed_query.md),
  [`docs/DESIGN_dynamic_query_peer_discovery_2026-09.md`](docs/DESIGN_dynamic_query_peer_discovery_2026-09.md)).
- **Single-tenant until you say otherwise.** Both boundaries route every
  caller to the default tenant, and a header naming another tenant is refused
  with `403` rather than honoured or ignored. Tenancy comes from exactly one
  of a verified JWT claim (`oidc.tenantClaim`, the setting for a shared
  cluster) or a gateway-trusted header (`trustScopeHeader`); configuring a
  claim makes it mandatory on both boundaries, and identifiers are validated,
  never repaired ([Multi-tenancy](docs/ARCHITECTURE.md#multi-tenancy)).
- **Schema changes are additive, and refusal beats silent loss.** A binary
  newer than a table cannot write the columns the table lacks, so the write is
  refused — the rows stay in the WAL and the client retries — instead of being
  acknowledged with a column dropped. `siglake migrate-schema` is additive and
  idempotent, both control planes run it before rolling workloads, and rolling
  the code back across an additive change is supported and tested
  ([Storage](docs/ARCHITECTURE.md#storage)).
- **The Helm chart is the supported install surface, and it refuses unsafe
  shapes at render time.** A compactor tier that can hold more than one pod
  needs the catalog claim and the mirror it claims from, the embedded
  `--with-compactor` is refused, and a multi-replica query tier needs the
  shared Postgres job store. The operator renders the same workload kinds
  from a `SiglakeCluster` CRD but a deliberate subset of the chart's surface
  ([Deployment](docs/ARCHITECTURE.md#deployment),
  [Helm](https://docs.siglake.dev/operations/helm/),
  [Operator](https://docs.siglake.dev/operations/operator/)). The local kind
  round drives the query tier 2 → 4 → 2 while checking distributed counts and
  retains its samples in `results/scale-2-4-2.json`
  ([kind round](deploy/kind/README.md#monitoring-evidence-round)).
- **No built-in UI.** The query tier is reached through SQL over HTTP and the
  Jaeger-compatible shim; Grafana is the front end. What ships for operations
  is a starter dashboard (`deploy/grafana/siglake-overview.json`, imported by
  you) and a `PrometheusRule` with 35 alerts grouped by what an operator
  should do, rendered when `prometheusRule.enabled` is set
  ([Monitoring](https://docs.siglake.dev/operations/monitoring/)).
- **Metrics are Prometheus; logs and traces are OTLP, and off until you point
  them somewhere.** The `/metrics` endpoint the alerts, the KEDA scalers and
  the dashboard read does not change. Setting
  `OTEL_EXPORTER_OTLP_ENDPOINT` also exports the existing log lines as OTLP log
  records and the request/cycle spans as OTLP spans, with W3C `traceparent`
  carried across the query fan-out so a distributed query is one trace. Unset,
  the exporters and their batch processors are never constructed, and the
  per-request residue is +104 ns against the span layer that was already
  there ([Observability](docs/ARCHITECTURE.md#observability-opentelemetry-emission)).
- **Iceberg is vendored, not waited for.** `third_party/iceberg` and
  `third_party/iceberg-catalog-sql` are first-class forks carrying the atomic
  `rewrite_files` action, count- and age-based snapshot expiry, commit-reload
  elision, S3 conditional puts and the incremental append scan that table
  subscriptions need; they are rebased against upstream periodically
  ([`third_party/README.md`](third_party/README.md)).
- **The metrics port is an internal surface, and profiling is off in every
  released binary.** `--metrics-bind` (9100/9101/9105) serves `/metrics` and
  nothing else in a release build, with no token check of its own — the query
  tier's auth guards 8089 — so it belongs behind your cluster's network policy
  and never behind an Ingress. The chart's `networkPolicy` writes egress rules
  only; restricting who may scrape is an operator decision. On-demand CPU, heap
  and tokio-runtime profiles (`/debug/pprof/*`) ride that same port, for the
  same reason `/metrics` does — it is the one surface every role shares — and
  reaching them takes two opt-ins that will not become defaults: a
  `--build-arg PROFILING=1` image, which no release is, and
  `SIGLAKE_PPROF_ENABLED=1` in the process. A stack-trace oracle is not a thing
  a query-authorized caller should be able to ask for
  ([Diagnostics](docs/ARCHITECTURE.md#diagnostics)).
- **Omissions are recorded, not hidden.** Everything left out on purpose, with
  the reason and what would change it, is one entry in
  [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md), kept current with the code.

## Performance

Measured on AWS 3-node clusters against open data and methodology
(https://github.com/limnion-ai/siglake-benchmarks). At 200 GB / 394 M rows:
ingest→queryable freshness ~5.5 s at full ingest rate; ~415 K rows/s sustained
fleet ingest with exact row counts held through node crashes; zero-scan
aggregates ~3 ms warm; keyword search ~8–12 ms and label filters ~9 ms through
bloom-pruned scans; whole-table `ORDER BY timestamp DESC LIMIT 100` ~45 ms warm.
At 1 TB / 2.02 B rows the same 21-shape suite runs zero-error with exact
counts. The full record is the
[Performance](docs/ARCHITECTURE.md#performance-measured-on-aws-3-node-clusters)
section of the architecture document and the docs site's
[Performance](https://docs.siglake.dev/about/performance/) page.

## Repo layout

```
crates/
  siglake-core         events, schemas, doc mappings, sharding
  siglake-ingest       OTLP/bulk decode → WAL writes, backpressure
  siglake-wal          segment format (Arrow IPC + CRC framing), lifecycle
  siglake-storage      Iceberg integration: writes, compaction, aggregates,
                       indexes, GC, retention, delete tasks
  siglake-compactor    drain loop + maintenance scheduling
  siglake-query-server distributed SQL, fast paths, WAL buffer,
                       Elastic/Jaeger shims, jobs, audit
  siglake-index        inverted-index build/serve
  siglake-bloom        trigram/token blooms
  siglake-cli          the `siglake` binary (all roles + ops commands:
                       audit-rotate, gc-orphans, retention-sweep,
                       delete-sweep, migrate-schema, wal-recover,
                       wal-requeue, …)
  siglake-operator     Kubernetes operator
  siglake-openapi      emits the committed OpenAPI 3.1 specs (docs/api/)
  siglake-bench (private, not in the public tree) / -loadgen   tooling
third_party/
  iceberg, iceberg-catalog-sql                   vendored forks (see Storage)
deploy/                Helm charts, Dockerfile, Terraform, Grafana
docs/                  ARCHITECTURE.md, LIMITATIONS.md, the DESIGN_* and
                       PERF_* records; api/ holds the generated OpenAPI specs
scripts/               local + kind dev environments
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how changes are proposed and gated,
[SECURITY.md](SECURITY.md) for reporting a vulnerability privately, and
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE). The vendored forks under
`third_party/` retain their upstream Apache-2.0 LICENSE and NOTICE files
(see `third_party/README.md` and [NOTICE](NOTICE)).
