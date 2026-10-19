# Siglake

**Open-source log and trace analytics on your object storage.**

Siglake collects telemetry through OpenTelemetry, stores it as Apache Parquet
in Apache Iceberg tables, and lets you explore it with SQL. Built in Rust on
DataFusion, it separates ingestion, compaction, and querying so each can scale
independently.

[Documentation](https://docs.siglake.dev/) ·
[Benchmarks](https://benchmarks.siglake.dev/) ·
[Releases](https://github.com/siglake/siglake/releases) ·
[Changelog](CHANGELOG.md)

- **Use your existing collectors.** Ingest logs and traces over OTLP/HTTP or
  OTLP/gRPC, or send logs through the Elasticsearch-compatible bulk API.
- **Query with your tools.** Use SQL over HTTP, the command-line client, or the
  [read-only MCP server](integrations/mcp/README.md) for Claude and Codex.
- **Keep your data open.** Read committed Iceberg snapshots directly with
  [DuckDB, Spark, Trino, and other compatible engines](https://docs.siglake.dev/guides/external-engines/).
- **Run it on your infrastructure.** Start locally with Docker Compose, or
  deploy on Kubernetes with Helm, the operator, and AWS deployment tooling.

## Try it locally

You'll need Git, Bash, curl, and Docker with the Compose plugin. Clone the
repository and start the local stack:

```sh
git clone https://github.com/siglake/siglake.git
cd siglake
scripts/up.sh
```

The first run builds the images from source, then starts the ingester,
compactor, query server, PostgreSQL catalog, and MinIO object store.

Send a log record over OTLP/HTTP:

```sh
curl -fsS http://localhost:8088/v1/logs \
  -H 'Content-Type: application/json' \
  --data-binary @- <<EOF
{
  "resourceLogs": [{
    "scopeLogs": [{
      "logRecords": [{
        "timeUnixNano": "$(date +%s)000000000",
        "severityText": "INFO",
        "body": {"stringValue": "hello siglake"}
      }]
    }]
  }]
}
EOF
```

Query it with SQL:

```sh
curl -fsS http://localhost:8089/api/v1/sql \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT timestamp, raw FROM events ORDER BY timestamp DESC LIMIT 10"}'
```

Events become queryable asynchronously; if the first query returns no rows,
try it again shortly. The [quickstart guide](https://docs.siglake.dev/getting-started/quickstart/)
covers sending more data and exploring it.

Stop the local stack while keeping its data:

```sh
scripts/down.sh --keep-volumes
```

## Deploy and connect

- [Helm](https://docs.siglake.dev/operations/helm/) and the
  [Kubernetes operator](https://docs.siglake.dev/operations/operator/)
- [AWS deployment](deploy/aws/README.md)
- [HTTP API](https://docs.siglake.dev/reference/http-api/)
- [Claude and Codex via MCP](integrations/mcp/README.md)
- [Grafana and Prometheus monitoring](https://docs.siglake.dev/operations/monitoring/)

## Benchmarks

See [benchmarks.siglake.dev](https://benchmarks.siglake.dev/) for query results,
engine comparisons, and resource measurements. The
[methodology](https://benchmarks.siglake.dev/methodology.html) explains datasets,
hardware, cache settings, and comparison limits so you can evaluate the results
in context.

## Learn more and contribute

The [architecture guide](docs/ARCHITECTURE.md) explains how ingestion, the
write-ahead log, Iceberg storage, and distributed queries fit together.
[Design notes](docs/) cover individual subsystems, and
[scope and limitations](docs/LIMITATIONS.md) describe supported behavior and
tradeoffs.

See [CONTRIBUTING.md](CONTRIBUTING.md) to build from source or submit a change,
[SECURITY.md](SECURITY.md) to report a vulnerability, and the
[code of conduct](CODE_OF_CONDUCT.md) for community guidelines.

## License

[Apache License 2.0](LICENSE). Vendored dependencies retain their upstream
licenses and notices; see [third_party/README.md](third_party/README.md) and
[NOTICE](NOTICE).
