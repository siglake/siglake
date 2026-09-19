# Ingest lane-cap response qualification

*2026-09-19 (#3878 qualification, #5531 implementation). Status: implemented
for 0.2.0. A typed lane-cap refusal maps to HTTP `503` / gRPC `Unavailable`
without a retry hint. Genuine writer failures keep `500` / `Internal`.*

## The refusal is persistent

`--ingest-max-lanes` bounds distinct `(tenant, index)` keys in one ingester
process. A lane owns one writer task and open file per shard. Once admitted, its
entry remains in `BackpressureRouter::lanes` even when its command queue is
empty. Only `BackpressureRouter::shutdown` drains the map, on process shutdown.

This is separate from a full lane queue. A full queue is transient and already
returns HTTP `503` with `Retry-After: 1`, or gRPC `Unavailable` with the plain
`retry-after` metadata. The lane-cardinality refusal now enters
`SubmitOutcome::LaneCapRefused`, while WAL conversion, writer and reply-channel
failures remain `SubmitOutcome::Failed`. The former maps to hintless HTTP `503`
or gRPC `Unavailable`; the latter stays HTTP `500` or gRPC `Internal`.

An empty queue cannot make a refused key admissible. These operator actions can:

- restart the process, which clears the map and lets keys race for slots again;
- raise `--ingest-max-lanes` / `SIGLAKE_INGEST_MAX_LANES`, which also needs a
  restart because the value is read at startup;
- add an ingester pod, giving traffic that reaches it a fresh per-process map;
- correct a client that is varying `X-Scope-OrgID` or `x-siglake-index`, then
  restart the full pod to clear the keys already admitted.

None gives a delay the refusing pod can predict. A retry can keep reaching that
pod and the same full map. Restart also changes which keys win the finite slots,
so it is reclamation rather than a guarantee for this key.

## Exporter evidence

The bounded client used for this qualification is `opentelemetry-otlp 0.32.0`,
the version pinned in this tree. Siglake itself builds its HTTP exporter without
the crate's `experimental-http-retry` feature. In that build the exporter calls
`export_http_once` once for every batch; the batch processor logs the returned
error and has already removed that batch from its buffer. HTTP `429`, `500` and
`503` therefore all lose that batch after one request in this shipped client.

The same crate's opt-in retry implementation supplies a bounded comparison for
both transports. Its own classification tests were run locally with both retry
features enabled:

```text
cargo test --manifest-path <opentelemetry-otlp-0.32.0>/Cargo.toml \
  --no-default-features \
  --features experimental-http-retry,experimental-grpc-retry,http-proto,trace,metrics,logs \
  retry_classification

23 passed; 0 failed
```

The tested decisions are:

| wire response | retry-enabled HTTP exporter | retry-enabled gRPC exporter |
|---|---|---|
| current `500` / `Internal` | retries with its exponential backoff | treats `Internal` as non-retryable; the batch is lost |
| `429` / `ResourceExhausted`, no hint | retries with exponential backoff | treats it as non-retryable; the batch is lost |
| `429` + `Retry-After` / `ResourceExhausted` + `RetryInfo` | waits the supplied HTTP delay, capped at 600 s | waits the `google.rpc.RetryInfo` delay, capped at 600 s |
| `503` / `Unavailable` | retries with exponential backoff; `Retry-After` is not read for a 5xx | retries with exponential backoff |

The crate's retry policy makes three retries by default and then reports that
the telemetry data is lost. A retryable code postpones the drop; it does not
preserve the batch indefinitely.

Siglake's gRPC mapper currently copies a delay into plain `retry-after` metadata.
That is not `google.rpc.RetryInfo` in the status details, and this client does
not read it. A future `429` mapping must add `Code::ResourceExhausted`; if it
promises a delay, it must encode the standard detail as well as the HTTP header.

This is one exporter implementation, pinned to one release. It is enough to
disprove transport parity and the premise that a status change makes every
exporter retry correctly. It is not a census of SDKs, collectors or agents.

## Decision and implementation for 0.2.0

The selected contract is HTTP `503` / gRPC `Unavailable` for the typed lane-cap
outcome. It carries no HTTP `Retry-After`, plain gRPC `retry-after` metadata or
`google.rpc.RetryInfo`. Logs and traces use the same mapping on HTTP and gRPC;
both Elasticsearch bulk routes use the HTTP mapping. A request for an existing
lane still succeeds after the cap is full. The queue-full path keeps its
transient retry hint, and genuine lane-creation or writer failures keep
`500` / `Internal`.

This status makes retry-enabled versions of the pinned exporter agree across
transports, accepting that a bounded retry policy can exhaust itself while the
same pod remains full. Recovery still needs routing or operator action. No lane
default, lifecycle, reclamation rule or exporter retry feature changed.

## Scope not measured

- No collector, agent or non-Rust SDK was run.
- No load balancer was used, so retry routing across pods was not measured.
- No live exporter was sent to an ingester after the response change; handler
  integration tests cover the selected HTTP and gRPC mapping directly.
- Exporter classification tests cover the code-selection behavior; they do not
  time the randomized backoff.
