#!/usr/bin/env bash
#
# deploy/aws/smoke.sh — end-to-end smoke test against a live AWS install.
#
# Steps:
#   1. port-forward to the ingester + query-server.
#   2. POST $N synthetic events as OTLP/HTTP logs to /v1/logs.
#   3. Poll the query-server's /api/v1/sql until count(*) >= $N.
#   4. SQL `SELECT host, count(*) GROUP BY host` and assert the per-host
#      counts sum to the full row count (cross-shard merge check).
#
# Exits 0 on success, non-zero on any failure.
#
# Usage:
#   deploy/aws/smoke.sh [N]
#
# Environment overrides:
#   SIGLAKE_RELEASE     helm release name (default: siglake)
#   SIGLAKE_NAMESPACE   k8s namespace     (default: siglake)
#   SIGLAKE_QUERY_TOKEN bearer token if the chart was installed with auth.
#                      Omit when query.tokens is open.

set -euo pipefail

N="${1:-100}"
RELEASE="${SIGLAKE_RELEASE:-siglake}"
NAMESPACE="${SIGLAKE_NAMESPACE:-siglake}"
TOKEN="${SIGLAKE_QUERY_TOKEN:-}"

INGESTER_SVC="${RELEASE}-ingester"
QUERY_SVC="${RELEASE}-query"

INGESTER_LOCAL_PORT=18088
QUERY_LOCAL_PORT=18089

log() { printf '==> %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

cleanup() {
  if [[ -n "${INGESTER_PF_PID:-}" ]]; then kill "$INGESTER_PF_PID" 2>/dev/null || true; fi
  if [[ -n "${QUERY_PF_PID:-}"   ]]; then kill "$QUERY_PF_PID"   2>/dev/null || true; fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1. port-forward
# ---------------------------------------------------------------------------
log "port-forward ingester $INGESTER_LOCAL_PORT -> $INGESTER_SVC:8088"
kubectl -n "$NAMESPACE" port-forward "svc/$INGESTER_SVC" "$INGESTER_LOCAL_PORT:8088" >/dev/null 2>&1 &
INGESTER_PF_PID=$!

log "port-forward query   $QUERY_LOCAL_PORT -> $QUERY_SVC:8089"
kubectl -n "$NAMESPACE" port-forward "svc/$QUERY_SVC" "$QUERY_LOCAL_PORT:8089" >/dev/null 2>&1 &
QUERY_PF_PID=$!

# Wait for both forwards to accept connections.
for endpoint in \
  "http://localhost:$INGESTER_LOCAL_PORT/healthz" \
  "http://localhost:$QUERY_LOCAL_PORT/healthz"
do
  for _ in $(seq 1 30); do
    if curl -fsS "$endpoint" >/dev/null 2>&1; then break; fi
    sleep 1
  done
  curl -fsS "$endpoint" >/dev/null \
    || die "port-forward never came up for $endpoint"
done

# ---------------------------------------------------------------------------
# Auth header for the query-server (optional).
# ---------------------------------------------------------------------------
AUTH_ARGS=()
if [[ -n "$TOKEN" ]]; then
  AUTH_ARGS=(-H "Authorization: Bearer $TOKEN")
fi

# ---------------------------------------------------------------------------
# 2. POST N events
# ---------------------------------------------------------------------------
log "POST $N events as OTLP/HTTP logs to /v1/logs"
PAYLOAD=$(python3 - "$N" <<'PY'
import json, sys
n = int(sys.argv[1])
# One resourceLogs per event so host/source vary per record. Mirrors the
# ingester's OTLP->column mapping (host.name->host, service.name->source,
# body->raw, the sourcetype/index record attrs).
resource_logs = []
for i in range(n):
    resource_logs.append({
        "resource": {"attributes": [
            {"key": "host.name", "value": {"stringValue": f"host-{i % 4}"}},
            {"key": "service.name", "value": {"stringValue": "smoke"}},
        ]},
        "scopeLogs": [{
            "scope": {"name": "smoke"},
            "logRecords": [{
                "body": {"stringValue": f"smoke {i} status={i%3}"},
                "attributes": [
                    {"key": "sourcetype", "value": {"stringValue": "smoke:json"}},
                    {"key": "index", "value": {"stringValue": "main"}},
                ],
            }],
        }],
    })
print(json.dumps({"resourceLogs": resource_logs}))
PY
)

# OTLP returns the standard (empty) ExportLogsServiceResponse on success;
# `curl -fsS` already fails the script on any non-2xx, so a 200 is the assert.
# Tenancy is header-based: target the `default` tenant explicitly.
# NB: payload goes via stdin (`--data @-`) — a single argv element is capped
# at 128 KiB (MAX_ARG_STRLEN), which an N≳400 payload exceeds
# ("Argument list too long").
printf '%s' "$PAYLOAD" | curl -fsS -X POST "http://localhost:$INGESTER_LOCAL_PORT/v1/logs" \
  -H 'Content-Type: application/json' \
  -H 'X-Scope-OrgID: default' \
  --data @- >/dev/null
log "  OTLP accepted $N events (HTTP 200)"

# ---------------------------------------------------------------------------
# 3. Poll the query-server for the committed row count
# ---------------------------------------------------------------------------
log "polling query-server SQL count(*) for up to 60s"
EXPECTED=$N
TOTAL=0
for attempt in $(seq 1 60); do
  RESP=$(curl -fsS -X POST "http://localhost:$QUERY_LOCAL_PORT/api/v1/sql" \
    "${AUTH_ARGS[@]}" \
    -H 'Content-Type: application/json' \
    --data '{"query":"SELECT count(*) AS n FROM events"}')
  TOTAL=$(printf '%s' "$RESP" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['rows'][0]['n'])")
  if [ "$TOTAL" -ge "$EXPECTED" ]; then
    log "  attempt $attempt: count=$TOTAL >= $EXPECTED"
    break
  fi
  sleep 1
done

[ "$TOTAL" -ge "$EXPECTED" ] \
  || die "expected at least $EXPECTED rows, saw $TOTAL after 60s"

# ---------------------------------------------------------------------------
# 4. SQL GROUP BY host — exercises the distributed /api/v1/sql cross-shard
#    merge: the per-host counts must sum to the full committed row count.
# ---------------------------------------------------------------------------
log "SQL: SELECT host, count(*) AS n FROM events GROUP BY host"
curl -fsS -X POST "http://localhost:$QUERY_LOCAL_PORT/api/v1/sql" \
  "${AUTH_ARGS[@]}" \
  -H 'Content-Type: application/json' \
  --data '{"query":"SELECT host, count(*) AS n FROM events GROUP BY host"}' \
  | TOTAL="$TOTAL" python3 -c '
import json, os, sys
r = json.load(sys.stdin)
hosts = {row["host"]: row["n"] for row in r["rows"]}
print("  host counts:", hosts)
assert len(hosts) >= 4, f"expected >=4 hosts, got {hosts}"
assert all(c > 0 for c in hosts.values()), f"unexpected zeros: {hosts}"
# Cross-shard merge invariant: per-key counts sum to the full row count.
total = int(os.environ["TOTAL"])
assert sum(hosts.values()) == total, f"GROUP BY sum {sum(hosts.values())} != count(*) {total}"
print("  cross-shard merge OK: sum(per-host) ==", total)
'

log "smoke passed"
