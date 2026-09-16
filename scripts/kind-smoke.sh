#!/usr/bin/env bash
#
# scripts/kind-smoke.sh — POST events to the kind-hosted ingester,
# poll the query-server until the row count lands, exercise the
# cost-explain endpoint, then verify the batch path round-trips.
#
# Prereq: scripts/kind-up.sh has been run.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kind-common.bash
source "$ROOT/scripts/kind-common.bash"

N="${1:-100}"
INGEST="http://localhost:8088"
QUERY="http://localhost:8089"
PAYLOAD_FILE="$(mktemp "${TMPDIR:-/tmp}/siglake-kind-smoke.XXXXXX")"

cleanup() {
  rm -f -- "$PAYLOAD_FILE"
}
trap cleanup EXIT

log() { printf '==> %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

log "POST $N events as OTLP/HTTP logs to /v1/logs"
python3 - "$N" >"$PAYLOAD_FILE" <<'PY'
import json, sys
n = int(sys.argv[1])
resource_logs = [{
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
} for i in range(n)]
print(json.dumps({"resourceLogs": resource_logs}))
PY
# curl -fsS fails on non-2xx; OTLP returns an empty success body, so 200 is the assert.
post_json_file "$INGEST/v1/logs" "$PAYLOAD_FILE" \
  -H 'X-Scope-OrgID: default' \
  >/dev/null
log "  accepted $N events (HTTP 200)"

log "explain a query"
curl -fsS -X POST "$QUERY/api/v1/sql/explain" \
  -H 'Content-Type: application/json' \
  --data '{"query":"SELECT count(*) FROM events"}' \
  | python3 -c '
import json, sys
r = json.load(sys.stdin)
print("  complexity={}  warnings={}".format(r["complexity_class"], len(r["warnings"])))'

log "poll query-server for count >= $N"
TOTAL=0
for i in $(seq 1 60); do
  RESP=$(curl -fsS -X POST "$QUERY/api/v1/sql" \
    -H 'Content-Type: application/json' \
    --data '{"query":"SELECT count(*) AS n FROM events"}')
  TOTAL=$(printf '%s' "$RESP" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['rows'][0]['n'])")
  if [ "$TOTAL" -ge "$N" ]; then
    log "  attempt $i: count=$TOTAL"
    break
  fi
  sleep 1
done
[ "$TOTAL" -ge "$N" ] || die "expected at least $N, got $TOTAL after 60s"

log "submit a batch job + poll for terminal state"
JOB=$(curl -fsS -X POST "$QUERY/api/v1/sql" \
  -H 'Content-Type: application/json' \
  --data '{"query":"SELECT count(*) FROM events","priority":"batch"}')
JOB_ID=$(printf '%s' "$JOB" | python3 -c "import json,sys; print(json.load(sys.stdin)['job_id'])")
echo "  job_id=$JOB_ID"
STATUS=
for _ in $(seq 1 30); do
  STATUS=$(curl -fsS "$QUERY/api/v1/jobs/$JOB_ID" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")
  if [ "$STATUS" = "succeeded" ]; then
    log "  batch job succeeded"
    break
  fi
  if [ "$STATUS" = "failed" ] || [ "$STATUS" = "timeout" ]; then
    die "batch job ended in $STATUS"
  fi
  sleep 1
done
[ "$STATUS" = "succeeded" ] || die "batch job did not finish after 30s (last status: $STATUS)"

log "smoke passed"
