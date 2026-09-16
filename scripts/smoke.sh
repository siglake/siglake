#!/usr/bin/env bash
#
# scripts/smoke.sh — end-to-end smoke test against a running siglake stack.
#
# Prereq: scripts/up.sh has been run and the stack is healthy.
#
# This script:
#   1. POSTs a small batch of synthetic OTLP/HTTP log records.
#   2. Waits for WAL roll + compactor cycle.
#   3. Lists the minio bucket to confirm Iceberg artifacts landed.
#   4. Runs SQL against the query-server and checks the total row count
#      matches what we sent.
#
# Exits non-zero on any unexpected output.

# The host-count formatter is the last command in a curl pipeline. Keep
# pipefail so an upstream request failure cannot be hidden by valid JSON output.
set -euo pipefail

N=${1:-100}

echo "==> POST $N events as OTLP/HTTP logs to /v1/logs"
PAYLOAD=$(python3 -c "
import json, sys
n = int(sys.argv[1])
resource_logs = []
for i in range(n):
    resource_logs.append({
        'resource': {'attributes': [
            {'key': 'host.name', 'value': {'stringValue': f'host-{i%4}'}},
            {'key': 'service.name', 'value': {'stringValue': 'app'}},
        ]},
        'scopeLogs': [{
            'scope': {'name': 'smoke'},
            'logRecords': [{
                'body': {'stringValue': f'e{i} status={i%3} latency_ms={(i*7)%200}'},
                'attributes': [
                    {'key': 'sourcetype', 'value': {'stringValue': 'json'}},
                    {'key': 'index', 'value': {'stringValue': 'main'}},
                ],
            }],
        }],
    })
print(json.dumps({'resourceLogs': resource_logs}))
" "$N")

# curl -fsS fails on non-2xx; OTLP returns an empty success body, so 200 is the assert.
curl -fsS -X POST http://localhost:8088/v1/logs \
    -H 'Content-Type: application/json' \
    -H 'X-Scope-OrgID: default' \
    -d "$PAYLOAD" >/dev/null
echo "  OTLP accepted $N events (HTTP 200)"

echo "==> waiting 8s for WAL roll + compactor cycle"
sleep 8

echo "==> minio bucket inventory"
docker run --rm --network siglake-dev_default --entrypoint /bin/sh \
  quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin --quiet && \
   mc ls --recursive local/siglake-warehouse" \
  | grep -E '\.(parquet|json|avro)$' \
  | sed 's/^/  /'

echo "==> SQL: total count (via query-server)"
TOTAL=$(curl -fsS -X POST http://localhost:8089/api/v1/sql \
  -H 'Content-Type: application/json' \
  --data '{"query":"SELECT count(*) AS n FROM events"}' \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['rows'][0]['n'])")

echo "  total rows in events table: $TOTAL"
if [ "$TOTAL" -lt "$N" ]; then
  echo "  FAIL: expected at least $N, got $TOTAL"
  exit 1
fi

echo "==> SQL: count by host"
curl -fsS -X POST http://localhost:8089/api/v1/sql \
  -H 'Content-Type: application/json' \
  --data '{"query":"SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC"}' \
  | python3 -c '
import json, sys
r = json.load(sys.stdin)
rows = r["rows"]
for row in rows:
    print("  {}: {}".format(row["host"], row["n"]))

expected_per_host = int(sys.argv[1]) // 4
grouped_total = sum(row["n"] for row in rows)
if len(rows) != 4 or any(row["n"] != expected_per_host for row in rows):
    print("  FAIL: expected exactly four host groups of {} rows".format(expected_per_host), file=sys.stderr)
    sys.exit(1)
if grouped_total != int(sys.argv[2]):
    print("  FAIL: grouped counts sum to {}, total count is {}".format(grouped_total, sys.argv[2]), file=sys.stderr)
    sys.exit(1)' "$N" "$TOTAL"

echo
echo "smoke test passed"
