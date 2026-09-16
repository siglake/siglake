#!/usr/bin/env bash
#
# scripts/up.sh — bring up the local siglake stress-test environment.
#
# - Builds the siglake image (cached after first run).
# - Starts postgres, minio, ingester, compactor via docker compose.
# - Waits until ingester /healthz returns 200.
# - Prints connection info.
#
# Every published host port has a SIGLAKE_*_HOST_PORT override; their defaults
# are listed in scripts/compose-common.bash. SIGLAKE_COMPOSE_PROJECT selects the
# compose project for both this script and scripts/down.sh.
#
# SIGLAKE_OBJECT_STORE=garage runs the warehouse on Garage instead of MinIO
# (task #2958's comparison arm). MinIO still starts — the siglake services
# declare a static depends_on — but takes no traffic; stop it after this script
# returns for a matched measurement.
#
# Tear down with scripts/down.sh.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/deploy/docker-compose.yml"
# shellcheck source=scripts/compose-common.bash
source "$ROOT/scripts/compose-common.bash"

case "${1:-}" in
  --preflight-only)
    siglake_compose_preflight
    exit
    ;;
  -h|--help)
    grep '^#' "$0" | sed 's/^# \{0,1\}//'
    exit
    ;;
  "") ;;
  *)
    echo "unknown arg: $1" >&2
    exit 1
    ;;
esac

siglake_compose_preflight

if [ "$SIGLAKE_OBJECT_STORE" = garage ]; then
  # garage-init is profiled, so it cannot sit in the siglake services'
  # depends_on without pulling garage into the default MinIO arm. Order it here
  # instead: `run --rm` blocks and returns the probe's exit code.
  echo "==> starting garage and waiting for its default bucket"
  docker compose -p "$SIGLAKE_COMPOSE_PROJECT" -f "$COMPOSE" up -d garage
  docker compose -p "$SIGLAKE_COMPOSE_PROJECT" -f "$COMPOSE" run --rm garage-init
fi

echo "==> docker compose up (build + start)"
docker compose -p "$SIGLAKE_COMPOSE_PROJECT" -f "$COMPOSE" up --build -d

echo "==> waiting for ingest server to become healthy"
for i in $(seq 1 60); do
  if curl -fsS "http://localhost:$SIGLAKE_INGEST_HOST_PORT/healthz" >/dev/null 2>&1; then
    echo "  ingest up after ${i}s"
    break
  fi
  if [ "$i" = "60" ]; then
    echo "  ingest didn't become healthy after 60s; recent ingester logs:"
    docker compose -p "$SIGLAKE_COMPOSE_PROJECT" -f "$COMPOSE" logs --tail 50 ingester
    exit 1
  fi
  sleep 1
done

cat <<EOF

siglake stress-test environment is up.

  OTLP/HTTP ingest:  http://localhost:$SIGLAKE_INGEST_HOST_PORT/v1/logs
  OTLP/gRPC ingest:  http://localhost:$SIGLAKE_OTLP_GRPC_HOST_PORT
  ingest health:     curl http://localhost:$SIGLAKE_INGEST_HOST_PORT/healthz
  query API:         http://localhost:$SIGLAKE_QUERY_HOST_PORT/api/v1/sql
  minio S3 API:      http://localhost:$SIGLAKE_MINIO_HOST_PORT
  minio console:     http://localhost:$SIGLAKE_MINIO_CONSOLE_HOST_PORT  (minioadmin / minioadmin)
  postgres:          localhost:$SIGLAKE_PG_HOST_PORT         (siglake / siglake / siglake)
  warehouse store:   $SIGLAKE_OBJECT_STORE at $SIGLAKE_S3_HOST_ENDPOINT

Send a test event (single-tenant by default, so no X-Scope-OrgID needed):
  curl -X POST http://localhost:$SIGLAKE_INGEST_HOST_PORT/v1/logs \\
    -H 'Content-Type: application/json' \\
    -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"host.name","value":{"stringValue":"h1"}}]},"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hello"}}]}]}]}'

Drive load (next phase):
  scripts/loadgen.sh --eps 5000 --duration 60s

Watch logs:
  docker compose -p $SIGLAKE_COMPOSE_PROJECT -f deploy/docker-compose.yml logs -f compactor

Tear down:
  scripts/down.sh

EOF

if [ "$SIGLAKE_OBJECT_STORE" = garage ]; then
  cat <<EOF
Garage arm (task #2958). Garage has no web console; 3903 is the admin API.

  garage S3 API:     http://localhost:$SIGLAKE_GARAGE_HOST_PORT
  garage admin API:  http://localhost:$SIGLAKE_GARAGE_ADMIN_HOST_PORT
  garage metrics:    http://localhost:$SIGLAKE_GARAGE_ADMIN_HOST_PORT/metrics

MinIO is up but idle in this arm. Stop it before a throughput measurement so
the two arms run the same number of containers:

  docker compose -p $SIGLAKE_COMPOSE_PROJECT -f deploy/docker-compose.yml stop minio

EOF
fi
