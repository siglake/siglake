#!/usr/bin/env bash
#
# scripts/loadgen.sh — drive ingest + concurrent SQL queries against the
# running siglake stack and dump a final telemetry snapshot.
#
# Prereqs: scripts/up.sh has been run, and the release load generator exists
# (`cargo build --release -p siglake-loadgen`). Docker-only users can instead
# run `docker compose exec ingester siglake-loadgen ...`.
#
# Phases:
#   1. Pre-flight: confirm ingest + Prometheus are responding.
#   2. Spawn `siglake-loadgen` for sustained ingest at --eps for --duration.
#   3. In parallel, run SQL queries every --query-interval and time them.
#   4. After ingest finishes, drain time: wait for compactor + final query.
#   5. Dump:
#       - Loadgen summary (events/sec achieved, latency percentiles)
#       - Query latency stats
#       - Selected Prometheus counters/histograms
#       - Iceberg row count (via SQL)
#
# Defaults are tuned for a laptop kind/docker-compose.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --- args -------------------------------------------------------------------

EPS=5000
DURATION=30s
WORKERS=4
BATCH_SIZE=50
QUERY_INTERVAL=2
TARGET=http://localhost:8088
QUERY_TARGET=http://localhost:8089
DRAIN_SECS=10

while [ $# -gt 0 ]; do
  case "$1" in
    --eps) EPS=$2; shift 2 ;;
    --duration) DURATION=$2; shift 2 ;;
    --workers) WORKERS=$2; shift 2 ;;
    --batch-size) BATCH_SIZE=$2; shift 2 ;;
    --query-interval) QUERY_INTERVAL=$2; shift 2 ;;
    --target) TARGET=$2; shift 2 ;;
    --query-target) QUERY_TARGET=$2; shift 2 ;;
    --drain-secs) DRAIN_SECS=$2; shift 2 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

LOADGEN_BIN="${CARGO_TARGET_DIR:-$ROOT/target}/release/siglake-loadgen"
if [ ! -x "$LOADGEN_BIN" ]; then
  cat >&2 <<EOF
siglake-loadgen is missing or not executable at $LOADGEN_BIN.
Build it with:
  cargo build --release -p siglake-loadgen
Docker-only alternative:
  docker compose exec ingester siglake-loadgen ...
EOF
  exit 1
fi

# --- pre-flight -------------------------------------------------------------

echo "==> pre-flight"
curl -fsS "$TARGET/healthz" >/dev/null \
  || { echo "  ingest not responding at $TARGET — run scripts/up.sh first" >&2; exit 1; }
curl -fsS http://localhost:9090/-/ready >/dev/null \
  || { echo "  Prometheus not responding at :9090" >&2; exit 1; }
echo "  ingest + Prometheus ready"

# --- background SQL query worker ------------------------------------------

QUERY_LOG=$(mktemp -t siglake-queries.XXXXXX)
QUERY_QUERIES=(
  "SELECT count(*) FROM events"
  "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC"
  "SELECT host, raw FROM events ORDER BY timestamp DESC LIMIT 10"
  "SELECT host, count(*) AS n FROM events WHERE raw LIKE '%status=500%' GROUP BY host"
)
RUNNING=1
trap 'RUNNING=0' INT TERM

(
  while [ "$RUNNING" -eq 1 ]; do
    Q=${QUERY_QUERIES[$RANDOM % ${#QUERY_QUERIES[@]}]}
    T0=$(python3 -c "import time; print(time.monotonic_ns())")
    OUT=$(curl -fsS -X POST "$QUERY_TARGET/api/v1/sql" \
        -H 'Content-Type: application/json' \
        --data "$(python3 -c 'import json,sys; print(json.dumps({"query": sys.argv[1]}))' "$Q")" \
        2>/dev/null) || OUT="ERROR"
    T1=$(python3 -c "import time; print(time.monotonic_ns())")
    DT_MS=$(( (T1 - T0) / 1000000 ))
    if [ "$OUT" = "ERROR" ]; then
      echo "ERR ${Q}" >> "$QUERY_LOG"
    else
      echo "${DT_MS} ${Q}" >> "$QUERY_LOG"
    fi
    sleep "$QUERY_INTERVAL"
  done
) &
QUERY_PID=$!

# --- ingest load ------------------------------------------------------------

echo
echo "==> ingest: $EPS EPS for $DURATION ($WORKERS workers, batch=$BATCH_SIZE)"
LOADGEN_LOG=$(mktemp -t siglake-loadgen.XXXXXX)
"$LOADGEN_BIN" \
  --target "$TARGET" \
  --eps "$EPS" \
  --duration "$DURATION" \
  --workers "$WORKERS" \
  --batch-size "$BATCH_SIZE" \
  2>&1 | tee "$LOADGEN_LOG"

# Stop the query worker.
RUNNING=0
kill "$QUERY_PID" 2>/dev/null || true
wait "$QUERY_PID" 2>/dev/null || true

echo
echo "==> drain: waiting ${DRAIN_SECS}s for compactor to flush WAL"
sleep "$DRAIN_SECS"

# --- final telemetry ---------------------------------------------------------

echo
echo "==> query latency (ms): observed during the run"
if [ -s "$QUERY_LOG" ]; then
  python3 - <<PY "$QUERY_LOG"
import sys, statistics
lats = []
errs = 0
with open(sys.argv[1]) as f:
    for line in f:
        parts = line.strip().split(None, 1)
        if not parts: continue
        if parts[0] == "ERR":
            errs += 1
        else:
            try: lats.append(int(parts[0]))
            except ValueError: pass
n = len(lats)
if n == 0:
    print(f"  no completed queries (errors: {errs})")
else:
    lats.sort()
    p50 = lats[int(n*0.5)]
    p95 = lats[min(int(n*0.95), n-1)]
    p99 = lats[min(int(n*0.99), n-1)]
    mx  = lats[-1]
    print(f"  queries: {n}  errors: {errs}  p50={p50}ms p95={p95}ms p99={p99}ms max={mx}ms")
PY
fi

echo
echo "==> ingester metrics"
curl -s http://localhost:9100/metrics \
  | grep -E '^siglake_(events_accepted|ingest_requests|wal_segments_sealed|wal_rows_written|wal_bytes_written)_total' \
  | sed 's/^/  /'

echo
echo "==> compactor metrics"
curl -s http://localhost:9101/metrics \
  | grep -E '^siglake_compactor_(cycles|segments|rows|commit_duration_seconds_count|commit_duration_seconds_sum)' \
  | sed 's/^/  /'

echo
echo "==> Iceberg row count"
curl -fsS -X POST "$QUERY_TARGET/api/v1/sql" \
  -H 'Content-Type: application/json' \
  --data '{"query":"SELECT count(*) AS n FROM events"}' 2>/dev/null \
  | python3 -c "import json,sys; print('  events:', json.load(sys.stdin)['rows'][0]['n'])"

echo
echo "loadgen run complete."
echo "  loadgen log: $LOADGEN_LOG"
echo "  query log:   $QUERY_LOG"
echo "  Prometheus:  http://localhost:9090/graph"
echo "  minio UI:    http://localhost:9001  (minioadmin / minioadmin)"
