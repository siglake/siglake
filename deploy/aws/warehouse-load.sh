#!/usr/bin/env bash
# Replay the deterministic 3M/72h corpus N times into the live ingester
# via a port-forward, to build a large warehouse for query perf rounds.
# Exact final counts are MEASURED afterward (not assumed), so a partial
# replay cannot corrupt bench assertions.
set -uo pipefail

NS="${SIGLAKE_NAMESPACE:-siglake}"
CORPUS="${CORPUS:-/tmp/siglake-corpus-72h-3m-r31}"
BIN="${BIN:-target/release/siglake-corpus}"
REPLAYS="${REPLAYS:-34}"
BATCH="${BATCH:-1000}"
CONC="${CONC:-32}"
PORT=8088

log() { printf '==> %s\n' "$*"; }

cleanup() { [[ -n "${PF_PID:-}" ]] && kill "$PF_PID" 2>/dev/null || true; }
trap cleanup EXIT

log "starting port-forward to svc/siglake-ingester :$PORT"
kubectl -n "$NS" port-forward svc/siglake-ingester "$PORT:$PORT" >/tmp/wh-pf.log 2>&1 &
PF_PID=$!
sleep 5

for i in $(seq 1 "$REPLAYS"); do
  log "replay $i/$REPLAYS $(date -u +%H:%M:%S)"
  for attempt in 1 2 3; do
    if "$BIN" load --corpus "$CORPUS" --target "http://localhost:$PORT" \
         --batch-size "$BATCH" --concurrency "$CONC"; then
      break
    fi
    log "replay $i attempt $attempt failed; restarting port-forward and retrying"
    kill "$PF_PID" 2>/dev/null || true
    kubectl -n "$NS" port-forward svc/siglake-ingester "$PORT:$PORT" >/tmp/wh-pf.log 2>&1 &
    PF_PID=$!
    sleep 5
  done
done

log "all $REPLAYS replay passes attempted; letting compaction settle (90s)"
sleep 90
log "WAREHOUSE LOAD COMPLETE"
