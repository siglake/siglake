#!/usr/bin/env bash
# WS-5 inverted-index validation against the live EKS cluster.
#
# Requires the compactor deployed with compactor.invertedIndex.enabled=true
# (SIGLAKE_INVERTED_INDEX=1) so committed events files carry the per-file index
# blob in their Parquet footer KV.
#
# 1. Build: ingest a marked batch carrying a rare token, let the compactor
#    commit, and confirm `siglake_index_build_bytes` advanced on the compactor
#    (the index was built into the data file).
# 2. Correctness: `raw LIKE '%<rare-token>%'` returns exactly the marked rows
#    (the row-selection superset + the engine's exact re-check).
# 3. Pruning fired: `siglake_iceberg_inverted_index_used_total` advanced on a
#    query pod (the index produced a row selection at scan time).
set -euo pipefail

NS=${SIGLAKE_NAMESPACE:-siglake}
N=${N:-300}
TOKEN=${TOKEN:-ws5tok$(date +%s)}   # a rare, delimiter-free, >=3-char token

log() { printf '\n=== %s ===\n' "$*"; }
FAIL=0
chk() { if [ "$2" = "$3" ]; then echo "  PASS  $1: $2 (== $3)"; else echo "  FAIL  $1: $2 (expected $3)"; FAIL=1; fi; }

q() {
  curl -fsS -X POST "http://localhost:${QPORT}/api/v1/sql${1}" \
    -H 'Content-Type: application/json' -H 'X-Scope-OrgID: default' \
    --data "{\"query\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$2")}"
}
n_of() { python3 -c "import json,sys;print(json.load(sys.stdin)['rows'][0]['n'])"; }
# Sum a counter family across a pod's /metrics (handles labelled series).
metric_sum() { # metric_sum <pod> <metric-name>
  kubectl -n "$NS" exec "$1" -- sh -c "curl -fsS localhost:9105/metrics 2>/dev/null || curl -fsS localhost:9101/metrics 2>/dev/null" 2>/dev/null \
    | awk -v m="$2" '$1 ~ "^"m"(\\{|$| )" {s+=$2} END {printf "%d", s+0}'
}

kubectl -n "$NS" port-forward svc/siglake-ingester 18088:8088 >/tmp/pf-ing5.log 2>&1 & PF_ING=$!
kubectl -n "$NS" port-forward svc/siglake-query 18089:8089 >/tmp/pf-q5.log 2>&1 & PF_Q=$!
IPORT=18088; QPORT=18089
trap 'kill $PF_ING $PF_Q 2>/dev/null || true' EXIT
sleep 5

COMPACTOR=$(kubectl -n "$NS" get pods --no-headers | grep compactor | awk '{print $1}' | head -1)
log "preconditions"
echo "  compactor pod: $COMPACTOR"
INV_ENV=$(kubectl -n "$NS" get deploy siglake-compactor -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="SIGLAKE_INVERTED_INDEX")].value}' 2>/dev/null || true)
echo "  SIGLAKE_INVERTED_INDEX on compactor = '${INV_ENV:-<unset>}'"
if [ "${INV_ENV:-}" != "1" ]; then
  echo "  FAIL  compactor.invertedIndex.enabled must be true for this round"; exit 1
fi
BUILD_BEFORE=$(metric_sum "$COMPACTOR" siglake_index_build_bytes_count 2>/dev/null || echo 0)
echo "  index build samples before = ${BUILD_BEFORE:-0}"

log "POST $N OTLP events carrying the rare token '$TOKEN'"
PAYLOAD=$(python3 - "$N" "$TOKEN" <<'PY'
import json, sys
n=int(sys.argv[1]); tok=sys.argv[2]
rl=[]
for i in range(n):
    rl.append({"resource":{"attributes":[{"key":"host.name","value":{"stringValue":f"host-{i%4}"}}]},
      "scopeLogs":[{"logRecords":[{"body":{"stringValue":f"audit {tok} event number {i}"}}]}]})
print(json.dumps({"resourceLogs":rl}))
PY
)
curl -fsS -X POST "http://localhost:${IPORT}/v1/logs" \
  -H 'Content-Type: application/json' -H 'X-Scope-OrgID: default' --data "$PAYLOAD" >/dev/null
echo "  accepted $N events"

log "wait for the compactor to commit (index built into the data file)"
AFTER=0
for i in $(seq 1 30); do
  sleep 5
  AFTER=$(q "" "SELECT count(*) AS n FROM events WHERE raw LIKE '%$TOKEN%'" | n_of)
  echo "  attempt $i: rows matching '$TOKEN' = $AFTER"
  [ "$AFTER" = "$N" ] && break
done
chk "LIKE '%$TOKEN%' returns the marked rows" "$AFTER" "$N"

log "index was built (compactor siglake_index_build_bytes advanced)"
# The LIKE check above can pass instantly off the WS-6 WAL buffer — BEFORE the
# compactor commits — so poll the build counter on its own clock.
BUILD_AFTER=${BUILD_BEFORE:-0}
for i in $(seq 1 24); do
  BUILD_AFTER=$(metric_sum "$COMPACTOR" siglake_index_build_bytes_count 2>/dev/null || echo 0)
  [ "${BUILD_AFTER:-0}" -gt "${BUILD_BEFORE:-0}" ] && break
  sleep 5
done
echo "  index build samples after = ${BUILD_AFTER:-0}"
if [ "${BUILD_AFTER:-0}" -gt "${BUILD_BEFORE:-0}" ]; then echo "  PASS  index build advanced"; else echo "  FAIL  no index built (build_bytes did not advance)"; FAIL=1; fi

log "pruning fired at query time (siglake_iceberg_inverted_index_used_total > 0)"
# Run the selective query a few times across both query pods, then sum the counter.
for _ in 1 2 3; do q "" "SELECT count(*) AS n FROM events WHERE raw LIKE '%$TOKEN%'" >/dev/null; done
USED=0
for pod in $(kubectl -n "$NS" get pods --no-headers | grep '^siglake-query' | awk '{print $1}'); do
  u=$(metric_sum "$pod" siglake_iceberg_inverted_index_used_total 2>/dev/null || echo 0)
  echo "  $pod inverted_index_used_total = ${u:-0}"
  USED=$((USED + ${u:-0}))
done
if [ "$USED" -gt 0 ]; then echo "  PASS  inverted index pruned at scan time"; else echo "  FAIL  index never used at query time"; FAIL=1; fi

log "SUMMARY"
if [ "$FAIL" = "0" ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; exit 1; fi
