#!/usr/bin/env bash
# WS-8 framed WAL + WS-6 hot-cache validation against the live EKS cluster.
#
# 1. WS-8 framing: with the compactor paused, POST a marked batch, let the WAL
#    seal, and assert a sealed segment on the shared WAL volume begins with the
#    framed magic "KWAL" (header + zstd body) — not raw Arrow IPC.
# 2. WS-8 read path: re-enable the compactor and confirm the marked rows commit
#    to Iceberg (the compactor decodes framed segments) — count rises by N.
# 3. WS-6 hot caches: the last_values() / distinct_values('host') UDTFs serve
#    the just-ingested series from the query-tier WAL-tail cache.
set -euo pipefail

NS=${SIGLAKE_NAMESPACE:-siglake}
N=${N:-200}
MARKER=${MARKER:-ws8-$(date +%s)}
SEAL_WAIT=${SEAL_WAIT:-8}   # > ingester.walSegmentMaxAgeSecs (5)

log() { printf '\n=== %s ===\n' "$*"; }
FAIL=0
chk() { if [ "$2" = "$3" ]; then echo "  PASS  $1: $2 (== $3)"; else echo "  FAIL  $1: $2 (expected $3)"; FAIL=1; fi; }

q() { # q <endpoint> <sql>
  curl -fsS -X POST "http://localhost:${QPORT}/api/v1/sql${1}" \
    -H 'Content-Type: application/json' -H 'X-Scope-OrgID: default' \
    --data "{\"query\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$2")}"
}
n_of() { python3 -c "import json,sys;print(json.load(sys.stdin)['rows'][0]['n'])"; }

kubectl -n "$NS" port-forward svc/siglake-ingester 18088:8088 >/tmp/pf-ing8.log 2>&1 & PF_ING=$!
kubectl -n "$NS" port-forward svc/siglake-query 18089:8089 >/tmp/pf-q8.log 2>&1 & PF_Q=$!
IPORT=18088; QPORT=18089
# Always restore the compactor + kill port-forwards, even on an early failure,
# so a mid-run abort can't leave WAL→Iceberg commits paused.
cleanup() {
  kubectl -n "$NS" scale deploy/siglake-compactor --replicas=1 >/dev/null 2>&1 || true
  kill "$PF_ING" "$PF_Q" 2>/dev/null || true
}
trap cleanup EXIT
sleep 5

ING=$(kubectl -n "$NS" get pods --no-headers | grep ingester | awk '{print $1}' | head -1)
echo "ingester pod: $ING"

log "baseline marked count"
BEFORE=$(q "" "SELECT count(*) AS n FROM events WHERE source='$MARKER'" | n_of)
echo "  marked before = $BEFORE"

log "pause compactor (keep sealed segments around for inspection)"
kubectl -n "$NS" scale deploy/siglake-compactor --replicas=0
kubectl -n "$NS" rollout status deploy/siglake-compactor --timeout=60s || true
sleep 3

log "POST $N OTLP events (service.name=$MARKER, host-0..3)"
PAYLOAD=$(python3 - "$N" "$MARKER" <<'PY'
import json, sys
n=int(sys.argv[1]); marker=sys.argv[2]
rl=[]
for i in range(n):
    rl.append({"resource":{"attributes":[
        {"key":"host.name","value":{"stringValue":f"host-{i%4}"}},
        {"key":"service.name","value":{"stringValue":marker}}]},
      "scopeLogs":[{"scope":{"name":marker},"logRecords":[{
        "body":{"stringValue":f"{marker} seq {i}"},
        "attributes":[{"key":"sourcetype","value":{"stringValue":"ws8:json"}}]}]}]})
print(json.dumps({"resourceLogs":rl}))
PY
)
curl -fsS -X POST "http://localhost:${IPORT}/v1/logs" \
  -H 'Content-Type: application/json' -H 'X-Scope-OrgID: default' --data "$PAYLOAD" >/dev/null
echo "  accepted $N events"

log "wait ${SEAL_WAIT}s for the WAL to seal"
sleep "$SEAL_WAIT"

# --- WS-8 framing check: a sealed segment must begin with magic "KWAL" --------
log "WS-8: assert a sealed WAL segment is framed (magic KWAL)"
MAGIC=$(kubectl -n "$NS" exec "$ING" -- sh -c '
  for d in /var/lib/siglake/wal/sealed /var/lib/siglake/wal/*/sealed; do
    f=$(ls "$d"/*.arrow 2>/dev/null | head -1)
    if [ -n "$f" ]; then head -c 4 "$f"; exit 0; fi
  done' 2>/dev/null || true)
echo "  first 4 bytes of a sealed segment: '${MAGIC}'"
chk "sealed segment framed magic" "$MAGIC" "KWAL"

# --- WS-6 hot caches (served from the WAL-tail cache) -------------------------
log "WS-6 hot caches: distinct_values('host') contains the marker hosts"
sleep 2   # hot-cache refresh interval is 1s
DV_RAW=$(q "" "SELECT value FROM distinct_values('host') WHERE value LIKE 'host-%'" || echo '{}')
DV=$(echo "$DV_RAW" | python3 -c "import json,sys
try: rows=json.load(sys.stdin)['rows']
except Exception: rows=[]
print(sorted(r['value'] for r in rows))")
echo "  distinct hosts (host-*): $DV"
if echo "$DV" | python3 -c "import sys; d=eval(sys.stdin.read()); sys.exit(0 if all(f'host-{i}' in d for i in range(4)) else 1)"; then
  echo "  PASS  distinct_values has host-0..3"
else
  echo "  FAIL  missing marker hosts (raw: $DV_RAW)"; FAIL=1
fi

log "WS-6 hot caches: last_values() returns a row per marker host"
LVN=$(q "" "SELECT count(*) AS n FROM last_values() WHERE host LIKE 'host-%'" 2>/dev/null | n_of 2>/dev/null || echo 0)
echo "  last_values rows for host-*: $LVN (expect >= 4)"
if [ "$LVN" -ge 4 ]; then echo "  PASS  last_values populated"; else echo "  FAIL  last_values empty"; FAIL=1; fi

# --- WS-8 read path: compactor commits framed segments -----------------------
log "re-enable compactor; confirm framed segments commit to Iceberg"
kubectl -n "$NS" scale deploy/siglake-compactor --replicas=1
kubectl -n "$NS" rollout status deploy/siglake-compactor --timeout=120s
EXPECT=$((BEFORE + N))
AFTER=0
for i in $(seq 1 30); do
  sleep 5
  AFTER=$(q "" "SELECT count(*) AS n FROM events WHERE source='$MARKER'" | n_of)
  echo "  attempt $i: marked committed = $AFTER"
  [ "$AFTER" = "$EXPECT" ] && break
done
chk "framed segments committed (compactor decodes them)" "$AFTER" "$EXPECT"

log "SUMMARY"
if [ "$FAIL" = "0" ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; exit 1; fi
