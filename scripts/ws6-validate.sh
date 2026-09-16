#!/usr/bin/env bash
# WS-6 real-time queryability validation against the live EKS cluster.
#
# Proves that `events` queries see just-ingested rows that are sealed in the
# WAL but NOT yet committed to Iceberg, on BOTH the distributed `/api/v1/sql`
# coordinator path and the single-pod `/api/v1/sql/local` path. Method: pause
# the compactor (scale to 0) so commits can't happen, POST a marked batch, let
# the WAL seal, and assert the rows are queryable; then re-enable the compactor
# and confirm the rows persist (now served from Iceberg, dropped from the
# buffer) — count stays stable, no double-count.
set -euo pipefail

NS=${SIGLAKE_NAMESPACE:-siglake}
N=${N:-300}
# Unique per run so the warehouse is clean for this marker (the cluster is a
# persistent test warehouse; prior runs leave their own marked rows committed).
# Override MARKER to reuse a fixed value. All assertions are delta-based against
# the pre-run committed count regardless, so a non-clean marker still validates.
MARKER=${MARKER:-ws6rt-$(date +%s)}
SEAL_WAIT=${SEAL_WAIT:-8}   # > ingester.walSegmentMaxAgeSecs (5)

log() { printf '\n=== %s ===\n' "$*"; }

q() { # q <endpoint> <sql>  -> prints rows JSON
  curl -fsS -X POST "http://localhost:${QPORT}/api/v1/sql${1}" \
    -H 'Content-Type: application/json' -H 'X-Scope-OrgID: default' \
    --data "{\"query\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$2")}"
}
n_of() { python3 -c "import json,sys;print(json.load(sys.stdin)['rows'][0]['n'])"; }

# --- port-forwards ---------------------------------------------------------
kubectl -n "$NS" port-forward svc/siglake-ingester 18088:8088 >/tmp/pf-ing.log 2>&1 &
PF_ING=$!
kubectl -n "$NS" port-forward svc/siglake-query 18089:8089 >/tmp/pf-q.log 2>&1 &
PF_Q=$!
IPORT=18088; QPORT=18089
trap 'kill $PF_ING $PF_Q 2>/dev/null || true' EXIT
sleep 5

log "baseline total row count (Iceberg, buffer should add 0 right now)"
BEFORE=$(q "" "SELECT count(*) AS n FROM events" | n_of)
echo "  events total = $BEFORE"
MARKED_BEFORE=$(q "" "SELECT count(*) AS n FROM events WHERE source='$MARKER'" | n_of)
echo "  marked('$MARKER') before = $MARKED_BEFORE"

# --- pause the compactor so sealed segments stay un-committed ---------------
log "scale compactor to 0 (pause WAL->Iceberg commits)"
kubectl -n "$NS" scale deploy/siglake-compactor --replicas=0
kubectl -n "$NS" rollout status deploy/siglake-compactor --timeout=60s || true
sleep 3

# --- POST a marked batch ----------------------------------------------------
log "POST $N OTLP events (service.name=$MARKER, host-0..3)"
PAYLOAD=$(python3 - "$N" "$MARKER" <<'PY'
import json, sys
n=int(sys.argv[1]); marker=sys.argv[2]
rl=[]
for i in range(n):
    rl.append({
      "resource":{"attributes":[
        {"key":"host.name","value":{"stringValue":f"host-{i%4}"}},
        {"key":"service.name","value":{"stringValue":marker}}]},
      "scopeLogs":[{"scope":{"name":marker},"logRecords":[{
        "body":{"stringValue":f"{marker} {i}"},
        "attributes":[
          {"key":"sourcetype","value":{"stringValue":"ws6:json"}},
          {"key":"index","value":{"stringValue":"main"}}]}]}]})
print(json.dumps({"resourceLogs":rl}))
PY
)
curl -fsS -X POST "http://localhost:${IPORT}/v1/logs" \
  -H 'Content-Type: application/json' -H 'X-Scope-OrgID: default' \
  --data "$PAYLOAD" >/dev/null
echo "  accepted $N events"

log "wait ${SEAL_WAIT}s for the WAL to seal (age threshold) — still un-committed"
sleep "$SEAL_WAIT"

# Everything is asserted as a DELTA over the pre-run committed counts, so a
# warehouse that already holds rows for this marker still validates cleanly.
EXPECT_MARKED=$((MARKED_BEFORE + N))   # committed + un-committed buffer rows
EXPECT_TOTAL=$((BEFORE + N))
FAIL=0
chk() { # chk <label> <actual> <expected>
  if [ "$2" = "$3" ]; then echo "  PASS  $1: $2 (== $3)";
  else echo "  FAIL  $1: $2 (expected $3)"; FAIL=1; fi
}

# --- assert real-time visibility while compactor is down --------------------
log "DISTRIBUTED /api/v1/sql — marked count (expect committed $MARKED_BEFORE + buffer $N = $EXPECT_MARKED)"
D=$(q "" "SELECT count(*) AS n FROM events WHERE source='$MARKER'" | n_of)
chk "distributed marked" "$D" "$EXPECT_MARKED"

log "LOCAL /api/v1/sql/local — marked count (expect $EXPECT_MARKED)"
L=$(q "/local" "SELECT count(*) AS n FROM events WHERE source='$MARKER'" | n_of)
chk "local marked" "$L" "$EXPECT_MARKED"

log "DISTRIBUTED GROUP BY host (cross-shard merge; per-key sum must == $EXPECT_MARKED)"
GB=$(q "" "SELECT host, count(*) AS n FROM events WHERE source='$MARKER' GROUP BY host")
echo "$GB" | python3 -c "import json,sys; print('  rows:',json.load(sys.stdin)['rows'])"
GBSUM=$(echo "$GB" | python3 -c "import json,sys; print(sum(r['n'] for r in json.load(sys.stdin)['rows']))")
chk "GROUP BY host sum" "$GBSUM" "$EXPECT_MARKED"

log "total count delta (distributed): expect BEFORE+$N = $EXPECT_TOTAL"
TOTAL_NOW=$(q "" "SELECT count(*) AS n FROM events" | n_of)
chk "distributed total" "$TOTAL_NOW" "$EXPECT_TOTAL"

# --- re-enable compactor; confirm rows persist (Iceberg) + no double-count --
log "scale compactor back to 1 (resume commits)"
kubectl -n "$NS" scale deploy/siglake-compactor --replicas=1
kubectl -n "$NS" rollout status deploy/siglake-compactor --timeout=120s

log "poll until the marked rows are committed to Iceberg + count stays $EXPECT_MARKED (no double-count)"
for i in $(seq 1 30); do
  sleep 5
  AFTER=$(q "" "SELECT count(*) AS n FROM events WHERE source='$MARKER'" | n_of)
  echo "  attempt $i: marked = $AFTER"
  [ "$AFTER" = "$EXPECT_MARKED" ] && break
done
chk "marked after re-commit (no double-count)" "$AFTER" "$EXPECT_MARKED"

log "SUMMARY"
cat <<EOF
  baseline total            : $BEFORE
  marked committed before   : $MARKED_BEFORE
  marked, compactor paused  : distributed=$D local=$L  (expect $EXPECT_MARKED each)
  total during pause        : $TOTAL_NOW  (expect $EXPECT_TOTAL)
  marked, after re-commit   : $AFTER  (expect $EXPECT_MARKED — no double-count)
EOF
if [ "$FAIL" = "0" ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; exit 1; fi
