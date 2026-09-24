#!/usr/bin/env bash
# Cancellation soak: does a query server survive clients that go away?
#
# Exercises cancellation of in-flight queries and recovery of admission slots.
# Requires a dedicated test deployment and an expensive query; preflight refuses
# workloads that finish before the client disconnects.
#
# The soak alternates two things a real deployment does constantly:
#   - honest queries, whose latency must NOT drift upward over the run
#   - abandoned queries (client hangs up mid-flight), the leak trigger
#
# FAILS the run on any of:
#   - the server dying (OOM or otherwise)
#   - exec_pool_in_flight not returning to ~0 once quiescent  <- the leak itself
#   - abandoned_total not tracking the abandonments issued    <- accounting lies
#   - late-window p50 regressing badly vs the early window    <- silent decay
#
# A pass here is meaningful precisely because the OLD binary fails every one.
#
#   EP=http://host:8089 METRICS=http://host:9105 scripts/release-validation/cancellation-soak.sh
set -uo pipefail
EP="${EP:?set EP to the query endpoint}"
METRICS="${METRICS:-}"
INDEX="${INDEX:-logs-bench}"
ROUNDS="${ROUNDS:-12}"           # honest/abandon cycles
ABANDON_PER_ROUND="${ABANDON_PER_ROUND:-10}"
HONEST_PER_ROUND="${HONEST_PER_ROUND:-5}"
ABANDON_AFTER="${ABANDON_AFTER:-1}"   # seconds before the client hangs up
# Honest queries answer in 20-300ms on a healthy server, so 15s is enormously
# generous -- and it means a BROKEN server fails the soak in minutes rather than
# burning the full 70s query timeout on every probe. The first run of this script
# against the old binary took >40 minutes for that reason.
HONEST_TIMEOUT="${HONEST_TIMEOUT:-15}"
OUT="${OUT:?set OUT to a new durable result directory}"
mkdir "$OUT" || exit 2
say() { echo "==> [soak] $*"; }

# A shape SLOW enough that the client timeout lands mid-flight. This matters more
# than it looks: the first version used a filtered browse that happened to take
# 60s on the 1TB fleet and 9ms on the 200G one, so on the latter the soak
# abandoned NOTHING and tested nothing. count(DISTINCT raw) over the whole corpus
# is slow by construction (measured: 29.9s at 394M rows), and PREFLIGHT below
# refuses to run if it is not slow enough here.
# Overridable: the right abandonment shape depends on the LAYOUT, not just the
# dataset. `count(DISTINCT raw)` is heavy on an unconverged table and 704ms on a
# converged one, where the group-count fast path answers it -- at which point
# the preflight below correctly refuses to run a soak that would test nothing
# (measured 2026-08-25 at converged depth 17, 2B rows).
SLOW_Q="${SLOW_Q:-SELECT count(DISTINCT raw) AS n FROM \\\"$INDEX\\\"}"
# A shape that is fast and pool-routed: this is what degrades when the pool dies.
FAST_Q="SELECT timestamp, raw FROM \\\"$INDEX\\\" WHERE region = 'us-east-2' LIMIT 100"

metric() {
  [ -z "$METRICS" ] && { echo ""; return; }
  curl -s -m 10 "$METRICS/metrics" 2>/dev/null | awk -v n="$1" '$1==n {print $2; exit}'
}

honest_ms() {
  local s e code
  s=$(date +%s%N)
  code=$(curl -s -o /dev/null -w '%{http_code}' -m "$HONEST_TIMEOUT" -X POST "$EP/api/v1/sql" \
          -H 'Content-Type: application/json' -d "{\"query\":\"$FAST_Q\"}" 2>/dev/null)
  e=$(( ($(date +%s%N) - s) / 1000000 ))
  echo "$code $e"
}

say "endpoint $EP  rounds=$ROUNDS  abandon/round=$ABANDON_PER_ROUND"

# PREFLIGHT. A soak whose "slow" query is fast abandons nothing and then reports
# on a run that never happened. Prove the shape is abandonable BEFORE trusting a
# single result from it.
say "preflight: timing the abandonment shape"
_t0=$(date +%s%N)
curl -s -o /dev/null -m 120 -X POST "$EP/api/v1/sql" -H 'Content-Type: application/json' \
  -d "{\"query\":\"$SLOW_Q\"}" 2>/dev/null
_slow_ms=$(( ($(date +%s%N) - _t0) / 1000000 ))
say "  abandonment shape takes ${_slow_ms}ms; client hangs up after $((ABANDON_AFTER * 1000))ms"
if [ "$_slow_ms" -lt $((ABANDON_AFTER * 3000)) ]; then
  echo "FAIL: the abandonment shape completes in ${_slow_ms}ms, too fast to be"
  echo "      abandoned after ${ABANDON_AFTER}s. This soak would test NOTHING."
  echo "      Pick a slower SLOW_Q for this dataset, or raise ABANDON_AFTER."
  exit 1
fi
before_abandoned=$(metric siglake_query_exec_pool_abandoned_total); before_abandoned=${before_abandoned:-0}
issued=0
: > "$OUT/honest.txt"

for r in $(seq 1 "$ROUNDS"); do
  for _ in $(seq 1 "$ABANDON_PER_ROUND"); do
    curl -s -o /dev/null -m "$ABANDON_AFTER" -X POST "$EP/api/v1/sql" \
      -H 'Content-Type: application/json' -d "{\"query\":\"$SLOW_Q\"}" 2>/dev/null
    issued=$((issued + 1))
  done
  for _ in $(seq 1 "$HONEST_PER_ROUND"); do
    read -r code ms <<<"$(honest_ms)"
    echo "$r $code $ms" >> "$OUT/honest.txt"
  done
  inflight=$(metric siglake_query_exec_pool_in_flight)
  say "round $r/$ROUNDS: issued=$issued abandoned  in_flight=${inflight:-?}"
done

say "quiescing for 20s so genuinely-running queries can finish"
sleep 20

fail=0
inconclusive=0
alive=$(curl -s -o /dev/null -w '%{http_code}' -m 20 -X POST "$EP/api/v1/sql" \
  -H 'Content-Type: application/json' -d '{"query":"SELECT 1 AS x"}' 2>/dev/null)
if [ "$alive" != "200" ]; then
  echo "FAIL: server is not answering after the soak (HTTP $alive) — it likely died"
  fail=1
else
  say "server alive after $issued abandonments"
fi

if [ -n "$METRICS" ]; then
  inflight=$(metric siglake_query_exec_pool_in_flight)
  after_abandoned=$(metric siglake_query_exec_pool_abandoned_total)
  delta=$(( ${after_abandoned:-0} - ${before_abandoned:-0} ))
  say "in_flight=${inflight:-?}  abandoned_total delta=$delta (issued $issued)"
  # in_flight is the leak, directly. Quiescent, it must be ~0.
  if [ -n "$inflight" ] && [ "${inflight%.*}" -gt 2 ]; then
    echo "FAIL: exec_pool_in_flight=${inflight} while quiescent — slots are leaking."
    echo "      This is the 2026-08-17 defect: 53 timeouts left the gauge at 58."
    fail=1
  fi
  # A CHECK THAT COULD NOT RUN MUST NOT REPORT FAILURE. If the metrics endpoint
  # was unreachable, `delta` is 0 because nothing was ever read -- which is
  # indistinguishable from "cancellation is broken" unless we say so. Measured
  # 2026-08-25: a soak that PASSED functionally (120 abandonments, server alive,
  # 60/60 honest queries, p50 drift 0.96x) reported FAILED, because port 9105 is
  # not exposed outside the node and the counter had in fact moved by exactly
  # 120. That is the same defect class this whole harness exists to catch.
  if [ -z "$METRICS" ] || [ -z "${inflight:-}" ]; then
    echo "INCONCLUSIVE: metrics at '${METRICS:-unset}' were unreachable, so the"
    echo "      leak and accounting assertions could NOT be evaluated. The"
    echo "      functional result above still stands; the metric result does not."
    echo "      Read them on the node (localhost:9105) and re-check by hand."
    inconclusive=1
  elif [ "$delta" -lt 1 ]; then
    echo "FAIL: $issued queries were abandoned but abandoned_total moved by $delta."
    echo "      Either cancellation is not reaching the guard, or the metric is dead."
    fail=1
  fi
fi

# Silent decay: compare the first third of honest queries against the last third.
python3 - "$OUT/honest.txt" <<'PY' || fail=1
import sys, statistics
rows = [l.split() for l in open(sys.argv[1]) if l.strip()]
ok = [(int(r[0]), int(r[2])) for r in rows if r[1] == "200"]
errs = len(rows) - len(ok)
if not ok:
    print("FAIL: no honest query succeeded during the soak"); sys.exit(1)
n = len(ok)
early = [ms for _, ms in ok[: max(1, n // 3)]]
late = [ms for _, ms in ok[-max(1, n // 3):]]
e, l = statistics.median(early), statistics.median(late)
print(f"  honest queries: {len(ok)} ok, {errs} failed")
print(f"  p50 early={e:.0f}ms  late={l:.0f}ms  drift={l / e if e else 0:.2f}x")
if errs:
    print(f"FAIL: {errs} honest queries failed during the soak"); sys.exit(1)
# 3x is loose on purpose: this catches a pool dying, not ordinary jitter. The
# real failure took latency from 20ms to a 60s timeout -- 3000x.
if e and l / e > 3.0:
    print(f"FAIL: honest-query p50 degraded {l/e:.1f}x across the soak"); sys.exit(1)
PY

if [ "$fail" != 0 ]; then
  say "FAILED"
elif [ "${inconclusive:-0}" != 0 ]; then
  # Distinct from PASS on purpose: the functional half passed, the metric half
  # was never evaluated, and reporting that as PASS would claim more than was
  # measured.
  say "INCONCLUSIVE (functional checks passed; metric checks could not run)"
else
  say "PASS"
fi
exit "$fail"
