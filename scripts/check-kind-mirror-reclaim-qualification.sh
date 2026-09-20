#!/usr/bin/env bash
# Offline contract check for #4953's two-arm kind-round qualification.

set -euo pipefail

cd "$(dirname "$0")/.."

ROUND=scripts/kind-round.sh
TRAP_LINE='trap cleanup EXIT INT TERM'

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

[[ -f "$ROUND" ]] || fail "$ROUND does not exist"
bash -n "$ROUND"
body=$(grep -vE '^[[:space:]]*(#|$)' "$ROUND")

for input in \
  KIND_ROUND_MIRROR_RECLAIM_ARM \
  KIND_ROUND_CATALOG_CLAIM_ENABLED \
  KIND_ROUND_WAL_MIRROR_ENABLED \
  KIND_ROUND_WAL_MIRROR_ACTIVE_INTERVAL_SECS \
  KIND_ROUND_COMMITTED_RETENTION_SECS \
  KIND_ROUND_MIRROR_LEDGER_RECLAIM \
  KIND_ROUND_LOAD_SECONDS; do
  contains "$body" "$input" || fail "$ROUND does not read $input"
done

for contract in \
  'mirror-reclaim qualification requires KIND_ROUND_CATALOG_CLAIM_ENABLED=false' \
  'mirror-reclaim qualification requires KIND_ROUND_WAL_MIRROR_ENABLED=true' \
  'mirror-reclaim qualification requires KIND_ROUND_WAL_MIRROR_ACTIVE_INTERVAL_SECS=0' \
  'mirror-reclaim qualification requires KIND_ROUND_COMMITTED_RETENTION_SECS=901' \
  'mirror-reclaim qualification requires KIND_ROUND_LOAD_SECONDS=3600' \
  'mirror-reclaim arm %s requires KIND_ROUND_MIRROR_LEDGER_RECLAIM=%s'; do
  contains "$body" "$contract" || fail "$ROUND lost qualification guard: $contract"
done

for setting in \
  '--set wal.mirror.enabled="$WAL_MIRROR_ENABLED"' \
  '--set wal.mirror.activeIntervalSecs="$WAL_MIRROR_ACTIVE_INTERVAL_SECS"' \
  '--set compactor.catalogClaim.enabled="$CATALOG_CLAIM_ENABLED"' \
  '--set compactor.committedRetentionSecs="$COMMITTED_RETENTION_SECS"' \
  '--set compactor.mirrorLedgerReclaim="$MIRROR_LEDGER_RECLAIM"'; do
  contains "$body" "$setting" || fail "$ROUND launch lacks $setting"
done
contains "$body" 'write_mirror_reclaim_launch "${SIGLAKE_HELM_ARGS[@]}"' ||
  fail "$ROUND does not retain the launch argument array"
contains "$body" '"$ROOT/deploy/helm/siglake" "${SIGLAKE_HELM_ARGS[@]}"' ||
  fail "$ROUND does not execute the retained launch argument array"

for artifact in \
  'launch.json' \
  'effective-config.json' \
  'measurements.jsonl' \
  'load-window.json' \
  'row-reconciliation.json' \
  'compactor-metrics-start.prom' \
  'compactor-metrics-end.prom'; do
  count=$(grep -Fc "$artifact" "$ROUND" || true)
  ((count == 1)) || fail "$ROUND names $artifact $count times, expected once"
done
contains "$body" 'MIRROR_RECLAIM_RESULTS_DIR="$RESULTS_DIR/mirror-reclaim-$MIRROR_RECLAIM_ARM"' ||
  fail "$ROUND does not isolate the off/on result directories"

for evidence in \
  'compactor.catalogClaim.enabled' \
  'wal.mirror.enabled' \
  'wal.mirror.activeIntervalSecs' \
  'compactor.committedRetentionSecs' \
  'compactor.mirrorLedgerReclaim' \
  'SIGLAKE_REMOTE_WAL_DRAIN' \
  'siglake_compactor_retention_purged_total' \
  'siglake_compactor_mirror_unreclaimed_total' \
  'siglake_compactor_mirror_mark_errors_total' \
  'siglake_compactor_rows_committed_total' \
  'mirror_prefix' \
  'wal_segments' \
  'counter_process' \
  'query_rows_after_committed_drain'; do
  contains "$body" "$evidence" || fail "$ROUND does not retain $evidence"
done
contains "$body" 'MIRROR_RECLAIM_SAMPLE_SECONDS=60' ||
  fail "$ROUND does not sample the hour-long arm at one-minute intervals"
contains "$body" 'MIRROR_RECLAIM_LOAD_STARTED_EPOCH=$(date +%s)' ||
  fail "$ROUND does not record the actual load start"
contains "$body" 'MIRROR_RECLAIM_LOAD_FINISHED_EPOCH=$(date +%s)' ||
  fail "$ROUND does not record the actual load finish"
contains "$body" 'finish_mirror_reclaim_evidence "$next_event" "$rounds"' ||
  fail "$ROUND does not reconcile every sent row after the load"

# Drive the setup prelude only. This proves ordinary-round defaults stay at the
# old 330-second/catalog-claim shape and both qualification launch forms resolve
# to their exact one-hour filesystem-drain configurations before any tool check
# or cluster setup can run.
sandbox=$(mktemp -d "${TMPDIR:-/tmp}/siglake-kind-mirror-reclaim.XXXXXX")
trap 'rm -rf -- "$sandbox"' EXIT
mkdir -p "$sandbox/scripts" "$sandbox/tmp"
grep -qxF "$TRAP_LINE" "$ROUND" || fail "$ROUND lost the setup boundary"
sed -n "1,/^${TRAP_LINE}\$/p" "$ROUND" | sed '$d' >"$sandbox/scripts/prelude.bash"
cp scripts/kind-common.bash "$sandbox/scripts/kind-common.bash"

run_prelude() {
  local mode=$1
  shift
  env TMPDIR="$sandbox/tmp" "$@" bash -c '
    source "$1"
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$MIRROR_RECLAIM_ARM" "$LOAD_SECONDS" "$CATALOG_CLAIM_ENABLED" \
      "$WAL_MIRROR_ENABLED" "$WAL_MIRROR_ACTIVE_INTERVAL_SECS" \
      "$COMMITTED_RETENTION_SECS" "$MIRROR_LEDGER_RECLAIM" \
      "${MIRROR_RECLAIM_RESULTS_DIR-ordinary}"
    rm -rf -- "$TMP_DIR"
  ' _ "$sandbox/scripts/prelude.bash" 2>"$sandbox/$mode.err"
}

ordinary=$(run_prelude ordinary)
[[ "$ordinary" == $'\t330\ttrue\ttrue\t0\t86400\tfalse\tordinary' ]] ||
  fail "ordinary round defaults changed: $ordinary"

common=(
  KIND_ROUND_CATALOG_CLAIM_ENABLED=false
  KIND_ROUND_WAL_MIRROR_ENABLED=true
  KIND_ROUND_WAL_MIRROR_ACTIVE_INTERVAL_SECS=0
  KIND_ROUND_COMMITTED_RETENTION_SECS=901
  KIND_ROUND_LOAD_SECONDS=3600
  RESULTS_DIR=/evidence
)
off=$(run_prelude off "${common[@]}" \
  KIND_ROUND_MIRROR_RECLAIM_ARM=off KIND_ROUND_MIRROR_LEDGER_RECLAIM=false)
[[ "$off" == $'off\t3600\tfalse\ttrue\t0\t901\tfalse\t/evidence/mirror-reclaim-off' ]] ||
  fail "off-arm launch resolved incorrectly: $off"
on=$(run_prelude on "${common[@]}" \
  KIND_ROUND_MIRROR_RECLAIM_ARM=on KIND_ROUND_MIRROR_LEDGER_RECLAIM=true)
[[ "$on" == $'on\t3600\tfalse\ttrue\t0\t901\ttrue\t/evidence/mirror-reclaim-on' ]] ||
  fail "on-arm launch resolved incorrectly: $on"

if run_prelude short "${common[@]/KIND_ROUND_LOAD_SECONDS=3600/KIND_ROUND_LOAD_SECONDS=3599}" \
    KIND_ROUND_MIRROR_RECLAIM_ARM=on KIND_ROUND_MIRROR_LEDGER_RECLAIM=true >/dev/null; then
  fail "a 3599-second qualification arm was accepted"
fi
grep -Fq 'requires KIND_ROUND_LOAD_SECONDS=3600' "$sandbox/short.err" ||
  fail "the short-arm refusal did not name the one-hour contract"

echo "ok ($ROUND: ordinary defaults plus off/on #4953 launch, effective config, one-hour load, series, counters, and row artifacts)"
