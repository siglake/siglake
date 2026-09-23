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
contains "$body" '"$(source_commit)" "$(source_commit_origin)" "$@"' ||
  fail "$ROUND does not resolve the retained launch revision through the source resolver"

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

# Exercise the actual launch writer through both revision paths. The stand-in
# SHA deliberately differs from the injection, and the injected arm must not
# ask git for a second answer.
mkdir -p "$sandbox/bin" "$sandbox/results"
sed -n '/^SIGLAKE_SOURCE_COMMIT=/,/^# --- #1838/p' "$ROUND" | sed '$d' \
  >"$sandbox/scripts/source-commit.bash"
sed -n '/^write_mirror_reclaim_launch()/,/^capture_mirror_reclaim_effective_config()/p' \
  "$ROUND" | sed '$d' >"$sandbox/scripts/write-launch.bash"
cat >"$sandbox/bin/git" <<'STANDIN'
#!/usr/bin/env bash
set -euo pipefail
printf 'called\n' >>"$STANDIN_GIT_CALLS"
[[ "$*" == *'rev-parse HEAD' ]] || exit 64
printf '%s\n' "$STANDIN_GIT_COMMIT"
STANDIN
chmod +x "$sandbox/bin/git"

write_launch_fixture() {
  local output=$1
  shift
  env PATH="$sandbox/bin:$PATH" STANDIN_GIT_CALLS="$sandbox/git-calls" \
    STANDIN_GIT_COMMIT=1111111111111111111111111111111111111111 "$@" \
    bash -c '
      set -euo pipefail
      ROOT=$1
      MIRROR_RECLAIM_ARM=off
      MIRROR_RECLAIM_LAUNCH_JSON=$2
      CATALOG_CLAIM_ENABLED=false
      WAL_MIRROR_ENABLED=true
      WAL_MIRROR_ACTIVE_INTERVAL_SECS=0
      COMMITTED_RETENTION_SECS=901
      MIRROR_LEDGER_RECLAIM=false
      LOAD_SECONDS=3600
      MIRROR_RECLAIM_RESULTS_DIR=$ROOT/results/mirror-reclaim-off
      source "$ROOT/scripts/source-commit.bash"
      source "$ROOT/scripts/write-launch.bash"
      write_mirror_reclaim_launch --set fixture=true
    ' _ "$sandbox" "$output"
}

write_launch_fixture "$sandbox/results/fallback.json"
write_launch_fixture "$sandbox/results/injected.json" \
  SIGLAKE_SOURCE_COMMIT=2222222222222222222222222222222222222222
python3 - "$sandbox/results/fallback.json" "$sandbox/results/injected.json" <<'PY' ||
import json
import sys

fallback, injected = (json.load(open(path, encoding="utf-8")) for path in sys.argv[1:])
assert fallback["source_commit"] == "1" * 40, fallback
assert fallback["source_commit_source"] == "git_rev_parse_head", fallback
assert injected["source_commit"] == "2" * 40, injected
assert injected["source_commit_source"] == "siglake_source_commit_env", injected
assert fallback["helm_command"][-2:] == ["--set", "fixture=true"], fallback
assert injected["helm_command"][-2:] == ["--set", "fixture=true"], injected
PY
  fail "$ROUND launch writer did not retain both revision origins"
[[ $(wc -l <"$sandbox/git-calls") -eq 1 ]] ||
  fail "the injected launch revision still called git"

# Exercise the actual effective-config capture against fixture cluster state.
# Since #5880 the chart renders SIGLAKE_WAL_MIRROR_PREFIX on the compactor in
# both drain modes, and this arm qualifies the drain whose prefix was wrong, so
# a compactor prefix that is missing or differs from the ingester's has to fail
# the arm instead of being recorded.
sed -n '/^capture_mirror_reclaim_effective_config()/,/^start_mirror_reclaim_observer()/p' \
  "$ROUND" | sed '$d' >"$sandbox/scripts/capture-config.bash"
cat >"$sandbox/bin/helm" <<'STANDIN'
#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == *'get values siglake'* ]] || exit 64
cat "$STANDIN_FIXTURES/helm-values.json"
STANDIN
cat >"$sandbox/bin/kubectl" <<'STANDIN'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *siglake-ingester*) cat "$STANDIN_FIXTURES/ingester.json" ;;
  *siglake-compactor*) cat "$STANDIN_FIXTURES/compactor.json" ;;
  *) exit 64 ;;
esac
STANDIN
chmod +x "$sandbox/bin/helm" "$sandbox/bin/kubectl"

write_capture_fixtures() {
  python3 - "$sandbox/fixtures" <<'PY'
import json
import os
import sys

root = sys.argv[1]
values = {
    "compactor": {
        "catalogClaim": {"enabled": False},
        "committedRetentionSecs": 901,
        "mirrorLedgerReclaim": True,
    },
    "wal": {
        "mirror": {"enabled": True, "activeIntervalSecs": 0, "prefix": "wal-mirror"},
    },
}


def deployment(uid, name, env):
    container = {
        "name": name,
        "args": ["--warehouse", "s3://bucket/warehouse"],
        "env": [{"name": key, "value": value} for key, value in env.items()],
    }
    return {
        "metadata": {"uid": uid},
        "spec": {"template": {"spec": {"containers": [container]}}},
    }


for case, compactor_prefix in (
    ("match", "wal-mirror"),
    ("mismatch", "wal-mirror-stale"),
    ("absent", None),
):
    compactor_env = {
        "SIGLAKE_COMMITTED_RETENTION_SECS": "901",
        "SIGLAKE_MIRROR_LEDGER_RECLAIM": "1",
    }
    if compactor_prefix is not None:
        compactor_env["SIGLAKE_WAL_MIRROR_PREFIX"] = compactor_prefix
    directory = os.path.join(root, case)
    os.makedirs(directory, exist_ok=True)
    written = {
        "helm-values.json": values,
        "ingester.json": deployment(
            "uid-ingester",
            "ingester",
            {
                "SIGLAKE_WAL_MIRROR_PREFIX": "wal-mirror",
                "SIGLAKE_REMOTE_WAL_DRAIN": "0",
            },
        ),
        "compactor.json": deployment("uid-compactor", "compactor", compactor_env),
    }
    for name, document in written.items():
        with open(os.path.join(directory, name), "w", encoding="utf-8") as out:
            json.dump(document, out, indent=2)
            out.write("\n")
PY
}

run_capture() {
  local case=$1
  local output=$2
  env PATH="$sandbox/bin:$PATH" STANDIN_FIXTURES="$sandbox/fixtures/$case" bash -c '
    set -euo pipefail
    TMP_DIR=$1
    MIRROR_RECLAIM_ARM=on
    MIRROR_RECLAIM_CONFIG_JSON=$2
    KUBE_CONTEXT=kind-fixture
    NAMESPACE=siglake
    source "$3"
    capture_mirror_reclaim_effective_config
  ' _ "$sandbox/tmp" "$output" "$sandbox/scripts/capture-config.bash"
}

write_capture_fixtures
run_capture match "$sandbox/results/effective-match.json" 2>"$sandbox/match.err" ||
  fail "the capture rejected a compactor prefix equal to the ingester's: $(cat "$sandbox/match.err")"
python3 - "$sandbox/results/effective-match.json" <<'PY' ||
import json
import sys

document = json.load(open(sys.argv[1], encoding="utf-8"))
deployed = document["deployed"]
assert deployed["ingester"]["wal_mirror_prefix"] == "wal-mirror", document
assert deployed["compactor"]["wal_mirror_prefix"] == "wal-mirror", document
assert deployed["compactor"]["mirror_ledger_reclaim"] == "1", document
PY
  fail "$ROUND does not retain the compactor's rendered mirror prefix"

for case in mismatch absent; do
  if run_capture "$case" "$sandbox/results/effective-$case.json" 2>"$sandbox/$case.err"; then
    fail "the capture accepted a $case compactor mirror prefix"
  fi
  grep -Fq 'WAL mirror prefixes differ' "$sandbox/$case.err" ||
    fail "the $case refusal did not name the mirror-prefix disagreement"
  grep -Fq 'compactor' "$sandbox/$case.err" ||
    fail "the $case refusal did not report the compactor's reading"
  [[ ! -e "$sandbox/results/effective-$case.json" ]] ||
    fail "the $case arm still retained an effective-config document"
done

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
