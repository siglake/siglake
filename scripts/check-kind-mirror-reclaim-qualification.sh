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
  'compactor-metrics-end.prom' \
  'minio-alias-setup.stdout' \
  'minio-alias-setup.stderr' \
  'mirror-prefix-listing-failure.raw' \
  'mirror-prefix-listing-failure.txt'; do
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
contains "$body" 'MIRROR_RECLAIM_QUERY_VISIBILITY_SECONDS=120' ||
  fail "$ROUND does not allow for the shipped 60-second metadata staleness ceiling"
contains "$body" 'MIRROR_RECLAIM_QUERY_POLL_SECONDS=5' ||
  fail "$ROUND does not poll final query visibility every five seconds"
contains "$body" 'MIRROR_RECLAIM_LOAD_STARTED_EPOCH=$(date +%s)' ||
  fail "$ROUND does not record the actual load start"
contains "$body" 'MIRROR_RECLAIM_LOAD_FINISHED_EPOCH=$(date +%s)' ||
  fail "$ROUND does not record the actual load finish"
contains "$body" 'finish_mirror_reclaim_evidence "$next_event" "$rounds"' ||
  fail "$ROUND does not reconcile every sent row after the load"
contains "$body" 'mc alias set local http://minio:9000 minioadmin minioadmin --quiet' ||
  fail "$ROUND does not set up the MinIO alias separately"
contains "$body" 'mc ls --recursive --json local/siglake-warehouse/warehouse/wal-mirror/' ||
  fail "$ROUND does not request a JSON-only mirror-prefix listing"
if contains "$body" "sh -c 'mc alias set"; then
  fail "$ROUND still combines MinIO alias setup with the JSON listing"
fi
contains "$body" 'cp "$objects_file" "$MIRROR_RECLAIM_LISTING_FAILURE_RAW"' ||
  fail "$ROUND does not retain the raw mirror-prefix listing on failure"
contains "$body" '$(<"$objects_error")' ||
  fail "$ROUND does not emit the offending mirror-prefix line"

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

# Exercise the production line parser without a cluster. The first fixture is
# the hypothesized old mc shape: a harmless alias acknowledgement followed by
# valid JSON records. Every other non-record line, malformed record and mc
# error must fail with the exact offending line instead of becoming a zero.
sed -n '/^parse_mirror_reclaim_listing()/,/^capture_mirror_reclaim_sample()/p' \
  "$ROUND" | sed '$d' >"$sandbox/scripts/parse-listing.bash"
source "$sandbox/scripts/parse-listing.bash"
cat >"$sandbox/listing-prologue.jsonl" <<'EOF'
Added `local` successfully.
{"status":"success","type":"file","size":5}
{"status":"success","type":"folder","size":0}
{"status":"success","type":"file","size":7}
EOF
totals=$(parse_mirror_reclaim_listing "$sandbox/listing-prologue.jsonl") ||
  fail "the parser rejected the recognized mc setup prologue"
[[ "$totals" == $'2\t12' ]] ||
  fail "the parser counted the prologue fixture as $totals, expected 2 objects and 12 bytes"

assert_listing_refused() {
  local fixture=$1 expected=$2
  if parse_mirror_reclaim_listing "$fixture" >"$sandbox/parser.out" 2>"$sandbox/parser.err"; then
    fail "the parser accepted $(basename "$fixture") as a successful listing"
  fi
  grep -Fq "$expected" "$sandbox/parser.err" ||
    fail "the refusal for $(basename "$fixture") did not include the offending line: $(cat "$sandbox/parser.err")"
  [[ ! -s "$sandbox/parser.out" ]] ||
    fail "the refused $(basename "$fixture") produced successful totals"
}

cat >"$sandbox/listing-unknown-prologue.jsonl" <<'EOF'
An unexpected MinIO client banner
{"status":"success","type":"file","size":5}
EOF
assert_listing_refused "$sandbox/listing-unknown-prologue.jsonl" \
  "line 1: 'An unexpected MinIO client banner'"

cat >"$sandbox/listing-malformed.jsonl" <<'EOF'
{"status":"success","type":"file","size":5}
{this is not JSON}
EOF
assert_listing_refused "$sandbox/listing-malformed.jsonl" \
  "line 2: '{this is not JSON}'"

cat >"$sandbox/listing-error.jsonl" <<'EOF'
{"status":"error","error":{"message":"Access Denied"}}
EOF
assert_listing_refused "$sandbox/listing-error.jsonl" \
  'line 1: '\''{"status":"error","error":{"message":"Access Denied"}}'\'''

# Exercise final reconciliation without a cluster. The fake clock lets each
# fixture drive the production polling and timeout branches without sleeping.
sed -n '/^mirror_reclaim_elapsed_seconds()/,/^log "bring up the base kind deployment"/p' \
  "$ROUND" | sed '$d' >"$sandbox/scripts/finish-evidence.bash"

run_reconciliation_fixture() {
  local case=$1
  local case_dir="$sandbox/reconciliation-$case"
  mkdir -p "$case_dir/tmp" "$case_dir/results"
  case "$case" in
    delayed_visibility)
      cat >"$case_dir/responses" <<'EOF'
ok|{"rows":[{"n":8}]}
ok|{"rows":[{"n":10}]}
EOF
      ;;
    persistent_mismatch)
      cat >"$case_dir/responses" <<'EOF'
ok|{"rows":[{"n":8}]}
ok|{"rows":[{"n":8}]}
ok|{"rows":[{"n":8}]}
EOF
      ;;
    query_failure)
      cat >"$case_dir/responses" <<'EOF'
ok|not-json
fail|query endpoint refused
fail|query endpoint still refused
EOF
      ;;
    committed_drain_timeout)
      : >"$case_dir/responses"
      ;;
    *) fail "unknown reconciliation fixture $case" ;;
  esac
  if env FIXTURE_CASE="$case" FIXTURE_DIR="$case_dir" bash -c '
    set -euo pipefail
    source "$1"
    ROOT=$FIXTURE_DIR
    TMP_DIR=$FIXTURE_DIR/tmp
    MIRROR_RECLAIM_ARM=on
    MIRROR_RECLAIM_RESULTS_DIR=$FIXTURE_DIR/results
    MIRROR_RECLAIM_LOAD_JSON=$FIXTURE_DIR/results/load-window.json
    MIRROR_RECLAIM_ROWS_JSON=$FIXTURE_DIR/results/row-reconciliation.json
    MIRROR_RECLAIM_LOAD_STARTED_EPOCH=100
    MIRROR_RECLAIM_LOAD_FINISHED_EPOCH=200
    MIRROR_RECLAIM_FIRST_SAMPLE_EPOCH=100
    MIRROR_RECLAIM_LAST_SAMPLE_EPOCH=200
    MIRROR_RECLAIM_BASE_ROWS_COMMITTED=0
    MIRROR_RECLAIM_SAMPLE_ROWS_COMMITTED=0
    LOAD_SECONDS=3600
    MIRROR_RECLAIM_COMMITTED_DRAIN_SECONDS=2
    MIRROR_RECLAIM_QUERY_VISIBILITY_SECONDS=2
    MIRROR_RECLAIM_QUERY_POLL_SECONDS=1
    FIXTURE_NOW=0

    mirror_reclaim_elapsed_seconds() { printf "%s\n" "$FIXTURE_NOW"; }
    mirror_reclaim_sleep() { FIXTURE_NOW=$((FIXTURE_NOW + $1)); }
    capture_mirror_reclaim_sample() {
      if [[ "$FIXTURE_CASE" == committed_drain_timeout ]]; then
        MIRROR_RECLAIM_SAMPLE_ROWS_COMMITTED=6
      else
        MIRROR_RECLAIM_SAMPLE_ROWS_COMMITTED=10
      fi
    }
    run_sql() {
      local calls_file=$FIXTURE_DIR/calls index line status payload
      touch "$calls_file"
      index=$(($(wc -l <"$calls_file") + 1))
      printf "%s\n" "$index" >>"$calls_file"
      line=$(sed -n "${index}p" "$FIXTURE_DIR/responses")
      status=${line%%|*}
      payload=${line#*|}
      if [[ "$status" == ok ]]; then
        printf "%s\n" "$payload"
      else
        printf "%s\n" "$payload" >&2
        return 1
      fi
    }
    finish_mirror_reclaim_evidence 10 4
  ' _ "$sandbox/scripts/finish-evidence.bash" \
      >"$case_dir/stdout" 2>"$case_dir/stderr"; then
    [[ "$case" == delayed_visibility ]] ||
      fail "$case unexpectedly passed final reconciliation"
  else
    [[ "$case" != delayed_visibility ]] ||
      fail "delayed visibility did not converge: $(cat "$case_dir/stderr")"
  fi
  [[ -s "$case_dir/results/load-window.json" ]] ||
    fail "$case did not retain load-window.json"
  [[ -s "$case_dir/results/row-reconciliation.json" ]] ||
    fail "$case did not retain row-reconciliation.json"
}

for fixture in delayed_visibility persistent_mismatch query_failure committed_drain_timeout; do
  run_reconciliation_fixture "$fixture"
done

python3 - "$sandbox" <<'PY' || fail "final reconciliation fixtures differ from the contract"
import json
import os
import sys

root = sys.argv[1]


def documents(case):
    directory = os.path.join(root, f"reconciliation-{case}", "results")
    load = json.load(open(os.path.join(directory, "load-window.json"), encoding="utf-8"))
    rows = json.load(open(os.path.join(directory, "row-reconciliation.json"), encoding="utf-8"))
    assert load["sent_rows"] == 10, (case, load)
    assert load["workload_rounds"] == 4, (case, load)
    return rows


delayed = documents("delayed_visibility")
assert delayed["verified"] is True, delayed
assert delayed["query_rows_after_committed_drain"] == 10, delayed
assert [attempt["observed_rows"] for attempt in delayed["query_visibility"]["attempts"]] == [8, 10], delayed
assert [attempt["elapsed_seconds"] for attempt in delayed["query_visibility"]["attempts"]] == [0, 1], delayed

mismatch = documents("persistent_mismatch")
assert mismatch["verified"] is False, mismatch
assert mismatch["failure_reason"] == "query_visibility_timeout", mismatch
assert mismatch["query_rows_after_committed_drain"] == 8, mismatch
assert len(mismatch["query_visibility"]["attempts"]) == 3, mismatch

failed = documents("query_failure")
assert failed["verified"] is False, failed
assert failed["failure_reason"] == "query_unavailable", failed
assert failed["query_rows_after_committed_drain"] is None, failed
errors = [attempt["error"] for attempt in failed["query_visibility"]["attempts"]]
assert [error["stage"] for error in errors] == ["parse", "query", "query"], failed
assert "Expecting value" in errors[0]["message"], failed
assert errors[1]["message"] == "query endpoint refused", failed

drain = documents("committed_drain_timeout")
assert drain["verified"] is False, drain
assert drain["failure_reason"] == "committed_drain_timeout", drain
assert drain["committed_rows"] == 6, drain
assert drain["query_rows_after_committed_drain"] is None, drain
assert drain["query_visibility"]["outcome"] == "not_run_committed_drain_timeout", drain
assert drain["query_visibility"]["attempts"] == [], drain
PY

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
