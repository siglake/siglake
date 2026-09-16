#!/usr/bin/env bash
# Offline fixtures for the kind Postgres-outage evidence grader. No cluster.

set -euo pipefail

cd "$(dirname "$0")/.."

GRADER=scripts/grade-kind-postgres-outage.py
FIXTURE=scripts/testdata/kind-postgres-outage-verified.json
ROUND=scripts/kind-round.sh
PROBE=scripts/kind-postgres-outage-probe.sh
JOBS_SOURCE=crates/siglake-query-server/src/jobs.rs
SQL_SOURCE=crates/siglake-query-server/src/sql.rs

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

for file in "$GRADER" "$FIXTURE" "$ROUND" "$PROBE"; do
  [[ -f "$file" ]] || fail "$file does not exist"
done

# The live path must remain opt-in, and it must measure the job store the chart
# ships: persistent since 2026-09-11, so the round installs it unconditionally
# and the probe is the only thing the knob switches on.
round_body=$(grep -vE '^[[:space:]]*(#|$)' "$ROUND")
probe_body=$(grep -vE '^[[:space:]]*(#|$)' "$PROBE")
contains "$round_body" 'POSTGRES_OUTAGE_PROBE="${POSTGRES_OUTAGE_PROBE:-0}"' ||
  fail "$ROUND does not default POSTGRES_OUTAGE_PROBE off"
contains "$round_body" 'PERSISTENT_JOB_STORE=true' ||
  fail "$ROUND does not install the persistent job store the probe pauses"
contains "$round_body" '--set query.jobs.persistent="$PERSISTENT_JOB_STORE"' ||
  fail "$ROUND does not set query.jobs.persistent from that boolean"
contains "$round_body" 'scripts/kind-postgres-outage-probe.sh' ||
  fail "$ROUND never invokes the outage probe"

# The probe's strict grader may return nonzero after fault restoration. The
# round must retain that status without letting `set -e` skip the remaining
# panel and ScaledObject observations, then apply it at the final verdict.
python3 - "$ROUND" <<'PY' || fail "$ROUND does not defer the outage-probe verdict until after evidence collection"
import sys

lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
guard = [i for i, line in enumerate(lines)
         if line == 'if [[ "$POSTGRES_OUTAGE_PROBE" == 1 ]]; then']
if len(guard) != 1:
    raise SystemExit(f"expected one outage-probe opt-in block, found {guard}")
opens = guard[0]
closes = next(i for i, line in enumerate(lines[opens + 1:], opens + 1)
              if line == "fi")
body = "\n".join(lines[opens:closes + 1])
if 'POSTGRES_OUTAGE_FAILURE=$?' not in body:
    raise SystemExit("the opt-in block does not retain the probe exit status")
if 'POSTGRES_OUTAGE_PROBE status=failed exit_status=%s' not in body:
    raise SystemExit("the opt-in block does not report its deferred failure")
panel = next(i for i, line in enumerate(lines)
             if line == 'log "dashboard panel evidence"')
scaledobject = next(i for i, line in enumerate(lines)
                    if line == 'log "ScaledObject evidence"')
verdict = next(i for i, line in enumerate(lines)
               if line.startswith('[[ "$POSTGRES_OUTAGE_FAILURE" -eq 0 ]]'))
if not (closes < panel < scaledobject < verdict):
    raise SystemExit(
        f"expected probe block, panel evidence, ScaledObject evidence and verdict in order; "
        f"got {closes + 1}, {panel + 1}, {scaledobject + 1}, {verdict + 1}"
    )
PY

# PID 1 is only the postmaster in the postgres image. Established sessions
# run in child processes, so both the normal and trap paths must use the same
# exact-name scan that stops and continues those backends as well.
contains "$probe_body" 'pause_postgres_processes()' ||
  fail "$PROBE has no bounded Postgres pause helper"
contains "$probe_body" 'continue_postgres_processes()' ||
  fail "$PROBE has no bounded Postgres continuation helper"
contains "$probe_body" '[ "$(cat /proc/1/comm)" = postgres ]' ||
  fail "$PROBE does not positively identify the Postgres container"
contains "$probe_body" 'for comm_path in /proc/[0-9]*/comm; do' ||
  fail "$PROBE does not enumerate established Postgres backends"
contains "$probe_body" '[ "$(cat "$comm_path" 2>/dev/null || true)" = postgres ]' ||
  fail "$PROBE does not limit backend signals to exact postgres process names"
contains "$probe_body" 'kill -STOP "$pid"' ||
  fail "$PROBE does not stop established Postgres backends"
contains "$probe_body" 'kill -CONT "$pid" 2>/dev/null || true' ||
  fail "$PROBE does not continue every surviving Postgres backend"
contains "$probe_body" 'continue_postgres_processes >/dev/null 2>&1 || true' ||
  fail "$PROBE trap cleanup does not restore the backend process set"
[[ $(grep -c '^continue_postgres_processes >/dev/null$' "$PROBE") -eq 1 ]] ||
  fail "$PROBE normal path does not restore through the shared continuation helper"
if contains "$probe_body" 'kill -STOP -1'; then
  fail "$PROBE must not treat kill -STOP -1 as a targeted process-group signal"
fi

# These values are retained as the effective fixed settings in every trace.
# Pin their source literals so a code change cannot leave the evidence claiming
# settings that the measured binary no longer uses.
contains "$(<"$JOBS_SOURCE")" 'const MAX_FINISHED_UNPERSISTED: usize = 1024;' ||
  fail "$JOBS_SOURCE no longer has the recorded 1024-id cap"
contains "$(<"$JOBS_SOURCE")" 'const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);' ||
  fail "$JOBS_SOURCE no longer has the recorded 5s reconcile interval"
contains "$(<"$JOBS_SOURCE")" 'const RECONCILE_WRITE_TIMEOUT: Duration = Duration::from_secs(10);' ||
  fail "$JOBS_SOURCE no longer has the recorded 10s reconcile write timeout"
contains "$(<"$SQL_SOURCE")" 'const TERMINAL_PERSIST_ATTEMPTS: usize = 3;' ||
  fail "$SQL_SOURCE no longer has the recorded three terminal-write attempts"
contains "$(<"$SQL_SOURCE")" 'const TERMINAL_PERSIST_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);' ||
  fail "$SQL_SOURCE no longer has the recorded 30s terminal-write budget"
for setting in \
  '"reconcile_interval_seconds": 5' \
  '"reconcile_write_timeout_seconds": 10' \
  '"max_unreconciled_per_pod": 1024' \
  '"terminal_write_attempts": 3' \
  '"terminal_write_deadline_seconds": 30'; do
  contains "$probe_body" "$setting" ||
    fail "$PROBE does not retain the effective setting $setting"
done

fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-postgres-outage-evidence.XXXXXX")
trap 'rm -rf -- "$fixture_dir"' EXIT

# Execute the round's exact opt-in block and final outage verdict around
# stand-in observations. This stays offline: ROOT points at a failing child in
# the fixture directory, and the only output between the two extracted pieces
# is the evidence that a premature `set -e` exit used to skip.
arm_root="$fixture_dir/arm-root"
mkdir -p "$arm_root/scripts"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'printf "PROBE_CHILD called=yes exit_status=%s\\n" "$PROBE_CHILD_RC"' \
  'exit "$PROBE_CHILD_RC"' \
  >"$arm_root/scripts/kind-postgres-outage-probe.sh"
chmod +x "$arm_root/scripts/kind-postgres-outage-probe.sh"

python3 - "$ROUND" "$fixture_dir/round-arm.bash" <<'PY'
import pathlib
import sys

source, output = map(pathlib.Path, sys.argv[1:])
lines = source.read_text(encoding="utf-8").splitlines()
opens = next(i for i, line in enumerate(lines)
             if line == 'if [[ "$POSTGRES_OUTAGE_PROBE" == 1 ]]; then')
closes = next(i for i, line in enumerate(lines[opens + 1:], opens + 1)
              if line == "fi")
verdict = next(line for line in lines
               if line.startswith('[[ "$POSTGRES_OUTAGE_FAILURE" -eq 0 ]]'))
script = [
    "#!/usr/bin/env bash",
    "set -euo pipefail",
    'ROOT=$1',
    'POSTGRES_OUTAGE_PROBE=$2',
    'KUBE_CONTEXT=fixture',
    'NAMESPACE=fixture',
    'PROM_URL=http://fixture.invalid',
    'RESULTS_DIR=$ROOT/results',
    'POSTGRES_OUTAGE_FAILURE=0',
    'log() { printf "==> %s\\n" "$*" >&2; }',
    'die() { printf "ERROR: %s\\n" "$*" >&2; exit 1; }',
    *lines[opens:closes + 1],
    "printf 'PANEL_TABLE_BEGIN\\nPANEL_TABLE_END\\n'",
    "printf 'SCALEDOBJECT_WIDE_BEGIN\\nSCALEDOBJECT_WIDE_END\\n'",
    verdict,
    "printf 'ROUND_PASSED\\n'",
]
output.write_text("\n".join(script) + "\n", encoding="utf-8")
PY
chmod +x "$fixture_dir/round-arm.bash"

fixtures=0
arm_rc=0
PROBE_CHILD_RC=17 "$fixture_dir/round-arm.bash" "$arm_root" 1 \
  >"$fixture_dir/arm-failed.out" 2>&1 || arm_rc=$?
[[ "$arm_rc" -ne 0 ]] || fail "failing outage child left the round green"
failed_arm="$(<"$fixture_dir/arm-failed.out")"
contains "$failed_arm" 'POSTGRES_OUTAGE_PROBE status=failed exit_status=17' ||
  fail "failing outage child did not report its deferred status: $failed_arm"
contains "$failed_arm" 'PANEL_TABLE_BEGIN' ||
  fail "failing outage child skipped panel evidence: $failed_arm"
contains "$failed_arm" 'SCALEDOBJECT_WIDE_BEGIN' ||
  fail "failing outage child skipped ScaledObject evidence: $failed_arm"
contains "$failed_arm" 'ERROR: the requested Postgres outage probe failed with exit status 17' ||
  fail "failing outage child did not make the final verdict nonzero: $failed_arm"
fixtures=$((fixtures + 1))

PROBE_CHILD_RC=17 "$fixture_dir/round-arm.bash" "$arm_root" 0 \
  >"$fixture_dir/arm-default-off.out" 2>&1 ||
  fail "default-off outage path failed: $(<"$fixture_dir/arm-default-off.out")"
default_off_arm="$(<"$fixture_dir/arm-default-off.out")"
if contains "$default_off_arm" 'PROBE_CHILD called=yes'; then
  fail "default-off outage path invoked the child"
fi
contains "$default_off_arm" 'PANEL_TABLE_BEGIN' ||
  fail "default-off outage path skipped panel evidence: $default_off_arm"
contains "$default_off_arm" 'ROUND_PASSED' ||
  fail "default-off outage path did not preserve the successful verdict: $default_off_arm"
fixtures=$((fixtures + 1))

verified="$fixture_dir/verified.json"
python3 "$GRADER" "$FIXTURE" --output "$verified" 2>"$fixture_dir/verified.log" ||
  fail "verified fixture did not pass: $(cat "$fixture_dir/verified.log")"
python3 - "$verified" <<'PY' || fail "verified fixture summary is wrong"
import json, sys
document = json.load(open(sys.argv[1], encoding="utf-8"))
evidence = document["evidence"]
assert evidence["grade"] == "verified"
assert evidence["summary"]["peak_backlog_total"] == 3
assert evidence["summary"]["time_to_drain_seconds"] == 14
assert evidence["summary"]["completion_delta"] == 3
PY

fixtures=$((fixtures + 1))

# Mutate a copy of the passing trace. Each case must become unverified for the
# intended reason, so a grader that silently treats a missing observation as
# zero cannot pass these fixtures.
expect_unverified() {
  local mutation=$1 want=$2
  local input="$fixture_dir/${mutation}.input.json"
  local output="$fixture_dir/${mutation}.output.json"
  local log="$fixture_dir/${mutation}.log" rc=0
  python3 - "$FIXTURE" "$input" "$mutation" <<'PY'
import json, sys
source, destination, mutation = sys.argv[1:]
document = json.load(open(source, encoding="utf-8"))
if mutation == "missing-series":
    document["samples"][1]["backlog"] = []
elif mutation == "no-rise":
    for sample in document["samples"]:
        for row in sample["backlog"]:
            row["value"] = 0
elif mutation == "no-drain":
    for sample in document["samples"]:
        if sample["phase"] == "recovery":
            sample["backlog"][0]["value"] = 1
elif mutation == "missing-revision":
    del document["revisions"]["repository_commit"]
else:
    raise SystemExit(f"unknown mutation {mutation}")
json.dump(document, open(destination, "w", encoding="utf-8"))
PY
  python3 "$GRADER" "$input" --output "$output" 2>"$log" || rc=$?
  [[ "$rc" -eq 1 ]] || fail "$mutation exited $rc, expected the unverified exit 1"
  grade=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["evidence"]["grade"])' "$output")
  [[ "$grade" == unverified ]] || fail "$mutation was not graded unverified"
  problem=$(python3 -c 'import json,sys; print("\n".join(json.load(open(sys.argv[1]))["evidence"]["problems"]))' "$output")
  contains "$problem" "$want" || fail "$mutation was caught for the wrong reason: $problem"
  fixtures=$((fixtures + 1))
}

expect_unverified missing-series 'no usable backlog observation'
expect_unverified no-rise 'no observed unreconciled backlog'
expect_unverified no-drain 'did not drain after restoration'
expect_unverified missing-revision 'missing pinned repository revision'

echo "ok ($fixtures offline Postgres-outage fixtures; live probe is opt-in)"
