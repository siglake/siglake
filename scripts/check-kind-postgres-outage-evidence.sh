#!/usr/bin/env bash
# Offline fixtures for the kind Postgres-outage evidence grader. No cluster.

set -euo pipefail

cd "$(dirname "$0")/.."

GRADER=scripts/grade-kind-postgres-outage.py
FIXTURE=scripts/testdata/kind-postgres-outage-verified.json
RUN76=scripts/testdata/kind-postgres-outage-run76.json
ROUND=scripts/kind-round.sh
PROBE=scripts/kind-postgres-outage-probe.sh
JOBS_SOURCE=crates/siglake-query-server/src/jobs.rs
SQL_SOURCE=crates/siglake-query-server/src/sql.rs

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

for file in "$GRADER" "$FIXTURE" "$RUN76" "$ROUND" "$PROBE"; do
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

# Run #76 graded `verified` on a trace with no evidence that the pause held and
# no way to date the counters it read, so the probe must now retain a process
# state per sample, a bounded write in each phase, the container's identity
# across the window, and the Prometheus scrape each value came from.
contains "$probe_body" '"schema_version": 2' ||
  fail "$PROBE does not retain the pause-evidence schema version"
contains "$probe_body" 'state_status=$(postgres_process_state "$TMP_DIR/postgres-state")' ||
  fail "$PROBE does not read the Postgres process state on every sample"
contains "$probe_body" 'scrape_expr="timestamp(siglake_query_jobs_unreconciled' ||
  fail "$PROBE does not retain the Prometheus scrape timestamp behind each value"
contains "$probe_body" '"sample_time": scrapes.get(pod, (None, None))[0]' ||
  fail "$PROBE does not attach the scrape timestamp to each retained value"
for phase in baseline outage recovery; do
  contains "$probe_body" "postgres_write_probe $phase" ||
    fail "$PROBE does not take a bounded write probe in the $phase phase"
done
contains "$probe_body" '"postgres_container": {' ||
  fail "$PROBE does not retain the Postgres container identity across the window"

# The two remote readers are the pieces a cluster would run, so run their exact
# text here: the state reader against a synthetic /proc, the write probe against
# a psql stand-in that returns, hangs, or fails.
extract_snippet() {
  python3 - "$PROBE" "$1" "$2" <<'PY'
import pathlib
import sys

source, marker, output = sys.argv[1:]
lines = pathlib.Path(source).read_text(encoding="utf-8").splitlines()
opens = lines.index(f"# {marker}-begin")
closes = lines.index(f"# {marker}-end")
body = "\n".join(lines[opens + 1:closes])
head, _, rest = body.partition("='")
if not rest.endswith("'"):
    raise SystemExit(f"{marker} is not a single-quoted shell variable")
pathlib.Path(output).write_text(rest[:-1], encoding="utf-8")
PY
}

fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/siglake-postgres-outage-evidence.XXXXXX")
trap 'rm -rf -- "$fixture_dir"' EXIT

extract_snippet state-snippet "$fixture_dir/state.sh"
extract_snippet write-probe-snippet "$fixture_dir/write-probe.sh"

# `pid:comm:state:starttime` per process. Field 3 of /proc/<pid>/stat is the
# state and field 22 the start time; the reader has to find both by position
# after the parenthesised comm.
make_proc_tree() {
  python3 - "$@" <<'PY'
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
for spec in sys.argv[2:]:
    pid, comm, state, starttime = spec.split(":")
    directory = root / pid
    directory.mkdir(parents=True)
    (directory / "comm").write_text(comm + "\n", encoding="utf-8")
    fields = [pid, f"({comm})", state] + ["0"] * 18 + [starttime] + ["0"] * 30
    (directory / "stat").write_text(" ".join(fields) + "\n", encoding="utf-8")
PY
}

fixtures=0
stopped_tree="$fixture_dir/proc-stopped"
make_proc_tree "$stopped_tree" \
  1:postgres:T:311 42:postgres:T:742 43:postgres:R:743 44:sh:R:744
state_out=$(PROC_ROOT="$stopped_tree" sh -eu "$fixture_dir/state.sh") ||
  fail "the process-state reader failed on a synthetic /proc"
[[ "$state_out" == "$(printf '1\tT\t311\n42\tT\t742\n43\tR\t743')" ]] ||
  fail "the process-state reader did not report pid/state/start time: $state_out"
fixtures=$((fixtures + 1))

foreign_tree="$fixture_dir/proc-foreign"
make_proc_tree "$foreign_tree" 1:bash:S:311 42:postgres:T:742
if PROC_ROOT="$foreign_tree" sh -eu "$fixture_dir/state.sh" >/dev/null 2>&1; then
  fail "the process-state reader accepted a container whose PID 1 is not postgres"
fi
fixtures=$((fixtures + 1))

write_probe_arm() {
  local name=$1 body=$2 want=$3 out
  printf '#!/bin/sh\n%s\n' "$body" >"$fixture_dir/psql-$name"
  chmod +x "$fixture_dir/psql-$name"
  out=$(WRITE_PROBE_PSQL="$fixture_dir/psql-$name" \
    WRITE_PROBE_ERRORS="$fixture_dir/psql-$name.err" \
    sh -eu -c "$(<"$fixture_dir/write-probe.sh")" write-probe 2 2>/dev/null) ||
    fail "the write probe failed on the $name stand-in"
  contains "$out" "outcome=$want" ||
    fail "the $name write-probe stand-in was graded $out, expected outcome=$want"
  fixtures=$((fixtures + 1))
}

write_probe_arm completes 'exit 0' completed
write_probe_arm hangs 'sleep 60' blocked
write_probe_arm refuses 'echo "connection refused" >&2; exit 2' error

# Drive the whole probe once against recording stand-ins, so the retained
# document's shape is proven by the script that writes it rather than by a
# fixture someone kept in step by hand. The `kubectl` stand-in runs only the two
# read-only snippets, by allowlist: the pause and continuation snippets signal
# real processes and must never run outside a throwaway container.
standin_dir="$fixture_dir/bin"
mkdir -p "$standin_dir"
cat >"$standin_dir/kubectl" <<'STANDIN'
#!/usr/bin/env bash
set -euo pipefail
command=
skip=0
for arg in "$@"; do
  if [[ "$skip" -eq 1 ]]; then skip=0; continue; fi
  case "$arg" in
    -n|--namespace|--context) skip=1 ;;
    -*) ;;
    *) command=$arg; break ;;
  esac
done
case "$command" in
  wait) exit 0 ;;
  exec)
    body=()
    seen=0
    for arg in "$@"; do
      if [[ "$seen" -eq 1 ]]; then
        body+=("$arg")
      elif [[ "$arg" == "--" ]]; then
        seen=1
      fi
    done
    text=${body[3]:-}
    # Record the pause the round would have taken, so the write stand-in can
    # answer the way a stopped postmaster does.
    if [[ "$text" == *"kill -STOP"* ]]; then
      : >"$STANDIN_STATE/paused"
    elif [[ "$text" == *"kill -CONT"* ]]; then
      rm -f "$STANDIN_STATE/paused"
    fi
    if [[ "$text" == *"kill -STOP"* || "$text" == *"kill -CONT"* ]]; then
      exit 0
    fi
    if [[ "$text" == *PROC_ROOT* || "$text" == *WRITE_PROBE_PSQL* ]]; then
      exec "${body[@]}"
    fi
    exit 0
    ;;
  get)
    if [[ "$*" == *"app=postgres"* ]]; then
      printf 'postgres-0'
    elif [[ "$*" == *"component=query"* ]]; then
      cat "$STANDIN_STATE/query-pods.json"
    elif [[ "$*" == *jsonpath* ]]; then
      printf 'pod-uid-3f1b\t0\t2026-09-07T11:58:00Z'
    else
      cat "$STANDIN_STATE/postgres-pod.json"
    fi
    ;;
  *) exit 64 ;;
esac
STANDIN
cat >"$standin_dir/curl" <<'STANDIN'
#!/usr/bin/env bash
set -euo pipefail
expression=
response=
for arg in "$@"; do
  case "$arg" in
    query=*) expression=${arg#query=} ;;
  esac
done
if [[ -z "$expression" ]]; then
  # A batch submission: accept it and report the code the probe reads.
  for ((i = 1; i <= $#; i++)); do
    if [[ "${!i}" == "-o" ]]; then
      response=${@:i+1:1}
    fi
  done
  if [[ -n "$response" ]]; then
    printf '{"job_id":"job-%s"}' "$RANDOM" >"$response"
  fi
  printf '202'
  exit 0
fi
calls=$(cat "$STANDIN_STATE/backlog-calls" 2>/dev/null || echo 0)
if [[ "$expression" != *jobs_total* && "$expression" != timestamp* ]]; then
  calls=$((calls + 1))
  printf '%s' "$calls" >"$STANDIN_STATE/backlog-calls"
fi
# One baseline call, then the outage samples, then the drained recovery sample.
value=0
if [[ "$expression" == timestamp* ]]; then
  value=$(( $(date +%s) - 1 ))
elif [[ "$expression" == *jobs_total* ]]; then
  if ((calls >= 4)); then value=2; fi
elif ((calls >= 2 && calls <= 3)); then
  value=1
fi
printf '{"status":"success","data":{"resultType":"vector","result":['
printf '{"metric":{"pod":"siglake-query-0"},"value":[%s,"%s"]},' "$(date +%s)" "$value"
printf '{"metric":{"pod":"siglake-query-1"},"value":[%s,"%s"]}' "$(date +%s)" "$value"
printf ']}}'
STANDIN
chmod +x "$standin_dir/kubectl" "$standin_dir/curl"

standin_state="$fixture_dir/state"
mkdir -p "$standin_state/results"
python3 - "$standin_state" <<'PY'
import json
import pathlib
import sys

state = pathlib.Path(sys.argv[1])
def pod(name, image_id):
    return {
        "metadata": {"name": name},
        "spec": {"containers": [{"image": "siglake:kind"}]},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "containerStatuses": [{"imageID": image_id}],
        },
    }

(state / "query-pods.json").write_text(json.dumps({
    "items": [pod("siglake-query-0", "sha256:query0"), pod("siglake-query-1", "sha256:query1")]
}), encoding="utf-8")
postgres = pod("postgres-0", "sha256:postgres")
postgres["spec"]["containers"][0]["image"] = "postgres:16-alpine"
(state / "postgres-pod.json").write_text(json.dumps(postgres), encoding="utf-8")
PY
paused_tree="$fixture_dir/proc-paused"
make_proc_tree "$paused_tree" \
  1:postgres:T:311 42:postgres:T:742 43:postgres:T:743 44:sh:R:744
printf '#!/bin/sh\n[ -e "$STANDIN_STATE/paused" ] && exec sleep 60\nexit 0\n' \
  >"$fixture_dir/psql-standin"
chmod +x "$fixture_dir/psql-standin"

standin_rc=0
PATH="$standin_dir:$PATH" \
  STANDIN_STATE="$standin_state" \
  PROC_ROOT="$paused_tree" \
  WRITE_PROBE_PSQL="$fixture_dir/psql-standin" \
  WRITE_PROBE_ERRORS="$fixture_dir/standin-psql.err" \
  KUBE_CONTEXT=fixture NAMESPACE=fixture PROM_URL=http://fixture.invalid \
  RESULTS_DIR="$standin_state/results" \
  POSTGRES_OUTAGE_JOBS=2 POSTGRES_OUTAGE_SECONDS=2 \
  POSTGRES_OUTAGE_SAMPLE_INTERVAL_SECONDS=1 \
  POSTGRES_OUTAGE_DRAIN_TIMEOUT_SECONDS=2 \
  POSTGRES_OUTAGE_WRITE_PROBE_SECONDS=1 \
  "$PROBE" >"$fixture_dir/standin.log" 2>&1 || standin_rc=$?
[[ "$standin_rc" -eq 0 ]] ||
  fail "the probe did not produce gradeable evidence against stand-ins (exit $standin_rc): $(<"$fixture_dir/standin.log")"
python3 - "$standin_state/results/postgres-outage-reconnect.json" <<'PY' ||
import json, sys
document = json.load(open(sys.argv[1], encoding="utf-8"))
assert document["schema_version"] == 2, document["schema_version"]
evidence = document["evidence"]
assert evidence["grade"] == "verified", evidence["problems"]
summary = evidence["summary"]
assert summary["peak_backlog_total"] == 2, summary
assert summary["pre_restoration_drain"] is None, summary
assert summary["stopped_postgres_processes"] == 3, summary
assert summary["outage_samples_with_process_state"] >= 1, summary
assert summary["max_observation_lag_seconds"] is not None, summary
assert summary["write_probe_outcomes"]["outage"] == ["blocked"], summary
assert summary["write_probe_outcomes"]["baseline"] == ["completed"], summary
PY
  fail "the stand-in probe run did not retain the evidence the grader needs: $(<"$fixture_dir/standin.log")"
fixtures=$((fixtures + 1))

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
import datetime as dt
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
elif mutation == "pre-restoration-drain":
    # The run #76 shape: the backlog empties and completions rise while the
    # samples are still labelled `outage`.
    sample = document["samples"][2]
    for row in sample["backlog"]:
        row["value"] = 0
    for row in sample["completions"]:
        row["value"] += 1
elif mutation == "delayed-observation":
    sample = document["samples"][2]
    for row in sample["backlog"]:
        row["value"] = 0
    for row in sample["completions"]:
        row["value"] += 1
    outage_at = dt.datetime.fromisoformat(
        document["timestamps"]["outage_started_at"].replace("Z", "+00:00")
    ).timestamp()
    for field in ("backlog", "completions"):
        for row in sample[field]:
            row["sample_time"] = outage_at - 1
elif mutation == "missing-scrape-time":
    document["samples"][1]["completions"][0]["sample_time"] = None
elif mutation == "missing-process-state":
    document["samples"][1]["postgres"] = {"exec_status": 1, "processes": []}
elif mutation == "running-backend":
    document["samples"][2]["postgres"]["processes"][1]["state"] = "R"
elif mutation == "postmaster-replaced":
    document["samples"][2]["postgres"]["processes"][0]["starttime"] = "9999"
elif mutation == "fewer-stopped-processes":
    document["samples"][2]["postgres"]["processes"].pop()
elif mutation == "write-completed-in-pause":
    for probe in document["write_probes"]:
        if probe["phase"] == "outage":
            probe["outcome"] = "completed"
            probe["exit_status"] = 0
elif mutation == "no-write-probe-in-pause":
    document["write_probes"] = [
        probe for probe in document["write_probes"] if probe["phase"] != "outage"
    ]
elif mutation == "container-restarted":
    document["postgres_container"]["after"]["restart_count"] = 1
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
expect_unverified pre-restoration-drain 'shows zero backlog with completions up by 2 before restoration'
expect_unverified delayed-observation 'delayed observation of pre-pause work, not a write during the pause'
expect_unverified missing-scrape-time 'no Prometheus scrape timestamp in 1 of 5 samples'
expect_unverified missing-process-state 'no usable Postgres process-state observation in 1 of 2 outage samples'
expect_unverified running-backend 'observed Postgres processes that were not stopped: pid 42 state R'
expect_unverified postmaster-replaced 'the postmaster was replaced during the pause'
expect_unverified fewer-stopped-processes 'the stopped Postgres process set shrank during the pause'
expect_unverified write-completed-in-pause 'while Postgres was paused, so the pause did not block writes'
expect_unverified no-write-probe-in-pause 'no bounded write was attempted while Postgres was paused'
expect_unverified container-restarted 'the Postgres container restarted during the probe'

# The retained run #76 trace, which the previous grader called `verified` with
# `time_to_drain_seconds: 0.0`. It has to stay in the tree and it has to fail:
# its backlog emptied ten seconds before restoration, and it carries neither
# pause evidence nor the scrape timestamps that would date the counters.
run76="$fixture_dir/run76.json"
run76_rc=0
python3 "$GRADER" "$RUN76" --output "$run76" 2>"$fixture_dir/run76.log" || run76_rc=$?
[[ "$run76_rc" -eq 1 ]] || fail "the retained run #76 trace exited $run76_rc, expected 1"
python3 - "$run76" <<'PY' || fail "the retained run #76 trace was not graded on its own evidence"
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["grade"] == "unverified", evidence["grade"]
problems = "\n".join(evidence["problems"])
for want in (
    "shows zero backlog with completions up by 5 before restoration",
    "no usable Postgres process-state observation in 12 of 12 outage samples",
    "no Prometheus scrape timestamp in 14 of 14 samples",
    "missing bounded write-block observations",
):
    assert want in problems, f"{want!r} not in:\n{problems}"
summary = evidence["summary"]
assert summary["time_to_drain_seconds"] is None, summary["time_to_drain_seconds"]
assert summary["pre_restoration_drain"]["at"] == "2026-09-13T22:54:01Z", summary
PY
fixtures=$((fixtures + 1))

echo "ok ($fixtures offline Postgres-outage fixtures; live probe is opt-in)"
