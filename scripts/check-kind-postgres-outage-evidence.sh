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
POSTGRES_MANIFEST=deploy/kind/manifests/postgres.yaml
CHART_DIR=deploy/helm/siglake

fail() { echo "FAIL $*" >&2; exit 1; }
contains() { case "$1" in *"$2"*) ;; *) return 1 ;; esac; }

for file in "$GRADER" "$FIXTURE" "$RUN76" "$ROUND" "$PROBE" "$POSTGRES_MANIFEST"; do
  [[ -f "$file" ]] || fail "$file does not exist"
done

# Commit timestamps are postmaster-only, so the throwaway kind install has to
# ask for them at start-up; Postgres defaults the setting off, which is what
# every other deployment keeps. A chart that started setting it would be a
# behaviour change nobody asked this probe for.
contains "$(<"$POSTGRES_MANIFEST")" 'args: ["-c", "track_commit_timestamp=on"]' ||
  fail "$POSTGRES_MANIFEST does not start kind Postgres with commit timestamps tracked"
if grep -rq 'track_commit_timestamp' "$CHART_DIR" deploy/aws deploy/docker-compose.yml 2>/dev/null; then
  fail "track_commit_timestamp leaked out of the throwaway kind install"
fi

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
contains "$probe_body" '"schema_version": 3' ||
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

# A counter cannot say when a row was written, so the row itself has to be
# dated. The commit-time reading is taken after the bounded recovery window --
# a ready postmaster is not a finished reconciliation -- and the stamps read
# either side of the pause and continuation execs are what place a commit
# inside the window rather than against a stamp taken before the signal.
contains "$probe_body" 'COMMIT_TIMES_STATUS=$(postgres_commit_times "$TMP_DIR/commit-times")' ||
  fail "$PROBE does not read the job rows' commit timestamps"
contains "$probe_body" 'pg_xact_commit_timestamp(xmin)' ||
  fail "$PROBE does not date the job rows by their Postgres commit timestamp"
contains "$probe_body" 'SHOW track_commit_timestamp' ||
  fail "$PROBE does not retain the effective track_commit_timestamp setting"
python3 - "$PROBE" <<'PY' || fail "$PROBE does not collect commit times after the bounded recovery window"
import sys

lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
recovery = next(i for i, line in enumerate(lines)
                if line == "SAMPLING_ENDED_AT=$(iso_now)")
collect = next(i for i, line in enumerate(lines)
               if line.startswith("COMMIT_TIMES_STATUS="))
ready = next(i for i, line in enumerate(lines)
             if line == "POSTGRES_READY_AT=$(iso_now)")
if not ready < recovery < collect:
    raise SystemExit(
        "expected readiness, the end of the recovery window and the commit-time read in "
        f"order; got {ready + 1}, {recovery + 1}, {collect + 1}"
    )
PY
for stamp in PAUSE_APPLIED_AT RESTORATION_APPLIED_AT; do
  contains "$probe_body" "$stamp=\$(iso_now)" ||
    fail "$PROBE does not stamp the applied side of the pause/restoration signal ($stamp)"
done
contains "$probe_body" '"pause_applied_at": pause_applied,' ||
  fail "$PROBE does not retain pause_applied_at"
contains "$probe_body" '"restoration_applied_at": restoration_applied,' ||
  fail "$PROBE does not retain restoration_applied_at"
# `pg_xact_commit_timestamp` raises when the setting is off, so the reader must
# not report a failed query as a row set with nothing committed in the pause.
contains "$probe_body" '"exec_status": int(exec_status) if exec_status.isdigit() else 1,' ||
  fail "$PROBE does not retain the commit-time reader's exec status"

# The three remote readers are the pieces a cluster would run, so run their
# exact text here: the state reader against a synthetic /proc, the write probe
# against a psql stand-in that returns, hangs, or fails, and the commit-time
# reader against one that has the setting on and one that has it off.
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
extract_snippet commit-times-snippet "$fixture_dir/commit-times.sh"

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

# The commit-time reader, against a psql stand-in that answers `SHOW` and the
# row query separately. The second arm is the one that matters: with the
# setting off, `pg_xact_commit_timestamp` raises, and the reader has to come
# back saying so rather than as an empty row set.
commit_times_arm() {
  local name=$1 setting=$2 rows=$3 rc=$4 out
  cat >"$fixture_dir/psql-commit-$name" <<STANDIN
#!/bin/sh
for arg in "\$@"; do
  case "\$arg" in
    "SHOW track_commit_timestamp") printf '%s\n' "$setting"; exit 0 ;;
  esac
done
printf '%s' "$rows"
[ "$rc" -eq 0 ] || echo "ERROR: could not get commit timestamp data" >&2
exit "$rc"
STANDIN
  chmod +x "$fixture_dir/psql-commit-$name"
  out=$(COMMIT_TIMES_PSQL="$fixture_dir/psql-commit-$name" \
    COMMIT_TIMES_ERRORS="$fixture_dir/psql-commit-$name.err" \
    COMMIT_TIMES_ROWS="$fixture_dir/psql-commit-$name.rows" \
    sh -eu -c "$(<"$fixture_dir/commit-times.sh")" commit-times)
  printf '%s' "$out"
}

on_arm=$(commit_times_arm on on \
  'job-a	succeeded	2026-09-07 12:00:01+00	2026-09-07 12:00:51+00		2026-09-07 12:00:51.88+00
' 0)
contains "$on_arm" "$(printf 'status\t0\t0')" ||
  fail "the commit-time reader did not report both query statuses: $on_arm"
contains "$on_arm" "$(printf 'setting\ton')" ||
  fail "the commit-time reader did not report the effective setting: $on_arm"
contains "$on_arm" "$(printf 'row\tjob-a\tsucceeded')" ||
  fail "the commit-time reader did not tag its rows: $on_arm"
fixtures=$((fixtures + 1))

off_arm=$(commit_times_arm off off '' 3)
contains "$off_arm" "$(printf 'status\t0\t3')" ||
  fail "the commit-time reader hid a failed row query: $off_arm"
contains "$off_arm" "$(printf 'setting\toff')" ||
  fail "the commit-time reader did not report the off setting: $off_arm"
contains "$off_arm" 'could not get commit timestamp data' ||
  fail "the commit-time reader dropped the error detail: $off_arm"
if contains "$off_arm" "$(printf 'row\t')"; then
  fail "the commit-time reader invented rows from a failed query: $off_arm"
fi
fixtures=$((fixtures + 1))

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
    if [[ "$text" == *PROC_ROOT* || "$text" == *WRITE_PROBE_PSQL* \
      || "$text" == *COMMIT_TIMES_PSQL* ]]; then
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
    # Named after the probe's own submission index, and recorded, so the psql
    # stand-in can answer with commit times for the ids that were accepted.
    job_id="job-$(basename "$response" .response.json)"
    printf '{"job_id":"%s"}' "$job_id" >"$response"
    printf '%s\n' "$job_id" >>"$STANDIN_STATE/job-ids"
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

# The commit-time stand-in answers for the ids the curl stand-in accepted. Its
# timestamps sit a few seconds ahead of the stand-in's own collection instant:
# the grader places a commit against the recorded pause and restoration stamps,
# and this keeps the arm from depending on how long a stand-in recovery loop
# happens to take to reach the read.
cat >"$fixture_dir/psql-commit-standin" <<'STANDIN'
#!/bin/sh
for arg in "$@"; do
  case "$arg" in
    "SHOW track_commit_timestamp") echo on; exit 0 ;;
  esac
done
committed=$(date -u -d "+3 seconds" "+%Y-%m-%d %H:%M:%S.%6N+00")
while IFS= read -r job_id; do
  printf '%s\tsucceeded\t%s\t%s\t\t%s\n' "$job_id" "$committed" "$committed" "$committed"
done <"$STANDIN_STATE/job-ids"
STANDIN
chmod +x "$fixture_dir/psql-commit-standin"

standin_rc=0
PATH="$standin_dir:$PATH" \
  STANDIN_STATE="$standin_state" \
  PROC_ROOT="$paused_tree" \
  WRITE_PROBE_PSQL="$fixture_dir/psql-standin" \
  WRITE_PROBE_ERRORS="$fixture_dir/standin-psql.err" \
  COMMIT_TIMES_PSQL="$fixture_dir/psql-commit-standin" \
  COMMIT_TIMES_ERRORS="$fixture_dir/standin-commit.err" \
  COMMIT_TIMES_ROWS="$fixture_dir/standin-commit.rows" \
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
assert document["schema_version"] == 3, document["schema_version"]
evidence = document["evidence"]
assert evidence["grade"] == "verified", evidence["problems"]
summary = evidence["summary"]
commits = summary["job_commit_times"]
assert commits["track_commit_timestamp"] == "on", commits
assert commits["correlated_jobs"] == 2, commits
assert commits["committed_after_restoration"] == 2, commits
assert commits["committed_in_pause"] == [], commits
assert commits["unplaceable_commits"] == [], commits
assert commits["gaps"] == [], commits
assert commits["settles_pause"] is True, commits
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
mutate() {
  local mutation=$1 input=$2
  python3 - "$FIXTURE" "$input" "$mutation" <<'PY'
import datetime as dt
import json, sys
source, destination, mutation = sys.argv[1:]
document = json.load(open(source, encoding="utf-8"))

def commit_row(job_id):
    return next(
        row for row in document["job_commit_times"]["rows"] if row["job_id"] == job_id
    )

def pre_restoration_drain():
    """Run #76's shape: the backlog empties and completions rise while the
    samples are still labelled `outage`."""
    sample = document["samples"][2]
    for row in sample["backlog"]:
        row["value"] = 0
    for row in sample["completions"]:
        row["value"] += 1

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
    pre_restoration_drain()
elif mutation == "pre-restoration-drain-undated":
    pre_restoration_drain()
    document["job_commit_times"]["rows"] = [
        row for row in document["job_commit_times"]["rows"] if row["job_id"] != "job-b"
    ]
elif mutation == "commit-inside-pause":
    pre_restoration_drain()
    commit_row("job-b")["committed_at"] = "2026-09-07 12:00:20.551200+00"
elif mutation == "commit-at-pause-edge":
    # Within the second the pause stamps are truncated to: the signal may not
    # have landed yet when this commit was made.
    commit_row("job-b")["committed_at"] = "2026-09-07 12:00:04.412000+00"
elif mutation == "commit-at-restoration-edge":
    commit_row("job-b")["committed_at"] = "2026-09-07 12:00:48.114000+00"
elif mutation == "amended-job-row":
    pre_restoration_drain()
    commit_row("job-a")["recovered_at"] = "2026-09-07 12:00:51.000000+00"
elif mutation == "null-commit-time":
    commit_row("job-b")["committed_at"] = None
elif mutation == "nonterminal-job-row":
    commit_row("job-b")["status"] = "running"
elif mutation == "stray-in-pause-commit":
    # Not one of this burst's ids, and still a write that landed while the
    # processes were stopped -- which is what the pause claims is impossible.
    document["job_commit_times"]["rows"].append({
        "job_id": "job-from-an-earlier-round",
        "status": "succeeded",
        "submitted_at": "2026-09-07 11:59:00+00",
        "ended_at": "2026-09-07 12:00:20+00",
        "recovered_at": None,
        "committed_at": "2026-09-07 12:00:21.330000+00",
    })
elif mutation == "uncorrelated-job-row":
    commit_row("job-b")["job_id"] = "job-from-an-earlier-round"
elif mutation == "commit-tracking-off":
    document["job_commit_times"]["track_commit_timestamp"] = "off"
elif mutation == "commit-query-failed":
    document["job_commit_times"]["query_status"] = 3
    document["job_commit_times"]["rows"] = []
    document["job_commit_times"]["detail"] = "ERROR: could not get commit timestamp data"
elif mutation == "missing-commit-times":
    del document["job_commit_times"]
elif mutation == "delayed-observation":
    # A drain whose scrape predates the pause, with the commit times dropped:
    # the scrape provenance is the only reading left, and it is not enough to
    # settle where the write landed.
    pre_restoration_drain()
    document["job_commit_times"]["rows"] = []
    outage_at = dt.datetime.fromisoformat(
        document["timestamps"]["outage_started_at"].replace("Z", "+00:00")
    ).timestamp()
    for field in ("backlog", "completions"):
        for row in document["samples"][2][field]:
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
}

expect_unverified() {
  local mutation=$1 want=$2
  local input="$fixture_dir/${mutation}.input.json"
  local output="$fixture_dir/${mutation}.output.json"
  local log="$fixture_dir/${mutation}.log" rc=0
  mutate "$mutation" "$input"
  python3 "$GRADER" "$input" --output "$output" 2>"$log" || rc=$?
  [[ "$rc" -eq 1 ]] || fail "$mutation exited $rc, expected the unverified exit 1"
  grade=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["evidence"]["grade"])' "$output")
  [[ "$grade" == unverified ]] || fail "$mutation was not graded unverified"
  problem=$(python3 -c 'import json,sys; print("\n".join(json.load(open(sys.argv[1]))["evidence"]["problems"]))' "$output")
  contains "$problem" "$want" || fail "$mutation was caught for the wrong reason: $problem"
  fixtures=$((fixtures + 1))
}

# The other half of the reading: a drain observation whose accepted job rows are
# all dated outside the pause is resolved, not a standing problem. It has to
# stay a `verified` grade with the resolution recorded, otherwise the commit
# times are just another unreadable observation.
expect_resolved() {
  local mutation=$1
  local input="$fixture_dir/${mutation}.input.json"
  local output="$fixture_dir/${mutation}.output.json"
  local log="$fixture_dir/${mutation}.log"
  mutate "$mutation" "$input"
  python3 "$GRADER" "$input" --output "$output" 2>"$log" ||
    fail "$mutation did not clear the pre-restoration drain: $(cat "$log")"
  python3 - "$output" <<'PY' || fail "$mutation did not record how the drain was resolved"
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["grade"] == "verified", evidence["problems"]
drain = evidence["summary"]["pre_restoration_drain"]
assert drain is not None, evidence["summary"]
resolution = drain["resolution"]
assert resolution["state"] == "resolved", resolution
assert resolution["by"] == "job_commit_times", resolution
assert "none inside it" in resolution["detail"], resolution
problems = "\n".join(evidence["problems"])
assert "the pause window is unexplained" not in problems, problems
PY
  fixtures=$((fixtures + 1))
}

expect_unverified missing-series 'no usable backlog observation'
expect_unverified no-rise 'no observed unreconciled backlog'
expect_unverified no-drain 'did not drain after restoration'
expect_unverified missing-revision 'missing pinned repository revision'
expect_resolved pre-restoration-drain
expect_unverified pre-restoration-drain-undated 'shows zero backlog with completions up by 2 before restoration'
expect_unverified pre-restoration-drain-undated 'no job row for accepted job job-b'
expect_unverified commit-inside-pause 'job job-b committed at 2026-09-07T12:00:20.551200+00:00, inside the pause window'
expect_unverified commit-inside-pause 'shows zero backlog with completions up by 2 before restoration'
expect_unverified commit-at-pause-edge 'within 1s of the pause applied at 2026-09-07T12:00:04+00:00'
expect_unverified commit-at-restoration-edge 'within 1s of restoration at 2026-09-07T12:00:48+00:00'
expect_unverified amended-job-row 'may be an amendment of an earlier terminal write'
expect_unverified amended-job-row 'the pause window is unexplained'
expect_unverified null-commit-time 'job job-b has no usable commit timestamp'
expect_unverified nonterminal-job-row "job job-b is 'running' after the recovery window"
expect_unverified uncorrelated-job-row 'no job row for accepted job job-b'
expect_unverified stray-in-pause-commit 'job job-from-an-earlier-round committed at 2026-09-07T12:00:21.330000+00:00, inside the pause window'
expect_unverified commit-tracking-off "track_commit_timestamp='off'"
expect_unverified commit-query-failed 'the job-row commit-time query did not run'
expect_unverified missing-commit-times 'missing job-row commit-time observations'
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
