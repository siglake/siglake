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

# PID 1 ignores SIGSTOP from its own PID namespace. The probe must resolve the
# current CRI container through its owning kind node and signal the exact
# postmaster/backend identities from that ancestor namespace.
contains "$probe_body" 'pause_postgres_processes()' ||
  fail "$PROBE has no bounded Postgres pause helper"
contains "$probe_body" 'continue_postgres_processes()' ||
  fail "$PROBE has no bounded Postgres continuation helper"
contains "$probe_body" 'docker exec "$POSTGRES_NODE" crictl inspect "$POSTGRES_CONTAINER_ID"' ||
  fail "$PROBE does not resolve the current CRI container on its owning kind node"
contains "$probe_body" 'docker exec "$POSTGRES_NODE" sh -eu -c "$POSTGRES_NODE_SIGNAL_SNIPPET"' ||
  fail "$PROBE does not signal from the kind node PID namespace"
contains "$probe_body" 'io.kubernetes.pod.uid' ||
  fail "$PROBE does not bind the CRI container to the selected pod UID"
contains "$probe_body" 'pid_namespace=$(readlink "/proc/$init_pid/ns/pid")' ||
  fail "$PROBE does not bind backends to the Postgres PID namespace"
contains "$probe_body" 'grep -Fq -- "$container_id" "/proc/$pid/cgroup"' ||
  fail "$PROBE does not bind signalled processes to the current container cgroup"
contains "$probe_body" 'the Postgres process set did not reach the state required by $signal' ||
  fail "$PROBE does not reject an ineffective STOP"
contains "$probe_body" 'continue_postgres_processes >/dev/null 2>&1 || true' ||
  fail "$PROBE trap cleanup does not restore the backend process set"
[[ $(grep -c '^continue_postgres_processes >"$TMP_DIR/restoration-signal-boundaries"$' "$PROBE") -eq 1 ]] ||
  fail "$PROBE normal path does not restore through the shared continuation helper"
contains "$probe_body" 'node-signal-group "$signal" "$POSTGRES_CONTAINER_PID"' ||
  fail "$PROBE does not signal the recorded process set in one kind-node exec"
contains "$probe_body" 'started_at=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)' ||
  fail "$PROBE does not measure signal start inside the kind-node exec"
if grep -A30 '^pause_postgres_processes()' "$PROBE" | grep -q 'kubectl .*exec'; then
  fail "$PROBE still sends STOP from inside the Postgres container namespace"
fi
if contains "$probe_body" 'kill -STOP -1'; then
  fail "$PROBE must not treat kill -STOP -1 as a targeted process-group signal"
fi

# Run #76 graded `verified` on a trace with no evidence that the pause held and
# no way to date the counters it read, so the probe must now retain a process
# state per sample, a bounded write in each phase, the container's identity
# across the window, and the Prometheus scrape each value came from.
contains "$probe_body" '"schema_version": 5' ||
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
contains "$probe_body" 'iso_now() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }' ||
  fail "$PROBE does not retain millisecond precision for its local observations"

# A counter cannot say when a row was written, so the row itself has to be
# dated. The commit-time reading is taken after the bounded recovery window --
# a ready postmaster is not a finished reconciliation -- and the stamps read
# at the first signal attempt and after every process reaches its new state
# place a commit inside the measured transition window.
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
# The visible row version is not the terminal write when the amendment path
# rewrote it, so the probe installs an insert-only transition history on the
# throwaway Postgres before it submits anything, and reads it back dated by the
# transaction that wrote each job row.
contains "$probe_body" 'JOB_HISTORY_INSTALL_STATUS=$(postgres_install_job_history' ||
  fail "$PROBE does not install the job-row write history"
contains "$probe_body" 'JOB_HISTORY_STATUS=$(postgres_job_history "$TMP_DIR/job-history")' ||
  fail "$PROBE does not read the job-row write history back"
contains "$probe_body" 'FOR EACH ROW EXECUTE FUNCTION siglake_outage_record_job_write();' ||
  fail "$PROBE does not record a history row per job-row write"
contains "$probe_body" 'pg_xact_commit_timestamp(xmin) FROM siglake_outage_job_history ORDER BY seq' ||
  fail "$PROBE does not date each history row by the transaction that wrote it"
if grep -rq 'siglake_outage_job_history' "$CHART_DIR" deploy/aws deploy/docker-compose.yml \
  crates 2>/dev/null; then
  fail "the probe's job write history leaked out of the throwaway kind install"
fi
python3 - "$PROBE" <<'PY' || fail "$PROBE does not install the write history before the burst"
import sys

lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
install = next(i for i, line in enumerate(lines)
               if line.startswith("JOB_HISTORY_INSTALL_STATUS="))
burst = next(i for i, line in enumerate(lines)
             if line == 'for index in $(seq 1 "$REQUESTED_JOBS"); do')
read = next(i for i, line in enumerate(lines)
            if line.startswith("JOB_HISTORY_STATUS="))
collect = next(i for i, line in enumerate(lines)
               if line.startswith("COMMIT_TIMES_STATUS="))
if not install < burst < collect < read:
    raise SystemExit(
        "expected the history install, the burst, the commit-time read and the history read "
        f"in order; got {install + 1}, {burst + 1}, {collect + 1}, {read + 1}"
    )
PY
# The write probe is the one write whose side of the pause the probe knows in
# advance, so its rows carry the phase that wrote them and are read back dated
# by Postgres. Without that control, a clock skew between the kind node and
# Postgres moves every job-row commit together and leaves no trace.
contains "$probe_body" 'INSERT INTO siglake_outage_write_probe (phase) VALUES ($phase_literal)' ||
  fail "$PROBE does not record which phase took each write-probe row"
contains "$probe_body" 'write-probe "$WRITE_PROBE_SECONDS"' ||
  fail "$PROBE does not pass the write-probe timeout to its snippet"
contains "$probe_body" 'WRITE_PROBE_TIMES_STATUS=$(postgres_write_probe_times' ||
  fail "$PROBE does not read the write probe's own commit timestamps"
contains "$probe_body" 'pg_xact_commit_timestamp(xmin) FROM siglake_outage_write_probe' ||
  fail "$PROBE does not date the write-probe rows by their Postgres commit timestamp"
if grep -rq 'siglake_outage_write_probe' "$CHART_DIR" deploy/aws deploy/docker-compose.yml \
  crates 2>/dev/null; then
  fail "the probe's write-probe control table leaked out of the throwaway kind install"
fi
python3 - "$PROBE" <<'PY' || fail "$PROBE does not date the write-probe rows after the recovery write"
import sys

lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
recovery = next(i for i, line in enumerate(lines)
                if line == "postgres_write_probe recovery")
read = next(i for i, line in enumerate(lines)
            if line.startswith("WRITE_PROBE_TIMES_STATUS="))
if not recovery < read:
    raise SystemExit(
        f"expected the recovery write probe before the reading that dates it; got "
        f"{recovery + 1}, {read + 1}"
    )
PY
contains "$probe_body" 'PAUSE_APPLIED_AT=$(awk' ||
  fail "$PROBE does not retain the measured pause-applied bound"
contains "$probe_body" 'RESTORATION_APPLIED_AT=$(awk' ||
  fail "$PROBE does not retain the measured restoration-applied bound"
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
extract_snippet node-process-list-snippet "$fixture_dir/node-process-list.sh"
extract_snippet node-signal-snippet "$fixture_dir/node-signal.sh"
extract_snippet job-history-install-snippet "$fixture_dir/job-history-install.sh"
extract_snippet job-history-snippet "$fixture_dir/job-history.sh"
extract_snippet write-probe-times-snippet "$fixture_dir/write-probe-times.sh"
sh -n "$fixture_dir/node-process-list.sh" "$fixture_dir/node-signal.sh" \
  "$fixture_dir/job-history-install.sh" "$fixture_dir/job-history.sh" \
  "$fixture_dir/write-probe-times.sh" ||
  fail "the kind-node process snippets are not valid POSIX shell"

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
  printf '#!/bin/sh\nprintf "%%s\\n" "$@" >"%s"\n%s\n' \
    "$fixture_dir/psql-$name.argv" "$body" >"$fixture_dir/psql-$name"
  chmod +x "$fixture_dir/psql-$name"
  out=$(WRITE_PROBE_PSQL="$fixture_dir/psql-$name" \
    WRITE_PROBE_ERRORS="$fixture_dir/psql-$name.err" \
    sh -eu -c "$(<"$fixture_dir/write-probe.sh")" write-probe 2 '$phase$baseline$phase$' \
    2>/dev/null) ||
    fail "the write probe failed on the $name stand-in"
  contains "$out" "outcome=$want" ||
    fail "the $name write-probe stand-in was graded $out, expected outcome=$want"
  fixtures=$((fixtures + 1))
}

write_probe_arm completes 'exit 0' completed
write_probe_arm hangs 'sleep 60' blocked
write_probe_arm refuses 'echo "connection refused" >&2; exit 2' error

# The phase reaches psql as the caller's dollar-quoted literal. The snippet is
# a single-quoted bash string run by `sh -eu -c`, so a tag written inside it
# would have been expanded before psql saw it -- invisible in review, and only
# visible against a real Postgres.
contains "$(<"$fixture_dir/psql-completes.argv")" \
  'INSERT INTO siglake_outage_write_probe (phase) VALUES ($phase$baseline$phase$)' ||
  fail "the write probe did not send its phase as an intact SQL literal: $(<"$fixture_dir/psql-completes.argv")"
contains "$(<"$fixture_dir/psql-completes.argv")" \
  'CREATE TABLE IF NOT EXISTS siglake_outage_write_probe (phase text NOT NULL' ||
  fail "the write probe's control table does not carry the phase that wrote each row"
fixtures=$((fixtures + 1))

# The reader that dates those rows, with the table present and with it absent.
write_probe_times_arm() {
  local name=$1 rows=$2 rc=$3 out
  cat >"$fixture_dir/psql-probe-times-$name" <<STANDIN
#!/bin/sh
printf '%s' "$rows"
[ "$rc" -eq 0 ] || echo "ERROR: relation siglake_outage_write_probe does not exist" >&2
exit "$rc"
STANDIN
  chmod +x "$fixture_dir/psql-probe-times-$name"
  out=$(WRITE_PROBE_TIMES_PSQL="$fixture_dir/psql-probe-times-$name" \
    WRITE_PROBE_TIMES_ERRORS="$fixture_dir/psql-probe-times-$name.err" \
    WRITE_PROBE_TIMES_ROWS="$fixture_dir/psql-probe-times-$name.rows" \
    sh -eu -c "$(<"$fixture_dir/write-probe-times.sh")" write-probe-times)
  printf '%s' "$out"
}

probe_times_arm=$(write_probe_times_arm present \
  'baseline	2026-09-07 12:00:00.331904+00	2026-09-07 12:00:00.332715+00
recovery	2026-09-07 12:00:51.108220+00	2026-09-07 12:00:51.109044+00
' 0)
contains "$probe_times_arm" "$(printf 'status\t0')" ||
  fail "the write-probe commit-time reader did not report its query status: $probe_times_arm"
contains "$probe_times_arm" "$(printf 'row\tbaseline\t2026-09-07 12:00:00.331904+00')" ||
  fail "the write-probe commit-time reader did not tag its rows: $probe_times_arm"
fixtures=$((fixtures + 1))

missing_probe_times_arm=$(write_probe_times_arm absent '' 3)
contains "$missing_probe_times_arm" "$(printf 'status\t3')" ||
  fail "the write-probe commit-time reader hid a failed query: $missing_probe_times_arm"
contains "$missing_probe_times_arm" 'relation siglake_outage_write_probe does not exist' ||
  fail "the write-probe commit-time reader dropped the error detail: $missing_probe_times_arm"
if contains "$missing_probe_times_arm" "$(printf 'row\t')"; then
  fail "the write-probe commit-time reader invented rows from a failed query: $missing_probe_times_arm"
fi
fixtures=$((fixtures + 1))

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

# The history installer, against a psql stand-in that keeps the DDL it was fed.
# Both snippets are single-quoted bash strings run by `sh -eu -c`, so the
# dollar-quote tags that carry the plpgsql body would become the shell's PID if
# the DDL were not in a quoted heredoc. That is invisible in a review and only
# fails against a real Postgres, so the tags are checked in what psql received.
job_history_install_arm() {
  local name=$1 rc=$2 out
  cat >"$fixture_dir/psql-history-$name" <<STANDIN
#!/bin/sh
cat >"$fixture_dir/history-ddl-$name"
[ "$rc" -eq 0 ] || echo "ERROR: relation siglake_query_jobs does not exist" >&2
exit "$rc"
STANDIN
  chmod +x "$fixture_dir/psql-history-$name"
  out=$(JOB_HISTORY_PSQL="$fixture_dir/psql-history-$name" \
    JOB_HISTORY_INSTALL_ERRORS="$fixture_dir/psql-history-$name.err" \
    sh -eu -c "$(<"$fixture_dir/job-history-install.sh")" job-history-install)
  printf '%s' "$out"
}

install_arm=$(job_history_install_arm ok 0)
contains "$install_arm" "$(printf 'install\t0')" ||
  fail "the history installer did not report its status: $install_arm"
install_ddl="$(<"$fixture_dir/history-ddl-ok")"
contains "$install_ddl" 'LANGUAGE plpgsql AS $fn$' ||
  fail "the shell expanded the plpgsql dollar-quote tag before psql saw it: $install_ddl"
contains "$install_ddl" 'CASE WHEN TG_OP = $op$INSERT$op$ THEN NULL ELSE OLD.status END' ||
  fail "the shell expanded the INSERT literal's dollar-quote tag: $install_ddl"
contains "$install_ddl" 'AFTER INSERT OR UPDATE ON siglake_query_jobs' ||
  fail "the history trigger does not fire on every job-row write: $install_ddl"
[[ $(grep -c -F -- '$fn$' "$fixture_dir/history-ddl-ok") -eq 2 ]] ||
  fail "the plpgsql body is not delimited by a matched dollar-quote pair: $install_ddl"
fixtures=$((fixtures + 1))

failed_install_arm=$(job_history_install_arm missing 3)
contains "$failed_install_arm" "$(printf 'install\t3')" ||
  fail "the history installer hid a failed install: $failed_install_arm"
contains "$failed_install_arm" 'relation siglake_query_jobs does not exist' ||
  fail "the history installer dropped the install error: $failed_install_arm"
fixtures=$((fixtures + 1))

# The reader, with the history present and with the table absent. The second
# arm has to come back as a failed query, not as a job that made no writes.
job_history_read_arm() {
  local name=$1 rows=$2 rc=$3 out
  cat >"$fixture_dir/psql-history-read-$name" <<STANDIN
#!/bin/sh
printf '%s' "$rows"
[ "$rc" -eq 0 ] || echo "ERROR: relation siglake_outage_job_history does not exist" >&2
exit "$rc"
STANDIN
  chmod +x "$fixture_dir/psql-history-read-$name"
  out=$(JOB_HISTORY_PSQL="$fixture_dir/psql-history-read-$name" \
    JOB_HISTORY_ERRORS="$fixture_dir/psql-history-read-$name.err" \
    JOB_HISTORY_ROWS="$fixture_dir/psql-history-read-$name.rows" \
    sh -eu -c "$(<"$fixture_dir/job-history.sh")" job-history)
  printf '%s' "$out"
}

history_arm=$(job_history_read_arm present \
  '1	job-a	INSERT		pending		2026-09-07 12:00:01.204817+00	2026-09-07 12:00:01.205901+00
2	job-a	UPDATE	running	failed	2026-09-07 12:00:51+00	2026-09-07 12:00:51.771902+00	2026-09-07 12:00:51.884113+00
' 0)
contains "$history_arm" "$(printf 'status\t0')" ||
  fail "the history reader did not report its query status: $history_arm"
contains "$history_arm" "$(printf 'row\t2\tjob-a\tUPDATE\trunning\tfailed')" ||
  fail "the history reader did not tag its rows: $history_arm"
fixtures=$((fixtures + 1))

missing_history_arm=$(job_history_read_arm absent '' 3)
contains "$missing_history_arm" "$(printf 'status\t3')" ||
  fail "the history reader hid a failed query: $missing_history_arm"
contains "$missing_history_arm" 'relation siglake_outage_job_history does not exist' ||
  fail "the history reader dropped the error detail: $missing_history_arm"
if contains "$missing_history_arm" "$(printf 'row\t')"; then
  fail "the history reader invented rows from a failed query: $missing_history_arm"
fi
fixtures=$((fixtures + 1))

# Drive the whole probe once against recording stand-ins, so the retained
# document's shape is proven by the script that writes it rather than by a
# fixture someone kept in step by hand. The `kubectl` stand-in runs only the
# read-only in-container snippets. The recording `docker` stand-in proves that
# STOP/CONT target the owning kind node and supplies controlled process states.
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
    if [[ "$text" == *PROC_ROOT* || "$text" == *WRITE_PROBE_PSQL* \
      || "$text" == *COMMIT_TIMES_PSQL* || "$text" == *JOB_HISTORY_PSQL* \
      || "$text" == *WRITE_PROBE_TIMES_PSQL* ]]; then
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
cat >"$standin_dir/docker" <<'STANDIN'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == inspect ]]; then
  printf 'true\tfixture\tcontrol-plane\t/fixture-control-plane\n'
  exit 0
fi
[[ "${1:-}" == exec && "${2:-}" == fixture-control-plane ]] || exit 64
shift 2
if [[ "${1:-}" == crictl && "${2:-}" == inspect ]]; then
  cat <<JSON
{"status":{"id":"${3}","metadata":{"name":"postgres"},"state":"CONTAINER_RUNNING","labels":{"io.kubernetes.pod.uid":"pod-uid-3f1b"}},"info":{"pid":100}}
JSON
  exit 0
fi
action=
action_index=0
for ((i = 1; i <= $#; i++)); do
  case "${!i}" in
    node-process-list|node-signal-group) action=${!i}; action_index=$i; break ;;
  esac
done
case "$action" in
  node-process-list)
    for pid in 100 142 143; do
      state=S
      [[ -e "$STANDIN_STATE/stopped-$pid" ]] && state=T
      printf '%s\t%s\t%s\tpid:[4026533000]\n' "$pid" "$state" "$((700 + pid))"
    done
    ;;
  node-signal-group)
    signal_index=$((action_index + 1))
    init_index=$((action_index + 2))
    identities_index=$((action_index + 4))
    signal=${!signal_index}
    init_pid=${!init_index}
    identities=${!identities_index}
    printf '%s\n' "$signal" >>"$STANDIN_STATE/signal-execs"
    started_at=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
    signal_one() {
      local pid=$1
      printf '%s %s\n' "$signal" "$pid" >>"$STANDIN_STATE/signals"
      if [[ "$signal" == STOP ]]; then
        if [[ "${STANDIN_STOP_MODE:-working}" == partial && "$pid" == 142 ]]; then
          return 17
        fi
        if [[ "${STANDIN_STOP_MODE:-working}" != ineffective ]]; then
          : >"$STANDIN_STATE/stopped-$pid"
          : >"$STANDIN_STATE/paused"
        fi
      else
        rm -f "$STANDIN_STATE/stopped-$pid"
        if [[ "$pid" == "$init_pid" ]]; then
          rm -f "$STANDIN_STATE/paused"
        fi
      fi
      return 0
    }
    if [[ "$signal" == STOP ]]; then
      while IFS=$'\t' read -r pid _; do
        signal_one "$pid" || exit $?
      done <<<"$identities"
    else
      status=0
      while IFS=$'\t' read -r pid _; do
        [[ "$pid" == "$init_pid" ]] && continue
        signal_one "$pid" || status=1
      done <<<"$identities"
      while IFS=$'\t' read -r pid _; do
        [[ "$pid" == "$init_pid" ]] || continue
        signal_one "$pid" || status=1
      done <<<"$identities"
      if [[ "$status" -ne 0 ]]; then
        exit "$status"
      fi
    fi
    printf 'started_at\t%s\napplied_at\t%s\n' \
      "$started_at" "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
    exit 0
    ;;
  *) exit 64 ;;
esac
STANDIN
# Keep the offline trace ordered even if the host wall clock steps while other
# gate jobs run. Epoch seconds still delegate to the real clock for timeouts;
# only retained ISO stamps, scrape generations and the fixture commit move on
# this fixed millisecond clock.
cat >"$standin_dir/date" <<'STANDIN'
#!/usr/bin/env bash
set -euo pipefail
counter_file="$STANDIN_STATE/iso-clock"
if [[ $# -eq 2 && $1 == -u && $2 == +%Y-%m-%dT%H:%M:%S.%3NZ ]]; then
  counter=$(cat "$counter_file" 2>/dev/null || echo 0)
  counter=$((counter + 1))
  printf '%s' "$counter" >"$counter_file"
  python3 - "$counter" <<'PY'
import datetime as dt
import sys

base = dt.datetime(2026, 9, 20, 12, tzinfo=dt.timezone.utc)
stamp = base + dt.timedelta(milliseconds=int(sys.argv[1]))
print(stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z"))
PY
  exit 0
fi
if [[ $# -eq 1 && $1 == +%s.%N ]]; then
  counter=$(cat "$counter_file" 2>/dev/null || echo 0)
  python3 - "$counter" <<'PY'
import datetime as dt
import sys

base = dt.datetime(2026, 9, 20, 12, tzinfo=dt.timezone.utc).timestamp()
print(f"{base + int(sys.argv[1]) / 1000:.3f}")
PY
  exit 0
fi
if [[
  $# -eq 4 && $1 == -u && $2 == -d && $3 == "+3 seconds" &&
  $4 == +%Y-%m-%d\ %H:%M:%S.%6N+00
]]; then
  printf '%s\n' '2026-09-20 12:01:00.000000+00'
  exit 0
fi
exec /usr/bin/date "$@"
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
  value=$(date +%s.%N)
elif [[ "$expression" == *jobs_total* ]]; then
  if [[ ! -e "$STANDIN_STATE/paused" ]] && ((calls >= 4)); then value=2; fi
elif ((calls >= 2 && calls <= 3)); then
  value=1
fi
printf '{"status":"success","data":{"resultType":"vector","result":['
printf '{"metric":{"pod":"siglake-query-0"},"value":[%s,"%s"]},' "$(date +%s)" "$value"
printf '{"metric":{"pod":"siglake-query-1"},"value":[%s,"%s"]}' "$(date +%s)" "$value"
printf ']}}'
STANDIN
chmod +x \
  "$standin_dir/kubectl" "$standin_dir/docker" "$standin_dir/date" \
  "$standin_dir/curl"

standin_state="$fixture_dir/state"
mkdir -p "$standin_state/results"
python3 - "$standin_state" <<'PY'
import json
import pathlib
import sys

state = pathlib.Path(sys.argv[1])
def pod(name, image_id):
    return {
        "metadata": {"name": name, "uid": f"uid-{name}"},
        "spec": {
            "nodeName": "fixture-control-plane",
            "containers": [{"name": name, "image": "siglake:kind"}],
        },
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "containerStatuses": [{"name": name, "imageID": image_id}],
        },
    }

(state / "query-pods.json").write_text(json.dumps({
    "items": [pod("siglake-query-0", "sha256:query0"), pod("siglake-query-1", "sha256:query1")]
}), encoding="utf-8")
postgres = pod("postgres-0", "sha256:postgres")
postgres["metadata"]["uid"] = "pod-uid-3f1b"
postgres["spec"]["containers"][0]["image"] = "postgres:16-alpine"
postgres["spec"]["containers"][0]["name"] = "postgres"
postgres["status"]["containerStatuses"][0]["name"] = "postgres"
postgres["status"]["containerStatuses"][0]["containerID"] = "containerd://" + "a" * 64
(state / "postgres-pod.json").write_text(json.dumps(postgres), encoding="utf-8")
PY
paused_tree="$fixture_dir/proc-paused"
make_proc_tree "$paused_tree" \
  1:postgres:T:311 42:postgres:T:742 43:postgres:T:743 44:sh:R:744
# The write probe's stand-in hangs while the processes are stopped, and
# otherwise records the row its insert would have made: the phase carried in
# the statement, dated on the same fixed clock the probe reads its own stamps
# from. The reader stand-in below hands those rows back, so the control's two
# placements come out of the run rather than out of a hand-kept fixture.
cat >"$fixture_dir/psql-standin" <<'STANDIN'
#!/bin/sh
[ -e "$STANDIN_STATE/paused" ] && exec sleep 60
phase=
for arg in "$@"; do
  case "$arg" in
    *"INSERT INTO siglake_outage_write_probe"*)
      phase=${arg##*VALUES (}
      phase=${phase%)}
      phase=${phase#\$phase\$}
      phase=${phase%\$phase\$}
      ;;
  esac
done
[ -n "$phase" ] || exit 0
committed=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
printf '%s\t%s\t%s\n' "$phase" "$committed" "$committed" \
  >>"$STANDIN_STATE/write-probe-rows"
exit 0
STANDIN
chmod +x "$fixture_dir/psql-standin"
printf '#!/bin/sh\ncat "$STANDIN_STATE/write-probe-rows" 2>/dev/null || true\n' \
  >"$fixture_dir/psql-probe-times-standin"
chmod +x "$fixture_dir/psql-probe-times-standin"

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

# One stand-in for both history snippets: the installer feeds its DDL on stdin
# and asks for nothing back, the reader passes `-c`. It answers for the same
# accepted ids, with a terminal transition dated where the commit-time
# stand-in dates the row, and an earlier `pending` insert before the pause.
cat >"$fixture_dir/psql-history-standin" <<'STANDIN'
#!/bin/sh
for arg in "$@"; do
  case "$arg" in
    -f) cat >"$STANDIN_STATE/history-installed"; exit 0 ;;
  esac
done
committed=$(date -u -d "+3 seconds" "+%Y-%m-%d %H:%M:%S.%6N+00")
seq=0
while IFS= read -r job_id; do
  seq=$((seq + 1))
  printf '%s\t%s\tINSERT\t\tpending\t\t%s\t%s\n' \
    "$seq" "$job_id" "2026-09-07 12:00:01.204817+00" "2026-09-07 12:00:01.205901+00"
done <"$STANDIN_STATE/job-ids"
while IFS= read -r job_id; do
  seq=$((seq + 1))
  printf '%s\t%s\tUPDATE\trunning\tsucceeded\t\t%s\t%s\n' \
    "$seq" "$job_id" "$committed" "$committed"
done <"$STANDIN_STATE/job-ids"
STANDIN
chmod +x "$fixture_dir/psql-history-standin"

# Leave two sampling intervals after the midpoint. With a two-second window,
# the first sample can finish just before the midpoint, then `sleep 1` lands at
# the deadline and the loop never takes its scheduled outage write probe.
standin_rc=0
PATH="$standin_dir:$PATH" \
  STANDIN_STATE="$standin_state" \
  PROC_ROOT="$paused_tree" \
  WRITE_PROBE_PSQL="$fixture_dir/psql-standin" \
  WRITE_PROBE_ERRORS="$fixture_dir/standin-psql.err" \
  COMMIT_TIMES_PSQL="$fixture_dir/psql-commit-standin" \
  COMMIT_TIMES_ERRORS="$fixture_dir/standin-commit.err" \
  COMMIT_TIMES_ROWS="$fixture_dir/standin-commit.rows" \
  JOB_HISTORY_PSQL="$fixture_dir/psql-history-standin" \
  JOB_HISTORY_INSTALL_ERRORS="$fixture_dir/standin-history-install.err" \
  JOB_HISTORY_ERRORS="$fixture_dir/standin-history.err" \
  JOB_HISTORY_ROWS="$fixture_dir/standin-history.rows" \
  WRITE_PROBE_TIMES_PSQL="$fixture_dir/psql-probe-times-standin" \
  WRITE_PROBE_TIMES_ERRORS="$fixture_dir/standin-probe-times.err" \
  WRITE_PROBE_TIMES_ROWS="$fixture_dir/standin-probe-times.rows" \
  KUBE_CONTEXT=kind-fixture NAMESPACE=fixture PROM_URL=http://fixture.invalid \
  RESULTS_DIR="$standin_state/results" \
  POSTGRES_OUTAGE_JOBS=2 POSTGRES_OUTAGE_SECONDS=4 \
  POSTGRES_OUTAGE_SAMPLE_INTERVAL_SECONDS=1 \
  POSTGRES_OUTAGE_DRAIN_TIMEOUT_SECONDS=2 \
  POSTGRES_OUTAGE_WRITE_PROBE_SECONDS=1 \
  "$PROBE" >"$fixture_dir/standin.log" 2>&1 || standin_rc=$?
[[ "$standin_rc" -eq 0 ]] ||
  fail "the probe did not produce gradeable evidence against stand-ins (exit $standin_rc): $(<"$fixture_dir/standin.log")"
python3 - "$standin_state/results/postgres-outage-reconnect.json" <<'PY' ||
import datetime as dt
import json, sys
document = json.load(open(sys.argv[1], encoding="utf-8"))
assert document["schema_version"] == 5, document["schema_version"]
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
history = summary["job_write_history"]
assert history["install_status"] == 0, history
assert history["query_status"] == 0, history
assert history["tracked_jobs"] == 2, history
assert sorted(history["terminal_writes"]) == ["job-1", "job-2"], history
assert history["committed_in_pause"] == [], history
assert history["unplaceable_commits"] == [], history
assert history["gaps"] == [], history
assert document["job_write_history"]["rows"][0]["seq"] == 1, document["job_write_history"]
pause_bounds = commits["signal_boundaries"]["pause_transition"]
pause_earliest = dt.datetime.fromisoformat(pause_bounds["earliest"])
pause_latest = dt.datetime.fromisoformat(pause_bounds["latest"])
assert (pause_latest - pause_earliest).total_seconds() < 0.1, pause_bounds
assert summary["peak_backlog_total"] == 2, summary
assert summary["pre_restoration_drain"] is None, summary
assert summary["stopped_postgres_processes"] == 3, summary
assert summary["outage_samples_with_process_state"] >= 1, summary
assert summary["max_observation_lag_seconds"] is not None, summary
assert summary["write_probe_outcomes"]["outage"] == ["blocked"], summary
assert summary["write_probe_outcomes"]["baseline"] == ["completed"], summary
control = summary["write_probe_commit_times"]
assert control["query_status"] == 0, control
assert control["rows_returned"] == 2, control
assert control["placements"] == {
    "baseline": "before_pause",
    "recovery": "after_restoration",
}, control
assert control["contradictions"] == [], control
assert control["gaps"] == [], control
rows = document["write_probe_commit_times"]["rows"]
assert [row["phase"] for row in rows] == ["baseline", "recovery"], rows
assert all(row["committed_at"] for row in rows), rows
target = document["fault_target"]
assert target["node"] == "fixture-control-plane", target
assert target["container_id"] == "a" * 64, target
assert target["container_init_pid"] == 100, target
assert [row["node_pid"] for row in target["processes"]] == [100, 142, 143], target
PY
  fail "the stand-in probe run did not retain the evidence the grader needs: $(<"$fixture_dir/standin.log")"
fixtures=$((fixtures + 1))

signals=$(<"$standin_state/signals")
[[ "$signals" == $'STOP 100\nSTOP 142\nSTOP 143\nCONT 142\nCONT 143\nCONT 100' ]] ||
  fail "the recording path did not stop the postmaster first and continue it last: $signals"
[[ $(<"$standin_state/signal-execs") == $'STOP\nCONT' ]] ||
  fail "the recording path did not use one kind-node exec per process-set signal"
fixtures=$((fixtures + 1))

# An exec that returns success without changing process state is the original
# defect's shape. The post-signal rescan must reject it, then cleanup must still
# CONT every recorded identity.
rm -f "$standin_state"/{backlog-calls,iso-clock,job-ids,history-installed,paused,signal-execs,signals,write-probe-rows,stopped-*}
ineffective_rc=0
PATH="$standin_dir:$PATH" STANDIN_STATE="$standin_state" \
  STANDIN_STOP_MODE=ineffective PROC_ROOT="$paused_tree" \
  WRITE_PROBE_PSQL="$fixture_dir/psql-standin" \
  COMMIT_TIMES_PSQL="$fixture_dir/psql-commit-standin" \
  JOB_HISTORY_PSQL="$fixture_dir/psql-history-standin" \
  KUBE_CONTEXT=kind-fixture NAMESPACE=fixture PROM_URL=http://fixture.invalid \
  RESULTS_DIR="$standin_state/results" POSTGRES_OUTAGE_JOBS=1 \
  POSTGRES_OUTAGE_SECONDS=4 POSTGRES_OUTAGE_SAMPLE_INTERVAL_SECONDS=1 \
  POSTGRES_OUTAGE_DRAIN_TIMEOUT_SECONDS=1 POSTGRES_OUTAGE_WRITE_PROBE_SECONDS=1 \
  "$PROBE" >"$fixture_dir/ineffective.log" 2>&1 || ineffective_rc=$?
[[ "$ineffective_rc" -ne 0 ]] || fail "an ineffective STOP was accepted"
contains "$(<"$fixture_dir/ineffective.log")" \
  'the Postgres process set was not wholly stopped and unchanged' ||
  fail "an ineffective STOP failed for the wrong reason: $(<"$fixture_dir/ineffective.log")"
contains "$(<"$standin_state/signals")" $'CONT 142\nCONT 143\nCONT 100' ||
  fail "ineffective-stop cleanup did not continue the recorded process set"
fixtures=$((fixtures + 1))

# If one backend STOP fails after the postmaster was stopped, the pre-recorded
# process list must let the EXIT trap restore that postmaster and attempt every
# other candidate without restarting the container.
rm -f "$standin_state"/{backlog-calls,iso-clock,job-ids,history-installed,paused,signal-execs,signals,write-probe-rows,stopped-*}
partial_rc=0
PATH="$standin_dir:$PATH" STANDIN_STATE="$standin_state" \
  STANDIN_STOP_MODE=partial PROC_ROOT="$paused_tree" \
  WRITE_PROBE_PSQL="$fixture_dir/psql-standin" \
  COMMIT_TIMES_PSQL="$fixture_dir/psql-commit-standin" \
  JOB_HISTORY_PSQL="$fixture_dir/psql-history-standin" \
  KUBE_CONTEXT=kind-fixture NAMESPACE=fixture PROM_URL=http://fixture.invalid \
  RESULTS_DIR="$standin_state/results" POSTGRES_OUTAGE_JOBS=1 \
  POSTGRES_OUTAGE_SECONDS=4 POSTGRES_OUTAGE_SAMPLE_INTERVAL_SECONDS=1 \
  POSTGRES_OUTAGE_DRAIN_TIMEOUT_SECONDS=1 POSTGRES_OUTAGE_WRITE_PROBE_SECONDS=1 \
  "$PROBE" >"$fixture_dir/partial.log" 2>&1 || partial_rc=$?
[[ "$partial_rc" -ne 0 ]] || fail "a partial STOP failure was accepted"
partial_signals=$(<"$standin_state/signals")
contains "$partial_signals" $'STOP 100\nSTOP 142' ||
  fail "the partial-failure arm did not stop the postmaster before failing: $partial_signals"
contains "$partial_signals" $'CONT 142\nCONT 143\nCONT 100' ||
  fail "partial-failure cleanup did not attempt every recorded process: $partial_signals"
[[ ! -e "$standin_state/stopped-100" ]] ||
  fail "partial-failure cleanup left the postmaster stopped"
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
assert evidence["summary"]["time_to_drain_seconds"] == {
    "lower_bound": 0.0,
    "upper_bound": 11.0,
    "restoration_boundary": "restoration_applied_at",
    "last_positive_scrape_at": "2026-09-07T12:00:50Z",
    "all_pods_zero_by_scrape_at": "2026-09-07T12:01:00Z",
}
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

def history_row(seq):
    return next(row for row in document["job_write_history"]["rows"] if row["seq"] == seq)

def write_probe_row(phase):
    return next(
        row for row in document["write_probe_commit_times"]["rows"] if row["phase"] == phase
    )

def amended_job_row():
    """The amendment path's shape: job-a is recovered, went terminal at
    12:00:51.884113, and `AMEND_RECOVERED_ERROR_SQL` rewrote its error two
    seconds later. `pg_xact_commit_timestamp(xmin)` on the visible row now
    dates the amendment, and no poll of the row version could have caught the
    terminal version in between. Only the insert-only history still carries
    it, as the first terminal transition for the job."""
    pre_restoration_drain()
    recovered_at = "2026-09-07 12:00:51.000000+00"
    visible = commit_row("job-a")
    visible["status"] = "failed"
    visible["recovered_at"] = recovered_at
    visible["committed_at"] = "2026-09-07 12:00:53.402881+00"
    terminal = history_row(5)
    terminal["new_status"] = "failed"
    terminal["recovered_at"] = recovered_at
    document["job_write_history"]["rows"].append({
        "seq": 7,
        "job_id": "job-a",
        "op": "UPDATE",
        "old_status": "failed",
        "new_status": "failed",
        "recovered_at": recovered_at,
        "observed_at": "2026-09-07 12:00:53.301660+00",
        "committed_at": "2026-09-07 12:00:53.402881+00",
    })

def pre_restoration_drain():
    """Run #76's shape: the backlog empties and completions rise while the
    samples are still labelled `outage`."""
    sample = document["samples"][2]
    for row in sample["backlog"]:
        row["value"] = 0
    for row in sample["completions"]:
        row["value"] += 1

def zero_before_positive():
    """Run #126's shape: an early zero/completion observation precedes the
    positive backlog episode whose later recovery drain is measured."""
    for row in document["samples"][1]["backlog"]:
        row["value"] = 0
    for sample in document["samples"][1:]:
        for row in sample["completions"]:
            row["value"] += 1

def subsecond_boundaries():
    document["timestamps"].update({
        "outage_started_at": "2026-09-07T12:00:03.100Z",
        "pause_applied_at": "2026-09-07T12:00:03.200Z",
        "restoration_started_at": "2026-09-07T12:00:48.100Z",
        "restoration_applied_at": "2026-09-07T12:00:48.200Z",
        "postgres_ready_at": "2026-09-07T12:00:48.300Z",
    })

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
elif mutation == "duplicate-scrape-generations":
    duplicate = json.loads(json.dumps(document["samples"][3]))
    duplicate["at"] = "2026-09-07T12:00:57Z"
    document["samples"].insert(4, duplicate)
elif mutation == "staggered-pod-scrapes":
    document["samples"][3]["backlog"][0]["sample_time"] = 1788782451.0
    document["samples"][3]["backlog"][1]["sample_time"] = 1788782449.0
    document["samples"][4]["backlog"][0]["sample_time"] = 1788782458.0
    document["samples"][4]["backlog"][1]["sample_time"] = 1788782459.0
elif mutation == "missing-drain-pod":
    document["samples"][4]["backlog"].pop()
elif mutation == "missing-revision":
    del document["revisions"]["repository_commit"]
elif mutation == "pre-restoration-drain":
    pre_restoration_drain()
elif mutation == "zero-before-positive":
    zero_before_positive()
    # Run #126's retained generations place the last positive scrape about
    # five seconds after restoration_applied_at and every pod at zero by
    # twenty seconds after it. Preserve that relationship in fixed UTC time.
    document["timestamps"]["restoration_applied_at"] = "2026-09-07T12:00:49.000Z"
    document["samples"][3]["backlog"][0]["sample_time"] = 1788782454.001
    for row in document["samples"][4]["backlog"]:
        row["sample_time"] = 1788782469.0
    document["samples"][4]["at"] = "2026-09-07T12:01:10Z"
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
elif mutation == "subsecond-clear-placements":
    subsecond_boundaries()
    commit_row("job-a")["committed_at"] = "2026-09-07 12:00:03.050000+00"
    commit_row("job-b")["committed_at"] = "2026-09-07 12:00:20.551200+00"
    document["job_commit_times"]["rows"].append({
        "job_id": "job-after-restoration",
        "status": "succeeded",
        "submitted_at": "2026-09-07 11:59:00+00",
        "ended_at": "2026-09-07 12:00:48.300000+00",
        "recovered_at": None,
        "committed_at": "2026-09-07 12:00:48.300000+00",
    })
elif mutation == "subsecond-pause-transition":
    subsecond_boundaries()
    commit_row("job-b")["committed_at"] = "2026-09-07 12:00:03.150000+00"
elif mutation == "subsecond-restoration-transition":
    subsecond_boundaries()
    commit_row("job-b")["committed_at"] = "2026-09-07 12:00:48.150000+00"
elif mutation == "amended-job-row":
    amended_job_row()
elif mutation == "amended-job-row-without-history":
    amended_job_row()
    del document["job_write_history"]
elif mutation == "amended-job-row-before-history":
    # A trace retained before the history existed. It has to grade exactly as
    # it did then -- a recovered row is a gap -- and it must not be asked for
    # a reading its own version never promised.
    amended_job_row()
    del document["job_write_history"]
    document["schema_version"] = 3
elif mutation == "amended-job-row-undated-history":
    amended_job_row()
    history_row(5)["committed_at"] = None
elif mutation == "history-terminal-write-in-pause":
    # The visible row version says the terminal write landed after
    # restoration. The history says the transition it amended did not.
    amended_job_row()
    history_row(5)["committed_at"] = "2026-09-07 12:00:20.551200+00"
elif mutation == "history-not-installed":
    document["job_write_history"]["install_status"] = 3
    document["job_write_history"]["rows"] = []
    document["job_write_history"]["install_detail"] = (
        "ERROR: relation siglake_query_jobs does not exist"
    )
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
elif mutation == "write-probe-row-in-pause":
    # The control on the whole dating scheme: this write was taken before the
    # pause was applied, and Postgres dates it inside the stopped window. One
    # of the two clocks is wrong, and every job-row commit in the trace was
    # placed with them.
    write_probe_row("baseline")["committed_at"] = "2026-09-07 12:00:20.551200+00"
elif mutation == "recovery-write-probe-row-before-pause":
    write_probe_row("recovery")["committed_at"] = "2026-09-07 12:00:00.902100+00"
elif mutation == "write-probe-row-at-restoration-edge":
    write_probe_row("recovery")["committed_at"] = "2026-09-07 12:00:48.114000+00"
elif mutation == "write-probe-row-missing":
    document["write_probe_commit_times"]["rows"] = [
        row for row in document["write_probe_commit_times"]["rows"]
        if row["phase"] != "baseline"
    ]
elif mutation == "outage-write-probe-row":
    # The probe reported that write killed by its watchdog, so a row for it
    # says the write landed anyway.
    document["write_probe_commit_times"]["rows"].append({
        "phase": "outage",
        "observed_at": "2026-09-07 12:00:20.400000+00",
        "committed_at": "2026-09-07 12:00:20.551200+00",
    })
elif mutation == "write-probe-times-query-failed":
    document["write_probe_commit_times"]["query_status"] = 3
    document["write_probe_commit_times"]["rows"] = []
    document["write_probe_commit_times"]["detail"] = (
        "ERROR: relation siglake_outage_write_probe does not exist"
    )
elif mutation == "missing-write-probe-times":
    del document["write_probe_commit_times"]
elif mutation == "write-probe-times-before-schema":
    # A trace retained before the write probe's rows were dated. It carries no
    # such reading and must not be asked for one.
    del document["write_probe_commit_times"]
    document["schema_version"] = 4
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
assert evidence["summary"]["time_to_drain_seconds"] is not None, evidence["summary"]
problems = "\n".join(evidence["problems"])
assert "the pause window is unexplained" not in problems, problems
PY
  fixtures=$((fixtures + 1))
}

# One subsecond trace exercises all three placements that are separated from
# the transition bounds. The inside commit makes the grade red, while the
# summary still has to put the other two on the correct sides of the pause.
subsecond_placements_input="$fixture_dir/subsecond-clear-placements.input.json"
subsecond_placements_output="$fixture_dir/subsecond-clear-placements.output.json"
mutate subsecond-clear-placements "$subsecond_placements_input"
subsecond_placements_rc=0
python3 "$GRADER" "$subsecond_placements_input" \
  --output "$subsecond_placements_output" 2>"$fixture_dir/subsecond-clear-placements.log" ||
  subsecond_placements_rc=$?
[[ "$subsecond_placements_rc" -eq 1 ]] ||
  fail "subsecond clear placements exited $subsecond_placements_rc, expected 1"
python3 - "$subsecond_placements_output" <<'PY' ||
import json, sys
commits = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]["summary"]["job_commit_times"]
assert commits["committed_before_pause"] == 1, commits
assert commits["committed_in_pause"] == ["job-b"], commits
assert commits["committed_after_restoration"] == 1, commits
assert commits["unplaceable_commits"] == [], commits
assert commits["signal_boundaries"] == {
    "pause_transition": {
        "earliest": "2026-09-07T12:00:03.100000+00:00",
        "latest": "2026-09-07T12:00:03.201000+00:00",
    },
    "proven_stopped": {
        "earliest": "2026-09-07T12:00:03.201000+00:00",
        "latest": "2026-09-07T12:00:48.100000+00:00",
    },
    "restoration_transition": {
        "earliest": "2026-09-07T12:00:48.100000+00:00",
        "latest": "2026-09-07T12:00:48.201000+00:00",
    },
}, commits
PY
  fail "subsecond commits were not placed against the proven signal bounds"
fixtures=$((fixtures + 1))

expect_interval() {
  local mutation=$1 lower=$2 upper=$3
  local input="$fixture_dir/${mutation}.input.json"
  local output="$fixture_dir/${mutation}.output.json"
  mutate "$mutation" "$input"
  python3 "$GRADER" "$input" --output "$output" 2>"$fixture_dir/${mutation}.log" ||
    fail "$mutation did not retain a recovery drain interval: $(cat "$fixture_dir/${mutation}.log")"
  python3 - "$output" "$lower" "$upper" <<'PY' ||
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
interval = evidence["summary"]["time_to_drain_seconds"]
assert interval["lower_bound"] == float(sys.argv[2]), interval
assert interval["upper_bound"] == float(sys.argv[3]), interval
assert interval["restoration_boundary"] == "restoration_applied_at", interval
PY
    fail "$mutation reported the wrong recovery drain interval"
  fixtures=$((fixtures + 1))
}

expect_no_interval() {
  local mutation=$1 want=$2
  local input="$fixture_dir/${mutation}.input.json"
  local output="$fixture_dir/${mutation}.output.json"
  local rc=0
  mutate "$mutation" "$input"
  python3 "$GRADER" "$input" --output "$output" 2>"$fixture_dir/${mutation}.log" || rc=$?
  [[ "$rc" -eq 1 ]] || fail "$mutation exited $rc, expected the unverified exit 1"
  python3 - "$output" "$want" <<'PY' ||
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["summary"]["time_to_drain_seconds"] is None, evidence["summary"]
assert sys.argv[2] in "\n".join(evidence["problems"]), evidence["problems"]
PY
    fail "$mutation forced a drain interval from insufficient observations"
  fixtures=$((fixtures + 1))
}

expect_unverified missing-series 'no usable backlog observation'
expect_unverified no-rise 'no observed unreconciled backlog'
expect_no_interval no-drain 'did not drain after restoration'
expect_no_interval missing-drain-pod 'backlog sample at 2026-09-07T12:01:02+00:00 missed pods'
expect_interval duplicate-scrape-generations 0 11
expect_interval staggered-pod-scrapes 1 10
expect_unverified missing-revision 'missing pinned repository revision'
expect_resolved pre-restoration-drain
expect_resolved zero-before-positive
expect_interval zero-before-positive 5 20
expect_unverified pre-restoration-drain-undated 'shows zero backlog with completions up by 2 before restoration'
python3 - "$fixture_dir/pre-restoration-drain-undated.output.json" <<'PY' ||
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["grade"] == "unverified", evidence
assert evidence["summary"]["time_to_drain_seconds"] is not None, evidence["summary"]
PY
  fail "an unresolved pause finding suppressed the independent recovery interval"
expect_unverified pre-restoration-drain-undated 'no job row for accepted job job-b'
expect_unverified commit-inside-pause 'job job-b committed at 2026-09-07T12:00:20.551200+00:00, inside the proven stopped window'
expect_unverified commit-inside-pause 'shows zero backlog with completions up by 2 before restoration'
expect_unverified commit-at-pause-edge 'overlapping the pause transition bounded by 2026-09-07T12:00:03+00:00 (1s precision) and 2026-09-07T12:00:05+00:00 (1s precision)'
expect_unverified commit-at-restoration-edge 'overlapping the restoration transition bounded by 2026-09-07T12:00:48+00:00 (1s precision) and 2026-09-07T12:00:50+00:00 (1s precision)'
expect_unverified subsecond-pause-transition 'overlapping the pause transition bounded by 2026-09-07T12:00:03.100000+00:00 (1ms precision) and 2026-09-07T12:00:03.201000+00:00 (1ms precision)'
expect_unverified subsecond-restoration-transition 'overlapping the restoration transition bounded by 2026-09-07T12:00:48.100000+00:00 (1ms precision) and 2026-09-07T12:00:48.201000+00:00 (1ms precision)'
# The amended row is the case the write history exists for. Its visible
# version is dated by the amendment, so the row version alone leaves the drain
# unexplained; the retained history still says when the terminal write
# committed, and that is what resolves it.
expect_resolved amended-job-row
python3 - "$fixture_dir/amended-job-row.output.json" <<'PY' ||
import json, sys
summary = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]["summary"]
commits = summary["job_commit_times"]
assert commits["gaps"] == [], commits
assert commits["terminal_writes_dated_by_history"] == 1, commits
history = summary["job_write_history"]
terminal = history["terminal_writes"]["job-a"]
assert terminal["seq"] == 5, terminal
assert terminal["status"] == "failed", terminal
assert terminal["recorded_committed_at"] == "2026-09-07 12:00:51.884113+00", terminal
assert history["amendments"] == {"job-a": 1}, history
assert history["committed_in_pause"] == [], history
assert history["unplaceable_commits"] == [], history
PY
  fail "the write history did not date job-a's terminal write ahead of its amendment"
fixtures=$((fixtures + 1))
# Without the history, or with a terminal transaction Postgres cannot date, the
# same trace is back to the visible row version and stays a gap.
expect_unverified amended-job-row-without-history 'may be an amendment of an earlier terminal write'
expect_unverified amended-job-row-without-history 'the pause window is unexplained'
expect_unverified amended-job-row-without-history 'missing job-row write-history observations'
expect_unverified amended-job-row-before-history 'may be an amendment of an earlier terminal write'
python3 - "$fixture_dir/amended-job-row-before-history.output.json" <<'PY' ||
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
problems = "\n".join(evidence["problems"])
assert "write-history" not in problems, problems
assert "write history" not in problems, problems
assert evidence["summary"]["job_write_history"] is None, evidence["summary"]
PY
  fail "a pre-history trace was held to a reading its own version never carried"
fixtures=$((fixtures + 1))
expect_unverified amended-job-row-undated-history 'its retained terminal transition has no usable commit timestamp'
expect_unverified amended-job-row-undated-history 'the pause window is unexplained'
expect_unverified history-terminal-write-in-pause 'inside the proven stopped window'
expect_unverified history-terminal-write-in-pause 'so the pause did not block writes'
expect_unverified history-not-installed 'the job-row write history is incomplete'
expect_unverified null-commit-time 'job job-b has no usable commit timestamp'
expect_unverified nonterminal-job-row "job job-b is 'running' after the recovery window"
expect_unverified uncorrelated-job-row 'no job row for accepted job job-b'
expect_unverified stray-in-pause-commit 'job job-from-an-earlier-round committed at 2026-09-07T12:00:21.330000+00:00, inside the proven stopped window'
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

# The write probe's own rows are the control on the dating scheme: the probe
# took the baseline write before the pause and the recovery write after
# restoration, so Postgres placing either on the wrong side says the two clocks
# disagree, whatever the job rows look like. The in-pause baseline row is the
# case that must stand on its own -- an otherwise clean trace, one problem.
expect_unverified write-probe-row-in-pause \
  "the baseline write probe's row committed at 2026-09-07T12:00:20.551200+00:00, inside the proven stopped window"
python3 - "$fixture_dir/write-probe-row-in-pause.output.json" <<'PY' ||
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["problems"] == [
    "the baseline write probe's row committed at 2026-09-07T12:00:20.551200+00:00, inside the "
    "proven stopped window from 2026-09-07T12:00:05+00:00 through 2026-09-07T12:00:48+00:00, "
    "although the probe took that write before the pause was applied: Postgres's commit clock "
    "and the probe's signal stamps disagree, so no commit timestamp in this trace can be placed "
    "against the pause"
], evidence["problems"]
control = evidence["summary"]["write_probe_commit_times"]
assert control["contradictions"] == ["baseline"], control
assert control["gaps"] == [], control
assert evidence["summary"]["job_commit_times"]["settles_pause"] is True, evidence["summary"]
PY
  fail "an in-pause baseline write-probe row was not the only reason the trace failed"
fixtures=$((fixtures + 1))
expect_unverified recovery-write-probe-row-before-pause \
  "the recovery write probe's row committed at 2026-09-07T12:00:00.902100+00:00, before the pause was applied, although the probe took that write after restoration was applied"
expect_unverified write-probe-row-at-restoration-edge \
  'overlapping the restoration transition bounded by 2026-09-07T12:00:48+00:00 (1s precision) and 2026-09-07T12:00:50+00:00 (1s precision), so the one write whose side of the pause the probe knows cannot be placed on it'
expect_unverified write-probe-row-missing \
  "the baseline write probe completed but left no row in the probe's control table to date"
expect_unverified outage-write-probe-row \
  "the bounded write taken during the pause left a row in the probe's control table, committed at 2026-09-07T12:00:20.551200+00:00, so the pause did not block writes"
expect_unverified write-probe-times-query-failed \
  'the write-probe commit-time query did not run'
expect_unverified missing-write-probe-times 'missing write-probe commit-time observations'

# A trace from before the reading existed grades exactly as it did then.
before_schema="$fixture_dir/write-probe-times-before-schema.input.json"
mutate write-probe-times-before-schema "$before_schema"
python3 "$GRADER" "$before_schema" --output "$fixture_dir/write-probe-times-before-schema.output.json" \
  2>"$fixture_dir/write-probe-times-before-schema.log" ||
  fail "a pre-control trace did not pass: $(cat "$fixture_dir/write-probe-times-before-schema.log")"
python3 - "$fixture_dir/write-probe-times-before-schema.output.json" <<'PY' ||
import json, sys
evidence = json.load(open(sys.argv[1], encoding="utf-8"))["evidence"]
assert evidence["grade"] == "verified", evidence["problems"]
assert evidence["summary"]["write_probe_commit_times"] is None, evidence["summary"]
PY
  fail "a pre-control trace was held to a reading its own version never carried"
fixtures=$((fixtures + 1))

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
