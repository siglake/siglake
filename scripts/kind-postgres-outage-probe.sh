#!/usr/bin/env bash
# Bounded, opt-in Postgres pause/reconnect probe for a throwaway kind round.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KUBE_CONTEXT=${KUBE_CONTEXT:?set KUBE_CONTEXT}
NAMESPACE=${NAMESPACE:-default}
PROM_URL=${PROM_URL:?set PROM_URL}
RESULTS_DIR=${RESULTS_DIR:-$ROOT/results}
REQUESTED_JOBS=${POSTGRES_OUTAGE_JOBS:-8}
OUTAGE_SECONDS=${POSTGRES_OUTAGE_SECONDS:-60}
DRAIN_TIMEOUT_SECONDS=${POSTGRES_OUTAGE_DRAIN_TIMEOUT_SECONDS:-120}
SAMPLE_INTERVAL_SECONDS=${POSTGRES_OUTAGE_SAMPLE_INTERVAL_SECONDS:-5}
QUERY=${POSTGRES_OUTAGE_QUERY:-"SELECT sum(length(a.raw) + length(b.raw)) AS n FROM events a CROSS JOIN events b"}
QUERY_TIMEOUT_SECONDS=${POSTGRES_OUTAGE_QUERY_TIMEOUT_SECONDS:-5}
WRITE_PROBE_SECONDS=${POSTGRES_OUTAGE_WRITE_PROBE_SECONDS:-5}
EVIDENCE_JSON="$RESULTS_DIR/postgres-outage-reconnect.json"

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/siglake-postgres-outage.XXXXXX")
SAMPLES_FILE="$TMP_DIR/samples.jsonl"
WRITE_PROBES_FILE="$TMP_DIR/write-probes.jsonl"
SUBMISSIONS_DIR="$TMP_DIR/submissions"
POSTGRES_PAUSED=0
POSTGRES_POD=
POSTGRES_POD_UID=
POSTGRES_NODE=
POSTGRES_CONTAINER_ID=
POSTGRES_CONTAINER_PID=
POSTGRES_PID_NAMESPACE=
POSTGRES_PROCESSES_FILE="$TMP_DIR/postgres-processes.tsv"

log() { printf '==> postgres-outage: %s\n' "$*" >&2; }
die() { printf 'ERROR: postgres-outage: %s\n' "$*" >&2; exit 1; }
iso_now() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }

restore_postgres() {
  [[ "$POSTGRES_PAUSED" -eq 1 ]] || return 0
  log "restore Postgres after interrupted probe"
  continue_postgres_processes >/dev/null 2>&1 || true
  POSTGRES_PAUSED=0
}

# `docker exec` enters the kind node's PID namespace, which is an ancestor of
# the Postgres container's private namespace. The snippets below never enter
# the workload container. Every process is tied to the CRI-reported init PID by
# PID-namespace inode, exact comm, start time and container cgroup before a
# signal is delivered.
# node-process-list-snippet-begin
POSTGRES_NODE_PROCESS_LIST_SNIPPET='
init_pid=$1
container_id=$2
[ "$(cat "/proc/$init_pid/comm" 2>/dev/null || true)" = postgres ] || {
  echo "CRI init PID $init_pid is not postgres" >&2
  exit 1
}
grep -Fq -- "$container_id" "/proc/$init_pid/cgroup" || {
  echo "CRI init PID $init_pid is outside container $container_id" >&2
  exit 1
}
pid_namespace=$(readlink "/proc/$init_pid/ns/pid")
for comm_path in /proc/[0-9]*/comm; do
  pid=${comm_path#/proc/}
  pid=${pid%/comm}
  [ "$(cat "$comm_path" 2>/dev/null || true)" = postgres ] || continue
  [ "$(readlink "/proc/$pid/ns/pid" 2>/dev/null || true)" = "$pid_namespace" ] || continue
  line=$(cat "/proc/$pid/stat" 2>/dev/null || true)
  [ -n "$line" ] || continue
  rest=${line##*") "}
  state=${rest%% *}
  starttime=$(printf %s "$rest" | cut -d" " -f20)
  printf "%s\t%s\t%s\t%s\n" "$pid" "$state" "$starttime" "$pid_namespace"
done
'
# node-process-list-snippet-end

# node-signal-snippet-begin
POSTGRES_NODE_SIGNAL_SNIPPET='
signal=$1
init_pid=$2
container_id=$3
identities=$4
tab=$(printf "\t")

signal_one() {
  pid=$1
  starttime=$2
  pid_namespace=$3
  [ "$(cat "/proc/$pid/comm" 2>/dev/null || true)" = postgres ] || {
    echo "PID $pid is no longer postgres" >&2
    return 1
  }
  [ "$(readlink "/proc/$pid/ns/pid" 2>/dev/null || true)" = "$pid_namespace" ] || {
    echo "PID $pid changed PID namespace" >&2
    return 1
  }
  grep -Fq -- "$container_id" "/proc/$pid/cgroup" || {
    echo "PID $pid is outside container $container_id" >&2
    return 1
  }
  line=$(cat "/proc/$pid/stat")
  rest=${line##*") "}
  [ "$(printf %s "$rest" | cut -d" " -f20)" = "$starttime" ] || {
    echo "PID $pid was replaced" >&2
    return 1
  }
  kill "-$signal" "$pid"
}

started_at=
status=0
if [ "$signal" = STOP ]; then
  first=1
  while IFS="$tab" read -r pid _ starttime pid_namespace; do
    [ -n "$pid" ] || continue
    if [ "$first" -eq 1 ]; then
      [ "$pid" = "$init_pid" ] || {
        echo "the Postgres postmaster is not first in the signal set" >&2
        exit 1
      }
      started_at=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
      first=0
    fi
    # Stopping the postmaster first prevents it from forking another backend
    # while the remaining recorded identities are checked and stopped.
    signal_one "$pid" "$starttime" "$pid_namespace" || exit $?
  done <<EOF
$identities
EOF
else
  # Resume every child before the postmaster. Accumulate identity failures so
  # cleanup still attempts every process selected by the pause.
  started_at=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
  while IFS="$tab" read -r pid _ starttime pid_namespace; do
    [ -n "$pid" ] || continue
    [ "$pid" = "$init_pid" ] && continue
    signal_one "$pid" "$starttime" "$pid_namespace" || status=1
  done <<EOF
$identities
EOF
  while IFS="$tab" read -r pid _ starttime pid_namespace; do
    [ "$pid" = "$init_pid" ] || continue
    signal_one "$pid" "$starttime" "$pid_namespace" || status=1
  done <<EOF
$identities
EOF
  [ "$status" -eq 0 ] || exit "$status"
fi

attempts=0
while [ "$attempts" -lt 50 ]; do
  ready=1
  while IFS="$tab" read -r pid _ _ _; do
    [ -n "$pid" ] || continue
    line=$(cat "/proc/$pid/stat" 2>/dev/null || true)
    if [ -z "$line" ]; then
      [ "$signal" = CONT ] || ready=0
      continue
    fi
    rest=${line##*") "}
    state=${rest%% *}
    if { [ "$signal" = STOP ] && [ "$state" != T ]; } ||
      { [ "$signal" = CONT ] && [ "$state" = T ]; }; then
      ready=0
    fi
  done <<EOF
$identities
EOF
  if [ "$ready" -eq 1 ]; then
    printf "started_at\t%s\napplied_at\t%s\n" \
      "$started_at" "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
    exit 0
  fi
  attempts=$((attempts + 1))
  sleep 0.02
done
echo "the Postgres process set did not reach the state required by $signal" >&2
exit 1
'
# node-signal-snippet-end

node_processes() {
  docker exec "$POSTGRES_NODE" sh -eu -c "$POSTGRES_NODE_PROCESS_LIST_SNIPPET" \
    node-process-list "$POSTGRES_CONTAINER_PID" "$POSTGRES_CONTAINER_ID"
}

signal_postgres_processes() {
  local signal=$1 identities
  identities=$(<"$POSTGRES_PROCESSES_FILE")
  docker exec "$POSTGRES_NODE" sh -eu -c "$POSTGRES_NODE_SIGNAL_SNIPPET" \
    node-signal-group "$signal" "$POSTGRES_CONTAINER_PID" \
    "$POSTGRES_CONTAINER_ID" "$identities"
}

# Freeze the postmaster first so it cannot fork a new backend while the exact
# established process set is stopped. Record every selected identity before
# the first signal; trap cleanup can therefore repair a partial sequence.
pause_postgres_processes() {
  local current="$TMP_DIR/postgres-processes.current" verified="$TMP_DIR/postgres-processes.verified"
  local pid state starttime pid_namespace backend_count=0
  node_processes >"$current"
  awk -F '\t' -v init="$POSTGRES_CONTAINER_PID" '$1 == init { print; found=1 } END { exit !found }' \
    "$current" >"$POSTGRES_PROCESSES_FILE" ||
    die "the CRI init PID was absent from the verified Postgres process set"
  while IFS=$'\t' read -r pid state starttime pid_namespace; do
    [[ "$pid" == "$POSTGRES_CONTAINER_PID" ]] && continue
    printf '%s\t%s\t%s\t%s\n' "$pid" "$state" "$starttime" "$pid_namespace" \
      >>"$POSTGRES_PROCESSES_FILE"
    backend_count=$((backend_count + 1))
  done <"$current"
  ((backend_count > 0)) || die "no established Postgres backend process found"
  POSTGRES_PID_NAMESPACE=$(awk -F '\t' 'NR == 1 { print $4 }' "$POSTGRES_PROCESSES_FILE")

  signal_postgres_processes STOP

  node_processes >"$verified"
  awk -F '\t' '$2 == "T" { print $1 "\t" $3 "\t" $4 }' "$verified" | sort -n >"$verified.ids"
  awk -F '\t' '{ print $1 "\t" $3 "\t" $4 }' "$POSTGRES_PROCESSES_FILE" | sort -n \
    >"$current.ids"
  cmp -s "$current.ids" "$verified.ids" ||
    die "the Postgres process set was not wholly stopped and unchanged"
}

# Continue children before the postmaster. Each attempt repeats every identity
# check; a replaced or foreign PID is never signalled. Failures are accumulated
# so one stale process cannot prevent restoration of the remaining set.
continue_postgres_processes() {
  [[ -s "$POSTGRES_PROCESSES_FILE" ]] || return 0
  signal_postgres_processes CONT
}

# Read the exact process set the pause selected, one `pid<TAB>state<TAB>starttime`
# line per postgres process. `state` is field 3 of /proc/<pid>/stat and is `T`
# while a process is stopped; `starttime` is field 22 and changes only when the
# process is replaced, so a restart cannot hide behind a matching pid. Held in a
# variable, and reading ${PROC_ROOT:-/proc}, so the offline fixtures can run this
# exact text against a synthetic process tree.
# state-snippet-begin
POSTGRES_STATE_SNIPPET='
proc=${PROC_ROOT:-/proc}
[ "$(cat "$proc/1/comm" 2>/dev/null || true)" = postgres ] || {
  echo "PID 1 is not postgres; refusing to report container state" >&2
  exit 1
}
for comm_path in "$proc"/[0-9]*/comm; do
  pid=${comm_path#"$proc"/}
  pid=${pid%/comm}
  [ "$(cat "$comm_path" 2>/dev/null || true)" = postgres ] || continue
  line=$(cat "$proc/$pid/stat" 2>/dev/null || true)
  [ -n "$line" ] || continue
  rest=${line##*") "}
  state=${rest%% *}
  starttime=$(printf %s "$rest" | cut -d" " -f20)
  printf "%s\t%s\t%s\n" "$pid" "$state" "$starttime"
done
'
# state-snippet-end

# One bounded write against the paused database, run three times: before the
# pause, inside it, and after restoration. The postmaster is stopped, so the
# connection sits in the listen backlog and psql never returns; the watchdog
# kills it and `blocked` is that timeout, distinguished from a write that
# actually completed. Emits a single `key=value` TSV line.
#
# The row carries the phase that wrote it, because this is the one write whose
# intended commit time the probe already knows: a baseline row Postgres dates
# inside the proven stopped window is the dating scheme contradicting itself,
# which no job-row reading can show. `$2` is the phase as a ready-made SQL
# literal, built by the caller: this snippet is a single-quoted bash string, so
# a single quote cannot appear in it, and a dollar-quote tag written here would
# be expanded by the `sh -eu -c` that runs it. Substituting the caller's value
# does not re-expand it. The table is created by the baseline write, so a
# cluster carrying one from an earlier probe version fails the insert loudly
# rather than recording a row with no phase.
# write-probe-snippet-begin
POSTGRES_WRITE_PROBE_SNIPPET='
timeout_seconds=$1
phase_literal=$2
errors=${WRITE_PROBE_ERRORS:-/tmp/siglake-outage-write-probe.err}
: >"$errors"
started=$(date +%s)
${WRITE_PROBE_PSQL:-psql} -qtAX -v ON_ERROR_STOP=1 \
  -U "${PGUSER:-${POSTGRES_USER:-postgres}}" \
  -d "${PGDATABASE:-${POSTGRES_DB:-postgres}}" \
  -c "CREATE TABLE IF NOT EXISTS siglake_outage_write_probe (phase text NOT NULL, observed_at timestamptz NOT NULL DEFAULT now())" \
  -c "INSERT INTO siglake_outage_write_probe (phase) VALUES ($phase_literal)" \
  >/dev/null 2>"$errors" &
probe_pid=$!
( sleep "$timeout_seconds"; kill -KILL "$probe_pid" 2>/dev/null || true ) >/dev/null 2>&1 &
watchdog_pid=$!
status=0
wait "$probe_pid" || status=$?
kill "$watchdog_pid" 2>/dev/null || true
elapsed=$(( $(date +%s) - started ))
if [ "$status" -eq 0 ]; then
  outcome=completed
elif [ "$status" -ge 128 ] && [ "$((elapsed + 1))" -ge "$timeout_seconds" ]; then
  outcome=blocked
else
  outcome=error
fi
printf "outcome=%s\tstatus=%s\tseconds=%s\tdetail=%s\n" "$outcome" "$status" \
  "$elapsed" "$(tr "\n\t" "  " <"$errors" | cut -c1-200)"
'
# write-probe-snippet-end

# Date each job row's visible version by its commit timestamp, which is the one
# reading that says whether a write landed while Postgres was stopped:
# `ended_at` is application-supplied and `submitted_at` predates the fault. The
# effective `track_commit_timestamp` comes back in the same output, because
# `pg_xact_commit_timestamp` raises rather than returning NULL when the setting
# is off, and a failed query must not read as "no rows committed in the pause".
# No SQL string literal and no single quote appears here, so the offline
# fixtures can run this exact text through a psql stand-in.
# commit-times-snippet-begin
POSTGRES_COMMIT_TIMES_SNIPPET='
errors=${COMMIT_TIMES_ERRORS:-/tmp/siglake-outage-commit-times.err}
rows=${COMMIT_TIMES_ROWS:-/tmp/siglake-outage-commit-times.rows}
: >"$errors"
: >"$rows"
tab=$(printf "\t")
psql_bin=${COMMIT_TIMES_PSQL:-psql}
user=${PGUSER:-${POSTGRES_USER:-postgres}}
database=${PGDATABASE:-${POSTGRES_DB:-postgres}}
setting_status=0
setting=$("$psql_bin" -qtAX -v ON_ERROR_STOP=1 -U "$user" -d "$database" \
  -c "SHOW track_commit_timestamp" 2>>"$errors") || setting_status=$?
query_status=0
"$psql_bin" -qtAX -F"$tab" -v ON_ERROR_STOP=1 -U "$user" -d "$database" \
  -c "SELECT job_id, status, submitted_at, ended_at, recovered_at, pg_xact_commit_timestamp(xmin) FROM siglake_query_jobs ORDER BY 6" \
  >"$rows" 2>>"$errors" || query_status=$?
printf "status\t%s\t%s\n" "$setting_status" "$query_status"
printf "setting\t%s\n" "$setting"
while IFS= read -r line; do
  printf "row\t%s\n" "$line"
done <"$rows"
printf "detail\t%s\n" "$(tr "\n\t" "  " <"$errors" | cut -c1-200)"
'
# commit-times-snippet-end

# An insert-only record of every job-row write, installed on the throwaway kind
# Postgres before the burst is submitted. The trigger inserts from inside the
# transaction that wrote the job row, so the history row's own `xmin` is that
# transaction and `pg_xact_commit_timestamp` over the history dates the
# transition itself. `AMEND_RECOVERED_ERROR_SQL`
# (crates/siglake-query-server/src/jobs.rs:2313) rewrites an already-terminal
# recovered row, which is what leaves the visible row version postdating the
# terminal write it replaced; that amendment is a second history row here,
# never an edit of the first. Polling the visible row instead would miss a
# terminal version amended between two polls.
#
# The DDL travels in a quoted heredoc: this snippet is a single-quoted bash
# string executed by `sh -eu -c`, which would expand the `$fn$` and `$op$`
# dollar-quote tags to its own PID. No single quote appears here either, for
# the same reason the reader above has none.
# job-history-install-snippet-begin
POSTGRES_JOB_HISTORY_INSTALL_SNIPPET='
errors=${JOB_HISTORY_INSTALL_ERRORS:-/tmp/siglake-outage-job-history-install.err}
: >"$errors"
psql_bin=${JOB_HISTORY_PSQL:-psql}
user=${PGUSER:-${POSTGRES_USER:-postgres}}
database=${PGDATABASE:-${POSTGRES_DB:-postgres}}
install_status=0
"$psql_bin" -qtAX -v ON_ERROR_STOP=1 -U "$user" -d "$database" -f - \
  >/dev/null 2>>"$errors" <<"SQL" || install_status=$?
CREATE TABLE IF NOT EXISTS siglake_outage_job_history (
    seq          bigserial PRIMARY KEY,
    job_id       text NOT NULL,
    op           text NOT NULL,
    old_status   text,
    new_status   text,
    recovered_at timestamptz,
    observed_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE OR REPLACE FUNCTION siglake_outage_record_job_write() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
    INSERT INTO siglake_outage_job_history
        (job_id, op, old_status, new_status, recovered_at)
    VALUES (
        NEW.job_id,
        TG_OP,
        CASE WHEN TG_OP = $op$INSERT$op$ THEN NULL ELSE OLD.status END,
        NEW.status,
        NEW.recovered_at
    );
    RETURN NULL;
END;
$fn$;
DROP TRIGGER IF EXISTS siglake_outage_job_history_trigger ON siglake_query_jobs;
CREATE TRIGGER siglake_outage_job_history_trigger
    AFTER INSERT OR UPDATE ON siglake_query_jobs
    FOR EACH ROW EXECUTE FUNCTION siglake_outage_record_job_write();
SQL
printf "install\t%s\n" "$install_status"
printf "detail\t%s\n" "$(tr "\n\t" "  " <"$errors" | cut -c1-200)"
'
# job-history-install-snippet-end

# Read the history back with each row dated by the transaction that wrote it.
# Ordered by `seq`, which is the insert order and therefore the transition
# order, so the first terminal row is the terminal write and anything after it
# for the same job is an amendment.
# job-history-snippet-begin
POSTGRES_JOB_HISTORY_SNIPPET='
errors=${JOB_HISTORY_ERRORS:-/tmp/siglake-outage-job-history.err}
rows=${JOB_HISTORY_ROWS:-/tmp/siglake-outage-job-history.rows}
: >"$errors"
: >"$rows"
tab=$(printf "\t")
psql_bin=${JOB_HISTORY_PSQL:-psql}
user=${PGUSER:-${POSTGRES_USER:-postgres}}
database=${PGDATABASE:-${POSTGRES_DB:-postgres}}
query_status=0
"$psql_bin" -qtAX -F"$tab" -v ON_ERROR_STOP=1 -U "$user" -d "$database" \
  -c "SELECT seq, job_id, op, old_status, new_status, recovered_at, observed_at, pg_xact_commit_timestamp(xmin) FROM siglake_outage_job_history ORDER BY seq" \
  >"$rows" 2>>"$errors" || query_status=$?
printf "status\t%s\n" "$query_status"
while IFS= read -r line; do
  printf "row\t%s\n" "$line"
done <"$rows"
printf "detail\t%s\n" "$(tr "\n\t" "  " <"$errors" | cut -c1-200)"
'
# job-history-snippet-end

# Date the write probe's own rows by the transaction that wrote each one. The
# probe knows when it took those writes -- baseline before the pause,
# recovery after restoration -- so they are the control on every other commit
# timestamp in the trace: a systematic skew between the kind node's clock and
# Postgres's moves every job-row commit together, and nothing else here would
# notice. Ordered by phase and commit time so a phase that wrote twice is
# visible as two rows rather than as one arbitrary row.
# write-probe-times-snippet-begin
POSTGRES_WRITE_PROBE_TIMES_SNIPPET='
errors=${WRITE_PROBE_TIMES_ERRORS:-/tmp/siglake-outage-write-probe-times.err}
rows=${WRITE_PROBE_TIMES_ROWS:-/tmp/siglake-outage-write-probe-times.rows}
: >"$errors"
: >"$rows"
tab=$(printf "\t")
psql_bin=${WRITE_PROBE_TIMES_PSQL:-psql}
user=${PGUSER:-${POSTGRES_USER:-postgres}}
database=${PGDATABASE:-${POSTGRES_DB:-postgres}}
query_status=0
"$psql_bin" -qtAX -F"$tab" -v ON_ERROR_STOP=1 -U "$user" -d "$database" \
  -c "SELECT phase, observed_at, pg_xact_commit_timestamp(xmin) FROM siglake_outage_write_probe ORDER BY phase, 3" \
  >"$rows" 2>>"$errors" || query_status=$?
printf "status\t%s\n" "$query_status"
while IFS= read -r line; do
  printf "row\t%s\n" "$line"
done <"$rows"
printf "detail\t%s\n" "$(tr "\n\t" "  " <"$errors" | cut -c1-200)"
'
# write-probe-times-snippet-end

# The pause-window readers are advisory: a failed exec is retained as evidence that
# the pause window went unobserved, never as a reason to leave Postgres stopped.
# Their request timeouts are short for the same reason — an exec that cannot be
# served against a stopped PID 1 costs one sample, not the window.
postgres_process_state() {
  local output=$1 status=0
  kubectl --context "$KUBE_CONTEXT" --request-timeout=10s -n "$NAMESPACE" \
    exec "$POSTGRES_POD" -- sh -eu -c "$POSTGRES_STATE_SNIPPET" \
    >"$output" 2>"$output.err" || status=$?
  printf '%s' "$status"
}

postgres_write_probe() {
  local phase=$1 at status=0 line=
  # The phase reaches psql as a dollar-quoted literal built here, where a
  # single quote is allowed and nothing re-expands the tag. Only these three
  # names are ever written, so the literal cannot carry its own tag.
  case "$phase" in
    baseline | outage | recovery) ;;
    *) die "unknown write-probe phase: $phase" ;;
  esac
  at=$(iso_now)
  line=$(kubectl --context "$KUBE_CONTEXT" \
    --request-timeout="$((WRITE_PROBE_SECONDS + 15))s" -n "$NAMESPACE" \
    exec "$POSTGRES_POD" -- sh -eu -c "$POSTGRES_WRITE_PROBE_SNIPPET" \
    write-probe "$WRITE_PROBE_SECONDS" '$phase$'"$phase"'$phase$' \
    2>"$TMP_DIR/write-probe.err") || status=$?
  python3 - "$phase" "$at" "$WRITE_PROBE_SECONDS" "$status" "$line" \
    "$WRITE_PROBES_FILE" <<'PY'
import json, sys
phase, at, timeout_seconds, exec_status, line, output = sys.argv[1:]
fields = dict(
    part.split("=", 1) for part in line.split("\t") if "=" in part
)
probe = {
    "at": at,
    "phase": phase,
    "timeout_seconds": int(timeout_seconds),
    "exec_status": int(exec_status),
    "outcome": fields.get("outcome", "error"),
    "exit_status": int(fields["status"]) if fields.get("status", "").isdigit() else None,
    "seconds": float(fields["seconds"]) if fields.get("seconds", "").isdigit() else None,
    "detail": fields.get("detail", ""),
}
with open(output, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(probe) + "\n")
PY
  log "write probe ($phase): $line"
}

# Advisory in the same way as the two pause-window readers: a failed exec is
# retained as evidence that the job rows could not be dated, never as a reason
# to leave the run without a verdict.
postgres_commit_times() {
  local output=$1 status=0
  kubectl --context "$KUBE_CONTEXT" --request-timeout=30s -n "$NAMESPACE" \
    exec "$POSTGRES_POD" -- sh -eu -c "$POSTGRES_COMMIT_TIMES_SNIPPET" \
    commit-times >"$output" 2>"$output.err" || status=$?
  printf '%s' "$status"
}

# Advisory in the same way: a failed install is retained as the reason the
# history is empty, never as a reason to abandon the run. Without it the
# grader is back to dating the visible row version alone, which is the gap
# this reader exists to close.
postgres_install_job_history() {
  local output=$1 status=0
  kubectl --context "$KUBE_CONTEXT" --request-timeout=30s -n "$NAMESPACE" \
    exec "$POSTGRES_POD" -- sh -eu -c "$POSTGRES_JOB_HISTORY_INSTALL_SNIPPET" \
    job-history-install >"$output" 2>"$output.err" || status=$?
  printf '%s' "$status"
}

postgres_job_history() {
  local output=$1 status=0
  kubectl --context "$KUBE_CONTEXT" --request-timeout=30s -n "$NAMESPACE" \
    exec "$POSTGRES_POD" -- sh -eu -c "$POSTGRES_JOB_HISTORY_SNIPPET" \
    job-history >"$output" 2>"$output.err" || status=$?
  printf '%s' "$status"
}

# Advisory in the same way: without it the trace keeps the write probes'
# outcomes and loses the control on the clock they were dated against.
postgres_write_probe_times() {
  local output=$1 status=0
  kubectl --context "$KUBE_CONTEXT" --request-timeout=30s -n "$NAMESPACE" \
    exec "$POSTGRES_POD" -- sh -eu -c "$POSTGRES_WRITE_PROBE_TIMES_SNIPPET" \
    write-probe-times >"$output" 2>"$output.err" || status=$?
  printf '%s' "$status"
}

postgres_container_status() {
  kubectl --context "$KUBE_CONTEXT" --request-timeout=30s -n "$NAMESPACE" \
    get pod "$POSTGRES_POD" -o \
    jsonpath='{.metadata.uid}{"\t"}{.status.containerStatuses[0].restartCount}{"\t"}{.status.containerStatuses[0].state.running.startedAt}' \
    2>/dev/null || true
}

resolve_postgres_signal_target() {
  local pod_target node_inspection container_name node_running kind_cluster kind_role node_name
  local cri_inspection="$TMP_DIR/postgres-cri.json"
  pod_target=$(python3 - "$TMP_DIR/postgres-pod.json" <<'PY'
import json, sys

pod = json.load(open(sys.argv[1], encoding="utf-8"))
statuses = pod.get("status", {}).get("containerStatuses", [])
if len(statuses) != 1:
    raise SystemExit(f"expected one Postgres container status, found {len(statuses)}")
status = statuses[0]
container_id = status.get("containerID", "")
if not container_id.startswith("containerd://"):
    raise SystemExit(f"expected a containerd runtime ID, got {container_id!r}")
runtime_id = container_id.removeprefix("containerd://")
if len(runtime_id) != 64 or any(char not in "0123456789abcdef" for char in runtime_id):
    raise SystemExit(f"invalid containerd ID {runtime_id!r}")
fields = (
    pod.get("metadata", {}).get("uid", ""),
    pod.get("spec", {}).get("nodeName", ""),
    status.get("name", ""),
    runtime_id,
)
if not all(fields) or fields[2] != "postgres":
    raise SystemExit(f"incomplete or unexpected Postgres target: {fields!r}")
print("\t".join(fields))
PY
) || die "could not resolve the Postgres pod's node and container identity"
  IFS=$'\t' read -r POSTGRES_POD_UID POSTGRES_NODE container_name \
    POSTGRES_CONTAINER_ID <<<"$pod_target"

  node_inspection=$(docker inspect --format \
    '{{.State.Running}}{{"\t"}}{{with index .Config.Labels "io.x-k8s.kind.cluster"}}{{.}}{{end}}{{"\t"}}{{with index .Config.Labels "io.x-k8s.kind.role"}}{{.}}{{end}}{{"\t"}}{{.Name}}' \
    "$POSTGRES_NODE") || die "could not inspect the Postgres pod's node $POSTGRES_NODE"
  IFS=$'\t' read -r node_running kind_cluster kind_role node_name <<<"$node_inspection"
  [[ "$node_running" == true && -n "$kind_cluster" && \
    ("$kind_role" == control-plane || "$kind_role" == worker) && \
    "$KUBE_CONTEXT" == "kind-$kind_cluster" && \
    "$node_name" == "/$POSTGRES_NODE" ]] ||
    die "$POSTGRES_NODE is not the running kind node that owns the Postgres pod"

  docker exec "$POSTGRES_NODE" crictl inspect "$POSTGRES_CONTAINER_ID" \
    >"$cri_inspection" ||
    die "could not inspect Postgres container $POSTGRES_CONTAINER_ID on $POSTGRES_NODE"
  POSTGRES_CONTAINER_PID=$(python3 - "$cri_inspection" "$POSTGRES_CONTAINER_ID" \
    "$POSTGRES_POD_UID" <<'PY'
import json, sys

path, expected_id, expected_pod_uid = sys.argv[1:]
inspection = json.load(open(path, encoding="utf-8"))
status = inspection.get("status", {})
labels = status.get("labels", {})
pid = inspection.get("info", {}).get("pid")
if status.get("id") != expected_id:
    raise SystemExit("CRI returned a different container ID")
if status.get("metadata", {}).get("name") != "postgres":
    raise SystemExit("CRI container name is not postgres")
if status.get("state") != "CONTAINER_RUNNING":
    raise SystemExit(f"Postgres container is not running: {status.get('state')!r}")
if labels.get("io.kubernetes.pod.uid") != expected_pod_uid:
    raise SystemExit("CRI pod UID does not match the selected Kubernetes pod")
try:
    pid = int(pid)
except (TypeError, ValueError):
    raise SystemExit(f"CRI returned an invalid init PID: {pid!r}")
if pid <= 1:
    raise SystemExit(f"CRI returned an invalid init PID: {pid}")
print(pid)
PY
) || die "CRI identity did not match the selected Postgres pod"
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  restore_postgres
  rm -rf -- "$TMP_DIR"
  exit "$status"
}
trap cleanup EXIT INT TERM

for tool in curl docker git kubectl python3; do
  command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool"
done
for value in "$REQUESTED_JOBS" "$OUTAGE_SECONDS" "$DRAIN_TIMEOUT_SECONDS" \
  "$SAMPLE_INTERVAL_SECONDS" "$QUERY_TIMEOUT_SECONDS" "$WRITE_PROBE_SECONDS"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "probe durations and counts must be positive integers: $value"
done
((REQUESTED_JOBS <= 64)) || die "POSTGRES_OUTAGE_JOBS exceeds the 64-job safety cap"
((OUTAGE_SECONDS <= 300)) || die "POSTGRES_OUTAGE_SECONDS exceeds the 300s safety cap"
((DRAIN_TIMEOUT_SECONDS <= 600)) || die "POSTGRES_OUTAGE_DRAIN_TIMEOUT_SECONDS exceeds the 600s safety cap"
((SAMPLE_INTERVAL_SECONDS <= 60)) || die "POSTGRES_OUTAGE_SAMPLE_INTERVAL_SECONDS exceeds the 60s safety cap"
((QUERY_TIMEOUT_SECONDS <= 60)) || die "POSTGRES_OUTAGE_QUERY_TIMEOUT_SECONDS exceeds the 60s safety cap"
((WRITE_PROBE_SECONDS <= 30)) || die "POSTGRES_OUTAGE_WRITE_PROBE_SECONDS exceeds the 30s safety cap"
((WRITE_PROBE_SECONDS < OUTAGE_SECONDS)) || die "POSTGRES_OUTAGE_WRITE_PROBE_SECONDS must be shorter than the outage window"

mkdir -p "$RESULTS_DIR" "$SUBMISSIONS_DIR"
: >"$SAMPLES_FILE"
: >"$WRITE_PROBES_FILE"

POSTGRES_POD=$(kubectl --context "$KUBE_CONTEXT" --request-timeout=30s \
  -n "$NAMESPACE" get pods \
  -l app=postgres -o jsonpath='{.items[0].metadata.name}')
[[ -n "$POSTGRES_POD" ]] || die "no Postgres pod found"
kubectl --context "$KUBE_CONTEXT" --request-timeout=125s -n "$NAMESPACE" wait \
  --for=condition=Ready "pod/$POSTGRES_POD" --timeout=120s >/dev/null

kubectl --context "$KUBE_CONTEXT" --request-timeout=30s -n "$NAMESPACE" get pods \
  -l 'app.kubernetes.io/instance=siglake,app.kubernetes.io/component=query' \
  --sort-by=.metadata.name -o json >"$TMP_DIR/query-pods.json"
kubectl --context "$KUBE_CONTEXT" --request-timeout=30s -n "$NAMESPACE" \
  get pod "$POSTGRES_POD" \
  -o json >"$TMP_DIR/postgres-pod.json"
resolve_postgres_signal_target
python3 - "$TMP_DIR/query-pods.json" >"$TMP_DIR/expected-pods" <<'PY'
import json, sys
items = json.load(open(sys.argv[1], encoding="utf-8")).get("items", [])
for item in items:
    conditions = {row.get("type"): row.get("status") for row in item.get("status", {}).get("conditions", [])}
    if conditions.get("Ready") == "True":
        print(item["metadata"]["name"])
PY
[[ -s "$TMP_DIR/expected-pods" ]] || die "no ready query pods found"

prometheus_vector() {
  local expression=$1 output=$2
  if ! curl -fsS --connect-timeout 5 --max-time 10 --get \
    "$PROM_URL/api/v1/query" --data-urlencode "query=$expression" >"$output"; then
    printf '{"status":"error","data":{"result":[]}}\n' >"$output"
  fi
}

# Append one instant sample and print its total backlog, or `missing`. The
# completion expression uses the always-present backlog gauge as the observed
# zero for pods that have not completed a batch job yet. `timestamp()` on that
# same gauge carries the scrape each value came from, which is the only thing
# that separates a counter moving during the pause from the delayed observation
# of work that finished before it: the instant query's own `value[0]` is the
# evaluation time and says nothing about sample age.
sample_metrics() {
  local phase=$1 at backlog_expr completion_expr scrape_expr state_status
  at=$(iso_now)
  backlog_expr="siglake_query_jobs_unreconciled{namespace=\"$NAMESPACE\"}"
  completion_expr="sum by (pod) (siglake_query_jobs_total{namespace=\"$NAMESPACE\",priority=\"batch\"}) or on (pod) (0 * siglake_query_jobs_unreconciled{namespace=\"$NAMESPACE\"})"
  scrape_expr="timestamp(siglake_query_jobs_unreconciled{namespace=\"$NAMESPACE\"})"
  prometheus_vector "$backlog_expr" "$TMP_DIR/backlog.json"
  prometheus_vector "$completion_expr" "$TMP_DIR/completions.json"
  prometheus_vector "$scrape_expr" "$TMP_DIR/scrape-times.json"
  state_status=$(postgres_process_state "$TMP_DIR/postgres-state")
  python3 - "$phase" "$at" "$TMP_DIR/backlog.json" "$TMP_DIR/completions.json" \
    "$TMP_DIR/scrape-times.json" "$TMP_DIR/expected-pods" \
    "$TMP_DIR/postgres-state" "$state_status" "$SAMPLES_FILE" <<'PY'
import json, sys
(
    phase, at, backlog_path, completion_path, scrape_path, expected_path,
    state_path, state_status, output,
) = sys.argv[1:]
expected = {line.strip() for line in open(expected_path, encoding="utf-8") if line.strip()}

def vector(path):
    try:
        document = json.load(open(path, encoding="utf-8"))
        if document.get("status") != "success":
            return {}
        rows = {}
        for item in document.get("data", {}).get("result", []):
            pod = item.get("metric", {}).get("pod")
            evaluated, value = item.get("value", [None, None])
            if pod in expected and value is not None:
                rows[pod] = (float(value), float(evaluated) if evaluated is not None else None)
        return rows
    except (OSError, ValueError, TypeError, IndexError):
        return {}

def series(rows, scrapes):
    return [
        {"pod": pod, "value": value, "sample_time": scrapes.get(pod, (None, None))[0]}
        for pod, (value, _) in sorted(rows.items())
    ]

def processes(path):
    rows = []
    try:
        for line in open(path, encoding="utf-8"):
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 3 and all(parts):
                rows.append({"pid": parts[0], "state": parts[1], "starttime": parts[2]})
    except OSError:
        return []
    return rows

backlog = vector(backlog_path)
completions = vector(completion_path)
scrapes = vector(scrape_path)
evaluated = next((stamp for _, stamp in backlog.values() if stamp is not None), None)
sample = {
    "at": at,
    "phase": phase,
    "evaluated_at": evaluated,
    "backlog": series(backlog, scrapes),
    "completions": series(completions, scrapes),
    "postgres": {
        "exec_status": int(state_status) if state_status.isdigit() else 1,
        "processes": processes(state_path),
    },
}
with open(output, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(sample) + "\n")
print(sum(value for value, _ in backlog.values()) if backlog else "missing")
PY
}

submit_one() {
  local index=$1 at status response="$SUBMISSIONS_DIR/$index.response.json"
  at=$(iso_now)
  status=$(curl -sS --max-time 15 -o "$response" -w '%{http_code}' \
    -X POST 'http://127.0.0.1:8089/api/v1/sql' \
    -H 'Content-Type: application/json' -H 'X-Scope-OrgID: default' \
    --data-binary "@$TMP_DIR/batch-payload.json" || true)
  [[ "$status" =~ ^[0-9][0-9][0-9]$ ]] || status=0
  printf '%s\t%s\t%s\n' "$at" "$status" "$response" >"$SUBMISSIONS_DIR/$index.meta"
}

python3 - "$QUERY" "$QUERY_TIMEOUT_SECONDS" >"$TMP_DIR/batch-payload.json" <<'PY'
import json, sys
json.dump({
    "query": sys.argv[1],
    "priority": "batch",
    "limits": {"timeout_seconds": int(sys.argv[2])},
}, sys.stdout, separators=(",", ":"))
PY

log "record baseline from Prometheus"
sample_metrics baseline >/dev/null
postgres_write_probe baseline
CONTAINER_BEFORE=$(postgres_container_status)
# Before the burst, so every transition this probe is about is recorded. The
# job table itself already exists: the query server creates it at start-up
# (`SCHEMA_SQL_STATEMENTS`, crates/siglake-query-server/src/jobs.rs:2322).
log "install the job-row write history on the throwaway Postgres"
JOB_HISTORY_INSTALL_STATUS=$(postgres_install_job_history \
  "$TMP_DIR/job-history-install")
SUBMISSION_STARTED_AT=$(iso_now)
log "submit a burst of $REQUESTED_JOBS bounded batch jobs before the fault"
for index in $(seq 1 "$REQUESTED_JOBS"); do
  submit_one "$index" &
done
wait
SUBMISSION_FINISHED_AT=$(iso_now)

log "pause only $POSTGRES_POD for ${OUTAGE_SECONDS}s"
POSTGRES_PAUSED=1
pause_postgres_processes >"$TMP_DIR/pause-signal-boundaries"
# These bounds are read inside the single kind-node exec, at the first signal
# attempt and after every selected process reports stopped. The grader
# accounts for their retained millisecond precision, so a commit that still
# overlaps the transition remains unplaceable.
OUTAGE_STARTED_AT=$(awk -F '\t' '$1 == "started_at" { print $2 }' \
  "$TMP_DIR/pause-signal-boundaries")
PAUSE_APPLIED_AT=$(awk -F '\t' '$1 == "applied_at" { print $2 }' \
  "$TMP_DIR/pause-signal-boundaries")
[[ -n "$OUTAGE_STARTED_AT" && -n "$PAUSE_APPLIED_AT" ]] ||
  die "the Postgres pause did not return measured signal bounds"

observed_positive=0
write_probed=0
outage_deadline=$((SECONDS + OUTAGE_SECONDS))
write_probe_at=$((SECONDS + OUTAGE_SECONDS / 2))
while ((SECONDS < outage_deadline)); do
  total=$(sample_metrics outage)
  if [[ "$total" != missing ]] && python3 -c 'import sys; raise SystemExit(0 if float(sys.argv[1]) > 0 else 1)' "$total"; then
    observed_positive=1
  fi
  # Once, in the middle of the window, so the pause is proven by a write that
  # could not land rather than only by the process states around it.
  if ((write_probed == 0 && SECONDS >= write_probe_at)); then
    postgres_write_probe outage
    write_probed=1
  fi
  sleep "$SAMPLE_INTERVAL_SECONDS"
done

log "continue $POSTGRES_POD and wait for readiness"
continue_postgres_processes >"$TMP_DIR/restoration-signal-boundaries"
RESTORATION_STARTED_AT=$(awk -F '\t' '$1 == "started_at" { print $2 }' \
  "$TMP_DIR/restoration-signal-boundaries")
RESTORATION_APPLIED_AT=$(awk -F '\t' '$1 == "applied_at" { print $2 }' \
  "$TMP_DIR/restoration-signal-boundaries")
[[ -n "$RESTORATION_STARTED_AT" && -n "$RESTORATION_APPLIED_AT" ]] ||
  die "the Postgres continuation did not return measured signal bounds"
POSTGRES_PAUSED=0
kubectl --context "$KUBE_CONTEXT" --request-timeout=125s -n "$NAMESPACE" wait \
  --for=condition=Ready "pod/$POSTGRES_POD" --timeout=120s >/dev/null
POSTGRES_READY_AT=$(iso_now)
postgres_write_probe recovery
CONTAINER_AFTER=$(postgres_container_status)

recovery_deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS))
while ((SECONDS < recovery_deadline)); do
  total=$(sample_metrics recovery)
  if [[ "$observed_positive" -eq 1 && "$total" != missing ]] && \
    python3 -c 'import sys; raise SystemExit(0 if float(sys.argv[1]) == 0 else 1)' "$total"; then
    break
  fi
  sleep "$SAMPLE_INTERVAL_SECONDS"
done
SAMPLING_ENDED_AT=$(iso_now)

# After the bounded recovery window, not at readiness: a ready postmaster says
# connections are served, not that reconciliation has finished writing the rows
# this reading is about.
log "read job-row commit timestamps"
COMMIT_TIMES_AT=$(iso_now)
COMMIT_TIMES_STATUS=$(postgres_commit_times "$TMP_DIR/commit-times")

log "read the job-row write history"
JOB_HISTORY_AT=$(iso_now)
JOB_HISTORY_STATUS=$(postgres_job_history "$TMP_DIR/job-history")

# After the recovery write probe, which is the last row this table gets.
log "read the write probe's own commit timestamps"
WRITE_PROBE_TIMES_AT=$(iso_now)
WRITE_PROBE_TIMES_STATUS=$(postgres_write_probe_times "$TMP_DIR/write-probe-times")

python3 - "$ROOT" "$TMP_DIR" "$SAMPLES_FILE" "$SUBMISSIONS_DIR" \
  "$SUBMISSION_STARTED_AT" "$SUBMISSION_FINISHED_AT" "$OUTAGE_STARTED_AT" \
  "$RESTORATION_STARTED_AT" "$POSTGRES_READY_AT" "$SAMPLING_ENDED_AT" \
  "$REQUESTED_JOBS" "$OUTAGE_SECONDS" "$DRAIN_TIMEOUT_SECONDS" \
  "$SAMPLE_INTERVAL_SECONDS" "$QUERY_TIMEOUT_SECONDS" "$QUERY" \
  "$WRITE_PROBE_SECONDS" "$WRITE_PROBES_FILE" "$CONTAINER_BEFORE" \
  "$CONTAINER_AFTER" "$PAUSE_APPLIED_AT" "$RESTORATION_APPLIED_AT" \
  "$COMMIT_TIMES_AT" "$COMMIT_TIMES_STATUS" "$TMP_DIR/commit-times" \
  "$POSTGRES_NODE" "$POSTGRES_CONTAINER_ID" "$POSTGRES_CONTAINER_PID" \
  "$POSTGRES_PID_NAMESPACE" "$POSTGRES_PROCESSES_FILE" \
  "$JOB_HISTORY_AT" "$JOB_HISTORY_INSTALL_STATUS" "$JOB_HISTORY_STATUS" \
  "$TMP_DIR/job-history-install" "$TMP_DIR/job-history" \
  "$WRITE_PROBE_TIMES_AT" "$WRITE_PROBE_TIMES_STATUS" \
  "$TMP_DIR/write-probe-times" \
  "$TMP_DIR/raw.json" <<'PY'
import datetime, json, pathlib, subprocess, sys
(
    root, tmp, samples_path, submissions_dir, submission_started, submission_finished,
    outage_started, restoration_started, postgres_ready, sampling_ended,
    requested_jobs, outage_seconds, drain_timeout, sample_interval, query_timeout,
    query, write_probe_seconds, write_probes_path, container_before, container_after,
    pause_applied, restoration_applied, commit_times_at, commit_times_status,
    commit_times_path, postgres_node, postgres_container_id, postgres_container_pid,
    postgres_pid_namespace, postgres_processes_path, job_history_at,
    job_history_install_status, job_history_status, job_history_install_path,
    job_history_path, write_probe_times_at, write_probe_times_status,
    write_probe_times_path, output,
) = sys.argv[1:]
tmp = pathlib.Path(tmp)

# `status`, `setting`, `row` and `detail` lines, in the shape the remote snippet
# prints them. A row keeps every field as text, including the empty strings a
# NULL `ended_at`, `recovered_at` or commit timestamp comes back as: the grader
# reads a missing commit timestamp as evidence it cannot date, so this must not
# quietly turn one into a value.
def commit_times(path, at, exec_status):
    reading = {
        "at": at,
        "exec_status": int(exec_status) if exec_status.isdigit() else 1,
        "setting_status": None,
        "query_status": None,
        "track_commit_timestamp": None,
        "rows": [],
        "detail": "",
    }
    fields = ("job_id", "status", "submitted_at", "ended_at", "recovered_at", "committed_at")
    try:
        lines = open(path, encoding="utf-8").read().splitlines()
    except OSError:
        return reading
    for line in lines:
        parts = line.split("\t")
        if parts[0] == "status" and len(parts) == 3:
            reading["setting_status"] = int(parts[1]) if parts[1].isdigit() else None
            reading["query_status"] = int(parts[2]) if parts[2].isdigit() else None
        elif parts[0] == "setting" and len(parts) == 2:
            reading["track_commit_timestamp"] = parts[1] or None
        elif parts[0] == "row" and len(parts) == len(fields) + 1:
            reading["rows"].append(
                {name: (parts[index + 1] or None) for index, name in enumerate(fields)}
            )
        elif parts[0] == "detail" and len(parts) == 2:
            reading["detail"] = parts[1]
    if not reading["detail"]:
        # An exec that never reached the snippet leaves its complaint here, and
        # that is the only thing that would say why the rows are missing.
        try:
            reading["detail"] = (
                open(path + ".err", encoding="utf-8").read().replace("\n", " ").strip()[:200]
            )
        except OSError:
            pass
    return reading

# The insert-only transition history, in the two shapes its snippets print.
# Its rows are never rewritten, so each one's commit timestamp dates the
# transaction that made the transition rather than the latest version of the
# job row. An install that failed is retained as the reason the history is
# empty; the grader reads an empty history as something it cannot date, so
# this must not turn a failure into "the job made no transitions".
def snippet_lines(path):
    try:
        return open(path, encoding="utf-8").read().splitlines()
    except OSError:
        return []


def exec_complaint(path):
    try:
        return open(path + ".err", encoding="utf-8").read().replace("\n", " ").strip()[:200]
    except OSError:
        return ""


def write_history(install_path, read_path, at, install_exec_status, exec_status):
    reading = {
        "at": at,
        "install_exec_status": int(install_exec_status) if install_exec_status.isdigit() else 1,
        "install_status": None,
        "install_detail": "",
        "exec_status": int(exec_status) if exec_status.isdigit() else 1,
        "query_status": None,
        "rows": [],
        "detail": "",
    }
    for line in snippet_lines(install_path):
        parts = line.split("\t")
        if parts[0] == "install" and len(parts) == 2:
            reading["install_status"] = int(parts[1]) if parts[1].isdigit() else None
        elif parts[0] == "detail" and len(parts) == 2:
            reading["install_detail"] = parts[1]
    if not reading["install_detail"]:
        reading["install_detail"] = exec_complaint(install_path)
    fields = (
        "seq", "job_id", "op", "old_status", "new_status", "recovered_at",
        "observed_at", "committed_at",
    )
    for line in snippet_lines(read_path):
        parts = line.split("\t")
        if parts[0] == "status" and len(parts) == 2:
            reading["query_status"] = int(parts[1]) if parts[1].isdigit() else None
        elif parts[0] == "row" and len(parts) == len(fields) + 1:
            row = {name: (parts[index + 1] or None) for index, name in enumerate(fields)}
            row["seq"] = int(row["seq"]) if (row["seq"] or "").isdigit() else None
            reading["rows"].append(row)
        elif parts[0] == "detail" and len(parts) == 2:
            reading["detail"] = parts[1]
    if not reading["detail"]:
        reading["detail"] = exec_complaint(read_path)
    return reading


# The write probe's own rows, dated by Postgres. A failed query is retained as
# a failed query: an empty row set here would read as "the baseline write never
# committed", which is a different finding from "nothing could read the table".
def write_probe_times(path, at, exec_status):
    reading = {
        "at": at,
        "exec_status": int(exec_status) if exec_status.isdigit() else 1,
        "query_status": None,
        "rows": [],
        "detail": "",
    }
    fields = ("phase", "observed_at", "committed_at")
    for line in snippet_lines(path):
        parts = line.split("\t")
        if parts[0] == "status" and len(parts) == 2:
            reading["query_status"] = int(parts[1]) if parts[1].isdigit() else None
        elif parts[0] == "row" and len(parts) == len(fields) + 1:
            reading["rows"].append(
                {name: (parts[index + 1] or None) for index, name in enumerate(fields)}
            )
        elif parts[0] == "detail" and len(parts) == 2:
            reading["detail"] = parts[1]
    if not reading["detail"]:
        reading["detail"] = exec_complaint(path)
    return reading


def container_identity(raw):
    parts = raw.split("\t")
    if len(parts) != 3 or not parts[0] or not parts[1].isdigit():
        return None
    return {"uid": parts[0], "restart_count": int(parts[1]), "started_at": parts[2] or None}

def container_revision(item):
    spec = item.get("spec", {}).get("containers", [{}])[0]
    status = item.get("status", {}).get("containerStatuses", [{}])[0]
    return {
        "pod": item["metadata"]["name"],
        "image": spec.get("image"),
        "image_id": status.get("imageID"),
    }

def fault_processes(path):
    rows = []
    for line in open(path, encoding="utf-8"):
        pid, state, starttime, pid_namespace = line.rstrip("\n").split("\t")
        rows.append({
            "node_pid": int(pid),
            "state_before": state,
            "starttime": starttime,
            "pid_namespace": pid_namespace,
        })
    return rows

query_items = json.load(open(tmp / "query-pods.json", encoding="utf-8")).get("items", [])
postgres_item = json.load(open(tmp / "postgres-pod.json", encoding="utf-8"))
expected = [line.strip() for line in open(tmp / "expected-pods", encoding="utf-8") if line.strip()]
samples = [json.loads(line) for line in open(samples_path, encoding="utf-8") if line.strip()]
submissions = []
for meta in sorted(pathlib.Path(submissions_dir).glob("*.meta"), key=lambda p: int(p.stem)):
    at, status, response_path = meta.read_text(encoding="utf-8").strip().split("\t")
    try:
        response = json.load(open(response_path, encoding="utf-8"))
    except (OSError, ValueError):
        response = {}
    submissions.append({
        "submitted_at": at,
        "http_status": int(status),
        "job_id": response.get("job_id"),
    })
write_probes = [
    json.loads(line) for line in open(write_probes_path, encoding="utf-8") if line.strip()
]
document = {
    "schema_version": 5,
    "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "revisions": {
        "repository_commit": subprocess.check_output(
            ["git", "-C", root, "rev-parse", "HEAD"], text=True
        ).strip(),
        "query_pods": [container_revision(item) for item in query_items if item["metadata"]["name"] in expected],
        "postgres": container_revision(postgres_item),
    },
    "settings": {
        "persistent_job_store": True,
        "reconcile_interval_seconds": 5,
        "reconcile_write_timeout_seconds": 10,
        "max_unreconciled_per_pod": 1024,
        "terminal_write_attempts": 3,
        "terminal_write_deadline_seconds": 30,
        "sample_interval_seconds": int(sample_interval),
        "outage_seconds": int(outage_seconds),
        "drain_timeout_seconds": int(drain_timeout),
        "requested_jobs": int(requested_jobs),
        "query_timeout_seconds": int(query_timeout),
        "write_probe_timeout_seconds": int(write_probe_seconds),
        "query": query,
    },
    "expected_pods": expected,
    "postgres_container": {
        "pod": postgres_item["metadata"]["name"],
        "before": container_identity(container_before),
        "after": container_identity(container_after),
    },
    "fault_target": {
        "node": postgres_node,
        "container_id": postgres_container_id,
        "container_init_pid": int(postgres_container_pid),
        "pid_namespace": postgres_pid_namespace,
        "processes": fault_processes(postgres_processes_path),
    },
    "write_probes": write_probes,
    "write_probe_commit_times": write_probe_times(
        write_probe_times_path, write_probe_times_at, write_probe_times_status
    ),
    "job_commit_times": commit_times(commit_times_path, commit_times_at, commit_times_status),
    "job_write_history": write_history(
        job_history_install_path, job_history_path, job_history_at,
        job_history_install_status, job_history_status,
    ),
    "timestamps": {
        "submission_started_at": submission_started,
        "submission_finished_at": submission_finished,
        "outage_started_at": outage_started,
        "pause_applied_at": pause_applied,
        "restoration_started_at": restoration_started,
        "restoration_applied_at": restoration_applied,
        "postgres_ready_at": postgres_ready,
        "sampling_ended_at": sampling_ended,
    },
    "submissions": submissions,
    "samples": samples,
}
json.dump(document, open(output, "w", encoding="utf-8"), indent=2)
PY

log "grade retained evidence"
python3 "$ROOT/scripts/grade-kind-postgres-outage.py" "$TMP_DIR/raw.json" \
  --output "$EVIDENCE_JSON"
log "retained $EVIDENCE_JSON"
