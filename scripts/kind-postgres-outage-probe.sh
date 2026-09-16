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
EVIDENCE_JSON="$RESULTS_DIR/postgres-outage-reconnect.json"

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/siglake-postgres-outage.XXXXXX")
SAMPLES_FILE="$TMP_DIR/samples.jsonl"
SUBMISSIONS_DIR="$TMP_DIR/submissions"
POSTGRES_PAUSED=0
POSTGRES_POD=

log() { printf '==> postgres-outage: %s\n' "$*" >&2; }
die() { printf 'ERROR: postgres-outage: %s\n' "$*" >&2; exit 1; }
iso_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

restore_postgres() {
  [[ "$POSTGRES_PAUSED" -eq 1 ]] || return 0
  log "restore Postgres after interrupted probe"
  continue_postgres_processes >/dev/null 2>&1 || true
  POSTGRES_PAUSED=0
}

# The postgres image execs the postmaster as PID 1, but every established
# client is served by another process. Freeze PID 1 first so it cannot fork a
# new backend while the exact postgres process set is being stopped. The
# remote identity checks keep this bounded to the disposable postgres pod.
pause_postgres_processes() {
  kubectl --context "$KUBE_CONTEXT" --request-timeout=30s -n "$NAMESPACE" \
    exec "$POSTGRES_POD" -- sh -eu -c '
      [ "$(cat /proc/1/comm)" = postgres ] || {
        echo "PID 1 is not postgres; refusing to pause the container" >&2
        exit 1
      }
      kill -STOP 1
      backend_count=0
      for comm_path in /proc/[0-9]*/comm; do
        pid=${comm_path#/proc/}
        pid=${pid%/comm}
        [ "$pid" = 1 ] && continue
        [ "$(cat "$comm_path" 2>/dev/null || true)" = postgres ] || continue
        kill -STOP "$pid"
        backend_count=$((backend_count + 1))
      done
      if [ "$backend_count" -eq 0 ]; then
        kill -CONT 1
        echo "no established postgres backend process found" >&2
        exit 1
      fi
    '
}

# Continue children before PID 1, which prevents the postmaster from creating
# a new backend until every surviving process selected by the pause is live.
# Re-scan by exact process name so this also repairs a partially completed
# pause from an error or signal path.
continue_postgres_processes() {
  kubectl --context "$KUBE_CONTEXT" --request-timeout=30s -n "$NAMESPACE" \
    exec "$POSTGRES_POD" -- sh -eu -c '
      [ "$(cat /proc/1/comm)" = postgres ] || {
        echo "PID 1 is not postgres; refusing to signal the container" >&2
        exit 1
      }
      for comm_path in /proc/[0-9]*/comm; do
        pid=${comm_path#/proc/}
        pid=${pid%/comm}
        [ "$pid" = 1 ] && continue
        [ "$(cat "$comm_path" 2>/dev/null || true)" = postgres ] || continue
        kill -CONT "$pid" 2>/dev/null || true
      done
      kill -CONT 1
    '
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  restore_postgres
  rm -rf -- "$TMP_DIR"
  exit "$status"
}
trap cleanup EXIT INT TERM

for tool in curl git kubectl python3; do
  command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool"
done
for value in "$REQUESTED_JOBS" "$OUTAGE_SECONDS" "$DRAIN_TIMEOUT_SECONDS" \
  "$SAMPLE_INTERVAL_SECONDS" "$QUERY_TIMEOUT_SECONDS"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "probe durations and counts must be positive integers: $value"
done
((REQUESTED_JOBS <= 64)) || die "POSTGRES_OUTAGE_JOBS exceeds the 64-job safety cap"
((OUTAGE_SECONDS <= 300)) || die "POSTGRES_OUTAGE_SECONDS exceeds the 300s safety cap"
((DRAIN_TIMEOUT_SECONDS <= 600)) || die "POSTGRES_OUTAGE_DRAIN_TIMEOUT_SECONDS exceeds the 600s safety cap"
((SAMPLE_INTERVAL_SECONDS <= 60)) || die "POSTGRES_OUTAGE_SAMPLE_INTERVAL_SECONDS exceeds the 60s safety cap"
((QUERY_TIMEOUT_SECONDS <= 60)) || die "POSTGRES_OUTAGE_QUERY_TIMEOUT_SECONDS exceeds the 60s safety cap"

mkdir -p "$RESULTS_DIR" "$SUBMISSIONS_DIR"
: >"$SAMPLES_FILE"

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
# zero for pods that have not completed a batch job yet.
sample_metrics() {
  local phase=$1 at backlog_expr completion_expr
  at=$(iso_now)
  backlog_expr="siglake_query_jobs_unreconciled{namespace=\"$NAMESPACE\"}"
  completion_expr="sum by (pod) (siglake_query_jobs_total{namespace=\"$NAMESPACE\",priority=\"batch\"}) or on (pod) (0 * siglake_query_jobs_unreconciled{namespace=\"$NAMESPACE\"})"
  prometheus_vector "$backlog_expr" "$TMP_DIR/backlog.json"
  prometheus_vector "$completion_expr" "$TMP_DIR/completions.json"
  python3 - "$phase" "$at" "$TMP_DIR/backlog.json" "$TMP_DIR/completions.json" \
    "$TMP_DIR/expected-pods" "$SAMPLES_FILE" <<'PY'
import json, sys
phase, at, backlog_path, completion_path, expected_path, output = sys.argv[1:]
expected = {line.strip() for line in open(expected_path, encoding="utf-8") if line.strip()}

def vector(path):
    try:
        document = json.load(open(path, encoding="utf-8"))
        if document.get("status") != "success":
            return []
        rows = []
        for item in document.get("data", {}).get("result", []):
            pod = item.get("metric", {}).get("pod")
            value = item.get("value", [None, None])[1]
            if pod in expected and value is not None:
                rows.append({"pod": pod, "value": float(value)})
        return sorted(rows, key=lambda row: row["pod"])
    except (OSError, ValueError, TypeError, IndexError):
        return []

backlog = vector(backlog_path)
sample = {
    "at": at,
    "phase": phase,
    "backlog": backlog,
    "completions": vector(completion_path),
}
with open(output, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(sample) + "\n")
print(sum(row["value"] for row in backlog) if backlog else "missing")
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
SUBMISSION_STARTED_AT=$(iso_now)
log "submit a burst of $REQUESTED_JOBS bounded batch jobs before the fault"
for index in $(seq 1 "$REQUESTED_JOBS"); do
  submit_one "$index" &
done
wait
SUBMISSION_FINISHED_AT=$(iso_now)

OUTAGE_STARTED_AT=$(iso_now)
log "pause only $POSTGRES_POD for ${OUTAGE_SECONDS}s"
POSTGRES_PAUSED=1
pause_postgres_processes >/dev/null

observed_positive=0
outage_deadline=$((SECONDS + OUTAGE_SECONDS))
while ((SECONDS < outage_deadline)); do
  total=$(sample_metrics outage)
  if [[ "$total" != missing ]] && python3 -c 'import sys; raise SystemExit(0 if float(sys.argv[1]) > 0 else 1)' "$total"; then
    observed_positive=1
  fi
  sleep "$SAMPLE_INTERVAL_SECONDS"
done

RESTORATION_STARTED_AT=$(iso_now)
log "continue $POSTGRES_POD and wait for readiness"
continue_postgres_processes >/dev/null
POSTGRES_PAUSED=0
kubectl --context "$KUBE_CONTEXT" --request-timeout=125s -n "$NAMESPACE" wait \
  --for=condition=Ready "pod/$POSTGRES_POD" --timeout=120s >/dev/null
POSTGRES_READY_AT=$(iso_now)

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

python3 - "$ROOT" "$TMP_DIR" "$SAMPLES_FILE" "$SUBMISSIONS_DIR" \
  "$SUBMISSION_STARTED_AT" "$SUBMISSION_FINISHED_AT" "$OUTAGE_STARTED_AT" \
  "$RESTORATION_STARTED_AT" "$POSTGRES_READY_AT" "$SAMPLING_ENDED_AT" \
  "$REQUESTED_JOBS" "$OUTAGE_SECONDS" "$DRAIN_TIMEOUT_SECONDS" \
  "$SAMPLE_INTERVAL_SECONDS" "$QUERY_TIMEOUT_SECONDS" "$QUERY" \
  "$TMP_DIR/raw.json" <<'PY'
import datetime, json, pathlib, subprocess, sys
(
    root, tmp, samples_path, submissions_dir, submission_started, submission_finished,
    outage_started, restoration_started, postgres_ready, sampling_ended,
    requested_jobs, outage_seconds, drain_timeout, sample_interval, query_timeout,
    query, output,
) = sys.argv[1:]
tmp = pathlib.Path(tmp)

def container_revision(item):
    spec = item.get("spec", {}).get("containers", [{}])[0]
    status = item.get("status", {}).get("containerStatuses", [{}])[0]
    return {
        "pod": item["metadata"]["name"],
        "image": spec.get("image"),
        "image_id": status.get("imageID"),
    }

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
document = {
    "schema_version": 1,
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
        "query": query,
    },
    "expected_pods": expected,
    "timestamps": {
        "submission_started_at": submission_started,
        "submission_finished_at": submission_finished,
        "outage_started_at": outage_started,
        "restoration_started_at": restoration_started,
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
