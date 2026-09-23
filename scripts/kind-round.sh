#!/usr/bin/env bash
#
# Run the manager's evidence-producing kind round: Siglake, Prometheus and
# KEDA, followed by sustained ingest/query load and machine-readable evidence.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kind-common.bash
source "$ROOT/scripts/kind-common.bash"

CLUSTER_NAME="${KIND_CLUSTER_NAME:-siglake}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE=default
PROM_NAMESPACE=monitoring
KEDA_NAMESPACE=keda
PROM_RELEASE=kube-prometheus-stack
PROM_CHART_VERSION=77.11.1
KEDA_CHART_VERSION=2.17.2
PROM_LOCAL_PORT=19090
PROM_URL="http://127.0.0.1:${PROM_LOCAL_PORT}"
QUERY_LOCAL_PORT=18089
QUERY_METRICS_LOCAL_PORT=19105
LOAD_EVENTS="${KIND_ROUND_EVENTS:-6000}"
[[ "$LOAD_EVENTS" =~ ^[1-9][0-9]*$ ]] || {
  printf 'ERROR: KIND_ROUND_EVENTS must be a positive integer\n' >&2
  exit 1
}
LOAD_BATCH=500
LOAD_SECONDS="${KIND_ROUND_LOAD_SECONDS:-330}"
[[ "$LOAD_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
  printf 'ERROR: KIND_ROUND_LOAD_SECONDS must be a positive integer\n' >&2
  exit 1
}
STEADY_BATCH=64
BENCH_DIR=bench
QUERY_HEADLESS_SERVICE=siglake-query-headless
QUERY_SCALEDOBJECT=siglake-query
# Opt-in only: the outage probe pauses the throwaway kind Postgres process and
# extends the round while it observes reconciliation. Ordinary rounds run the
# chart's own batch-job store -- shared Postgres since 2026-09-11 -- and do not
# inject a fault.
POSTGRES_OUTAGE_PROBE="${POSTGRES_OUTAGE_PROBE:-0}"
[[ "$POSTGRES_OUTAGE_PROBE" == 0 || "$POSTGRES_OUTAGE_PROBE" == 1 ]] || {
  printf 'ERROR: POSTGRES_OUTAGE_PROBE must be 0 or 1\n' >&2
  exit 1
}
# The chart default, stated: the round installs what customers install, and the
# outage probe has nothing to pause without it.
PERSISTENT_JOB_STORE=true
# Opt-in only: the schema-rollback probe builds a SECOND image from this
# checkout, rolls the release A -> B -> A -> B through the chart's pre-upgrade
# migration Job, and reverts an operator-managed cluster's `spec.image`. It
# adds two container image builds and two chart rollouts to the round, and it
# is the only step here that takes the release away from the round's own image.
# Ordinary rounds do not build a second image and do not roll the release.
SCHEMA_ROLLBACK_PROBE="${SCHEMA_ROLLBACK_PROBE:-0}"
[[ "$SCHEMA_ROLLBACK_PROBE" == 0 || "$SCHEMA_ROLLBACK_PROBE" == 1 ]] || {
  printf 'ERROR: SCHEMA_ROLLBACK_PROBE must be 0 or 1\n' >&2
  exit 1
}

# #1838: the query tier is installed with KEDA headroom to QUERY_SCALE_TARGET
# and driven base → target → base inside the load window, which is #968's gate.
QUERY_SCALE_BASE=2
QUERY_SCALE_TARGET=4
# How long the load window may run PAST LOAD_SECONDS waiting for the second
# transition to land. The whole step is bounded by this, so the round can grow
# by at most this much.
SCALE_GRACE_SECONDS=300
# KEDA's anti-flap defaults (300s scale-down stabilization, 300s cooldown) are
# sized for production thrash: at those values the target → base leg alone
# outlasts the whole round. The round installs short windows instead. They
# apply to the ingester ScaledObject too, which changes nothing there — the
# round's ingest rate is two orders of magnitude below its accept-rate trigger,
# so its desired replica count is its minimum at any polling interval.
SCALE_POLLING_SECONDS=5
SCALE_STABILIZATION_SECONDS=30
SCALE_COOLDOWN_SECONDS=30

# #3647: the operator reads the ingester tier's load as
# `avg(sum by (pod) (rate(siglake_ingest_requests_total{...}[1m])))`
# (crates/siglake-operator/src/prom.rs:177). `pod` is attached by Prometheus
# Operator's own target relabeling, not by anything this repository renders, so
# only a live round can show it is there -- and only a round with TWO scraped
# ingester pods, each publishing more than one series, can tell the per-pod
# mean apart from the fleet total and the per-series average.
# The live reading has been retained, so ordinary rounds no longer pay for the
# scale-out and traffic window. Set this only on a round explicitly tasked with
# collecting the per-pod label evidence again.
INGESTER_POD_LABEL_CAPTURE="${INGESTER_POD_LABEL_CAPTURE:-0}"
[[ "$INGESTER_POD_LABEL_CAPTURE" == 0 || "$INGESTER_POD_LABEL_CAPTURE" == 1 ]] || {
  printf 'ERROR: INGESTER_POD_LABEL_CAPTURE must be 0 or 1\n' >&2
  exit 1
}
#
# WHY THE FLOOR AND NOT THE THRESHOLD. Lowering
# `keda.ingester.requestsPerSecondTarget` for the round is the smaller edit, but
# a threshold the round's ~1 rps steady load crosses also lets the ingester
# scale out and back in DURING the query-scaling window above, which would put
# ingester rollouts inside evidence the round already collects (pre-registered
# zeros, restart counts, the WAL mirror's sealing). The floor is the same
# instrument `patch_query_floor` uses on the query tier, it is raised only for
# this one bounded phase after every other observation is written, and it is put
# back. The installed KEDA range is still narrowed to these two constants, so a
# trigger cannot take the tier anywhere the phase did not ask for.
INGESTER_SCALE_BASE=1
INGESTER_SCALE_TARGET=2
INGESTER_SCALEDOBJECT=siglake-ingester
# How long to hold logs+traces traffic on the scaled tier, and the cap on the
# whole phase (pods becoming ready, the [1m] rate window filling, the capture).
INGESTER_POD_LABEL_SECONDS=90
INGESTER_POD_LABEL_GRACE_SECONDS=300
INGESTER_POD_LABEL_BATCH=48
INGESTER_POD_LABEL_TRACES=8
# #4151: unlike the ingester signal, the catalog-claim compactor gauge is a
# copy of one shared queue on every replica. This opt-in installs TWO compactors
# only after the round's ordinary evidence is complete, holds newly sealed rows
# below a deliberately unreachable commit-batch threshold, and retains two
# successive Prometheus scrape generations which agree on the queue depth.
# Ordinary rounds keep the chart's one-compactor kind value.
COMPACTOR_POD_LABEL_CAPTURE="${COMPACTOR_POD_LABEL_CAPTURE:-0}"
[[ "$COMPACTOR_POD_LABEL_CAPTURE" == 0 || "$COMPACTOR_POD_LABEL_CAPTURE" == 1 ]] || {
  printf 'ERROR: COMPACTOR_POD_LABEL_CAPTURE must be 0 or 1\n' >&2
  exit 1
}
COMPACTOR_SCALE_TARGET=2
COMPACTOR_POD_LABEL_LOAD_SECONDS=15
COMPACTOR_POD_LABEL_GRACE_SECONDS=120
COMPACTOR_POD_LABEL_BATCH=500
COMPACTOR_INTERVAL_SECONDS=1
COMPACTOR_SCRAPE_INTERVAL_SECONDS=15
COMPACTOR_CAPTURE_BATCH_TARGET_MB=1024
COMPACTOR_CAPTURE_BATCH_MAX_AGE_SECONDS=300
# The Helm release, restated for the operator expression the capture evaluates:
# `app_kubernetes_io_instance` is the release name.
RELEASE=siglake

# #4953's two-arm mirror-prefix qualification is explicit and off by default.
# Each arm is a separate kind round over the same frozen source. Stating every
# value here makes the launch record self-contained; the exact qualification
# recipe is checked below so an almost-correct arm cannot produce plausible
# evidence. Ordinary rounds retain their previous effective configuration.
MIRROR_RECLAIM_ARM="${KIND_ROUND_MIRROR_RECLAIM_ARM:-}"
CATALOG_CLAIM_ENABLED="${KIND_ROUND_CATALOG_CLAIM_ENABLED:-true}"
WAL_MIRROR_ENABLED="${KIND_ROUND_WAL_MIRROR_ENABLED:-true}"
WAL_MIRROR_ACTIVE_INTERVAL_SECS="${KIND_ROUND_WAL_MIRROR_ACTIVE_INTERVAL_SECS:-0}"
COMMITTED_RETENTION_SECS="${KIND_ROUND_COMMITTED_RETENTION_SECS:-86400}"
MIRROR_LEDGER_RECLAIM="${KIND_ROUND_MIRROR_LEDGER_RECLAIM:-false}"
for boolean_name in CATALOG_CLAIM_ENABLED WAL_MIRROR_ENABLED MIRROR_LEDGER_RECLAIM; do
  boolean_value="${!boolean_name}"
  [[ "$boolean_value" == true || "$boolean_value" == false ]] || {
    printf 'ERROR: %s must be true or false\n' "$boolean_name" >&2
    exit 1
  }
done
[[ "$WAL_MIRROR_ACTIVE_INTERVAL_SECS" =~ ^[0-9]+$ ]] || {
  printf 'ERROR: KIND_ROUND_WAL_MIRROR_ACTIVE_INTERVAL_SECS must be a non-negative integer\n' >&2
  exit 1
}
[[ "$COMMITTED_RETENTION_SECS" =~ ^[0-9]+$ ]] || {
  printf 'ERROR: KIND_ROUND_COMMITTED_RETENTION_SECS must be a non-negative integer\n' >&2
  exit 1
}
case "$MIRROR_RECLAIM_ARM" in
  '') ;;
  off | on)
    for incompatible_name in POSTGRES_OUTAGE_PROBE SCHEMA_ROLLBACK_PROBE \
      INGESTER_POD_LABEL_CAPTURE COMPACTOR_POD_LABEL_CAPTURE; do
      incompatible_value="${!incompatible_name}"
      [[ "$incompatible_value" == 0 ]] || {
        printf 'ERROR: mirror-reclaim qualification cannot run with %s=1\n' \
          "$incompatible_name" >&2
        exit 1
      }
    done
    [[ "$CATALOG_CLAIM_ENABLED" == false ]] || {
      printf 'ERROR: mirror-reclaim qualification requires KIND_ROUND_CATALOG_CLAIM_ENABLED=false\n' >&2
      exit 1
    }
    [[ "$WAL_MIRROR_ENABLED" == true ]] || {
      printf 'ERROR: mirror-reclaim qualification requires KIND_ROUND_WAL_MIRROR_ENABLED=true\n' >&2
      exit 1
    }
    [[ "$WAL_MIRROR_ACTIVE_INTERVAL_SECS" == 0 ]] || {
      printf 'ERROR: mirror-reclaim qualification requires KIND_ROUND_WAL_MIRROR_ACTIVE_INTERVAL_SECS=0\n' >&2
      exit 1
    }
    [[ "$COMMITTED_RETENTION_SECS" == 901 ]] || {
      printf 'ERROR: mirror-reclaim qualification requires KIND_ROUND_COMMITTED_RETENTION_SECS=901\n' >&2
      exit 1
    }
    [[ "$LOAD_SECONDS" == 3600 ]] || {
      printf 'ERROR: mirror-reclaim qualification requires KIND_ROUND_LOAD_SECONDS=3600\n' >&2
      exit 1
    }
    expected_reclaim=false
    [[ "$MIRROR_RECLAIM_ARM" == on ]] && expected_reclaim=true
    [[ "$MIRROR_LEDGER_RECLAIM" == "$expected_reclaim" ]] || {
      printf 'ERROR: mirror-reclaim arm %s requires KIND_ROUND_MIRROR_LEDGER_RECLAIM=%s\n' \
        "$MIRROR_RECLAIM_ARM" "$expected_reclaim" >&2
      exit 1
    }
    ;;
  *)
    printf 'ERROR: KIND_ROUND_MIRROR_RECLAIM_ARM must be off, on, or unset\n' >&2
    exit 1
    ;;
esac
MIRROR_RECLAIM_SAMPLE_SECONDS=60

# Evidence the manager's read_results run collects: `deploy/aws-runner/run.sh`
# rsyncs the snapshot's results/ back off the throwaway box.
RESULTS_DIR="${RESULTS_DIR:-$ROOT/results}"
SCALE_JSON="$RESULTS_DIR/scale-${QUERY_SCALE_BASE}-${QUERY_SCALE_TARGET}-${QUERY_SCALE_BASE}.json"
MEMBERSHIP_LOG="$RESULTS_DIR/membership.log"
# #3647's capture. The four `*-raw|per-series|per-pod|expression.json` files are
# Prometheus' answers verbatim, label sets and all; the graded document embeds
# the same four responses and carries the arithmetic over them.
INGESTER_POD_LABEL_JSON="$RESULTS_DIR/ingester-pod-labels.json"
INGESTER_RAW_JSON="$RESULTS_DIR/ingester-pod-labels-raw.json"
INGESTER_PER_SERIES_JSON="$RESULTS_DIR/ingester-pod-labels-per-series.json"
INGESTER_PER_POD_JSON="$RESULTS_DIR/ingester-pod-labels-per-pod.json"
INGESTER_EXPRESSION_JSON="$RESULTS_DIR/ingester-pod-labels-expression.json"
COMPACTOR_POD_LABEL_JSON="$RESULTS_DIR/compactor-pod-labels.json"
COMPACTOR_RAW_JSON="$RESULTS_DIR/compactor-pod-labels-raw.json"
COMPACTOR_SAMPLE_TIMES_JSON="$RESULTS_DIR/compactor-pod-labels-sample-times.json"
COMPACTOR_PER_POD_JSON="$RESULTS_DIR/compactor-pod-labels-per-pod.json"
COMPACTOR_EXPRESSION_JSON="$RESULTS_DIR/compactor-pod-labels-expression.json"
if [[ -n "$MIRROR_RECLAIM_ARM" ]]; then
  MIRROR_RECLAIM_RESULTS_DIR="$RESULTS_DIR/mirror-reclaim-$MIRROR_RECLAIM_ARM"
  MIRROR_RECLAIM_LAUNCH_JSON="$MIRROR_RECLAIM_RESULTS_DIR/launch.json"
  MIRROR_RECLAIM_CONFIG_JSON="$MIRROR_RECLAIM_RESULTS_DIR/effective-config.json"
  MIRROR_RECLAIM_SERIES_JSONL="$MIRROR_RECLAIM_RESULTS_DIR/measurements.jsonl"
  MIRROR_RECLAIM_LOAD_JSON="$MIRROR_RECLAIM_RESULTS_DIR/load-window.json"
  MIRROR_RECLAIM_ROWS_JSON="$MIRROR_RECLAIM_RESULTS_DIR/row-reconciliation.json"
  MIRROR_RECLAIM_METRICS_START="$MIRROR_RECLAIM_RESULTS_DIR/compactor-metrics-start.prom"
  MIRROR_RECLAIM_METRICS_END="$MIRROR_RECLAIM_RESULTS_DIR/compactor-metrics-end.prom"
fi

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/siglake-kind-round.XXXXXX")"
KIND_CLUSTER_OWNERSHIP_FILE="$TMP_DIR/kind-cluster-owned"
SCALE_SAMPLES_FILE="$TMP_DIR/scale-samples.jsonl"
SCALE_TRANSITIONS_FILE="$TMP_DIR/scale-transitions.jsonl"
SCALE_FAILURES_FILE="$TMP_DIR/scale-failures"
GROUPBY_FILE="$TMP_DIR/scale-group-by.json"
ACTIVE_PF_PID=

log() { printf '==> %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

dump_section() { printf -- '--- %s\n' "$*"; }

initial_load_description() {
  local inline_group_count_ceiling=4096 relation='at or below'
  ((LOAD_EVENTS > inline_group_count_ceiling)) && relation=above
  printf 'ingest %s events with %s distinct hosts (%s the %s inline group-count ceiling)' \
    "$LOAD_EVENTS" "$LOAD_EVENTS" "$relation" "$inline_group_count_ceiling"
}

# Is $1 one of the remaining arguments, compared whole? Deliberately not
# `printf '%s\n' "${array[@]}" | grep -qx`: `grep -q` exits at its first match,
# so the write into the pipe can lose the race and die of SIGPIPE, which
# `pipefail` then reports as a FAILED match. Negated at the call site below that
# reads as "this pod's job was not dumped above" and the diagnostics print a
# second copy of the pod's logs. Same hazard as the one
# scripts/check-kind-round-scale.sh:50 documents, where it made that guard red
# on a good tree in ~7% of runs.
array_contains() {
  local needle=$1 element
  shift
  for element in "$@"; do
    [[ "$element" == "$needle" ]] && return 0
  done
  return 1
}

# Cluster state for the round log, printed before the cluster is deleted so a
# round is readable from its log alone. Kind round #2 lost the failed
# migrate-schema hook's stderr to the immediate teardown and its root cause had
# to be inferred from the chart render. Every command here is best-effort: the
# dump must never change the status cleanup exits with.
#
# On every exit: pods with restart counts and BackOff events, which is what the
# playbook's restart check reads. On a failed exit, additionally: the release
# namespace's Jobs and events, describe + logs of every migrate-schema or
# failed Job, and describe + logs of every pod that is not Ready.
dump_cluster_state() {
  local status=$1
  local -a kc=(kubectl --context "$KUBE_CONTEXT" --request-timeout=30s)

  if ! "${kc[@]}" get --raw /readyz >/dev/null 2>&1; then
    log "kind: cluster ${CLUSTER_NAME} is not reachable, skipping the pre-teardown state dump"
    return 0
  fi

  log "kind: pods before teardown"
  printf 'PODS_BEFORE_TEARDOWN_BEGIN status=%s\n' "$status"
  "${kc[@]}" get pods -A -o wide || true
  printf 'PODS_BEFORE_TEARDOWN_END\n'
  printf 'POD_RESTARTS_BEGIN\n'
  "${kc[@]}" get pods -A \
    -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,RESTARTS:.status.containerStatuses[*].restartCount' || true
  printf 'POD_RESTARTS_END\n'
  printf 'BACKOFF_EVENTS_BEGIN\n'
  "${kc[@]}" get events -A --field-selector reason=BackOff || true
  printf 'BACKOFF_EVENTS_END\n'

  local -a restarted_containers=()
  mapfile -t restarted_containers < <(
    "${kc[@]}" get pods -A -o json 2>/dev/null | python3 -c '
import json, sys
try:
    items = json.load(sys.stdin).get("items", [])
except ValueError:
    raise SystemExit(0)
for item in items:
    meta = item["metadata"]
    status = item.get("status", {})
    for field in ("initContainerStatuses", "containerStatuses"):
        for container in status.get(field, []):
            restarts = container.get("restartCount", 0)
            if restarts > 0:
                print(
                    meta["namespace"],
                    meta["name"],
                    container["name"],
                    restarts,
                    sep="\t",
                )
'
  ) || true
  local restarted_container ns pod container restarts
  for restarted_container in "${restarted_containers[@]}"; do
    IFS=$'\t' read -r ns pod container restarts <<<"$restarted_container"
    printf 'PREVIOUS_LOGS_BEGIN ns=%s pod=%s container=%s restarts=%s\n' \
      "$ns" "$pod" "$container" "$restarts"
    "${kc[@]}" -n "$ns" logs "$pod" --previous -c "$container" --tail=60 || true
    printf 'PREVIOUS_LOGS_END\n'
  done

  [[ "$status" -ne 0 ]] || return 0

  log "kind: failure diagnostics before teardown"
  printf 'FAILURE_DIAGNOSTICS_BEGIN status=%s\n' "$status"
  dump_section "jobs in ${NAMESPACE}"
  "${kc[@]}" -n "$NAMESPACE" get jobs -o wide || true
  dump_section "events in ${NAMESPACE}"
  "${kc[@]}" -n "$NAMESPACE" get events --sort-by=.lastTimestamp || true

  # Every migrate-schema hook Job (a failed pre-upgrade hook is left in place
  # by before-hook-creation) plus any other Job with a failed pod.
  local -a jobs=()
  mapfile -t jobs < <(
    {
      "${kc[@]}" -n "$NAMESPACE" get jobs -o name \
        -l app.kubernetes.io/component=migrate-schema
      "${kc[@]}" -n "$NAMESPACE" get jobs \
        -o jsonpath='{range .items[?(@.status.failed>0)]}job.batch/{.metadata.name}{"\n"}{end}'
    } 2>/dev/null | sed -n 's#^job\.batch/##p' | sort -u
  ) || true
  local job
  for job in "${jobs[@]}"; do
    dump_section "describe job ${NAMESPACE}/${job}"
    "${kc[@]}" -n "$NAMESPACE" describe job "$job" || true
    dump_section "logs job ${NAMESPACE}/${job}"
    "${kc[@]}" -n "$NAMESPACE" logs --all-containers --prefix --tail=200 \
      -l "job-name=${job}" || true
  done

  # Pods that are not Ready, skipping completed ones. Pods of a Job dumped
  # above keep their describe (exit code, OOMKilled) but not a second log copy.
  local -a pods=()
  mapfile -t pods < <("${kc[@]}" get pods -A -o json 2>/dev/null | python3 -c '
import json, sys
try:
    items = json.load(sys.stdin).get("items", [])
except ValueError:
    raise SystemExit(0)
for item in items:
    status = item.get("status", {})
    if status.get("phase") == "Succeeded":
        continue
    conditions = {c.get("type"): c.get("status") for c in status.get("conditions", [])}
    if conditions.get("Ready") == "True":
        continue
    meta = item["metadata"]
    job = meta.get("labels", {}).get("job-name", "")
    print(meta["namespace"], meta["name"], job or "-", sep="\t")
') || true
  local pod ns name pod_job
  for pod in "${pods[@]}"; do
    IFS=$'\t' read -r ns name pod_job <<<"$pod"
    dump_section "describe pod ${ns}/${name}"
    "${kc[@]}" -n "$ns" describe pod "$name" || true
    if [[ "$pod_job" == "-" ]] || ! array_contains "$pod_job" "${jobs[@]}"; then
      dump_section "logs pod ${ns}/${name}"
      "${kc[@]}" -n "$ns" logs --all-containers --prefix --tail=100 "$name" || true
    fi
  done
  printf 'FAILURE_DIAGNOSTICS_END\n'
}

cleanup() {
  local status=$? owned_control_plane_id
  trap - EXIT INT TERM
  if [[ -n "$ACTIVE_PF_PID" ]]; then
    kill "$ACTIVE_PF_PID" 2>/dev/null || true
    wait "$ACTIVE_PF_PID" 2>/dev/null || true
  fi
  dump_cluster_state "$status" || true
  if [[ "${KEEP:-0}" == 1 ]]; then
    log "KEEP=1: leaving kind cluster ${CLUSTER_NAME} running"
  elif [[ ! -f "$KIND_CLUSTER_OWNERSHIP_FILE" ]]; then
    log "kind: this round did not create cluster ${CLUSTER_NAME}, skipping teardown"
  elif ! read -r owned_control_plane_id <"$KIND_CLUSTER_OWNERSHIP_FILE" || \
    [[ -z "$owned_control_plane_id" ]]; then
    log "kind: ownership marker for cluster ${CLUSTER_NAME} is invalid, refusing teardown"
    [[ "$status" -ne 0 ]] || status=1
  elif ! KIND_CLUSTER_NAME="$CLUSTER_NAME" \
    KIND_EXPECTED_CONTROL_PLANE_ID="$owned_control_plane_id" \
    "$ROOT/scripts/kind-down.sh"; then
    [[ "$status" -ne 0 ]] || status=1
  fi
  rm -rf -- "$TMP_DIR"
  exit "$status"
}
trap cleanup EXIT INT TERM

for tool in curl helm kind kubectl python3; do
  command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool"
done

wait_for_forward() {
  local pid=$1 log_file=$2
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || {
      sed 's/^/  /' "$log_file" >&2
      die "kubectl port-forward exited before becoming ready"
    }
    grep -q 'Forwarding from' "$log_file" && return 0
    sleep 1
  done
  die "kubectl port-forward did not become ready"
}

scrape_preregistered_zeros() {
  local role=$1 remote_port=$2 local_port=$3 pod pf_pid metrics_file pf_log
  pod="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pod \
    -l "app.kubernetes.io/instance=siglake,app.kubernetes.io/component=${role}" \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[-1].metadata.name}')"
  [[ -n "$pod" ]] || die "no ${role} pod found for pre-load metrics scrape"
  metrics_file="$TMP_DIR/${role}.metrics"
  pf_log="$TMP_DIR/${role}.port-forward.log"
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" port-forward \
    "pod/$pod" "${local_port}:${remote_port}" >"$pf_log" 2>&1 &
  pf_pid=$!
  ACTIVE_PF_PID=$pf_pid
  wait_for_forward "$pf_pid" "$pf_log"

  # This is deliberately the pod's first and only evidence scrape in the
  # round, before any load can increment an alerted counter.
  curl -fsS "http://127.0.0.1:${local_port}/metrics" >"$metrics_file"
  kill "$pf_pid" 2>/dev/null || true
  wait "$pf_pid" 2>/dev/null || true
  ACTIVE_PF_PID=

  python3 - "$role" "$pod" "$metrics_file" <<'PY'
import json
import re
import sys

role, pod, path = sys.argv[1:]
expected = {
    "ingester": [
        ("siglake_ingest_lane_refused_total", {}),
        ("siglake_ingest_tenant_denied_total", {"reason": "not_allowed"}),
        ("siglake_ingest_tenant_denied_total", {"reason": "claim_missing"}),
        ("siglake_ingest_tenant_denied_total", {"reason": "claim_invalid"}),
        ("siglake_ingest_tenant_denied_total", {"reason": "header_mismatch"}),
        ("siglake_wal_mirror_register_abandoned_total", {}),
        ("siglake_wal_mirror_upload_abandoned_total", {}),
        ("siglake_wal_crc_mismatch_total", {}),
        ("siglake_wal_ipc_framing_refused_total", {}),
        ("siglake_wal_partials_adopted_total", {}),
    ],
    "compactor": [
        ("siglake_compactor_reclaim_unprovable_total", {}),
        ("siglake_compactor_watchdog_trips_total", {"stage": "expire"}),
        ("siglake_compactor_watchdog_trips_total", {"stage": "drain"}),
        ("siglake_compactor_watchdog_trips_total", {"stage": "agg_fold"}),
        ("siglake_compactor_watchdog_trips_total", {"stage": "recluster"}),
        ("siglake_compactor_watchdog_trips_total", {"stage": "delete_tasks"}),
        (
            "siglake_group_count_delta_write_failures_total",
            {"iceberg_namespace": "siglake", "table": "events"},
        ),
        (
            "siglake_side_aggregate_publish_failures_total",
            {"iceberg_namespace": "siglake", "table": "events"},
        ),
        ("siglake_wal_crc_mismatch_total", {}),
        ("siglake_wal_ipc_framing_refused_total", {}),
    ],
}

series = []
line_re = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+([^\s]+)$')
label_re = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')
for raw in open(path, encoding="utf-8"):
    raw = raw.strip()
    if not raw or raw.startswith("#"):
        continue
    match = line_re.match(raw)
    if not match:
        continue
    labels = {
        key: json.loads(f'"{value}"')
        for key, value in label_re.findall(match.group(2) or "")
    }
    series.append((match.group(1), labels, match.group(3), raw))

print(f"PREREGISTERED_ZERO_BEGIN role={role} pod={pod}")
missing = []
for metric, labels in expected[role]:
    found = [item for item in series if item[0] == metric and item[1] == labels]
    if len(found) != 1:
        missing.append(f"{metric}{labels}: expected one series, found {len(found)}")
        continue
    try:
        value = float(found[0][2])
    except ValueError:
        missing.append(f"{found[0][3]}: value is not numeric")
        continue
    if value != 0.0:
        missing.append(f"{found[0][3]}: expected pre-load zero")
        continue
    print(f"PREREGISTERED_ZERO role={role} {found[0][3]}")
print(f"PREREGISTERED_ZERO_END role={role}")
if missing:
    print("pre-registration evidence failed:", file=sys.stderr)
    for problem in missing:
        print(f"  {problem}", file=sys.stderr)
    raise SystemExit(1)
PY
}

ingest_events() {
  local start=$1 count=$2 payload_file="$TMP_DIR/otlp-payload.json"
  python3 - "$start" "$count" >"$payload_file" <<'PY'
import json
import sys

start, count = map(int, sys.argv[1:])
resource_logs = []
for i in range(start, start + count):
    terms = [f"kind-round event={i}"]
    if i % 2 == 0:
        terms.append("error")
    if i % 7 == 0:
        terms.append("quantum entanglement cascade")
    if i % 10 == 0:
        terms.append("zugzwang0")
    if i % 100 == 0:
        terms.append("zugzwang2")
    if i % 1000 == 0:
        terms.append("zugzwang4")
    if i % 11 == 0:
        terms.append("xqzfrag")
    resource_logs.append({
        "resource": {"attributes": [
            {"key": "host.name", "value": {"stringValue": f"host-{i:08d}"}},
            {"key": "service.name", "value": {"stringValue": "kind-round"}},
        ]},
        "scopeLogs": [{
            "scope": {"name": "kind-round"},
            "logRecords": [{
                "body": {"stringValue": " ".join(terms)},
                "attributes": [
                    {"key": "sourcetype", "value": {"stringValue": "kind:json"}},
                    {"key": "index", "value": {"stringValue": "main"}},
                ],
            }],
        }],
    })
json.dump({"resourceLogs": resource_logs}, sys.stdout, separators=(",", ":"))
PY
  post_json_file 'http://127.0.0.1:8088/v1/logs' "$payload_file" \
    -H 'X-Scope-OrgID: default' \
    >/dev/null
}

sql_payload() {
  python3 -c 'import json,sys; print(json.dumps({"query": sys.argv[1]}))' "$1"
}

run_sql() {
  run_sql_at 'http://127.0.0.1:8089/api/v1/sql' "$1"
}

run_sql_at() {
  local url=$1 sql=$2 payload_file="$TMP_DIR/sql-payload.json"
  sql_payload "$sql" >"$payload_file"
  post_json_file "$url" "$payload_file" \
    -H 'X-Scope-OrgID: default'
}

start_query_pod_forward() {
  local pod=$1 local_port=$2 remote_port=$3 purpose=$4
  local pf_log="$TMP_DIR/${pod}.${purpose}.port-forward.log"
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" port-forward \
    "pod/$pod" "${local_port}:${remote_port}" >"$pf_log" 2>&1 &
  ACTIVE_PF_PID=$!
  wait_for_forward "$ACTIVE_PF_PID" "$pf_log"
}

stop_active_forward() {
  [[ -n "$ACTIVE_PF_PID" ]] || return 0
  kill "$ACTIVE_PF_PID" 2>/dev/null || true
  wait "$ACTIVE_PF_PID" 2>/dev/null || true
  ACTIVE_PF_PID=
}

LAST_QUERY_COUNT=0
wait_for_query_pod_count() {
  local pod=$1 expected=$2 response count=0
  start_query_pod_forward "$pod" "$QUERY_LOCAL_PORT" 8089 query
  for attempt in $(seq 1 90); do
    response="$(run_sql_at \
      "http://127.0.0.1:${QUERY_LOCAL_PORT}/api/v1/sql/local" \
      'SELECT count(*) AS n FROM events')"
    count="$(printf '%s' "$response" |
      python3 -c 'import json,sys; print(json.load(sys.stdin)["rows"][0]["n"])')"
    if [[ "$count" =~ ^[0-9]+$ ]] && ((count >= expected)); then
      stop_active_forward
      LAST_QUERY_COUNT=$count
      log "  pod=${pod} attempt=${attempt} count=${count}"
      return 0
    fi
    sleep 2
  done
  stop_active_forward
  die "expected at least ${expected} queryable rows on ${pod}, saw ${count}"
}

QUERY_PINNED_TOTAL=0
query_pinned_total() {
  local pod value total=0
  # The Service can pick either coordinator, so the peer is not knowable in
  # advance. Sum the worker-side pinned counter across every query pod.
  for pod in "${QUERY_PODS[@]}"; do
    start_query_pod_forward "$pod" "$QUERY_METRICS_LOCAL_PORT" 9105 metrics
    value="$(curl -fsS "http://127.0.0.1:${QUERY_METRICS_LOCAL_PORT}/metrics" |
      awk 'BEGIN { value=0 }
        /^siglake_query_shard_pin_total\{/ && /outcome="pinned"/ { value += $2 }
        END { printf "%.0f", value }')"
    stop_active_forward
    [[ "$value" =~ ^[0-9]+$ ]] || die "non-integer pinned-shard count for ${pod}: ${value}"
    total=$((total + value))
  done
  QUERY_PINNED_TOTAL=$total
}

iso_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# --- #5556: the source revision a capture pins -------------------------------
#
# `git rev-parse HEAD` is the right answer only when the round runs in the
# repository the source came from. The aws-runner rsyncs a snapshot without
# `.git` and commits it fresh on the box, so HEAD there names a commit that
# resolves in no repository -- run 82 retained
# 0653a6824db17b08b19bf8c2847e6326a40e2eda for snapshot 5f3a51489c46. A launcher
# that knows the real revision exports it in SIGLAKE_SOURCE_COMMIT and the
# captures prefer it. Which of the two answered is recorded beside the commit,
# so a reader never has to guess whether a SHA is a checkout or an injection.
SIGLAKE_SOURCE_COMMIT="${SIGLAKE_SOURCE_COMMIT:-}"
source_commit() {
  if [[ -n "$SIGLAKE_SOURCE_COMMIT" ]]; then
    printf '%s\n' "$SIGLAKE_SOURCE_COMMIT"
  else
    git -C "$ROOT" rev-parse HEAD 2>/dev/null || true
  fi
}
source_commit_origin() {
  if [[ -n "$SIGLAKE_SOURCE_COMMIT" ]]; then
    printf 'siglake_source_commit_env\n'
  else
    printf 'git_rev_parse_head\n'
  fi
}

# --- #1838: 2 → 4 → 2 query scaling under load -------------------------------
#
# Six kind rounds read #968 "unverified" because this script pinned the query
# tier at two replicas and the chart refused a wider KEDA range. #967 (3e5d481)
# lifted the render-time ceiling and gave the pods runtime SRV membership, so
# the round can now drive a real scale event and ask the question the card is
# actually about: while membership moves, does a transparent `/api/v1/sql`
# cross-shard `GROUP BY` still account for every row, and does a pod that
# joined take shard work?
#
# The step is interleaved with the workload loop (`advance_query_scale` is
# called once per pass and never blocks) rather than run beside it: a
# background load generator would race this script's shared payload files, and
# a separate quiet phase would prove exactness at rest, which is not the claim.
# Only the three evidence samples pause the mix, and they pause ingest with it,
# so `sum(n) == count(*)` is a statement about one settled table state.
#
# Every failure here is deferred like PANEL_FAILURES rather than fatal: the
# round's remaining evidence (panels, ScaledObject health, the teardown dump)
# is worth more than an early exit, and the numbers that failed are in the log
# and in the JSON either way.
SCALE_FAILURES=0
# The one clock the whole step obeys, set once the load window starts. Without
# it a sample's own retries could outlast the grace period the window checks,
# and the round would grow by more than the step is allowed to cost.
SCALE_HARD_DEADLINE=0
scale_time_left() { ((SCALE_HARD_DEADLINE == 0 || SECONDS < SCALE_HARD_DEADLINE)); }

scale_failure() {
  SCALE_FAILURES=1
  printf 'SCALE_FAILURE %s\n' "$*"
  printf '%s\n' "$*" >>"$SCALE_FAILURES_FILE"
}

init_scale_evidence() {
  mkdir -p "$RESULTS_DIR"
  : >"$SCALE_SAMPLES_FILE"
  : >"$SCALE_TRANSITIONS_FILE"
  : >"$SCALE_FAILURES_FILE"
  : >"$MEMBERSHIP_LOG"
  printf 'SCALE_EVIDENCE json=%s membership=%s\n' \
    "${SCALE_JSON#"$ROOT/"}" "${MEMBERSHIP_LOG#"$ROOT/"}"
}

# Ready, non-terminating query pods in ordinal order. Readiness is exactly the
# gate DNS uses since #967 (the headless Service no longer publishes not-ready
# addresses), so this is the membership the coordinators are converging on.
ready_query_pods() {
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods \
    -l 'app.kubernetes.io/instance=siglake,app.kubernetes.io/component=query' \
    --sort-by=.metadata.name -o json 2>/dev/null | python3 -c '
import json, sys
try:
    items = json.load(sys.stdin).get("items", [])
except ValueError:
    raise SystemExit(0)
for item in items:
    meta, status = item["metadata"], item.get("status", {})
    if meta.get("deletionTimestamp"):
        continue
    conditions = {c.get("type"): c.get("status") for c in status.get("conditions", [])}
    if conditions.get("Ready") == "True":
        print(meta["name"])
'
}

ready_query_pod_count() {
  ready_query_pods | grep -c . || true
}

# One pod's own view of the table through /sql/local (no fan-out). A global
# rather than a return value: the port-forward bookkeeping has to happen in
# this shell so cleanup can still see ACTIVE_PF_PID.
POD_ROW_COUNT=
read_pod_row_count() {
  local pod=$1 response
  POD_ROW_COUNT=
  start_query_pod_forward "$pod" "$QUERY_LOCAL_PORT" 8089 scale
  response="$(run_sql_at \
    "http://127.0.0.1:${QUERY_LOCAL_PORT}/api/v1/sql/local" \
    'SELECT count(*) AS n FROM events')" || {
    stop_active_forward
    return 1
  }
  stop_active_forward
  POD_ROW_COUNT="$(printf '%s' "$response" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["rows"][0]["n"])')"
  [[ "$POD_ROW_COUNT" =~ ^[0-9]+$ ]]
}

# The row count every ready pod agrees on. Ingest is paused for the duration
# (the caller is the ingest loop), so disagreement here is table-cache lag on a
# pod that just joined, not new rows arriving.
CONVERGED_ROWS=0
converge_query_rows() {
  local attempt pod agreed
  for attempt in $(seq 1 10); do
    agreed=
    for pod in "$@"; do
      if ! read_pod_row_count "$pod"; then
        agreed=
        break
      fi
      if [[ -z "$agreed" ]]; then
        agreed=$POD_ROW_COUNT
      elif [[ "$POD_ROW_COUNT" != "$agreed" ]]; then
        agreed=
        break
      fi
    done
    if [[ -n "$agreed" ]]; then
      CONVERGED_ROWS=$agreed
      return 0
    fi
    scale_time_left || return 1
    sleep 5
  done
  return 1
}

# One `pod<TAB>pinned_shards<TAB>published_members<TAB>membership_changes` line
# per pod. The first is the worker-side signal that this pod answered shard
# requests; the other two are its own published membership, which is what "the
# membership each pod saw" means for a pod that is not the coordinator.
QUERY_POD_METRICS=
read_query_pod_metrics() {
  local pod out= metrics_file
  for pod in "$@"; do
    metrics_file="$TMP_DIR/${pod}.scale.metrics"
    start_query_pod_forward "$pod" "$QUERY_METRICS_LOCAL_PORT" 9105 scale-metrics
    if ! curl -fsS "http://127.0.0.1:${QUERY_METRICS_LOCAL_PORT}/metrics" \
      >"$metrics_file"; then
      stop_active_forward
      return 1
    fi
    stop_active_forward
    out+="$(awk -v pod="$pod" '
      /^siglake_query_shard_pin_total\{/ && /outcome="pinned"/ { pinned += $NF }
      /^siglake_query_peer_discovery_members[ {]/ { members = $NF }
      /^siglake_query_peer_discovery_refresh_total\{/ && /outcome="changed"/ { changed += $NF }
      END { printf "%s\t%.0f\t%.0f\t%.0f", pod, pinned, members, changed }
    ' "$metrics_file")"$'\n'
  done
  QUERY_POD_METRICS="$out"
}

# Wait until every pod has published the membership we are about to assert
# about. A pod that has not refreshed yet is a five-second artefact of the
# discovery poll interval, not a defect, and failing on it would make the step
# flaky in exactly the way that teaches a reader to ignore it. Leaves the
# accepted scrape in QUERY_POD_METRICS.
converge_published_members() {
  local expect=$1 attempt pod size settled
  shift
  for attempt in $(seq 1 12); do
    if read_query_pod_metrics "$@"; then
      settled=1
      while IFS=$'\t' read -r pod _ size _; do
        [[ -n "$pod" ]] || continue
        [[ "$size" == "$expect" ]] || settled=0
      done <<<"$QUERY_POD_METRICS"
      ((settled == 0)) || return 0
    fi
    scale_time_left || return 1
    sleep 5
  done
  return 1
}

# The transparent cross-shard GROUP BY, saved for the record. Not a result-cache
# hit however often it repeats: the response carries one row per host and
# SQL_RESULT_CACHE_MAX_ROWS is 128, so a wide GROUP BY is never stored.
GROUPBY_SUM=
GROUPBY_PEERS=
read_group_by_sample() {
  local response parsed
  GROUPBY_SUM=
  GROUPBY_PEERS=
  response="$(run_sql 'SELECT host, sum(1) AS n FROM events GROUP BY host')" || return 1
  printf '%s' "$response" >"$GROUPBY_FILE"
  parsed="$(python3 -c '
import json, sys
response = json.load(open(sys.argv[1], encoding="utf-8"))
dist = (response.get("stats") or {}).get("phases", {}).get("distributed") or {}
print(sum(int(row["n"]) for row in response["rows"]), dist.get("peers", 0), sep="\t")
' "$GROUPBY_FILE")" || return 1
  IFS=$'\t' read -r GROUPBY_SUM GROUPBY_PEERS <<<"$parsed"
  [[ "$GROUPBY_SUM" =~ ^[0-9]+$ && "$GROUPBY_PEERS" =~ ^[0-9]+$ ]]
}

# What DNS could answer (the headless Service's ready endpoints) beside what
# each pod actually published (its own "membership changed" lines).
record_membership_evidence() {
  local phase=$1 pod
  shift
  {
    printf '=== phase=%s at=%s ready_pods=%s\n' "$phase" "$(iso_now)" "$*"
    printf -- '--- ready endpoints of %s\n' "$QUERY_HEADLESS_SERVICE"
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get endpointslices \
      -l "kubernetes.io/service-name=${QUERY_HEADLESS_SERVICE}" -o json 2>/dev/null |
      python3 -c '
import json, sys
try:
    items = json.load(sys.stdin).get("items", [])
except ValueError:
    raise SystemExit(0)
for item in items:
    for endpoint in item.get("endpoints", []):
        print("endpoint pod=%s hostname=%s ready=%s addresses=%s" % (
            endpoint.get("targetRef", {}).get("name", "-"),
            endpoint.get("hostname", "-"),
            endpoint.get("conditions", {}).get("ready"),
            ",".join(endpoint.get("addresses", [])),
        ))
' || true
    for pod in "$@"; do
      printf -- '--- %s published membership\n' "$pod"
      kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" logs "$pod" \
        -c query-server 2>/dev/null | grep 'query peer discovery' ||
        printf 'no peer-discovery log lines\n'
    done
  } >>"$MEMBERSHIP_LOG"
}

# Move the tier's floor. Only minReplicaCount is patched: raising it is a hard
# clamp the HPA acts on at its next sync, and putting it back leaves the
# installed base → target range in place, so the round's ScaledObject evidence
# still shows the headroom that made the scale event possible. Coming back down
# is then KEDA's own decision from its triggers, not a forced ceiling.
patch_query_floor() {
  local min=$1
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" patch \
    "scaledobject/${QUERY_SCALEDOBJECT}" --type=merge \
    -p "{\"spec\":{\"minReplicaCount\":${min}}}" >/dev/null
}

record_transition() {
  local from=$1 to=$2 requested=$3
  python3 - "$from" "$to" "$requested" "$(iso_now)" "$SCALE_TRANSITIONS_FILE" <<'PY'
import datetime
import json
import sys

from_replicas, to_replicas, requested, observed, out_path = sys.argv[1:]


def parse(stamp):
    return datetime.datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ")


record = {
    "from": int(from_replicas),
    "to": int(to_replicas),
    "requested_at": requested,
    "observed_at": observed,
    "seconds": int((parse(observed) - parse(requested)).total_seconds()),
}
with open(out_path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\n")
print(
    "SCALE_TRANSITION from={from} to={to} requested_at={requested_at} "
    "observed_at={observed_at} seconds={seconds}".format(**record)
)
PY
}

# One before/during/after sample: the row count every pod agrees on, the
# membership every pod published, the cross-shard GROUP BY against that row
# count, and each pod's shard work across the query.
sample_query_scale() {
  local phase=$1 expect_members=$2
  local -a pods=()
  local at attempt pair before_metrics
  mapfile -t pods < <(ready_query_pods)
  at="$(iso_now)"
  if ((${#pods[@]} != expect_members)); then
    scale_failure "phase=${phase}: expected ${expect_members} ready query pods, found ${#pods[@]}"
    return 1
  fi
  # Never let a previous phase's answer stand in for this one's.
  rm -f "$GROUPBY_FILE"
  GROUPBY_SUM=
  for pair in 1 2 3; do
    if ! converge_query_rows "${pods[@]}"; then
      scale_failure "phase=${phase}: query pods did not agree on a row count"
      return 1
    fi
    if ! converge_published_members "$expect_members" "${pods[@]}"; then
      scale_failure "phase=${phase}: not every pod published a ${expect_members}-member snapshot"
      return 1
    fi
    before_metrics="$QUERY_POD_METRICS"
    # The Service hands the request to any pod, so an answer from a coordinator
    # that has not refreshed yet is expected. It is exact either way; the retry
    # is about the shard PLACEMENT claim, which is only interpretable against
    # the membership the answer pinned.
    for attempt in $(seq 1 24); do
      if read_group_by_sample && [[ "$GROUPBY_PEERS" == "$expect_members" ]]; then
        break
      fi
      scale_time_left || break
      sleep 5
    done
    # A row count from one pod and a fan-out sum from another can straddle a
    # commit. Re-sample the pair before believing a mismatch; a persistent one
    # is recorded by the writer below and fails the round.
    if [[ "$GROUPBY_SUM" == "$CONVERGED_ROWS" ]]; then
      break
    fi
    scale_time_left || break
    log "  phase=${phase}: sum=${GROUPBY_SUM} row_count=${CONVERGED_ROWS}, re-sampling"
    sleep 5
  done
  if [[ -z "$GROUPBY_SUM" ]]; then
    scale_failure "phase=${phase}: the cross-shard GROUP BY never answered"
    return 1
  fi
  if ! read_query_pod_metrics "${pods[@]}"; then
    scale_failure "phase=${phase}: could not scrape a query pod's metrics"
    return 1
  fi
  record_membership_evidence "$phase" "${pods[@]}"
  if ! python3 - "$phase" "$at" "$expect_members" "$CONVERGED_ROWS" \
    "$before_metrics" "$QUERY_POD_METRICS" "$GROUPBY_FILE" \
    "$SCALE_SAMPLES_FILE" <<'PY'; then
import json
import sys

(
    phase,
    at,
    expect_members,
    row_count,
    before_block,
    after_block,
    response_path,
    out_path,
) = sys.argv[1:]
expect_members = int(expect_members)
row_count = int(row_count)
with open(response_path, encoding="utf-8") as handle:
    response = json.load(handle)
rows = response["rows"]
dist = (response.get("stats") or {}).get("phases", {}).get("distributed") or {}
grouped = sum(int(row["n"]) for row in rows)
mode = dist.get("mode")
peers = int(dist.get("peers", 0))


def parse(block):
    parsed = {}
    for line in block.splitlines():
        if not line.strip():
            continue
        pod, pinned, members, changed = line.split("\t")
        parsed[pod] = (int(pinned), int(members), int(changed))
    return parsed


before, after = parse(before_block), parse(after_block)
per_pod = []
for pod in sorted(after):
    pinned, members, changed = after[pod]
    per_pod.append(
        {
            "pod": pod,
            "pinned_shards_total": pinned,
            "pinned_shards_delta": pinned - before.get(pod, (0, 0, 0))[0],
            "published_members": members,
            "membership_changes": changed,
        }
    )

problems = []
if grouped != row_count:
    problems.append(f"GROUP BY sum {grouped} != count(*) {row_count}")
if response.get("truncated"):
    problems.append(f"the GROUP BY result truncated at {response.get('max_rows')} rows")
if response.get("approximation") is not None:
    problems.append("the GROUP BY answer is approximate, so it cannot settle exactness")
if expect_members > 1:
    if mode not in {"aggregate", "ordered_aggregate"}:
        problems.append(f"GROUP BY did not fan out: distributed mode={mode!r}")
    if peers != expect_members:
        problems.append(
            f"the answer pinned {peers} peers, not the {expect_members} ready pods"
        )
sizes = sorted({pod["published_members"] for pod in per_pod})
if sizes != [expect_members]:
    problems.append(f"published membership sizes {sizes} != [{expect_members}]")

sample = {
    "phase": phase,
    "at": at,
    "ready_pods": sorted(after),
    "row_count": row_count,
    "cross_shard_group_by": {
        "groups": len(rows),
        "sum": grouped,
        "row_count": row_count,
        "exact": grouped == row_count,
        "mode": mode,
        "peers": peers,
        "peer_generation": int(dist.get("peer_generation", 0)),
        "shards_answered": len(dist.get("shard_wall_micros") or []),
    },
    "per_pod": per_pod,
    "problems": problems,
}
with open(out_path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(sample) + "\n")
print(
    f"SCALE_SAMPLE phase={phase} at={at} ready_pods={len(per_pod)} "
    f"row_count={row_count} groups={len(rows)} group_by_sum={grouped} "
    f"exact={'ok' if grouped == row_count else 'FAILED'} mode={mode} "
    f"peers={peers} peer_generation={sample['cross_shard_group_by']['peer_generation']}"
)
for pod in per_pod:
    print(
        "SCALE_POD phase={phase} pod={pod} pinned_shards_delta={pinned_shards_delta} "
        "pinned_shards_total={pinned_shards_total} members={published_members} "
        "membership_changes={membership_changes}".format(phase=phase, **pod)
    )
for problem in problems:
    print(f"  {problem}", file=sys.stderr)
raise SystemExit(1 if problems else 0)
PY
    scale_failure "phase=${phase}: the evidence sample did not hold"
    return 1
  fi
}

# One non-blocking step of the scale state machine, called once per pass of the
# workload loop.
SCALE_PHASE=before
SCALE_REQUESTED_AT=
advance_query_scale() {
  case "$SCALE_PHASE" in
  before)
    sample_query_scale before "$QUERY_SCALE_BASE" || true
    log "scale the query tier ${QUERY_SCALE_BASE} -> ${QUERY_SCALE_TARGET} under load"
    SCALE_REQUESTED_AT="$(iso_now)"
    patch_query_floor "$QUERY_SCALE_TARGET" ||
      scale_failure "could not raise the query tier's floor to ${QUERY_SCALE_TARGET}"
    SCALE_PHASE=up
    ;;
  up)
    [[ "$(ready_query_pod_count)" == "$QUERY_SCALE_TARGET" ]] || return 0
    record_transition "$QUERY_SCALE_BASE" "$QUERY_SCALE_TARGET" "$SCALE_REQUESTED_AT"
    sample_query_scale during "$QUERY_SCALE_TARGET" || true
    log "scale the query tier ${QUERY_SCALE_TARGET} -> ${QUERY_SCALE_BASE} under load"
    SCALE_REQUESTED_AT="$(iso_now)"
    patch_query_floor "$QUERY_SCALE_BASE" ||
      scale_failure "could not lower the query tier's floor to ${QUERY_SCALE_BASE}"
    SCALE_PHASE=down
    ;;
  down)
    [[ "$(ready_query_pod_count)" == "$QUERY_SCALE_BASE" ]] || return 0
    record_transition "$QUERY_SCALE_TARGET" "$QUERY_SCALE_BASE" "$SCALE_REQUESTED_AT"
    sample_query_scale after "$QUERY_SCALE_BASE" || true
    SCALE_PHASE=done
    ;;
  esac
}

write_scale_evidence() {
  python3 - "$SCALE_SAMPLES_FILE" "$SCALE_TRANSITIONS_FILE" "$SCALE_FAILURES_FILE" \
    "$SCALE_JSON" "$QUERY_SCALE_BASE" "$QUERY_SCALE_TARGET" "$(iso_now)" <<'PY'
import json
import sys

samples_path, transitions_path, failures_path, out_path, base, target, generated_at = (
    sys.argv[1:]
)
base, target = int(base), int(target)


def load_json_lines(path):
    try:
        with open(path, encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]
    except FileNotFoundError:
        return []


def load_lines(path):
    try:
        with open(path, encoding="utf-8") as handle:
            return [line.strip() for line in handle if line.strip()]
    except FileNotFoundError:
        return []


samples = load_json_lines(samples_path)
transitions = load_json_lines(transitions_path)
failures = load_lines(failures_path)

phases = [sample["phase"] for sample in samples]
if phases != ["before", "during", "after"]:
    failures.append(f"expected before/during/after samples, recorded {phases}")
legs = [(transition["from"], transition["to"]) for transition in transitions]
if legs != [(base, target), (target, base)]:
    failures.append(f"expected the {base}->{target}->{base} pair, recorded {legs}")

# The claim scale-out exists for: a pod that was not in the tier at the
# "before" sample answered a pinned shard while it was.
before_pods = set(samples[0]["ready_pods"]) if samples else set()
during = next((sample for sample in samples if sample["phase"] == "during"), None)
if during is not None:
    joined = [pod for pod in during["per_pod"] if pod["pod"] not in before_pods]
    if not joined:
        failures.append("the during-scale sample saw no pod that had joined the tier")
    elif not any(pod["pinned_shards_delta"] > 0 for pod in joined):
        failures.append(
            "no pod that joined the tier answered a pinned shard: "
            + ", ".join(f"{pod['pod']}={pod['pinned_shards_delta']}" for pod in joined)
        )

document = {
    "generated_at": generated_at,
    "base_replicas": base,
    "scaled_replicas": target,
    "samples": samples,
    "transitions": transitions,
    "failures": failures,
    "status": "failed" if failures else "ok",
}
with open(out_path, "w", encoding="utf-8") as handle:
    json.dump(document, handle, indent=2)
    handle.write("\n")
print(
    f"SCALE_{base}_{target}_{base} status={document['status']} "
    f"samples={len(samples)} transitions={len(transitions)} "
    f"exact={all(s['cross_shard_group_by']['exact'] for s in samples) if samples else False}"
)
for failure in failures:
    print(f"  {failure}", file=sys.stderr)
raise SystemExit(1 if failures else 0)
PY
}

prometheus_result() {
  local expression=$1
  curl -fsS --get "$PROM_URL/api/v1/query" --data-urlencode "query=$expression" |
    python3 -c '
import json, sys
response = json.load(sys.stdin)
if response.get("status") != "success":
    raise SystemExit(f"Prometheus query failed: {response}")
result = response["data"]["result"]
sample = result[0].get("value", [None, "-"])[1] if result else "-"
print(f"{len(result)}\t{sample}")
'
}

PANEL_FAILURES=0
POSTGRES_OUTAGE_FAILURE=0
query_panel() {
  local panel=$1 ref=$2 expression=$3 result count sample
  result="$(prometheus_result "$expression")"
  IFS=$'\t' read -r count sample <<<"$result"
  printf 'PANEL id=%s ref=%s series=%s sample=%s\n' "$panel" "$ref" "$count" "$sample"
  [[ "$count" -gt 0 ]] || PANEL_FAILURES=1
}

# --- #3647: per-pod ingester request series ----------------------------------
#
# `prometheus_result` above keeps a series COUNT and the first value, which is
# all a panel check needs and none of what this one is about: the claim is that
# every contributing series carries a nonempty `pod` label, and that the
# operator's expression reads the MEAN OF THE PER-POD SUMS -- not the fleet
# total, not the average over series. All four queries below are evaluated at
# one fixed `time=`, so the arithmetic is over one instant and not over four.
INGESTER_POD_LABEL_FAILURE=0
ingester_pod_label_failure() {
  INGESTER_POD_LABEL_FAILURE=1
  printf 'INGESTER_POD_LABEL_FAILURE %s\n' "$*"
}

# The label matcher shared by all four expressions, and the one
# crates/siglake-operator/src/prom.rs:177 builds from the release and namespace.
ingester_selector() {
  printf 'namespace="%s",app_kubernetes_io_instance="%s",app_kubernetes_io_component="ingester"' \
    "$NAMESPACE" "$RELEASE"
}
ingester_raw_expression() {
  printf 'siglake_ingest_requests_total{%s}' "$(ingester_selector)"
}
ingester_per_series_expression() {
  printf 'rate(siglake_ingest_requests_total{%s}[1m])' "$(ingester_selector)"
}
ingester_per_pod_expression() {
  printf 'sum by (pod) (rate(siglake_ingest_requests_total{%s}[1m]))' "$(ingester_selector)"
}
# Character for character the operator's own query. The guard in
# scripts/check-kind-ingester-pod-labels.sh reads the format string out of
# prom.rs and compares it with this one, so a change on either side is caught
# offline rather than by a round that captured an expression nothing evaluates.
ingester_operator_expression() {
  printf 'avg(sum by (pod) (rate(siglake_ingest_requests_total{%s}[1m])))' "$(ingester_selector)"
}

# One instant query at a fixed evaluation timestamp, retained verbatim. A failed
# query is retained as an error document rather than dropped: the grader must
# see that the response was not a vector, not an empty results directory.
prometheus_capture() {
  local expression=$1 at=$2 output=$3
  if ! curl -fsS --connect-timeout 5 --max-time 30 --get "$PROM_URL/api/v1/query" \
    --data-urlencode "query=$expression" \
    --data-urlencode "time=$at" >"$output"; then
    printf '{"status":"error","data":{"resultType":"vector","result":[]}}\n' >"$output"
    return 1
  fi
}

# Ready, non-terminating ingester pods. Same readiness gate the Service (and so
# Prometheus' scrape targets) uses.
ready_ingester_pods() {
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods \
    -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=ingester" \
    --sort-by=.metadata.name -o json 2>/dev/null | python3 -c '
import json, sys
try:
    items = json.load(sys.stdin).get("items", [])
except ValueError:
    raise SystemExit(0)
for item in items:
    meta, status = item["metadata"], item.get("status", {})
    if meta.get("deletionTimestamp"):
        continue
    conditions = {c.get("type"): c.get("status") for c in status.get("conditions", [])}
    if conditions.get("Ready") == "True":
        print(meta["name"])
'
}

# Only minReplicaCount, for the reason patch_query_floor gives: it is a clamp the
# HPA acts on at its next sync, and putting it back leaves the installed range
# and KEDA's own triggers in charge.
patch_ingester_floor() {
  local min=$1
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" patch \
    "scaledobject/${INGESTER_SCALEDOBJECT}" --type=merge \
    -p "{\"spec\":{\"minReplicaCount\":${min}}}" >/dev/null
}

# How many pods Prometheus currently reads a NONZERO 1m request rate for. A pod
# that exists but has not been scraped twice inside the window is not evidence.
active_ingester_pods() {
  local expression
  expression="count(sum by (pod) (rate(siglake_ingest_requests_total{$(ingester_selector)}[1m])) > 0)"
  curl -fsS --connect-timeout 5 --max-time 30 --get "$PROM_URL/api/v1/query" \
    --data-urlencode "query=$expression" 2>/dev/null | python3 -c '
import json, sys
try:
    document = json.load(sys.stdin)
except ValueError:
    print(0)
    raise SystemExit(0)
result = document.get("data", {}).get("result", []) if document.get("status") == "success" else []
print(int(float(result[0]["value"][1])) if result else 0)
' || printf '0\n'
}

# One OTLP trace export. This is the second endpoint the capture needs: an
# ingester publishes `siglake_ingest_requests_total` per endpoint/status/tenant/
# index, so a tier carrying only /v1/logs publishes one series per pod and the
# per-series average and the per-pod mean are the same number. Traces land in
# their own index and table, so they add series without touching the `events`
# corpus the round's query evidence was taken against.
ingest_traces() {
  local start=$1 count=$2 payload_file="$TMP_DIR/otlp-traces-payload.json"
  python3 - "$start" "$count" >"$payload_file" <<'PY'
import json
import sys

start, count = map(int, sys.argv[1:])
spans = []
for i in range(start, start + count):
    begin = 1_700_000_000_000_000_000 + i * 1_000_000
    spans.append({
        "traceId": f"{i:032x}",
        "spanId": f"{i + 1:016x}",
        "name": f"kind-round-span-{i % 8}",
        "kind": "SPAN_KIND_SERVER",
        "startTimeUnixNano": str(begin),
        "endTimeUnixNano": str(begin + 5_000),
    })
json.dump({"resourceSpans": [{
    "resource": {"attributes": [
        {"key": "service.name", "value": {"stringValue": "kind-round"}},
    ]},
    "scopeSpans": [{"scope": {"name": "kind-round"}, "spans": spans}],
}]}, sys.stdout, separators=(",", ":"))
PY
  post_json_file 'http://127.0.0.1:8088/v1/traces' "$payload_file" \
    -H 'X-Scope-OrgID: default' \
    >/dev/null
}

# $1 = the first unused event id, so the hosts this phase writes stay distinct
# from the load window's. Every failure is deferred like PANEL_FAILURES: the
# round's remaining evidence outlives a capture that did not hold, and the floor
# must come back down either way.
capture_ingester_pod_labels() {
  local next=$1
  local deadline at pods=() active=0 rc=0
  mkdir -p "$RESULTS_DIR"
  deadline=$((SECONDS + INGESTER_POD_LABEL_GRACE_SECONDS))

  log "raise the ingester floor ${INGESTER_SCALE_BASE} -> ${INGESTER_SCALE_TARGET} for the per-pod capture"
  if ! patch_ingester_floor "$INGESTER_SCALE_TARGET"; then
    ingester_pod_label_failure "could not raise the ingester floor to ${INGESTER_SCALE_TARGET}"
    return 1
  fi
  while ((SECONDS < deadline)); do
    mapfile -t pods < <(ready_ingester_pods)
    ((${#pods[@]} < INGESTER_SCALE_TARGET)) || break
    sleep 5
  done
  mapfile -t pods < <(ready_ingester_pods)
  printf 'INGESTER_POD_LABEL_PODS count=%s pods=%s\n' "${#pods[@]}" "${pods[*]:-none}"
  if ((${#pods[@]} < INGESTER_SCALE_TARGET)); then
    ingester_pod_label_failure "only ${#pods[@]} ready ingester pods, expected ${INGESTER_SCALE_TARGET}"
    patch_ingester_floor "$INGESTER_SCALE_BASE" || true
    return 1
  fi

  # The Service spreads requests over every ready pod, so this is what puts a
  # nonzero rate on both of them at once. It runs for its own window and then
  # keeps running while the [1m] rate fills, because a window that empties while
  # waiting is a capture with one active pod in it.
  log "drive logs and traces at ${#pods[@]} ingester pods for ${INGESTER_POD_LABEL_SECONDS}s"
  local traffic_deadline=$((SECONDS + INGESTER_POD_LABEL_SECONDS))
  while ((SECONDS < traffic_deadline)) || { ((SECONDS < deadline)) && ((active < INGESTER_SCALE_TARGET)); }; do
    ingest_events "$next" "$INGESTER_POD_LABEL_BATCH" || true
    next=$((next + INGESTER_POD_LABEL_BATCH))
    ingest_traces "$next" "$INGESTER_POD_LABEL_TRACES" || true
    next=$((next + INGESTER_POD_LABEL_TRACES))
    if ((SECONDS >= traffic_deadline)); then
      active="$(active_ingester_pods)"
      [[ "$active" =~ ^[0-9]+$ ]] || active=0
    fi
    # Paced like the load window above rather than run flat out: a nonzero 1m
    # rate on both pods is the claim, and an unthrottled loop would spend the
    # phase writing tens of thousands of events through a kind-sized compactor
    # for a reading it already had.
    sleep 1
  done
  printf 'INGESTER_POD_LABEL_ACTIVE pods=%s expected=%s\n' "$active" "$INGESTER_SCALE_TARGET"

  # One timestamp for all four answers.
  at="$(date -u +%s)"
  prometheus_capture "$(ingester_raw_expression)" "$at" "$INGESTER_RAW_JSON" ||
    ingester_pod_label_failure "the raw ingester series query failed"
  prometheus_capture "$(ingester_per_series_expression)" "$at" "$INGESTER_PER_SERIES_JSON" ||
    ingester_pod_label_failure "the per-series rate query failed"
  prometheus_capture "$(ingester_per_pod_expression)" "$at" "$INGESTER_PER_POD_JSON" ||
    ingester_pod_label_failure "the per-pod rate query failed"
  prometheus_capture "$(ingester_operator_expression)" "$at" "$INGESTER_EXPRESSION_JSON" ||
    ingester_pod_label_failure "the operator expression query failed"

  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods \
    -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=ingester" \
    --sort-by=.metadata.name -o json >"$TMP_DIR/ingester-pods.json" 2>/dev/null || true
  printf '%s\n' "${pods[@]}" >"$TMP_DIR/ingester-expected-pods"

  python3 - "$TMP_DIR/ingester-capture.json" "$at" "$NAMESPACE" "$RELEASE" \
    "$(iso_now)" "$(source_commit)" "$(source_commit_origin)" \
    "$TMP_DIR/ingester-pods.json" "$TMP_DIR/ingester-expected-pods" \
    "$INGESTER_SCALE_BASE" "$INGESTER_SCALE_TARGET" "$INGESTER_POD_LABEL_SECONDS" \
    "$(ingester_raw_expression)" "$INGESTER_RAW_JSON" \
    "$(ingester_per_series_expression)" "$INGESTER_PER_SERIES_JSON" \
    "$(ingester_per_pod_expression)" "$INGESTER_PER_POD_JSON" \
    "$(ingester_operator_expression)" "$INGESTER_EXPRESSION_JSON" <<'PY' || rc=$?
import json
import sys

(
    out_path,
    at,
    namespace,
    release,
    generated_at,
    commit,
    commit_source,
    pods_path,
    expected_path,
    base,
    target,
    load_seconds,
    raw_expression,
    raw_path,
    per_series_expression,
    per_series_path,
    per_pod_expression,
    per_pod_path,
    operator_expression,
    operator_path,
) = sys.argv[1:]


def response(path):
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return {"status": "error", "data": {"resultType": "vector", "result": []}}


with open(pods_path, encoding="utf-8") as handle:
    try:
        items = json.load(handle).get("items", [])
    except ValueError:
        items = []
revisions = []
for item in items:
    for container in item.get("status", {}).get("containerStatuses", []):
        revisions.append(
            {
                "pod": item["metadata"]["name"],
                "container": container.get("name"),
                "image": container.get("image"),
                "image_id": container.get("imageID"),
            }
        )
with open(expected_path, encoding="utf-8") as handle:
    expected = [line.strip() for line in handle if line.strip()]

document = {
    "schema_version": 1,
    "generated_at": generated_at,
    "evaluated_at": int(at),
    "revisions": {
        "repository_commit": commit,
        "repository_commit_source": commit_source,
        "ingester_pods": revisions,
    },
    "settings": {
        "namespace": namespace,
        "release": release,
        # Named so a reader knows the second pod came from a floor patch and not
        # from the tier's own accept-rate trigger.
        "scale_path": "scaledobject_min_replica_patch",
        "ingester_min_replicas": int(base),
        "ingester_max_replicas": int(target),
        "ingester_floor_during_capture": int(target),
        "load_seconds": int(load_seconds),
        "rate_window": "1m",
    },
    "expected_pods": expected,
    "queries": {
        "raw": {"expression": raw_expression, "time": int(at), "response": response(raw_path)},
        "per_series_rate": {
            "expression": per_series_expression,
            "time": int(at),
            "response": response(per_series_path),
        },
        "per_pod_rate": {
            "expression": per_pod_expression,
            "time": int(at),
            "response": response(per_pod_path),
        },
        "operator_expression": {
            "expression": operator_expression,
            "time": int(at),
            "response": response(operator_path),
        },
    },
}
with open(out_path, "w", encoding="utf-8") as handle:
    json.dump(document, handle, indent=2)
    handle.write("\n")
PY
  if ((rc != 0)); then
    ingester_pod_label_failure "could not assemble the capture document"
  elif ! python3 "$ROOT/scripts/grade-kind-ingester-pod-labels.py" \
    "$TMP_DIR/ingester-capture.json" --output "$INGESTER_POD_LABEL_JSON"; then
    ingester_pod_label_failure "the capture did not grade verified; see ${INGESTER_POD_LABEL_JSON#"$ROOT/"}"
  fi

  log "lower the ingester floor back to ${INGESTER_SCALE_BASE}"
  patch_ingester_floor "$INGESTER_SCALE_BASE" ||
    ingester_pod_label_failure "could not lower the ingester floor to ${INGESTER_SCALE_BASE}"
  ((INGESTER_POD_LABEL_FAILURE == 0))
}

# --- #4151: shared catalog-claim queue on every compactor -------------------
COMPACTOR_POD_LABEL_FAILURE=0
compactor_pod_label_failure() {
  COMPACTOR_POD_LABEL_FAILURE=1
  printf 'COMPACTOR_POD_LABEL_FAILURE %s\n' "$*"
}

compactor_selector() {
  printf 'namespace="%s",app_kubernetes_io_instance="%s",app_kubernetes_io_component="compactor"' \
    "$NAMESPACE" "$RELEASE"
}
compactor_raw_expression() {
  printf 'siglake_compactor_sealed_pending{%s}' "$(compactor_selector)"
}
compactor_sample_times_expression() {
  printf 'timestamp(siglake_compactor_sealed_pending{%s})' "$(compactor_selector)"
}
compactor_per_pod_expression() {
  printf 'sum by (pod) (siglake_compactor_sealed_pending{%s})' "$(compactor_selector)"
}
# Character for character the operator's query in prom.rs. The offline check
# compares the two format strings, so this cannot drift into evidence for a
# query the reconciler does not run.
compactor_operator_expression() {
  printf 'avg(sum by (pod) (siglake_compactor_sealed_pending{%s}))' "$(compactor_selector)"
}

ready_compactor_pods() {
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods \
    -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=compactor" \
    --sort-by=.metadata.name -o json 2>/dev/null | python3 -c '
import json, sys
try:
    items = json.load(sys.stdin).get("items", [])
except ValueError:
    raise SystemExit(0)
for item in items:
    meta, status = item["metadata"], item.get("status", {})
    if meta.get("deletionTimestamp"):
        continue
    conditions = {c.get("type"): c.get("status") for c in status.get("conditions", [])}
    if conditions.get("Ready") == "True":
        print(meta["name"])
'
}

# The queue changes while the mirror registers freshly sealed segments, and
# each compactor refreshes its own gauge. One unequal scrape is therefore not
# evidence of a sharded queue. Accept only after two successive scrape
# generations cover the same ready pods, carry the same positive total on each
# pod, and advance every pod's source-sample timestamp. The final raw and
# timestamp() responses are still retained verbatim below.
compactor_capture_settled() {
  local raw=$1 times=$2 expected=$3 candidate=$4 settled=$5
  python3 - "$raw" "$times" "$expected" "$candidate" "$settled" \
    "$COMPACTOR_SCRAPE_INTERVAL_SECONDS" <<'PY'
import json
import math
import pathlib
import sys

raw_path, times_path, expected_path, candidate_path, settled_path, interval = sys.argv[1:]
interval = float(interval)


def vector(path):
    try:
        response = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if response.get("status") != "success":
        return []
    data = response.get("data", {})
    return data.get("result", []) if data.get("resultType") == "vector" else []


expected = {line.strip() for line in open(expected_path, encoding="utf-8") if line.strip()}
values = {}
for row in vector(raw_path):
    labels = row.get("metric", {})
    pod = labels.get("pod", "").strip()
    if not pod or labels.get("tenant") != "default":
        continue
    try:
        value = float(row["value"][1])
    except (KeyError, IndexError, TypeError, ValueError):
        continue
    if math.isfinite(value):
        values[pod] = values.get(pod, 0.0) + value

sample_times = {}
for row in vector(times_path):
    pod = row.get("metric", {}).get("pod", "").strip()
    try:
        stamp = float(row["value"][1])
    except (KeyError, IndexError, TypeError, ValueError):
        continue
    if pod and math.isfinite(stamp):
        sample_times[pod] = max(sample_times.get(pod, stamp), stamp)

valid = (
    len(expected) >= 2
    and set(values) == expected
    and set(sample_times) == expected
    and min(values.values(), default=0.0) > 0.0
    and len({round(value, 9) for value in values.values()}) == 1
    and max(sample_times.values(), default=0.0) - min(sample_times.values(), default=0.0)
        <= interval
)
candidate = pathlib.Path(candidate_path)
if not valid:
    candidate.unlink(missing_ok=True)
    raise SystemExit(1)

current = {
    "pods": [
        {"pod": pod, "value": values[pod], "sample_time": sample_times[pod]}
        for pod in sorted(expected)
    ]
}
try:
    previous = json.loads(candidate.read_text(encoding="utf-8"))
except (OSError, ValueError):
    previous = None
if previous:
    old = {row["pod"]: row for row in previous.get("pods", [])}
    new = {row["pod"]: row for row in current["pods"]}
    if (
        set(old) == set(new)
        and all(math.isclose(old[p]["value"], new[p]["value"], rel_tol=1e-9) for p in new)
        and all(new[p]["sample_time"] > old[p]["sample_time"] for p in new)
    ):
        pathlib.Path(settled_path).write_text(
            json.dumps([previous, current], indent=2) + "\n", encoding="utf-8"
        )
        raise SystemExit(0)
candidate.write_text(json.dumps(current, indent=2) + "\n", encoding="utf-8")
raise SystemExit(1)
PY
}

capture_compactor_pod_labels() {
  local next=$1 deadline at pods=() rc=0 settled=0
  local candidate="$TMP_DIR/compactor-settling-candidate.json"
  local settling="$TMP_DIR/compactor-settling.json"
  mkdir -p "$RESULTS_DIR"

  # This Helm upgrade is the live installability check the card asks for. It
  # uses the round's existing mirror + catalog claim, changes no HPA guard, and
  # runs after the ordinary evidence. The large/old batch gate keeps the queue
  # still long enough for two independently scraped gauges to converge.
  log "install ${COMPACTOR_SCALE_TARGET} catalog-claim compactors for the shared-queue capture"
  if ! helm --kube-context "$KUBE_CONTEXT" upgrade "$RELEASE" \
    "$ROOT/deploy/helm/siglake" --namespace "$NAMESPACE" --reuse-values \
    --set compactor.replicas="$COMPACTOR_SCALE_TARGET" \
    --set compactor.commitBatch.targetMb="$COMPACTOR_CAPTURE_BATCH_TARGET_MB" \
    --set compactor.commitBatch.maxAgeSecs="$COMPACTOR_CAPTURE_BATCH_MAX_AGE_SECONDS" \
    --wait --timeout 10m; then
    compactor_pod_label_failure "the chart did not install a two-compactor catalog-claim tier"
    return 1
  fi

  deadline=$((SECONDS + COMPACTOR_POD_LABEL_GRACE_SECONDS))
  while ((SECONDS < deadline)); do
    mapfile -t pods < <(ready_compactor_pods)
    ((${#pods[@]} < COMPACTOR_SCALE_TARGET)) || break
    sleep 5
  done
  mapfile -t pods < <(ready_compactor_pods)
  printf 'COMPACTOR_POD_LABEL_PODS count=%s pods=%s\n' "${#pods[@]}" "${pods[*]:-none}"
  if ((${#pods[@]} != COMPACTOR_SCALE_TARGET)); then
    compactor_pod_label_failure "found ${#pods[@]} ready compactor pods, expected ${COMPACTOR_SCALE_TARGET}"
    return 1
  fi
  printf '%s\n' "${pods[@]}" >"$TMP_DIR/compactor-expected-pods"

  log "drive sealed-WAL load for ${COMPACTOR_POD_LABEL_LOAD_SECONDS}s"
  local traffic_deadline=$((SECONDS + COMPACTOR_POD_LABEL_LOAD_SECONDS))
  while ((SECONDS < traffic_deadline)); do
    ingest_events "$next" "$COMPACTOR_POD_LABEL_BATCH" || true
    next=$((next + COMPACTOR_POD_LABEL_BATCH))
    sleep 1
  done

  # Poll Prometheus, not the pods directly: this is the label and arithmetic
  # path the operator consumes. timestamp(metric) retains the source scrape
  # time behind each gauge rather than the query evaluation time.
  while ((SECONDS < deadline)); do
    at="$(date -u +%s)"
    prometheus_capture "$(compactor_raw_expression)" "$at" "$COMPACTOR_RAW_JSON" || true
    prometheus_capture "$(compactor_sample_times_expression)" "$at" \
      "$COMPACTOR_SAMPLE_TIMES_JSON" || true
    if compactor_capture_settled "$COMPACTOR_RAW_JSON" "$COMPACTOR_SAMPLE_TIMES_JSON" \
      "$TMP_DIR/compactor-expected-pods" "$candidate" "$settling"; then
      settled=1
      break
    fi
    sleep 2
  done
  if ((settled == 0)); then
    compactor_pod_label_failure "the two compactor gauges did not converge across two scrape generations"
  fi

  at="${at:-$(date -u +%s)}"
  prometheus_capture "$(compactor_per_pod_expression)" "$at" "$COMPACTOR_PER_POD_JSON" ||
    compactor_pod_label_failure "the grouped compactor query failed"
  prometheus_capture "$(compactor_operator_expression)" "$at" "$COMPACTOR_EXPRESSION_JSON" ||
    compactor_pod_label_failure "the operator compactor expression failed"
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods \
    -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=compactor" \
    --sort-by=.metadata.name -o json >"$TMP_DIR/compactor-pods.json" 2>/dev/null || true

  python3 - "$TMP_DIR/compactor-capture.json" "$at" "$NAMESPACE" "$RELEASE" \
    "$(iso_now)" "$(source_commit)" "$(source_commit_origin)" \
    "$TMP_DIR/compactor-pods.json" "$TMP_DIR/compactor-expected-pods" "$settling" \
    "$COMPACTOR_SCALE_TARGET" "$COMPACTOR_POD_LABEL_LOAD_SECONDS" \
    "$COMPACTOR_INTERVAL_SECONDS" "$COMPACTOR_SCRAPE_INTERVAL_SECONDS" \
    "$COMPACTOR_CAPTURE_BATCH_TARGET_MB" "$COMPACTOR_CAPTURE_BATCH_MAX_AGE_SECONDS" \
    "$(compactor_raw_expression)" "$COMPACTOR_RAW_JSON" \
    "$(compactor_sample_times_expression)" "$COMPACTOR_SAMPLE_TIMES_JSON" \
    "$(compactor_per_pod_expression)" "$COMPACTOR_PER_POD_JSON" \
    "$(compactor_operator_expression)" "$COMPACTOR_EXPRESSION_JSON" <<'PY' || rc=$?
import json
import sys

(
    out_path, at, namespace, release, generated_at, commit, commit_source, pods_path,
    expected_path, settling_path, target, load_seconds, compactor_interval, scrape_interval,
    batch_target, batch_max_age, raw_expression, raw_path, times_expression,
    times_path, per_pod_expression, per_pod_path, operator_expression, operator_path,
) = sys.argv[1:]


def response(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return {"status": "error", "data": {"resultType": "vector", "result": []}}


try:
    items = json.load(open(pods_path, encoding="utf-8")).get("items", [])
except (OSError, ValueError):
    items = []
revisions = []
for item in items:
    for container in item.get("status", {}).get("containerStatuses", []):
        revisions.append({
            "pod": item["metadata"]["name"],
            "container": container.get("name"),
            "image": container.get("image"),
            "image_id": container.get("imageID"),
        })
expected = [line.strip() for line in open(expected_path, encoding="utf-8") if line.strip()]
try:
    settling = json.load(open(settling_path, encoding="utf-8"))
except (OSError, ValueError):
    settling = []
document = {
    "schema_version": 1,
    "generated_at": generated_at,
    "evaluated_at": int(at),
    "revisions": {
        "repository_commit": commit,
        "repository_commit_source": commit_source,
        "compactor_pods": revisions,
    },
    "settings": {
        "namespace": namespace,
        "release": release,
        "scale_path": "helm_upgrade_reuse_values",
        "compactor_replicas": int(target),
        "load_seconds": int(load_seconds),
        "compactor_interval_seconds": int(compactor_interval),
        "scrape_interval_seconds": int(scrape_interval),
        "commit_batch_target_mb": int(batch_target),
        "commit_batch_max_age_seconds": int(batch_max_age),
    },
    "expected_pods": expected,
    "settling_samples": settling,
    "queries": {
        "raw": {"expression": raw_expression, "time": int(at), "response": response(raw_path)},
        "sample_times": {
            "expression": times_expression, "time": int(at), "response": response(times_path),
        },
        "per_pod": {
            "expression": per_pod_expression, "time": int(at), "response": response(per_pod_path),
        },
        "operator_expression": {
            "expression": operator_expression, "time": int(at), "response": response(operator_path),
        },
    },
}
json.dump(document, open(out_path, "w", encoding="utf-8"), indent=2)
open(out_path, "a", encoding="utf-8").write("\n")
PY
  if ((rc != 0)); then
    compactor_pod_label_failure "could not assemble the compactor capture document"
  elif ! python3 "$ROOT/scripts/grade-kind-compactor-pod-labels.py" \
    "$TMP_DIR/compactor-capture.json" --output "$COMPACTOR_POD_LABEL_JSON"; then
    compactor_pod_label_failure "the capture did not grade verified; see ${COMPACTOR_POD_LABEL_JSON#"$ROOT/"}"
  fi
  ((COMPACTOR_POD_LABEL_FAILURE == 0))
}

# --- #4953: filesystem-drain mirror-reclamation qualification ---------------
MIRROR_RECLAIM_MC_POD=siglake-mirror-reclaim-mc
MIRROR_RECLAIM_SAMPLE_OBJECTS=0
MIRROR_RECLAIM_SAMPLE_BYTES=0
MIRROR_RECLAIM_SAMPLE_LEDGER=0
MIRROR_RECLAIM_SAMPLE_ROWS_COMMITTED=0
MIRROR_RECLAIM_BASE_ROWS_COMMITTED=0
MIRROR_RECLAIM_FIRST_SAMPLE_EPOCH=0
MIRROR_RECLAIM_LAST_SAMPLE_EPOCH=0
MIRROR_RECLAIM_LOAD_STARTED_EPOCH=0
MIRROR_RECLAIM_LOAD_FINISHED_EPOCH=0

write_mirror_reclaim_launch() {
  [[ -n "$MIRROR_RECLAIM_ARM" ]] || return 0
  mkdir -p "$MIRROR_RECLAIM_RESULTS_DIR"
  python3 - "$MIRROR_RECLAIM_LAUNCH_JSON" "$MIRROR_RECLAIM_ARM" \
    "$CATALOG_CLAIM_ENABLED" "$WAL_MIRROR_ENABLED" \
    "$WAL_MIRROR_ACTIVE_INTERVAL_SECS" "$COMMITTED_RETENTION_SECS" \
    "$MIRROR_LEDGER_RECLAIM" "$LOAD_SECONDS" "$MIRROR_RECLAIM_RESULTS_DIR" \
    "$(source_commit)" "$(source_commit_origin)" "$@" <<'PY'
import json
import sys

(
    path, arm, catalog_claim, mirror, active_interval, retention, reclaim,
    load_seconds, results_dir, commit, commit_source, *helm_args,
) = sys.argv[1:]
document = {
    "schema": "siglake.kind.mirror_reclaim_launch.v1",
    "arm": arm,
    "source_commit": commit,
    "source_commit_source": commit_source,
    "results_directory": results_dir,
    "inputs": {
        "KIND_ROUND_MIRROR_RECLAIM_ARM": arm,
        "KIND_ROUND_CATALOG_CLAIM_ENABLED": catalog_claim,
        "KIND_ROUND_WAL_MIRROR_ENABLED": mirror,
        "KIND_ROUND_WAL_MIRROR_ACTIVE_INTERVAL_SECS": int(active_interval),
        "KIND_ROUND_COMMITTED_RETENTION_SECS": int(retention),
        "KIND_ROUND_MIRROR_LEDGER_RECLAIM": reclaim,
        "KIND_ROUND_LOAD_SECONDS": int(load_seconds),
    },
    "helm_command": [
        "helm", "--kube-context", "<kind-context>", "upgrade", "--install",
        "siglake", "deploy/helm/siglake", *helm_args,
    ],
}
with open(path, "w", encoding="utf-8") as out:
    json.dump(document, out, indent=2)
    out.write("\n")
PY
}

capture_mirror_reclaim_effective_config() {
  [[ -n "$MIRROR_RECLAIM_ARM" ]] || return 0
  local helm_json="$TMP_DIR/mirror-reclaim-helm-values.json"
  local ingester_json="$TMP_DIR/mirror-reclaim-ingester.json"
  local compactor_json="$TMP_DIR/mirror-reclaim-compactor.json"
  helm --kube-context "$KUBE_CONTEXT" get values siglake -n "$NAMESPACE" --all -o json \
    >"$helm_json"
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get deployment siglake-ingester -o json \
    >"$ingester_json"
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get deployment siglake-compactor -o json \
    >"$compactor_json"
  python3 - "$MIRROR_RECLAIM_CONFIG_JSON" "$MIRROR_RECLAIM_ARM" \
    "$helm_json" "$ingester_json" "$compactor_json" <<'PY'
import json
import sys

out_path, arm, helm_path, ingester_path, compactor_path = sys.argv[1:]
values = json.load(open(helm_path, encoding="utf-8"))
ingester = json.load(open(ingester_path, encoding="utf-8"))
compactor = json.load(open(compactor_path, encoding="utf-8"))


def container(deployment, name):
    matches = [item for item in deployment["spec"]["template"]["spec"]["containers"]
               if item["name"] == name]
    if len(matches) != 1:
        raise SystemExit(f"expected one {name} container, found {len(matches)}")
    return matches[0]


def env_map(item):
    return {entry["name"]: entry.get("value") for entry in item.get("env", [])
            if "value" in entry}


ingester_container = container(ingester, "ingester")
compactor_container = container(compactor, "compactor")
ingester_env = env_map(ingester_container)
compactor_env = env_map(compactor_container)
selected = {
    "compactor.catalogClaim.enabled": values["compactor"]["catalogClaim"]["enabled"],
    "compactor.committedRetentionSecs": values["compactor"]["committedRetentionSecs"],
    "compactor.mirrorLedgerReclaim": values["compactor"]["mirrorLedgerReclaim"],
    "wal.mirror.enabled": values["wal"]["mirror"]["enabled"],
    "wal.mirror.activeIntervalSecs": values["wal"]["mirror"]["activeIntervalSecs"],
}
expected = {
    "compactor.catalogClaim.enabled": False,
    "compactor.committedRetentionSecs": 901,
    "compactor.mirrorLedgerReclaim": arm == "on",
    "wal.mirror.enabled": True,
    "wal.mirror.activeIntervalSecs": 0,
}
if selected != expected:
    raise SystemExit(f"effective Helm values differ: expected={expected!r} got={selected!r}")
if "--catalog-claim" in compactor_container.get("args", []):
    raise SystemExit("filesystem-drain arm rendered --catalog-claim")
# Since #5880 the chart renders the prefix on the compactor too, in both drain
# modes. The arm exists for the drain whose prefix was wrong, so all three
# readings — the value, the writer, the reclaimer — have to be the same string.
mirror_prefixes = {
    "values.wal.mirror.prefix": values["wal"]["mirror"]["prefix"],
    "ingester": ingester_env.get("SIGLAKE_WAL_MIRROR_PREFIX"),
    "compactor": compactor_env.get("SIGLAKE_WAL_MIRROR_PREFIX"),
}
if len(set(mirror_prefixes.values())) != 1:
    raise SystemExit(f"WAL mirror prefixes differ: {mirror_prefixes!r}")
if ingester_env.get("SIGLAKE_REMOTE_WAL_DRAIN") != "0":
    raise SystemExit("ingester did not render the filesystem-drain arm")
if "SIGLAKE_WAL_ACTIVE_MIRROR_INTERVAL_SECS" in ingester_env:
    raise SystemExit("activeIntervalSecs=0 rendered an active-mirror interval")
if compactor_env.get("SIGLAKE_COMMITTED_RETENTION_SECS") != "901":
    raise SystemExit("compactor did not render committedRetentionSecs=901")
if compactor_env.get("SIGLAKE_MIRROR_LEDGER_RECLAIM") != ("1" if arm == "on" else "0"):
    raise SystemExit("compactor did not render the selected ledger-reclaim arm")

document = {
    "schema": "siglake.kind.mirror_reclaim_effective_config.v1",
    "arm": arm,
    "helm_values": selected,
    "deployed": {
        "ingester": {
            "deployment_uid": ingester["metadata"]["uid"],
            "wal_mirror_prefix": ingester_env["SIGLAKE_WAL_MIRROR_PREFIX"],
            "remote_wal_drain": ingester_env["SIGLAKE_REMOTE_WAL_DRAIN"],
            "active_mirror_interval_env": ingester_env.get(
                "SIGLAKE_WAL_ACTIVE_MIRROR_INTERVAL_SECS"
            ),
        },
        "compactor": {
            "deployment_uid": compactor["metadata"]["uid"],
            "args": compactor_container.get("args", []),
            "committed_retention_secs": int(
                compactor_env["SIGLAKE_COMMITTED_RETENTION_SECS"]
            ),
            "mirror_ledger_reclaim": compactor_env["SIGLAKE_MIRROR_LEDGER_RECLAIM"],
            "wal_mirror_prefix": compactor_env["SIGLAKE_WAL_MIRROR_PREFIX"],
        },
    },
}
with open(out_path, "w", encoding="utf-8") as out:
    json.dump(document, out, indent=2)
    out.write("\n")
PY
}

start_mirror_reclaim_observer() {
  [[ -n "$MIRROR_RECLAIM_ARM" ]] || return 0
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" run "$MIRROR_RECLAIM_MC_POD" \
    --image=quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z \
    --restart=Never --command -- sleep 7200 >/dev/null
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" wait \
    --for=condition=Ready "pod/$MIRROR_RECLAIM_MC_POD" --timeout=120s >/dev/null
  : >"$MIRROR_RECLAIM_SERIES_JSONL"
}

capture_mirror_reclaim_sample() {
  [[ -n "$MIRROR_RECLAIM_ARM" ]] || return 0
  local now_epoch metrics_file objects_file ledger_file pod pod_uid restart_count
  now_epoch=$(date +%s)
  metrics_file="$TMP_DIR/mirror-reclaim-compactor.metrics"
  objects_file="$TMP_DIR/mirror-reclaim-objects.jsonl"
  ledger_file="$TMP_DIR/mirror-reclaim-ledger.tsv"
  pod="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pod \
    -l 'app.kubernetes.io/instance=siglake,app.kubernetes.io/component=compactor' \
    --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')"
  [[ -n "$pod" ]] || die "no compactor pod found for mirror-reclaim sample"
  pod_uid="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pod "$pod" \
    -o jsonpath='{.metadata.uid}')"
  restart_count="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pod "$pod" \
    -o jsonpath='{.status.containerStatuses[?(@.name=="compactor")].restartCount}')"
  start_query_pod_forward "$pod" 19102 9101 mirror-reclaim
  curl -fsS http://127.0.0.1:19102/metrics >"$metrics_file"
  stop_active_forward
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" exec "$MIRROR_RECLAIM_MC_POD" -- \
    sh -c 'mc alias set local http://minio:9000 minioadmin minioadmin --quiet && mc ls --recursive --json local/siglake-warehouse/warehouse/wal-mirror/' \
    >"$objects_file"
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" exec postgres-0 -- \
    psql -U siglake -d siglake -At -F $'\t' -c \
    "SELECT status, count(*), coalesce(sum(rows), 0) FROM wal_segments WHERE tenant = 'default' AND index_id = '' GROUP BY status ORDER BY status" \
    >"$ledger_file"
  IFS=$'\t' read -r MIRROR_RECLAIM_SAMPLE_OBJECTS MIRROR_RECLAIM_SAMPLE_BYTES \
    MIRROR_RECLAIM_SAMPLE_LEDGER MIRROR_RECLAIM_SAMPLE_ROWS_COMMITTED < <(
    python3 - "$MIRROR_RECLAIM_SERIES_JSONL" "$MIRROR_RECLAIM_ARM" \
      "$now_epoch" "$MIRROR_RECLAIM_FIRST_SAMPLE_EPOCH" "$objects_file" \
      "$ledger_file" "$metrics_file" "$pod" "$pod_uid" "$restart_count" <<'PY'
import datetime
import json
import re
import sys

(
    out_path, arm, epoch, first_epoch, objects_path, ledger_path, metrics_path,
    pod, pod_uid, restart_count,
) = sys.argv[1:]
epoch = int(epoch)
first_epoch = int(first_epoch) or epoch

object_count = 0
object_bytes = 0
for raw in open(objects_path, encoding="utf-8"):
    raw = raw.strip()
    if not raw:
        continue
    item = json.loads(raw)
    if item.get("type") == "file" or "size" in item:
        object_count += 1
        object_bytes += int(item.get("size", 0))

ledger = {}
ledger_rows = 0
for raw in open(ledger_path, encoding="utf-8"):
    status, count, rows = raw.rstrip("\n").split("\t")
    ledger[status] = {"segments": int(count), "rows": int(rows)}
    ledger_rows += int(count)

metric_re = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([^\s]+)$")
metrics = []
for raw in open(metrics_path, encoding="utf-8"):
    match = metric_re.match(raw.strip())
    if match:
        metrics.append(match.groups())


def total(name, required=False, tenant=None):
    found = []
    for metric, labels, value in metrics:
        if metric != name:
            continue
        if tenant is not None and f'tenant="{tenant}"' not in (labels or ""):
            continue
        found.append(float(value))
    if required and not found:
        raise SystemExit(f"required metric {name} is absent")
    return sum(found)


counters = {
    "siglake_compactor_retention_purged_total": total(
        "siglake_compactor_retention_purged_total"
    ),
    "siglake_compactor_mirror_unreclaimed_total": total(
        "siglake_compactor_mirror_unreclaimed_total", required=True
    ),
    "siglake_compactor_mirror_mark_errors_total": total(
        "siglake_compactor_mirror_mark_errors_total", required=True
    ),
    "siglake_compactor_rows_committed_total": total(
        "siglake_compactor_rows_committed_total", tenant="default"
    ),
}
document = {
    "schema": "siglake.kind.mirror_reclaim_sample.v1",
    "arm": arm,
    "timestamp": datetime.datetime.fromtimestamp(
        epoch, datetime.timezone.utc
    ).isoformat().replace("+00:00", "Z"),
    "unix_seconds": epoch,
    "elapsed_seconds": epoch - first_epoch,
    "mirror_prefix": {"objects": object_count, "bytes": object_bytes},
    "wal_segments": {"scope": {"tenant": "default", "index_id": ""},
                     "total": ledger_rows, "by_status": ledger},
    "counters": counters,
    "counter_process": {
        "pod": pod, "pod_uid": pod_uid, "restart_count": int(restart_count)
    },
}
with open(out_path, "a", encoding="utf-8") as out:
    json.dump(document, out, separators=(",", ":"))
    out.write("\n")
print(
    object_count, object_bytes, ledger_rows,
    int(counters["siglake_compactor_rows_committed_total"]), sep="\t"
)
PY
  )
  if ((MIRROR_RECLAIM_FIRST_SAMPLE_EPOCH == 0)); then
    MIRROR_RECLAIM_FIRST_SAMPLE_EPOCH=$now_epoch
    MIRROR_RECLAIM_BASE_ROWS_COMMITTED=$MIRROR_RECLAIM_SAMPLE_ROWS_COMMITTED
    cp "$metrics_file" "$MIRROR_RECLAIM_METRICS_START"
  fi
  MIRROR_RECLAIM_LAST_SAMPLE_EPOCH=$now_epoch
  cp "$metrics_file" "$MIRROR_RECLAIM_METRICS_END"
}

finish_mirror_reclaim_evidence() {
  [[ -n "$MIRROR_RECLAIM_ARM" ]] || return 0
  local sent_rows=$1 workload_rounds=$2 query_response query_rows deadline committed_rows
  deadline=$((SECONDS + 300))
  while ((SECONDS < deadline)); do
    capture_mirror_reclaim_sample
    if ((MIRROR_RECLAIM_SAMPLE_ROWS_COMMITTED - MIRROR_RECLAIM_BASE_ROWS_COMMITTED == sent_rows)); then
      break
    fi
    sleep 10
  done
  committed_rows=$((MIRROR_RECLAIM_SAMPLE_ROWS_COMMITTED - MIRROR_RECLAIM_BASE_ROWS_COMMITTED))
  ((committed_rows == sent_rows)) ||
    die "mirror-reclaim row reconciliation did not drain: sent=${sent_rows} committed=${committed_rows}"
  query_response="$(run_sql 'SELECT count(*) AS n FROM events')"
  query_rows="$(printf '%s' "$query_response" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["rows"][0]["n"])')"
  python3 - "$MIRROR_RECLAIM_LOAD_JSON" "$MIRROR_RECLAIM_ROWS_JSON" \
    "$MIRROR_RECLAIM_ARM" "$MIRROR_RECLAIM_LOAD_STARTED_EPOCH" \
    "$MIRROR_RECLAIM_LOAD_FINISHED_EPOCH" "$LOAD_SECONDS" "$workload_rounds" \
    "$sent_rows" "$committed_rows" "$query_rows" \
    "$MIRROR_RECLAIM_BASE_ROWS_COMMITTED" "$MIRROR_RECLAIM_SAMPLE_ROWS_COMMITTED" \
    "$MIRROR_RECLAIM_FIRST_SAMPLE_EPOCH" "$MIRROR_RECLAIM_LAST_SAMPLE_EPOCH" <<'PY'
import datetime
import json
import sys

(
    load_path, rows_path, arm, started, finished, configured, rounds, sent,
    committed, query_rows, counter_start, counter_end, sample_start, sample_end,
) = sys.argv[1:]
started, finished, configured, rounds, sent, committed, query_rows = map(
    int, (started, finished, configured, rounds, sent, committed, query_rows)
)
counter_start, counter_end, sample_start, sample_end = map(
    int, (counter_start, counter_end, sample_start, sample_end)
)


def stamp(epoch):
    return datetime.datetime.fromtimestamp(
        epoch, datetime.timezone.utc
    ).isoformat().replace("+00:00", "Z")


load = {
    "schema": "siglake.kind.mirror_reclaim_load_window.v1",
    "arm": arm,
    "configured_seconds": configured,
    "started_at": stamp(started),
    "finished_at": stamp(finished),
    "actual_seconds": finished - started,
    "workload_rounds": rounds,
    "sent_rows": sent,
    "measurement_window": {
        "started_at": stamp(sample_start), "finished_at": stamp(sample_end),
        "seconds": sample_end - sample_start,
    },
}
rows = {
    "schema": "siglake.kind.mirror_reclaim_row_reconciliation.v1",
    "arm": arm,
    "sent_rows": sent,
    "committed_rows": committed,
    "difference": committed - sent,
    "committed_evidence": {
        "metric": "siglake_compactor_rows_committed_total{tenant=\"default\"}",
        "counter_start": counter_start,
        "counter_end": counter_end,
        "counter_delta": counter_end - counter_start,
        "meaning": "incremented only after a successful Iceberg commit",
    },
    "query_rows_after_committed_drain": query_rows,
    "verified": committed == sent == query_rows,
}
if not rows["verified"]:
    raise SystemExit(
        f"row reconciliation differs: sent={sent} committed={committed} query={query_rows}"
    )
for path, document in ((load_path, load), (rows_path, rows)):
    with open(path, "w", encoding="utf-8") as out:
        json.dump(document, out, indent=2)
        out.write("\n")
PY
  printf 'MIRROR_RECLAIM_EVIDENCE arm=%s directory=%s sent=%s committed=%s status=ok\n' \
    "$MIRROR_RECLAIM_ARM" "${MIRROR_RECLAIM_RESULTS_DIR#"$ROOT/"}" \
    "$sent_rows" "$committed_rows"
}

log "bring up the base kind deployment"
KIND_CLUSTER_NAME="$CLUSTER_NAME" \
  KIND_CLUSTER_OWNERSHIP_FILE="$KIND_CLUSTER_OWNERSHIP_FILE" \
  "$ROOT/scripts/kind-up.sh"

log "install pinned kube-prometheus-stack ${PROM_CHART_VERSION} and KEDA ${KEDA_CHART_VERSION}"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
helm repo add kedacore https://kedacore.github.io/charts --force-update
helm repo update prometheus-community kedacore
helm --kube-context "$KUBE_CONTEXT" upgrade --install "$PROM_RELEASE" \
  prometheus-community/kube-prometheus-stack \
  --version "$PROM_CHART_VERSION" \
  --namespace "$PROM_NAMESPACE" --create-namespace \
  --set grafana.enabled=false \
  --set alertmanager.enabled=false \
  --wait --timeout 10m
helm --kube-context "$KUBE_CONTEXT" upgrade --install keda kedacore/keda \
  --version "$KEDA_CHART_VERSION" \
  --namespace "$KEDA_NAMESPACE" --create-namespace \
  --wait --timeout 10m

log "upgrade Siglake with monitoring, mirror reconciliation and a ${QUERY_SCALE_BASE}-${QUERY_SCALE_TARGET} query KEDA range"
SIGLAKE_HELM_ARGS=(
  --namespace "$NAMESPACE" \
  --values "$ROOT/deploy/kind/values.kind.yaml" \
  --set prometheusRule.enabled=true \
  --set prometheusRule.labels.release="$PROM_RELEASE" \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.labels.release="$PROM_RELEASE" \
  --set wal.mirror.enabled="$WAL_MIRROR_ENABLED" \
  --set wal.mirror.activeIntervalSecs="$WAL_MIRROR_ACTIVE_INTERVAL_SECS" \
  --set compactor.catalogClaim.enabled="$CATALOG_CLAIM_ENABLED" \
  --set compactor.committedRetentionSecs="$COMMITTED_RETENTION_SECS" \
  --set compactor.mirrorLedgerReclaim="$MIRROR_LEDGER_RECLAIM" \
  --set query.jobs.persistent="$PERSISTENT_JOB_STORE" \
  --set query.replicas="$QUERY_SCALE_BASE" \
  --set keda.enabled=true \
  --set keda.query.minReplicas="$QUERY_SCALE_BASE" \
  --set keda.query.maxReplicas="$QUERY_SCALE_TARGET" \
  --set keda.ingester.minReplicas="$INGESTER_SCALE_BASE" \
  --set keda.ingester.maxReplicas="$INGESTER_SCALE_TARGET" \
  --set keda.pollingIntervalSeconds="$SCALE_POLLING_SECONDS" \
  --set keda.scaleDownStabilizationSeconds="$SCALE_STABILIZATION_SECONDS" \
  --set keda.cooldownPeriodSeconds="$SCALE_COOLDOWN_SECONDS" \
  --set-string keda.prometheusServerAddress="http://${PROM_RELEASE}-prometheus.${PROM_NAMESPACE}.svc.cluster.local:9090" \
  --wait --timeout 10m
)
write_mirror_reclaim_launch "${SIGLAKE_HELM_ARGS[@]}"
helm --kube-context "$KUBE_CONTEXT" upgrade --install siglake \
  "$ROOT/deploy/helm/siglake" "${SIGLAKE_HELM_ARGS[@]}"

kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" rollout status deployment/siglake-ingester --timeout=180s
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" rollout status deployment/siglake-compactor --timeout=180s
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" rollout status statefulset/siglake-query --timeout=180s

capture_mirror_reclaim_effective_config
start_mirror_reclaim_observer

log "scrape pre-registered alerted counters before load"
scrape_preregistered_zeros ingester 9100 19100
scrape_preregistered_zeros compactor 9101 19101
capture_mirror_reclaim_sample

log "$(initial_load_description)"
for ((offset = 0; offset < LOAD_EVENTS; offset += LOAD_BATCH)); do
  count=$LOAD_BATCH
  ((offset + count <= LOAD_EVENTS)) || count=$((LOAD_EVENTS - offset))
  ingest_events "$offset" "$count"
done

log "wait for every query pod's table cache to see all initial rows"
mapfile -t QUERY_PODS < <(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods \
  -l 'app.kubernetes.io/instance=siglake,app.kubernetes.io/component=query' \
  --sort-by=.metadata.name \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
((${#QUERY_PODS[@]} >= 2)) || die "expected at least two query pods, found ${#QUERY_PODS[@]}"
TOTAL=0
for pod in "${QUERY_PODS[@]}"; do
  wait_for_query_pod_count "$pod" "$LOAD_EVENTS"
  if ((TOTAL == 0)); then
    TOTAL=$LAST_QUERY_COUNT
  elif ((LAST_QUERY_COUNT != TOTAL)); then
    die "query pod row counts did not converge: expected ${TOTAL}, ${pod} saw ${LAST_QUERY_COUNT}"
  fi
done

log "transparent distributed GROUP BY cross-shard merge check"
query_pinned_total
PINNED_BEFORE=$QUERY_PINNED_TOTAL
# Keep records format so the response carries distributed phase stats, but use
# a non-count aggregate that the coordinator's Tier-1 battery cannot answer.
response="$(run_sql 'SELECT host, sum(1) AS n FROM events GROUP BY host')"
query_pinned_total
PINNED_AFTER=$QUERY_PINNED_TOTAL
printf '%s' "$response" |
  TOTAL="$TOTAL" PINNED_BEFORE="$PINNED_BEFORE" PINNED_AFTER="$PINNED_AFTER" python3 -c '
import json, os, sys
response = json.load(sys.stdin)
rows = response["rows"]
grouped = sum(int(row["n"]) for row in rows)
total = int(os.environ["TOTAL"])
if grouped != total:
    raise SystemExit(f"GROUP BY sum {grouped} != count(*) {total}")
mode = response.get("stats", {}).get("phases", {}).get("distributed", {}).get("mode")
if mode not in {"aggregate", "ordered_aggregate"}:
    raise SystemExit(f"GROUP BY did not fan out: distributed mode={mode!r}")
pinned_before = int(os.environ["PINNED_BEFORE"])
pinned_after = int(os.environ["PINNED_AFTER"])
if pinned_after <= pinned_before:
    raise SystemExit(
        f"GROUP BY reached no pinned shard: before={pinned_before} after={pinned_after}"
    )
print(
    f"CROSS_SHARD_GROUP_BY groups={len(rows)} sum={grouped} row_count={total} "
    f"mode={mode} pinned_before={pinned_before} pinned_after={pinned_after} status=ok"
)
'

# GROUP BY raw cannot use the declared dimension summaries, so it guarantees a
# Tier-2 call. The wide-host check above exercises the delta fold. Together they
# make panels 131 and 141 evidence-bearing on this small corpus.
log "prime the Tier-2 and wide-delta group-count histograms"
run_sql 'SELECT raw, count(*) AS n FROM events GROUP BY raw ORDER BY n DESC LIMIT 10' >/dev/null

log "load all benchmark SQL shapes for ${LOAD_SECONDS}s"
python3 - "$ROOT/${BENCH_DIR}/workloads.yaml" >"$TMP_DIR/workloads.tsv" <<'PY'
import json
import os
import re
import sys

path = sys.argv[1]
workloads = []
if os.path.exists(path):
    name = None
    for line in open(path, encoding="utf-8"):
        match = re.match(r"  - name: ([a-zA-Z0-9_-]+)\s*$", line)
        if match:
            name = match.group(1)
            continue
        match = re.match(r"    siglake_sql: (.+)\s*$", line)
        if match and name is not None:
            workloads.append((name, json.loads(match.group(1))))
else:
    # The benchmark corpus is omitted from the published tree. Keep this
    # operational script useful there with the same SQL shape set.
    workloads = [
        ("count_all", "SELECT count(*) AS n FROM events"),
        ("rare_needle_10", "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang0')"),
        ("rare_needle_1000", "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang2')"),
        ("rare_needle_100000", "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang4')"),
        ("common_term", "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'error')"),
        ("phrase", "SELECT count(*) AS n FROM events WHERE match_phrase(raw, 'quantum entanglement cascade')"),
        ("like_substring", "SELECT count(*) AS n FROM events WHERE raw LIKE '%xqzfrag%'"),
        ("date_histogram_1h", "SELECT date_bin(INTERVAL '1 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket"),
        ("date_histogram_24h", "SELECT date_bin(INTERVAL '24 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket"),
        ("date_histogram_full", "SELECT date_bin(INTERVAL '24 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket"),
        ("terms_top10_hosts", "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC, host ASC LIMIT 10"),
        ("newest_first_100", "SELECT timestamp, host, raw FROM events LIMIT 100"),
    ]
if not workloads:
    raise SystemExit("no siglake_sql workloads found")
for name, sql in workloads:
    sql = sql.replace("{{full_window_hours}}", "24")
    if "{{" in sql:
        raise SystemExit(f"unresolved workload template in {name}: {sql}")
    print(f"{name}\t{sql}")
PY
mapfile -t WORKLOADS <"$TMP_DIR/workloads.tsv"
init_scale_evidence
started=$SECONDS
deadline=$((SECONDS + LOAD_SECONDS))
if [[ -n "$MIRROR_RECLAIM_ARM" ]]; then
  MIRROR_RECLAIM_LOAD_STARTED_EPOCH=$(date +%s)
  MIRROR_RECLAIM_NEXT_SAMPLE_EPOCH=$((MIRROR_RECLAIM_LOAD_STARTED_EPOCH + MIRROR_RECLAIM_SAMPLE_SECONDS))
fi
# The window runs on past LOAD_SECONDS only while the scale step still has a
# transition outstanding, and never past this.
SCALE_HARD_DEADLINE=$((deadline + SCALE_GRACE_SECONDS))
next_event=$LOAD_EVENTS
rounds=0
while ((SECONDS < deadline)) || [[ "$SCALE_PHASE" != done ]]; do
  if ! scale_time_left; then
    scale_failure "the ${QUERY_SCALE_BASE}->${QUERY_SCALE_TARGET}->${QUERY_SCALE_BASE} step did not finish within ${SCALE_GRACE_SECONDS}s of the load window: stuck in phase ${SCALE_PHASE} with $(ready_query_pod_count) ready query pods"
    break
  fi
  ingest_events "$next_event" "$STEADY_BATCH"
  next_event=$((next_event + STEADY_BATCH))
  for workload in "${WORKLOADS[@]}"; do
    name="${workload%%$'\t'*}"
    sql="${workload#*$'\t'}"
    run_sql "$sql" >/dev/null || die "workload failed: $name"
  done
  # Keep the Tier-2 histogram moving inside the dashboard's 5m rate window;
  # the benchmark shapes otherwise settle onto the wide Tier-1 host aggregate.
  run_sql 'SELECT raw, count(*) AS n FROM events GROUP BY raw ORDER BY n DESC LIMIT 10' >/dev/null
  rounds=$((rounds + 1))
  advance_query_scale
  if [[ -n "$MIRROR_RECLAIM_ARM" ]] &&
      (( $(date +%s) >= MIRROR_RECLAIM_NEXT_SAMPLE_EPOCH )); then
    capture_mirror_reclaim_sample
    MIRROR_RECLAIM_NEXT_SAMPLE_EPOCH=$((MIRROR_RECLAIM_LAST_SAMPLE_EPOCH + MIRROR_RECLAIM_SAMPLE_SECONDS))
  fi
  sleep 1
done
if [[ -n "$MIRROR_RECLAIM_ARM" ]]; then
  MIRROR_RECLAIM_LOAD_FINISHED_EPOCH=$(date +%s)
fi
printf 'WORKLOADS shapes=%s rounds=%s duration_seconds=%s elapsed_seconds=%s status=ok\n' \
  "${#WORKLOADS[@]}" "$rounds" "$LOAD_SECONDS" "$((SECONDS - started))"
write_scale_evidence ||
  scale_failure "the scale evidence did not hold; see ${SCALE_JSON#"$ROOT/"}"
finish_mirror_reclaim_evidence "$next_event" "$rounds"

log "port-forward Prometheus and wait for its API"
kubectl --context "$KUBE_CONTEXT" -n "$PROM_NAMESPACE" port-forward \
  "service/${PROM_RELEASE}-prometheus" "${PROM_LOCAL_PORT}:9090" \
  >"$TMP_DIR/prometheus.port-forward.log" 2>&1 &
PROM_PF_PID=$!
ACTIVE_PF_PID=$PROM_PF_PID
wait_for_forward "$PROM_PF_PID" "$TMP_DIR/prometheus.port-forward.log"
for _ in $(seq 1 30); do
  curl -fsS "$PROM_URL/-/ready" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS "$PROM_URL/-/ready" >/dev/null || die "Prometheus API did not become ready"

if [[ "$POSTGRES_OUTAGE_PROBE" == 1 ]]; then
  log "run the opt-in persistent-job Postgres outage/reconnect probe"
  KUBE_CONTEXT="$KUBE_CONTEXT" NAMESPACE="$NAMESPACE" PROM_URL="$PROM_URL" \
    RESULTS_DIR="$RESULTS_DIR" "$ROOT/scripts/kind-postgres-outage-probe.sh" ||
    POSTGRES_OUTAGE_FAILURE=$?
  if [[ "$POSTGRES_OUTAGE_FAILURE" -ne 0 ]]; then
    printf 'POSTGRES_OUTAGE_PROBE status=failed exit_status=%s\n' \
      "$POSTGRES_OUTAGE_FAILURE" >&2
  fi
fi

# After every other observation and before the panels, for the reason the
# constants at the top give: this is the one step that changes a tier's replica
# count outside the query-scaling window, so nothing the round already measured
# can have been taken across it.
if [[ "$INGESTER_POD_LABEL_CAPTURE" == 1 ]]; then
  log "run the opt-in ingester per-pod request-series capture"
  capture_ingester_pod_labels "$next_event" || true
fi

log "dashboard panel evidence"
printf 'PANEL_TABLE_BEGIN\n'
query_panel 103 A 'histogram_quantile(0.99, sum by (le, endpoint) (rate(siglake_ingest_request_duration_seconds_bucket{namespace="default"}[5m])))'
query_panel 118 A 'histogram_quantile(0.99, sum by (le) (rate(siglake_compactor_commit_duration_seconds_bucket{namespace="default"}[5m])))'
query_panel 124 A 'histogram_quantile(0.50, sum by (le, endpoint) (rate(siglake_query_request_duration_seconds_bucket{namespace="default"}[5m])))'
query_panel 124 B 'histogram_quantile(0.99, sum by (le, endpoint) (rate(siglake_query_request_duration_seconds_bucket{namespace="default"}[5m])))'
query_panel 131 A 'histogram_quantile(0.50, sum by (le) (rate(siglake_group_count_tier2_files_per_call_bucket{namespace="default"}[5m])))'
query_panel 131 B 'histogram_quantile(0.99, sum by (le) (rate(siglake_group_count_tier2_files_per_call_bucket{namespace="default"}[5m])))'
query_panel 134 B 'histogram_quantile(0.99, sum by (le) (rate(siglake_compactor_mirror_sync_duration_seconds_bucket{namespace="default"}[15m])))'
query_panel 141 A 'histogram_quantile(0.50, sum by (le) (rate(siglake_group_count_deltas_folded_bucket{namespace="default"}[5m])))'
query_panel 141 B 'histogram_quantile(0.99, sum by (le) (rate(siglake_group_count_deltas_folded_bucket{namespace="default"}[5m])))'
printf 'PANEL_TABLE_END\n'

queue_result="$(prometheus_result 'histogram_quantile(0.95, sum(rate(siglake_query_exec_pool_queue_seconds_bucket[5m])) by (le))')"
IFS=$'\t' read -r queue_count queue_sample <<<"$queue_result"
printf 'KEDA_TRIGGER name=p95_queue_wait_seconds series=%s sample=%s target=0.5\n' \
  "$queue_count" "$queue_sample"
[[ "$queue_count" -gt 0 ]] || PANEL_FAILURES=1

log "ScaledObject evidence"
printf 'SCALEDOBJECT_WIDE_BEGIN\n'
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get scaledobject -o wide
printf 'SCALEDOBJECT_WIDE_END\n'
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get scaledobject -o json |
  python3 -c '
import json, sys
items = json.load(sys.stdin).get("items", [])
if not items:
    raise SystemExit("no ScaledObjects found")
errors = []
for item in items:
    name = item["metadata"]["name"]
    status = item.get("status", {})
    conditions = {c.get("type"): c for c in status.get("conditions", [])}
    ready = conditions.get("Ready", {}).get("status", "Unknown")
    active = conditions.get("Active", {}).get("status", "Unknown")
    fallback = conditions.get("Fallback", {}).get("status", "Unknown")
    print(f"SCALEDOBJECT_STATUS name={name} ready={ready} active={active} fallback={fallback}")
    if ready != "True":
        message = conditions.get("Ready", {}).get("message", "no message")
        errors.append(f"{name}: Ready={ready}: {message}")
    if fallback == "True":
        errors.append(f"{name}: fallback is active")
    for trigger, health in status.get("health", {}).items():
        if health.get("status") == "Failing":
            errors.append(f"{name}: trigger {trigger} is failing")
if errors:
    print("ScaledObject errors:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)
    raise SystemExit(1)
'

# Last, and after the panel and ScaledObject evidence rather than beside the
# Postgres probe above: this one rolls the release off the round's own image and
# back twice. Everything the round measures about THIS image is already
# collected and written by the time it runs, so a rollout it drives cannot be
# what made a panel query or a ScaledObject condition read the way it did.
if [[ "$SCHEMA_ROLLBACK_PROBE" == 1 ]]; then
  log "run the opt-in chart/operator schema-rollback arm"
  # The operator the arm installs reads its load signals from the round's own
  # Prometheus, addressed the same way the chart's KEDA trigger is above. The
  # operator chart's default names a Service this cluster does not have, and an
  # operator that cannot query Prometheus holds every replica count, which the
  # arm would meet as a rollout timeout.
  KUBE_CONTEXT="$KUBE_CONTEXT" KIND_CLUSTER_NAME="$CLUSTER_NAME" \
    NAMESPACE="$NAMESPACE" RESULTS_DIR="$RESULTS_DIR" \
    SCHEMA_ROLLBACK_OPERATOR_PROMETHEUS_URL="http://${PROM_RELEASE}-prometheus.${PROM_NAMESPACE}.svc.cluster.local:9090" \
    "$ROOT/scripts/kind-schema-rollback-probe.sh"
fi

# Last because the capture deliberately raises the compactor replica floor and
# commit-batch hold. Nothing from the ordinary round is measured across that
# temporary evidence-only configuration, and teardown follows its verdict.
if [[ "$COMPACTOR_POD_LABEL_CAPTURE" == 1 ]]; then
  log "run the opt-in compactor shared-queue per-pod capture"
  capture_compactor_pod_labels "$next_event" || true
fi

[[ "$PANEL_FAILURES" -eq 0 ]] || die "one or more required panel/trigger queries returned zero series"
[[ "$SCALE_FAILURES" -eq 0 ]] || die "the ${QUERY_SCALE_BASE} -> ${QUERY_SCALE_TARGET} -> ${QUERY_SCALE_BASE} query scaling step failed; see the SCALE_FAILURE lines and ${SCALE_JSON#"$ROOT/"}"
[[ "$POSTGRES_OUTAGE_FAILURE" -eq 0 ]] || die "the requested Postgres outage probe failed with exit status ${POSTGRES_OUTAGE_FAILURE}; see POSTGRES_OUTAGE_EVIDENCE and POSTGRES_OUTAGE_PROBE above"
[[ "$INGESTER_POD_LABEL_FAILURE" -eq 0 ]] || die "the ingester per-pod label capture failed; see the INGESTER_POD_LABEL_FAILURE lines and ${INGESTER_POD_LABEL_JSON#"$ROOT/"}"
[[ "$COMPACTOR_POD_LABEL_FAILURE" -eq 0 ]] || die "the compactor shared-queue capture failed; see the COMPACTOR_POD_LABEL_FAILURE lines and ${COMPACTOR_POD_LABEL_JSON#"$ROOT/"}"
log "kind evidence round passed"
