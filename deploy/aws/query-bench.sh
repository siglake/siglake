#!/usr/bin/env bash
#
# deploy/aws/query-bench.sh — run SQL timing cases from inside the cluster.
#
# Reads a tab-separated query case file with columns:
#   label<TAB>expected_count<TAB>sql<TAB>tags
#
# For each case:
#   1. run SERIAL_RUNS serial requests
#   2. run each matching concurrency spec from CONCURRENCY_SPECS
#   3. capture query pod stats, metrics, and recent logs
#
# Example case file:
#   day1    9994680   SELECT count(*) AS n FROM events WHERE ...    all
#   day3    30000000  SELECT count(*) AS n FROM events WHERE ...    all,wide
#
# Environment overrides:
#   SIGLAKE_RELEASE                 Helm release name      (default: siglake)
#   SIGLAKE_NAMESPACE               Kubernetes namespace   (default: siglake)
#   SIGLAKE_QUERY_CASES_FILE        path to TSV cases file (required)
#   SIGLAKE_QUERY_BENCH_OUTDIR      output directory       (default: /tmp/siglake-query-bench-<ts>)
#   SIGLAKE_QUERY_SERIAL_RUNS       serial passes/case     (default: 4)
#   SIGLAKE_QUERY_CONCURRENCY_SPECS comma list of tag:concurrency:requests
#                                  (default: all:20:40)
#   SIGLAKE_QUERY_RUNNER_IMAGE      temp bench pod image   (default: alpine:3.20)

set -euo pipefail

RELEASE="${SIGLAKE_RELEASE:-siglake}"
NAMESPACE="${SIGLAKE_NAMESPACE:-siglake}"
CASES_FILE="${SIGLAKE_QUERY_CASES_FILE:-}"
OUTDIR="${SIGLAKE_QUERY_BENCH_OUTDIR:-/tmp/siglake-query-bench-$(date +%Y%m%d-%H%M%S)}"
SERIAL_RUNS="${SIGLAKE_QUERY_SERIAL_RUNS:-4}"
CONCURRENCY_SPECS="${SIGLAKE_QUERY_CONCURRENCY_SPECS:-all:20:40}"
RUNNER_IMAGE="${SIGLAKE_QUERY_RUNNER_IMAGE:-alpine:3.20}"

QUERY_SVC="${RELEASE}-query"
ENDPOINT="http://${QUERY_SVC}:8089/api/v1/sql"
RUNNER="siglake-query-bench-$$"

log() { printf '==> %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

cleanup() {
  kubectl -n "$NAMESPACE" delete pod "$RUNNER" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

[[ -n "$CASES_FILE" ]] || die "set SIGLAKE_QUERY_CASES_FILE"
[[ -f "$CASES_FILE" ]] || die "cases file not found: $CASES_FILE"

mkdir -p "$OUTDIR"
SERIAL_TSV="$OUTDIR/serial.tsv"
CONCURRENCY_TSV="$OUTDIR/concurrency.tsv"
: > "$SERIAL_TSV"
: > "$CONCURRENCY_TSV"

query_pod_name() {
  local pod
  pod="$(kubectl -n "$NAMESPACE" get pod \
    -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=query" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$pod" ]]; then
    pod="$(kubectl -n "$NAMESPACE" get pod \
      -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=query-server" \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  fi
  [[ -n "$pod" ]] || die "could not locate query pod for release $RELEASE"
  printf '%s' "$pod"
}

has_tag() {
  local wanted="$1" tags="${2:-}"
  [[ "$wanted" == "all" ]] && return 0
  IFS=',' read -r -a tag_list <<<"$tags"
  for tag in "${tag_list[@]}"; do
    [[ "$tag" == "$wanted" ]] && return 0
  done
  return 1
}

log "creating in-cluster runner pod $RUNNER"
kubectl -n "$NAMESPACE" run "$RUNNER" \
  --image="$RUNNER_IMAGE" \
  --restart=Never \
  --command -- sh -lc 'apk add --no-cache bash curl jq coreutils >/dev/null && sleep 3600' >/dev/null
kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$RUNNER" --timeout=5m >/dev/null

log "installing runner script"
kubectl -n "$NAMESPACE" exec -i "$RUNNER" -- sh -lc 'cat > /tmp/query-bench-runner.sh && chmod +x /tmp/query-bench-runner.sh' <<'SH'
#!/usr/bin/env bash
set -euo pipefail

run_one() {
  local tmp response_time
  tmp="$(mktemp)"
  response_time="$(curl -sS -o "$tmp" -w '%{time_total}' \
    -H 'content-type: application/json' \
    "$ENDPOINT" \
    -d "{\"query\":\"$QUERY\"}")"
  jq -e --argjson expected "$EXPECTED" '.rows[0].n == $expected' "$tmp" >/dev/null
  rm -f "$tmp"
  printf '%s\n' "$response_time"
}

case "$MODE" in
  serial)
    for i in $(seq 1 "$RUNS"); do
      printf 'serial\t%s\t%s\t%s\n' "$LABEL" "$i" "$(run_one)"
    done
    ;;
  concurrency)
    run_worker() {
      local seqno="$1"
      printf '%s\t%s\n' "$seqno" "$(run_one)"
    }
    export -f run_one run_worker
    seq 1 "$REQUESTS" \
      | xargs -P "$CONCURRENCY" -I{} bash -lc 'run_worker "$1"' _ {} \
      | sort -n \
      | awk -v label="$LABEL" -v conc="$CONCURRENCY" 'BEGIN{FS="\t"} {printf "concurrency\t%s\t%s\t%s\t%s\n", label, conc, $1, $2}'
    ;;
  *)
    printf 'unknown MODE=%s\n' "$MODE" >&2
    exit 1
    ;;
esac
SH

while IFS=$'\t' read -r label expected sql tags; do
  [[ -n "${label:-}" ]] || continue
  [[ "${label:0:1}" == "#" ]] && continue

  log "serial: $label"
  kubectl -n "$NAMESPACE" exec "$RUNNER" -- env \
    MODE=serial \
    LABEL="$label" \
    EXPECTED="$expected" \
    QUERY="$sql" \
    RUNS="$SERIAL_RUNS" \
    ENDPOINT="$ENDPOINT" \
    bash /tmp/query-bench-runner.sh >>"$SERIAL_TSV"

  IFS=',' read -r -a specs <<<"$CONCURRENCY_SPECS"
  for spec in "${specs[@]}"; do
    IFS=':' read -r spec_tag spec_concurrency spec_requests <<<"$spec"
    has_tag "$spec_tag" "${tags:-}" || continue
    log "concurrency: $label tag=$spec_tag c=$spec_concurrency requests=$spec_requests"
    kubectl -n "$NAMESPACE" exec "$RUNNER" -- env \
      MODE=concurrency \
      LABEL="$label" \
      EXPECTED="$expected" \
      QUERY="$sql" \
      CONCURRENCY="$spec_concurrency" \
      REQUESTS="$spec_requests" \
      ENDPOINT="$ENDPOINT" \
      bash /tmp/query-bench-runner.sh >>"$CONCURRENCY_TSV"
  done
done <"$CASES_FILE"

QUERY_POD="$(query_pod_name)"
log "capturing query pod stats from $QUERY_POD"
kubectl -n "$NAMESPACE" exec "$QUERY_POD" -- sh -lc '
echo "# /proc/1/status"
grep -E "VmRSS|VmHWM|Threads" /proc/1/status || true
echo
echo "# cgroup"
for path in /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.peak /sys/fs/cgroup/cpu.stat; do
  if [ -f "$path" ]; then
    echo "## $path"
    cat "$path"
    echo
  fi
done
' >"$OUTDIR/query_pod_stats.txt"

kubectl -n "$NAMESPACE" exec "$QUERY_POD" -- sh -lc 'curl -sf localhost:9105/metrics' \
  >"$OUTDIR/query_metrics.txt"
kubectl -n "$NAMESPACE" logs "$QUERY_POD" --tail=400 >"$OUTDIR/query.log"

cat <<EOF
query bench complete
  outdir: $OUTDIR
  serial: $SERIAL_TSV
  concurrency: $CONCURRENCY_TSV
  query pod stats: $OUTDIR/query_pod_stats.txt
  metrics: $OUTDIR/query_metrics.txt
  log: $OUTDIR/query.log
EOF
