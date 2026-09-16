#!/usr/bin/env bash
# Opt-in chart/operator schema-rollback arm for a throwaway kind round.
#
# WHAT THIS ANSWERS. README.md ships the limitation "No rollback has been
# qualified against an actual older image". `crates/siglake-storage/tests/
# storage/schema_rollback.rs` pins the MECHANISM a rolled-back writer depends
# on, but it runs one binary against a hand-widened table and says so in its
# own module doc. The question left over needs two images that differ in their
# DECLARED column set, a real `helm rollback`, and a real `spec.image` revert.
#
# THE TWO IMAGES. Image A is the round's ordinary image. Image B is a second
# build of the SAME checkout with `siglake-core/experimental-schema-rollback-
# probe` on, which declares one extra nullable `events` column
# (`rollback_probe`) and populates it. Nothing else differs, and nothing about
# it reaches a release image: `deploy/Dockerfile` builds with no `--features`
# unless SIGLAKE_CARGO_FEATURES is passed, and the chart defaults never name it.
# Both image ids and the feature string go into the evidence.
#
# THE ARMS.
#   chart:    install A (already up) → ingest → upgrade to B (the pre-upgrade
#             migration Job runs, the table widens) → ingest under B →
#             `helm rollback` to A (no migration Job, A ingests, the rows B
#             wrote keep their values, the rows A writes read null, the column
#             stays) → `helm upgrade` forward to B again.
#   operator: revert `spec.image` with `spec.schemaVersion` unchanged. The
#             operator names its Job from the schema version AND the template
#             digest (render.rs) and retains a finished Job for a day, so a
#             revert either REUSES the retained Job for the old image or, once
#             it has been reaped, creates it again under the same name. Both
#             are exercised; neither is "a fresh Job on every revert".
#
# Every row count goes through the distributed `/api/v1/sql`, and every one of
# them carries a cross-shard `GROUP BY` whose per-key counts must sum to it.
#
# The evidence is graded by scripts/grade-kind-schema-rollback.py into
# results/schema-rollback.json. A lane cannot run kind; the offline half of
# this file is scripts/check-kind-schema-rollback-evidence.sh, which runs it
# end to end under PATH stand-ins and compares the recorded command sequence
# against the steps above.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/kind-common.bash
source "$ROOT/scripts/kind-common.bash"

KUBE_CONTEXT=${KUBE_CONTEXT:?set KUBE_CONTEXT}
CLUSTER_NAME=${KIND_CLUSTER_NAME:-siglake}
NAMESPACE=${NAMESPACE:-default}
RELEASE=${SCHEMA_ROLLBACK_RELEASE:-siglake}
RESULTS_DIR=${RESULTS_DIR:-$ROOT/results}
EVIDENCE_JSON="$RESULTS_DIR/schema-rollback.json"
# The operator's own log, retained beside the evidence. kind-round.sh's failure
# diagnostics dump logs for pods that are not Ready, and an operator that cannot
# reach Prometheus is Ready while it holds every replica count, so nothing else
# in the round keeps the line that says so.
OPERATOR_LOG="$RESULTS_DIR/schema-rollback-operator.log"

INGEST_URL=${SCHEMA_ROLLBACK_INGEST_URL:-http://127.0.0.1:8088/v1/logs}
QUERY_URL=${SCHEMA_ROLLBACK_QUERY_URL:-http://127.0.0.1:8089/api/v1/sql}

IMAGE_A=${SIGLAKE_KIND_IMAGE_TAG:-siglake:kind}
IMAGE_B=${SCHEMA_ROLLBACK_IMAGE_B:-siglake:kind-rollback-probe}
# The one feature that makes image B a different declared schema. Named here
# and asserted against crates/siglake-core/Cargo.toml by the offline checker,
# so a rename there cannot leave this building image A twice.
PROBE_FEATURE=siglake-core/experimental-schema-rollback-probe
PROBE_COLUMN=rollback_probe
PROBE_VALUE=1
# Its own cargo target cache: the mount in deploy/Dockerfile is not keyed by
# feature set, so sharing `default` would leave feature-built objects behind
# for the next release image build to link.
BUILD_CACHE_ID=${SCHEMA_ROLLBACK_BUILD_CACHE_ID:-rollback-probe}

OPERATOR_IMAGE=${SCHEMA_ROLLBACK_OPERATOR_IMAGE:-siglake-operator:kind}
OPERATOR_NAMESPACE=${SCHEMA_ROLLBACK_OPERATOR_NAMESPACE:-siglake-rollback}
OPERATOR_RELEASE=${SCHEMA_ROLLBACK_OPERATOR_RELEASE:-siglake-operator-rollback}
OPERATOR_METRICS_PORT=${SCHEMA_ROLLBACK_OPERATOR_METRICS_PORT:-19191}
CR_NAME=${SCHEMA_ROLLBACK_CR_NAME:-rollback}
CR_SCHEMA_VERSION=${SCHEMA_ROLLBACK_CR_SCHEMA_VERSION:-1}
# The operator arm's own warehouse prefix and tenant namespace: it must not
# migrate or widen the table the chart arm is measuring.
OPERATOR_WAREHOUSE_URL=${SCHEMA_ROLLBACK_OPERATOR_WAREHOUSE_URL:-s3://siglake-warehouse/operator-rollback/}
OPERATOR_CATALOG_URI=${SCHEMA_ROLLBACK_OPERATOR_CATALOG_URI:-postgres://siglake:siglake@postgres.default.svc.cluster.local:5432/siglake}
OPERATOR_TENANT_NAMESPACE=${SCHEMA_ROLLBACK_OPERATOR_TENANT_NAMESPACE:-oprollback}
OPERATOR_S3_ENDPOINT=${SCHEMA_ROLLBACK_OPERATOR_S3_ENDPOINT:-http://minio.default.svc.cluster.local:9000}
OPERATOR_AWS_REGION=${SCHEMA_ROLLBACK_OPERATOR_AWS_REGION:-us-east-1}
# The Prometheus the operator reads its load signals from. The chart's default
# names the prometheus-community chart's Service; the round installs
# kube-prometheus-stack, whose Service is `<release>-prometheus` on 9090, and
# passes its address in. Left at the chart default the reconciler holds every
# replica count and logs `prometheus query failed; HOLDING` on every cycle,
# which is a rollout this probe would time out waiting for.
OPERATOR_PROMETHEUS_URL=${SCHEMA_ROLLBACK_OPERATOR_PROMETHEUS_URL:-http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090}

INGEST_BATCH=${SCHEMA_ROLLBACK_INGEST_BATCH:-200}
CONVERGE_TIMEOUT_SECONDS=${SCHEMA_ROLLBACK_CONVERGE_TIMEOUT_SECONDS:-300}
CONVERGE_INTERVAL_SECONDS=${SCHEMA_ROLLBACK_CONVERGE_INTERVAL_SECONDS:-5}
HELM_TIMEOUT=${SCHEMA_ROLLBACK_HELM_TIMEOUT:-10m}
RECONCILE_TIMEOUT_SECONDS=${SCHEMA_ROLLBACK_RECONCILE_TIMEOUT_SECONDS:-300}

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/siglake-schema-rollback.XXXXXX")
CHART_STEPS="$TMP_DIR/chart-steps.jsonl"
OPERATOR_STEPS="$TMP_DIR/operator-steps.jsonl"
ACTIVE_PF_PID=

log() { printf '==> schema-rollback: %s\n' "$*" >&2; }
die() {
  printf 'ERROR: schema-rollback: %s\n' "$*" >&2
  exit 1
}
iso_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

stop_forward() {
  [[ -n "$ACTIVE_PF_PID" ]] || return 0
  kill "$ACTIVE_PF_PID" 2>/dev/null || true
  wait "$ACTIVE_PF_PID" 2>/dev/null || true
  ACTIVE_PF_PID=
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  stop_forward
  rm -rf -- "$TMP_DIR"
  exit "$status"
}
trap cleanup EXIT INT TERM

for tool in curl docker git helm kind kubectl python3; do
  command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool"
done
for value in "$INGEST_BATCH" "$CONVERGE_TIMEOUT_SECONDS" \
  "$CONVERGE_INTERVAL_SECONDS" "$RECONCILE_TIMEOUT_SECONDS" \
  "$CR_SCHEMA_VERSION" "$OPERATOR_METRICS_PORT"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "probe counts and durations must be positive integers: $value"
done
((INGEST_BATCH <= 20000)) || die "SCHEMA_ROLLBACK_INGEST_BATCH exceeds the 20000-event safety cap"
((CONVERGE_TIMEOUT_SECONDS <= 900)) || die "SCHEMA_ROLLBACK_CONVERGE_TIMEOUT_SECONDS exceeds the 900s safety cap"
((RECONCILE_TIMEOUT_SECONDS <= 900)) || die "SCHEMA_ROLLBACK_RECONCILE_TIMEOUT_SECONDS exceeds the 900s safety cap"
[[ "$OPERATOR_PROMETHEUS_URL" =~ ^https?://[^[:space:]]+$ ]] ||
  die "SCHEMA_ROLLBACK_OPERATOR_PROMETHEUS_URL is not an http(s) URL: $OPERATOR_PROMETHEUS_URL"

mkdir -p "$RESULTS_DIR"
: >"$CHART_STEPS"
: >"$OPERATOR_STEPS"
: >"$OPERATOR_LOG"

kc() { kubectl --context "$KUBE_CONTEXT" --request-timeout=60s "$@"; }
hc() { helm --kube-context "$KUBE_CONTEXT" "$@"; }

# Append the operator Deployment's whole log to $OPERATOR_LOG, under a header
# naming what the probe had just done. A failure to read it is recorded in the
# file rather than raised: this is diagnostics, and losing them must not be what
# ends the run.
retain_operator_logs() {
  local step=$1
  {
    printf -- '--- %s %s\n' "$(iso_now)" "$step"
    kc -n "$OPERATOR_NAMESPACE" logs "deploy/$OPERATOR_RELEASE-siglake-operator" \
      --all-containers --tail=-1 2>&1 || printf 'ERROR: could not read the operator log\n'
  } >>"$OPERATOR_LOG"
}

# --- image B -----------------------------------------------------------------

log "build image B ($IMAGE_B) from this checkout with $PROBE_FEATURE"
docker build \
  --build-arg "SIGLAKE_CARGO_FEATURES=$PROBE_FEATURE" \
  --build-arg "BUILD_CACHE_ID=$BUILD_CACHE_ID" \
  -t "$IMAGE_B" -f "$ROOT/deploy/Dockerfile" "$ROOT"
log "load image B into the cluster"
kind load docker-image "$IMAGE_B" --name "$CLUSTER_NAME"

log "build and load the operator image ($OPERATOR_IMAGE)"
docker build -t "$OPERATOR_IMAGE" -f "$ROOT/deploy/Dockerfile.operator" "$ROOT"
kind load docker-image "$OPERATOR_IMAGE" --name "$CLUSTER_NAME"

image_id() {
  docker image inspect --format '{{.Id}}' "$1" 2>/dev/null || printf ''
}
IMAGE_A_ID=$(image_id "$IMAGE_A")
IMAGE_B_ID=$(image_id "$IMAGE_B")
OPERATOR_IMAGE_ID=$(image_id "$OPERATOR_IMAGE")
[[ -n "$IMAGE_A_ID" ]] || die "cannot read the image id of $IMAGE_A"
[[ -n "$IMAGE_B_ID" ]] || die "cannot read the image id of $IMAGE_B"
[[ "$IMAGE_A_ID" != "$IMAGE_B_ID" ]] ||
  die "$IMAGE_A and $IMAGE_B are the same image ($IMAGE_A_ID) -- the feature build produced no new binary, so there is no rollback to measure"

# --- the chart arm -----------------------------------------------------------

release_revision() {
  hc status "$RELEASE" -n "$NAMESPACE" -o json >"$TMP_DIR/release-status.json"
  python3 - "$TMP_DIR/release-status.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], encoding="utf-8")).get("version", 0))
PY
}

ingest_events() {
  local start=$1 count=$2 payload="$TMP_DIR/otlp-payload.json"
  python3 - "$start" "$count" >"$payload" <<'PY'
import json
import sys

start, count = map(int, sys.argv[1:])
resource_logs = []
for i in range(start, start + count):
    resource_logs.append({
        "resource": {"attributes": [
            {"key": "host.name", "value": {"stringValue": f"rollback-host-{i % 8:02d}"}},
            {"key": "service.name", "value": {"stringValue": "schema-rollback"}},
        ]},
        "scopeLogs": [{
            "scope": {"name": "schema-rollback"},
            "logRecords": [{
                "body": {"stringValue": f"schema-rollback event={i}"},
                "attributes": [
                    {"key": "sourcetype", "value": {"stringValue": "kind:json"}},
                    {"key": "index", "value": {"stringValue": "main"}},
                ],
            }],
        }],
    })
json.dump({"resourceLogs": resource_logs}, sys.stdout, separators=(",", ":"))
PY
  post_json_file "$INGEST_URL" "$payload" -H 'X-Scope-OrgID: default' >/dev/null
}

# Run one SQL statement against the DISTRIBUTED endpoint. $1 = statement,
# $2 = output file. Never /api/v1/sql/local: a per-pod answer cannot show a
# cross-shard merge, which is the whole point of the row checks here.
run_sql() {
  local statement=$1 output=$2 payload="$TMP_DIR/sql-payload.json"
  python3 -c 'import json,sys; print(json.dumps({"query": sys.argv[1]}))' "$statement" >"$payload"
  post_json_file "$QUERY_URL" "$payload" -H 'X-Scope-OrgID: default' >"$output"
}

# Poll count(*) and GROUP BY together until count reaches $1 and the grouped
# sum is exactly that count. Leave both converged responses in $TMP_DIR for
# emit_chart_step. For rollback_probe observations, require the null bucket
# and, when $4 is 1, the value written by image B. $3 names retained timeout
# diagnostics.
converge_total() {
  local expected=$1 key=$2 label=$3 require_probe_value=$4
  local deadline=$((SECONDS + CONVERGE_TIMEOUT_SECONDS)) observation=missing
  while ((SECONDS < deadline)); do
    run_sql 'SELECT count(*) AS n FROM events' "$TMP_DIR/count.json" 2>/dev/null || true
    run_sql "SELECT ${key}, sum(1) AS n FROM events GROUP BY ${key}" \
      "$TMP_DIR/group-by.json" 2>/dev/null || true
    if observation=$(python3 - "$TMP_DIR/count.json" "$TMP_DIR/group-by.json" \
      "$expected" "$key" "$PROBE_COLUMN" "$PROBE_VALUE" \
      "$require_probe_value" <<'PY'
import json, sys

count_path, group_path, expected, key, probe_column, probe_value, require_probe = sys.argv[1:]
expected = int(expected)
require_probe = require_probe == "1"


def load(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return {}


try:
    count = int(load(count_path)["rows"][0]["n"])
except (ValueError, KeyError, IndexError, TypeError):
    count = None

rows = load(group_path).get("rows")
group_sum = None
buckets = set()
if isinstance(rows, list):
    try:
        group_sum = sum(int(row["n"]) for row in rows)
        buckets = {
            None if row.get(key) is None else str(row.get(key))
            for row in rows
        }
    except (ValueError, KeyError, TypeError):
        group_sum = None

required = []
if key == probe_column:
    required.append(None)
    if require_probe:
        required.append(str(probe_value))
missing = ["null" if value is None else str(value) for value in required if value not in buckets]
print(
    f"count={count if count is not None else 'missing'} "
    f"grouped={group_sum if group_sum is not None else 'missing'} "
    f"missing_buckets={','.join(missing) if missing else 'none'}"
)
ready = (
    count is not None
    and count >= expected
    and group_sum == count
    and not missing
)
raise SystemExit(0 if ready else 1)
PY
    ); then
      log "  converged $observation (expected at least $expected)"
      return 0
    fi
    sleep "$CONVERGE_INTERVAL_SECONDS"
  done
  cp "$TMP_DIR/count.json" "$RESULTS_DIR/schema-rollback-${label}-count.json" 2>/dev/null || true
  cp "$TMP_DIR/group-by.json" "$RESULTS_DIR/schema-rollback-${label}-group-by.json" 2>/dev/null || true
  die "the distributed row observations did not converge within ${CONVERGE_TIMEOUT_SECONDS}s (expected at least $expected; last: $observation; responses: schema-rollback-${label}-{count,group-by}.json)"
}

# $1 = step name, $2 = the image this step expects to be running, $3 = release
# revision, $4 = GROUP BY key column, $5 = events ingested in this step,
# $6 = what the probe did. Appends one object to $CHART_STEPS.
emit_chart_step() {
  local name=$1 image=$2 revision=$3 key=$4 ingested=$5 action=$6
  # converge_total left a non-count aggregate the coordinator's Tier-1 battery
  # cannot answer, so its distributed phase stats show that the merge is real.
  kc -n "$NAMESPACE" get jobs \
    -l 'app.kubernetes.io/instance=siglake,app.kubernetes.io/component=migrate-schema' \
    -o json >"$TMP_DIR/jobs.json"
  kc -n "$NAMESPACE" get pods \
    -l 'app.kubernetes.io/instance=siglake' \
    -o json >"$TMP_DIR/pods.json"
  python3 - "$name" "$image" "$revision" "$key" "$ingested" "$action" "$(iso_now)" \
    "$QUERY_URL" "$PROBE_COLUMN" "$TMP_DIR/count.json" "$TMP_DIR/group-by.json" \
    "$TMP_DIR/jobs.json" "$TMP_DIR/pods.json" "$CHART_STEPS" <<'PY'
import json
import sys

(
    name, image, revision, key, ingested, action, at, endpoint, probe_column,
    count_path, group_path, jobs_path, pods_path, output,
) = sys.argv[1:]


def load(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return {}


count = load(count_path)
group = load(group_path)
try:
    total = int(count["rows"][0]["n"])
except (KeyError, IndexError, TypeError, ValueError):
    total = None

groups = []
group_rows = group.get("rows")
if isinstance(group_rows, list):
    for row in group_rows:
        if not isinstance(row, dict):
            continue
        value = row.get(key)
        groups.append({
            "key": None if value is None else str(value),
            "n": row.get("n"),
        })
mode = (
    group.get("stats", {}).get("phases", {}).get("distributed", {}).get("mode")
    if isinstance(group.get("stats"), dict)
    else None
)

# The column is present exactly when the GROUP BY over it came back at all:
# a narrow table answers `GROUP BY rollback_probe` with an error, not rows.
probe_present = key == probe_column and isinstance(group_rows, list)

jobs = []
for item in load(jobs_path).get("items", []) or []:
    meta = item.get("metadata", {})
    status = item.get("status", {})
    containers = item.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [{}])
    jobs.append({
        "name": meta.get("name"),
        "uid": meta.get("uid"),
        "created_at": meta.get("creationTimestamp"),
        "completed_at": status.get("completionTime"),
        "succeeded": status.get("succeeded", 0),
        "failed": status.get("failed", 0),
        "image": (containers[0] if containers else {}).get("image"),
    })
jobs.sort(key=lambda row: row.get("created_at") or "")

pods = []
for item in load(pods_path).get("items", []) or []:
    meta = item.get("metadata", {})
    labels = meta.get("labels", {})
    spec = item.get("spec", {}).get("containers", [{}])
    pods.append({
        "pod": meta.get("name"),
        "component": labels.get("app.kubernetes.io/component"),
        "image": (spec[0] if spec else {}).get("image"),
        "phase": item.get("status", {}).get("phase"),
    })
pods.sort(key=lambda row: row.get("pod") or "")

step = {
    "name": name,
    "at": at,
    "action": action,
    "expected_image": image,
    "revision": int(revision),
    "ingested": int(ingested),
    "rows": {
        "endpoint": endpoint,
        "total": total,
        "group_by_key": key,
        "groups": groups,
        "distributed_mode": mode,
    },
    "probe_column_present": probe_present,
    "migration_jobs": jobs,
    "pods": pods,
}
with open(output, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(step) + "\n")
PY
}

CHART_START_REVISION=$(release_revision)
[[ "$CHART_START_REVISION" =~ ^[0-9]+$ ]] && ((CHART_START_REVISION > 0)) ||
  die "cannot read the current revision of release $RELEASE in $NAMESPACE"
log "chart arm starts at revision $CHART_START_REVISION on $IMAGE_A"

INGESTED=0
log "step 1/6: ingest $INGEST_BATCH events under image A"
converge_total 0 host baseline 0
BASELINE_ROWS=$(python3 -c 'import json,sys; print(int(json.load(open(sys.argv[1]))["rows"][0]["n"]))' "$TMP_DIR/count.json")
ingest_events 900000 "$INGEST_BATCH"
INGESTED=$((INGESTED + INGEST_BATCH))
converge_total $((BASELINE_ROWS + INGESTED)) host ingest-under-a 0
emit_chart_step ingest_under_a "$IMAGE_A" "$CHART_START_REVISION" host \
  "$INGEST_BATCH" "ingest under the installed image"

log "step 2/6: upgrade the release to image B (the pre-upgrade migration Job runs)"
hc upgrade "$RELEASE" "$ROOT/deploy/helm/siglake" \
  --namespace "$NAMESPACE" \
  --reuse-values \
  --set-string image.repository="${IMAGE_B%%:*}" \
  --set-string image.tag="${IMAGE_B##*:}" \
  --wait --timeout "$HELM_TIMEOUT"
REVISION_B=$(release_revision)
converge_total $((BASELINE_ROWS + INGESTED)) "$PROBE_COLUMN" upgrade-to-b 0
emit_chart_step upgrade_to_b "$IMAGE_B" "$REVISION_B" "$PROBE_COLUMN" 0 \
  "helm upgrade to image B"

log "step 3/6: ingest $INGEST_BATCH events under image B"
ingest_events 910000 "$INGEST_BATCH"
INGESTED=$((INGESTED + INGEST_BATCH))
converge_total $((BASELINE_ROWS + INGESTED)) "$PROBE_COLUMN" ingest-under-b 1
emit_chart_step ingest_under_b "$IMAGE_B" "$REVISION_B" "$PROBE_COLUMN" \
  "$INGEST_BATCH" "ingest under image B"

log "step 4/6: helm rollback to revision $REVISION_B's predecessor (image A)"
hc rollback "$RELEASE" "$CHART_START_REVISION" \
  --namespace "$NAMESPACE" \
  --wait --timeout "$HELM_TIMEOUT"
REVISION_ROLLBACK=$(release_revision)
converge_total $((BASELINE_ROWS + INGESTED)) "$PROBE_COLUMN" rollback-to-a 1
emit_chart_step rollback_to_a "$IMAGE_A" "$REVISION_ROLLBACK" "$PROBE_COLUMN" 0 \
  "helm rollback to the pre-upgrade revision"

log "step 5/6: ingest $INGEST_BATCH events under the rolled-back image A"
ingest_events 920000 "$INGEST_BATCH"
INGESTED=$((INGESTED + INGEST_BATCH))
converge_total $((BASELINE_ROWS + INGESTED)) "$PROBE_COLUMN" ingest-after-rollback 1
emit_chart_step ingest_after_rollback "$IMAGE_A" "$REVISION_ROLLBACK" "$PROBE_COLUMN" \
  "$INGEST_BATCH" "ingest under the rolled-back image A"

log "step 6/6: upgrade forward to image B again"
hc upgrade "$RELEASE" "$ROOT/deploy/helm/siglake" \
  --namespace "$NAMESPACE" \
  --reuse-values \
  --set-string image.repository="${IMAGE_B%%:*}" \
  --set-string image.tag="${IMAGE_B##*:}" \
  --wait --timeout "$HELM_TIMEOUT"
REVISION_FORWARD=$(release_revision)
converge_total $((BASELINE_ROWS + INGESTED)) "$PROBE_COLUMN" upgrade-forward-to-b 1
emit_chart_step upgrade_forward_to_b "$IMAGE_B" "$REVISION_FORWARD" "$PROBE_COLUMN" 0 \
  "helm upgrade forward to image B"

# --- the operator arm --------------------------------------------------------

log "install the operator into $OPERATOR_NAMESPACE"
kc create namespace "$OPERATOR_NAMESPACE" --dry-run=client -o yaml >"$TMP_DIR/operator-namespace.yaml"
kc apply -f "$TMP_DIR/operator-namespace.yaml"
hc upgrade --install "$OPERATOR_RELEASE" "$ROOT/deploy/helm/siglake-operator" \
  --namespace "$OPERATOR_NAMESPACE" \
  --set-string image.repository="${OPERATOR_IMAGE%%:*}" \
  --set-string image.tag="${OPERATOR_IMAGE##*:}" \
  --set-string image.pullPolicy=IfNotPresent \
  --set-string prometheus.url="$OPERATOR_PROMETHEUS_URL" \
  --wait --timeout "$HELM_TIMEOUT"

# $1 = image the CR should ask for.
write_cr() {
  python3 - "$1" "$CR_NAME" "$OPERATOR_NAMESPACE" "$CR_SCHEMA_VERSION" \
    "$OPERATOR_WAREHOUSE_URL" "$OPERATOR_CATALOG_URI" "$OPERATOR_AWS_REGION" \
    "$OPERATOR_S3_ENDPOINT" "$OPERATOR_TENANT_NAMESPACE" >"$TMP_DIR/cluster.yaml" <<'PY'
import json
import sys

(
    image, name, namespace, schema_version, warehouse_url, catalog_uri, region,
    endpoint, tenant_namespace,
) = sys.argv[1:]
# JSON is valid YAML, so the CR is emitted without a YAML dependency (this runs
# in the round's python, which installs nothing).
document = {
    "apiVersion": "siglake.limnion.ai/v1alpha1",
    "kind": "SiglakeCluster",
    "metadata": {"name": name, "namespace": namespace},
    "spec": {
        "image": image,
        "schemaVersion": int(schema_version),
        "warehouseUrl": warehouse_url,
        "catalogUri": catalog_uri,
        "awsRegion": region,
        # The tenant namespace keeps this cluster's `events` table out of the
        # chart arm's, and the MinIO credentials are the kind values'.
        "extraEnv": [
            {"name": "AWS_ACCESS_KEY_ID", "value": "minioadmin"},
            {"name": "AWS_SECRET_ACCESS_KEY", "value": "minioadmin"},
            {"name": "AWS_ENDPOINT_URL", "value": endpoint},
            {"name": "AWS_S3_FORCE_PATH_STYLE", "value": "true"},
            {"name": "SIGLAKE_TENANT_NAMESPACE", "value": tenant_namespace},
        ],
        "autoscaling": {
            "ingester": {"min": 1, "max": 1, "target": 1000.0},
            "compactor": {"min": 1, "max": 1, "target": 5.0},
            "query": {"min": 1, "max": 1, "target": 4.0},
        },
        "storage": {"walSize": "1Gi", "walAccessMode": "ReadWriteOnce"},
    },
}
json.dump(document, sys.stdout, indent=2)
PY
}

# The Job the operator renders for the CR's current image, or nothing. Written
# to $TMP_DIR/operator-jobs.json for the emitter below.
read_operator_jobs() {
  kc -n "$OPERATOR_NAMESPACE" get jobs -o json >"$TMP_DIR/operator-jobs.json"
}

# Snapshot the CR and every workload and pod carrying the operator's labels.
# The same files feed the bounded rollout wait, timeout diagnostics and the
# retained step, so the grader sees the state that satisfied the wait.
read_operator_rollout() {
  local selector="app.kubernetes.io/instance=$CR_NAME,app.kubernetes.io/managed-by=siglake-operator"
  kc -n "$OPERATOR_NAMESPACE" get siglakecluster "$CR_NAME" -o json \
    >"$TMP_DIR/operator-cr.json"
  kc -n "$OPERATOR_NAMESPACE" get deployments -l "$selector" -o json \
    >"$TMP_DIR/operator-deployments.json"
  kc -n "$OPERATOR_NAMESPACE" get statefulsets -l "$selector" -o json \
    >"$TMP_DIR/operator-statefulsets.json"
  kc -n "$OPERATOR_NAMESPACE" get pods -l "$selector" -o json \
    >"$TMP_DIR/operator-pods.json"
}

# Exit zero only when the operator has observed the current CR generation and
# all core workloads and selected pods have completed their rollout to $1.
operator_rollout_ready() {
  local image=$1
  python3 - "$image" "$TMP_DIR/operator-cr.json" \
    "$TMP_DIR/operator-deployments.json" "$TMP_DIR/operator-statefulsets.json" \
    "$TMP_DIR/operator-pods.json" <<'PY'
import json
import sys

image, cr_path, deployments_path, statefulsets_path, pods_path = sys.argv[1:]


def load(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return {}


cr = load(cr_path)
generation = (cr.get("metadata") or {}).get("generation")
observed = (cr.get("status") or {}).get("observedGeneration")
if not isinstance(generation, int) or generation < 1 or observed != generation:
    raise SystemExit(1)

deployments = load(deployments_path).get("items", []) or []
statefulsets = load(statefulsets_path).get("items", []) or []
pods = load(pods_path).get("items", []) or []
if not deployments or not statefulsets or not pods:
    raise SystemExit(1)

components = set()
deployment_components = set()
desired_by_component = {}
for item in deployments:
    meta = item.get("metadata") or {}
    spec = item.get("spec") or {}
    status = item.get("status") or {}
    template = (spec.get("template") or {}).get("spec") or {}
    containers = template.get("containers") or []
    replicas = spec.get("replicas", 1)
    component = (meta.get("labels") or {}).get("app.kubernetes.io/component")
    components.add(component)
    deployment_components.add(component)
    desired_by_component[component] = replicas
    if (
        [container.get("image") for container in containers] != [image]
        or status.get("observedGeneration") != meta.get("generation")
        or status.get("updatedReplicas", 0) != replicas
        or status.get("availableReplicas", 0) != replicas
        or status.get("readyReplicas", 0) != replicas
    ):
        raise SystemExit(1)

statefulset_components = set()
for item in statefulsets:
    meta = item.get("metadata") or {}
    spec = item.get("spec") or {}
    status = item.get("status") or {}
    template = (spec.get("template") or {}).get("spec") or {}
    containers = template.get("containers") or []
    replicas = spec.get("replicas", 1)
    component = (meta.get("labels") or {}).get("app.kubernetes.io/component")
    components.add(component)
    statefulset_components.add(component)
    desired_by_component[component] = replicas
    if (
        [container.get("image") for container in containers] != [image]
        or status.get("observedGeneration") != meta.get("generation")
        or status.get("currentReplicas", 0) != replicas
        or status.get("updatedReplicas", 0) != replicas
        or status.get("readyReplicas", 0) != replicas
        or not status.get("currentRevision")
        or status.get("currentRevision") != status.get("updateRevision")
    ):
        raise SystemExit(1)

if not {"ingester", "compactor"}.issubset(deployment_components):
    raise SystemExit(1)
if "query" not in statefulset_components:
    raise SystemExit(1)
pod_counts = {}
for item in pods:
    labels = (item.get("metadata") or {}).get("labels") or {}
    component = labels.get("app.kubernetes.io/component")
    # Migration and scheduled maintenance Jobs share the CR's instance and
    # managed-by labels. They are not workload rollout pods.
    if component not in components:
        continue
    containers = (item.get("spec") or {}).get("containers") or []
    conditions = (item.get("status") or {}).get("conditions") or []
    ready = any(row.get("type") == "Ready" and row.get("status") == "True" for row in conditions)
    pod_counts[component] = pod_counts.get(component, 0) + 1
    if (
        [container.get("image") for container in containers] != [image]
        or (item.get("status") or {}).get("phase") != "Running"
        or not ready
    ):
        raise SystemExit(1)
if any(
    pod_counts.get(component, 0) != replicas
    for component, replicas in desired_by_component.items()
):
    raise SystemExit(1)
PY
}

retain_operator_rollout_timeout() {
  local step=$1 image=$2 output="$RESULTS_DIR/schema-rollback-operator-rollout-${step}-timeout.json"
  retain_operator_logs "$step timed out waiting for $image"
  python3 - "$step" "$image" "$(iso_now)" "$TMP_DIR/operator-cr.json" \
    "$TMP_DIR/operator-deployments.json" "$TMP_DIR/operator-statefulsets.json" \
    "$TMP_DIR/operator-pods.json" "$TMP_DIR/operator-jobs.json" "$output" <<'PY'
import json
import sys

step, image, at, cr, deployments, statefulsets, pods, jobs, output = sys.argv[1:]


def load(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return None


document = {
    "step": step,
    "requested_image": image,
    "at": at,
    "cr": load(cr),
    "deployments": load(deployments),
    "statefulsets": load(statefulsets),
    "pods": load(pods),
    "jobs": load(jobs),
}
with open(output, "w", encoding="utf-8") as handle:
    json.dump(document, handle, indent=2)
    handle.write("\n")
PY
  printf '%s' "$output"
}

wait_for_operator_rollout() {
  local step=$1 image=$2 deadline=$((SECONDS + RECONCILE_TIMEOUT_SECONDS))
  while ((SECONDS < deadline)); do
    read_operator_rollout
    if operator_rollout_ready "$image"; then
      return 0
    fi
    sleep "$CONVERGE_INTERVAL_SECONDS"
  done
  read_operator_rollout || true
  local diagnostic
  diagnostic=$(retain_operator_rollout_timeout "$step" "$image")
  die "operator workloads did not roll out image $image within ${RECONCILE_TIMEOUT_SECONDS}s; diagnostics: $diagnostic"
}

# `siglake_operator_rollout_held_total` off the operator's own /metrics. The
# counter is not pre-registered, so it is ABSENT before the first
# hold; that is recorded as absent rather than as zero.
read_rollout_held() {
  # The chart's fullname is <release>-<chart name>; the metrics Service uses
  # that fullname directly (it does not add a `-metrics` suffix).
  local service="$OPERATOR_RELEASE-siglake-operator"
  stop_forward
  : >"$TMP_DIR/operator-metrics.txt"
  kubectl --context "$KUBE_CONTEXT" --request-timeout=60s \
    -n "$OPERATOR_NAMESPACE" port-forward "service/$service" \
    "${OPERATOR_METRICS_PORT}:9190" >"$TMP_DIR/operator-pf.log" 2>&1 &
  ACTIVE_PF_PID=$!
  for _ in $(seq 1 30); do
    curl -fsS --max-time 5 "http://127.0.0.1:${OPERATOR_METRICS_PORT}/metrics" \
      >"$TMP_DIR/operator-metrics.txt" 2>/dev/null && break
    sleep 1
  done
  stop_forward
}

# $1 = step name, $2 = the image the CR asks for, $3 = what the probe did,
# $4 = the uid of the Job deleted before this step, or the empty string.
emit_operator_step() {
  local name=$1 image=$2 action=$3 deleted_uid=${4:-}
  read_operator_jobs
  read_rollout_held
  retain_operator_logs "$name"
  python3 - "$name" "$image" "$action" "$deleted_uid" "$(iso_now)" \
    "$TMP_DIR/operator-cr.json" "$TMP_DIR/operator-jobs.json" \
    "$TMP_DIR/operator-deployments.json" "$TMP_DIR/operator-statefulsets.json" \
    "$TMP_DIR/operator-pods.json" "$TMP_DIR/operator-metrics.txt" "$OPERATOR_STEPS" <<'PY'
import json
import sys

(
    name, image, action, deleted_uid, at, cr_path, jobs_path, deployments_path,
    statefulsets_path, pods_path, metrics_path, output,
) = sys.argv[1:]


def load(path):
    try:
        return json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return {}


cr = load(cr_path)
spec = cr.get("spec", {}) if isinstance(cr, dict) else {}
status = cr.get("status", {}) if isinstance(cr, dict) else {}


def workload_rows(path, kind):
    rows = []
    for item in load(path).get("items", []) or []:
        meta = item.get("metadata", {})
        item_spec = item.get("spec", {})
        item_status = item.get("status", {})
        containers = item_spec.get("template", {}).get("spec", {}).get("containers", [])
        row = {
            "name": meta.get("name"),
            "component": meta.get("labels", {}).get("app.kubernetes.io/component"),
            "generation": meta.get("generation"),
            "observed_generation": item_status.get("observedGeneration"),
            "desired_replicas": item_spec.get("replicas", 1),
            "updated_replicas": item_status.get("updatedReplicas", 0),
            "ready_replicas": item_status.get("readyReplicas", 0),
            "image": (containers[0] if containers else {}).get("image"),
        }
        if kind == "Deployment":
            row["available_replicas"] = item_status.get("availableReplicas", 0)
        else:
            row.update({
                "current_replicas": item_status.get("currentReplicas", 0),
                "current_revision": item_status.get("currentRevision"),
                "update_revision": item_status.get("updateRevision"),
            })
        rows.append(row)
    return sorted(rows, key=lambda row: row.get("name") or "")


workload_components = {
    (item.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component"))
    for path in (deployments_path, statefulsets_path)
    for item in load(path).get("items", []) or []
}
pods = []
for item in load(pods_path).get("items", []) or []:
    meta = item.get("metadata", {})
    component = meta.get("labels", {}).get("app.kubernetes.io/component")
    if component not in workload_components:
        continue
    pod_status = item.get("status", {})
    containers = item.get("spec", {}).get("containers", [])
    conditions = pod_status.get("conditions", []) or []
    pods.append({
        "name": meta.get("name"),
        "component": component,
        "image": (containers[0] if containers else {}).get("image"),
        "phase": pod_status.get("phase"),
        "ready": any(
            row.get("type") == "Ready" and row.get("status") == "True"
            for row in conditions
        ),
    })
pods.sort(key=lambda row: row.get("name") or "")

jobs = []
for item in load(jobs_path).get("items", []) or []:
    meta = item.get("metadata", {})
    job_status = item.get("status", {})
    containers = item.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [{}])
    jobs.append({
        "name": meta.get("name"),
        "uid": meta.get("uid"),
        "created_at": meta.get("creationTimestamp"),
        "completed_at": job_status.get("completionTime"),
        "succeeded": job_status.get("succeeded", 0),
        "failed": job_status.get("failed", 0),
        "image": (containers[0] if containers else {}).get("image"),
    })
jobs.sort(key=lambda row: row.get("created_at") or "")

# The migration Job for the image this step asks for: the operator names it
# from the schema version and a digest over image + storage settings, so the
# Job carrying this step's image IS the one the revert reaches for.
current = next((job for job in jobs if job.get("image") == image), None)

held = None
try:
    for line in open(metrics_path, encoding="utf-8"):
        if line.startswith("siglake_operator_rollout_held_total"):
            held = float(line.rsplit(" ", 1)[1])
            break
except (OSError, ValueError, IndexError):
    held = None

step = {
    "name": name,
    "at": at,
    "action": action,
    "requested_image": image,
    "spec_image": spec.get("image"),
    "generation": cr.get("metadata", {}).get("generation"),
    "observed_generation": status.get("observedGeneration"),
    "spec_schema_version": spec.get("schemaVersion"),
    "status_schema_version": status.get("schemaVersion"),
    "deployments": workload_rows(deployments_path, "Deployment"),
    "statefulsets": workload_rows(statefulsets_path, "StatefulSet"),
    "pods": pods,
    "deleted_job_uid": deleted_uid or None,
    "job": current,
    "jobs": jobs,
    "rollout_held_total": {
        "present": held is not None,
        "value": held,
    },
}
with open(output, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(step) + "\n")
PY
}

# Wait for `status.schemaVersion` to be reported, which is what makes every
# later reconcile an UPGRADE in the reconciler's sense (a fresh install is
# deliberately not gated on the migration).
wait_for_status_schema_version() {
  local deadline=$((SECONDS + RECONCILE_TIMEOUT_SECONDS)) seen=absent
  while ((SECONDS < deadline)); do
    seen=$(kc -n "$OPERATOR_NAMESPACE" get siglakecluster "$CR_NAME" \
      -o 'jsonpath={.status.schemaVersion}' 2>/dev/null || printf '')
    [[ -n "$seen" ]] && {
      log "  status.schemaVersion=$seen"
      return 0
    }
    sleep "$CONVERGE_INTERVAL_SECONDS"
  done
  die "the operator never reported status.schemaVersion for $CR_NAME (last: ${seen:-absent})"
}

# Wait for the operator's migration Job for image $1 to report a success.
wait_for_migration_job() {
  local want_image=$1 deadline=$((SECONDS + RECONCILE_TIMEOUT_SECONDS))
  while ((SECONDS < deadline)); do
    read_operator_jobs
    if python3 - "$TMP_DIR/operator-jobs.json" "$want_image" <<'PY'; then
import json, sys
path, image = sys.argv[1:]
try:
    items = json.load(open(path, encoding="utf-8")).get("items", [])
except (OSError, ValueError):
    items = []
for item in items:
    containers = item.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [{}])
    if (containers[0] if containers else {}).get("image") != image:
        continue
    if (item.get("status", {}).get("succeeded") or 0) >= 1:
        raise SystemExit(0)
raise SystemExit(1)
PY
      return 0
    fi
    sleep "$CONVERGE_INTERVAL_SECONDS"
  done
  die "no completed operator migration Job for image $want_image within ${RECONCILE_TIMEOUT_SECONDS}s"
}

# Nudge the CR so the operator reconciles now rather than at its requeue.
touch_cr() {
  kc -n "$OPERATOR_NAMESPACE" annotate siglakecluster "$CR_NAME" \
    "siglake.limnion.ai/rollback-probe=$(date -u +%s)" --overwrite
}

log "operator step 1/4: apply the CR at image A with schemaVersion $CR_SCHEMA_VERSION"
write_cr "$IMAGE_A"
kc apply -f "$TMP_DIR/cluster.yaml"
wait_for_migration_job "$IMAGE_A"
wait_for_status_schema_version
wait_for_operator_rollout install_at_a "$IMAGE_A"
emit_operator_step install_at_a "$IMAGE_A" "apply the CR at image A"
JOB_A_UID=$(python3 - "$OPERATOR_STEPS" <<'PY'
import json, sys
step = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()][-1]
print((step.get("job") or {}).get("uid") or "")
PY
)
[[ -n "$JOB_A_UID" ]] || die "the operator's migration Job for image A was not observed"
JOB_A_NAME=$(python3 - "$OPERATOR_STEPS" <<'PY'
import json, sys
step = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()][-1]
print((step.get("job") or {}).get("name") or "")
PY
)
[[ -n "$JOB_A_NAME" ]] || die "the operator's migration Job name for image A was not observed"

log "operator step 2/4: patch spec.image to B, schemaVersion unchanged"
write_cr "$IMAGE_B"
kc apply -f "$TMP_DIR/cluster.yaml"
wait_for_migration_job "$IMAGE_B"
wait_for_operator_rollout upgrade_to_b "$IMAGE_B"
emit_operator_step upgrade_to_b "$IMAGE_B" "patch spec.image to image B"

log "operator step 3/4: revert spec.image to A while its Job is still retained"
write_cr "$IMAGE_A"
kc apply -f "$TMP_DIR/cluster.yaml"
wait_for_migration_job "$IMAGE_A"
wait_for_operator_rollout revert_to_a_retained "$IMAGE_A"
emit_operator_step revert_to_a_retained "$IMAGE_A" \
  "revert spec.image to image A with the completed Job still retained"

log "operator step 4/4: delete the retained Job and revert again, so it must be recreated"
kc -n "$OPERATOR_NAMESPACE" delete job "$JOB_A_NAME" --ignore-not-found
touch_cr
wait_for_migration_job "$IMAGE_A"
wait_for_operator_rollout revert_to_a_recreated "$IMAGE_A"
emit_operator_step revert_to_a_recreated "$IMAGE_A" \
  "delete the retained Job, then reconcile the same image A revert" "$JOB_A_UID"

# --- evidence ----------------------------------------------------------------

log "assemble the trace"
python3 - "$ROOT" "$CHART_STEPS" "$OPERATOR_STEPS" "$TMP_DIR/raw.json" \
  "$IMAGE_A" "$IMAGE_A_ID" "$IMAGE_B" "$IMAGE_B_ID" "$OPERATOR_IMAGE" \
  "$OPERATOR_IMAGE_ID" "$PROBE_FEATURE" "$PROBE_COLUMN" "$PROBE_VALUE" \
  "$NAMESPACE" "$RELEASE" "$OPERATOR_NAMESPACE" "$OPERATOR_RELEASE" "$CR_NAME" \
  "$CR_SCHEMA_VERSION" "$CHART_START_REVISION" "$INGEST_BATCH" "$QUERY_URL" <<'PY'
import datetime
import json
import subprocess
import sys

(
    root, chart_path, operator_path, output, image_a, image_a_id, image_b,
    image_b_id, operator_image, operator_image_id, probe_feature, probe_column,
    probe_value, namespace, release, operator_namespace, operator_release,
    cr_name, cr_schema_version, start_revision, ingest_batch, query_url,
) = sys.argv[1:]


def steps(path):
    return [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]


repository_commit = subprocess.check_output(
    ["git", "-C", root, "rev-parse", "HEAD"], text=True
).strip()
document = {
    "schema_version": 1,
    "generated_at": datetime.datetime.now(datetime.timezone.utc)
    .isoformat()
    .replace("+00:00", "Z"),
    "revisions": {
        "repository_commit": repository_commit,
        "image_a": {
            "tag": image_a,
            "id": image_a_id,
            "source_revision": repository_commit,
            "cargo_features": "",
        },
        "image_b": {
            "tag": image_b,
            "id": image_b_id,
            "source_revision": repository_commit,
            "cargo_features": probe_feature,
        },
        "operator_image": {"tag": operator_image, "id": operator_image_id},
    },
    "settings": {
        "namespace": namespace,
        "release": release,
        "operator_namespace": operator_namespace,
        "operator_release": operator_release,
        "cr_name": cr_name,
        "cr_schema_version": int(cr_schema_version),
        "chart_start_revision": int(start_revision),
        "ingest_batch": int(ingest_batch),
        "probe_column": probe_column,
        "probe_value": int(probe_value),
        "query_endpoint": query_url,
    },
    "chart_arm": {"steps": steps(chart_path)},
    "operator_arm": {"steps": steps(operator_path)},
}
json.dump(document, open(output, "w", encoding="utf-8"), indent=2)
PY

log "grade retained evidence"
python3 "$ROOT/scripts/grade-kind-schema-rollback.py" "$TMP_DIR/raw.json" \
  --output "$EVIDENCE_JSON"
log "retained $EVIDENCE_JSON"
