#!/usr/bin/env bash
#
# scripts/kind-up.sh — bring up a single-node kind cluster running
# postgres + minio + the siglake chart for fast inner-dev smoke.
#
# See deploy/kind/README.md for the full layout.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KIND_DIR="$ROOT/deploy/kind"
CHART_DIR="$ROOT/deploy/helm/siglake"
# shellcheck source=scripts/kind-common.bash
source "$ROOT/scripts/kind-common.bash"

CLUSTER_NAME="${KIND_CLUSTER_NAME:-siglake}"
IMAGE_TAG="${SIGLAKE_KIND_IMAGE_TAG:-siglake:kind}"
OWNERSHIP_FILE="${KIND_CLUSTER_OWNERSHIP_FILE:-}"

log() { printf '==> %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# One resolution of the warehouse selector, read by the manifest apply, the
# readiness waits and the chart install below. An unknown store stops here,
# before a cluster exists: leaving the MinIO defaults in place for a typo is
# how an arm ends up measuring the store it was not asked for.
OBJECT_STORE="$(siglake_kind_object_store "${SIGLAKE_OBJECT_STORE:-}")" ||
  die "SIGLAKE_OBJECT_STORE must be 'minio' or 'garage' (got '${SIGLAKE_OBJECT_STORE:-}')"
IFS=$'\t' read -r STORE_MANIFEST STORE_APP STORE_INIT_JOB STORE_CONFIG _ \
  < <(siglake_kind_store_settings "$OBJECT_STORE")
mapfile -t STORE_VALUES < <(siglake_kind_store_values "$OBJECT_STORE")
STORE_VALUES_ARGS=()
for store_values_file in "${STORE_VALUES[@]}"; do
  STORE_VALUES_ARGS+=(--values "$ROOT/$store_values_file")
done

remove_exited_kind_control_plane() {
  local container_name="${CLUSTER_NAME}-control-plane"
  local matches container_id inspection state cluster_label role_label
  local -a fields=()

  if ! matches=$(docker ps -a --no-trunc \
    --filter "name=^/${container_name}$" --format '{{.ID}}'); then
    die "docker could not inspect the unregistered kind container name ${container_name}"
  fi
  [[ -n "$matches" ]] || return 0
  if [[ "$matches" == *$'\n'* ]]; then
    die "multiple containers matched the exact kind control-plane name ${container_name}; refusing cleanup"
  fi
  container_id=$matches

  if ! inspection=$(docker inspect --format \
    '{{"state="}}{{.State.Status}}{{"\ncluster="}}{{with index .Config.Labels "io.x-k8s.kind.cluster"}}{{.}}{{end}}{{"\nrole="}}{{with index .Config.Labels "io.x-k8s.kind.role"}}{{.}}{{end}}' \
    "$container_id"); then
    die "could not inspect container ${container_id} named ${container_name}; refusing cleanup"
  fi
  mapfile -t fields <<<"$inspection"
  if [[ ${#fields[@]} -ne 3 || ${fields[0]} != state=* || \
    ${fields[1]} != cluster=* || ${fields[2]} != role=* ]]; then
    die "container ${container_id} returned incomplete identity details; refusing cleanup"
  fi
  state=${fields[0]#state=}
  cluster_label=${fields[1]#cluster=}
  role_label=${fields[2]#role=}

  if [[ "$cluster_label" != "$CLUSTER_NAME" || "$role_label" != control-plane || \
    "$state" != exited ]]; then
    die "container ${container_id} named ${container_name} is not an abandoned kind control plane: io.x-k8s.kind.cluster=${cluster_label:-<missing>}, io.x-k8s.kind.role=${role_label:-<missing>}, state=${state:-<missing>}; stop the owning round or remove the container manually after verifying ownership"
  fi

  log "docker: remove exited kind control plane ${container_id} (${container_name})"
  docker rm "$container_id" >/dev/null ||
    die "could not remove exited kind control plane ${container_id}; refusing to force removal"
}

record_cluster_ownership() {
  local container_name="${CLUSTER_NAME}-control-plane" matches
  if ! matches=$(docker ps -a --no-trunc \
    --filter "name=^/${container_name}$" --format '{{.ID}}'); then
    die "docker could not find the control plane created for kind cluster ${CLUSTER_NAME}"
  fi
  if [[ -z "$matches" || "$matches" == *$'\n'* ]]; then
    die "kind created cluster ${CLUSTER_NAME}, but its exact control-plane container ID is ambiguous; refusing to record ownership"
  fi
  printf '%s\n' "$matches" >"$OWNERSHIP_FILE" ||
    die "could not record ownership of kind cluster ${CLUSTER_NAME} in ${OWNERSHIP_FILE}"
}

wait_for_pod_ready() {
  local app=$1 timeout_seconds=$2 pods elapsed=0

  log "kubectl: wait up to ${timeout_seconds}s for an app=${app} pod to exist"
  while ((elapsed < timeout_seconds)); do
    pods=$(kubectl get pods -l "app=${app}" -o name)
    if [[ -n "$pods" ]]; then
      log "kubectl: wait for app=${app} pods to be ready"
      kubectl wait --for=condition=ready pod -l "app=${app}" \
        --timeout="${timeout_seconds}s"
      return 0
    fi
    sleep 1
    ((elapsed += 1))
  done

  die "timed out after ${timeout_seconds}s waiting for an app=${app} pod to exist"
}

for tool in kind kubectl helm docker; do
  command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool"
done

log "kind: create cluster ($CLUSTER_NAME)"
clusters=$(kind get clusters)
if [[ $'\n'"$clusters"$'\n' == *$'\n'"$CLUSTER_NAME"$'\n'* ]]; then
  if [[ -n "$OWNERSHIP_FILE" ]]; then
    die "kind cluster ${CLUSTER_NAME} already exists and was not created by this round; refusing to reuse it"
  fi
  log "  cluster $CLUSTER_NAME exists, skipping create"
else
  remove_exited_kind_control_plane
  kind create cluster --name "$CLUSTER_NAME" --config "$KIND_DIR/cluster.yaml"
  if [[ -n "$OWNERSHIP_FILE" ]]; then
    record_cluster_ownership
  fi
fi

log "docker: build siglake:kind"
docker build -t "$IMAGE_TAG" -f "$ROOT/deploy/Dockerfile" "$ROOT"

log "kind: load image into the cluster"
kind load docker-image "$IMAGE_TAG" --name "$CLUSTER_NAME"

log "kubectl: apply postgres + $OBJECT_STORE manifests"
kubectl apply -f "$KIND_DIR/manifests/postgres.yaml"
if [[ -n "$STORE_CONFIG" ]]; then
  # The store's own configuration stays in one file for both stacks: compose
  # bind-mounts it, kind installs that same file as a ConfigMap.
  log "kubectl: install $STORE_CONFIG as the ${OBJECT_STORE}-config ConfigMap"
  kubectl create configmap "${OBJECT_STORE}-config" \
    --from-file="$(basename "$STORE_CONFIG")=$ROOT/$STORE_CONFIG" \
    --dry-run=client -o yaml | kubectl apply -f -
fi
kubectl apply -f "$ROOT/$STORE_MANIFEST"

wait_for_pod_ready postgres 120
wait_for_pod_ready "$STORE_APP" 120

log "kubectl: wait for the $STORE_INIT_JOB Job to complete"
kubectl wait --for=condition=complete "job/$STORE_INIT_JOB" --timeout=120s

log "helm: install siglake"
helm upgrade --install siglake "$CHART_DIR" \
  "${STORE_VALUES_ARGS[@]}" \
  --wait --timeout 5m

log "kubectl: apply NodePort fronts"
kubectl apply -f "$KIND_DIR/manifests/services-nodeport.yaml"

case "$OBJECT_STORE" in
  minio) STORE_LINE='Minio console:       http://localhost:9001  (minioadmin / minioadmin)' ;;
  # Garage has no web console; 3903 is its admin/metrics API, in-cluster only.
  garage) STORE_LINE='Garage S3 endpoint:  http://garage:3900 (in-cluster; no console)' ;;
esac

cat <<EOF

siglake (kind) is up.

  Warehouse store:     $OBJECT_STORE
  OTLP ingest:         http://localhost:8088/v1/logs
  Query server:        http://localhost:8089
  $STORE_LINE

Smoke test:
  scripts/kind-smoke.sh

Tear down:
  scripts/kind-down.sh
EOF
