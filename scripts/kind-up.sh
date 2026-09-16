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

CLUSTER_NAME="${KIND_CLUSTER_NAME:-siglake}"
IMAGE_TAG="${SIGLAKE_KIND_IMAGE_TAG:-siglake:kind}"
OWNERSHIP_FILE="${KIND_CLUSTER_OWNERSHIP_FILE:-}"

log() { printf '==> %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

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

log "kubectl: apply postgres + minio manifests"
kubectl apply -f "$KIND_DIR/manifests/postgres.yaml"
kubectl apply -f "$KIND_DIR/manifests/minio.yaml"

log "kubectl: wait for postgres + minio to be ready"
kubectl wait --for=condition=ready pod -l app=postgres --timeout=120s
kubectl wait --for=condition=ready pod -l app=minio --timeout=120s

log "kubectl: wait for the bucket-init Job to complete"
kubectl wait --for=condition=complete job/minio-bucket-init --timeout=120s

log "helm: install siglake"
helm upgrade --install siglake "$CHART_DIR" \
  --values "$KIND_DIR/values.kind.yaml" \
  --wait --timeout 5m

log "kubectl: apply NodePort fronts"
kubectl apply -f "$KIND_DIR/manifests/services-nodeport.yaml"

cat <<EOF

siglake (kind) is up.

  OTLP ingest:         http://localhost:8088/v1/logs
  Query server:        http://localhost:8089
  Minio console:       http://localhost:9001  (minioadmin / minioadmin)

Smoke test:
  scripts/kind-smoke.sh

Tear down:
  scripts/kind-down.sh
EOF
