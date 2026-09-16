#!/usr/bin/env bash
#
# scripts/kind-down.sh — delete the kind cluster created by kind-up.sh.

set -uo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-siglake}"
EXPECTED_CONTROL_PLANE_ID="${KIND_EXPECTED_CONTROL_PLANE_ID:-}"

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

verify_expected_control_plane() {
  local container_name="${CLUSTER_NAME}-control-plane" matches
  [[ -n "$EXPECTED_CONTROL_PLANE_ID" ]] || return 0
  if ! matches=$(docker ps -a --no-trunc \
    --filter "name=^/${container_name}$" --format '{{.ID}}'); then
    die "docker could not verify the control plane for kind cluster ${CLUSTER_NAME}; refusing teardown"
  fi
  if [[ "$matches" != "$EXPECTED_CONTROL_PLANE_ID" ]]; then
    die "kind cluster ${CLUSTER_NAME} now uses control-plane container ${matches:-<missing>}, but this round created ${EXPECTED_CONTROL_PLANE_ID}; refusing to delete another round's cluster"
  fi
}

clusters=$(kind get clusters 2>/dev/null) || clusters=
if [[ $'\n'"$clusters"$'\n' == *$'\n'"$CLUSTER_NAME"$'\n'* ]]; then
  verify_expected_control_plane
  log "kind: delete cluster $CLUSTER_NAME"
  kind delete cluster --name "$CLUSTER_NAME"
else
  remove_exited_kind_control_plane
  log "kind: cluster $CLUSTER_NAME not found, orphan check complete"
fi
