#!/usr/bin/env bash
#
# End-to-end smoke for the siglake-operator: spin up a kind cluster,
# apply the CRD, run the `--ignored` integration test, tear down.
#
# Requires: kind, kubectl, cargo.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-siglake-op-smoke}"
KEEP="${KEEP:-0}"

cleanup() {
  if [[ "$KEEP" != "1" ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo ">> creating kind cluster $CLUSTER_NAME"
kind create cluster --name "$CLUSTER_NAME"

echo ">> applying CRD"
kubectl apply -f deploy/operator/crd.yaml

echo ">> running operator integration tests"
cargo test -p siglake-operator -- --ignored --nocapture

echo ">> smoke OK"
