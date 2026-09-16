#!/usr/bin/env bash
#
# deploy/aws/up.sh — stand up a siglake environment in AWS.
#
# Steps:
#   1. Pre-flight: required CLIs present and authenticated.
#   2. terraform apply       (VPC + EKS + RDS + S3 + IRSA + ECR)
#   3. kubeconfig + namespace
#   4. Materialize the Postgres Secret from Secrets Manager.
#   5. helm install/upgrade with the Terraform-emitted values.
#   6. Wait for query-server + ingester to become ready.
#
# Environment overrides:
#   SIGLAKE_RELEASE       Helm release name (default: siglake)
#   SIGLAKE_NAMESPACE     k8s namespace     (default: siglake)
#   SIGLAKE_IMAGE_TAG     image tag to deploy (default: 0.1.1)
#   SIGLAKE_VALUES_EXTRA  path to additional values file (default: ./config/values.smoke.yaml)
#   TF_DIR               terraform working dir (default: deploy/terraform/aws)
#
# Tear down with deploy/aws/down.sh (defaults to keeping EKS warm;
# set SIGLAKE_DOWN_MODE=all for a full destroy).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HERE="$ROOT/deploy/aws"
TF_DIR="${TF_DIR:-$ROOT/deploy/terraform/aws}"
CHART_DIR="$ROOT/deploy/helm/siglake"

RELEASE="${SIGLAKE_RELEASE:-siglake}"
NAMESPACE="${SIGLAKE_NAMESPACE:-siglake}"
IMAGE_TAG="${SIGLAKE_IMAGE_TAG:-0.1.1}"
VALUES_EXTRA="${SIGLAKE_VALUES_EXTRA:-$HERE/config/values.smoke.yaml}"
case "$VALUES_EXTRA" in
  /*) ;;
  *) VALUES_EXTRA="$ROOT/$VALUES_EXTRA" ;;
esac

log() { printf '==> %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

HELM_VALUES_FILE=""
TMP_SECRET=""
KUBECONFIG_PATH=""
RETAIN_KUBECONFIG=0

cleanup() {
  [[ -z "$HELM_VALUES_FILE" ]] || rm -f "$HELM_VALUES_FILE"
  [[ -z "$TMP_SECRET" ]] || rm -f "$TMP_SECRET"
  if [[ "$RETAIN_KUBECONFIG" != 1 && -n "$KUBECONFIG_PATH" ]]; then
    rm -f "$KUBECONFIG_PATH"
  fi
}
trap cleanup EXIT

preserve_warm_node_shape() {
  [[ -n "${TF_VAR_node_instance_types:-}" ]] && return 0

  local region cluster nodegroup current_types
  region="$(terraform output -raw region 2>/dev/null || true)"
  cluster="$(terraform output -raw cluster_name 2>/dev/null || true)"
  [[ -n "$region" && -n "$cluster" ]] || return 0

  nodegroup="$(aws eks list-nodegroups \
    --region "$region" \
    --cluster-name "$cluster" \
    --query 'nodegroups[0]' \
    --output text 2>/dev/null || true)"
  [[ -n "$nodegroup" && "$nodegroup" != "None" ]] || return 0

  current_types="$(aws eks describe-nodegroup \
    --region "$region" \
    --cluster-name "$cluster" \
    --nodegroup-name "$nodegroup" \
    --query 'nodegroup.instanceTypes' \
    --output json 2>/dev/null || true)"
  [[ -n "$current_types" && "$current_types" != "null" && "$current_types" != "[]" ]] || return 0

  export TF_VAR_node_instance_types="$current_types"
  log "terraform: preserving warm nodegroup instance types $TF_VAR_node_instance_types"
}

# ---------------------------------------------------------------------------
# 1. Pre-flight
# ---------------------------------------------------------------------------
log "pre-flight: tools"
for tool in aws terraform kubectl helm; do
  command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool"
done

aws sts get-caller-identity >/dev/null 2>&1 \
  || die "aws CLI is not authenticated (try: aws sso login or aws configure)"

terraform_version=$(terraform version -json 2>/dev/null | python3 -c \
  'import json,sys; print(json.load(sys.stdin)["terraform_version"])')
case "$terraform_version" in
  0.*|1.0.*|1.1.*|1.2.*|1.3.*|1.4.*|1.5.*) die "terraform >= 1.6 required, got $terraform_version" ;;
esac

helm_version=$(helm version --short)
case "$helm_version" in
  v3.0.*|v3.1.*|v3.2.*|v3.3.*|v3.4.*|v3.5.*|v3.6.*|v3.7.*|v3.8.*|v3.9.*|v3.10.*|v3.11.*) die "helm >= 3.12 required, got $helm_version" ;;
esac

# ---------------------------------------------------------------------------
# 2. terraform apply
# ---------------------------------------------------------------------------
log "terraform: init + apply"
cd "$TF_DIR"
terraform init -input=false -upgrade
preserve_warm_node_shape
terraform apply -input=false -auto-approve

REGION=$(terraform output -raw region)
CLUSTER=$(terraform output -raw cluster_name)
SECRET_ARN=$(terraform output -raw rds_secret_arn)
ECR_REPO_URL=$(terraform output -raw ecr_repository_url)
HELM_VALUES_FILE="$(mktemp -t siglake-helm-values.XXXXXX.yaml)"
terraform output -raw helm_values > "$HELM_VALUES_FILE"

# ---------------------------------------------------------------------------
# 3. kubeconfig + namespace
# ---------------------------------------------------------------------------
log "kubeconfig: $CLUSTER in $REGION"
KUBECONFIG_PATH="$(mktemp -t siglake-kubeconfig.XXXXXX)"
chmod 600 "$KUBECONFIG_PATH"
aws eks update-kubeconfig \
  --region "$REGION" \
  --name "$CLUSTER" \
  --alias "$CLUSTER" \
  --kubeconfig "$KUBECONFIG_PATH" >/dev/null

CURRENT_CONTEXT=$(kubectl --kubeconfig "$KUBECONFIG_PATH" config current-context)
[[ "$CURRENT_CONTEXT" == "$CLUSTER" ]] \
  || die "configured kube context is '$CURRENT_CONTEXT', expected '$CLUSTER'"

KUBECTL=(kubectl --kubeconfig "$KUBECONFIG_PATH" --context "$CLUSTER")
HELM=(helm --kubeconfig "$KUBECONFIG_PATH" --kube-context "$CLUSTER")

"${KUBECTL[@]}" get ns "$NAMESPACE" >/dev/null 2>&1 \
  || "${KUBECTL[@]}" create namespace "$NAMESPACE"

# ---------------------------------------------------------------------------
# 4. Materialize the Postgres Secret
# ---------------------------------------------------------------------------
log "secret: $NAMESPACE/$RELEASE-postgres"
# `--region "$REGION"` is non-optional: secretsmanager honors
# AWS_DEFAULT_REGION from the caller's env, and if that's set to a
# region other than the one terraform created the secret in (e.g. the
# operator sourced an .aws-env with AWS_DEFAULT_REGION=us-west-2 but
# left terraform on its us-east-1 default), the GetSecretValue call
# returns ResourceNotFoundException and the rest of the script
# proceeds with an empty Secret.
SECRET_JSON=$(aws secretsmanager get-secret-value --region "$REGION" --secret-id "$SECRET_ARN" --query SecretString --output text)
TMP_SECRET="$(mktemp -t siglake-secret.XXXXXX.yaml)"
python3 - "$SECRET_JSON" "$NAMESPACE" "${RELEASE}-postgres" > "$TMP_SECRET" <<'PY'
import base64, json, sys
secret_json, ns, name = sys.argv[1:4]
data = json.loads(secret_json)
print(f"""apiVersion: v1
kind: Secret
metadata:
  name: {name}
  namespace: {ns}
type: Opaque
data:""")
for k, v in data.items():
    print(f"  {k}: {base64.b64encode(str(v).encode()).decode()}")
PY
"${KUBECTL[@]}" apply -f "$TMP_SECRET" >/dev/null
rm -f "$TMP_SECRET"
TMP_SECRET=""

# ---------------------------------------------------------------------------
# 4b. EFS StorageClass install
# ---------------------------------------------------------------------------
# Terraform provisions the EFS file system + the CSI driver but
# doesn't install the StorageClass (no kubernetes provider). Apply
# one here so chart values can opt into RWX-on-EFS via
# `wal.storageClassName: efs-sc`. The single-replica smoke profile
# (values.smoke.yaml) sticks with gp2/RWO, so customers that don't
# need EFS pay nothing — the StorageClass is just available.
EFS_ID=$(cd "$TF_DIR" && terraform output -raw efs_file_system_id)
log "storageclass: install efs-sc pointing at $EFS_ID"
cat <<YAML | "${KUBECTL[@]}" apply -f - >/dev/null
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: efs-sc
provisioner: efs.csi.aws.com
parameters:
  provisioningMode: efs-ap
  fileSystemId: $EFS_ID
  directoryPerms: "700"
reclaimPolicy: Delete
volumeBindingMode: Immediate
YAML

# Provision a `gp3-wal` StorageClass with explicit IOPS + throughput
# tunables. The default `gp2` SC EKS ships with is
# size-tied (3 IOPS/GiB, 100 baseline). gp3 decouples IOPS from size,
# letting the chart's `wal.storageClassName: gp3-wal` opt into
# 6000 IOPS / 250 MBps regardless of volume size. Cost on top of gp3
# baseline: (6000-3000)*$0.005 + (250-125)*$0.040 = $20/mo per volume.
#
# NOTE (2026-05-21): gp3 with 6000 IOPS does NOT raise the
# per-pod EPS ceiling vs gp2 100 IOPS — the bottleneck past ~130k EPS
# is the per-tenant serial writer task in siglake-ingest, not disk.
# The SC is still useful for multi-tenant deployments where the aggregate
# write rate per volume matters, just not for breaking the single-tenant
# ceiling measured in that validation.
#
# CAVEAT: gp3 caps the IOPS-to-size ratio at 500 IOPS/GiB. 6000 IOPS
# requires ≥ 12 GiB. Smoke chart's default `wal.size: 5Gi` fails to
# provision with `Iops to volume size ratio of 1200 is too high;
# maximum is 500`. Set `wal.size: 16Gi` when using gp3-wal at 6000 IOPS.
log "storageclass: install gp3-wal (6000 IOPS, 250 MBps)"
cat <<'YAML' | "${KUBECTL[@]}" apply -f - >/dev/null
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3-wal
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
  iops: "6000"
  throughput: "250"
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
YAML

# ---------------------------------------------------------------------------
# 5. helm install/upgrade
# ---------------------------------------------------------------------------
log "helm: install/upgrade $RELEASE"
# `image.repository` defaults to ghcr.io/siglake/siglake in the
# chart (and in the Terraform-emitted helm_values). For smoke we
# pushed a fresh build to the per-account ECR — override
# explicitly so EKS pulls from there rather than ghcr.
# The static values file pins ingester+compactor to one AZ (RWO PVC
# co-mount), historically hardcoded `us-east-1a` — which strands every pod
# Pending when `TF_VAR_region` overrides the region (hit on the us-west-2
# stand-up). Derive the zone from the live region instead; `<region>a`
# exists in every commercial region. Override with SIGLAKE_WAL_ZONE.
WAL_ZONE="${SIGLAKE_WAL_ZONE:-${REGION}a}"
"${HELM[@]}" upgrade --install "$RELEASE" "$CHART_DIR" \
  --namespace "$NAMESPACE" \
  --values "$HELM_VALUES_FILE" \
  --values "$VALUES_EXTRA" \
  --set "image.repository=$ECR_REPO_URL" \
  --set "image.tag=$IMAGE_TAG" \
  --set "postgres.existingSecret=${RELEASE}-postgres" \
  --set "ingester.nodeSelector.topology\.kubernetes\.io/zone=$WAL_ZONE" \
  --set "compactor.nodeSelector.topology\.kubernetes\.io/zone=$WAL_ZONE" \
  --wait --timeout 10m
rm -f "$HELM_VALUES_FILE"
HELM_VALUES_FILE=""

# ---------------------------------------------------------------------------
# 6. Wait for readiness
# ---------------------------------------------------------------------------
log "rollout: wait for ingester + query-server"
# The chart's `siglake.fullname` helper deduplicates if the release
# name already contains the chart name, so the actual Deployment
# names are `${RELEASE}-ingester` / `${RELEASE}-query` (not
# `${RELEASE}-siglake-${role}`) for the default RELEASE=siglake.
"${KUBECTL[@]}" -n "$NAMESPACE" rollout status \
  "deployment/${RELEASE}-ingester" --timeout=5m
"${KUBECTL[@]}" -n "$NAMESPACE" rollout status \
  "deployment/${RELEASE}-query"    --timeout=5m

WAREHOUSE_BUCKET=$(cd "$TF_DIR" && terraform output -raw warehouse_bucket)
RDS_ENDPOINT=$(cd "$TF_DIR" && terraform output -raw rds_endpoint)
printf -v KUBECONFIG_SHELL '%q' "$KUBECONFIG_PATH"
RETAIN_KUBECONFIG=1

cat <<EOF

siglake is up.

  namespace:        $NAMESPACE
  release:          $RELEASE
  cluster:          $CLUSTER
  warehouse bucket: $WAREHOUSE_BUCKET
  rds endpoint:     $RDS_ENDPOINT

Next:
  export KUBECONFIG=$KUBECONFIG_SHELL
  deploy/aws/smoke.sh                 # run the smoke test
  deploy/aws/down.sh                  # remove workload, keep EKS warm
  SIGLAKE_DOWN_MODE=all deploy/aws/down.sh
                                     # fully tear down AWS resources
  rm -f $KUBECONFIG_SHELL             # after the final command above
EOF
