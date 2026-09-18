#!/usr/bin/env bash
#
# deploy/aws/down.sh — tear down the AWS environment provisioned by up.sh.
#
# Default mode preserves the EKS cluster and its supporting VPC/EFS/ECR
# so subsequent smoke runs can skip the longest apply steps. Use
# SIGLAKE_DOWN_MODE=all for a full terraform destroy.
#
# SIGLAKE_DOWN_MODE is validated before step 1, so an unknown mode leaves the
# cluster and the bucket alone.
#
# Steps 1-3 are best-effort; each command logs but doesn't bail on the next.
# Step 4 is not: a failed destroy exits nonzero and "down complete" is only
# printed after terraform succeeded.
#   1. helm uninstall
#   2. drop the chart-managed Postgres Secret
#   3. drop PVCs + namespace
#   4. destroy either:
#      - app-only AWS resources (default, keep EKS warm), or
#      - the full terraform stack (SIGLAKE_DOWN_MODE=all)
#
# S3 buckets refuse to delete while non-empty. By default the warehouse
# bucket is NOT force-destroyed (see s3.tf). In keep-EKS mode the script
# defaults EMPTY_WAREHOUSE=1 because the bucket must be emptied before
# the targeted destroy can succeed. Override EMPTY_WAREHOUSE=0 if you
# intentionally want the bucket left in place.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TF_DIR="${TF_DIR:-$ROOT/deploy/terraform/aws}"

RELEASE="${SIGLAKE_RELEASE:-siglake}"
NAMESPACE="${SIGLAKE_NAMESPACE:-siglake}"
DOWN_MODE="${SIGLAKE_DOWN_MODE:-cluster}"
EMPTY_WAREHOUSE="${EMPTY_WAREHOUSE:-}"

log() { printf '==> %s\n' "$*" >&2; }

# Settle the mode before anything writes to the cluster. The check used to sit
# on the destroy's `case` at the end, so a typo'd SIGLAKE_DOWN_MODE uninstalled
# the release and deleted the namespace, destroyed nothing in AWS, and exited 1:
# the smoke run gone and the billing resources still up.
case "$DOWN_MODE" in
  cluster | all) ;;
  *)
    echo "ERROR: SIGLAKE_DOWN_MODE must be 'cluster' or 'all' (got '$DOWN_MODE')" >&2
    exit 1
    ;;
esac

if [ -z "$EMPTY_WAREHOUSE" ]; then
  if [ "$DOWN_MODE" = "cluster" ]; then
    EMPTY_WAREHOUSE=1
  else
    EMPTY_WAREHOUSE=0
  fi
fi

readonly KEEP_CLUSTER_TARGETS=(
  aws_db_instance.rds
  aws_db_subnet_group.rds
  aws_db_parameter_group.rds
  aws_security_group_rule.rds_from_cluster
  aws_security_group_rule.rds_from_nodes
  aws_security_group_rule.rds_egress
  aws_security_group.rds
  aws_secretsmanager_secret_version.rds
  aws_secretsmanager_secret.rds
  random_password.rds
  aws_s3_bucket_lifecycle_configuration.warehouse
  aws_s3_bucket_public_access_block.warehouse
  aws_s3_bucket_server_side_encryption_configuration.warehouse
  aws_s3_bucket_versioning.warehouse
  aws_s3_bucket.warehouse
  aws_iam_role_policy_attachment.irsa_warehouse_rw
  aws_iam_role.irsa
  aws_iam_policy.warehouse_rw
)

# ---------------------------------------------------------------------------
# 1. helm uninstall
# ---------------------------------------------------------------------------
if kubectl get ns "$NAMESPACE" >/dev/null 2>&1; then
  log "helm: uninstall $RELEASE -n $NAMESPACE"
  helm uninstall "$RELEASE" -n "$NAMESPACE" || true

  log "kubectl: delete the chart-managed Postgres Secret"
  kubectl -n "$NAMESPACE" delete secret "${RELEASE}-postgres" --ignore-not-found

  log "kubectl: delete PVCs"
  kubectl -n "$NAMESPACE" delete pvc -l "app.kubernetes.io/instance=$RELEASE" --ignore-not-found

  log "kubectl: delete namespace"
  kubectl delete namespace "$NAMESPACE" --ignore-not-found
else
  log "namespace $NAMESPACE not present, skipping helm + kubectl steps"
fi

# ---------------------------------------------------------------------------
# 2. Optionally empty the warehouse bucket so terraform destroy succeeds.
# ---------------------------------------------------------------------------
if [ "$EMPTY_WAREHOUSE" = "1" ]; then
  WAREHOUSE_BUCKET=""
  if pushd "$TF_DIR" >/dev/null; then
    WAREHOUSE_BUCKET=$(terraform output -raw warehouse_bucket 2>/dev/null || true)
    popd >/dev/null || true
  fi
  if [ -n "$WAREHOUSE_BUCKET" ]; then
    log "s3: emptying $WAREHOUSE_BUCKET (EMPTY_WAREHOUSE=1)"
    aws s3 rm "s3://$WAREHOUSE_BUCKET" --recursive || true
    # Delete remaining object versions (versioning is enabled on the bucket).
    # Use batched DeleteObjects calls; one-object-at-a-time deletes make
    # warm-cluster iteration unnecessarily slow once the warehouse has
    # accumulated many snapshots and manifests.
    aws s3api list-object-versions --bucket "$WAREHOUSE_BUCKET" --output json \
      2>/dev/null \
      | B="$WAREHOUSE_BUCKET" python3 -c '
import json, os, subprocess, sys, tempfile
data = json.loads(sys.stdin.read() or "{}")
versions = (data.get("Versions") or []) + (data.get("DeleteMarkers") or [])
bucket = os.environ["B"]
batch = []
for v in versions:
    batch.append({"Key": v["Key"], "VersionId": v["VersionId"]})
    if len(batch) == 200:
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump({"Objects": batch, "Quiet": True}, f)
            path = f.name
        try:
            subprocess.run(["aws", "s3api", "delete-objects",
                            "--bucket", bucket,
                            "--delete", f"file://{path}"],
                           check=False, stdout=subprocess.DEVNULL)
        finally:
            os.unlink(path)
        batch.clear()
if batch:
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        json.dump({"Objects": batch, "Quiet": True}, f)
        path = f.name
    try:
        subprocess.run(["aws", "s3api", "delete-objects",
                        "--bucket", bucket,
                        "--delete", f"file://{path}"],
                       check=False, stdout=subprocess.DEVNULL)
    finally:
        os.unlink(path)
' || true
  fi
fi

# ---------------------------------------------------------------------------
# 3. terraform destroy
# ---------------------------------------------------------------------------
cd "$TF_DIR" || { echo "ERROR: TF_DIR=$TF_DIR not found" >&2; exit 1; }
destroy_rc=0
case "$DOWN_MODE" in
  cluster)
    log "terraform: destroy app resources, keep EKS/VPC/EFS/ECR warm"
    args=(destroy -input=false -auto-approve)
    for target in "${KEEP_CLUSTER_TARGETS[@]}"; do
      args+=("-target=$target")
    done
    terraform "${args[@]}" || destroy_rc=$?
    ;;
  all)
    log "terraform: full destroy"
    terraform destroy -input=false -auto-approve || destroy_rc=$?
    ;;
  *)
    # Unreachable: the mode was settled above, before the cleanup steps. Left
    # as a backstop so a future arm added to one `case` and not the other
    # cannot fall through to "down complete" with nothing destroyed.
    echo "ERROR: unhandled SIGLAKE_DOWN_MODE '$DOWN_MODE'" >&2
    exit 1
    ;;
esac

# The Helm and kubectl steps above are best-effort on purpose; the destroy is
# not. Without this the script's status was the final `log`, so a destroy that
# left RDS, the warehouse bucket and the IAM role running and billing still
# exited 0 and the caller read the teardown as finished.
if [ "$destroy_rc" -ne 0 ]; then
  echo "ERROR: terraform destroy failed (exit $destroy_rc) in SIGLAKE_DOWN_MODE=$DOWN_MODE;" \
       "AWS resources are still up -- re-run after fixing the cause" >&2
  exit "$destroy_rc"
fi

log "down complete"
