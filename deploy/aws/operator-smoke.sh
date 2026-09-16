#!/usr/bin/env bash
#
# deploy/aws/operator-smoke.sh — apply the operator-managed `SiglakeCluster`
# sample against a `deploy/aws/up.sh`-provisioned EKS cluster, wait for
# the operator-rendered Deployments to reach Ready.
#
# Assumes you've already run:
#   - `deploy/aws/up.sh`         (terraform + main chart)
#   - `helm install siglake-op deploy/helm/siglake-operator` (operator chart)
#   - pushed `<ECR>:operator-0.1.0` and `<ECR>:0.1.0`
#
# What this script does:
#   1. Read the necessary terraform outputs (ECR URL, warehouse URL,
#      RDS endpoint, region).
#   2. Resolve the postgres password from Secrets Manager and build a
#      `catalogUri` of the form `postgres://siglake:<pw>@<host>/siglake`.
#   3. Create the two HEC token Secrets the sample CR references.
#   4. Substitute every `__PLACEHOLDER__` in
#      `deploy/operator/sample-cluster.smoke.yaml` and kubectl apply it.
#   5. Wait for the operator-rendered `example-ingester`,
#      `example-compactor`, `example-query` Deployments to reach
#      Available.
#
# Tear down by deleting the `example` SiglakeCluster (the operator
# owns the Deployments via ownerReferences):
#
#   kubectl -n default delete siglakecluster example
#
# Or run the main `deploy/aws/down.sh` to nuke everything.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TF_DIR="${TF_DIR:-$ROOT/deploy/terraform/aws}"
NAMESPACE="${SIGLAKE_CR_NAMESPACE:-default}"
CR_NAME="${SIGLAKE_CR_NAME:-example}"

log() { printf '==> %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Pre-flight: tools + reachable cluster.
# ---------------------------------------------------------------------------
for tool in aws terraform kubectl python3; do
  command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool"
done

kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || die "namespace $NAMESPACE not present; run up.sh first?"

# ---------------------------------------------------------------------------
# 2. Read terraform outputs + build catalog URI.
# ---------------------------------------------------------------------------
log "terraform: read outputs"
cd "$TF_DIR"
REGION=$(terraform output -raw region)
ECR_REPO_URL=$(terraform output -raw ecr_repository_url)
WAREHOUSE_BUCKET=$(terraform output -raw warehouse_bucket)
RDS_ENDPOINT=$(terraform output -raw rds_endpoint)
RDS_SECRET_ARN=$(terraform output -raw rds_secret_arn)
cd - >/dev/null

log "rds: fetch credentials from Secrets Manager (region=$REGION)"
SECRET_JSON=$(aws secretsmanager get-secret-value \
  --region "$REGION" --secret-id "$RDS_SECRET_ARN" \
  --query SecretString --output text)

# RDS secret payload from terraform's `random_password.rds` carries
# fields {`user`, `password`, `host`, `port`, `database`}. Compose
# into a libpq URI. `user` not `username` — this script will silently
# emit an empty username if you change that.
read -r PG_USER PG_PASS PG_DB <<<"$(python3 -c '
import json, sys, urllib.parse
d = json.loads(sys.stdin.read())
pw = urllib.parse.quote(str(d["password"]), safe="")
print(d["user"], pw, d["database"])
' <<<"$SECRET_JSON")"

CATALOG_URI="postgres://${PG_USER}:${PG_PASS}@${RDS_ENDPOINT}/${PG_DB}"
WAREHOUSE_URL="s3://${WAREHOUSE_BUCKET}/warehouse"

log "ecr:        $ECR_REPO_URL"
log "warehouse:  $WAREHOUSE_URL"
log "catalog:    postgres://${PG_USER}:***@${RDS_ENDPOINT}/${PG_DB}"

# ---------------------------------------------------------------------------
# 3. Cluster-wide bearer-token auth Secret the sample CR references
#    (authTokensSecretRef). Tenancy is resolved by the ingester, not by a
#    credential, so there are no per-tenant Secrets to provision.
# ---------------------------------------------------------------------------
log "secret: siglake-auth (cluster bearer-token allow-list)"
kubectl -n "$NAMESPACE" create secret generic siglake-auth \
  --from-literal=tokens="smoke-$(date +%s)" \
  --dry-run=client -o yaml | kubectl apply -f -

# Provision an IRSA-annotated ServiceAccount in $NAMESPACE so the
# operator-rendered pods (and the audit-rotate
# CronJob) inherit the warehouse-rw permissions terraform
# created. The chart's terraform module creates the same SA in
# its own namespace; for the operator-managed CR we need one
# wherever the CR lives.
SA_NAME="${SIGLAKE_CR_SERVICE_ACCOUNT:-siglake}"
IRSA_ROLE_ARN=$(cd "$TF_DIR" && terraform output -raw serviceaccount_role_arn)
log "serviceaccount: $NAMESPACE/$SA_NAME (IRSA: $IRSA_ROLE_ARN)"
kubectl -n "$NAMESPACE" create serviceaccount "$SA_NAME" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" annotate serviceaccount "$SA_NAME" \
  "eks.amazonaws.com/role-arn=$IRSA_ROLE_ARN" --overwrite

# ---------------------------------------------------------------------------
# 4. Substitute placeholders + apply.
# ---------------------------------------------------------------------------
TMP_CR="$(mktemp -t siglakecluster.XXXXXX.yaml)"
trap 'rm -f "$TMP_CR"' EXIT
sed \
  -e "s|__ECR_REPO_URL__|${ECR_REPO_URL}|g" \
  -e "s|__WAREHOUSE_URL__|${WAREHOUSE_URL}|g" \
  -e "s|__CATALOG_URI__|${CATALOG_URI}|g" \
  -e "s|__AWS_REGION__|${REGION}|g" \
  -e "s|__SERVICE_ACCOUNT__|${SA_NAME}|g" \
  -e "s|__NAMESPACE__|${NAMESPACE}|g" \
  "$ROOT/deploy/operator/sample-cluster.smoke.yaml" > "$TMP_CR"

log "apply: $CR_NAME in $NAMESPACE"
kubectl -n "$NAMESPACE" apply -f "$TMP_CR"

# ---------------------------------------------------------------------------
# 5. Wait for the operator-rendered Deployments.
# ---------------------------------------------------------------------------
log "rollout: wait for ingester / compactor / query (up to 5m each)"
for component in ingester compactor; do
  kubectl -n "$NAMESPACE" rollout status \
    "deployment/${CR_NAME}-${component}" --timeout=5m \
    || die "deployment ${CR_NAME}-${component} did not become Ready"
done
# Query is a StatefulSet (stable per-pod DNS for distributed peers).
kubectl -n "$NAMESPACE" rollout status \
  "statefulset/${CR_NAME}-query" --timeout=5m \
  || die "statefulset ${CR_NAME}-query did not become Ready"

cat <<EOF

operator-managed siglake is up.

  namespace:   $NAMESPACE
  CR:          $CR_NAME
  warehouse:   $WAREHOUSE_URL
  catalog:     postgres://${PG_USER}:***@${RDS_ENDPOINT}/${PG_DB}

Tear down:
  kubectl -n $NAMESPACE delete siglakecluster $CR_NAME
EOF
