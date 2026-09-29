# Siglake AWS Terraform

Provisions everything the BYOC Helm chart references but doesn't
manage itself:

- **VPC** (10.42.0.0/16, 3 AZs, single NAT gateway). Optional —
  skip via `create_vpc=false` to wire into an existing VPC.
- **EKS cluster** (managed control plane + a managed node group) at
  the version pinned in `variables.tf`.
- **RDS Postgres** for the Iceberg catalog. `db.t4g.micro` default;
  bump `rds_instance_class` for production.
- **S3 bucket** for the Iceberg warehouse, with versioning,
  encryption-at-rest, and (optional) Glacier-IR lifecycle.
- **IRSA role** with R/W on the warehouse bucket, trusted by the
  cluster's OIDC provider, scoped to the `siglake/siglake`
  ServiceAccount.
- **ECR repo** for one-off image pushes + an optional pull-through
  cache against `ghcr.io`.
- **Secrets Manager** secret with the RDS credentials in the same JSON
  shape the chart expects (`host`/`port`/`user`/`password`/`database`).

## Prerequisites

- Terraform **≥ 1.6**.
- AWS CLI **≥ 2.13** authenticated to the target account (IAM
  permissions to create VPC, EKS, RDS, IAM, S3, ECR).
- The standard EKS module pulls a fairly recent provider; you'll need
  unrestricted egress so Terraform can fetch modules from the registry.

## Apply

```bash
cd deploy/terraform/aws
terraform init
terraform apply \
  -var "name=siglake-prod" \
  -var "region=us-west-2"
```

The full plan creates ~60 resources and takes ~15 minutes (the EKS
control plane is the long pole; RDS comes up in 5–7 min).

When the apply completes, fetch credentials and assemble Helm values:

```bash
REGION=$(terraform output -raw region)
CLUSTER=$(terraform output -raw cluster_name)
KUBE_CONTEXT=$CLUSTER
KUBECONFIG=$(mktemp -t siglake-kubeconfig.XXXXXX)
chmod 600 "$KUBECONFIG"
aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER" \
  --alias "$KUBE_CONTEXT" --kubeconfig "$KUBECONFIG"
export KUBECONFIG

CURRENT_CONTEXT=$(kubectl config current-context)
[ "$CURRENT_CONTEXT" = "$KUBE_CONTEXT" ] || {
  printf 'wrong kube context: got %s, expected %s\n' \
    "$CURRENT_CONTEXT" "$KUBE_CONTEXT" >&2
  exit 1
}

# Materialize the chart-shaped Postgres Secret in the siglake namespace.
# (The orchestration script in deploy/aws/up.sh does this automatically.)
kubectl --context "$KUBE_CONTEXT" create ns siglake
SECRET_ARN=$(terraform output -raw rds_secret_arn)
aws secretsmanager get-secret-value --region "$REGION" --secret-id "$SECRET_ARN" \
  --query SecretString --output text \
  | KUBE_CONTEXT="$KUBE_CONTEXT" python3 -c '
import json, os, sys, subprocess
d = json.loads(sys.stdin.read())
subprocess.check_call([
  "kubectl", "--context", os.environ["KUBE_CONTEXT"],
  "create", "secret", "generic", "siglake-postgres",
  "-n", "siglake",
  *[f"--from-literal={k}={v}" for k, v in d.items()],
])'

# Install the chart with the values snippet Terraform built.
terraform output -raw helm_values > /tmp/siglake.values.yaml
helm install siglake ../../helm/siglake \
  --kube-context "$KUBE_CONTEXT" \
  -n siglake -f /tmp/siglake.values.yaml \
  --set image.tag=0.2.1 \
  --set wal.storageClassName=efs-sc
```

## RWX storage for the WAL

The chart's `wal` PVC needs `ReadWriteMany`. This Terraform module
intentionally **does not** provision EFS — there are several
deployment shapes (per-tenant filesystems, shared with mount targets,
encrypted-at-rest with a customer KMS key) that customers usually want
to choose themselves. Standard recipe:

```bash
helm repo add aws-efs-csi-driver https://kubernetes-sigs.github.io/aws-efs-csi-driver
helm install aws-efs-csi-driver aws-efs-csi-driver/aws-efs-csi-driver \
  --kube-context "$KUBE_CONTEXT" -n kube-system

aws efs create-file-system --tags Key=Name,Value=siglake-wal
# … add mount targets in each private subnet …
# … create StorageClass with `provisioningMode: efs-ap` …
```

Then pass `--set wal.storageClassName=efs-sc` to the chart.
Keep the private kubeconfig for smoke and teardown commands, then remove it
with `rm -f "$KUBECONFIG"` when the environment no longer needs Kubernetes
access.

## Destroy

```bash
terraform destroy
```

RDS gets a final snapshot when `rds_deletion_protection=true`; with
the default `false`, `terraform destroy` deletes RDS straight away.
S3 buckets refuse to delete while non-empty — for smoke envs you may
need `force_destroy=true` or `aws s3 rm s3://<bucket> --recursive`
first.

## Variables of interest

| Variable                          | Default          | Notes                                                              |
|-----------------------------------|------------------|--------------------------------------------------------------------|
| `name`                            | `siglake`         | Prefix on every resource.                                          |
| `region`                          | `us-east-1`      |                                                                    |
| `kubernetes_version`              | `1.35`           | EKS control plane. Keep this on a current standard-support release to avoid EKS extended-support charges. |
| `rds_instance_class`              | `db.t4g.micro`   | Bump for production; `db.r6g.large` is a reasonable starting size. |
| `rds_backup_retention_days`       | `7`              | RDS PITR window.                                                   |
| `warehouse_lifecycle_days_to_glacier` | `0`          | `0` disables. Set to e.g. `90` for cold-data savings.              |
| `create_ecr_pull_through`         | `false`          | True = ECR caches GHCR pulls for the cluster.                      |

## What gets emitted

`terraform output` prints:

- `cluster_name`, `region`, `kubeconfig_command`
- `warehouse_bucket`, `rds_endpoint`, `rds_secret_arn`
- `serviceaccount_role_arn` (paste into `serviceAccount.annotations`)
- `ecr_repository_url`, `ghcr_pull_through_repo_prefix`
- `helm_values` (chart-shaped YAML snippet)
