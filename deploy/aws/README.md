# deploy/aws — AWS orchestration scripts

End-to-end install / smoke / teardown wrappers around
`deploy/terraform/aws/` and `deploy/helm/siglake/`. These are the
quickest path to validate a siglake build against real AWS.

## Prerequisites

- AWS CLI 2.13+, authenticated to the target account.
- Terraform 1.6+.
- Helm 3.12+, kubectl, Python 3.
- An RWX-capable StorageClass in the cluster for the WAL PVC. The
  Terraform module doesn't provision EFS — see
  `deploy/terraform/aws/README.md` for the standard EFS CSI recipe,
  or set `wal.existingClaim` to a PVC you've created out-of-band.

## Quick start

```bash
# 1. Stand it all up.
deploy/aws/up.sh
# Run the printed `export KUBECONFIG=...` before the remaining commands.

# 2. Run the smoke test (OTLP POST → query-server SQL → assert).
deploy/aws/smoke.sh

# 3. Optional: install the operator chart + roll out an
#    operator-managed `SiglakeCluster`.
helm install siglake-op deploy/helm/siglake-operator \
  --namespace siglake-system --create-namespace \
  --set image.repository="$(terraform -chdir=deploy/terraform/aws output -raw ecr_repository_url)" \
  --set image.tag=operator-0.1.1
deploy/aws/operator-smoke.sh

# 4. Tear it down.
deploy/aws/down.sh                     # keep EKS/VPC/EFS/ECR warm
SIGLAKE_DOWN_MODE=all EMPTY_WAREHOUSE=1 deploy/aws/down.sh
                                       # fully destroy AWS resources
```

## What each script does

| Script              | What it does                                                                                                                                          |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `up.sh`             | `terraform apply` → write and verify a private kubeconfig → create namespace + Postgres Secret from Secrets Manager → `helm upgrade --install` → wait-ready. It prints the `KUBECONFIG` export used by the follow-on scripts. |
| `smoke.sh`          | `kubectl port-forward` to ingester + query-server, POST N events, poll `/api/v1/sql` until the row count matches, then run an SPL stats-by-host.      |
| `query-bench.sh`    | Creates a temporary in-cluster runner pod, executes tab-separated SQL timing cases against `siglake-query`, writes serial/concurrency TSVs, and captures query pod stats, metrics, and logs. |
| `operator-smoke.sh` | Templates `deploy/operator/sample-cluster.smoke.yaml` against `terraform output -raw {ecr_repository_url, warehouse_bucket, rds_endpoint, rds_secret_arn}` + the postgres password from Secrets Manager. Creates the ingest token Secrets + applies the CR. Waits for the operator-rendered `example-{ingester,compactor,query}` Deployments to roll out. Run *after* `up.sh` *and* `helm install siglake-op deploy/helm/siglake-operator …`. |
| `down.sh`           | `helm uninstall`, delete PVCs + namespace, then destroy either app-only AWS resources (default, keeps EKS warm) or the full terraform stack (`SIGLAKE_DOWN_MODE=all`). With `EMPTY_WAREHOUSE=1` it also `aws s3 rm` the warehouse bucket first.              |

## Environment overrides

All three scripts honor the same set of vars:

| Var                  | Default                                        | Notes                                                       |
|----------------------|------------------------------------------------|-------------------------------------------------------------|
| `SIGLAKE_RELEASE`     | `siglake`                                       | Helm release name.                                          |
| `SIGLAKE_NAMESPACE`   | `siglake`                                       | Target namespace.                                           |
| `SIGLAKE_IMAGE_TAG`   | `0.1.1`                                        | Image tag to deploy.                                        |
| `SIGLAKE_VALUES_EXTRA`| `deploy/aws/config/values.smoke.yaml`          | Extra values file layered onto Terraform's emitted values.  |
| `SIGLAKE_QUERY_TOKEN` | _empty_                                        | Bearer token for `smoke.sh` if the chart's `query.tokens` is set. |
| `SIGLAKE_QUERY_CASES_FILE` | _empty_                                   | TSV input file for `query-bench.sh` (`label<TAB>expected<TAB>sql<TAB>tags`). |
| `SIGLAKE_QUERY_BENCH_OUTDIR` | `/tmp/siglake-query-bench-<ts>`         | Output directory for `query-bench.sh` artifacts.            |
| `SIGLAKE_QUERY_CONCURRENCY_SPECS` | `all:20:40`                        | Comma list of `tag:concurrency:requests` specs for `query-bench.sh`. |
| `SIGLAKE_DOWN_MODE`   | `cluster`                                      | `cluster` keeps EKS/VPC/EFS/ECR warm; `all` fully destroys terraform-managed AWS resources. |
| `TF_DIR`             | `deploy/terraform/aws`                         | Terraform working directory.                                |
| `EMPTY_WAREHOUSE`    | `1` in `cluster` mode, else `0`                | When `1`, `down.sh` empties the S3 warehouse before destroy.|

## Failure recovery

- **`up.sh` fails mid-apply**: `terraform apply` is idempotent — rerun
  the script and it'll pick up where it left off. Its private kubeconfig is
  removed on failure.
- **Helm release stuck pending-install**: `helm uninstall $RELEASE -n
  $NAMESPACE --kubeconfig <path printed by up.sh> --kube-context <cluster>`
  and re-run.
- **`down.sh` fails on S3**: `EMPTY_WAREHOUSE=1` empties the bucket
  first. In the default keep-EKS mode the script already turns this
  on unless you override it. For very large buckets this can take
  minutes; consider `aws s3 rm s3://<bucket> --recursive` in parallel
  + a versioned delete sweep.
- **`down.sh` fails on RDS**: RDS deletion blocks if
  `rds_deletion_protection=true`. Run `terraform apply -var
  rds_deletion_protection=false` first.
