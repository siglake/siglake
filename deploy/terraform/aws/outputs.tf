data "aws_caller_identity" "current" {}

output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "region" {
  description = "AWS region."
  value       = var.region
}

output "kubeconfig_command" {
  description = "Command to fetch the kubeconfig context for this cluster."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "warehouse_bucket" {
  description = "S3 bucket holding the Iceberg warehouse."
  value       = aws_s3_bucket.warehouse.id
}

output "rds_endpoint" {
  description = "RDS Postgres endpoint (host:port)."
  value       = "${aws_db_instance.rds.address}:${aws_db_instance.rds.port}"
}

output "rds_secret_arn" {
  description = "Secrets Manager ARN containing the RDS credentials (host/port/user/password/database)."
  value       = aws_secretsmanager_secret.rds.arn
}

output "serviceaccount_role_arn" {
  description = "IRSA role ARN to annotate on the siglake ServiceAccount."
  value       = aws_iam_role.irsa.arn
}

output "ecr_repository_url" {
  description = "Customer-side ECR repo URL (set image.repository to this if you don't want to pull from GHCR)."
  value       = aws_ecr_repository.siglake.repository_url
}

output "efs_file_system_id" {
  description = "EFS FileSystem ID for the WAL volume. Used by the post-apply StorageClass install."
  value       = aws_efs_file_system.wal.id
}

output "ghcr_pull_through_repo_prefix" {
  description = "ECR pull-through cache prefix for GHCR (only useful when create_ecr_pull_through=true)."
  value = (
    var.create_ecr_pull_through
    ? "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.region}.amazonaws.com/ghcr/limnion-ai/siglake"
    : null
  )
}

# Helm values snippet ready to paste into a values file or `helm install -f`.
output "helm_values" {
  description = "Helm values snippet matching this Terraform deployment."
  value = yamlencode({
    image = {
      repository = "ghcr.io/siglake/siglake"
    }
    s3 = {
      bucket = aws_s3_bucket.warehouse.id
      region = var.region
    }
    postgres = {
      existingSecret = "${var.name}-postgres"
    }
    serviceAccount = {
      annotations = {
        "eks.amazonaws.com/role-arn" = aws_iam_role.irsa.arn
      }
    }
  })
}
