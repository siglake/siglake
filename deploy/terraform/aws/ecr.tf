# Optional ECR pull-through cache against GHCR. When enabled, the
# Helm chart's `image.repository` can point at
# `<account>.dkr.ecr.<region>.amazonaws.com/ghcr/siglake/siglake`
# instead of `ghcr.io/siglake/siglake`, and ECR will fetch + cache
# images on demand.

resource "aws_ecr_pull_through_cache_rule" "ghcr" {
  count = var.create_ecr_pull_through ? 1 : 0

  ecr_repository_prefix = "ghcr"
  upstream_registry_url = "ghcr.io"
  credential_arn        = var.ghcr_credentials_arn
}

# A vanilla ECR repo for one-off builds (e.g. CI publishing release
# candidates that haven't reached GHCR yet). The pull-through cache
# above handles the canonical release path.
resource "aws_ecr_repository" "siglake" {
  name                 = var.name
  image_tag_mutability = "IMMUTABLE"

  # `terraform destroy` refuses to remove a repo with images in it
  # otherwise — every smoke iteration would need a manual
  # `aws ecr delete-repository --force` to unblock teardown.
  # Real customer deployments (where images are long-lived) may
  # want to flip this back to `false` and rely on lifecycle
  # policies for image churn.
  force_delete = true

  image_scanning_configuration {
    scan_on_push = true
  }
}
