provider "aws" {
  region = var.region

  default_tags {
    tags = merge(
      {
        Project   = "siglake"
        ManagedBy = "terraform"
      },
      var.tags,
    )
  }
}
