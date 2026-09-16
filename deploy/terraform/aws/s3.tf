resource "aws_s3_bucket" "warehouse" {
  bucket        = local.warehouse_bucket
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket                  = aws_s3_bucket.warehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "warehouse" {
  count  = var.warehouse_lifecycle_days_to_glacier > 0 ? 1 : 0
  bucket = aws_s3_bucket.warehouse.id

  rule {
    id     = "warehouse-glacier-transition"
    status = "Enabled"

    # Cover the whole bucket — Iceberg manifests + Parquet data both
    # benefit from cold storage once they age out.
    filter {}

    transition {
      days          = var.warehouse_lifecycle_days_to_glacier
      storage_class = "GLACIER_IR"
    }

    # Always clean up old object versions so the versioning above
    # doesn't blow the bill out.
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}
