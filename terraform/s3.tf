locals {
  suffix = var.bucket_suffix != "" ? "-${var.bucket_suffix}" : ""

  # The medallion object-storage layers. Gold lives in a warehouse (Snowflake),
  # so only bronze (raw) and silver (Iceberg) are S3 buckets.
  buckets = {
    bronze = "${var.project_name}-bronze${local.suffix}"
    silver = "${var.project_name}-silver${local.suffix}"
  }
}

resource "aws_s3_bucket" "layer" {
  for_each = local.buckets

  bucket        = each.value
  force_destroy = var.force_destroy_buckets

  tags = {
    Layer = each.key
  }
}

# Keep object history - supports the Iceberg time-travel / reproducibility story.
resource "aws_s3_bucket_versioning" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Encrypt everything at rest (SSE-S3 by default; swap to aws:kms for CMKs).
resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# These are private data lakes - block all public access.
resource "aws_s3_bucket_public_access_block" "layer" {
  for_each = aws_s3_bucket.layer

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Expire old object versions and clean up incomplete multipart uploads.
resource "aws_s3_bucket_lifecycle_configuration" "layer" {
  for_each = aws_s3_bucket.layer

  bucket = each.value.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
