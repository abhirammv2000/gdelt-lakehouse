data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

# Trust policy: the compute that runs the pipeline (Glue / EMR / EC2) assumes this role.
data "aws_iam_policy_document" "assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com", "elasticmapreduce.amazonaws.com", "ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${var.project_name}-${var.environment}-pipeline"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

# Least-privilege permissions: only the two lake buckets and the silver Glue DB.
data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "ListLakeBuckets"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for b in aws_s3_bucket.layer : b.arn]
  }

  statement {
    sid       = "ReadWriteLakeObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [for b in aws_s3_bucket.layer : "${b.arn}/*"]
  }

  statement {
    sid    = "GlueIcebergCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:BatchCreatePartition",
      "glue:GetPartitions",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.silver.name}",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.silver.name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "pipeline" {
  name   = "${var.project_name}-${var.environment}-pipeline"
  role   = aws_iam_role.pipeline.id
  policy = data.aws_iam_policy_document.pipeline.json
}
