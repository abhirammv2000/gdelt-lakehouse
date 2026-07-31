output "bronze_bucket" {
  description = "Bronze (raw) S3 bucket name."
  value       = aws_s3_bucket.layer["bronze"].id
}

output "silver_bucket" {
  description = "Silver (Iceberg) S3 bucket name."
  value       = aws_s3_bucket.layer["silver"].id
}

output "glue_database" {
  description = "Glue catalog database backing the silver Iceberg tables."
  value       = aws_glue_catalog_database.silver.name
}

output "pipeline_role_arn" {
  description = "IAM role the pipeline compute assumes."
  value       = aws_iam_role.pipeline.arn
}
