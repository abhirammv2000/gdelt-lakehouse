# AWS Glue Data Catalog is the production Iceberg catalog (the cloud stand-in for
# the local iceberg-rest-fixture). Spark writes the silver Iceberg tables here via
#   spark.sql.catalog.lakehouse.type = glue
# and the warehouse points at the silver bucket.
resource "aws_glue_catalog_database" "silver" {
  name        = replace("${var.project_name}_silver", "-", "_")
  description = "Iceberg silver tables for the GDELT lakehouse (managed by Spark)."

  location_uri = "s3://${aws_s3_bucket.layer["silver"].id}/warehouse"
}
