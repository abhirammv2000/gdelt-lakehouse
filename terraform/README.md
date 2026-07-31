# Terraform — AWS lakehouse infrastructure

Provisions the cloud side of the "same code, two targets" story. The pipeline
code already switches on `GDELT_ENV=local|aws` (MinIO ⇄ S3, iceberg-rest ⇄ Glue);
this module stands up the AWS half.

## What it creates

| Resource | Purpose |
|---|---|
| `aws_s3_bucket` bronze / silver | Raw landing zone + Iceberg silver warehouse (versioned, SSE-encrypted, public access blocked, lifecycle-expiring old versions) |
| `aws_glue_catalog_database` | Iceberg catalog for the silver tables — the production replacement for the local iceberg-rest-fixture |
| `aws_iam_role` + policy | Least-privilege role the pipeline compute (Glue/EMR/EC2) assumes: read/write only the two lake buckets and the silver Glue DB |

Gold lives in **Snowflake** and orchestration in **MWAA** in the full design;
those are intentionally left as separate root modules (they carry standing cost
and org-specific wiring) rather than bundled here.

## Usage

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # set bucket_suffix to your account id
terraform init
terraform plan
terraform apply
```

For CI / offline validation (no credentials, no state):

```bash
terraform fmt -check
terraform init -backend=false
terraform validate
```

## Wiring the app to AWS

After `apply`, point the pipeline at the outputs:

```bash
export GDELT_ENV=aws
export GDELT_BRONZE_BUCKET=$(terraform output -raw bronze_bucket)
export GDELT_SILVER_BUCKET=$(terraform output -raw silver_bucket)
# S3 endpoint/keys unset -> the AWS SDK uses the assumed IAM role.
```

Spark then uses the Glue catalog instead of the REST catalog:

```
spark.sql.catalog.lakehouse            org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type       glue
spark.sql.catalog.lakehouse.warehouse  s3://<silver_bucket>/warehouse
```
