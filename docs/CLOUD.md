# Running on other warehouses and on Azure

Build the same gold models on another warehouse:

```bash
# BigQuery
gcloud auth application-default login
dbt build --target bq --project-dir dbt --profiles-dir dbt

# Databricks SQL (Azure). Terraform prints the host; the HTTP path names a
# SQL warehouse, not a cluster.
export DATABRICKS_HOST=$(terraform -chdir=terraform/azure output -raw databricks_host)
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
export DATABRICKS_TOKEN=<token>
export GDELT_SILVER_DB=gdelt_lakehouse GDELT_SILVER_SCHEMA=gdelt
dbt build --target databricks --project-dir dbt --profiles-dir dbt
```

Run the whole thing on Azure:

```bash
cd terraform/azure && cp terraform.tfvars.example terraform.tfvars   # set budget_alert_email
az login && terraform init && terraform apply

export GDELT_ENV=azure
export GDELT_AZURE_STORAGE_ACCOUNT=$(terraform output -raw storage_account)
export GDELT_BRONZE_BUCKET=bronze GDELT_SILVER_BUCKET=silver
gdelt ingest latest        # lands in ADLS Gen2, no secrets: uses your az login
```

> **On Git Bash for Windows**, export `MSYS_NO_PATHCONV=1` first. MSYS rewrites
> leading-slash values into Windows paths, so `/sql/1.0/warehouses/...` silently
> becomes `C:/Program Files/Git/sql/...` and dbt fails with a 404 that looks like a
> credentials problem. The same conversion mangles `/Shared/...` workspace paths
> passed to the Databricks CLI.
