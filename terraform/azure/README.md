# Terraform - Azure lakehouse infrastructure

The Azure half of the "same code, different target" story. The pipeline already
switches storage and catalog on configuration rather than code, and this module
stands up what that configuration points at on Azure.

## What it creates

| Resource | Purpose |
|---|---|
| `azurerm_resource_group` | One group holding everything, so teardown is a single delete |
| `azurerm_storage_account` (ADLS Gen2) | The lake. `is_hns_enabled = true` is the flag that makes it ADLS Gen2 rather than flat blob storage: real directories and atomic renames, which Spark and Delta commit protocols depend on |
| `azurerm_storage_container` bronze / silver | The medallion layers, matching the S3 bucket layout on the AWS side |
| `azurerm_storage_management_policy` | Expires old blob versions, the counterpart of the S3 noncurrent-version lifecycle rule |
| `azurerm_databricks_workspace` | Compute and warehouse. Replaces the EMR-plus-Athena pair with one service that runs both the PySpark silver job and the SQL warehouse dbt builds gold on |
| `azurerm_role_assignment` x2 | Least-privilege data access for the operator identity and the workspace identity, scoped to the storage account |
| `azurerm_consumption_budget_subscription` | Hard cost cap with alerts at 50%, 80%, and 100% of forecast |

## Two things that differ from the AWS module

**Control plane and data plane are separate in Azure RBAC.** Owning a storage
account does not let you read the blobs inside it. Data access needs an explicit
role, which is why `Storage Blob Data Contributor` is assigned separately in
`rbac.tf` rather than being implied by ownership. This trips people up because
the AWS mental model does not have the split.

**The budget is subscription-scoped, not resource-group-scoped.** Databricks
creates its own managed resource group for cluster VMs and networking, and that
is where compute spend actually lands. A budget scoped to the lakehouse resource
group would be watching the cheap half of the account.

## Usage

```bash
cd terraform/azure
cp terraform.tfvars.example terraform.tfvars   # set budget_alert_email
az login
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

## Wiring the app to Azure

After `apply`, point the pipeline at the outputs:

```bash
export GDELT_ENV=azure
export GDELT_BRONZE_URL=$(terraform output -raw bronze_url)
export GDELT_SILVER_URL=$(terraform output -raw silver_url)
export DATABRICKS_HOST=$(terraform output -raw databricks_host)
```

## Cost

An idle workspace and an empty lake bill essentially nothing: Azure Databricks
charges DBUs only while compute runs, and a few hundred MB of ADLS storage is
cents per month. The spend to watch is running clusters. Terminate them, or run
`terraform destroy`, which removes the workspace and its managed resource group
together.
