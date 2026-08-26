# Least-privilege access to the lake, the Azure counterpart of ../aws/iam.tf.
#
# Azure RBAC differs from IAM in a way worth stating: control-plane rights over a
# storage account (Contributor, Owner) do not grant data-plane access to the
# blobs inside it. Reading a file needs an explicit data role. That is why
# "Storage Blob Data Contributor" exists as its own assignment rather than being
# implied by ownership of the account.

data "azurerm_client_config" "current" {}

# The identity running Terraform also runs the pipeline locally, so it needs data
# access to the lake it just created. Scoped to this storage account only, not
# the resource group or subscription.
resource "azurerm_role_assignment" "operator_lake_data" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

# The Databricks workspace's managed identity needs the same data access so
# clusters can read bronze and write silver Delta tables without an account key
# baked into a notebook.
resource "azurerm_role_assignment" "databricks_lake_data" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_workspace.lakehouse.storage_account_identity[0].principal_id
}
