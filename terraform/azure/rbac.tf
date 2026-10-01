# Least-privilege access to the lake. This is the Azure version of ../aws/iam.tf.
#
# Owner or Contributor on a storage account does not let you read its blobs. You need a
# data role for that, so Storage Blob Data Contributor is a separate assignment.

data "azurerm_client_config" "current" {}

# Whoever runs Terraform also runs the pipeline locally, so they need data access to the
# lake. Scoped to this storage account only.
resource "azurerm_role_assignment" "operator_lake_data" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Databricks reads storage as the access connector, so the connector needs the same data
# access. That way no account key goes into a notebook.
resource "azurerm_role_assignment" "databricks_lake_data" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.lakehouse.identity[0].principal_id
}
