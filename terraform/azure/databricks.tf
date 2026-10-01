# Azure Databricks does the compute and the SQL warehouse. On AWS that takes two services
# (Spark on EMR and Athena). Here PySpark writes the Delta tables and a SQL warehouse runs
# the dbt gold models on them.
#
# An idle workspace costs nothing. You only pay while a cluster runs, so watch the clusters.
resource "azurerm_databricks_workspace" "lakehouse" {
  name                = "${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location
  sku                 = var.databricks_sku

  # Databricks makes its own locked resource group for the VNet and disks. Naming it keeps
  # `az group list` readable.
  managed_resource_group_name = "${var.project_name}-${var.environment}-databricks-managed"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# The workspace's own identity does not read the lake. Unity Catalog uses the access
# connector below for that. rbac.tf gives this connector the Storage Blob Data Contributor
# role.
resource "azurerm_databricks_access_connector" "lakehouse" {
  name                = "${var.project_name}-${var.environment}-access-connector"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
