# Azure Databricks is the compute and warehouse layer, replacing the pair that
# AWS needs (Spark on EMR for silver, Athena for gold) with one service that
# does both: PySpark jobs write Delta tables, and a SQL warehouse serves the dbt
# gold models over those same tables.
#
# Cost note: an idle workspace is free. Azure Databricks bills DBUs only while
# compute is running, so this resource can stay provisioned between sessions and
# still hold the project at zero. What has to be watched is clusters, not this.
resource "azurerm_databricks_workspace" "lakehouse" {
  name                = "${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location
  sku                 = var.databricks_sku

  # Databricks creates its own locked managed resource group for the workspace's
  # VNet, NSG, and managed disks. Naming it explicitly keeps `az group list`
  # readable instead of showing a generated GUID.
  managed_resource_group_name = "${var.project_name}-${var.environment}-databricks-managed"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# The workspace's own identity has nothing to do with reading the lake. It only
# appears at all when the workspace's managed DBFS storage uses customer-managed
# keys, and even then it authorizes access to Databricks' own storage, not to
# anything external. The identity Unity Catalog actually uses to reach external
# storage like this project's bronze and silver containers is a separate,
# dedicated resource: an access connector. This is the identity rbac.tf grants
# Storage Blob Data Contributor to, and the one a Unity Catalog storage
# credential and external location would be built against.
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
