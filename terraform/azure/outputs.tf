output "resource_group" {
  description = "Resource group holding every Azure resource for the lakehouse."
  value       = azurerm_resource_group.lakehouse.name
}

output "storage_account" {
  description = "ADLS Gen2 storage account backing bronze and silver."
  value       = azurerm_storage_account.lake.name
}

output "bronze_url" {
  description = "abfss:// URL for the bronze (raw) container."
  value       = "abfss://${azurerm_storage_container.layer["bronze"].name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
}

output "silver_url" {
  description = "abfss:// URL for the silver (Delta) container."
  value       = "abfss://${azurerm_storage_container.layer["silver"].name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
}

output "databricks_host" {
  description = "Databricks workspace URL, used as DATABRICKS_HOST by the CLI and dbt."
  value       = "https://${azurerm_databricks_workspace.lakehouse.workspace_url}"
}

output "access_connector_id" {
  description = "Resource ID of the Databricks access connector. This is what a Unity Catalog storage credential references, not the workspace itself, to reach the bronze and silver containers."
  value       = azurerm_databricks_access_connector.lakehouse.id
}

output "budget_name" {
  description = "Subscription cost cap; confirm the alert contact in Cost Management after apply."
  value       = azurerm_consumption_budget_subscription.monthly_cap.name
}
