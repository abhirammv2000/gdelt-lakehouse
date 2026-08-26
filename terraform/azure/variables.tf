variable "project_name" {
  description = "Name prefix for all resources."
  type        = string
  default     = "gdelt-lakehouse"
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "location" {
  description = "Azure region. eastus has the widest service availability and the most default quota on a new subscription, which matters because Databricks clusters need vCPU quota."
  type        = string
  default     = "eastus"
}

variable "storage_account_suffix" {
  description = "Suffix keeping the storage account name globally unique. Left empty a random 6-character suffix is generated, because Azure storage account names share one global namespace and 'gdeltlakehousedev' will already be taken."
  type        = string
  default     = ""

  validation {
    condition     = can(regex("^[a-z0-9]{0,8}$", var.storage_account_suffix))
    error_message = "storage_account_suffix must be 0-8 lowercase alphanumeric characters (Azure storage account names allow nothing else)."
  }
}

variable "blob_retention_days" {
  description = "Days to retain soft-deleted blobs and previous versions. The Azure counterpart of the S3 noncurrent-version expiry."
  type        = number
  default     = 30
}

variable "databricks_sku" {
  description = "Azure Databricks workspace tier. 'premium' is required for Unity Catalog and role-based access on notebooks and jobs. An idle workspace bills nothing at either tier; only running clusters consume DBUs, so premium costs nothing extra until compute starts."
  type        = string
  default     = "premium"

  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "databricks_sku must be one of: standard, premium, trial."
  }
}

variable "budget_limit_usd" {
  description = "Monthly Azure cost cap. Alerts fire at 50% and 80% of actual spend and 100% of forecast spend."
  type        = number
  default     = 10
}

variable "budget_alert_email" {
  description = "Email address that receives budget alerts. Required at apply time - a budget with no contact is a budget nobody hears about, so an empty value fails the apply rather than creating one silently."
  type        = string
  default     = ""

  validation {
    condition     = var.budget_alert_email == "" || can(regex("^[^@]+@[^@]+[.][^@]+$", var.budget_alert_email))
    error_message = "budget_alert_email must be a valid email address."
  }
}
