# ADLS Gen2 is the Azure counterpart of the S3 buckets in ../aws/s3.tf.
#
# The one flag that matters here is is_hns_enabled. A storage account without a
# hierarchical namespace is flat blob storage, where "directories" are just key
# prefixes and a rename is a copy of every object underneath. With it on, the
# account is ADLS Gen2: real directories, atomic renames, and POSIX-style ACLs.
# Spark and Delta Lake both depend on cheap atomic renames for commit protocols,
# so this is a correctness and performance flag, not a preference.

resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

locals {
  suffix = var.storage_account_suffix != "" ? var.storage_account_suffix : random_string.suffix.result

  # Storage account names are globally unique across all of Azure, 3-24 chars,
  # lowercase alphanumeric only. "gdelt-lakehouse" has to lose its hyphens, and
  # the name gets truncated to leave room for the suffix.
  storage_account_name = substr(replace("${var.project_name}${var.environment}", "-", ""), 0, 24 - length(local.suffix))

  # The medallion object-storage layers, matching the AWS bucket layout. Gold
  # lives in a warehouse (Databricks SQL), so only bronze and silver are here.
  containers = {
    bronze = "bronze"
    silver = "silver"
  }
}

resource "azurerm_resource_group" "lakehouse" {
  name     = "${var.project_name}-${var.environment}-rg"
  location = var.location

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "azurerm_storage_account" "lake" {
  name                = "${local.storage_account_name}${local.suffix}"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = azurerm_resource_group.lakehouse.location

  account_tier             = "Standard"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # ADLS Gen2. See the note at the top of this file.
  min_tls_version          = "TLS1_2"
  access_tier              = "Hot"
  account_replication_type = "LRS" # Locally redundant: this is reproducible data, not records.

  # This is a private data lake. The Azure equivalent of an S3 public access block.
  allow_nested_items_to_be_public = false
  public_network_access_enabled   = true # Needed: Spark and dbt run outside Azure.
  https_traffic_only_enabled      = true
  shared_access_key_enabled       = true # Local Spark authenticates with an account key.

  blob_properties {
    # Keep object history, supporting the same time-travel and reproducibility
    # story as S3 versioning on the AWS side.
    versioning_enabled = true

    delete_retention_policy {
      days = var.blob_retention_days
    }

    container_delete_retention_policy {
      days = var.blob_retention_days
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "azurerm_storage_container" "layer" {
  for_each = local.containers

  name                  = each.value
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# Expire previous blob versions so the lake does not grow without bound. The
# Azure counterpart of the S3 noncurrent-version expiry lifecycle rule.
resource "azurerm_storage_management_policy" "lake" {
  storage_account_id = azurerm_storage_account.lake.id

  rule {
    name    = "expire-old-versions"
    enabled = true

    filters {
      blob_types = ["blockBlob"]
    }

    actions {
      version {
        delete_after_days_since_creation = var.blob_retention_days
      }

      base_blob {
        delete_after_days_since_modification_greater_than = var.blob_retention_days
      }
    }
  }
}
