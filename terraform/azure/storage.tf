# ADLS Gen2 is the Azure version of the S3 buckets in ../aws/s3.tf.
#
# The setting that matters is is_hns_enabled. Without it this is flat blob storage, and
# renaming a folder copies every file. With it you get real folders and atomic renames,
# which Spark and Delta need.

resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

locals {
  suffix = var.storage_account_suffix != "" ? var.storage_account_suffix : random_string.suffix.result

  # Storage account names must be unique across Azure, 3 to 24 lowercase letters and digits.
  # So "gdelt-lakehouse" loses its hyphen and gets cut short to leave room for a suffix.
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
    # No versioning_enabled here. Azure does not allow blob versioning when the hierarchical
    # namespace is on, so there is no copy of the S3 "keep old versions" setup.
    #
    # Soft delete is the substitute. It can bring back a deleted or overwritten blob, but only
    # the last version, not the full history.
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

# No lifecycle rule for old versions, on purpose. Without versioning there are no old
# versions, so a rule that deletes blobs after N days would delete live data. Soft delete
# is the real equivalent.
