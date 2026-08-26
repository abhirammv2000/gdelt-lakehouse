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
    # No versioning_enabled here. Azure does not support blob versioning on an
    # account with a hierarchical namespace, full stop - Terraform will not even
    # apply the two together. That is the one place this account cannot mirror
    # the "keep object history" story the AWS S3 buckets tell with versioning.
    #
    # Soft delete is the substitute, and it is a real substitute, not a shrug: it
    # recovers an overwritten or deleted blob within blob_retention_days. What it
    # does not give back is full version history the way S3 versioning or
    # Iceberg's own snapshots do - only the most recent prior state, not every one.
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

# No lifecycle rule here expiring "old versions", on purpose. Without blob
# versioning (see the note in blob_properties above) there is no separate
# noncurrent-version state to expire - only current, live blobs exist. A rule
# that deletes a blob N days after its last modification, which is what the S3
# noncurrent-version-expiration pattern would naively become here, would delete
# live bronze and silver data instead of an old copy of it. That is a
# correctness bug, not a cost optimization, so it is not written.
#
# The real Azure counterpart of the S3 lifecycle rule is soft delete
# (delete_retention_policy above), which recovers what was actually deleted
# rather than pruning what is still live.
