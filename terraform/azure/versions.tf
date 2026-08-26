terraform {
  required_version = ">= 1.6"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Remote state with locking. Commented so `terraform init -backend=false`
  # (and CI validate) works without a pre-existing storage account; uncomment and
  # set the names to run for real. See backend.tf.example.
  # backend "azurerm" {}
}
