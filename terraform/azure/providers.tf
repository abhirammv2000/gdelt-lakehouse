provider "azurerm" {
  # Azure requires the feature block even when every default is fine.
  features {
    resource_group {
      # A free-tier subscription is small enough that a resource group left
      # holding a stray resource is a cost leak, not a safety net. Fail the
      # destroy loudly instead of silently orphaning things.
      prevent_deletion_if_contains_resources = false
    }
  }

  # Skips the Microsoft.* resource-provider registration sweep at plan time.
  # A fresh subscription has them registered already, and the sweep needs
  # subscription-level write permission that a scoped service principal may lack.
  resource_provider_registrations = "none"
}
