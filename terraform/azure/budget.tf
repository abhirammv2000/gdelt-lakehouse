# A hard stop on cost, independent of remembering to run `terraform destroy`.
# Same reasoning as ../aws/budget.tf: the guardrail has to work even when the
# teardown discipline fails, because that is exactly when it is needed.
#
# Scoped to the subscription rather than the resource group. A resource-group
# budget would miss the managed resource group Databricks creates for its VNet
# and cluster VMs, which is where the actual compute spend lands, so a
# group-scoped budget would watch the empty half of the account.
resource "azurerm_consumption_budget_subscription" "monthly_cap" {
  name            = "${var.project_name}-monthly-cap"
  subscription_id = "/subscriptions/${data.azurerm_client_config.current.subscription_id}"

  amount     = var.budget_limit_usd
  time_grain = "Monthly"

  time_period {
    # Azure rejects a start date in the past and requires the first of a month.
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timeadd(timestamp(), "744h"))
  }

  # Warn while there is still time to react.
  notification {
    enabled        = true
    threshold      = 50
    operator       = "GreaterThan"
    threshold_type = "Actual"
    contact_emails = [var.budget_alert_email]
  }

  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    threshold_type = "Actual"
    contact_emails = [var.budget_alert_email]
  }

  # Forecast crossing the cap means the current burn rate gets there this month,
  # which is the alert that arrives early enough to matter.
  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThan"
    threshold_type = "Forecasted"
    contact_emails = [var.budget_alert_email]
  }

  lifecycle {
    # start_date is computed from timestamp() and would otherwise show a diff on
    # every plan, making `terraform plan` noisy and hiding real changes.
    ignore_changes = [time_period]
  }
}
