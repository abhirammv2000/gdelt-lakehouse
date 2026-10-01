# A hard cost stop, in case I forget to tear things down. Same idea as ../aws/budget.tf.
#
# It covers the whole subscription, not one resource group. Databricks makes its own
# resource group for its VMs, and a budget on my group would miss that spend.
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
