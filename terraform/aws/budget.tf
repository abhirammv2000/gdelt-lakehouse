# A hard cost stop, in case I forget to run terraform destroy.
# It covers the whole account, not just this project. A budget scoped by tag would need the
# tag turned on by hand in Billing, and it takes a day to show up.
resource "aws_budgets_budget" "monthly_cap" {
  name         = "${var.project_name}-monthly-cap"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}
