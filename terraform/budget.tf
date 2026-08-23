# A hard stop on cost, independent of remembering to run `terraform destroy`.
# This is account-wide, not scoped to this project's resources: AWS Budgets can
# filter by cost-allocation tag, but a tag has to be activated by hand in the
# Billing console and takes up to 24 hours to start appearing in Cost Explorer,
# so a tag-scoped budget would silently see nothing on a fresh account. An
# account-wide cap is a real, working guardrail today; a tag-scoped one would
# be a claim this repo could not back up without a manual step outside Terraform.
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
