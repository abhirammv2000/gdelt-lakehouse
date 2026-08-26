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

variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "bucket_suffix" {
  description = "Suffix to keep S3 bucket names globally unique (e.g. your account id)."
  type        = string
  default     = ""
}

variable "force_destroy_buckets" {
  description = "Allow terraform destroy to delete non-empty buckets (dev only)."
  type        = bool
  default     = false
}

variable "noncurrent_version_expiration_days" {
  description = "Days to retain noncurrent object versions before expiry."
  type        = number
  default     = 30
}

variable "budget_limit_usd" {
  description = "Monthly AWS cost cap. An email alert fires at 50% of actual spend and 100% of forecast spend."
  type        = number
  default     = 10
}

variable "budget_alert_email" {
  description = "Email address that receives budget alerts. Required at apply time - AWS Budgets rejects an empty subscriber list, so leaving this unset fails the apply rather than silently creating a budget nobody is told about."
  type        = string
  default     = ""
}
