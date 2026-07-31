provider "aws" {
  region = var.aws_region

  # Every resource is tagged for cost allocation and ownership.
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
