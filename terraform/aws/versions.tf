terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state with locking. Commented so `terraform init -backend=false`
  # (and CI validate) works without a pre-existing bucket; uncomment and set the
  # names to run for real. See backend.tf.example.
  # backend "s3" {}
}
