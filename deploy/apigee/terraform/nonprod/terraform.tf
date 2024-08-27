terraform {
  backend "gcs" {
    bucket = "datcom-apigee-dev-tf"
    prefix = "terraform/state"
  }

  required_providers {
    apigee = {
      source  = "scastria/apigee"
      version = "~> 0.1.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = "us-central1"
}

provider "apigee" {
  access_token = var.access_token
  organization = var.project_id
  server       = "apigee.googleapis.com"
}
