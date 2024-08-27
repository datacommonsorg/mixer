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

provider "apigee" {
  // Run `gcloud auth print-access-token`. Don't commit the value!
  access_token = ""
  organization = "datcom-apigee-dev"
}
