module "apigee" {
  source     = "github.com/terraform-google-modules/cloud-foundation-fabric//modules/apigee"
  project_id = "datcom-apigee-dev"
  envgroups = {
    staging-api       = ["staging.api.datacommons.org"]
    staging-bard      = ["staging.bard.datacommons.org"]
    staging-datagemma = ["staging.datagemma.datacommons.org"]
  }
  environments = {
    // DO NOT destroy this resource without also re-deploying proxies.
    dev = {
      display_name = "dev"
      envgroups    = ["staging-api", "staging-bard", "staging-datagemma"]
      type         = "INTERMEDIATE"
    }
  }
  instances = {
    us-central1 = {
      name         = "us-central1"
      description  = ""
      region       = "us-central1"
      environments = ["dev"]
    }
  }
  endpoint_attachments = {
    datcom-bard-staging-website = {
      region             = "us-central1"
      service_attachment = "projects/datcom-bard-staging/regions/us-central1/serviceAttachments/website"
    }
  }
}

resource "apigee_proxy" "api" {
  name = "api"
  bundle = ".tmp/nonprod/api.zip"
  bundle_hash = filebase64sha256(".tmp/nonprod/api.zip")
}

resource "apigee_proxy" "bard" {
  name = "bard"
  bundle = ".tmp/nonprod/bard.zip"
  bundle_hash = filebase64sha256(".tmp/nonprod/bard.zip")
}

resource "apigee_proxy" "datagemma" {
  name = "datagemma"
  bundle = ".tmp/nonprod/datagemma.zip"
  bundle_hash = filebase64sha256(".tmp/nonprod/datagemma.zip")
}
