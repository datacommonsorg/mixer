module "apigee" {
  source     = "github.com/terraform-google-modules/cloud-foundation-fabric//modules/apigee"
  project_id = "datcom-apigee-dev"
  envgroups = {
    staging-api = ["staging.api.datacommons.org"]
    staging-bard = ["staging.bard.datacommons.org"]
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
      name = "us-central1"
      description = ""
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
