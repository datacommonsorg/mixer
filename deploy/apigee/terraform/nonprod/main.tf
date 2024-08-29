module "apigee" {
  source     = "github.com/terraform-google-modules/cloud-foundation-fabric//modules/apigee"
  project_id = var.project_id
  envgroups = {
    staging-api       = [var.api_hostname]
    staging-bard      = [var.nl_internal_api_hostname]
    staging-datagemma = [var.nl_api_hostname]
  }
  environments = {
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
      service_attachment = "projects/${var.psc_project}/regions/us-central1/serviceAttachments/${var.psc_service_name}"
    }
  }
}

resource "apigee_proxy" "api" {
  name = "api"
  bundle = ".tmp/api.zip"
  bundle_hash = filebase64sha256(".tmp/api.zip")
}

resource "apigee_proxy" "bard" {
  name = "bard"
  bundle = ".tmp/bard.zip"
  bundle_hash = filebase64sha256(".tmp/bard.zip")
}

resource "apigee_proxy" "datagemma" {
  name = "datagemma"
  bundle = ".tmp/datagemma.zip"
  bundle_hash = filebase64sha256(".tmp/datagemma.zip")
}

resource "apigee_proxy_deployment" "dev-api" {
  proxy_name = apigee_proxy.api.name
  environment_name = "dev"
  revision = apigee_proxy.api.revision # Deploy latest
}

resource "apigee_proxy_deployment" "dev-bard" {
  proxy_name = apigee_proxy.bard.name
  environment_name = "dev"
  revision = apigee_proxy.bard.revision # Deploy latest
}

resource "apigee_proxy_deployment" "dev-datagemma" {
  proxy_name = apigee_proxy.datagemma.name
  environment_name = "dev"
  revision = apigee_proxy.datagemma.revision # Deploy latest
}

resource "apigee_product" "datacommons-api-staging" {
  name = "datacommons-api-staging"
  display_name = "Data Commons API (Staging)"
  auto_approval_type = true
  description = var.api_hostname
  environments = [
    "dev",
  ]
  attributes = {
    access = "public"
  }
  operation {
    api_source = apigee_proxy.api.name
    path       = "/"
    methods    = [] # Accept all methods
  }
}

resource "apigee_product" "datacommons-nl-api-internal-staging" {
  name = "datacommons-nl-api-internal-staging"
  display_name = "Data Commons NL API (Internal, Staging)"
  auto_approval_type = true
  description = var.nl_internal_api_hostname
  environments = [
    "dev",
  ]
  attributes = {
    access = "internal"
  }
  operation {
    api_source = apigee_proxy.bard.name
    path       = "/"
    methods    = [] # Accept all methods
  }
}

resource "apigee_product" "datacommons-nl-api-staging" {
  name = "datacommons-nl-api-staging"
  display_name = "Data Commons NL API (Staging)"
  auto_approval_type = true
  description = var.nl_api_hostname
  environments = [
    "dev",
  ]
  attributes = {
    access = "public"
  }
  operation {
    api_source = apigee_proxy.datagemma.name
    path       = "/"
    methods    = [] # Accept all methods
  }
}
