module "apigee" {
  source     = "github.com/terraform-google-modules/cloud-foundation-fabric//modules/apigee"
  project_id = var.project_id
  envgroups = {
    api       = [var.api_hostname]
    bard      = [var.nl_api_hostname]
    datagemma = [var.llm_api_hostname]
  }
  environments = {
    main = {
      display_name = "main"
      envgroups    = ["api", "bard", "datagemma"]
      type         = "INTERMEDIATE"
    }
  }
  instances = {
    us-central1 = {
      name         = "us-central1"
      region       = "us-central1"
      environments = ["main"]
    }
  }
  endpoint_attachments = {
    nl-backend = {
      region             = "us-central1"
      service_attachment = "projects/${var.nl_psc_project}/regions/us-central1/serviceAttachments/${var.nl_psc_service_name}"
    }
    llm-backend = {
      region             = "us-central1"
      service_attachment = "projects/${var.llm_psc_project}/regions/us-central1/serviceAttachments/${var.llm_psc_service_name}"
    }
  }
}

resource "apigee_proxy" "api" {
  count       = var.include_proxies ? 1 : 0
  name        = "api"
  bundle      = ".tmp/api.zip"
  bundle_hash = filebase64sha256(".tmp/api.zip")
}

resource "apigee_proxy" "bard" {
  count       = var.include_proxies ? 1 : 0
  name        = "bard"
  bundle      = ".tmp/bard.zip"
  bundle_hash = filebase64sha256(".tmp/bard.zip")
}

resource "apigee_proxy" "datagemma" {
  count       = var.include_proxies ? 1 : 0
  name        = "datagemma"
  bundle      = ".tmp/datagemma.zip"
  bundle_hash = filebase64sha256(".tmp/datagemma.zip")
}

resource "apigee_proxy_deployment" "main-api" {
  count            = var.include_proxies ? 1 : 0
  proxy_name       = apigee_proxy.api[0].name
  environment_name = "main"
  revision         = apigee_proxy.api[0].revision # Deploy latest
}

resource "apigee_proxy_deployment" "main-bard" {
  count            = var.include_proxies ? 1 : 0
  proxy_name       = apigee_proxy.bard[0].name
  environment_name = "main"
  revision         = apigee_proxy.bard[0].revision # Deploy latest
}

resource "apigee_proxy_deployment" "main-datagemma" {
  count            = var.include_proxies ? 1 : 0
  proxy_name       = apigee_proxy.datagemma[0].name
  environment_name = "main"
  revision         = apigee_proxy.datagemma[0].revision # Deploy latest
}

resource "apigee_product" "datacommons-api" {
  count              = var.include_proxies ? 1 : 0
  name               = "datacommons-api"
  display_name       = "Data Commons API"
  auto_approval_type = true
  description        = var.api_hostname
  environments = [
    "main",
  ]
  attributes = {
    access = "public"
  }
  operation {
    api_source = apigee_proxy.api[0].name
    path       = "/"
    methods    = [] # Accept all methods
  }
}

resource "apigee_product" "datacommons-nl-api" {
  count              = var.include_proxies ? 1 : 0
  name               = "datacommons-nl-api"
  display_name       = "Bard API"
  auto_approval_type = true
  description        = var.nl_api_hostname
  environments = [
    "main",
  ]
  attributes = {
    access = "internal"
  }
  operation {
    api_source = apigee_proxy.bard[0].name
    path       = "/"
    methods    = [] # Accept all methods
  }
}

resource "apigee_product" "datacommons-llm-api" {
  count              = var.include_proxies ? 1 : 0
  name               = "datacommons-llm-api"
  display_name       = "DataGemma API"
  auto_approval_type = true
  description        = var.llm_api_hostname
  environments = [
    "main",
  ]
  attributes = {
    access = "internal"
  }
  operation {
    api_source = apigee_proxy.datagemma[0].name
    path       = "/"
    methods    = [] # Accept all methods
  }
}
