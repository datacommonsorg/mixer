# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module "apigee" {
  source     = "github.com/terraform-google-modules/cloud-foundation-fabric//modules/apigee"
  project_id = var.project_id
  envgroups = {
    api  = [var.api_hostname]
    bard = [var.nl_internal_api_hostname]
    nl   = [var.nl_api_hostname]
  }
  environments = {
    main = {
      display_name = "main"
      envgroups = [
        "api",
        "bard",
        "nl",
      ]
      type = "COMPREHENSIVE"
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
    nl-internal-backend = {
      region             = "us-central1"
      service_attachment = "projects/${var.nl_internal_psc_project}/regions/us-central1/serviceAttachments/${var.nl_internal_psc_service_name}"
    }
    nl-backend = {
      region             = "us-central1"
      service_attachment = "projects/${var.nl_psc_project}/regions/us-central1/serviceAttachments/${var.nl_psc_service_name}"
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

resource "apigee_proxy" "nl" {
  count       = var.include_proxies ? 1 : 0
  name        = "nl"
  bundle      = ".tmp/nl.zip"
  bundle_hash = filebase64sha256(".tmp/nl.zip")
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

resource "apigee_proxy_deployment" "main-nl" {
  count            = var.include_proxies ? 1 : 0
  proxy_name       = apigee_proxy.nl[0].name
  environment_name = "main"
  revision         = apigee_proxy.nl[0].revision # Deploy latest
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

resource "apigee_product" "datacommons-nl-api-internal" {
  count              = var.include_proxies ? 1 : 0
  name               = "datacommons-nl-api-internal"
  display_name       = "Data Commons NL API (Internal)"
  auto_approval_type = true
  description        = var.nl_internal_api_hostname
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

resource "apigee_product" "datacommons-nl-api" {
  count              = var.include_proxies ? 1 : 0
  name               = "datacommons-nl-api"
  display_name       = "Data Commons NL API"
  auto_approval_type = true
  description        = var.nl_api_hostname
  environments = [
    "main",
  ]
  attributes = {
    access = "public"
  }
  operation {
    api_source = apigee_proxy.nl[0].name
    path       = "/"
    methods    = [] # Accept all methods
  }
}
