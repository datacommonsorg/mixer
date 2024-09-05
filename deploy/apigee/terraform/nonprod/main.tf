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
