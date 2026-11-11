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
    staging-api = [var.api_hostname]
  }
  environments = {
    dev = {
      display_name = "dev"
      envgroups = [
        "staging-api",
      ]
      type = "INTERMEDIATE"
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
}

resource "apigee_proxy" "api" {
  name        = "api"
  bundle      = ".tmp/api.zip"
  bundle_hash = filebase64sha256(".tmp/api.zip")
}


resource "apigee_proxy_deployment" "dev-api" {
  proxy_name       = apigee_proxy.api.name
  environment_name = "dev"
  revision         = apigee_proxy.api.revision # Deploy latest
  service_account  = var.deployment_service_account
}


resource "apigee_product" "datacommons-api-staging" {
  name               = "datacommons-api-staging"
  display_name       = "Data Commons API (Staging)"
  auto_approval_type = true
  description        = var.api_hostname
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

