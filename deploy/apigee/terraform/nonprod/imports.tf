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

import {
  to = google_compute_url_map.apigee_lb
  id = var.apigee_lb_url_map_name
}

import {
  to = module.apigee.google_apigee_envgroup.envgroups["staging-bard"]
  id = "staging-bard"
}


import {
  to = module.apigee.google_apigee_environment.environments["dev"]
  id = "dev"
}

import {
  to = module.apigee.google_apigee_envgroup_attachment.envgroup_attachments["dev-staging-bard"]
  id = "staging-bard/attachments/99b235d2-31dd-4f32-ad63-fbeaa44871ca"
}


import {
  to = module.apigee.google_apigee_instance.instances["us-central1"]
  id = "us-central1"
}

import {
  to = module.apigee.google_apigee_instance_attachment.instance_attachments["us-central1-dev"]
  id = "us-central1/attachments/a4dd5147-38c9-4bde-82bb-312f14e535e1"
}

import {
  to = module.apigee.google_apigee_endpoint_attachment.endpoint_attachments["datcom-bard-staging-website"]
  id = "endpointAttachments/datcom-bard-staging-website"
}

import {
  to = apigee_proxy.api
  id = "api"
}

import {
  to = apigee_proxy_deployment.dev-api
  id = "dev:api"
}

import {
  to = apigee_proxy.bard
  id = "bard"
}

import {
  to = apigee_proxy_deployment.dev-bard
  id = "dev:bard"
}



import {
  to = apigee_product.datacommons-api-staging
  id = "datacommons-api-staging"
}

import {
  to = apigee_product.datacommons-nl-api-staging
  id = "datacommons-nl-api-staging"
}

import {
  to = google_compute_global_network_endpoint_group.api_esp_neg
  id = "api-esp-neg"
}

import {
  to = google_compute_global_network_endpoint.api_esp_endpoint
  id = "api-esp-neg//staging.api-esp.datacommons.org/0"
}

import {
  to = google_compute_backend_service.api_esp_backend_service
  id = "api-esp-backend-service"
}
