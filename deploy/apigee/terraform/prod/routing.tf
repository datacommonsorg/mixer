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

locals {
  service_id = "projects/${var.project_id}/global/backendServices/${var.apigee_backend_service_name}"

  matchers = {
    "matcher-api" = {
      hostname = var.api_hostname
      prefix   = "/api/"
    },
    "matcher-api2" = {
      hostname = var.api2_hostname
      prefix   = "/api/"
    },
    "matcher-bard" = {
      hostname = var.nl_internal_api_hostname
      prefix   = "/bard/"
    },
    "matcher-datagemma" = {
      hostname = var.nl_api_hostname
      prefix   = "/datagemma/"
    }
  }
}

resource "google_compute_url_map" "apigee_lb_url_map" {
  default_service = local.service_id
  name            = var.apigee_lb_url_map_name

  dynamic "host_rule" {
    for_each = local.matchers
    iterator = each
    content {
      hosts        = [each.value.hostname]
      path_matcher = each.key
    }
  }


  dynamic "path_matcher" {
    for_each = local.matchers
    iterator = each
    content {
      default_service = local.service_id
      name            = each.key

      route_rules {
        match_rules {
          prefix_match = "/healthz/ingress"
        }

        priority = 1

        route_action {
          weighted_backend_services {
            backend_service = local.service_id
            weight          = 100
          }
        }
      }

      route_rules {
        match_rules {
          prefix_match = "/"
        }

        priority = 2

        route_action {
          url_rewrite {
            path_prefix_rewrite = each.value.prefix
          }

          weighted_backend_services {
            backend_service = local.service_id
            weight          = 100
          }
        }
      }
    }
  }
}
