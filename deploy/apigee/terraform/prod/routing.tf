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
  apigee_service_id = "projects/${var.project_id}/global/backendServices/${var.apigee_backend_service_name}"

  apigee_matchers = {
    "matcher-api" = {
      hostname = var.api_hostname
      prefix   = "/api/"
    },
    "matcher-api2" = {
      hostname = var.api2_hostname
      # prefix   = "/api/"
      prefix   = "/bard/"
    },
    "matcher-bard" = {
      hostname = var.nl_internal_api_hostname
      prefix   = "/bard/"
    },
    "matcher-nl" = {
      hostname = var.nl_api_hostname
      prefix   = "/nl/"
    }
  }

  api_esp_matchers = {
    # "matcher-api" = {
    #   hostname = var.api_hostname
    # }
    # "matcher-api2" = {
    #   hostname = var.api2_hostname
    # }
  }
}

resource "google_compute_url_map" "apigee_lb_url_map" {
  default_service = local.apigee_service_id
  name            = var.apigee_lb_url_map_name

  dynamic "host_rule" {
    for_each = local.apigee_matchers
    iterator = each
    content {
      hosts        = [each.value.hostname]
      path_matcher = each.key
    }
  }

  dynamic "path_matcher" {
    for_each = local.apigee_matchers
    iterator = each
    content {
      default_service = local.apigee_service_id
      name            = each.key

      route_rules {
        match_rules {
          prefix_match = "/healthz/ingress"
        }

        priority = 1

        route_action {
          weighted_backend_services {
            backend_service = local.apigee_service_id
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
            backend_service = local.apigee_service_id
            weight          = 100
          }
        }
      }
    }
  }

  dynamic "host_rule" {
    for_each = local.api_esp_matchers
    iterator = each
    content {
      hosts        = [each.value.hostname]
      path_matcher = each.key
    }
  }

  dynamic "path_matcher" {
    for_each = local.api_esp_matchers
    iterator = each
    content {
      name            = each.key
      default_service = google_compute_backend_service.api_esp_backend_service.id
    }
  }
}

resource "google_compute_backend_service" "api_esp_backend_service" {
  connection_draining_timeout_sec = 0
  load_balancing_scheme           = "EXTERNAL_MANAGED"
  locality_lb_policy              = "ROUND_ROBIN"

  log_config {
    enable      = true
    sample_rate = 1
  }

  name             = "api-esp-backend-service"
  port_name        = "http"
  protocol         = "HTTPS"
  session_affinity = "NONE"
  timeout_sec      = 30

  backend {
    group = google_compute_global_network_endpoint_group.api_esp_neg.id
  }
}

resource "google_compute_global_network_endpoint_group" "api_esp_neg" {
  name                  = "api-esp-neg"
  network_endpoint_type = "INTERNET_FQDN_PORT"
}

resource "google_compute_global_network_endpoint" "api_esp_endpoint" {
  global_network_endpoint_group = google_compute_global_network_endpoint_group.api_esp_neg.id
  fqdn                          = var.api_esp_hostname
  port                          = 443
}

