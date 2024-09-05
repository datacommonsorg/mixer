locals {
  apigee_service_id = "projects/${var.project_id}/global/backendServices/${var.apigee_backend_service_name}"
  api_esp_service_id = "projects/${var.project_id}/global/backendServices/${var.api_esp_backend_service_name}"

  matchers = {
    "matcher-api" = {
      hostname = var.api_hostname
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

resource "google_compute_url_map" "apigee_lb" {
  default_service = local.apigee_service_id
  name            = var.apigee_lb_name

  dynamic "host_rule" {
    for_each = local.matchers
    iterator = each
    content {
      hosts        = [each.value.hostname]
      path_matcher = each.key
    }
  }

  host_rule {
    hosts        = ["staging.api2.datacommons.org"]
    path_matcher = "matcher-api2"
  }

  dynamic "path_matcher" {
    for_each = local.matchers
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

  path_matcher {
    name            = "matcher-api2"
    default_service = local.api_esp_service_id
  }
}
