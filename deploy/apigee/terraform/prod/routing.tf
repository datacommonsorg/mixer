locals {
  service_id = "projects/${var.project_id}/global/backendServices/${var.apigee_backend_service_name}"
  api_matcher = "matcher-api"
  api2_matcher = "matcher-api2"
  bard_matcher = "matcher-bard"
  datagemma_matcher = "matcher-datagemma"
}

resource "google_compute_url_map" "apigee_lb_url_map" {
  default_service = local.service_id

  host_rule {
    hosts        = [var.api_hostname]
    path_matcher = local.api_matcher
  }

  host_rule {
    hosts        = [var.nl_internal_api_hostname]
    path_matcher = local.bard_matcher
  }

  host_rule {
    hosts        = [var.nl_api_hostname]
    path_matcher = local.datagemma_matcher
  }

  host_rule {
    hosts        = [var.api2_hostname]
    path_matcher = local.api2_matcher
  }

  name = var.apigee_lb_url_map_name

  path_matcher {
    default_service = local.service_id
    name            = local.datagemma_matcher

    route_rules {
      match_rules {
        prefix_match = "/"
      }

      priority = 1

      route_action {
        url_rewrite {
          path_prefix_rewrite = "/datagemma/"
        }

        weighted_backend_services {
          backend_service = local.service_id
          weight          = 100
        }
      }
    }
  }

  path_matcher {
    default_service = local.service_id
    name            = local.api_matcher

    route_rules {
      match_rules {
        prefix_match = "/"
      }

      priority = 1

      route_action {
        url_rewrite {
          path_prefix_rewrite = "/api/"
        }

        weighted_backend_services {
          backend_service = local.service_id
          weight          = 100
        }
      }
    }
  }

  path_matcher {
    default_service = local.service_id
    name            = local.bard_matcher

    route_rules {
      match_rules {
        prefix_match = "/"
      }

      priority = 1

      route_action {
        url_rewrite {
          path_prefix_rewrite = "/bard/"
        }

        weighted_backend_services {
          backend_service = local.service_id
          weight          = 100
        }
      }
    }
  }

  path_matcher {
    default_service = local.service_id
    name            = local.api2_matcher

    route_rules {
      match_rules {
        prefix_match = "/"
      }

      priority = 1

      route_action {
        url_rewrite {
          path_prefix_rewrite = "/api/"
        }

        weighted_backend_services {
          backend_service = local.service_id
          weight          = 100
        }
      }
    }
  }
}
