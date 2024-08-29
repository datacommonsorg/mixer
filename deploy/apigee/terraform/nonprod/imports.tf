import {
  to = google_compute_url_map.apigee_lb
  id = var.apigee_lb_name
}

import {
  to = module.apigee.google_apigee_envgroup.envgroups["staging-bard"]
  id = "staging-bard"
}

import {
  to = module.apigee.google_apigee_envgroup.envgroups["staging-datagemma"]
  id = "staging-datagemma"
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
  to = module.apigee.google_apigee_envgroup_attachment.envgroup_attachments["dev-staging-datagemma"]
  id = "staging-datagemma/attachments/847e105c-a43c-4ef1-8fd1-3e4c9d920bf9"
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
  to = apigee_proxy.datagemma
  id = "datagemma"
}

import {
  to = apigee_proxy_deployment.dev-datagemma
  id = "dev:datagemma"
}

import {
  to = apigee_product.datacommons-api-staging
  id = "datacommons-api-staging"
}

import {
  to = apigee_product.datacommons-nl-api-staging
  id = "datacommons-nl-api-staging"
}
