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

# Token for the Apigee provider, generated with gcloud auth print-access-token.
variable "access_token" {
  type = string
}

# Project that hosts the Apigee organization and associated load balancer.
variable "project_id" {
  type = string
}

# Name of the backend service associated with the load balancer created during
# Apigee one-click provisioning.
variable "apigee_backend_service_name" {
  type = string
}

# Name of the URL map associated with the load balancer created during Apigee
# one-click provisioning.
variable "apigee_lb_url_map_name" {
  type = string
}

# Name of a backend service that points to an internet NEG for a domain that
# points to the Mixer API's Cloud Endpoints deployment.
variable "api_esp_backend_service_name" {
  type = string
}

# Hostname for the Mixer API once it is proxied by Apigee.
variable "api_hostname" {
  type = string
}

# Alternate hostname for the Mixer API. Used temporarily for testing changes.
variable "api2_hostname" {
  type = string
}

# Hostname for the internal version of the NL API.
variable "nl_internal_api_hostname" {
  type = string
}

# Hostname for the public-facing version of the NL API.
variable "nl_api_hostname" {
  type = string
}

# ID of the GCP project that publishes a PSC service for both versions of the
# NL API.
variable "psc_project" {
  type = string
}

# Name of the PSC service for the NL API.
variable "psc_service_name" {
  type = string
}
