#!/bin/bash
#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is a shared script that provides a function to iterate over clusters
# defined in feature flag configuration files. It is not meant to be executed
# directly but sourced by other scripts.
#
# It defines one primary function:
#   - iterate_clusters <config_dir> [environment] <callback_function>
#
# The callback_function will be invoked for each cluster with the following
# arguments:
#   - PROJECT_ID
#   - LOCATION
#   - CLUSTER_NAME
#   - CONFIG_FILE (path to the environment's yaml config)

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "This script is not meant to be executed directly. Please source it."
    exit 1
fi

# Function to check for required command-line tools
check_dependencies() {
  for cmd in gcloud kubectl yq; do
    if ! command -v "$cmd" &> /dev/null; then
      echo "Error: Required command '$cmd' not found. Please install it and ensure it's in your PATH."
      exit 1
    fi
  done
}

iterate_clusters() {
  if [[ "$#" -lt 2 || "$#" -gt 3 ]]; then
    echo "Usage: iterate_clusters <config_dir> [environment] <callback_function>"
    return 1
  fi

  # Validate dependencies before proceeding
  check_dependencies

  # Remove trailing slash from CONFIG_DIR if it exists
  local CONFIG_DIR="${1%/}"
  local ENV
  local CALLBACK

  if [[ "$#" -eq 3 ]]; then
    ENV="${2}"
    CALLBACK="${3}"
  else
    ENV=""
    CALLBACK="${2}"
  fi

  local CONFIG_FILES=()

  if [[ -z "$ENV" ]]; then
    echo "No environment specified. Scanning for all environment config files in ${CONFIG_DIR}..."
    for f in "${CONFIG_DIR}"/*.yaml; do
      # A valid env config file must have a non-empty 'clusters' key.
      if [[ -n "$(yq e '.clusters[]' "$f" 2>/dev/null)" ]]; then
        CONFIG_FILES+=("$f")
      fi
    done
    if [ ${#CONFIG_FILES[@]} -eq 0 ]; then
      echo "No environment config files with cluster details found."
      return 0
    fi
  else
    # If an environment is specified, use that single file.
    local CONFIG_FILE="${CONFIG_DIR}/${ENV}.yaml"
    if [[ ! -f "$CONFIG_FILE" ]]; then
      echo "Error: Config file not found at ${CONFIG_FILE}"
      return 1
    fi
    CONFIG_FILES=("$CONFIG_FILE")
  fi

  for CONFIG_FILE in "${CONFIG_FILES[@]}"; do
    echo "=============================================================================="
    echo "Processing environment config: ${CONFIG_FILE}"
    echo "=============================================================================="

    # Iterate over each cluster defined in the config file
    for cluster in $(yq e '.clusters[]' "$CONFIG_FILE"); do
      echo "---"
      # Validate and parse the full cluster resource path
      # e.g., projects/PROJECT/locations/LOCATION/clusters/CLUSTER_NAME
      local CLUSTER_REGEX="^projects/([^/]+)/locations/([^/]+)/clusters/([^/]+)$"
      if [[ ! "$cluster" =~ $CLUSTER_REGEX ]]; then
        echo "Error: Invalid cluster format in ${CONFIG_FILE}: ${cluster}"
        echo "Expected format: projects/PROJECT/locations/LOCATION/clusters/CLUSTER_NAME"
        return 1
      fi

      # Invoke the callback with cluster details
      "$CALLBACK" "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}" "${CONFIG_FILE}"
    done
  done
}


