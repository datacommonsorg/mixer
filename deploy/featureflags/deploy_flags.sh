#!/bin/bash

# Copyright 2025 Google LLC
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

# This script deploys feature flag ConfigMaps to the appropriate Kubernetes clusters and namespaces.
#
# It reads a feature flag configuration file for all environments
# (or a specific environment), extracts the list of target clusters,
# and then for each cluster, it finds all namespaces containing a 'mixer'
# deployment and deploys the flags.
#
# Prerequisites:
# - gcloud CLI
# - kubectl
# - yq (a command-line YAML processor)
#
# Usage:
# ./deploy_flags.sh <config_dir> [environment]
# Example:
# ./deploy_flags.sh deploy/featureflags dev
# ./deploy_flags.sh deploy/featureflags

set -e

# Function to check for required command-line tools
check_dependencies() {
  for cmd in gcloud kubectl yq; do
    if ! command -v "$cmd" &> /dev/null; then
      echo "Error: Required command '$cmd' not found. Please install it and ensure it's in your PATH."
      exit 1
    fi
  done
}

# Validate dependencies before proceeding
check_dependencies

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "Usage: $0 <config_dir> [environment]"
  echo "  <config_dir>: Directory containing the feature flag YAML files."
  echo "  [environment]: Optional. The specific environment to deploy to (e.g., 'dev')."
  exit 1
fi

CONFIG_DIR="${1}"
ENV="${2}"
CONFIG_MAP_NAME="mixer-feature-flags"
CONTAINER_NAME="mixer"
CONFIG_FILES=()

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
    exit 0
  fi
else
  # If an environment is specified, use that single file.
  CONFIG_FILE="${CONFIG_DIR}/${ENV}.yaml"
  if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: Config file not found at ${CONFIG_FILE}"
    exit 1
  fi
  CONFIG_FILES=("$CONFIG_FILE")
fi

for CONFIG_FILE in "${CONFIG_FILES[@]}"; do
  echo "=============================================================================="
  echo "Processing environment config: ${CONFIG_FILE}"
  echo "=============================================================================="

  # Extract the 'flags' section and store it in a temporary file.
  # The application expects the data key to be 'feature_flags.yaml'.
  FLAGS_CONTENT_FILE=$(mktemp)
  trap 'rm -f "$FLAGS_CONTENT_FILE"' EXIT
  yq e '.flags' "$CONFIG_FILE" > "$FLAGS_CONTENT_FILE"

  # Iterate over each cluster defined in the config file
  for cluster in $(yq e '.clusters[]' "$CONFIG_FILE"); do
    echo "---"
    # Validate and parse the full cluster resource path
    # e.g., projects/PROJECT/locations/LOCATION/clusters/CLUSTER_NAME
    CLUSTER_REGEX="^projects/([^/]+)/locations/([^/]+)/clusters/([^/]+)$"
    if [[ ! "$cluster" =~ $CLUSTER_REGEX ]]; then
      echo "Error: Invalid cluster format in ${CONFIG_FILE}: ${cluster}"
      echo "Expected format: projects/PROJECT/locations/LOCATION/clusters/CLUSTER_NAME"
      exit 1
    fi

    PROJECT_ID="${BASH_REMATCH[1]}"
    LOCATION="${BASH_REMATCH[2]}"
    CLUSTER_NAME="${BASH_REMATCH[3]}"

    echo "Switching to cluster: ${CLUSTER_NAME} in project ${PROJECT_ID} (${LOCATION})"
    gcloud container clusters get-credentials "${CLUSTER_NAME}" \
      --project="${PROJECT_ID}" \
      --location="${LOCATION}"

    # Find all namespaces with a mixer deployment and store them in an array.
    # Mixer containers are matched by name label (name is defined in the Helm chart).
    echo "Finding '${CONTAINER_NAME}' deployments in cluster..."
    mapfile -t namespaces < <(kubectl get deployment --all-namespaces -l app.kubernetes.io/name=${CONTAINER_NAME} -o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}' --context="gke_${PROJECT_ID}_${LOCATION}_${CLUSTER_NAME}")

    if [ ${#namespaces[@]} -eq 0 ]; then
      echo "No '${CONTAINER_NAME}' deployments found in any namespace."
      continue
    fi

    # Apply the ConfigMap to each namespace found.
    for ns in "${namespaces[@]}"; do
      echo "Deploying ConfigMap '${CONFIG_MAP_NAME}' to namespace '${ns}'..."
      kubectl create configmap "${CONFIG_MAP_NAME}" \
        --from-file=feature_flags.yaml="$FLAGS_CONTENT_FILE" \
        --namespace="$ns" \
        --dry-run=client -o yaml | kubectl apply --context="gke_${PROJECT_ID}_${LOCATION}_${CLUSTER_NAME}" -f -
    done
  done
done

echo "---"
echo "Successfully deployed all feature flags."
