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

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "Usage: $0 <config_dir> [environment]"
  echo "  <config_dir>: Directory containing the feature flag YAML files."
  echo "  [environment]: Optional. The specific environment to deploy to (e.g., 'dev')."
  exit 1
fi

# Source cluster iterator from the same directory as this script.
source "$(dirname "${BASH_SOURCE[0]}")/cluster_iterator.sh"

CONFIG_MAP_NAME="mixer-feature-flags"
CONTAINER_NAME="mixer"

# Callback function to deploy flags to a single cluster.
# Arguments: $1:PROJECT_ID, $2:LOCATION, $3:CLUSTER_NAME, $4:CONFIG_FILE
deploy_flags_to_cluster() {
  local PROJECT_ID=$1
  local LOCATION=$2
  local CLUSTER_NAME=$3
  local CONFIG_FILE=$4

  echo "Switching to cluster: ${CLUSTER_NAME} in project ${PROJECT_ID} (${LOCATION})"
  gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}"

  # Find all unique namespaces with a mixer deployment and store them in an array.
  # Mixer containers are matched by name label (name is defined in the Helm chart).
  echo "Finding '${CONTAINER_NAME}' deployments in cluster..."
  # Using a while-read loop for compatibility with older bash versions (pre-v4) that lack the 'mapfile' command.
  namespaces=()
  while IFS= read -r line; do
    [[ -n "$line" ]] && namespaces+=("$line")
  done < <(kubectl get deployment --all-namespaces -l app.kubernetes.io/name=${CONTAINER_NAME} -o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}' --context="gke_${PROJECT_ID}_${LOCATION}_${CLUSTER_NAME}" | sort -u)

  if [ ${#namespaces[@]} -eq 0 ]; then
    echo "No '${CONTAINER_NAME}' deployments found in any namespace."
    return
  fi

  # This block needs to be inside the callback to get the correct CONFIG_FILE
  # The application expects the data key to be 'feature_flags.yaml'.

  # Apply the ConfigMap to each namespace found.
  for ns in "${namespaces[@]}"; do
    echo "Deploying ConfigMap '${CONFIG_MAP_NAME}' to namespace '${ns}'..."
    kubectl create configmap "${CONFIG_MAP_NAME}" \
      --from-file=feature_flags.yaml="$CONFIG_FILE" \
      --namespace="$ns" \
      --dry-run=client -o yaml | kubectl apply --context="gke_${PROJECT_ID}_${LOCATION}_${CLUSTER_NAME}" -f -
  done
}

iterate_clusters "$1" "$2" "deploy_flags_to_cluster"

echo "---"
echo "Successfully deployed all feature flags."
