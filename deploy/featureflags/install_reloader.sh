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

# Installs Reloader to automatically rollout feature flag ConfigMap changes.

set -e

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "Usage: $0 <config_dir> [environment]"
  echo "  <config_dir>: Directory containing the feature flag YAML files."
  echo "  [environment]: Optional. The specific environment to deploy to (e.g., 'dev')."
  exit 1
fi
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/../cluster_iterator.sh"

# Callback function to install reloader on a single cluster.
# Arguments: $1:PROJECT_ID, $2:LOCATION, $3:CLUSTER_NAME
install_reloader_on_cluster() {
  local PROJECT_ID=$1
  local LOCATION=$2
  local CLUSTER_NAME=$3

  echo "Switching to cluster: ${CLUSTER_NAME} in project ${PROJECT_ID} (${LOCATION})"
  gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${LOCATION}"

  # Install reloader
  echo "Installing/upgrading Reloader in cluster '${CLUSTER_NAME}'..."
  helm upgrade --install reloader stakater/reloader --namespace default
}

# Add the stakater helm repository
helm repo add stakater https://stakater.github.io/stakater-charts

# Update helm repositories
helm repo update

iterate_clusters "$1" "$2" "install_reloader_on_cluster"

echo "---"
echo "Successfully installed/upgraded Reloader on all targeted clusters."
