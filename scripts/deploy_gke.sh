#!/bin/bash
# Copyright 2019 Google LLC
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


# Script to deploy mixer to a GKE cluster.
#
# Usage:
#
# ./deploy_key.sh <"mixer_prod"|"mixer_staging"|"mixer_autopush"|"mixer_encode"|"mixer_dev"|"mixer_private"> <commit_hash>
#
# First argument is either "mixer_prod" or "mixer_staging" or "mixer_autopush" or "mixer_encode" or "mixer_dev" or mixer_private.
# (Optional) second argument is the git commit hash of the mixer repo.
#
# !!! WARNING: Run this script in a clean Git checkout at the desired commit.
#
# This retrives the docker images and gRPC descriptor based on git commit hash,
# so these binaries should have been pushed to container registry and Cloud
# Storage by the continous deployment flow (../build/ci/cloudbuild.push.yaml).

set -e

ENV=$1

if [[
  $ENV != "mixer_staging" &&
  $ENV != "mixer_prod" &&
  $ENV != "mixer_autopush" &&
  $ENV != "mixer_encode" &&
  $ENV != "mixer_dev" &&
  $ENV != "mixer_private" &&
  $ENV != "mixer_stanford"
]]; then
  echo "First argument should be 'mixer_staging' or 'mixer_prod' or 'mixer_autopush' or 'mixer_encode' or 'mixer_dev' or 'mixer_private' or 'mixer_stanford'"
  exit
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$(dirname "$DIR")"

TAG=$(git rev-parse --short=7 HEAD)
if [[ $2 != "" ]]; then
  TAG=$2
  cd "$ROOT"
  # This is important to get the correct BT and BQ version
  git checkout "$TAG"
fi

mkdir -p "$ROOT/deploy/git"
cd "$ROOT/deploy/git"
echo -n "$TAG" > mixer_hash.txt

cd $ROOT

if [[ $ENV == "mixer_autopush" ]]; then
  # Update bigquery version
  gsutil cp gs://datcom-control/latest_base_bigquery_version.txt deploy/storage/bigquery.version
  # Import group
  yq eval -i 'del(.tables)' deploy/storage/base_bigtable_info.yaml
  yq eval -i '.tables = []' deploy/storage/base_bigtable_info.yaml
  for src in $(gsutil ls gs://datcom-control/autopush/*_latest_base_cache_version.txt); do
    echo "Copying $src"
    export TABLE="$(gsutil cat "$src")"
    yq eval -i '.tables += [env(TABLE)]' deploy/storage/base_bigtable_info.yaml
  done
fi
export PROJECT_ID=$(yq eval '.mixer.hostProject' deploy/helm_charts/envs/$ENV.yaml)
export REGION=$(yq eval '.region' deploy/helm_charts/envs/$ENV.yaml)
export IP=$(yq eval '.ip' deploy/helm_charts/envs/$ENV.yaml)
export DOMAIN=$(yq eval '.mixer.serviceName' deploy/helm_charts/envs/$ENV.yaml)
export API_TITLE=$(yq eval '.api_title' deploy/helm_charts/envs/$ENV.yaml)
export CLUSTER_NAME=mixer-$REGION

# Deploy to GKE
gcloud config set project $PROJECT_ID
gcloud container clusters get-credentials $CLUSTER_NAME --region $REGION

# Change "mixer_prod" for example, to "mixer-prod"
RELEASE=${ENV//_/-}

# Create a release specific image for the deployment, if it does not exist.
IMAGE_ERR=$(gcloud container images describe gcr.io/datcom-ci/datacommons-mixer:"$TAG" > /dev/null ; echo $?)
if [[ "$IMAGE_ERR" == "1" ]];  then ./scripts/push_binary.sh "$TAG"; fi

# Upgrade or install Mixer helm chart into the cluster
helm upgrade --install "$RELEASE" deploy/helm_charts/mixer \
  --atomic \
  --debug \
  --timeout 10m \
  --force  \
  -f "deploy/helm_charts/envs/$ENV.yaml" \
  --set mixer.image.tag="$TAG" \
  --set mixer.githash="$TAG" \
  --set-file mixer.schemaConfigs."base\.mcf"=deploy/mapping/base.mcf \
  --set-file mixer.schemaConfigs."encode\.mcf"=deploy/mapping/encode.mcf \
  --set-file kgStoreConfig.bigqueryVersion=deploy/storage/bigquery.version \
  --set-file kgStoreConfig.baseBigtableInfo=deploy/storage/base_bigtable_info.yaml

# Deploy Cloud Endpoints
cp $ROOT/esp/endpoints.yaml.tmpl endpoints.yaml
yq eval -i '.name = env(DOMAIN)' endpoints.yaml
yq eval -i '.title = env(API_TITLE)' endpoints.yaml
yq eval -i '.endpoints[0].target = env(IP)' endpoints.yaml
yq eval -i '.endpoints[0].name = env(DOMAIN)' endpoints.yaml
echo "endpoints.yaml content:"
cat endpoints.yaml

gsutil cp gs://datcom-mixer-grpc/mixer-grpc/mixer-grpc.$TAG.pb .
gcloud endpoints services deploy mixer-grpc.$TAG.pb endpoints.yaml --project $PROJECT_ID
