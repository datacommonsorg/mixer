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
# This retrives the docker images and gRPC descriptor based on git commit hash,
# so these binaries should have been pushed to container registry and Cloud
# Storage.

set -e

ENV=$1

TAG=$(git rev-parse --short HEAD)
if [[ $2 != "" ]]; then
  TAG=$2
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$(dirname "$DIR")"

PROJECT_ID=$(yq read deploy/gke/$ENV.yaml project)
REGION=$(yq read deploy/gke/$ENV.yaml region)
IP=$(yq read deploy/gke/$ENV.yaml ip)
DOMAIN=$(yq read deploy/gke/$ENV.yaml domain)
API_TITLE=$(yq read deploy/gke/$ENV.yaml api_title)
CLUSTER_NAME=mixer-$REGION

cd $ROOT/deploy/overlays/$ENV

# Deploy to GKE
kustomize edit set image gcr.io/datcom-ci/datacommons-mixer=gcr.io/datcom-ci/datacommons-mixer:$TAG
kustomize build > $ENV.yaml
gcloud config set project $PROJECT_ID
gcloud container clusters get-credentials $CLUSTER_NAME --region $REGION
kubectl apply -f $ENV.yaml

# Deploy Cloud Endpoints
yq w --style=double $ROOT/esp/endpoints.yaml.tmpl name $DOMAIN > endpoints.yaml
yq w -i endpoints.yaml title "$API_TITLE"
yq w -i endpoints.yaml endpoints[0].target "$IP"
yq w -i endpoints.yaml endpoints[0].name "$DOMAIN"
gsutil cp gs://artifacts.datcom-ci.appspot.com/mixer-grpc/mixer-grpc.$TAG.pb .
gcloud endpoints services deploy mixer-grpc.$TAG.pb endpoints.yaml --project $PROJECT_ID
