#!/bin/bash
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

export PROJECT_ID=$(yq eval '.project' config.yaml)
export DOMAIN=$(yq eval '.domain' config.yaml)
export API_TITLE=$(yq eval '.api_title' config.yaml)
export IP=$(yq eval '.ip' config.yaml)
export API=$(yq eval '.api' config.yaml)

if [[ $API_TITLE == '' ]]; then
  API_TITLE=$DOMAIN
fi

# ESP service configuration
cp ../esp/endpoints.yaml.tmpl endpoints.yaml
yq eval -i '.apis[0].name = env(API)' endpoints.yaml
yq eval -i '.name = env(DOMAIN)' endpoints.yaml
yq eval -i '.title = env(API_TITLE)' endpoints.yaml
yq eval -i '.endpoints[0].target = env(IP)' endpoints.yaml
yq eval -i '.endpoints[0].name = env(DOMAIN)' endpoints.yaml

## Deploy ESP configuration
gsutil cp gs://datcom-mixer-grpc/mixer-grpc/mixer-grpc.latest.pb .
gcloud endpoints services deploy mixer-grpc.latest.pb endpoints.yaml --project $PROJECT_ID
gcloud services enable $DOMAIN
