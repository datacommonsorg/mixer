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

PROJECT_ID=$(yq r config.yaml project)
DOMAIN=$(yq r config.yaml domain)
API_TITLE=$(yq r config.yaml api_title)
IP=$(yq r config.yaml ip)

if [[ $API_TITLE == '' ]]; then
  API_TITLE=$DOMAIN
fi

# ESP service configuration
yq w --style=double ../esp/endpoints.yaml.tmpl name $DOMAIN > endpoints.yaml
yq w -i endpoints.yaml title "$API_TITLE"
yq w -i endpoints.yaml endpoints[0].target "$IP"
yq w -i endpoints.yaml endpoints[0].name "$DOMAIN"

## Deploy ESP configuration
gsutil cp gs://datcom-mixer-grpc/mixer-grpc/mixer-grpc.latest.pb .
gcloud endpoints services deploy mixer-grpc.latest.pb endpoints.yaml --project $PROJECT_ID
gcloud services enable $DOMAIN
