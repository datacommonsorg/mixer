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
REGION=$(yq r config.yaml region)
NODES=$(yq r config.yaml nodes)

CLUSTER_NAME="mixer-$REGION"

gcloud config set project $PROJECT_ID

gcloud container clusters create $CLUSTER_NAME \
  --num-nodes=$NODES \
  --region=$REGION \
  --machine-type=e2-highmem-4 \
  --enable-ip-alias # VPC native-cluster to enable Ingress for Anthos