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

PROJECT_ID=$(yq eval project config.yaml)
REGION=$(yq eval region config.yaml)
NODES=$(yq eval nodes config.yaml)

CLUSTER_NAME="mixer-$REGION"

gcloud config set project $PROJECT_ID

# Create VPC native-cluster to enable Ingress for Anthos and enable Workload Identity
gcloud container clusters create $CLUSTER_NAME \
  --num-nodes=$NODES \
  --region=$REGION \
  --machine-type=e2-highmem-4 \
  --enable-ip-alias \
  --workload-pool=$PROJECT_ID.svc.id.goog

# All resources will be in mixer
kubectl create namespace mixer

# Create service account which is mapped to the GCP service account for Workload Identity.
kubectl create serviceaccount --namespace mixer mixer-ksa

# Allow the Kubernetes service account to impersonate the Google service account
gcloud iam service-accounts add-iam-policy-binding \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:$PROJECT_ID.svc.id.goog[mixer/mixer-ksa]" \
  mixer-robot@$PROJECT_ID.iam.gserviceaccount.com

# Annotate service account
kubectl annotate serviceaccount \
  --namespace mixer \
  mixer-ksa \
  iam.gke.io/gcp-service-account=mixer-robot@$PROJECT_ID.iam.gserviceaccount.com