#!/bin/bash
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

# Deploys the Google-built OTLP collector to the GKE cluster if it is not already deployed.
# This script can be run standalone, however,
# it expects that kubectl is already configured to point to the correct GKE cluster.

# The Google-built OTLP collector is documented here:
# https://cloud.google.com/stackdriver/docs/instrumentation/opentelemetry-collector-gke

set -e

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <PROJECT_ID> <PROJECT_NUMBER>" >&2
    exit 1
fi

# These two environment variables are expected for substitution in the
# Google-built collector config.
export GOOGLE_CLOUD_PROJECT=$1
export PROJECT_NUMBER=$2

# Check that kubectl context is set.
if ! kubectl config current-context > /dev/null 2>&1; then
  echo "Error: kubectl context is not set." >&2
  echo "Please run 'gcloud container clusters get-credentials' for your cluster." >&2
  exit 1
fi

# Check if collector is already deployed
if kubectl get deployment opentelemetry-collector --namespace opentelemetry > /dev/null 2>&1; then
  echo "Verified that the OTLP collector is already deployed."
  exit 0
fi

echo "OTLP collector deployment not found. Deploying the Google-built collector using kustomize..."
kubectl kustomize https://github.com/GoogleCloudPlatform/otlp-k8s-ingest.git/k8s/base \
| envsubst | kubectl apply -f -
echo "OTLP collector deployment applied."

echo "Updating permissions for OTLP collector service account..."
gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
    --role=roles/logging.logWriter \
    --member="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${GOOGLE_CLOUD_PROJECT}.svc.id.goog/subject/ns/opentelemetry/sa/opentelemetry-collector"
gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
    --role=roles/monitoring.metricWriter \
    --member="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${GOOGLE_CLOUD_PROJECT}.svc.id.goog/subject/ns/opentelemetry/sa/opentelemetry-collector"
gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
    --role=roles/cloudtrace.agent \
    --member="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${GOOGLE_CLOUD_PROJECT}.svc.id.goog/subject/ns/opentelemetry/sa/opentelemetry-collector"
echo "OTLP collector permissions updated."
