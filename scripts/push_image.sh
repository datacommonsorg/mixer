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

# This script builds and pushes the Mixer Docker image to GCR.
# It also generates and uploads the gRPC descriptor to GCS (gs://datcom-mixer-grpc).
# It is intended for manual or dev builds and avoids triggering the main CI/CD pipeline.
#
# Usage: ./scripts/push_image.sh [PROJECT_ID] [ENV]
#
# Arguments:
#   PROJECT_ID (Optional): The GCP project ID to push to. Defaults to "datcom-ci".
#   ENV        (Optional): Environment name. If set to "DEV", the tag will be prefixed with "dev-".
#
# Example: ./scripts/push_image.sh datcom-ci DEV
#
# Configuration:
#   Calls build/ci/cloudbuild.push_image.yaml

set -e

PROJECT_ID=$1
ENV=$2

if [[ $PROJECT_ID == "" ]]; then
  PROJECT_ID=datcom-ci
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$(dirname "$DIR")"

TAG=$(git rev-parse --short=7 HEAD)
if [[ $ENV == "DEV" ]]; then
  TAG="dev-$TAG"
fi

cd $ROOT
echo "Building and pushing mixer image to gcr.io/$PROJECT_ID/datacommons-mixer:$TAG"

gcloud builds submit . \
  --async \
  --project=$PROJECT_ID \
  --config=build/ci/cloudbuild.push_image.yaml \
  --substitutions=_TAG=$TAG,_PROJECT_ID=$PROJECT_ID
