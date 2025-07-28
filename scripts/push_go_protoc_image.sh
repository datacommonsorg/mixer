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

# Build Docker image on GCP and push to Cloud Container Registry
# Usage:
# ./push_go_protoc_image.sh [tag_mode] [project_id]
#
# tag_mode: "latest" to tag as latest, otherwise uses commit hash.
# project_id: GCP project to build in. Defaults to datcom-ci.

set -e

TAG_MODE=$1
PROJECT_ID=$2

if [[ $PROJECT_ID == "" ]]; then
  PROJECT_ID=datcom-ci
fi

TAG=$(git rev-parse --short=7 HEAD)
if [[ $TAG_MODE == "latest" ]]; then
  TAG="latest"
fi

# Get the current directory path and the root path of the repo
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$(dirname "$DIR")"

cd $ROOT

gcloud builds submit . \
  --async \
  --project=$PROJECT_ID \
  --config=build/ci/cloudbuild.goprotoc.yaml \
  --substitutions=_TAG=$TAG,_PROJECT_ID=$PROJECT_ID
