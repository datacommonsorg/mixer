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


# Push the Docker image used for deployment to Container Registry

set -e

# Get the current directory path and the root path of the repo
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$(dirname "$DIR")"

cd "$ROOT"

TAG="latest"
if [[ $1 != "" ]]; then
  TAG=$1
fi
echo "Using tag=$TAG for the deploy-tool image."

gcloud builds submit ./deploy \
    --project=datcom-ci \
    --substitutions=_TAG="$TAG" \
    --config=deploy/cloudbuild.yaml
