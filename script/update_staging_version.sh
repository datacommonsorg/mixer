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

#!/bin/bash
set -e

VERSION=$1

# Clone datcom-ci/deployment repo
gcloud source repos clone deployment /tmp/deployment --project=datcom-ci

# Enter repo
cd /tmp/deployment

# Configure Git to create commits with Cloud Build's service account
git config user.email $(gcloud auth list --filter=status:ACTIVE --format='value(account)')

# Update version file
git checkout master

echo $VERSION > /tmp/deployment/mixer/staging/version.txt
echo gs://artifacts.datcom-ci.appspot.com/mixer-grpc/mixer-grpc.$SHORT_SHA.pb > /tmp/deployment/mixer/staging/grpc.txt
echo gcr.io/datcom-ci/datacommons-mixer:$SHORT_SHA > /tmp/deployment/mixer/staging/docker.txt
cp /workspace/deployment/bigquery.txt /tmp/deployment/mixer/staging/bigquery.txt
cp /workspace/deployment/bigtable.txt /tmp/deployment/mixer/staging/bigtable.txt

# Commit the version file
git add /tmp/deployment/mixer/staging/*
git commit -m "Update staging mixer versions at commit https://github.com/datacommonsorg/mixer/commit/$VERSION"
git push origin master