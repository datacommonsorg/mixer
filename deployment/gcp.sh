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


PROJECT_ID=$1
SERVICE_ACCOUNT=mixer-robot@$PROJECT_ID.iam.gserviceaccount.com

# Gcloud setup
if [[ $(gcloud config configurations list --filter="name:dc-mixer" --format=yaml) ]]; then
    echo "dc-mixer config exists, continue..."
else
    gcloud config configurations create $PROJECT_ID
fi
gcloud auth login
gcloud config set project $PROJECT_ID
gcloud config set compute/zone us-central1


# Create service account
if [[ $(gcloud iam service-accounts list --filter="name:mixer-robot" --format=yaml) ]]; then
    echo "service account mixer-robot exists, continue..."
else
    gcloud beta iam service-accounts create mixer-robot \
        --description "service account for mixer" \
        --display-name "mixer-robot"
fi


# Enable service account
gcloud alpha iam service-accounts enable $SERVICE_ACCOUNT


# Service account policy
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member serviceAccount:$SERVICE_ACCOUNT \
  --role roles/bigtable.reader

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member serviceAccount:$SERVICE_ACCOUNT \
  --role roles/bigquery.admin