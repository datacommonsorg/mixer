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


export PROJECT_ID=$1
export IMAGE=$2
export DOMAIN=$3

SERVICE_ACCOUNT=mixer-robot@$PROJECT_ID.iam.gserviceaccount.com
if [ "$PROJECT_ID" == "datcom-mixer" ]; then
  echo "Set datacommons domain"
  DOMAIN="api.datacommons.org"
fi


cp template_deployment.yaml deployment.yaml
cp template_api_config.yaml api_config.yaml

# Set project id
perl -i -pe's/PROJECT_ID/$ENV{PROJECT_ID}/g' deployment.yaml api_config.yaml

# Set api title
if [ "$PROJECT_ID" == "datcom-mixer" ]; then
  perl -i -pe's/TITLE/Data Commons API/g' api_config.yaml
else
  perl -i -pe's/TITLE/Data Commons API ($ENV{PROJECT_ID})/g' api_config.yaml
fi

# Set docker image
perl -i -pe's/IMAGE/$ENV{IMAGE}/g' deployment.yaml

# Set endpints domain
if [[ $DOMAIN ]]; then
  perl -i -pe's/#_c\|//g' deployment.yaml
  perl -i -pe's/DOMAIN/$ENV{DOMAIN}/g' deployment.yaml
else
  perl -i -pe's/#_d\|//g' deployment.yaml
fi


# Get a static ip address
if ! [[ $(gcloud compute addresses list --global --filter='name:mixer-ip' --format=yaml) ]]; then
  gcloud compute addresses create mixer-ip --global
fi
ip=$(gcloud compute addresses list --global --filter='name:mixer-ip' --format='value(ADDRESS)')


# Deploy endpoints
perl -i -pe's/IP_ADDRESS/'"$ip"'/g' api_config.yaml

if [[ $DOMAIN ]]; then
  perl -i -pe's/#_c\|//g' api_config.yaml
  perl -i -pe's/DOMAIN/$ENV{DOMAIN}/g' api_config.yaml
else
  perl -i -pe's/#_d\|//g' api_config.yaml
fi

gcloud endpoints services deploy out.pb api_config.yaml


# GKE setup
gcloud components install kubectl
gcloud services enable container.googleapis.com

# Create GKE instance
if [[ $(gcloud container clusters list --filter='mixer-cluster' --format=yaml) ]]; then
  echo "mixer-cluster already exists, continue..."
else
  gcloud container clusters create mixer-cluster --zone=us-central1-c
fi

gcloud container clusters get-credentials mixer-cluster

# Create namespace
kubectl create namespace mixer

if [[ $(kubectl get secret bigquery-key --namespace mixer -o yaml | grep 'key.json') ]]; then
  echo "The secret bigquery-key already exists..."
else
  echo "Creating new bigquery-key..."
  # Create service account key and mount secret
    key_ids=$(gcloud iam service-accounts keys list --iam-account "$SERVICE_ACCOUNT" --managed-by=user --format="value(KEY_ID)")
    while read -r key_id; do
      if [[ $key_id ]]; then
        gcloud iam service-accounts keys delete $key_id --iam-account $SERVICE_ACCOUNT
      fi
    done <<< "$key_ids"
  gcloud iam service-accounts keys create key.json --iam-account $SERVICE_ACCOUNT

  # Mount secrete
  kubectl create secret generic bigquery-key --from-file=key.json=key.json --namespace=mixer
fi

# Mount nginx config
kubectl create configmap nginx-config --from-file=nginx.conf --namespace=mixer

# Mount schema mapping volumes
kubectl delete configmap schema-mapping --namespace mixer
kubectl create configmap schema-mapping --from-file=mapping/ --namespace=mixer

# Create certificate
if [ $DOMAIN ]; then
cat <<EOT > custom-certificate.yaml
apiVersion: networking.gke.io/v1beta1
kind: ManagedCertificate
metadata:
  name: custom-certificate
  namespace: mixer
spec:
  domains:
    - $DOMAIN
EOT
kubectl apply -f custom-certificate.yaml
else
cat <<EOT > certificate.yaml
apiVersion: networking.gke.io/v1beta1
kind: ManagedCertificate
metadata:
  name: mixer-certificate
  namespace: mixer
spec:
  domains:
    - datacommons.endpoints.$PROJECT_ID.cloud.goog
EOT
kubectl apply -f certificate.yaml
fi


# Bring up service and pods
kubectl apply -f service.yaml
kubectl apply -f deployment.yaml


# Bring ingress with certificate
if [ "$PROJECT_ID" == "datcom-mixer" ]; then
  perl -i -pe's/#__//g' ingress-ssl.yaml
fi
kubectl apply -f ingress-ssl.yaml
