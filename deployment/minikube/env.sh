#!/bin/bash
set -a

# Cloud Endpoints service name
email=$(git config user.email)
SERVICE_NAME="${email/@/.}.endpoints.datcom-mixer-staging.cloud.goog"


# ESP resources
ESP_MEM_REQ="0.5G"
ESP_CPU_REQ="500m"
ESP_MEM_LIMIT="1G"
ESP_CPU_LIMIT="1000m"

# Mixer resources
MIXER_MEM_REQ="2G"
MIXER_CPU_REQ="500m"
MIXER_MEM_LIMIT="2G"
MIXER_CPU_LIMIT="1000m"

# Pod replica
REPLICAS="1"

# Mixer container
MIXER_IMAGE="mixer:local"
IMAGE_PULL_POLICY="Never"

# Mixer arguments
BQ_DATASET="$(head -1 ../bigquery.txt)"
BT_TABLE="$(head -1 ../bigtable.txt)"
BT_PROJECT="google.com:datcom-store-dev"
BT_INSTANCE="prophet-cache"
PROJECT_ID="datcom-mixer-staging"
BRANCH_FOLDER="dummy"
