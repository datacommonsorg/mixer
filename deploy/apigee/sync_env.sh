#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Usage: ./sync_env.sh <env> [--pull | --push]

# Pulls or pushes Apigee deployment environment variables from/to Google Secret Manager.
# If no command is given, prompts the user to choose pull or push.

set -e

ENV=$1
COMMAND=$2

if [[ $ENV == "" ]]; then
  echo "Missing arg 1 (env). Possible values are prod or nonprod."
  exit 1
fi

ENV_DATA="envs/$ENV.yaml"
if [[ ! -f $ENV_DATA ]]; then
  echo "Env config file ${ENV_DATA} not found."
  exit 1
fi

PROJECT_ID=$(yq eval '.project_id' "$ENV_DATA")
SECRET_NAME="${ENV}-env"
ENV_VARS="${ENV}.env"

# Downloads env vars from Secrets Manager to local file.
function pull_env() {
  gcloud secrets versions access latest --secret="$SECRET_NAME" \
    --project="$PROJECT_ID" >"$ENV_VARS"
}

# Downloads env vars from Secrets Manager to local file.
# Prompts the user to confirm before overwriting local file.
function confirm_and_pull_env() {
  while true; do
    read -p "Proceed to overwrite ${ENV_VARS} locally? " yn
    case $yn in
    [Yy]*)
      pull_env
      break
      ;;
    [Nn]*) exit ;;
    *) echo "Please answer yes or no." ;;
    esac
  done
}

# Uploads local env vars to Secrets Manager if local and cloud versions differ.
# Prompts the user to confirm before uploading.
function confirm_and_push_env() {
  cloud_latest=$(gcloud secrets versions access latest --secret="$SECRET_NAME" \
    --project="$PROJECT_ID")
  if [ "$cloud_latest" = "$(cat "$ENV_VARS")" ]; then
    echo "Skipping upload since local and cloud versions of ${ENV_VARS} are the same."
  else
    while true; do
      read -p "${ENV_VARS} has local changes. Upload to Secrets Manager? " yn
      case $yn in
      [Yy]*)
        gcloud secrets versions add $SECRET_NAME \
          --project=$PROJECT_ID --data-file=$ENV_VARS
        break
        ;;
      [Nn]*) exit ;;
      *) echo "Please answer yes or no." ;;
      esac
    done
  fi
}

if [ -n "$COMMAND" ]; then
  case $COMMAND in
  --pull)
    confirm_and_pull_env
    ;;
  --push)
    confirm_and_push_env
    ;;
  *)
    echo "Invalid sync direction. Options are --pull or --push."
    exit 1
    ;;
  esac

elif [[ ! -f $ENV_VARS ]]; then
  echo "Env var file ${ENV_VARS} not found. Pulling from Secrets Manager."
  pull_env

else
  while true; do
    echo "Which direction do you want to sync?"
    echo "[1] Pull (cloud -> local)"
    echo "[2] Push (local -> cloud)"
    read -p "Enter your choice (1 or 2): " choice
    case $choice in
    1)
      confirm_and_pull_env
      break
      ;;
    2)
      confirm_and_push_env
      break
      ;;
    *) echo "Please choose 1 or 2." ;;
    esac
  done

fi
