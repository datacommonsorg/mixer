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

set -e

ENV=$1

if [[ $ENV == "" ]]; then
  echo "Missing arg 1 (env). Possible values are prod or nonprod."
  exit 1
fi

ENV_DATA="envs/$ENV.yaml"
if [[ ! -f $ENV_DATA ]]; then
  echo "Env config file ${ENV_DATA} not found"
  exit 1
fi

ENV_VARS="${ENV}.env"
if [[ ! -f $ENV_VARS ]]; then
  ./sync_env.sh $ENV
fi
source "$ENV_VARS"

ENV_BASE_DIR="terraform/$ENV"
ENV_TMP_DIR="$ENV_BASE_DIR/.tmp"
WORKING_DIR=$(pwd)

# Copies API proxy files to the expected structure in a temp directory for the
# chosen environment. Follows the env config yaml to decide which files to copy.
# Substitutes environment variables for REPLACE_WITH_ clauses in the copies.
function prep_proxies() {
  rm -rf "$ENV_TMP_DIR"
  proxy_names=($(yq eval '.proxies[].name' "$ENV_DATA"))
  for proxy_name in "${proxy_names[@]}"; do
    copy_file "$proxy_name" "proxies" "" "$proxy_name"
    copy_resources "$proxy_name" "policies" "policies"
    copy_resources "$proxy_name" "proxy_endpoints" "proxies"
    copy_resources "$proxy_name" "target_endpoints" "targets"
    cd "$ENV_TMP_DIR"
    mv "$proxy_name" apiproxy
    # Set a constant modification timestamp on all files so zip archive hash
    # won't change due to timestamps alone.
    find apiproxy -exec touch -t 202408270000 {} +
    zip -rX "$proxy_name.zip" "apiproxy/"
    mv apiproxy "$proxy_name"
    cd "$WORKING_DIR"
  done
}

# Copies all files listed in config yaml for the given proxy and
# resource source directory.
function copy_resources() {
  proxy_name="$1"
  source_dir="$2"
  dest_dir="$3"
  resources=($(yq eval ".proxies[] | select(.name == \"$proxy_name\") | .$source_dir[]" "$ENV_DATA"))
  for resource in "${resources[@]}"; do
    copy_file "$proxy_name" "$source_dir" "$dest_dir" "$resource"
  done
}

# Copies a single file for the given proxy. Looks in the given source dir for a
# file with the given source file name. If the file name has the format
# *.template.xml, substitutes environment variables for REPLACE_WITH_ clauses.
function copy_file() {
  proxy_name="$1"
  source_dir="$2"
  dest_dir="$3"
  source_file="$4"
  if [[ $dest_dir == "" ]]; then
    write_dir="$ENV_TMP_DIR/$proxy_name"
  else
    write_dir="$ENV_TMP_DIR/$proxy_name/$dest_dir"
  fi
  mkdir -p "$write_dir"
  if [[ -f "$source_dir/$source_file.xml" ]]; then
    cp "$source_dir/$source_file.xml" "$write_dir/$source_file.xml"
  elif [[ -f "$source_dir/$source_file.template.xml" ]]; then
    write_file="$write_dir/$source_file.xml"
    cp "$source_dir/$source_file.template.xml" "$write_file"
    template_vars=($(grep -oE "REPLACE_WITH_[A-Z_]+" "$write_file"))
    for template_var in "${template_vars[@]}"; do
      var_name=${template_var/"REPLACE_WITH_"/}
      var_value=${!var_name}
      if [[ "$var_value" == "" ]]; then
        echo "No value set for ${var_name}. Edit ${ENV_DATA} and re-run."
        exit 1
      fi
      sed -i "" "s/REPLACE_WITH_${var_name}/${!var_name}/g" "$write_file"
    done
  else
    echo "Not found: $source_dir/$source_file.xml"
  fi
}

function terraform_plan_and_maybe_apply() {
  cd "$ENV_BASE_DIR"

  terraform_cmd "plan"

  while true; do
    read -p "Proceed to terraform apply with auto-approve? " yn
    case $yn in
    [Yy]*)
      terraform_cmd "apply --auto-approve"
      cd "$WORKING_DIR"
      ./sync_env.sh "$ENV" --push
      break
      ;;
    [Nn]*)
      cd "$WORKING_DIR"
      exit
      ;;
    *) echo "Please answer yes or no." ;;
    esac
  done
}

# Runs the given Terraform verb with an access token and vars file.
function terraform_cmd() {
  verb=$1
  # shellcheck disable=SC2086
  terraform $verb \
    --var="access_token=$(gcloud auth print-access-token)" \
    -var-file=vars.tfvars
}

prep_proxies
terraform_plan_and_maybe_apply
