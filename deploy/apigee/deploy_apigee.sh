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
  echo "Missing arg 1 (env)"
  exit 1
fi

ENV_DATA="envs/$ENV.yaml"
if [[ ! -f $ENV_DATA ]]; then
  echo "Env config file ${ENV_DATA} not found"
  exit 1
fi

ENV_VARS="${ENV}.env"
if [[ ! -f $ENV_VARS ]]; then
  echo "Env var file ${ENV_VARS} not found. Proceeding without vars."
else
  source $ENV_VARS
fi

ENV_BASE_DIR="terraform/$ENV/.tmp"

function prep_env() {
  rm -rf $ENV_BASE_DIR
  proxy_names=($(yq eval '.proxies[].name' $ENV_DATA))
  for proxy_name in "${proxy_names[@]}"; do
    copy_file $proxy_name "" $proxy_name
    copy_resources $proxy_name "policies"
    copy_resources $proxy_name "proxy_endpoints"
    copy_resources $proxy_name "target_endpoints"
    zip -r "$ENV_BASE_DIR/$proxy_name.zip" "$ENV_BASE_DIR/$proxy_name"
  done
}

function copy_resources() {
  proxy_name="$1"
  resource_name="$2"
  resources=($(yq eval ".proxies[] | select(.name == \"$proxy_name\") | .$resource_name[]" $ENV_DATA))
  for resource in "${resources[@]}"; do
    copy_file $proxy_name $resource_name $resource
  done
}

function copy_file() {
  proxy_name="$1"
  dir="$2"
  file="$3"
  if [[ $dir == "" ]]; then
    source_dir="proxies"
    dest_dir="$ENV_BASE_DIR/$proxy_name"
  else
    source_dir="$dir"
    dest_dir="$ENV_BASE_DIR/$proxy_name/$dir"
  fi
  mkdir -p "$dest_dir"
  if [[ -f "$source_dir/$file.xml" ]]; then
    cp "$source_dir/$file.xml" "$dest_dir/$file.xml"
  elif [[ -f "$source_dir/$file.template.xml" ]]; then
    untemplated="$dest_dir/$file.xml"
    cp "$source_dir/$file.template.xml" "$untemplated"
    template_vars=($(grep -oE "REPLACE_WITH_[A-Z_]+" "$untemplated"))
    for template_var in "${template_vars[@]}"; do
      var_name=${template_var/"REPLACE_WITH_"/}
      var_value=${!var_name}
      if [[ "$var_value" == "" ]]; then
        echo "No value set for ${var_name}. Edit ${ENV_DATA} and re-run."
        exit 1
      fi
      sed -i "" "s/REPLACE_WITH_${var_name}/${!var_name}/g" "$untemplated"
    done
  else
    echo "Not found: $source_dir/$file.xml"
  fi
}

prep_env
