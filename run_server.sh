#!/bin/bash

# Copyright 2025 Google LLC
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

# This script provides a convenient way to run the Mixer locally for development.
#
# It simplifies the process of starting the Mixer server with a default
# configuration and a cleaner, less-dense log output suitable for local
# development.
#
# Flags:
#   --json-log: Use structured JSON logs instead of the default human-readable format.
#
# Example Usage:
#   # Run with cleaner, human-readable logs (default)
#   ./run_server.sh
#
#   # Run with structured JSON logs
#   ./run_server.sh --json-log
#
#   # Add more mixer flags
#   ./run_server.sh --use_sqlite=true --sqlite_path=$PWD/test/datacommons.db

set -e

# Default to local-friendly logs
export MIXER_LOCAL_LOGS=true

# Process arguments
args=()
for arg in "$@"; do
  if [[ "$arg" == "--json-log" ]]; then
    export MIXER_LOCAL_LOGS=false
  else
    args+=("$arg")
  fi
done

CMD=("go" "run" "cmd/main.go"
    "--host_project=datcom-mixer-dev-316822"
    "--bq_dataset=$(head -1 deploy/storage/bigquery.version)"
    "--base_bigtable_info=$(cat deploy/storage/base_bigtable_info.yaml)"
    "--schema_path=$PWD/deploy/mapping/"
    "--use_base_bigtable=true"
    "--use_branch_bigtable=false"
    "--feature_flags_path=deploy/featureflags/local.yaml")

if [ ${#args[@]} -ne 0 ]; then
  CMD+=("${args[@]}")
fi

"${CMD[@]}"
