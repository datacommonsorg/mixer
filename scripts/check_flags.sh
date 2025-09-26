#!/bin/bash
# Copyright 2025 Google LLC
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

set -e

# Remove trailing slash from CONFIG_DIR if it exists
CONFIG_DIR="${1%/}"

if [[ "$#" -lt 1 || "$#" -gt 1 ]]; then
  echo "Usage: $0 <config_dir>"
  echo "  <config_dir>: Directory containing the feature flag YAML files."
  exit 1
fi

echo "Checking feature flag parsing..."
for f in "${CONFIG_DIR}"/*.yaml; do
    go run scripts/check_flags.go "$f"
done
