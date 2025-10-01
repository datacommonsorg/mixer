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

set -eo pipefail

CHECK_LIVE=false
if [[ "$#" -gt 0 ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "--live" ]]; then
      CHECK_LIVE=true
      break
    fi
  done
fi

if [[ "$CHECK_LIVE" == "false" ]]; then
  echo "HEAD: Checking flags against mixer server at current HEAD."
else
  echo "LIVE: Checking flags against mixer server at the current live commit for each env."
fi

ARGS=()
for arg in "$@"; do
  if [[ "$arg" != "--live" ]]; then
    ARGS+=("$arg")
  fi
done

# If less than 1 or more than two non-"--live" args are set, fail.
if [[ "${#ARGS[@]}" -lt 1 || "${#ARGS[@]}" -gt 2 ]]; then
  echo "Usage: $0 <config_dir> [env] [commit_hash]"
  echo "  <config_dir>: Directory containing the feature flag YAML files."
  echo "  [env]: Optional. The specific environment to check (e.g., 'dev')."
  echo "  [--live]: Optional. Whether to validate against the the commit that is deployed for each env."
  exit 1
fi

# Convert config dir to an absolute path to handle directory changes.
if ! command -v realpath &> /dev/null; then
    # Basic realpath polyfill for systems that don't have it (like older macOS)
    realpath() {
        [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
    }
fi

CONFIG_DIR=$(realpath "${ARGS[0]}")
TARGET_ENV=${ARGS[1]}

TEMP_DIRS=()
cleanup() {
  if [[ ${#TEMP_DIRS[@]} -gt 0 ]]; then
    for dir in "${TEMP_DIRS[@]}"; do
        if [ -d "$dir" ]; then
            git worktree remove --force "$dir"
        fi
    done
  fi
}

check_dependencies() {
  for cmd in yq curl git; do
    if ! command -v "$cmd" &> /dev/null; then
      echo "Error: Required command '$cmd' not found. Please install it and ensure it's in your PATH."
      exit 1
    fi
  done
}

run_check_with_go() {
  local env_file=$1
  echo "Checking feature flag parsing for ${env_file}..."
  go run scripts/check_flags.go "$env_file"
}

check_env() {
  local env_file=$1
  local env_name
  env_name=$(basename "$env_file" .yaml)

  # If CHECK_LIVE is false, run check directly
  if [[ "$CHECK_LIVE" == "false" ]]; then
    run_check_with_go "$env_file"
    return 0
  fi

  # Else, check live commit.
  echo "Checking live commit for ${env_name}..."
  local live_url
  live_url=$(yq e '.liveUrl' "$env_file")
  if [[ -z "$live_url" || "$live_url" == "null" ]]; then
    echo "Warning: No liveUrl found for ${env_name}, skipping live commit check." >&2
    return 0
  fi

  local version_url="${live_url%/}/version"
  echo "Fetching live commit from ${version_url}"
  local commit_hash
  local version_output
  if ! version_output=$(curl -fsS "$version_url"); then
    echo "Error: Failed to fetch version info from ${version_url}" >&2
    return 1
  fi

  # Try to parse as JSON first.
  commit_hash=$(echo "$version_output" | yq e '.gitHash' 2>/dev/null || true)
  if [[ -z "$commit_hash" || "$commit_hash" == "null" ]]; then
    # Fallback to parsing as HTML.
    commit_hash=$(echo "$version_output" | grep -o 'mixer/commit/[a-f0-9]\{7,40\}' | sed 's|.*/||')
  fi

  if [[ -z "$commit_hash" ]]; then
    echo "Error: Could not determine live commit hash for ${env_name} from ${version_url}" >&2
    return 1
  fi

  # Check if commit exists locally; fail if not.
  if ! git cat-file -e "$commit_hash" &>/dev/null; then
    echo "Error: Live commit ${commit_hash} not found locally." >&2
    return 1
  fi

  echo "Checking out live commit for ${env_name}: ${commit_hash}"
  # Create a temporary directory for the worktree
  local TEMP_DIR
  TEMP_DIR=$(mktemp -d)
  TEMP_DIRS+=("$TEMP_DIR")
  git worktree add --detach "$TEMP_DIR" "$commit_hash"

  # Run the check in a subshell to isolate directory changes.
  (
    cd "$TEMP_DIR"
    run_check_with_go "$env_file"
  )
}

run_all_checks() {
  if [[ -n "$TARGET_ENV" ]]; then
    local file_to_check="${CONFIG_DIR}/${TARGET_ENV}.yaml"
    check_env "$file_to_check"
  else
    echo "Checking feature flag parsing for all envs in ${CONFIG_DIR}..."
    while IFS= read -r -d '' file; do
      check_env "$file"
    done < <(find "$CONFIG_DIR" -maxdepth 1 -name "*.yaml" -print0)
  fi
}

check_dependencies
trap cleanup EXIT
run_all_checks
