#!/bin/bash
# Copyright 2026 Google LLC
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

set -euo pipefail

usage() {
  echo "Usage: $0 [--generate-goldens]"
  echo "Runs the Spanner emulator test package against localhost."
}

generate_goldens=false
if [[ $# -gt 0 ]]; then
  case "$1" in
    --generate-goldens)
      generate_goldens=true
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
fi
if [[ $# -gt 1 ]]; then
  usage >&2
  exit 1
fi

export SPANNER_EMULATOR_HOST="${SPANNER_EMULATOR_HOST:-localhost:9010}"
case "$SPANNER_EMULATOR_HOST" in
  localhost:?* | 127.0.0.1:?* | "\[::1\]":?* | spanner-emulator:?*) ;;
  *)
    echo "Error: SPANNER_EMULATOR_HOST must be localhost, 127.0.0.1, [::1], or spanner-emulator with a port." >&2
    exit 1
    ;;
esac

export RUN_SPANNER_EMULATOR_TESTS=true
export GENERATE_GOLDEN="$generate_goldens"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(dirname "$script_dir")"
cd "$repo_root"

exec go test ./internal/server/spanner/golden/emulator -count=1 -v
