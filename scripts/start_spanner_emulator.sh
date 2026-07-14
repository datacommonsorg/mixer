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
  echo "Usage: $0"
  echo "Starts the Spanner emulator in the foreground on ports 9010 and 9020."
}

if [[ $# -gt 0 ]]; then
  if [[ $1 == "--help" && $# -eq 1 ]]; then
    usage
    exit 0
  fi
  usage >&2
  exit 1
fi

case "$(uname -s)" in
  Linux)
    if ! command -v gcloud >/dev/null 2>&1; then
      echo "Error: gcloud is required to start the Spanner emulator on Linux." >&2
      echo "Install the cloud-spanner-emulator gcloud component first." >&2
      exit 1
    fi
    exec gcloud emulators spanner start \
      --host-port=localhost:9010 \
      --rest-port=9020
    ;;
  Darwin)
    if ! command -v docker >/dev/null 2>&1; then
      echo "Error: Docker is required to start the Spanner emulator on macOS." >&2
      exit 1
    fi
    if ! docker info >/dev/null 2>&1; then
      echo "Error: Docker is installed but its daemon is not running." >&2
      exit 1
    fi
    exec docker run --rm \
      -p 9010:9010 \
      -p 9020:9020 \
      gcr.io/cloud-spanner-emulator/emulator
    ;;
  *)
    echo "Error: unsupported operating system: $(uname -s)." >&2
    exit 1
    ;;
esac
