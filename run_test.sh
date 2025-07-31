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


# This script is used to run all the mixer tests.

# `./run_test.sh` runs the tests with local Golang enviroment.
# `./run_test.sh -l` runs the linter.
# `./run_test.sh -f` runs the linter and fixes auto-fixable issues.

LINT=false
FIX=false
while getopts 'lf' c
do
  case $c in
    l) LINT=true ;;
    f) FIX=true ;;
    *) echo "Unknown option ${OPTARG}"
       exit 1
       ;;

  esac
done

set -e

if [ "$FIX" = true ] || [ "$LINT" = true ]; then
  if ! [ -x "$(command -v golangci-lint)" ]; then
    echo 'Error: golangci-lint is not installed.' >&2
    echo 'Install it from https://golangci-lint.run/usage/install/' >&2
    exit 1
  fi
  REQUIRED_VERSION="2.3.0"
  if ! golangci-lint --version | grep -q "$REQUIRED_VERSION"; then
    echo "Error: wrong golangci-lint version. Want $REQUIRED_VERSION" >&2
    echo "Found: $(golangci-lint --version)"
    exit 1
  fi
fi

if [ "$FIX" = true ]; then
  echo "Running golangci-lint with --fix..."
  golangci-lint run --fix
elif [ "$LINT" = true ]; then
  echo "Running golangci-lint..."
  golangci-lint run
else
  echo "Running tests..."
  go test -v ./...
fi
