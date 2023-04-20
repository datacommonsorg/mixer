#!/bin/bash
# Copyright 2019 Google LLC
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


# This script is used to run all the mixer test.
# `./run_test.sh` runs the tests with local Golang enviroment.
# `./run_test.sh -d` runs the test using Docker.

set -e

while true; do
  case "$1" in
    -d | --docker ) DOCKER=true; shift ;;
    * ) break ;;
  esac
done

if [[ $DOCKER == "true" ]]; then
  DOCKER_BUILDKIT=1 docker build --tag datacommons/mixer-test  -f build/Dockerfile --target test .
  docker run \
    -v $HOME/.config/gcloud:/root/.config/gcloud \
    datacommons/mixer-test
else
  go test -tags "sqlite_fts5" ./...
fi
