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

#!/bin/bash
set -e

while true; do
  case "$1" in
    -d | --docker ) DOCKER=true; shift ;;
    * ) break ;;
  esac
done

if [[ $DOCKER == "true" ]]; then
  docker build --tag datacommons/mixer  -f build/Dockerfile .
  echo "Start to run tests ..."
  docker run \
    -v $HOME/.config/gcloud:/root/.config/gcloud \
    datacommons/mixer \
    sh -c "go test ./..."
else
  go test ./...
fi

