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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$(dirname "$DIR")"

while getopts "dt:" OPTION; do
  case $OPTION in
    d)
        echo -e "### Update golden files in docker mode"
        DOCKER=true
        ;;
    t)
        echo -e "### Update golden files for test: ${OPTARG}"
        TARGET=${OPTARG}
        ;;
    *)
        break ;;
    esac
done
shift $((OPTIND-1))

if [[ $TARGET != "" ]]; then
    ARG="-run $TARGET"
else
    ARG=""
fi

if [[ $DOCKER == "true" ]]; then
  DOCKER_BUILDKIT=1 docker build --tag datacommons/mixer-golden-update  -f build/Dockerfile --target golden-update .
  docker run \
    -v $HOME/.config/gcloud:/root/.config/gcloud \
    -v $ROOT/test/integration:/result \
    -e arg="$ARG" \
    datacommons/mixer-golden-update
else
    go test -v $ROOT/test/integration -generate_golden=true $ARG
fi