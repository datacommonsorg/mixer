#!/bin/bash
# Copyright 2022 Google LLC
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

while getopts "t:" OPTION; do
  case $OPTION in
    t)
        echo -e "### Run latency test for: ${OPTARG}"
        TARGET=${OPTARG}
        ;;
    *)
        break ;;
    esac
done
shift $((OPTIND-1))

if [[ $TARGET != "" ]]; then
    ARG="-run ^$TARGET$"
else
    ARG=""
fi

export LATENCY_TEST=true
go clean -testcache
go test -parallel 1 ./... $ARG

cd test/e2e/latency
> latency
rm ../latency.csv
echo "method,total-latency" > latency
cat ./*.csv >> latency
mv latency ../latency.csv
