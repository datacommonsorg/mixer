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
go get google.golang.org/protobuf/cmd/protoc-gen-go@v1.23.0
go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@v0.0.0-20200824180931-410880dd7d91
mkdir -p proto/google/api/
curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto \
      --output proto/google/api/annotations.proto
curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto \
      --output proto/google/api/http.proto
