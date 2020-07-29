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


FROM golang:1.13

# Install protoc
RUN apt-get update && apt-get upgrade -y
RUN apt-get install protobuf-compiler -y
# Install protobuf go plugin
RUN go get -u google.golang.org/protobuf/cmd/protoc-gen-go
RUN go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc

# Copy the source from the current directory the working directory, excluding
# the deployment directory.
WORKDIR /mixer
COPY . .
RUN rm -r deployment

# Only download the two files. Can `git clone` entire library if needed.
RUN mkdir -p /mixer/proto/google/api/
RUN curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto \
         --output /mixer/proto/google/api/annotations.proto
RUN curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto \
         --output /mixer/proto/google/api/http.proto
RUN protoc \
    --proto_path=proto \
    --include_source_info \
    # --descriptor_set_out deployment/out.pb \
    --go_out=. \
    --go-grpc_out=. \
    --go-grpc_opt=requireUnimplementedServers=false \
    proto/mixer.proto


# Test.
ENV CGO_ENABLED 0
# RUN go test ./...

# Install the Go app.
RUN go install .

ENTRYPOINT ["/go/bin/mixer"]