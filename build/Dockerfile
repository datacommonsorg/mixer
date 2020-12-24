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


FROM golang:1.15

# Install protoc
RUN apt-get update && apt-get upgrade -y
RUN apt-get install protobuf-compiler -y

# Adding the grpc_health_probe
RUN GRPC_HEALTH_PROBE_VERSION=v0.3.5 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

# Only download the two files. Can `git clone` entire library if needed.
RUN mkdir -p /mixer/proto/google/api/
RUN curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto \
         --output /mixer/proto/google/api/annotations.proto
RUN curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto \
         --output /mixer/proto/google/api/http.proto

# Copy the source from the current directory the working directory, excluding
# the deployment directory.
WORKDIR /mixer

# Docker cache: Download modules
COPY go.mod go.sum /mixer/
RUN go mod download

# Install protobuf go plugin
RUN go get google.golang.org/protobuf/cmd/protoc-gen-go@v1.23.0
RUN go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@v0.0.0-20200824180931-410880dd7d91

# Build protobuf
COPY proto/*.proto /mixer/proto/

RUN protoc \
    --proto_path=proto \
    --go_out=. \
    --go-grpc_out=. \
    --go-grpc_opt=requireUnimplementedServers=false \
    proto/*.proto

# TODO(shifucun): re-structure the folders, put app code in one directory and
# deploy, ci codes in other top level folders
COPY base/ /mixer/base
COPY healthcheck/ /mixer/healthcheck
COPY server/ /mixer/server
COPY sparql/ /mixer/sparql
COPY translator/ /mixer/translator
COPY util/ /mixer/util
COPY main.go /mixer

# Install the Go app.
ARG SKAFFOLD_GO_GCFLAGS
RUN go build -gcflags="${SKAFFOLD_GO_GCFLAGS}"
RUN go install .

ENTRYPOINT ["/go/bin/mixer"]