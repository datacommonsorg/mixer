# Copyright 2023 Google LLC
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

# go-protoc: image with Golang, protoc, and proto + gRPC plugins for go

FROM golang:1.23.11-alpine3.22 AS go-protoc

# TODO: Sync the version of protoc recommended throughout our repos to match
# the one included with Alpine on the base image here.
ENV PROTOC_VERSION="29.4"
ENV PROTOC_GEN_GO_VERSION="1.30.0"
ENV PROTOC_GEN_GO_GRPC_VERSION="1.3.0"

WORKDIR /mixer

# Install protoc (protobuf compiler)
RUN apk update && \
    apk add --no-cache protobuf-dev
RUN INSTALLED_VERSION="$(protoc --version)" && \
    if [ "$INSTALLED_VERSION" != "libprotoc ${PROTOC_VERSION}" ]; then \
      echo "ERROR: Mismatched protoc version." && \
      echo "  Expected: libprotoc ${PROTOC_VERSION}" && \
      echo "  Found:    ${INSTALLED_VERSION}" && \
      exit 1; \
    fi

# Install protobuf go plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v${PROTOC_GEN_GO_VERSION}
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v${PROTOC_GEN_GO_GRPC_VERSION}
