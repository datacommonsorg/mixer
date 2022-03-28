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


# Creates a container that generates gRPC json config that can be used directly
# by local ESP container. These two containers share the same volume that holds
# the mixer-grpc.json.

FROM golang:1.15 AS base

RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    apt-get install -y git

# Install protobuf go plugin
ENV GO111MODULE=on
RUN go get google.golang.org/protobuf/cmd/protoc-gen-go@v1.23.0
RUN go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@v0.0.0-20200824180931-410880dd7d91

WORKDIR /workspace

RUN git clone https://github.com/googleapis/api-compiler

WORKDIR /workspace/api-compiler

RUN git submodule update --recursive --init

# Build protobuf
COPY proto/ proto
RUN protoc \
    --proto_path=proto \
    --include_source_info \
    --include_imports \
    --descriptor_set_out mixer-grpc.pb \
    proto/*.proto proto/v1/*.proto


FROM openjdk:8-slim

COPY --from=base /workspace/api-compiler /workspace/api-compiler
WORKDIR /workspace/api-compiler

COPY esp/endpoints-dev.yaml .
RUN mkdir /output
RUN ./run.sh \
    --configs endpoints-dev.yaml \
    --descriptor mixer-grpc.pb \
    --json_out /output/mixer-grpc.json

CMD [ "/bin/sh" ]