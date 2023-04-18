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

# This Dockerfile is used to build various container images. There are a few
# targets:
#
# go-protoc: image with Golang, protoc and protoc-go pluggin
# base: image with Go modules download and protobuf built
# server: image for mixer server
# test: image to run all the tests
# golden-update: image to update golden files

# =========  protoc stage ==========

FROM golang:1.20.2 AS go-protoc
WORKDIR /mixer
# Install protoc
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y unzip

RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.19.4/protoc-3.19.4-linux-x86_64.zip
RUN unzip protoc-3.19.4-linux-x86_64.zip
RUN chmod +x /mixer/bin/protoc
ENV PATH="${PATH}:/mixer/bin/"

# Install protobuf go plugin
ENV GO111MODULE=on
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.30.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0


# =========  base stage ==========
# Use pre-built go-protoc image from last step.

FROM gcr.io/datcom-ci/go-protoc:latest AS base
WORKDIR /mixer
# Docker cache: Download modules
COPY go.mod go.sum /mixer/
RUN go mod download -x

# Copy files
COPY proto/ proto
COPY internal/ internal
# Build protobuf
RUN protoc \
    --proto_path=proto \
    --go_out=paths=source_relative:internal/proto \
    --go-grpc_out=paths=source_relative:internal/proto \
    --go-grpc_opt=require_unimplemented_servers=false \
    --experimental_allow_proto3_optional \
    proto/*.proto proto/**/*.proto


# =========  server stage ==========

FROM base as server
WORKDIR /mixer
# Adding the grpc_health_probe
RUN GRPC_HEALTH_PROBE_VERSION=v0.4.7 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe
# Build binary
COPY cmd/ cmd
RUN go build -tags "sqlite_fts5" -o /go/bin/mixer cmd/main.go
ENTRYPOINT ["/go/bin/mixer"]


# =========  test stage ==========

FROM base as test
WORKDIR /mixer
# Golint
RUN wget -O- -nv https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.52.2
RUN ./bin/golangci-lint run --timeout=600s
# Test
COPY test/ test
COPY deploy/ deploy
CMD ["go", "test", "./..."]