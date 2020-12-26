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

ARG mod
ENV MOD=${mod}

# Install protoc
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y protobuf-compiler

# Install protobuf go plugin
ENV GO111MODULE=on
RUN go get google.golang.org/protobuf/cmd/protoc-gen-go@v1.23.0
RUN go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@v0.0.0-20200824180931-410880dd7d91

# Adding the grpc_health_probe
RUN GRPC_HEALTH_PROBE_VERSION=v0.3.5 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

# Copy the source from the current directory the working directory, excluding
# the deployment directory.
WORKDIR /mixer

# Golint
RUN wget -O- -nv https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.27.0

# Docker cache: Download modules
COPY go.mod go.sum /mixer/
RUN go mod download

# Copy files
COPY proto/ proto
COPY pkg/ pkg
COPY cmd/ cmd
COPY test/ test
COPY deploy/ deploy

# Build protobuf
RUN protoc \
    --proto_path=proto \
    --go_out=pkg \
    --go-grpc_out=pkg \
    --go-grpc_opt=requireUnimplementedServers=false \
    proto/*.proto

# Perform the action: test; update golden; build binary

RUN if [[ mod == "test" ]] ; then \
        go test ./... ; \
    elif [[ mod == "update" ]]; then \
        go test -v ./test/e2e -generate_golden=true \
    else \
        go build -o /go/bin/mixer cmd/main.go \
    fi