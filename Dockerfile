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

################################################################################
# First stage: the grpc container.
################################################################################

FROM grpc/go AS grpc
# Only download the two files. Can `git clone` entire library if needed.
RUN mkdir -p /proto_lib/google/api/
RUN curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto --output /proto_lib/google/api/annotations.proto
RUN curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto --output /proto_lib/google/api/http.proto

COPY ./proto/mixer.proto /proto_lib
WORKDIR /proto_lib
RUN protoc \
    --proto_path=/proto_lib \
    --include_source_info \
    --descriptor_set_out out.pb \
    --go_out=plugins=grpc:. mixer.proto

################################################################################
# Second stage: the golang container.
################################################################################

FROM golang:alpine AS builder
RUN apk add --no-cache ca-certificates git
WORKDIR /mixer

# Copy the source from the current directory the working directory, excluding
# the deployment directory.
COPY . .
RUN rm -r deployment

# Copy over protobufs.
COPY --from=grpc /proto_lib/out.pb ./
COPY --from=grpc /proto_lib/mixer.pb.go ./proto

# Test.
ENV CGO_ENABLED 0
RUN go test ./...

# Install the Go app.
RUN go install ./server

ENTRYPOINT ["/go/bin/server"]