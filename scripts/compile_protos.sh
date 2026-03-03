#!/bin/bash
#
# Copyright 2026 Google LLC
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


# Script to compile proto files.

# Usage: Run ./scripts/compile_protos.sh from repo root directory.


set -e

# Check if running from root
if [[ ! -d "proto" ]]; then
  echo "Error: 'proto' directory not found. Please run this script from the repository root."
  exit 1
fi

# Check for protoc
if ! command -v protoc &> /dev/null; then
  echo "Error: 'protoc' compiler not found. Please install Protocol Buffers compiler."
  exit 1
fi

# Check protoc version
required_version="3.21.12"
protoc_version=$(protoc --version | cut -d ' ' -f 2)

if [[ "$protoc_version" != "$required_version" ]]; then
  echo "Error: 'protoc' version must be exactly $required_version. Found $protoc_version."
  exit 1
fi

echo "Compiling proto files..."

protoc \
  --proto_path=proto \
  --go_out=paths=source_relative:internal/proto \
  --go-grpc_out=paths=source_relative:internal/proto \
  --go-grpc_opt=require_unimplemented_servers=false \
  --experimental_allow_proto3_optional \
  --include_imports \
  --include_source_info \
  --descriptor_set_out mixer-grpc.pb \
  proto/*.proto proto/**/*.proto

echo "Compilation successful!"