// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package restv2

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	originalURIHeader = "x-dc-original-uri"
	envoyPathHeader   = "x-envoy-original-path"
)

// OriginalURIFromMetadata returns the trusted request target before transcoding.
func OriginalURIFromMetadata(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.InvalidArgument, "missing SDMX request URI")
	}

	for _, key := range []string{originalURIHeader, envoyPathHeader} {
		values := md.Get(key)
		if len(values) > 0 && values[0] != "" {
			return values[0], nil
		}
	}
	return "", status.Error(codes.InvalidArgument, "missing SDMX request URI")
}

// ValidateDataAccept rejects SDMX wire formats that are not implemented yet.
func ValidateDataAccept(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	for _, key := range []string{"accept", "grpcgateway-accept"} {
		for _, value := range md.Get(key) {
			lower := strings.ToLower(value)
			if strings.Contains(lower, "application/vnd.sdmx.data+json") ||
				strings.Contains(lower, "application/vnd.sdmx.data+csv") {
				return status.Error(codes.Unimplemented, "SDMX JSON and CSV responses are not implemented yet")
			}
		}
	}
	return nil
}
