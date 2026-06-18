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
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ResourcePath struct {
	Context    string
	AgencyID   string
	ResourceID string
	Version    string
	Key        string
}

type AvailabilityPath struct {
	ResourcePath
	ComponentID string
}

func parseResourcePath(tail string) (ResourcePath, error) {
	tail = strings.Trim(tail, "/")
	if tail == "" {
		return ResourcePath{}, nil
	}

	parts := strings.Split(tail, "/")
	if len(parts) != 5 {
		return ResourcePath{}, status.Error(codes.InvalidArgument, "invalid SDMX data path")
	}
	for _, part := range parts {
		if part == "" {
			return ResourcePath{}, status.Error(codes.InvalidArgument, "invalid SDMX data path")
		}
	}
	if parts[4] != "*" {
		return ResourcePath{}, status.Error(codes.Unimplemented, "SDMX data keys other than * are not implemented yet")
	}
	return ResourcePath{
		Context:    parts[0],
		AgencyID:   parts[1],
		ResourceID: parts[2],
		Version:    parts[3],
		Key:        parts[4],
	}, nil
}

func parseAvailabilityPath(tail string) (AvailabilityPath, error) {
	tail = strings.Trim(tail, "/")
	if tail == "" {
		return AvailabilityPath{}, nil
	}

	parts := strings.Split(tail, "/")
	if len(parts) != 6 {
		return AvailabilityPath{}, status.Error(codes.InvalidArgument, "invalid SDMX availability path")
	}
	for _, part := range parts {
		if part == "" {
			return AvailabilityPath{}, status.Error(codes.InvalidArgument, "invalid SDMX availability path")
		}
	}
	if parts[4] != "*" {
		return AvailabilityPath{}, status.Error(codes.Unimplemented, "SDMX availability keys other than * are not implemented yet")
	}
	return AvailabilityPath{
		ResourcePath: ResourcePath{
			Context:    parts[0],
			AgencyID:   parts[1],
			ResourceID: parts[2],
			Version:    parts[3],
			Key:        parts[4],
		},
		ComponentID: parts[5],
	}, nil
}
