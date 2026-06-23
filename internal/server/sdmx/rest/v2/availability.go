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
	"net/url"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AvailabilityRequest struct {
	Path        AvailabilityPath
	Constraints map[string][]string
}

func ParseAvailabilityRequest(tail string, originalURI string) (*AvailabilityRequest, error) {
	parsedURI, err := url.ParseRequestURI(originalURI)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid SDMX request URI")
	}

	path, err := parseAvailabilityPath(tail)
	if err != nil {
		return nil, err
	}
	params, err := parseRawQuery(parsedURI.RawQuery)
	if err != nil {
		return nil, err
	}

	constraints := map[string][]string{}
	for _, param := range params {
		ok, err := parseComponentFilter(param, constraints)
		if err != nil {
			return nil, err
		}
		if ok {
			continue
		}
		if err := validateAvailabilityQueryParam(param); err != nil {
			return nil, err
		}
	}

	if err := validateAvailabilityRequest(path, constraints); err != nil {
		return nil, err
	}

	return &AvailabilityRequest{Path: path, Constraints: constraints}, nil
}

func validateAvailabilityQueryParam(param queryParam) error {
	switch param.Name {
	case "mode":
		if param.Value != "exact" {
			return status.Errorf(codes.Unimplemented, "SDMX availability mode %q is not implemented yet", param.Value)
		}
	case "references":
		if param.Value != "none" {
			return status.Errorf(codes.Unimplemented, "SDMX availability references %q are not implemented yet", param.Value)
		}
	case "updatedAfter", "reportingYearStartDay":
		return status.Errorf(codes.Unimplemented, "SDMX availability query parameter %q is not implemented yet", param.Name)
	}
	return nil
}
