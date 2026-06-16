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
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DataRequest struct {
	Path        ResourcePath
	Constraints map[string][]string
}

func ParseDataRequest(tail string, originalURI string) (*DataRequest, error) {
	parsedURI, err := url.ParseRequestURI(originalURI)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid SDMX request URI")
	}

	path, err := parseResourcePath(tail)
	if err != nil {
		return nil, err
	}
	params, err := parseRawQuery(parsedURI.RawQuery)
	if err != nil {
		return nil, err
	}

	constraints := map[string][]string{}
	for _, param := range params {
		componentID, ok, err := componentFilterName(param.Name)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if _, exists := constraints[componentID]; exists {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate SDMX component filter %q", componentID)
		}
		values, err := parseComponentValues(param.Value)
		if err != nil {
			return nil, err
		}
		constraints[componentID] = values
	}

	if err := validateDataRequest(path, constraints); err != nil {
		return nil, err
	}

	return &DataRequest{Path: path, Constraints: constraints}, nil
}

func componentFilterName(name string) (string, bool, error) {
	if !strings.HasPrefix(name, "c[") {
		return "", false, nil
	}
	if !strings.HasSuffix(name, "]") {
		return "", false, status.Error(codes.InvalidArgument, "invalid SDMX component filter name")
	}
	componentID := strings.TrimSuffix(strings.TrimPrefix(name, "c["), "]")
	if componentID == "" {
		return "", false, status.Error(codes.InvalidArgument, "invalid SDMX component filter name")
	}
	return componentID, true, nil
}

func parseComponentValues(value string) ([]string, error) {
	if value == "" {
		return nil, status.Error(codes.InvalidArgument, "empty SDMX component filter value")
	}
	if strings.Contains(value, "+") {
		return nil, status.Error(codes.Unimplemented, "SDMX AND filters are not implemented yet")
	}

	parts := strings.Split(value, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return nil, status.Error(codes.InvalidArgument, "empty SDMX component filter value")
		}
		if hasUnsupportedOperator(part) {
			return nil, status.Error(codes.Unimplemented, "SDMX component filter operators are not implemented yet")
		}
		values = append(values, part)
	}
	return values, nil
}

func hasUnsupportedOperator(value string) bool {
	operator, _, ok := strings.Cut(value, ":")
	if !ok {
		return false
	}
	switch operator {
	case "eq", "ne", "lt", "le", "gt", "ge", "co", "nc", "sw", "ew":
		return true
	default:
		return false
	}
}
