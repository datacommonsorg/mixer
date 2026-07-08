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

	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	dataContextDataflow = "dataflow"
)

func parseComponentFilter(param queryParam, constraints map[string][]string) (bool, error) {
	componentID, ok, err := componentFilterName(param.Name)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if _, exists := constraints[componentID]; exists {
		return true, status.Errorf(codes.InvalidArgument, "duplicate SDMX component filter %q", componentID)
	}
	values, err := parseComponentValues(param.Value)
	if err != nil {
		return true, err
	}
	constraints[componentID] = values
	return true, nil
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
		part = strings.TrimSpace(part)
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

func validateDataRequest(path ResourcePath, constraints map[string][]string) error {
	if path.Context == "" {
		return status.Error(codes.InvalidArgument, "SDMX data path is required")
	}
	if path.Context != dataContextDataflow || path.AgencyID != datacommons.DataflowAgencyID || path.ResourceID != datacommons.DataflowID {
		return status.Error(codes.Unimplemented, "unsupported SDMX dataflow")
	}
	if path.Version != datacommons.DataflowVersion {
		return status.Errorf(codes.InvalidArgument, "unsupported SDMX dataflow version %q", path.Version)
	}

	for componentID := range constraints {
		if !isAllowedDataComponent(componentID) {
			return status.Errorf(codes.Unimplemented, "unsupported SDMX component filter %q", componentID)
		}
	}
	if _, ok := constraints[datacommons.ComponentVariableMeasured]; !ok {
		return status.Error(codes.InvalidArgument, "missing required SDMX component filter variableMeasured")
	}
	return nil
}

func validateAvailabilityRequest(path AvailabilityPath, constraints map[string][]string) error {
	if path.Context == "" {
		return status.Error(codes.InvalidArgument, "SDMX availability path is required")
	}
	if path.Context != dataContextDataflow || path.AgencyID != datacommons.DataflowAgencyID || path.ResourceID != datacommons.DataflowID {
		return status.Error(codes.Unimplemented, "unsupported SDMX dataflow")
	}
	if path.Version != datacommons.DataflowVersion {
		return status.Errorf(codes.InvalidArgument, "unsupported SDMX dataflow version %q", path.Version)
	}
	if !isAvailabilityComponent(path.ComponentID) {
		return status.Errorf(codes.Unimplemented, "unsupported SDMX availability component %q", path.ComponentID)
	}

	for componentID := range constraints {
		if componentID != datacommons.ComponentVariableMeasured {
			return status.Errorf(codes.Unimplemented, "unsupported SDMX component filter %q", componentID)
		}
	}
	if _, ok := constraints[datacommons.ComponentVariableMeasured]; !ok {
		return status.Error(codes.InvalidArgument, "missing required SDMX component filter variableMeasured")
	}
	return nil
}

func isAllowedDataComponent(componentID string) bool {
	if componentID == datacommons.ComponentTimePeriod {
		return false
	}
	if kind, ok := datacommons.DataComponentKind(componentID); ok {
		return kind == datacommons.ComponentKindDimension
	}
	return isDynamicEntityComponent(componentID)
}

func isDynamicEntityComponent(componentID string) bool {
	if componentID == "" || componentID[0] < 'a' || componentID[0] > 'z' {
		return false
	}
	for i := 0; i < len(componentID); i++ {
		c := componentID[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			continue
		}
		return false
	}
	return true
}

func isAvailabilityComponent(componentID string) bool {
	if componentID == datacommons.ComponentTimePeriod {
		return false
	}
	kind, ok := datacommons.DataComponentKind(componentID)
	return ok && kind == datacommons.ComponentKindDimension
}
