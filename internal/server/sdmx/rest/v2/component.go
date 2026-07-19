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

	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	dataContextDataflow = "dataflow"
)

type componentSelector struct {
	componentID string
	propertyID  string
	transitive  bool
}

func parseComponentFilter(param queryParam, constraints map[string]*sdmxpb.SdmxComponentConstraint) (bool, error) {
	selector, ok, err := componentFilterName(param.Name)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	values, err := parseComponentValues(param.Value)
	if err != nil {
		return true, err
	}
	predicates := make([]*sdmxpb.SdmxPredicate, 0, len(values))
	for _, value := range values {
		predicates = append(predicates, &sdmxpb.SdmxPredicate{Value: value})
	}

	constraint := constraints[selector.componentID]
	if constraint == nil {
		constraint = &sdmxpb.SdmxComponentConstraint{}
		constraints[selector.componentID] = constraint
	}
	if selector.propertyID == "" {
		if len(constraint.GetPredicates()) > 0 {
			return true, status.Errorf(codes.InvalidArgument, "duplicate SDMX component filter %q", selector.componentID)
		}
		constraint.Predicates = predicates
		return true, nil
	}

	if constraint.PropertyConstraints == nil {
		constraint.PropertyConstraints = map[string]*sdmxpb.SdmxPropertyConstraint{}
	}
	if _, exists := constraint.PropertyConstraints[selector.propertyID]; exists {
		return true, status.Errorf(codes.InvalidArgument, "duplicate SDMX property filter %q", selector.componentID+"."+selector.propertyID)
	}
	constraint.PropertyConstraints[selector.propertyID] = &sdmxpb.SdmxPropertyConstraint{
		Predicates: predicates,
		Transitive: selector.transitive,
	}
	return true, nil
}

func componentFilterName(name string) (componentSelector, bool, error) {
	if !strings.HasPrefix(name, "c[") {
		return componentSelector{}, false, nil
	}
	if !strings.HasSuffix(name, "]") {
		return componentSelector{}, false, status.Error(codes.InvalidArgument, "invalid SDMX component filter name")
	}
	selectorName := strings.TrimSuffix(strings.TrimPrefix(name, "c["), "]")
	parts := strings.Split(selectorName, ".")
	if len(parts) > 2 || !isValidComponentID(parts[0]) {
		return componentSelector{}, false, status.Errorf(codes.InvalidArgument, "invalid SDMX component filter %q", selectorName)
	}
	selector := componentSelector{componentID: parts[0]}
	if len(parts) == 1 {
		return selector, true, nil
	}

	propertyID := parts[1]
	selector.transitive = strings.HasSuffix(propertyID, "+")
	if selector.transitive {
		propertyID = strings.TrimSuffix(propertyID, "+")
	}
	if !isValidComponentID(propertyID) {
		return componentSelector{}, false, status.Errorf(codes.InvalidArgument, "invalid SDMX component filter %q", selectorName)
	}
	selector.propertyID = propertyID
	return selector, true, nil
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

func validateDataRequest(path ResourcePath, constraints map[string]*sdmxpb.SdmxComponentConstraint) error {
	if path.Context == "" {
		return status.Error(codes.InvalidArgument, "SDMX data path is required")
	}
	if path.Context != dataContextDataflow || path.AgencyID != datacommons.DataflowAgencyID || path.ResourceID != datacommons.DataflowID {
		return status.Error(codes.Unimplemented, "unsupported SDMX dataflow")
	}
	if path.Version != datacommons.DataflowVersion {
		return status.Errorf(codes.InvalidArgument, "unsupported SDMX dataflow version %q", path.Version)
	}

	if err := validateComponentFilters(constraints, isFilterableDataComponentCandidate); err != nil {
		return err
	}
	return datacommons.ValidateDataConstraints(constraints)
}

func validateAvailabilityRequest(path AvailabilityPath, constraints map[string]*sdmxpb.SdmxComponentConstraint) error {
	if path.Context == "" {
		return status.Error(codes.InvalidArgument, "SDMX availability path is required")
	}
	if path.Context != dataContextDataflow || path.AgencyID != datacommons.DataflowAgencyID || path.ResourceID != datacommons.DataflowID {
		return status.Error(codes.Unimplemented, "unsupported SDMX dataflow")
	}
	if path.Version != datacommons.DataflowVersion {
		return status.Errorf(codes.InvalidArgument, "unsupported SDMX dataflow version %q", path.Version)
	}
	if !isValidComponentID(path.ComponentID) {
		return status.Errorf(codes.InvalidArgument, "invalid SDMX availability component %q", path.ComponentID)
	}
	if !isFilterableDimensionCandidate(path.ComponentID) {
		return status.Errorf(codes.Unimplemented, "unsupported SDMX availability component %q", path.ComponentID)
	}

	if err := validateComponentFilters(constraints, isFilterableDimensionCandidate); err != nil {
		return err
	}
	return datacommons.ValidateAvailabilityConstraints(constraints)
}

func validateComponentFilters(constraints map[string]*sdmxpb.SdmxComponentConstraint, isFilterable func(string) bool) error {
	for componentID := range constraints {
		if !isValidComponentID(componentID) {
			return status.Errorf(codes.InvalidArgument, "invalid SDMX component filter %q", componentID)
		}
		if !isFilterable(componentID) {
			return status.Errorf(codes.Unimplemented, "unsupported SDMX component filter %q", componentID)
		}
	}
	if _, ok := constraints[datacommons.ComponentVariableMeasured]; !ok {
		return status.Error(codes.InvalidArgument, "missing required SDMX component filter variableMeasured")
	}
	return nil
}

func isFilterableDataComponentCandidate(componentID string) bool {
	_, isFilterableAttribute := datacommons.FilterableAttributes[componentID]
	return componentID == datacommons.ComponentTimePeriod || isFilterableDimensionCandidate(componentID) || isFilterableAttribute
}

func isFilterableDimensionCandidate(componentID string) bool {
	if componentID == datacommons.ComponentTimePeriod {
		return false
	}
	if kind, ok := datacommons.DataComponentKind(componentID); ok {
		return kind == datacommons.ComponentKindDimension
	}
	return isDynamicEntityComponent(componentID)
}

func isValidComponentID(componentID string) bool {
	if componentID == "" {
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

func isDynamicEntityComponent(componentID string) bool {
	if !isValidComponentID(componentID) || componentID[0] < 'a' || componentID[0] > 'z' {
		return false
	}
	return true
}
