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
	serversdmx "github.com/datacommonsorg/mixer/internal/server/sdmx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ComponentVariableMeasured = "variableMeasured"
	ComponentObservationAbout = "observationAbout"
	ComponentTimePeriod       = "TIME_PERIOD"
	ComponentObservationValue = "OBS_VALUE"

	internalObservationDate = "observationDate"
)

func validateDataRequest(path ResourcePath, constraints map[string][]string) error {
	if path.Context == "" {
		return status.Error(codes.InvalidArgument, "SDMX data path is required")
	}
	if path.Context != serversdmx.DataContext || path.AgencyID != serversdmx.DataAgencyID || path.ResourceID != serversdmx.DataResourceID {
		return status.Error(codes.Unimplemented, "unsupported SDMX dataflow")
	}
	if path.Version != serversdmx.DataVersion {
		return status.Errorf(codes.InvalidArgument, "unsupported SDMX dataflow version %q", path.Version)
	}

	for componentID := range constraints {
		if !isAllowedDataComponent(componentID) {
			return status.Errorf(codes.Unimplemented, "unsupported SDMX component filter %q", componentID)
		}
	}
	if _, ok := constraints[ComponentVariableMeasured]; !ok {
		return status.Error(codes.InvalidArgument, "missing required SDMX component filter variableMeasured")
	}
	if _, ok := constraints[ComponentObservationAbout]; !ok {
		return status.Error(codes.InvalidArgument, "missing required SDMX component filter observationAbout")
	}
	return nil
}

func validateAvailabilityRequest(path AvailabilityPath, constraints map[string][]string) error {
	if path.Context == "" {
		return status.Error(codes.InvalidArgument, "SDMX availability path is required")
	}
	if path.Context != serversdmx.DataContext || path.AgencyID != serversdmx.DataAgencyID || path.ResourceID != serversdmx.DataResourceID {
		return status.Error(codes.Unimplemented, "unsupported SDMX dataflow")
	}
	if path.Version != serversdmx.DataVersion {
		return status.Errorf(codes.InvalidArgument, "unsupported SDMX dataflow version %q", path.Version)
	}
	if !isAvailabilityComponent(path.ComponentID) {
		return status.Errorf(codes.Unimplemented, "unsupported SDMX availability component %q", path.ComponentID)
	}

	for componentID := range constraints {
		if componentID != ComponentVariableMeasured {
			return status.Errorf(codes.Unimplemented, "unsupported SDMX component filter %q", componentID)
		}
	}
	if _, ok := constraints[ComponentVariableMeasured]; !ok {
		return status.Error(codes.InvalidArgument, "missing required SDMX component filter variableMeasured")
	}
	return nil
}

func isAllowedDataComponent(componentID string) bool {
	switch componentID {
	case ComponentVariableMeasured, ComponentObservationAbout, ComponentTimePeriod:
		return true
	default:
		return false
	}
}

func isAvailabilityComponent(componentID string) bool {
	if componentID == ComponentTimePeriod {
		return false
	}
	kind, ok := dataComponentKind(componentID)
	return ok && kind == serversdmx.ComponentKindDimension
}

func dataComponentKind(componentID string) (serversdmx.ComponentKind, bool) {
	for _, component := range serversdmx.DataCSVComponents {
		if component.ID == componentID {
			return component.Kind, true
		}
	}
	return "", false
}

func InternalConstraintComponentID(componentID string) (string, error) {
	switch componentID {
	case ComponentVariableMeasured, ComponentObservationAbout:
		return componentID, nil
	case ComponentTimePeriod:
		return internalObservationDate, nil
	case ComponentObservationValue:
		return "", status.Error(codes.Unimplemented, "SDMX observation value filters are not implemented yet")
	default:
		return "", status.Errorf(codes.Unimplemented, "unsupported SDMX component filter %q", componentID)
	}
}

func InternalAvailabilityComponentID(componentID string) (string, error) {
	if !isAvailabilityComponent(componentID) {
		return "", status.Errorf(codes.Unimplemented, "unsupported SDMX availability component %q", componentID)
	}
	return componentID, nil
}

func InternalAvailabilityConstraintComponentID(componentID string) (string, error) {
	if componentID != ComponentVariableMeasured {
		return "", status.Errorf(codes.Unimplemented, "unsupported SDMX component filter %q", componentID)
	}
	return componentID, nil
}
