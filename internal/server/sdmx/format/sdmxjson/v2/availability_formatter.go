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

package sdmxjsonv2

import (
	"encoding/json"

	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
)

const (
	StructureJSONSchema    = "https://json.sdmx.org/2.0.0/sdmx-json-structure-schema.json"
	availabilityRoleActual = "Actual"
)

type AvailabilityJSONFormatter struct {
	AgencyID   string
	ResourceID string
	Version    string
}

type AvailabilityComponentValues struct {
	ID     string
	Values []string
}

type availabilityMessage struct {
	Schema string           `json:"$schema,omitempty"`
	Data   availabilityData `json:"data"`
}

type availabilityData struct {
	DataConstraints []availabilityDataConstraint `json:"dataConstraints"`
}

type availabilityDataConstraint struct {
	ID          string                   `json:"id"`
	AgencyID    string                   `json:"agencyID"`
	Version     string                   `json:"version"`
	Name        string                   `json:"name"`
	Role        string                   `json:"role"`
	CubeRegions []availabilityCubeRegion `json:"cubeRegions,omitempty"`
}

type availabilityCubeRegion struct {
	Include   bool                          `json:"include"`
	KeyValues []availabilityCubeRegionValue `json:"keyValues,omitempty"`
}

type availabilityCubeRegionValue struct {
	ID      string   `json:"id"`
	Include bool     `json:"include"`
	Values  []string `json:"values,omitempty"`
}

func (f *AvailabilityJSONFormatter) Format(componentID string, values []string) (string, error) {
	return f.FormatComponents([]AvailabilityComponentValues{{ID: componentID, Values: values}})
}

func (f *AvailabilityJSONFormatter) FormatComponents(components []AvailabilityComponentValues) (string, error) {
	agencyID := defaultString(f.AgencyID, datacommons.DataflowAgencyID)
	resourceID := defaultString(f.ResourceID, datacommons.DataflowID)
	version := defaultString(f.Version, datacommons.DataflowVersion)
	constraint := availabilityDataConstraint{
		ID:       resourceID + "_AVAILABILITY",
		AgencyID: agencyID,
		Version:  version,
		Name:     "Available " + resourceID + " data",
		Role:     availabilityRoleActual,
	}
	keyValues := []availabilityCubeRegionValue{}
	for _, component := range components {
		if len(component.Values) == 0 {
			continue
		}
		keyValues = append(keyValues, availabilityCubeRegionValue{
			ID:      component.ID,
			Include: true,
			Values:  component.Values,
		})
	}
	if len(keyValues) > 0 {
		constraint.CubeRegions = []availabilityCubeRegion{
			{
				Include:   true,
				KeyValues: keyValues,
			},
		}
	}
	payload := availabilityMessage{
		Schema: StructureJSONSchema,
		Data: availabilityData{
			DataConstraints: []availabilityDataConstraint{constraint},
		},
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func defaultString(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
