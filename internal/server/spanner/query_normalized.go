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

package spanner

import (
	"context"
	"fmt"
	"strconv"
)


// GetObservations retrieves observations from Spanner given a list of variables and entities
// using the normalized schema.
func (nc *normalizedClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error) {
	if len(entities) == 0 {
		return nil, fmt.Errorf("entity must be specified")
	}

	rawObs, err := nc.fetchRawObservations(ctx, variables, entities)
	if err != nil {
		return nil, err
	}

	if len(rawObs) == 0 {
		return []*Observation{}, nil
	}

	return reconstructObservations(rawObs), nil
}

// fetchRawObservations fetches data from TimeSeries and StatVarObservation tables.
func (nc *normalizedClient) fetchRawObservations(ctx context.Context, variables []string, entities []string) ([]*rawObservation, error) {
	stmt := GetNormalizedObservationsQuery(variables, entities)

	var rawObs []*rawObservation
	err := queryStructs(ctx, nc.sc, *stmt, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	return rawObs, err
}

// reconstructObservations combines raw observations and attributes into full Observation structs.
func reconstructObservations(rawObs []*rawObservation) []*Observation {
	var result []*Observation

	for _, r := range rawObs {
		obs := &Observation{
			VariableMeasured: r.VariableMeasured,
			Observations:     TimeSeries{},
		}

		for _, dv := range r.DatesAndValues {
			obs.Observations = append(obs.Observations, &DateValue{Date: dv.Date, Value: dv.Value})
		}

		for _, attr := range r.Attributes {
			switch attr.Property {
			case "observationAbout":
				obs.ObservationAbout = attr.Value
			case "facetId":
				obs.FacetId = attr.Value
			case "importName":
				obs.ImportName = attr.Value
			case "provenanceUrl":
				obs.ProvenanceURL = attr.Value
			case "observationPeriod":
				obs.ObservationPeriod = attr.Value
			case "measurementMethod":
				obs.MeasurementMethod = attr.Value
			case "unit":
				obs.Unit = attr.Value
			case "scalingFactor":
				obs.ScalingFactor = attr.Value
			case "isDcAggregate":
				if b, err := strconv.ParseBool(attr.Value); err == nil {
					obs.IsDcAggregate = b
				}
			}
		}
		result = append(result, obs)
	}

	return result
}
