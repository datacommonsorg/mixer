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

	pb_int "github.com/datacommonsorg/mixer/internal/proto/sdmx"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
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

// CheckVariableExistence checks which variables exist for which entities using the normalized schema.
func (nc *normalizedClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	stmt, err := GetNormalizedStatVarsByEntityQuery(variables, entities)
	if err != nil {
		return nil, err
	}
	return queryDynamic(ctx, nc.sc, *stmt)
}

// GetObservationsContainedInPlace retrieves observations for entities contained in a place
// using the normalized schema.
func (nc *normalizedClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error) {
	if containedInPlace == nil {
		return nil, fmt.Errorf("containedInPlace must be specified")
	}

	rawObs, err := nc.fetchRawObservationsContainedInPlace(ctx, variables, containedInPlace)
	if err != nil {
		return nil, err
	}

	if len(rawObs) == 0 {
		return []*Observation{}, nil
	}

	return reconstructObservations(rawObs), nil
}

// fetchRawObservationsContainedInPlace fetches data from Graph, TimeSeries and StatVarObservation tables.
func (nc *normalizedClient) fetchRawObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*rawObservation, error) {
	stmt := GetNormalizedObservationsContainedInPlaceQuery(variables, containedInPlace)

	var rawObs []*rawObservation
	err := queryStructs(ctx, nc.sc, *stmt, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	return rawObs, err
}

var facetAttributes = map[string]bool{
	"unit":              true,
	"measurementMethod": true,
	"scalingFactor":     true,
	"observationPeriod": true,
	"importName":        true,
	"provenanceUrl":     true,
}

// GetSdmxObservations retrieves observations from Spanner given a list of constraints
// using the normalized schema and relational division.
func (nc *normalizedClient) GetSdmxObservations(ctx context.Context, req *pb_int.SdmxDataQuery) (*pb_int.SdmxDataResult, error) {
	stmt := GetSdmxObservationsQuery(req)

	var rawObs []*rawObservation
	err := queryStructs(ctx, nc.sc, *stmt, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	if err != nil {
		return nil, err
	}

	if len(rawObs) == 0 {
		return &pb_int.SdmxDataResult{Observations: []*pb_int.SdmxObservation{}}, nil
	}

	return &pb_int.SdmxDataResult{Observations: reconstructSdmxObservations(rawObs)}, nil
}

// reconstructSdmxObservations combines raw observations and attributes into full SdmxObservation structs for SDMX.
func reconstructSdmxObservations(rawObs []*rawObservation) []*pb_int.SdmxObservation {
	var result []*pb_int.SdmxObservation

	for _, r := range rawObs {
		obs := &pb_int.SdmxObservation{
			VariableMeasured: r.VariableMeasured,
			Provenance:       r.Provenance,
			DatesAndValues:   []*pb_int.SdmxDateValue{},
			Dimensions:       map[string]string{},
			Attributes:       map[string]string{},
		}

		for _, dv := range r.DatesAndValues {
			obs.DatesAndValues = append(obs.DatesAndValues, &pb_int.SdmxDateValue{Date: dv.Date, Value: dv.Value})
		}

		for _, attr := range r.Attributes {
			if attr.Property == "provenance" {
				continue
			}
			if facetAttributes[attr.Property] {
				obs.Attributes[attr.Property] = attr.Value
			} else {
				obs.Dimensions[attr.Property] = attr.Value
			}
		}
		result = append(result, obs)
	}

	return result
}
