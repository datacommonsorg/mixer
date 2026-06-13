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
	"cmp"
	"context"
	"fmt"
	"slices"

	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

// GetObservations retrieves observations using the new schema.
func (nc *multiEntityClient) GetObservations(ctx context.Context, variables []string, entities []string, date string) ([]*Observation, error) {
	stmt, err := GetMultiEntityObservationsQuery(variables, entities, date)
	if err != nil {
		return nil, err
	}

	var rawObs []*rawObservation
	err = queryStructs(ctx, nc.sc, *stmt, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	if err != nil {
		return nil, err
	}

	observations := reconstructObservations(rawObs)
	if err := validateObservations(observations); err != nil {
		return nil, err
	}
	return observations, nil
}

// CheckVariableExistence checks variable existence across all entity slots in a single CTE-based query.
func (nc *multiEntityClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	stmt, err := GetMultiEntityStatVarsByEntityQuery(variables, entities)
	if err != nil {
		return nil, err
	}
	return queryDynamic(ctx, nc.sc, *stmt)
}

// GetStatVarGroupNode fetches StatVarGroupNode info using multi-entity TimeSeries existence checks.
func (nc *multiEntityClient) GetStatVarGroupNode(ctx context.Context, nodes []string, includeDefinitions bool) ([]*StatVarGroupNode, error) {
	var svgNodes []*StatVarGroupNode
	if len(nodes) == 0 {
		return svgNodes, nil
	}

	err := queryStructs(
		ctx,
		nc.sc,
		*GetMultiEntityStatVarGroupNodeQuery(nodes, includeDefinitions),
		func() interface{} {
			return &StatVarGroupNode{}
		},
		func(rowStruct interface{}) {
			svgNodes = append(svgNodes, rowStruct.(*StatVarGroupNode))
		},
	)
	if err != nil {
		return svgNodes, err
	}

	return svgNodes, nil
}

// GetObservationsContainedInPlace fetches observations for children containment.
func (nc *multiEntityClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace, date string) ([]*Observation, error) {
	stmt, err := GetMultiEntityObservationsContainedInPlaceQuery(variables, containedInPlace, date)
	if err != nil {
		return nil, err
	}

	var rawObs []*rawObservation
	err = queryStructs(ctx, nc.sc, *stmt, func() interface{} { return &rawObservation{} }, func(row interface{}) {
		rawObs = append(rawObs, row.(*rawObservation))
	})
	if err != nil {
		return nil, err
	}

	observations := reconstructObservations(rawObs)
	if err := validateObservations(observations); err != nil {
		return nil, err
	}
	return observations, nil
}

// reconstructObservations processes raw Spanner rows and handles JSON facets extraction in Go code.
func reconstructObservations(rawObs []*rawObservation) []*Observation {
	var result []*Observation

	for _, r := range rawObs {
		obs := &Observation{
			VariableMeasured: r.VariableMeasured,
			ObservationAbout: r.ObservationAbout,
			FacetId:          r.FacetId,
			Observations:     TimeSeries{},
		}
		if r.ProvenanceID.Valid {
			obs.ProvenanceID = r.ProvenanceID.StringVal
		}

		for _, dv := range r.DatesAndValues {
			if dv == nil {
				continue
			}
			if dv.Date != "" {
				obs.Observations = append(obs.Observations, &DateValue{Date: dv.Date, Value: dv.Value})
			}
		}

		// Sort observations chronologically in Go.
		slices.SortFunc(obs.Observations, func(a, b *DateValue) int {
			return cmp.Compare(a.Date, b.Date)
		})

		if r.Facets.Valid {
			if m, ok := r.Facets.Value.(map[string]interface{}); ok {
				populateObservationFacets(obs, m)
			}
		}
		result = append(result, obs)
	}

	return result
}

func validateObservations(observations []*Observation) error {
	for _, obs := range observations {
		if obs.ProvenanceID == "" {
			return fmt.Errorf(
				"observation missing provenance: variable=%q entity=%q facet_id=%q",
				obs.VariableMeasured,
				obs.ObservationAbout,
				obs.FacetId,
			)
		}
	}
	return nil
}

func populateObservationFacets(obs *Observation, facets map[string]interface{}) {
	obs.ImportName = getJSONString(facets, "importName")
	obs.ObservationPeriod = getJSONString(facets, "observationPeriod")
	obs.MeasurementMethod = getJSONString(facets, "measurementMethod")
	obs.Unit = getJSONString(facets, "unit")
	obs.ScalingFactor = getJSONString(facets, "scalingFactor")
	obs.IsDcAggregate = getJSONBool(facets, "isDcAggregate")
	obs.ProvenanceURL = getJSONString(facets, "provenanceUrl")
}

func getJSONString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

func getJSONBool(m map[string]interface{}, key string) bool {
	if val, ok := m[key]; ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}
