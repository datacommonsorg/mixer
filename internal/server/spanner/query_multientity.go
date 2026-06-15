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

	"cloud.google.com/go/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
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

// CheckVariableGroupPlaceExistence checks SVG/topic existence across all entity slots.
func (nc *multiEntityClient) CheckVariableGroupPlaceExistence(ctx context.Context, variableGroups []string, entities []string, predicate string) ([][]string, error) {
	if len(variableGroups) == 0 || len(entities) == 0 {
		return [][]string{}, nil
	}
	stmt := GetMultiEntityGroupPlaceExistenceQuery(variableGroups, entities, predicate)
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

// GetFilteredStatVarGroupNode fetches filtered StatVarGroupNode info using multi-entity TimeSeries filters.
func (nc *multiEntityClient) GetFilteredStatVarGroupNode(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int, includeDefinitions bool) (map[string]*FilteredStatVarGroupNode, error) {
	response := map[string]*FilteredStatVarGroupNode{}
	errGroup, errCtx := errgroup.WithContext(ctx)
	errGroup.SetLimit(maxConcurrentFilteredSVGGoroutines)

	type nodeResult struct {
		node string
		resp *FilteredStatVarGroupNode
	}
	resps := make(chan nodeResult, len(nodes))

	for _, node := range nodes {
		node := node
		errGroup.Go(func() error {
			resp, err := nc.getSingleFilteredStatVarGroupNode(errCtx, node, constrainedPlaces, constrainedImport, numEntitiesExistence, includeDefinitions)
			if err != nil {
				return fmt.Errorf("error fetching filtered StatVarGroupNode for node %s: %w", node, err)
			}
			resps <- nodeResult{node: node, resp: resp}
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	close(resps)

	for res := range resps {
		response[res.node] = res.resp
	}

	return response, nil
}

func (nc *multiEntityClient) getSingleFilteredStatVarGroupNode(ctx context.Context, node string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int, includeDefinitions bool) (*FilteredStatVarGroupNode, error) {
	errGroup, errCtx := errgroup.WithContext(ctx)
	var svgChildren []*SVGChild
	var childSVs []*ChildSV
	var childSVGs []*ChildSVG

	errGroup.Go(func() error {
		return queryStructs(
			errCtx,
			nc.sc,
			*GetSVGChildrenQuery(node, includeDefinitions),
			func() interface{} {
				return &SVGChild{}
			},
			func(rowStruct interface{}) {
				svgChildren = append(svgChildren, rowStruct.(*SVGChild))
			},
		)
	})

	errGroup.Go(func() error {
		return queryStructs(
			errCtx,
			nc.sc,
			*GetMultiEntityFilteredSVGChildrenQuery(templateSV, node, constrainedPlaces, constrainedImport, numEntitiesExistence, includeDefinitions),
			func() interface{} {
				return &ChildSV{}
			},
			func(rowStruct interface{}) {
				childSVs = append(childSVs, rowStruct.(*ChildSV))
			},
		)
	})

	errGroup.Go(func() error {
		return queryStructs(
			errCtx,
			nc.sc,
			*GetMultiEntityFilteredSVGChildrenQuery(templateSVG, node, constrainedPlaces, constrainedImport, numEntitiesExistence, includeDefinitions),
			func() interface{} {
				return &ChildSVG{}
			},
			func(rowStruct interface{}) {
				childSVGs = append(childSVGs, rowStruct.(*ChildSVG))
			},
		)
	})

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return &FilteredStatVarGroupNode{
		SVGChild: svgChildren,
		ChildSV:  childSVs,
		ChildSVG: childSVGs,
	}, nil
}

// GetFilteredTopic fetches filtered Topic counts using multi-entity TimeSeries filters.
func (nc *multiEntityClient) GetFilteredTopic(ctx context.Context, nodes []string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int) (map[string]int, error) {
	counts := make(map[string]int, len(nodes))
	for _, node := range nodes {
		counts[node] = 0
	}

	stmt := GetMultiEntityFilteredTopicChildrenQuery(nodes, constrainedPlaces, constrainedImport, numEntitiesExistence)
	err := nc.sc.executeQuery(ctx, *stmt, func(iter *spanner.RowIterator) error {
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}

			var parent string
			var count int64
			if err := row.Columns(&parent, &count); err != nil {
				return fmt.Errorf("error reading row for filtered Topic children count: %w", err)
			}
			counts[parent] = int(count)
		}
		return nil
	})
	if err != nil {
		return counts, err
	}

	return counts, nil
}

// GetObservationsContainedInPlace fetches observations for children containment.
func (nc *multiEntityClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace, date string) ([]*Observation, error) {
	var observations []*Observation
	if len(variables) == 0 || containedInPlace == nil {
		return observations, nil
	}

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

	observations = reconstructObservations(rawObs)
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
