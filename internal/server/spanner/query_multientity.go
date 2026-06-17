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
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/spanner"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

// GetObservations retrieves observations using the new schema.
func (nc *multiEntityClient) GetObservations(ctx context.Context, variables []string, entities []string, date string) ([]*Observation, error) {
	stmt, err := GetMultiEntityObservationsQuery(variables, entities, date, nc.cfg)
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

	observations, err := reconstructObservations(rawObs)
	if err != nil {
		return nil, err
	}
	if err := validateObservations(observations); err != nil {
		return nil, err
	}
	return observations, nil
}

// CheckVariableExistence checks variable existence across all entity slots in a single CTE-based query.
func (nc *multiEntityClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	stmt, err := GetMultiEntityStatVarsByEntityQuery(variables, entities, nc.cfg)
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
	stmt := GetMultiEntityGroupPlaceExistenceQuery(variableGroups, entities, predicate, nc.cfg)
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
		*GetMultiEntityStatVarGroupNodeQuery(nodes, includeDefinitions, nc.cfg),
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
			*GetMultiEntityFilteredSVGChildrenQuery(templateSV, node, constrainedPlaces, constrainedImport, numEntitiesExistence, includeDefinitions, nc.cfg),
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
			*GetMultiEntityFilteredSVGChildrenQuery(templateSVG, node, constrainedPlaces, constrainedImport, numEntitiesExistence, includeDefinitions, nc.cfg),
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

	stmt := GetMultiEntityFilteredTopicChildrenQuery(nodes, constrainedPlaces, constrainedImport, numEntitiesExistence, nc.cfg)
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

	stmt, err := GetMultiEntityObservationsContainedInPlaceQuery(variables, containedInPlace, date, nc.cfg)
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

	observations, err = reconstructObservations(rawObs)
	if err != nil {
		return nil, err
	}
	if err := validateObservations(observations); err != nil {
		return nil, err
	}
	return observations, nil
}

// reconstructObservations processes raw Spanner rows and handles JSON facets extraction in Go code.
func reconstructObservations(rawObs []*rawObservation) ([]*Observation, error) {
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
				attributes, err := decodeObservationAttributes(dv.Attributes)
				if err != nil {
					return nil, fmt.Errorf(
						"invalid observation attributes: variable=%q entity=%q facet_id=%q date=%q: %w",
						r.VariableMeasured,
						r.ObservationAbout,
						r.FacetId,
						dv.Date,
						err,
					)
				}
				obs.Observations = append(obs.Observations, &DateValue{
					Date:       dv.Date,
					Value:      dv.Value,
					Attributes: attributes,
				})
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

	return result, nil
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

// decodeObservationAttributes expects attributes to be a JSON object that can be
// represented as a string:string map. Entries with incompatible values are skipped.
func decodeObservationAttributes(attrs spanner.NullJSON) (map[string]string, error) {
	if !attrs.Valid || attrs.Value == nil {
		return nil, nil
	}

	switch values := attrs.Value.(type) {
	case map[string]interface{}:
		result := make(map[string]string, len(values))
		for key, value := range values {
			stringValue, ok := observationAttributeValueToString(value)
			if !ok {
				continue
			}
			result[key] = stringValue
		}
		if len(result) == 0 {
			return nil, nil
		}
		return result, nil
	case map[string]string:
		if len(values) == 0 {
			return nil, nil
		}
		result := make(map[string]string, len(values))
		for key, value := range values {
			result[key] = value
		}
		return result, nil
	default:
		return nil, fmt.Errorf("attributes JSON must be an object, got %T", attrs.Value)
	}
}

func observationAttributeValueToString(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case bool, float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(v), true
	case json.Number:
		return v.String(), true
	default:
		return "", false
	}
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

// GetSdmxObservations retrieves observations for SDMX query using dynamic entity slot mappings.
func (nc *multiEntityClient) GetSdmxObservations(
	ctx context.Context,
	req *pb.SdmxDataQuery,
) (*pb.SdmxDataResult, error) {
	if req == nil {
		return nil, fmt.Errorf("GetSdmxObservations: request cannot be nil")
	}
	if req.Constraints == nil {
		return nil, fmt.Errorf("GetSdmxObservations: request constraints cannot be nil")
	}

	variables := []string{}
	if list, ok := req.Constraints["variableMeasured"]; ok {
		variables = list.Values
	}

	entityMappings := map[string]map[string]string{}
	if len(variables) > 0 {
		arc := &v2.Arc{
			Out:        true,
			SingleProp: "entityMapping",
		}
		edgesMap, err := nc.sc.GetNodeEdgesByID(ctx, variables, arc, 100, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch entityMapping: %w", err)
		}
		entityMappings = parseEntityMappings(edgesMap)
	}

	stmt, err := GetMultiEntitySdmxObservationsQuery(req.Constraints, entityMappings, nc.cfg)
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

	// Reconstruct the result, mapping entities back to requested keys
	result := &pb.SdmxDataResult{
		Observations: []*pb.SdmxObservation{},
	}

	for _, r := range rawObs {
		// Parse entities JSON to map slot columns (entity1, entity2) to actual DCIDs
		entitiesMap := map[string]string{}
		if r.Entities.Valid {
			if m, ok := r.Entities.Value.(map[string]interface{}); ok {
				for k, v := range m {
					if s, ok := v.(string); ok {
						entitiesMap[k] = s
					}
				}
			}
		}

		dimensions := map[string]string{
			"variableMeasured": r.VariableMeasured,
		}

		// Map entities back to their original dimension keys using the variable's entity mapping
		if mapping, ok := entityMappings[r.VariableMeasured]; ok {
			for reqKey, col := range mapping {
				if val, ok := entitiesMap[col]; ok {
					dimensions[reqKey] = val
				}
			}
		} else {
			// Single-entity fallback: map entity1 to observationAbout
			if r.ObservationAbout != "" {
				dimensions["observationAbout"] = r.ObservationAbout
			}
		}

		obs := &pb.SdmxObservation{
			VariableMeasured: r.VariableMeasured,
			Dimensions:       dimensions,
			DatesAndValues:   []*pb.SdmxDateValue{},
		}
		if r.ProvenanceID.Valid {
			obs.Provenance = r.ProvenanceID.StringVal
		}

		for _, dv := range r.DatesAndValues {
			if dv == nil {
				continue
			}
			if dv.Date != "" {
				obs.DatesAndValues = append(obs.DatesAndValues, &pb.SdmxDateValue{
					Date:  dv.Date,
					Value: dv.Value,
				})
			}
		}

		// Reconstruct and attach attributes from facet JSON
		if r.Facets.Valid {
			if m, ok := r.Facets.Value.(map[string]interface{}); ok {
				obs.Attributes = map[string]string{}
				for k, v := range m {
					if s, ok := observationAttributeValueToString(v); ok {
						obs.Attributes[k] = s
					}
				}
			}
		}

		result.Observations = append(result.Observations, obs)
	}

	return result, nil
}

// GetSdmxAvailability retrieves available observationAbout values for SDMX availability.
func (nc *multiEntityClient) GetSdmxAvailability(
	ctx context.Context,
	req *pb.SdmxAvailabilityQuery,
) (*pb.SdmxAvailabilityResult, error) {
	if req == nil {
		return nil, fmt.Errorf("GetSdmxAvailability: request cannot be nil")
	}

	stmt, err := GetMultiEntitySdmxAvailabilityQuery(req, nc.cfg)
	if err != nil {
		return nil, err
	}

	result := &pb.SdmxAvailabilityResult{}
	err = queryStructs(ctx, nc.sc, *stmt, func() interface{} { return &rawSdmxAvailabilityValue{} }, func(row interface{}) {
		value := row.(*rawSdmxAvailabilityValue).Value
		if value != "" {
			result.Values = append(result.Values, value)
		}
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func parseEntityMappings(edgesMap map[string][]*Edge) map[string]map[string]string {
	result := map[string]map[string]string{}
	for varDcid, edges := range edgesMap {
		mapping := map[string]string{}
		for _, edge := range edges {
			if edge == nil {
				continue
			}
			if edge.Predicate == "entityMapping" {
				parts := strings.SplitN(edge.Value, "=", 2)
				if len(parts) == 2 {
					k := strings.TrimSpace(parts[0])
					v := strings.TrimSpace(parts[1])
					if k != "" && v != "" {
						mapping[k] = v
					}
				}
			}
		}
		if len(mapping) > 0 {
			result[varDcid] = mapping
		}
	}
	return result
}
