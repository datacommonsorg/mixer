// Copyright 2024 Google LLC
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

// Utility functions used by the SpannerDataSource.

package spanner

import (
	"fmt"
	"log"
	"sort"
	"strconv"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/protobuf/proto"
)

const (
	// Indicates that all properties should be returned.
	WILDCARD = "*"
	// Indicates that recursive property paths should be returned.
	CHAIN = "+"
	// Used for Facet responses with an entity expression.
	ENTITY_PLACEHOLDER = ""
)

// Select options for Observation.
const (
	ENTITY   = "entity"
	VARIABLE = "variable"
	DATE     = "date"
	VALUE    = "value"
	FACET    = "facet"
)

// Represents options for Observation response.
type queryOptions struct {
	entity   bool
	variable bool
	date     bool
	value    bool
	facet    bool
}

// Variable and entity combination.
type variableEntity struct {
	variable string
	entity   string
}

// nodePropsToNodeResponse converts a map from subject id to its properties to a NodeResponse proto.
func nodePropsToNodeResponse(propsBySubjectID map[string][]*Property) *pbv2.NodeResponse {
	nodeResponse := &pbv2.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}

	for subjectID, props := range propsBySubjectID {
		linkedGraph, ok := nodeResponse.Data[subjectID]
		if !ok {
			nodeResponse.Data[subjectID] = &pbv2.LinkedGraph{}
			linkedGraph = nodeResponse.Data[subjectID]
		}
		for _, prop := range props {
			linkedGraph.Properties = append(linkedGraph.Properties, prop.Predicate)
		}
	}
	return nodeResponse
}

// getOffset returns the offset for a given Spanner data source id.
func getOffset(nextToken, dataSourceID string) (int32, error) {
	if nextToken == "" {
		return 0, nil
	}

	pi, err := pagination.Decode(nextToken)
	if err != nil {
		return 0, err
	}

	for _, cursorGroup := range pi.GetCursorGroups() {
		for _, key := range cursorGroup.GetKeys() {
			if key == dataSourceID {
				if len(cursorGroup.GetCursors()) < 1 {
					return 0, fmt.Errorf("pagination info missing cursor for Spanner data source: %s", dataSourceID)
				}
				return cursorGroup.GetCursors()[0].GetOffset(), nil
			}
		}
	}

	return 0, nil
}

// getNextToken encodes next offset in a nextToken string.
func getNextToken(offset int32, dataSourceID string) (string, error) {
	pi := &pbv1.PaginationInfo{
		CursorGroups: []*pbv1.CursorGroup{{
			Keys: []string{dataSourceID},
			Cursors: []*pbv1.Cursor{{
				Offset: offset,
			}},
		}},
	}
	nextToken, err := util.EncodeProto(pi)
	if err != nil {
		return "", err
	}

	return nextToken, nil
}

// nodeEdgesToNodeResponse converts a map from subject id to its edges to a NodeResponse proto.
func nodeEdgesToNodeResponse(nodes []string, edgesBySubjectID map[string][]*Edge, id string, offset int32) (*pbv2.NodeResponse, error) {
	nodeResponse := &pbv2.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}

	// Sort nodes to preserve order from Spanner.
	sort.Strings(nodes)

	rows := 0
	for _, subjectID := range nodes {
		edges, ok := edgesBySubjectID[subjectID]
		if !ok {
			nodeResponse.Data[subjectID] = &pbv2.LinkedGraph{}
			continue
		}

		rows += len(edges)

		// We requested PAGE_SIZE+1 rows,
		// so having this many rows indicates that we have at least one more request,
		// so generate nextToken.
		if rows == PAGE_SIZE+1 && nodeResponse.NextToken == "" {
			edges = edges[:len(edges)-1]
			nextToken, err := getNextToken(offset+PAGE_SIZE, id)
			if err != nil {
				return nil, err
			}
			nodeResponse.NextToken = nextToken
		}

		nodeResponse.Data[subjectID] = nodeEdgesToLinkedGraph(edges)
	}

	return nodeResponse, nil
}

// nodeEdgesToLinkedGraph converts an array of edges to a LinkedGraph proto.
// This method assumes all edges are from the same entity.
func nodeEdgesToLinkedGraph(edges []*Edge) *pbv2.LinkedGraph {
	linkedGraph := &pbv2.LinkedGraph{
		Arcs: make(map[string]*pbv2.Nodes),
	}

	for _, edge := range edges {
		nodes, ok := linkedGraph.Arcs[edge.Predicate]
		if !ok {
			nodes = &pbv2.Nodes{}
		}
		node := &pb.EntityInfo{
			Name:         edge.Name,
			Types:        edge.Types,
			Dcid:         edge.ObjectID,
			ProvenanceId: edge.Provenance,
			Value:        edge.ObjectValue,
		}
		nodes.Nodes = append(nodes.Nodes, node)

		linkedGraph.Arcs[edge.Predicate] = nodes
	}

	return linkedGraph
}

func selectFieldsToQueryOptions(selectFields []string) queryOptions {
	var qo queryOptions
	for _, field := range selectFields {
		if field == ENTITY {
			qo.entity = true
		} else if field == VARIABLE {
			qo.variable = true
		} else if field == DATE {
			qo.date = true
		} else if field == VALUE {
			qo.value = true
		} else if field == FACET {
			qo.facet = true
		}
	}
	return qo
}

// Whether to return all observations in the Observation response.
func queryObs(qo *queryOptions) bool {
	return qo.date && qo.value
}

func filterObservationsByDateAndFacet(observations []*Observation, date string, filter *pbv2.FacetFilter) []*Observation {
	var filtered []*Observation
	for _, observation := range observations {
		observation.Observations.FilterByDate(date)
		facet := observationToFacet(observation)
		if len(observation.Observations.Observations) > 0 && util.ShouldIncludeFacet(filter, facet) {
			filtered = append(filtered, observation)
		}
	}
	return filtered
}

func observationsToObservationResponse(req *pbv2.ObservationRequest, observations []*Observation) *pbv2.ObservationResponse {
	// The select options are handled separately since each has a different behavior in V2.
	// This includes:
	// - Whether to include requested entities that are missing data
	// - Whether to merge responses for entity expressions
	// For now, V3 will match the behavior of V2 to preserve backward compatibility and allow datasource merging.
	// TODO: Unify these responses more.
	qo := selectFieldsToQueryOptions(req.Select)
	if queryObs(&qo) {
		// Returns FacetObservations with PointStats.
		return obsToObsResponse(req, observations)
	} else if qo.facet {
		// Returns FacetObservations without PointStats.
		return obsToFacetResponse(req, observations)
	} else {
		// Returns variable and entities with data.
		return obsToExistenceResponse(req, observations)
	}
}

func newObservationResponse(variables []string) *pbv2.ObservationResponse {
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	for _, variable := range variables {
		result.ByVariable[variable] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{},
		}
	}
	return result
}

func groupObservationsByVariableAndEntity(observations []*Observation) map[variableEntity][]*Observation {
	result := map[variableEntity][]*Observation{}

	for _, obs := range observations {
		variableEntity := variableEntity{
			variable: obs.VariableMeasured,
			entity:   obs.ObservationAbout,
		}
		if result[variableEntity] == nil {
			result[variableEntity] = []*Observation{}
		}
		result[variableEntity] = append(result[variableEntity], obs)
	}

	return result
}

func generateObsResponse(variables []string, observations []*Observation, includeObs bool) *pbv2.ObservationResponse {
	response := newObservationResponse(variables)

	variableEntityObs := groupObservationsByVariableAndEntity(observations)
	for variableEntity, obs := range variableEntityObs {
		orderedFacets, facets := observationsToOrderedFacets(obs, includeObs)
		response.ByVariable[variableEntity.variable].ByEntity[variableEntity.entity] = &pbv2.EntityObservation{
			OrderedFacets: orderedFacets,
		}
		for facetId, facet := range facets {
			response.Facets[facetId] = facet
		}
	}

	return response
}

// The rows are sorted when returned from Spanner,  so the entities will be in order.
func getDistinctEntities(observations []*Observation) []string {
	entities := []string{}
	entitySet := map[string]bool{}
	for _, obs := range observations {
		entity := obs.ObservationAbout
		_, ok := entitySet[entity]
		if !ok {
			entities = append(entities, entity)
			entitySet[entity] = true
		}
	}
	return entities
}

func mergeEntityOrderedFacets(byEntity map[string]*pbv2.EntityObservation, childPlaces []string) []*pbv2.FacetObservation {
	// Reuse merging logic from ContainedInFacet for consistency.
	result := []*pbv2.FacetObservation{}

	seenFacet := map[string]*pbv2.FacetObservation{}
	orderedFacetId := []string{}
	for _, entity := range childPlaces {
		if facetData, ok := byEntity[entity]; ok {
			for _, item := range facetData.OrderedFacets {
				if facetObs, ok := seenFacet[item.FacetId]; ok {
					facetObs.ObsCount += item.ObsCount
					if item.EarliestDate < facetObs.EarliestDate {
						facetObs.EarliestDate = item.EarliestDate
					}
					if item.LatestDate > facetObs.LatestDate {
						facetObs.LatestDate = item.LatestDate
					}
				} else {
					orderedFacetId = append(orderedFacetId, item.FacetId)
					seenFacet[item.FacetId] = item
				}
			}
		}
	}

	for _, facetId := range orderedFacetId {
		result = append(result, seenFacet[facetId])
	}

	return result
}

func obsToObsResponse(req *pbv2.ObservationRequest, observations []*Observation) *pbv2.ObservationResponse {
	response := generateObsResponse(req.Variable.Dcids, observations, true /*includeObs*/)

	// Attach all requested entity dcids to response.
	if len(req.Entity.Dcids) > 0 {
		for _, variableObs := range response.ByVariable {
			for _, entity := range req.Entity.Dcids {
				_, ok := variableObs.ByEntity[entity]
				if !ok {
					variableObs.ByEntity[entity] = &pbv2.EntityObservation{}
				}
			}
		}
	}

	return response
}

func obsToFacetResponse(req *pbv2.ObservationRequest, observations []*Observation) *pbv2.ObservationResponse {
	response := generateObsResponse(req.Variable.Dcids, observations, false /*includeObs*/)

	if len(req.Entity.Dcids) > 0 {
		return response
	}

	// Merge child places for entity expression.
	mergedResponse := newObservationResponse(req.Variable.Dcids)
	mergedResponse.Facets = response.Facets
	childPlaces := getDistinctEntities(observations)
	for variable, variableObs := range mergedResponse.ByVariable {
		variableObs.ByEntity[ENTITY_PLACEHOLDER] = &pbv2.EntityObservation{}
		initialVariableObs, ok := response.ByVariable[variable]
		if ok {
			variableObs.ByEntity[ENTITY_PLACEHOLDER].OrderedFacets = mergeEntityOrderedFacets(initialVariableObs.ByEntity, childPlaces)
		}
	}
	return mergedResponse
}

func obsToExistenceResponse(req *pbv2.ObservationRequest, observations []*Observation) *pbv2.ObservationResponse {
	response := newObservationResponse(req.Variable.Dcids)
	for _, obs := range observations {
		response.ByVariable[obs.VariableMeasured].ByEntity[obs.ObservationAbout] = &pbv2.EntityObservation{}
	}
	return response
}

func observationsToOrderedFacets(observations []*Observation, includeObs bool) ([]*pbv2.FacetObservation, map[string]*pb.Facet) {
	facets := map[string]*pb.Facet{}
	placeVariableFacets := []*pb.PlaceVariableFacet{}
	facetIdToFacetObs := map[string]*pbv2.FacetObservation{}
	for _, obs := range observations {
		pvf, facetObs := observationToFacetObservation(obs, includeObs)

		// Skip rows with no time series.
		if pvf.ObsCount == 0 {
			continue
		}

		placeVariableFacets = append(placeVariableFacets, pvf)
		facetIdToFacetObs[facetObs.FacetId] = facetObs
		facets[facetObs.FacetId] = pvf.Facet
	}

	// Rank FacetObservations.
	orderedFacets := []*pbv2.FacetObservation{}
	sort.Sort(ranking.FacetByRank(placeVariableFacets))
	for _, pvf := range placeVariableFacets {
		facetId := util.GetFacetID(pvf.Facet)
		orderedFacets = append(orderedFacets, facetIdToFacetObs[facetId])
	}

	return orderedFacets, facets
}

func observationToFacetObservation(observation *Observation, includeObs bool) (*pb.PlaceVariableFacet, *pbv2.FacetObservation) {
	facet := observationToFacet(observation)

	var observations []*pb.PointStat

	for _, dateValue := range observation.Observations.Observations {
		pointStat, err := dateValueToPointStat(dateValue)

		// Skip observations with non-numeric values.
		if err != nil {
			log.Printf("Error decoding PointStat for variable (%v) and entity (%v): %v", observation.VariableMeasured, observation.ObservationAbout, err)
			continue
		}

		observations = append(observations, pointStat)
	}

	facetObservation := &pbv2.FacetObservation{
		FacetId:      util.GetFacetID(facet),
		ObsCount:     *proto.Int32(int32(len(observations))),
		EarliestDate: observations[0].Date,
		LatestDate:   observations[len(observations)-1].Date,
	}

	if includeObs {
		facetObservation.Observations = observations
	}

	placeVariableFacet := &pb.PlaceVariableFacet{
		Facet:        facet,
		ObsCount:     facetObservation.ObsCount,
		EarliestDate: facetObservation.EarliestDate,
		LatestDate:   facetObservation.LatestDate,
	}

	return placeVariableFacet, facetObservation
}

func observationToFacet(observation *Observation) *pb.Facet {
	facet := pb.Facet{
		ImportName:        observation.ImportName,
		ProvenanceUrl:     observation.ProvenanceURL,
		MeasurementMethod: observation.MeasurementMethod,
		ObservationPeriod: observation.ObservationPeriod,
		ScalingFactor:     observation.ScalingFactor,
		Unit:              observation.Unit,
	}
	return &facet
}

func dateValueToPointStat(dateValue *DateValue) (*pb.PointStat, error) {
	floatVal, err := strconv.ParseFloat(dateValue.Value, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode TimeSeries float value: (%v) for date: (%v)", floatVal, dateValue.Date)
	}
	return &pb.PointStat{
		Date:  dateValue.Date,
		Value: proto.Float64(floatVal),
	}, nil
}

func searchNodesToNodeSearchResponse(nodes []*SearchNode) *pbv2.NodeSearchResponse {
	response := &pbv2.NodeSearchResponse{}

	for _, node := range nodes {
		response.Results = append(response.Results, searchNodeToNodeSearchResult(node))
	}

	return response
}

func searchNodeToNodeSearchResult(node *SearchNode) *pbv2.NodeSearchResult {
	return &pbv2.NodeSearchResult{
		Node: &pb.EntityInfo{
			Dcid:  node.SubjectID,
			Name:  node.Name,
			Types: node.Types,
		},
		Match: &pb.PropertyValue{
			Property: node.MatchedPredicate,
			Value:    node.MatchedObjectValue,
		},
	}
}
