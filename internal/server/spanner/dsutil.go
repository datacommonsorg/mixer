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
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
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

// nodePropsToNodeResponse converts a map from subject id to its properties to a NodeResponse proto.
func nodePropsToNodeResponse(propsBySubjectID map[string][]*Property) *pbv3.NodeResponse {
	nodeResponse := &pbv3.NodeResponse{
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

// nodeEdgesToNodeResponse converts a map from subject id to its edges to a NodeResponse proto.
func nodeEdgesToNodeResponse(edgesBySubjectID map[string][]*Edge) *pbv3.NodeResponse {
	nodeResponse := &pbv3.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}

	for subjectID, edges := range edgesBySubjectID {
		nodeResponse.Data[subjectID] = nodeEdgesToLinkedGraph(edges)
	}

	return nodeResponse
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

func observationsToObservationResponse(req *pbv3.ObservationRequest, observations []*Observation) *pbv3.ObservationResponse {
	// The select options are handled separately since each has a different behavior in V2.
	// For now, V3 will match the behavior of V2 to preserve backward compatibility and allow datasource merging.
	// TODO: Unify these responses more.
	qo := selectFieldsToQueryOptions(req.Select)
	if queryObs(&qo) {
		return obsToObsResponse(req, observations)
	} else if qo.facet {
		return obsToFacetResponse(req, observations)
	} else {
		return obsToExistenceResponse(req, observations)
	}
}

func newObservationResponse(variables []string) *pbv3.ObservationResponse {
	result := &pbv3.ObservationResponse{
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

func groupObservationsByVariableAndEntity(observations []*Observation) map[string]map[string][]*Observation {
	result := map[string]map[string][]*Observation{}

	for _, obs := range observations {
		variable, entity := obs.VariableMeasured, obs.ObservationAbout
		if result[variable] == nil {
			result[variable] = map[string][]*Observation{}
		}
		if result[variable][entity] == nil {
			result[variable][entity] = []*Observation{}
		}
		result[variable][entity] = append(result[variable][entity], obs)
	}

	return result
}

func generateObsResponse(variables []string, observations []*Observation, includeObs bool) *pbv3.ObservationResponse {
	response := newObservationResponse(variables)

	variableEntityObs := groupObservationsByVariableAndEntity(observations)
	for variable, entityObs := range variableEntityObs {
		for entity, obs := range entityObs {
			orderedFacets, facets := observationsToOrderedFacets(obs, includeObs)
			response.ByVariable[variable].ByEntity[entity] = &pbv2.EntityObservation{
				OrderedFacets: orderedFacets,
			}
			for facetId, facet := range facets {
				response.Facets[facetId] = facet
			}
		}
	}

	return response
}

// The rows are sorted when returned from Spanner,  so the child places will be in order.
func getChildPlaces(observations []*Observation) []string {
	childPlaces := []string{}
	childPlacesSet := map[string]bool{}
	for _, obs := range observations {
		entity := obs.ObservationAbout
		_, ok := childPlacesSet[entity]
		if !ok {
			childPlaces = append(childPlaces, entity)
			childPlacesSet[entity] = true
		}
	}
	return childPlaces
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

func obsToObsResponse(req *pbv3.ObservationRequest, observations []*Observation) *pbv3.ObservationResponse {
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

func obsToFacetResponse(req *pbv3.ObservationRequest, observations []*Observation) *pbv3.ObservationResponse {
	response := generateObsResponse(req.Variable.Dcids, observations, false /*includeObs*/)

	if len(req.Entity.Dcids) > 0 {
		return response
	}

	// Merge child places for entity expression.
	mergedResponse := newObservationResponse(req.Variable.Dcids)
	mergedResponse.Facets = response.Facets
	childPlaces := getChildPlaces(observations)
	for variable, variableObs := range mergedResponse.ByVariable {
		variableObs.ByEntity[ENTITY_PLACEHOLDER] = &pbv2.EntityObservation{}
		initialVariableObs, ok := response.ByVariable[variable]
		if ok {
			variableObs.ByEntity[ENTITY_PLACEHOLDER].OrderedFacets = mergeEntityOrderedFacets(initialVariableObs.ByEntity, childPlaces)
		}
	}
	return mergedResponse
}

func obsToExistenceResponse(req *pbv3.ObservationRequest, observations []*Observation) *pbv3.ObservationResponse {
	// This is likely a bug in V2, but will be kept for now to not break existing behavior.
	// TODO: Return actual response.
	if req.Entity.Expression != "" {
		return &pbv3.ObservationResponse{}
	}

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
		observations = append(observations, dateValueToPointStat(dateValue))
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

func dateValueToPointStat(dateValue *DateValue) *pb.PointStat {
	return &pb.PointStat{
		Date:  dateValue.Date,
		Value: proto.Float64(dateValue.Value),
	}
}

func searchNodesToNodeSearchResponse(nodes []*SearchNode) *pbv3.NodeSearchResponse {
	response := &pbv3.NodeSearchResponse{}

	for _, node := range nodes {
		response.Nodes = append(response.Nodes, searchNodeToEntityInfo(node))
	}

	return response
}

func searchNodeToEntityInfo(node *SearchNode) *pb.EntityInfo {
	return &pb.EntityInfo{
		Dcid:  node.SubjectID,
		Name:  node.Name,
		Types: node.Types,
	}
}
