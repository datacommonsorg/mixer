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
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/protobuf/proto"
)

const (
	// Indicates that all properties should be returned.
	WILDCARD = "*"
	// Indicates that recursive property paths should be returned.
	CHAIN = "+"
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
func nodePropsToNodeResponse(propsBySubjectID map[string][]*Property, nodes []string) *pbv3.NodeResponse {
	nodeResponse := &pbv3.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}

	for _, subjectID := range nodes {
		linkedGraph := &pbv2.LinkedGraph{}
		nodeResponse.Data[subjectID] = linkedGraph

		props, ok := propsBySubjectID[subjectID]
		if !ok {
			continue
		}
		for _, prop := range props {
			linkedGraph.Properties = append(linkedGraph.Properties, prop.Predicate)
		}
	}

	return nodeResponse
}

// nodeEdgesToNodeResponse converts a map of subject->predicate->Edges to a NodeResponse proto.
func nodeEdgesToNodeResponse(edges map[string]map[string][]*Edge, nodes []string, bracketProps []string) *pbv3.NodeResponse {
	nodeResponse := &pbv3.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}

	for _, subjectID := range nodes {
		predicateToEdges, ok := edges[subjectID]
		if !ok {
			nodeResponse.Data[subjectID] = &pbv2.LinkedGraph{
				Arcs: make(map[string]*pbv2.Nodes),
			}
		} else {
			nodeResponse.Data[subjectID] = nodeEdgesToLinkedGraph(predicateToEdges, bracketProps)
		}
	}

	return nodeResponse
}

// nodeEdgesToLinkedGraph converts an array of edges to a LinkedGraph proto.
// This method assumes all edges are from the same entity.
func nodeEdgesToLinkedGraph(predicateToEdges map[string][]*Edge, bracketProps []string) *pbv2.LinkedGraph {
	linkedGraph := &pbv2.LinkedGraph{
		Arcs: make(map[string]*pbv2.Nodes),
	}

	// If multiple properties are explicitly requested, return result in the same order.
	// Otherwise, default to alphabetical order.
	if len(bracketProps) > 0 {
		for _, predicate := range bracketProps {
			edges, ok := predicateToEdges[predicate]
			if !ok {
				continue
			}
			addEdgesToLinkedGraph(linkedGraph, predicate, edges)
		}
	} else {
		for predicate, edges := range predicateToEdges {
			addEdgesToLinkedGraph(linkedGraph, predicate, edges)
		}
	}
	return linkedGraph
}

func addEdgesToLinkedGraph(linkedGraph *pbv2.LinkedGraph, predicate string, edges []*Edge) {
	nodes := &pbv2.Nodes{
		Nodes: []*pb.EntityInfo{},
	}
	for _, edge := range edges {
		node := &pb.EntityInfo{
			Name:         edge.Name,
			Types:        edge.Types,
			Dcid:         edge.ObjectID,
			ProvenanceId: edge.Provenance,
			Value:        edge.ObjectValue,
		}
		nodes.Nodes = append(nodes.Nodes, node)
	}
	linkedGraph.Arcs[predicate] = nodes
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

func filterObservationsByDate(observations []*Observation, date string) []*Observation {
	// No filtering required if date is not specified.
	if date == "" {
		return observations
	}
	var filtered []*Observation
	for _, observation := range observations {
		observation.Observations.FilterByDate(date)
		if len(observation.Observations.Observations) > 0 {
			filtered = append(filtered, observation)
		}
	}
	return filtered
}


func observationsToObservationResponse(variables []string, observations []*Observation, queryObs, queryFacet bool) *v3.ObservationResponse {
	response := newObservationResponse(variables)
	for _, observation := range observations {
		variable, entity := observation.VariableMeasured, observation.ObservationAbout
		if response.ByVariable[variable].ByEntity[entity] == nil {
			response.ByVariable[variable].ByEntity[entity] = &v2.EntityObservation{}
		}

		// Existence check only returns variables and entities.
		if !queryObs && !queryFacet {
			continue
		}

		facetId, facet, facetObservation := observationToFacetObservation(observation, queryObs)
		response.ByVariable[variable].ByEntity[entity].OrderedFacets = append(
			response.ByVariable[variable].ByEntity[entity].OrderedFacets,
			facetObservation,
		)
		response.Facets[facetId] = facet
	}
	return response
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

func observationToFacetObservation(observation *Observation, queryObs bool) (string, *pb.Facet, *v2.FacetObservation) {
	facetId, facet := observationToFacet(observation)

	var observations []*pb.PointStat

	for _, dateValue := range observation.Observations.Observations {
		observations = append(observations, dateValueToPointStat(dateValue))
	}

	facetObservation := &v2.FacetObservation{
		FacetId:      facetId,
		ObsCount:     *proto.Int32(int32(len(observations))),
		EarliestDate: observations[0].Date,
		LatestDate:   observations[len(observations)-1].Date,
	}

	// Return the full response.
	if queryObs {
		facetObservation.Observations = observations
	}

	return facetId, facet, facetObservation
}

func observationToFacet(observation *Observation) (string, *pb.Facet) {
	facet := pb.Facet{
		ImportName:        observation.ImportName,
		ProvenanceUrl:     observation.ProvenanceURL,
		MeasurementMethod: observation.MeasurementMethod,
		ObservationPeriod: observation.ObservationPeriod,
		ScalingFactor:     observation.ScalingFactor,
		Unit:              observation.Unit,
	}
	return util.GetFacetID(&facet), &facet
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
