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
	v2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	v3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/protobuf/proto"
)

// nodePropsToNodeResponse converts a slice of properties to a NodeResponse proto.
func nodePropsToNodeResponse(props []*Property) *v3.NodeResponse {
	nodeResponse := &v3.NodeResponse{
		Data: make(map[string]*v2.LinkedGraph),
	}

	for _, prop := range props {
		linkedGraph, ok := nodeResponse.Data[prop.SubjectID]
		if !ok {
			linkedGraph = &v2.LinkedGraph{}
			nodeResponse.Data[prop.SubjectID] = linkedGraph
		}
		linkedGraph.Properties = append(linkedGraph.Properties, prop.Predicate)
	}

	return nodeResponse
}

// nodeEdgesToNodeResponse converts a map from subject id to its edges to a NodeResponse proto.
func nodeEdgesToNodeResponse(edgesBySubjectID map[string][]*Edge) *v3.NodeResponse {
	nodeResponse := &v3.NodeResponse{
		Data: make(map[string]*v2.LinkedGraph),
	}

	for subjectID, edges := range edgesBySubjectID {
		nodeResponse.Data[subjectID] = nodeEdgesToLinkedGraph(edges)
	}

	return nodeResponse
}

// nodeEdgesToLinkedGraph converts an array of edges to a LinkedGraph proto.
// This method assumes all edges are from the same entity.
func nodeEdgesToLinkedGraph(edges []*Edge) *v2.LinkedGraph {
	linkedGraph := &v2.LinkedGraph{
		Arcs: make(map[string]*v2.Nodes),
	}

	for _, edge := range edges {
		nodes, ok := linkedGraph.Arcs[edge.Predicate]
		if !ok {
			nodes = &v2.Nodes{
				Nodes: []*pb.EntityInfo{},
			}
		}
		node := &pb.EntityInfo{
			Name:         edge.Name,
			Types:        edge.Types,
			ProvenanceId: edge.Provenance,
		}
		if edge.ObjectValue != "" {
			node.Value = edge.ObjectValue
		} else {
			node.Dcid = edge.ObjectID
		}
		nodes.Nodes = append(nodes.Nodes, node)

		linkedGraph.Arcs[edge.Predicate] = nodes
	}

	return linkedGraph
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

func observationsToObservationResponse(variables []string, observations []*Observation) *v3.ObservationResponse {
	response := newObservationResponse(variables)
	for _, observation := range observations {
		facetId, facet, facetObservation := observationToFacetObservation(observation)
		variable, entity := observation.VariableMeasured, observation.ObservationAbout
		if response.ByVariable[variable].ByEntity[entity] == nil {
			response.ByVariable[variable].ByEntity[entity] = &v2.EntityObservation{
				OrderedFacets: []*v2.FacetObservation{},
			}
		}
		response.ByVariable[variable].ByEntity[entity].OrderedFacets = append(
			response.ByVariable[variable].ByEntity[entity].OrderedFacets,
			facetObservation,
		)
		response.Facets[facetId] = facet
	}
	return response
}

func newObservationResponse(variables []string) *v3.ObservationResponse {
	result := &v3.ObservationResponse{
		ByVariable: map[string]*v2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	for _, variable := range variables {
		result.ByVariable[variable] = &v2.VariableObservation{
			ByEntity: map[string]*v2.EntityObservation{},
		}
	}
	return result
}

func observationToFacetObservation(observation *Observation) (string, *pb.Facet, *v2.FacetObservation) {
	facetId, facet := observationToFacet(observation)

	var observations []*pb.PointStat

	for _, dateValue := range observation.Observations.Observations {
		observations = append(observations, dateValueToPointStat(dateValue))
	}

	facetObservation := &v2.FacetObservation{
		Observations: observations,
		ObsCount:     *proto.Int32(int32(len(observations))),
		EarliestDate: observations[0].Date,
		LatestDate:   observations[len(observations)-1].Date,
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
