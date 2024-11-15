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
	"github.com/datacommonsorg/mixer/internal/proto"
	v2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	v3 "github.com/datacommonsorg/mixer/internal/proto/v3"
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
				Nodes: []*proto.EntityInfo{},
			}
		}
		node := &proto.EntityInfo{
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
