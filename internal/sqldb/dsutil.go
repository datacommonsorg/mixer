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

// Utility functions used by the SQLDataSource.

package sqldb

import (
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
)

const (
	namePredicate   = "name"
	typeOfPredicate = "typeOf"
	subjectIdColumn = "subject_id"
	objectIdColumn  = "object_id"
	defaultType     = "Thing"
)

type entityInfo struct {
	Name string
	Type string
}

func addNodeResponseNode(nodeResponse *pbv2.NodeResponse, nodeTriple *Triple, nodeDcid string, entityDcid string, entityInfo *entityInfo) {
	predicate, objectValue := nodeTriple.Predicate, nodeTriple.ObjectValue
	if _, ok := nodeResponse.Data[nodeDcid]; !ok {
		nodeResponse.Data[nodeDcid] = &pbv2.LinkedGraph{
			Arcs: make(map[string]*pbv2.Nodes),
		}
	}
	graph := nodeResponse.Data[nodeDcid]
	if _, ok := graph.Arcs[predicate]; !ok {
		graph.Arcs[predicate] = &pbv2.Nodes{Nodes: []*pb.EntityInfo{}}
	}
	graph.Arcs[predicate].Nodes = append(graph.Arcs[predicate].Nodes, &pb.EntityInfo{
		Dcid:  entityDcid,
		Value: objectValue,
		Types: []string{entityInfo.Type},
		Name:  entityInfo.Name,
	})
}

func triplesToNodeResponse(nodeTriples []*Triple, entityInfoTriples []*Triple, direction string) *pbv2.NodeResponse {
	nodeResponse := &pbv2.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}

	entityInfos := toEntityInfos(entityInfoTriples)
	for _, nodeTriple := range nodeTriples {
		var nodeDcid, entityDcid string
		if direction == util.DirectionOut {
			nodeDcid = nodeTriple.SubjectID
			entityDcid = nodeTriple.ObjectID
		} else {
			nodeDcid = nodeTriple.ObjectID
			entityDcid = nodeTriple.SubjectID
		}
		entityInfo, ok := entityInfos[entityDcid]
		if !ok {
			entityInfo = newEntityInfo()
		}
		addNodeResponseNode(nodeResponse, nodeTriple, nodeDcid, entityDcid, entityInfo)
	}

	return nodeResponse
}

// toEntityInfos converts entity info triples (name and type triples) to entity info (name and type) objects.
func toEntityInfos(entityInfoTriples []*Triple) map[string]*entityInfo {
	entityInfos := map[string]*entityInfo{}

	for _, row := range entityInfoTriples {
		entityInfo, ok := entityInfos[row.SubjectID]
		if !ok {
			entityInfo = newEntityInfo()
			entityInfos[row.SubjectID] = entityInfo
		}
		if row.Predicate == namePredicate {
			entityInfo.Name = row.ObjectValue
		} else if row.Predicate == typeOfPredicate {
			entityInfo.Type = row.ObjectID
		}
	}

	return entityInfos
}

// collectDcids collects all subject and object dcids from a list of triples.
func collectDcids(triples []*Triple) []string {
	dcidSet := map[string]struct{}{}
	for _, t := range triples {
		dcidSet[t.SubjectID] = struct{}{}
		if t.ObjectID != "" {
			dcidSet[t.ObjectID] = struct{}{}
		}
	}
	dcids := []string{}
	for dcid := range dcidSet {
		dcids = append(dcids, dcid)
	}
	return dcids
}

func newEntityInfo() *entityInfo {
	return &entityInfo{Type: defaultType}
}

func nodePredicatesToProperties(nodePredicates []*NodePredicate) []string {
	properties := []string{}
	for _, nodePredicate := range nodePredicates {
		properties = append(properties, nodePredicate.Predicate)
	}
	return properties
}

// nodePredicatesToNodeResponse converts a list of NodePredicates to a NodeResponse proto.
func nodePredicatesToNodeResponse(nodePredicates []*NodePredicate) *pbv2.NodeResponse {
	nodeResponse := &pbv2.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}

	for _, nodePredicate := range nodePredicates {
		node, predicate := nodePredicate.Node, nodePredicate.Predicate
		linkedGraph, ok := nodeResponse.Data[node]
		if !ok {
			nodeResponse.Data[node] = &pbv2.LinkedGraph{}
			linkedGraph = nodeResponse.Data[node]
		}
		linkedGraph.Properties = append(linkedGraph.Properties, predicate)
	}
	return nodeResponse
}
