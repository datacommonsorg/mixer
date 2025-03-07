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
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

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
