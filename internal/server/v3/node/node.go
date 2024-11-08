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

package node

import (
	"context"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/util"
)

func mergeLinkedGraph(
	mainData, auxData map[string]*pbv2.LinkedGraph,
) map[string]*pbv2.LinkedGraph {
	for dcid, linkedGraph := range auxData {
		if mainData == nil {
			mainData = map[string]*pbv2.LinkedGraph{}
		}
		if _, ok := mainData[dcid]; !ok || mainData[dcid].GetArcs() == nil {
			mainData[dcid] = linkedGraph
			continue
		}
		mainArcs := mainData[dcid].GetArcs()

		for prop, nodes := range linkedGraph.GetArcs() {
			if _, ok := mainArcs[prop]; !ok || len(mainArcs[prop].GetNodes()) == 0 {
				mainData[dcid].Arcs[prop] = nodes
				continue
			}
			dcidSet := map[string]struct{}{}
			valueSet := map[string]struct{}{}
			for _, n := range mainArcs[prop].Nodes {
				if n.Dcid != "" {
					dcidSet[n.Dcid] = struct{}{}
				} else {
					valueSet[n.Value] = struct{}{}
				}
			}
			for _, node := range nodes.Nodes {
				if node.Dcid != "" {
					if _, ok := dcidSet[node.Dcid]; !ok {
						mainArcs[prop].Nodes = append(mainArcs[prop].Nodes, node)
					}
				}
				if node.Value != "" {
					if _, ok := valueSet[node.Value]; !ok {
						mainArcs[prop].Nodes = append(mainArcs[prop].Nodes, node)
					}
				}
			}
		}
	}
	return mainData
}

// Merges two V3 NodeResponses.
func mergeNodeResponse(main, aux *pbv3.NodeResponse) (*pbv3.NodeResponse, error) {
	if aux == nil {
		return main, nil
	}
	if main == nil {
		if aux.GetNextToken() != "" {
			remotePaginationInfo, err := pagination.Decode(aux.GetNextToken())
			if err != nil {
				return nil, err
			}
			updatedPaginationInfo := &pbv1.PaginationInfo{
				RemotePaginationInfo: remotePaginationInfo,
			}
			updatedNextToken, err := util.EncodeProto(updatedPaginationInfo)
			if err != nil {
				return nil, err
			}
			aux.NextToken = updatedNextToken
		}
		return aux, nil
	}
	main.Data = mergeLinkedGraph(main.GetData(), aux.GetData())
	// Merge |next_token|.
	resPaginationInfo := &pbv1.PaginationInfo{}
	if main.GetNextToken() != "" {
		mainPaginationInfo, err := pagination.Decode(main.GetNextToken())
		if err != nil {
			return nil, err
		}
		resPaginationInfo = mainPaginationInfo
	}
	if aux.GetNextToken() != "" {
		auxPaginationInfo, err := pagination.Decode(aux.GetNextToken())
		if err != nil {
			return nil, err
		}
		resPaginationInfo.RemotePaginationInfo = auxPaginationInfo
	}
	if main.GetNextToken() != "" || aux.GetNextToken() != "" {
		resNextToken, err := util.EncodeProto(resPaginationInfo)
		if err != nil {
			return nil, err
		}
		main.NextToken = resNextToken
	}

	return main, nil
}

// Merges multiple V3 NodeResponses.
// Assumes the responses are in order of prioirty.
func mergeNode(
	allResp []*pbv3.NodeResponse,
) (*pbv3.NodeResponse, error) {
	if len(allResp) == 0 {
		return nil, nil
	}
	prev := allResp[0]
	for i := 1; i < len(allResp); i++ {
		cur, err := mergeNodeResponse(prev, allResp[i])
		if err != nil {
			return nil, err
		}
		prev = cur
	}
	return prev, nil
}

func V3NodeInternal(
	ctx context.Context,
	in *pbv3.NodeRequest,
	dataSources *datasource.DataSources,
) (*pbv3.NodeResponse, error) {
	allResp := []*pbv3.NodeResponse{}
	for _, source := range dataSources.Sources {
		resp, err := (*source).Node(ctx, in)
		if err != nil {
			return nil, err
		}
		allResp = append(allResp, resp)
	}
	return mergeNode(allResp)
}
