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

// Package merger provides function to merge V2 API ressponses.

// This package cares about the order of the input responses. The first argument
// is always prefered and put first.

package merger

import (
	"sort"

	"github.com/datacommonsorg/mixer/internal/proto"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/util"
)

// MergeResolve merges two V2 resolve responses.
func MergeResolve(main, aux *pbv2.ResolveResponse) *pbv2.ResolveResponse {
	if main == nil {
		return aux
	}
	if aux == nil {
		return main
	}
	// Change aux list into map for easy lookup
	auxStore := map[string]*pbv2.ResolveResponse_Entity{}
	for _, e := range aux.GetEntities() {
		node := e.Node
		auxStore[node] = e
	}
	// Merge common entities from aux into main
	for _, e := range main.GetEntities() {
		node := e.Node
		if auxEntity, ok := auxStore[node]; ok {
			existCandidates := map[string]struct{}{}
			for _, c := range e.Candidates {
				existCandidates[c.Dcid] = struct{}{}
			}
			for _, c := range auxEntity.Candidates {
				if _, ok := existCandidates[c.Dcid]; !ok {
					e.Candidates = append(e.Candidates, c)
				}
			}
			delete(auxStore, node)
		}
	}
	// Add aux entities that are not in main
	for _, e := range aux.Entities {
		if _, ok := auxStore[e.Node]; ok {
			main.Entities = append(main.Entities, e)
		}
	}
	return main
}

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
			mainNodes := mainArcs[prop].Nodes
			for _, n := range mainNodes {
				if n.Dcid != "" {
					dcidSet[n.Dcid] = struct{}{}
				} else {
					valueSet[n.Value] = struct{}{}
				}
			}
			for _, node := range nodes.Nodes {
				if node.Dcid != "" {
					if _, ok := dcidSet[node.Dcid]; !ok {
						mainNodes = append(mainNodes, node)
					}
				}
				if node.Value != "" {
					if _, ok := valueSet[node.Value]; !ok {
						mainNodes = append(mainNodes, node)
					}
				}
			}
		}
	}
	return mainData
}

// MergeNode merges two V2 node responses.
//
// NOTE: Make sure the order of the two arguments, it's important for merging
// |next_token|. When mergering local and remote mixer response, the remote
// response is always put as the second argument (aux)

// TODO: Add more unit tests with real data.
func MergeNode(main, aux *pbv2.NodeResponse) (*pbv2.NodeResponse, error) {
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

// MergeEvent merges two V2 event responses.
// If both main and aux have event with the same DCID, then aux event is not
// used. Otherwise event from aux is appended after main.
func MergeEvent(main, aux *pbv2.EventResponse) *pbv2.EventResponse {
	if main == nil {
		return aux
	}
	if aux == nil {
		return main
	}
	// Collect all event dcid and dates from main
	ids := map[string]struct{}{}
	dates := map[string]struct{}{}
	for _, ev := range main.GetEventCollection().GetEvents() {
		ids[ev.Dcid] = struct{}{}
	}
	for _, d := range main.GetEventCollectionDate().GetDates() {
		dates[d] = struct{}{}
	}
	// Merge aux
	mainCollection := main.GetEventCollection()
	auxCollection := aux.GetEventCollection()
	for _, ev := range auxCollection.GetEvents() {
		if _, ok := ids[ev.Dcid]; ok {
			continue
		}
		mainCollection.Events = append(mainCollection.Events, ev)
		mainCollection.ProvenanceInfo[ev.ProvenanceId] = auxCollection.ProvenanceInfo[ev.ProvenanceId]
	}
	if main.EventCollectionDate == nil {
		main.EventCollectionDate = &pbv1.EventCollectionDate{}
	}
	for _, d := range aux.GetEventCollectionDate().GetDates() {
		if _, ok := dates[d]; ok {
			continue
		}
		main.EventCollectionDate.Dates = append(main.EventCollectionDate.Dates, d)
	}
	if main.EventCollectionDate != nil {
		sort.Strings(main.EventCollectionDate.Dates)
	}
	return main
}

// MergeObservation merges multiple V2 observation responses.
func MergeObservation(main *pbv2.ObservationResponse, aux []*pbv2.ObservationResponse) *pbv2.ObservationResponse {
	if main == nil {
		if len(aux) > 1 {
			return MergeObservation(aux[0], aux[1:])
		} else {
			return aux[0]
		}
	}
	if len(aux) == 0 {
		return main
	}
	if aux[0] == nil {
		return main
	}
	for _, auxData := range aux {
		for v, vData := range auxData.ByVariable {
			if main.ByVariable == nil {
				main.ByVariable = map[string]*pbv2.VariableObservation{}
			}
			if _, ok := main.ByVariable[v]; !ok {
				main.ByVariable[v] = &pbv2.VariableObservation{
					ByEntity: map[string]*pbv2.EntityObservation{},
				}
			}
			if main.ByVariable[v].ByEntity == nil {
				main.ByVariable[v].ByEntity = map[string]*pbv2.EntityObservation{}
			}
			for e, eData := range vData.ByEntity {
				if _, ok := main.ByVariable[v].ByEntity[e]; !ok {
					main.ByVariable[v].ByEntity[e] = &pbv2.EntityObservation{
						OrderedFacets: []*pbv2.FacetObservation{},
					}
				}
				main.ByVariable[v].ByEntity[e].OrderedFacets = append(
					main.ByVariable[v].ByEntity[e].OrderedFacets,
					eData.OrderedFacets...,
				)
			}
		}
	}
	if main.Facets == nil {
		main.Facets = map[string]*proto.Facet{}
	}
	for _, auxData := range aux {
		for facetID, facet := range auxData.Facets {
			main.Facets[facetID] = facet
		}
	}
	return main
}

// MergeObservationDates merges two V1 observation-dates responses.
func MergeObservationDates(
	main, aux *pbv1.BulkObservationDatesLinkedResponse,
) *pbv1.BulkObservationDatesLinkedResponse {
	if main == nil {
		return aux
	}
	if aux == nil {
		return main
	}
	// Helper variable for merging
	mainVarIndex := map[string]int{}
	for idx, vData := range main.DatesByVariable {
		mainVarIndex[vData.Variable] = idx
	}
	// Merge aux into main
	for _, vData := range aux.DatesByVariable {
		if mainIdx, ok := mainVarIndex[vData.Variable]; ok {
			main.DatesByVariable[mainIdx].ObservationDates = append(
				main.DatesByVariable[mainIdx].ObservationDates,
				vData.ObservationDates...,
			)
		} else {
			main.DatesByVariable = append(main.DatesByVariable, vData)
		}
	}
	if main.Facets == nil {
		main.Facets = map[string]*proto.Facet{}
	}
	for facetID, facet := range aux.Facets {
		if _, ok := main.Facets[facetID]; !ok {
			main.Facets[facetID] = facet
		}
	}
	return main
}

// MergeStatVarSummary merges two StatVarSummary maps.
func MergeStatVarSummary(primary, secondary map[string]*proto.StatVarSummary) map[string]*proto.StatVarSummary {
	merged := map[string]*proto.StatVarSummary{}
	addInfos := func(in map[string]*proto.StatVarSummary) {
		for node, info := range in {
			if info != nil {
				// If the same SV is in multiple responses,
				// this will use the info from the first response.
				// TODO: Merge both infos.
				if _, ok := merged[node]; !ok {
					merged[node] = info
				}
			}
		}
	}
	addInfos(primary)
	addInfos(secondary)
	return merged
}

// MergeBulkVariableInfoResponse merges two BulkVariableInfoResponses.
func MergeBulkVariableInfoResponse(primary, secondary *pbv1.BulkVariableInfoResponse) *pbv1.BulkVariableInfoResponse {
	var primaryMap, secondaryMap map[string]*proto.StatVarSummary
	if primary != nil {
		primaryMap = toStatVarSummaryMap(primary.Data)
	}
	if secondary != nil {
		secondaryMap = toStatVarSummaryMap(secondary.Data)
	}

	mergedMap := MergeStatVarSummary(primaryMap, secondaryMap)

	merged := &pbv1.BulkVariableInfoResponse{
		Data: []*pbv1.VariableInfoResponse{},
	}
	for node, info := range mergedMap {
		merged.Data = append(merged.Data, &pbv1.VariableInfoResponse{
			Node: node,
			Info: info,
		})
	}

	sort.Slice(merged.Data, func(i, j int) bool {
		return merged.Data[i].Node < merged.Data[j].Node
	})

	return merged
}

func toStatVarSummaryMap(in []*pbv1.VariableInfoResponse) map[string]*proto.StatVarSummary {
	out := map[string]*proto.StatVarSummary{}
	for _, item := range in {
		if item.Info != nil {
			out[item.GetNode()] = item.Info
		}
	}
	return out
}

// Create map of SV to inputPropertyExpressions by merging local and remote responses.
func MergeFormulas(localResp map[string]map[string]map[string][]*pb.EntityInfo, remoteResp *pbv2.NodeResponse) (map[string][]string, error) {
	result := map[string][]string{}
	localResult := map[string]map[string]bool{}
	for _, props := range localResp {
		for _, outputProps := range props["outputProperty"] {
			for _, outputNode := range outputProps {
				for _, inputProps := range props["inputPropertyExpression"] {
					for _, inputNode := range inputProps {
						result[outputNode.Dcid] = append(result[outputNode.Dcid], inputNode.Value)
						if _, ok := localResult[outputNode.Dcid]; ok {
							localResult[outputNode.Dcid][inputNode.Value] = true
						} else {
							localResult[outputNode.Dcid] = map[string]bool{inputNode.Value: true}
						}
					}
				}
			}
		}
	}
	if remoteResp != nil {
		for _, props := range remoteResp.Data {
			for _, outputNode := range props.Arcs["outputProperty"].Nodes {
				for _, inputNode := range props.Arcs["inputPropertyExpression"].Nodes {
					// Don't duplicate local formulas.
					if _, ok := localResult[outputNode.Dcid]; !ok {
						result[outputNode.Dcid] = append(result[outputNode.Dcid], inputNode.Value)
					} else if _, ok := localResult[outputNode.Dcid][inputNode.Value]; !ok {
						result[outputNode.Dcid] = append(result[outputNode.Dcid], inputNode.Value)
					}
				}
			}
		}
	}
	// Sort for determinism.
	for _, formulas := range result {
		sort.Strings(formulas)
	}
	return result, nil
}
