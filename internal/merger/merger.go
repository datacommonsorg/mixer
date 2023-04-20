// Copyright 2023 Google LLC
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
package merger

import (
	"sort"

	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/pagination"
	"github.com/datacommonsorg/mixer/internal/util"
)

// MergeResolve merges two V2 resolve responses.
func MergeResolve(r1, r2 *pbv2.ResolveResponse) *pbv2.ResolveResponse {
	// Maps are used to dedup.
	nodeToResolvedIDSet := map[string]map[string]struct{}{}

	collectEntities := func(r *pbv2.ResolveResponse) {
		for _, e := range r.GetEntities() {
			node := e.GetNode()
			if _, ok := nodeToResolvedIDSet[node]; !ok {
				nodeToResolvedIDSet[node] = map[string]struct{}{}
			}
			for _, id := range e.GetResolvedIds() {
				nodeToResolvedIDSet[node][id] = struct{}{}
			}
		}
	}

	collectEntities(r1)
	collectEntities(r2)

	res := &pbv2.ResolveResponse{}
	for node, resolvedIDSet := range nodeToResolvedIDSet {
		var resolvedIDs []string
		for id := range resolvedIDSet {
			resolvedIDs = append(resolvedIDs, id)
		}

		// Sort to make result deterministic.
		sort.Strings(resolvedIDs)

		res.Entities = append(res.Entities, &pbv2.ResolveResponse_Entity{
			Node:        node,
			ResolvedIds: resolvedIDs,
		})
	}

	// Sort to make result deterministic.
	sort.Slice(res.Entities, func(i, j int) bool {
		return res.Entities[i].Node < res.Entities[j].Node
	})

	return res
}

// MergeNode merges two V2 node responses.
// NOTE: Make sure the order of the two arguments, it's important for merging |next_token|.
func MergeNode(local, remote *pbv2.NodeResponse) (*pbv2.NodeResponse, error) {
	type linkedGraphStore struct {
		propToNodes        map[string]*pbv2.Nodes
		propToLinkedGraphs map[string]*pbv2.LinkedGraph
		propSet            map[string]struct{}
	}
	dcidToLinkedGraph := map[string]*linkedGraphStore{}

	collectNode := func(n *pbv2.NodeResponse) {
		for dcid, linkedGrarph := range n.GetData() {
			if _, ok := dcidToLinkedGraph[dcid]; !ok {
				dcidToLinkedGraph[dcid] = &linkedGraphStore{}
			}
			if arcs := linkedGrarph.GetArcs(); len(arcs) > 0 {
				if dcidToLinkedGraph[dcid].propToNodes == nil {
					dcidToLinkedGraph[dcid].propToNodes = map[string]*pbv2.Nodes{}
				}
				for prop, nodes := range arcs {
					dcidToLinkedGraph[dcid].propToNodes[prop] = nodes
				}
			}
			if neighbor := linkedGrarph.GetNeighbor(); len(neighbor) > 0 {
				if dcidToLinkedGraph[dcid].propToLinkedGraphs == nil {
					dcidToLinkedGraph[dcid].propToLinkedGraphs = map[string]*pbv2.LinkedGraph{}
				}
				for prop, neighborLinkedGraph := range neighbor {
					dcidToLinkedGraph[dcid].propToLinkedGraphs[prop] = neighborLinkedGraph
				}
			}
			if props := linkedGrarph.GetProperties(); len(props) > 0 {
				if dcidToLinkedGraph[dcid].propSet == nil {
					dcidToLinkedGraph[dcid].propSet = map[string]struct{}{}
				}
				for _, prop := range props {
					dcidToLinkedGraph[dcid].propSet[prop] = struct{}{}
				}
			}
		}
	}

	collectNode(local)
	collectNode(remote)

	res := &pbv2.NodeResponse{Data: map[string]*pbv2.LinkedGraph{}}
	for dcid, store := range dcidToLinkedGraph {
		res.Data[dcid] = &pbv2.LinkedGraph{}
		if propToNodes := store.propToNodes; len(propToNodes) > 0 {
			res.Data[dcid].Arcs = map[string]*pbv2.Nodes{}
			for prop, nodes := range propToNodes {
				res.Data[dcid].Arcs[prop] = nodes
			}
		}
		if propToLinkedGraphs := store.propToLinkedGraphs; len(propToLinkedGraphs) > 0 {
			res.Data[dcid].Neighbor = map[string]*pbv2.LinkedGraph{}
			for prop, neighborLinkedGraph := range propToLinkedGraphs {
				res.Data[dcid].Neighbor[prop] = neighborLinkedGraph
			}
		}
		for prop := range store.propSet {
			res.Data[dcid].Properties = append(res.Data[dcid].Properties, prop)
		}
	}

	// Merge |next_token|.
	resPaginationInfo := &pbv1.PaginationInfo{}
	if local.GetNextToken() != "" {
		localPaginationInfo, err := pagination.Decode(local.GetNextToken())
		if err != nil {
			return nil, err
		}
		resPaginationInfo = localPaginationInfo
	}
	if remote.GetNextToken() != "" {
		remotePaginationInfo, err := pagination.Decode(remote.GetNextToken())
		if err != nil {
			return nil, err
		}
		resPaginationInfo.RemotePaginationInfo = remotePaginationInfo
	}
	if local.GetNextToken() != "" || remote.GetNextToken() != "" {
		resNextToken, err := util.EncodeProto(resPaginationInfo)
		if err != nil {
			return nil, err
		}
		res.NextToken = resNextToken
	}

	return res, nil
}

// MergeEvent merges two V2 event responses.
func MergeEvent(e1, e2 *pbv2.EventResponse) *pbv2.EventResponse {
	idToEvent := map[string]*pbv1.EventCollection_Event{}
	idToProvenance := map[string]*pbv1.EventCollection_ProvenanceInfo{}
	dateSet := map[string]struct{}{}

	collectEvent := func(e *pbv2.EventResponse) {
		if ec := e.GetEventCollection(); ec != nil {
			for k, v := range ec.GetProvenanceInfo() {
				if _, ok := idToProvenance[k]; ok {
					continue
				}
				idToProvenance[k] = v
			}
			for _, ev := range ec.GetEvents() {
				if _, ok := idToEvent[ev.GetDcid()]; ok {
					continue
				}
				idToEvent[ev.GetDcid()] = ev
			}
		}

		if ed := e.GetEventCollectionDate(); ed != nil {
			for _, date := range ed.GetDates() {
				dateSet[date] = struct{}{}
			}
		}
	}

	collectEvent(e1)
	collectEvent(e2)

	res := &pbv2.EventResponse{}
	if len(idToEvent) > 0 {
		res.EventCollection = &pbv1.EventCollection{
			ProvenanceInfo: idToProvenance,
		}
		for _, ev := range idToEvent {
			res.EventCollection.Events = append(res.EventCollection.Events, ev)
		}
	}
	if len(dateSet) > 0 {
		res.EventCollectionDate = &pbv1.EventCollectionDate{}
		for date := range dateSet {
			res.EventCollectionDate.Dates = append(res.EventCollectionDate.Dates, date)
		}
	}

	// Sort to make results deterministic.
	if res.EventCollection != nil {
		sort.Slice(res.EventCollection.Events, func(i, j int) bool {
			return res.EventCollection.Events[i].Dcid < res.EventCollection.Events[j].Dcid
		})
	}
	if res.EventCollectionDate != nil {
		sort.Strings(res.EventCollectionDate.Dates)
	}

	return res
}

// MergeObservation merges two V2 observation responses.
func MergeObservation(
	o1, o2 *pbv2.ObservationResponse) *pbv2.ObservationResponse {
	for v, vData := range o2.ByVariable {
		if _, ok := o1.ByVariable[v]; !ok {
			o1.ByVariable[v] = &pbv2.VariableObservation{
				ByEntity: map[string]*pbv2.EntityObservation{},
			}
		}
		for e, eData := range vData.ByEntity {
			if _, ok := o1.ByVariable[v].ByEntity[e]; !ok {
				o1.ByVariable[v].ByEntity[e] = &pbv2.EntityObservation{
					OrderedFacets: []*pbv2.FacetObservation{},
				}
			}
			o1.ByVariable[v].ByEntity[e].OrderedFacets = append(
				o1.ByVariable[v].ByEntity[e].OrderedFacets,
				eData.OrderedFacets...,
			)
		}
	}
	for facetID, facet := range o2.Facets {
		o1.Facets[facetID] = facet
	}
	return o1
}
