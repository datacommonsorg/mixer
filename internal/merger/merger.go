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
// NOTE: The merge isn't for pagination, so NodeResponse::next_token should be empty.
func MergeNode(n1, n2 *pbv2.NodeResponse) *pbv2.NodeResponse {
	// TODO(ws): Implement real MergeNode().
	return n1
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
