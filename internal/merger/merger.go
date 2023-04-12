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
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// MergeObservation merges two V2 observation response.
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
