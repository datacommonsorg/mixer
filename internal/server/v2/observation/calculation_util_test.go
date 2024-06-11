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

package observation

import (
	"reflect"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

func TestFindObservationResponseHoles(t *testing.T) {
	for _, c := range []struct {
		inputReq  *pbv2.ObservationRequest
		inputResp *pbv2.ObservationResponse
		want      map[string]*pbv2.DcidOrExpression
	}{
		{
			&pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{Dcids: []string{
					"Count_Person",
					"Count_Farm",
				}},
				Entity: &pbv2.DcidOrExpression{Dcids: []string{
					"geoId/01",
					"geoId/02",
				}},
			},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {ByEntity: map[string]*pbv2.EntityObservation{
						"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{{FacetId: "1"}}},
						"geoId/02": {OrderedFacets: []*pbv2.FacetObservation{}},
					}},
					"Count_Farm": {ByEntity: map[string]*pbv2.EntityObservation{
						"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{}},
						"geoId/02": {OrderedFacets: []*pbv2.FacetObservation{{FacetId: "2"}}},
					}},
				},
			},
			map[string]*pbv2.DcidOrExpression{
				"Count_Person": {Dcids: []string{"geoId/02"}},
				"Count_Farm":   {Dcids: []string{"geoId/01"}},
			},
		},
		{
			&pbv2.ObservationRequest{
				Variable: &pbv2.DcidOrExpression{Dcids: []string{
					"Count_Person",
					"Count_Farm",
				}},
				Entity: &pbv2.DcidOrExpression{Expression: "country/USA<-containedInPlace+{typeOf:State}"},
			},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"Count_Person": {ByEntity: map[string]*pbv2.EntityObservation{"geoId/01": {OrderedFacets: []*pbv2.FacetObservation{}}}},
					"Count_Farm":   {ByEntity: map[string]*pbv2.EntityObservation{}},
				},
			},
			map[string]*pbv2.DcidOrExpression{
				"Count_Farm": {Expression: "country/USA<-containedInPlace+{typeOf:State}"},
			},
		},
	} {
		got, err := findObservationResponseHoles(c.inputReq, c.inputResp)
		if err != nil {
			t.Errorf("error running TestFindObservationResponseHoles: %s", err)
			continue
		}
		if ok := reflect.DeepEqual(got, c.want); !ok {
			t.Errorf("findObservationResponseHoles(%v, %v) = %v, want %v",
				c.inputReq, c.inputResp, got, c.want)
		}
	}
}
