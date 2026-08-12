// Copyright 2026 Google LLC
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
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
)

func TestGetQueryType(t *testing.T) {
	tests := []struct {
		name string
		in   *pbv2.ObservationRequest
		want shared.QueryType
	}{
		{
			name: "value query - dcids",
			in: &pbv2.ObservationRequest{
				Select:   []string{"date", "value", "variable", "entity"},
				Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
				Entity:   &pbv2.DcidOrExpression{Dcids: []string{"country/USA"}},
			},
			want: shared.QueryTypeValue,
		},
		{
			name: "value query - expression",
			in: &pbv2.ObservationRequest{
				Select:   []string{"date", "value", "variable", "entity"},
				Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
				Entity:   &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf: City}"},
			},
			want: shared.QueryTypeValue,
		},
		{
			name: "derived series",
			in: &pbv2.ObservationRequest{
				Select:   []string{"date", "value", "variable", "entity"},
				Variable: &pbv2.DcidOrExpression{Formula: "foo / bar"},
				Entity:   &pbv2.DcidOrExpression{Dcids: []string{"country/USA"}},
			},
			want: shared.QueryTypeDerived,
		},
		{
			name: "facet query",
			in: &pbv2.ObservationRequest{
				Select:   []string{"facet", "variable", "entity"},
				Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
				Entity:   &pbv2.DcidOrExpression{Dcids: []string{"country/USA"}},
			},
			want: shared.QueryTypeFacet,
		},
		{
			name: "existence query",
			in: &pbv2.ObservationRequest{
				Select:   []string{"variable", "entity"},
				Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
				Entity:   &pbv2.DcidOrExpression{Dcids: []string{"country/USA"}},
			},
			want: shared.QueryTypeExistence,
		},
		{
			name: "missing variable select",
			in: &pbv2.ObservationRequest{
				Select:   []string{"entity", "date", "value"},
				Variable: &pbv2.DcidOrExpression{Dcids: []string{"Count_Person"}},
				Entity:   &pbv2.DcidOrExpression{Dcids: []string{"country/USA"}},
			},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GetQueryType(tc.in)
			if got != tc.want {
				t.Errorf("GetQueryType() = %v, want %v", got, tc.want)
			}
		})
	}
}
