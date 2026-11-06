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

package agent

import (
	"context"
	"errors"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
)

func TestV2ResolveEntitiesChildTypes(t *testing.T) {
	const unfiltered = PropDescription
	childColumns := []string{"query", "entity_dcid", "child_entity_type_dcid", "child_entity_type_name"}
	californiaChildTypes := []string{"AdministrativeArea", "AdministrativeArea2", "CensusCountyDivision", "CensusTract", "City", "County", "Place", "PublicSchool"}
	tests := []struct {
		name              string
		req               *V2ResolveEntitiesRequest
		resolveCandidates map[string][]*pbv2.ResolveResponse_Entity_Candidate
		childTypes        map[string][]string
		childTypesErr     error
		nodeData          map[string]*pbv2.LinkedGraph
		nodeErr           error
		wantChildRows     [][]any
		wantNoTable       bool
		wantNote          string
		wantChildCalls    [][]string
	}{
		{
			name: "each hint returns its best existing non-generic class",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "California", ChildEntityTypeHints: []string{"county", "administrative area 2", "region"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"):                {scoredType("County", "0.7493"), scoredType("Municipality", "0.6285"), scoredType("CensusCountyDivision", "0.6081")},
				resolveKey(ResolverType, "", "administrative area 2"): {scoredType("AdministrativeArea2", "0.7320"), scoredType("AdministrativeArea", "0.7172"), scoredType("AdministrativeArea5", "0.6658")},
				resolveKey(ResolverType, "", "region"):                {scoredType("AdministrativeArea", "0.70"), scoredType("Place", "0.69")},
				resolveKey(ResolverPlace, unfiltered, "California"):   {{Dcid: "geoId/06"}, {Dcid: "geoId/2412150"}},
			},
			childTypes: map[string][]string{"geoId/06": californiaChildTypes},
			nodeData: map[string]*pbv2.LinkedGraph{
				"County":              nodeGraph("County"),
				"AdministrativeArea2": nodeGraph("Administrative Area 2"),
			},
			wantChildRows: [][]any{
				{"California", "geoId/06", "County", "County"},
				{"California", "geoId/06", "AdministrativeArea2", "Administrative Area 2"},
			},
			wantChildCalls: [][]string{{"geoId/06"}},
		},
		{
			name: "near-tied existing classes are all returned for umbrella hints",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "California", ChildEntityTypeHints: []string{"school"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "school"):              {scoredType("HighSchool", "0.6618"), scoredType("ElementarySchool", "0.6599"), scoredType("MiddleSchool", "0.6455")},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			childTypes: map[string][]string{"geoId/06": {"ElementarySchool", "HighSchool", "MiddleSchool"}},
			wantChildRows: [][]any{
				{"California", "geoId/06", "HighSchool", ""},
				{"California", "geoId/06", "ElementarySchool", ""},
				{"California", "geoId/06", "MiddleSchool", ""},
			},
			wantChildCalls: [][]string{{"geoId/06"}},
		},
		{
			name: "no matching child type returns a placeholder row with the parent",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "California", ChildEntityTypeHints: []string{"hospital"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "hospital"):            {{Dcid: "Hospital"}},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			childTypes:     map[string][]string{"geoId/06": californiaChildTypes},
			wantChildRows:  [][]any{{"California", "geoId/06", "", ""}},
			wantChildCalls: [][]string{{"geoId/06"}},
		},
		{
			name: "child type lookup failure omits the child types table",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "California", ChildEntityTypeHints: []string{"county"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"):              {{Dcid: "County"}},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			childTypesErr:  errors.New("lookup failed"),
			wantNoTable:    true,
			wantNote:       v2ChildTypesUnavailableNote,
			wantChildCalls: [][]string{{"geoId/06"}},
		},
		{
			name: "child type ranked below the entity type cap is still matched",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "Canada", ChildEntityTypeHints: []string{"province"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "province"): {
					scoredType("AdministrativeArea", "0.74"), scoredType("State", "0.73"),
					scoredType("Region", "0.72"), scoredType("Province", "0.71"),
				},
				resolveKey(ResolverPlace, unfiltered, "Canada"): {{Dcid: "country/CAN"}},
			},
			childTypes:     map[string][]string{"country/CAN": {"AdministrativeArea", "Province"}},
			wantChildRows:  [][]any{{"Canada", "country/CAN", "Province", ""}},
			wantChildCalls: [][]string{{"country/CAN"}},
		},
		{
			name: "unresolved entity with child hints returns a placeholder row without a parent and no lookup",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "Nowhereville", ChildEntityTypeHints: []string{"county"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"): {{Dcid: "County"}},
			},
			wantChildRows: [][]any{{"Nowhereville", "", "", ""}},
		},
		{
			name: "name lookup failure keeps matched child types without names",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "California", ChildEntityTypeHints: []string{"county"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"):              {{Dcid: "County"}},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			childTypes:     map[string][]string{"geoId/06": californiaChildTypes},
			nodeErr:        errors.New("node lookup failed"),
			wantChildRows:  [][]any{{"California", "geoId/06", "County", ""}},
			wantChildCalls: [][]string{{"geoId/06"}},
		},
		{
			name: "child types of all parents are fetched in one call; only queries with non-blank hints get rows",
			req: &V2ResolveEntitiesRequest{Entities: []*V2EntityQuery{
				{Name: "India"},
				{Name: "Mexico", ChildEntityTypeHints: []string{" "}},
				{Name: "Kenya", ChildEntityTypeHints: []string{"county"}},
				{Name: "California", ChildEntityTypeHints: []string{"county"}},
			}},
			resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				resolveKey(ResolverType, "", "county"):              {scoredType("County", "0.75"), scoredType("AdministrativeArea1", "0.60")},
				resolveKey(ResolverPlace, unfiltered, "India"):      {{Dcid: "country/IND"}},
				resolveKey(ResolverPlace, unfiltered, "Mexico"):     {{Dcid: "country/MEX"}},
				resolveKey(ResolverPlace, unfiltered, "Kenya"):      {{Dcid: "country/KEN"}},
				resolveKey(ResolverPlace, unfiltered, "California"): {{Dcid: "geoId/06"}},
			},
			childTypes: map[string][]string{
				"country/KEN": {"AdministrativeArea1", "City", "County"},
				"geoId/06":    californiaChildTypes,
			},
			wantChildRows: [][]any{
				{"Kenya", "country/KEN", "County", ""},
				{"California", "geoId/06", "County", ""},
			},
			wantChildCalls: [][]string{{"country/KEN", "geoId/06"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockV2ResolveEntitiesServer{
				resolveCandidates: tc.resolveCandidates,
				childTypes:        tc.childTypes,
				childTypesErr:     tc.childTypesErr,
				nodeData:          withPlaceSubclasses(tc.nodeData),
				nodeErr:           tc.nodeErr,
			}
			got, err := NewService(mock, nil, nil).V2ResolveEntities(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("V2ResolveEntities() unexpected error: %v", err)
			}
			want := &Table{Columns: childColumns, Rows: tc.wantChildRows}
			if tc.wantNoTable {
				want = nil
			}
			if diff := cmp.Diff(want, got.CandidateChildEntityTypes); diff != "" {
				t.Errorf("CandidateChildEntityTypes diff (-want +got):\n%s", diff)
			}
			if got.ChildEntityTypesNote != tc.wantNote {
				t.Errorf("ChildEntityTypesNote = %q, want %q", got.ChildEntityTypesNote, tc.wantNote)
			}
			if diff := cmp.Diff(tc.wantChildCalls, mock.childTypesCalls); diff != "" {
				t.Errorf("ChildTypes calls diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMatchChildTypes(t *testing.T) {
	tests := []struct {
		name        string
		hints       []string
		typesByHint map[string][]typeCandidate
		available   []string
		want        []string
	}{
		{
			name:        "best existing class per hint, skipping missing and generic classes",
			hints:       []string{"administrative area 1"},
			typesByHint: map[string][]typeCandidate{"administrative area 1": {{"AdministrativeArea", 0.7377}, {"Province", 0.73}, {"AdministrativeArea1", 0.7183}}},
			available:   []string{"AdministrativeArea", "AdministrativeArea1"},
			want:        []string{"AdministrativeArea1"},
		},
		{
			name:        "classes within the margin of the best existing class are kept, capped per hint",
			hints:       []string{"school"},
			typesByHint: map[string][]typeCandidate{"school": {{"HighSchool", 0.66}, {"ElementarySchool", 0.65}, {"MiddleSchool", 0.645}}},
			available:   []string{"HighSchool", "ElementarySchool", "MiddleSchool"},
			want:        []string{"HighSchool", "ElementarySchool", "MiddleSchool"},
		},
		{
			name:  "loosely related existing classes far below the hint's best class are dropped",
			hints: []string{"state", "autonomous community"},
			typesByHint: map[string][]typeCandidate{
				"state":                {{"State", 0.724}, {"Governorate", 0.613}, {"County", 0.594}},
				"autonomous community": {{"AutonomousRegion", 0.594}, {"Neighborhood", 0.515}, {"AdministrativeArea1", 0.509}},
			},
			available: []string{"County", "Neighborhood", "AdministrativeArea1"},
		},
		{
			name:        "classes beyond the margin are dropped",
			hints:       []string{"county"},
			typesByHint: map[string][]typeCandidate{"county": {{"County", 0.75}, {"CensusCountyDivision", 0.70}}},
			available:   []string{"County", "CensusCountyDivision"},
			want:        []string{"County"},
		},
		{
			name:  "duplicates across hints are returned once, in hint order",
			hints: []string{"state", "province"},
			typesByHint: map[string][]typeCandidate{
				"state":    {{"State", 0.72}},
				"province": {{"Province", 0.71}, {"State", 0.70}},
			},
			available: []string{"State", "Province"},
			want:      []string{"State", "Province"},
		},
		{
			name:        "nothing available returns nothing",
			hints:       []string{"county"},
			typesByHint: map[string][]typeCandidate{"county": {{"County", 0.75}}},
		},
		{
			// Custom DCs store children whose type is unknown under an empty child type, which no hint matches.
			name:        "an empty available type is never matched",
			hints:       []string{"county"},
			typesByHint: map[string][]typeCandidate{"county": {{"County", 0.75}}},
			available:   []string{""},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := matchChildTypes(tc.hints, tc.typesByHint, tc.available)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("matchChildTypes() diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestV2ResolveEntitiesWithoutChildHintsOmitsChildTable(t *testing.T) {
	mock := &mockV2ResolveEntitiesServer{
		resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
			resolveKey(ResolverPlace, PropDescription, "California"): {{Dcid: "geoId/06"}},
		},
	}
	got, err := NewService(mock, nil, nil).V2ResolveEntities(context.Background(), &V2ResolveEntitiesRequest{
		Entities: []*V2EntityQuery{{Name: "California"}},
	})
	if err != nil {
		t.Fatalf("V2ResolveEntities() unexpected error: %v", err)
	}
	if got.CandidateChildEntityTypes != nil {
		t.Errorf("CandidateChildEntityTypes = %v, want nil", got.CandidateChildEntityTypes)
	}
}
