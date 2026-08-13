// Copyright 2025 Google LLC
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

package spanner

import (
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestAddOptimizationsToNodeRequest(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		description string
		in          *v2.Arc
		out         *v2.Arc
		artifacts   *nodeArtifacts
	}{
		{
			"no change",
			&v2.Arc{
				SingleProp: "name",
			},
			&v2.Arc{
				SingleProp: "name",
			},
			&nodeArtifacts{},
		},
		{
			"linkedContainedInPlace",
			&v2.Arc{
				SingleProp: "containedInPlace",
				Decorator:  "+",
				Filter: map[string][]string{
					"typeOf": {"State"},
				},
			},
			&v2.Arc{
				SingleProp: "linkedContainedInPlace",
				Filter: map[string][]string{
					"typeOf": {"State"},
				},
			},
			&nodeArtifacts{
				chainProp: "containedInPlace",
			},
		},
	} {
		actual := addOptimizationsToNodeRequest(c.in)

		if diff := cmp.Diff(c.in, c.out, cmpOpts); diff != "" {
			t.Errorf("ProcessNodeRequest(%v) got diff: %s", c.in, diff)
		}

		if !reflect.DeepEqual(actual, c.artifacts) {
			t.Errorf("Artifacts are not equal.\nExpected: %+v\nActual:   %+v", c.artifacts, actual)
		}
	}
}

func TestRemoveOptimizationsFromNodeResponse(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		description string
		in          *pbv2.NodeResponse
		artifacts   *nodeArtifacts
		out         *pbv2.NodeResponse
	}{
		{
			"no change",
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"name": {
								Nodes: []*pb.EntityInfo{
									{
										Value:        "name1",
										ProvenanceId: "dc/base/test",
									},
								},
							},
						},
					},
				},
			},
			&nodeArtifacts{},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"name": {
								Nodes: []*pb.EntityInfo{
									{
										Value:        "name1",
										ProvenanceId: "dc/base/test",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			"linkedContainedInPlace",
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"linkedContainedInPlace": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:         "place1",
										ProvenanceId: "dc/base/GeneratedGraphs",
									},
								},
							},
						},
					},
				},
			},
			&nodeArtifacts{
				chainProp: "containedInPlace",
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"containedInPlace+": {
								Nodes: []*pb.EntityInfo{
									{Dcid: "place1"},
								},
							},
						},
					},
				},
			},
		},
	} {
		removeOptimizationsFromNodeResponse(c.in, c.artifacts)

		if diff := cmp.Diff(c.in, c.out, cmpOpts); diff != "" {
			t.Errorf("ProcessNodeResponse(%v) got diff: %s", c.in, diff)
		}
	}
}

func TestFilterObservationsByDateAndFacet(t *testing.T) {
	tests := []struct {
		name     string
		obs      []*Observation
		date     string
		expected []*Observation
	}{
		// Test: Filter matches some data.
		// Situation: Two entities are provided. One has data for the requested date (2012), the other only has data for 2011.
		// Expectation: Only the entity with data for 2012 is returned, and its observations are filtered to that date.
		{
			name: "Filter matches some data",
			obs: []*Observation{
				{
					ObservationAbout: "entity1",
					Observations: TimeSeries{
						{Date: "2011", Value: "10"},
						{Date: "2012", Value: "20"},
					},
				},
				{
					ObservationAbout: "entity2",
					Observations: TimeSeries{
						{Date: "2011", Value: "15"},
					},
				},
			},
			date: "2012",
			expected: []*Observation{
				{
					ObservationAbout: "entity1",
					Observations: TimeSeries{
						{Date: "2012", Value: "20"},
					},
				},
			},
		},
		// Test: Filter matches no data.
		// Situation: Two entities are provided, but neither has data for the requested date (2012).
		// Expectation: No observations are returned (empty slice).
		{
			name: "Filter matches no data",
			obs: []*Observation{
				{
					ObservationAbout: "entity1",
					Observations: TimeSeries{
						{Date: "2011", Value: "10"},
					},
				},
				{
					ObservationAbout: "entity2",
					Observations: TimeSeries{
						{Date: "2011", Value: "15"},
					},
				},
			},
			date:     "2012",
			expected: nil,
		},
		// Test: Empty date filter keeps all.
		// Situation: Date filter is empty.
		// Expectation: All observations are retained as they were.
		{
			name: "Empty date filter keeps all",
			obs: []*Observation{
				{
					ObservationAbout: "entity1",
					Observations: TimeSeries{
						{Date: "2011", Value: "10"},
					},
				},
			},
			date: "",
			expected: []*Observation{
				{
					ObservationAbout: "entity1",
					Observations: TimeSeries{
						{Date: "2011", Value: "10"},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filtered := filterObservationsByDateAndFacet(tc.obs, tc.date, nil)
			if diff := cmp.Diff(tc.expected, filtered); diff != "" {
				t.Errorf("Test %s: unexpected filtered results (-want +got):\n%s", tc.name, diff)
			}
		})
	}
}

func TestIncludeObsMetadata(t *testing.T) {
	tests := []struct {
		name string
		req  *pbv2.ObservationRequest
		want bool
	}{
		{
			name: "contained in all dates",
			req: &pbv2.ObservationRequest{
				Entity: &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
			},
			want: true,
		},
		{
			name: "contained in specific date",
			req: &pbv2.ObservationRequest{
				Entity: &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
				Date:   "2020",
			},
			want: false,
		},
		{
			name: "contained in latest",
			req: &pbv2.ObservationRequest{
				Entity: &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
				Date:   "LATEST",
			},
			want: false,
		},
		{
			name: "direct latest",
			req: &pbv2.ObservationRequest{
				Entity: &pbv2.DcidOrExpression{Dcids: []string{"geoId/06"}},
				Date:   "LATEST",
			},
			want: true,
		},
		{
			name: "missing entity",
			req:  &pbv2.ObservationRequest{},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := includeObsMetadata(tc.req); got != tc.want {
				t.Errorf("includeObsMetadata() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestDatedExpressionResponseOmitsMetadataAndPreservesFacetRanking(t *testing.T) {
	const (
		variable = "Count_Person"
		entity   = "geoId/06001"
	)
	observations := []*Observation{
		{
			VariableMeasured:  variable,
			ObservationAbout:  entity,
			FacetId:           "latest-facet",
			ImportName:        "CensusACS5YearSurvey",
			MeasurementMethod: "CensusACS5yrSurvey",
			ProvenanceURL:     "z.example",
			Observations:      TimeSeries{{Date: "2020", Value: "2"}},
		},
		{
			VariableMeasured:  variable,
			ObservationAbout:  entity,
			FacetId:           "older-facet",
			ImportName:        "CensusACS5YearSurvey",
			MeasurementMethod: "CensusACS5yrSurvey",
			ProvenanceURL:     "a.example",
			Observations:      TimeSeries{{Date: "2019", Value: "1"}},
		},
	}
	containedInReq := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{variable}},
		Entity:   &pbv2.DcidOrExpression{Expression: "geoId/06<-containedInPlace+{typeOf:County}"},
		Date:     "LATEST",
	}

	response := obsToObsResponse(containedInReq, observations)
	orderedFacets := response.ByVariable[variable].ByEntity[entity].OrderedFacets
	if got, want := len(orderedFacets), 2; got != want {
		t.Fatalf("len(orderedFacets) = %d, want %d", got, want)
	}
	if got, want := orderedFacets[0].FacetId, "latest-facet"; got != want {
		t.Errorf("orderedFacets[0].FacetId = %q, want %q", got, want)
	}
	for _, facet := range orderedFacets {
		if facet.ObsCount != 0 || facet.EarliestDate != "" || facet.LatestDate != "" {
			t.Errorf("facet %q metadata = (%d, %q, %q), want omitted", facet.FacetId, facet.ObsCount, facet.EarliestDate, facet.LatestDate)
		}
	}

	facetResponse := obsToFacetResponse(containedInReq, observations)
	for _, facet := range facetResponse.ByVariable[variable].ByEntity[entityPlaceholder].OrderedFacets {
		if facet.ObsCount != 0 || facet.EarliestDate != "" || facet.LatestDate != "" {
			t.Errorf("facet-only response metadata = (%d, %q, %q), want omitted", facet.ObsCount, facet.EarliestDate, facet.LatestDate)
		}
	}

	directReq := &pbv2.ObservationRequest{
		Variable: &pbv2.DcidOrExpression{Dcids: []string{variable}},
		Entity:   &pbv2.DcidOrExpression{Dcids: []string{entity}},
		Date:     "LATEST",
	}
	directResponse := obsToObsResponse(directReq, observations)
	directFacet := directResponse.ByVariable[variable].ByEntity[entity].OrderedFacets[0]
	if directFacet.ObsCount == 0 || directFacet.EarliestDate == "" || directFacet.LatestDate == "" {
		t.Errorf("direct response metadata = (%d, %q, %q), want populated", directFacet.ObsCount, directFacet.EarliestDate, directFacet.LatestDate)
	}
}

func TestProcessSpecializedEntities(t *testing.T) {
	tests := []struct {
		name     string
		parentID string
		node     *pb.StatVarGroupNode
		want     []string // Expected SpecializedEntity values for children in order (after processing)
	}{
		{
			name:     "unique child specialized entities remain unchanged",
			parentID: "dc/g/Demographics",
			node: &pb.StatVarGroupNode{
				ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
					{Id: "dc/g/Person_Female", DisplayName: "Female"},
					{Id: "dc/g/Person_Male", DisplayName: "Male"},
				},
			},
			want: []string{"Female", "Male"},
		},
		{
			name:     "duplicate child specialized entities get appended with displayPopType",
			parentID: "dc/g/Demographics",
			node: &pb.StatVarGroupNode{
				ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
					{Id: "dc/g/Person_Female", DisplayName: "Female"},
					{Id: "dc/g/USCWorker_Female", DisplayName: "Female"},
					{Id: "dc/g/Person_Male", DisplayName: "Male"},
				},
			},
			want: []string{"Female (Individual)", "Female (USC Worker)", "Male"},
		},
		{
			name:     "curated hierarchy child is not modified",
			parentID: "dc/g/Root",
			node: &pb.StatVarGroupNode{
				ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
					{Id: "dc/g/SDG_1", DisplayName: "1: No Poverty"},
					{Id: "dc/g/SDG_2", DisplayName: "2: Zero Hunger"},
				},
			},
			want: []string{"1: No Poverty", "2: Zero Hunger"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			processSpecializedEntities(tc.parentID, tc.node)
			sortSVGNode(tc.node)
			var got []string
			for _, child := range tc.node.ChildStatVarGroups {
				got = append(got, child.SpecializedEntity)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("processSpecializedEntities() (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetSpecializedEntity(t *testing.T) {
	tests := []struct {
		name        string
		parent      string
		child       string
		childName   string
		wantSpec    string
		wantPopType string
	}{
		{
			name:        "age bucket child under age vertical",
			parent:      "dc/g/Person_Age",
			child:       "dc/g/Person_Age-Years86",
			childName:   "Person With Age = Years 86",
			wantSpec:    "Years 86",
			wantPopType: "Individual",
		},
		{
			name:        "age range child under age vertical",
			parent:      "dc/g/Person_Age",
			child:       "dc/g/Person_Age-Years90To94",
			childName:   "Person With Age = Years 90 To 94",
			wantSpec:    "Years 90 To 94",
			wantPopType: "Individual",
		},
		{
			name:        "open ended age child under age vertical",
			parent:      "dc/g/Person_Age",
			child:       "dc/g/Person_Age-Years90Onwards",
			childName:   "Person With Age = Years 90 Onwards",
			wantSpec:    "Years 90 Onwards",
			wantPopType: "Individual",
		},
		{
			name:        "non-basic population type direct child",
			parent:      "dc/g/Child",
			child:       "dc/g/Child_BenefitsStatus",
			childName:   "Child With Benefits Status",
			wantSpec:    "Benefits Status",
			wantPopType: "Child",
		},
		{
			name:        "nested non-basic population type child",
			parent:      "dc/g/Child",
			child:       "dc/g/Child_FoodSecurityStatus-FoodInSecure",
			childName:   "Child With Food Security Status = Food In Secure",
			wantSpec:    "Food In Secure",
			wantPopType: "Child",
		},
		{
			name:        "basic population type female child",
			parent:      "dc/g/Demographics",
			child:       "dc/g/Person_Female",
			childName:   "Female",
			wantSpec:    "Female",
			wantPopType: "Individual",
		},
		{
			name:        "worker population type female child",
			parent:      "dc/g/Demographics",
			child:       "dc/g/USCWorker_Female",
			childName:   "Female",
			wantSpec:    "Female",
			wantPopType: "USC Worker",
		},
		{
			name:        "curated SDG hierarchy child",
			parent:      "dc/g/Root",
			child:       "dc/g/SDG_1",
			childName:   "SDG 1",
			wantSpec:    "SDG 1",
			wantPopType: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSpec, gotPopType := getSpecializedEntity(tc.parent, tc.child, tc.childName)
			if gotSpec != tc.wantSpec {
				t.Errorf("getSpecializedEntity(%q, %q, %q) spec = %q, want %q", tc.parent, tc.child, tc.childName, gotSpec, tc.wantSpec)
			}
			if gotPopType != tc.wantPopType {
				t.Errorf("getSpecializedEntity(%q, %q, %q) popType = %q, want %q", tc.parent, tc.child, tc.childName, gotPopType, tc.wantPopType)
			}
		})
	}
}

func TestNodeEdgesToLinkedGraph(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	compressedBytes, err := util.Zip([]byte("Compressed description text"))
	if err != nil {
		t.Fatalf("util.Zip() failed: %v", err)
	}

	tests := []struct {
		name    string
		edges   []*Edge
		want    *pbv2.LinkedGraph
		wantErr bool
	}{
		{
			name:  "empty edges",
			edges: []*Edge{},
			want: &pbv2.LinkedGraph{
				Arcs: make(map[string]*pbv2.Nodes),
			},
		},
		{
			name: "unresolved reference node",
			edges: []*Edge{
				{
					SubjectID:  "country/USA",
					Predicate:  "specializationOf",
					ObjectID:   "country/UNRESOLVED_GEO",
					Provenance: "dc/base/WikidataOtherIdGeos",
					Value:      "",
					Bytes:      nil,
					Name:       "",
					Types:      nil,
				},
			},
			want: &pbv2.LinkedGraph{
				Arcs: map[string]*pbv2.Nodes{
					"specializationOf": {
						Nodes: []*pb.EntityInfo{
							{
								Dcid:         "country/UNRESOLVED_GEO",
								Types:        []string{"Thing"},
								ProvenanceId: "dc/base/WikidataOtherIdGeos",
							},
						},
					},
				},
			},
		},
		{
			name: "resolved reference node",
			edges: []*Edge{
				{
					SubjectID:  "country/USA",
					Predicate:  "typeOf",
					ObjectID:   "Country",
					Provenance: "dc/base/WikidataOtherIdGeos",
					Value:      "Country",
					Name:       "Country",
					Types:      []string{"Class"},
				},
			},
			want: &pbv2.LinkedGraph{
				Arcs: map[string]*pbv2.Nodes{
					"typeOf": {
						Nodes: []*pb.EntityInfo{
							{
								Dcid:         "Country",
								Name:         "Country",
								Types:        []string{"Class"},
								ProvenanceId: "dc/base/WikidataOtherIdGeos",
							},
						},
					},
				},
			},
		},
		{
			name: "terminal literal string value",
			edges: []*Edge{
				{
					SubjectID:  "country/USA",
					Predicate:  "name",
					ObjectID:   generateObjectValue("UnitedStates"),
					Provenance: "dc/base/HumanReadableStatVars",
					Value:      "United States",
					Types:      nil,
				},
			},
			want: &pbv2.LinkedGraph{
				Arcs: map[string]*pbv2.Nodes{
					"name": {
						Nodes: []*pb.EntityInfo{
							{
								Value:        "United States",
								ProvenanceId: "dc/base/HumanReadableStatVars",
							},
						},
					},
				},
			},
		},
		{
			name: "terminal literal compressed bytes",
			edges: []*Edge{
				{
					SubjectID:  "country/USA",
					Predicate:  "description",
					ObjectID:   "desc_object",
					Provenance: "dc/base/HumanReadableStatVars",
					Bytes:      compressedBytes,
					Types:      nil,
				},
			},
			want: &pbv2.LinkedGraph{
				Arcs: map[string]*pbv2.Nodes{
					"description": {
						Nodes: []*pb.EntityInfo{
							{
								Value:        "Compressed description text",
								ProvenanceId: "dc/base/HumanReadableStatVars",
							},
						},
					},
				},
			},
		},
		{
			name: "multiple arcs with mixed resolved, unresolved, and literal nodes with deterministic sorting",
			edges: []*Edge{
				{
					SubjectID:  "country/USA",
					Predicate:  "containedInPlace",
					ObjectID:   "place/Z_Unresolved",
					Provenance: "dc/base/WikidataOtherIdGeos",
				},
				{
					SubjectID:  "country/USA",
					Predicate:  "containedInPlace",
					ObjectID:   "northamerica",
					Provenance: "dc/base/WikidataOtherIdGeos",
					Value:      "northamerica",
					Name:       "North America",
					Types:      []string{"Continent"},
				},
				{
					SubjectID:  "country/USA",
					Predicate:  "name",
					ObjectID:   generateObjectValue("United States"),
					Provenance: "dc/base/HumanReadableStatVars",
					Value:      "United States",
				},
			},
			want: &pbv2.LinkedGraph{
				Arcs: map[string]*pbv2.Nodes{
					"containedInPlace": {
						Nodes: []*pb.EntityInfo{
							{
								Dcid:         "northamerica",
								Name:         "North America",
								Types:        []string{"Continent"},
								ProvenanceId: "dc/base/WikidataOtherIdGeos",
							},
							{
								Dcid:         "place/Z_Unresolved",
								Types:        []string{"Thing"},
								ProvenanceId: "dc/base/WikidataOtherIdGeos",
							},
						},
					},
					"name": {
						Nodes: []*pb.EntityInfo{
							{
								Value:        "United States",
								ProvenanceId: "dc/base/HumanReadableStatVars",
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := nodeEdgesToLinkedGraph(tc.edges)
			if (err != nil) != tc.wantErr {
				t.Fatalf("nodeEdgesToLinkedGraph() error = %v, wantErr %v", err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.want, got, cmpOpts); diff != "" {
				t.Errorf("nodeEdgesToLinkedGraph() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
