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

package golden

import (
	"context"
	"path"
	"runtime"
	"testing"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
)

const (
	// Number of matches to validate for tests.
	NUM_MATCHES = 20
)

func TestGetNodeProps(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")

	for _, c := range []struct {
		ids        []string
		out        bool
		goldenFile string
	}{
		{
			ids:        []string{"Count_Person", "Person", "foo"},
			out:        true,
			goldenFile: "get_node_props_by_subject_id.json",
		},
		{
			ids:        []string{"Count_Person", "Person"},
			out:        false,
			goldenFile: "get_node_props_by_object_id.json",
		},
	} {
		actual, err := client.GetNodeProps(ctx, c.ids, c.out)
		if err != nil {
			t.Fatalf("GetNodeProps error (%v): %v", c.goldenFile, err)
		}

		got, err := test.StructToJSON(actual)
		if err != nil {
			t.Fatalf("StructToJSON error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			err = test.WriteGolden(got, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.goldenFile, diff)
		}
	}
}

func TestGetNodeEdgesByID(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")

	for _, c := range []struct {
		ids        []string
		arc        *v2.Arc
		cursor     *spanner.Edge
		goldenFile string
	}{
		{
			ids: []string{"Aadhaar", "Monthly_Average_RetailPrice_Electricity_Residential", "foo"},
			arc: &v2.Arc{
				Out:        true,
				SingleProp: "*",
			},
			cursor:     nil,
			goldenFile: "get_node_edges_by_subject_id.json",
		},
		{
			ids: []string{"FireIncidentTypeEnum", "FoodTypeEnum"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "*",
			},
			cursor:     nil,
			goldenFile: "get_node_edges_by_object_id.json",
		},
		{
			ids: []string{"Person"},
			arc: &v2.Arc{
				Out:        true,
				SingleProp: "extendedName",
			},
			cursor:     nil,
			goldenFile: "get_node_edges_out_single_prop.json",
		},
		{
			ids: []string{"Person"},
			arc: &v2.Arc{
				Out:          true,
				BracketProps: []string{"source", "subClassOf"},
			},
			cursor:     nil,
			goldenFile: "get_node_edges_out_bracket_props.json",
		},
		{
			ids: []string{"nuts/UKI1"},
			arc: &v2.Arc{
				Out: true,
				Filter: map[string][]string{
					"subClassOf":   {"AdministrativeArea"},
					"extendedName": {"AdministrativeArea2"},
				},
			},
			cursor:     nil,
			goldenFile: "get_node_edges_out_filter.json",
		},
		{
			ids: []string{"dc/g/Person_Gender"},
			arc: &v2.Arc{
				Out:        true,
				SingleProp: "specializationOf",
				Decorator:  "+",
			},
			cursor:     nil,
			goldenFile: "get_node_edges_out_chain.json",
		},
		{
			ids: []string{"EarthquakeEvent"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "domainIncludes",
			},
			cursor:     nil,
			goldenFile: "get_node_edges_in_single_prop.json",
		},
		{
			ids: []string{"EarthquakeEvent"},
			arc: &v2.Arc{
				Out:          false,
				BracketProps: []string{"domainIncludes", "naturalHazardType"},
			},
			cursor:     nil,
			goldenFile: "get_node_edges_in_bracket_props.json",
		},
		{
			ids: []string{"Farm"},
			arc: &v2.Arc{
				Out: false,
				Filter: map[string][]string{
					"farmInventoryType": {"Melon"},
					"extendedName":      {"Area of Farm: Melon"},
				},
			},
			cursor:     nil,
			goldenFile: "get_node_edges_in_filter.json",
		},
		{
			ids: []string{"dc/g/Farm_FarmInventoryStatus"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "specializationOf",
				Decorator:  "+",
			},
			cursor:     nil,
			goldenFile: "get_node_edges_in_chain.json",
		},
		{
			ids: []string{"foo OR 1=1;"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "foo OR 1=1;",
				Filter: map[string][]string{
					"foo OR 1=1;": {"foo OR 1=1;"},
				},
			},
			cursor:     nil,
			goldenFile: "get_node_edges_malicious.json",
		},
		{
			ids: []string{"StatisticalVariable"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "typeOf",
			},
			cursor:     nil,
			goldenFile: "get_node_edges_first_page.json",
		},
		{
			ids: []string{"StatisticalVariable"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "typeOf",
			},
			cursor: &spanner.Edge{
				SubjectID:   "StatisticalVariable",
				Predicate:   "typeOf",
				ObjectID:    "AggregateMax_Percentile90AcrossModels_DifferenceRelativeToBaseDate2006To2020_Max_Temperature_RCP45",
				ObjectValue: "",
				Provenance:  "dc/base/HumanReadableStatVars",
			},
			goldenFile: "get_node_edges_second_page.json",
		},
		{
			ids: []string{"dc/g/Root"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "specializationOf",
				Decorator:  "+",
			},
			cursor:     nil,
			goldenFile: "get_node_edges_first_page_chain.json",
		},
		{
			ids: []string{"dc/g/Root"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "specializationOf",
				Decorator:  "+",
			},
			cursor: &spanner.Edge{
				SubjectID:   "dc/g/Root",
				Predicate:   "specializationOf+",
				ObjectID:    "WHO/g/FamilyPlanning",
				ObjectValue: "",
				Provenance:  "",
			},
			goldenFile: "get_node_edges_second_page_chain.json",
		},
	} {
		actual, err := client.GetNodeEdgesByID(ctx, c.ids, c.arc, c.cursor)
		if err != nil {
			t.Fatalf("GetNodeEdgesByID error (%v): %v", c.goldenFile, err)
		}

		actual = simplifyNodes(actual)

		got, err := test.StructToJSON(actual)
		if err != nil {
			t.Fatalf("StructToJSON error (%v): %v", c.goldenFile, err)
		}

		if true {
			err = test.WriteGolden(got, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.goldenFile, diff)
		}
	}
}
func TestGetObservations(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")

	for _, c := range []struct {
		variables        []string
		entities         []string
		containedInPlace *v2.ContainedInPlace
		goldenFile       string
	}{
		{
			variables:  []string{"AirPollutant_Cancer_Risk"},
			entities:   []string{"geoId/01001", "geoId/02013"},
			goldenFile: "get_observations.json",
		},
		{
			variables:        []string{"Count_Person", "Median_Age_Person"},
			containedInPlace: &v2.ContainedInPlace{Ancestor: "geoId/06", ChildPlaceType: "County"},
			goldenFile:       "get_observations_contained_in.json",
		},
	} {
		var actual []*spanner.Observation
		var err error

		if c.containedInPlace != nil {
			actual, err = client.GetObservationsContainedInPlace(ctx, c.variables, c.containedInPlace)
		} else {
			actual, err = client.GetObservations(ctx, c.variables, c.entities)
		}
		if err != nil {
			t.Fatalf("GetObservations error (%v): %v", c.goldenFile, err)
		}

		got, err := test.StructToJSON(actual)
		if err != nil {
			t.Fatalf("StructToJSON error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			err = test.WriteGolden(got, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			continue
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.goldenFile, diff)
		}
	}

}
func TestSearchNodes(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")

	for _, c := range []struct {
		query      string
		types      []string
		goldenFile string
	}{
		{
			query:      "income",
			types:      []string{"StatisticalVariable"},
			goldenFile: "search_nodes_with_type.json",
		},
		{
			query:      "income",
			goldenFile: "search_nodes_without_type.json",
		},
	} {
		actual, err := client.SearchNodes(ctx, c.query, c.types)
		if err != nil {
			t.Fatalf("SearchNodes error (%v): %v", c.goldenFile, err)
		}

		actual = simplifySearchNodes(actual)

		got, err := test.StructToJSON(actual)
		if err != nil {
			t.Fatalf("StructToJSON error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			err = test.WriteGolden(got, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			continue
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.goldenFile, diff)
		}
	}

}
func TestSearchObjectValues(t *testing.T) {
	client := test.NewSpannerClient()
	if client == nil {
		return
	}

	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query")

	for _, c := range []struct {
		query      string
		predicates []string
		types      []string
		goldenFile string
	}{
		{
			query:      "income",
			goldenFile: "search_object_values_with_query.json",
		},
		{
			query:      "income",
			types:      []string{"StatisticalVariable"},
			goldenFile: "search_object_values_with_query_and_type.json",
		},
		{
			query:      "ether",
			predicates: []string{"alternateName"},
			goldenFile: "search_object_values_with_query_and_predicates.json",
		},
		{
			query:      "ether",
			predicates: []string{"alternateName"},
			types:      []string{"ChemicalCompound"},
			goldenFile: "search_object_values_with_query_predicates_and_types.json",
		},
	} {
		actual, err := client.SearchObjectValues(ctx, c.query, c.predicates, c.types)
		if err != nil {
			t.Fatalf("TestObjectValues error (%v): %v", c.goldenFile, err)
		}

		actual = simplifySearchNodes(actual)

		got, err := test.StructToJSON(actual)
		if err != nil {
			t.Fatalf("StructToJSON error (%v): %v", c.goldenFile, err)
		}

		if test.GenerateGolden {
			err = test.WriteGolden(got, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			continue
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.goldenFile, diff)
		}
	}

}

// simplifySearchNodes simplifies search results for goldens.
func simplifySearchNodes(results []*spanner.SearchNode) []*spanner.SearchNode {
	if len(results) > NUM_MATCHES {
		results = results[:NUM_MATCHES]
	}

	for _, r := range results {
		r.Score = 0
	}

	return results
}

// simplifyNodes simplifies Node results for goldens.
func simplifyNodes(results map[string][]*spanner.Edge) map[string][]*spanner.Edge {
	filtered := map[string][]*spanner.Edge{}
	for subject_id, edges := range results {
		if len(edges) > NUM_MATCHES {
			edges = edges[:NUM_MATCHES]
		}
		filtered[subject_id] = edges
	}
	return filtered
}
