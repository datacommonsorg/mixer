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

package golden

import (
	"path"
	"runtime"
	"testing"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
)

func TestGetNodePropsQuery(t *testing.T) {
	t.Parallel()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query_builder")

	for _, c := range []struct {
		ids        []string
		out        bool
		goldenFile string
		wantParams map[string]interface{}
	}{
		{
			ids:        []string{"Count_Person", "Person", "foo"},
			out:        true,
			goldenFile: "get_node_props_by_subject_id.sql",
			wantParams: map[string]interface{}{
				"ids": []string{"Count_Person", "Person", "foo"},
			},
		},
		{
			ids:        []string{"Count_Person", "Person"},
			out:        false,
			goldenFile: "get_node_props_by_object_id.sql",
			wantParams: map[string]interface{}{
				"ids": []string{"Count_Person", "Person"},
			},
		},
	} {
		got := spanner.GetNodePropsQuery(c.ids, c.out)

		if test.GenerateGolden {
			err := test.WriteGolden(got.SQL, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		wantSQL, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(wantSQL, got.SQL); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", wantSQL, diff)
		}

		if diff := cmp.Diff(c.wantParams, got.Params); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.wantParams, diff)
		}
	}
}

func TestGetNodeOutEdgesByIDQuery(t *testing.T) {
	t.Parallel()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query_builder")

	for _, c := range []struct {
		ids        []string
		arc        *v2.Arc
		offset     int32
		goldenFile string
		wantParams map[string]interface{}
	}{
		{
			ids: []string{"Aadhaar", "Monthly_Average_RetailPrice_Electricity_Residential", "foo"},
			arc: &v2.Arc{
				Out:        true,
				SingleProp: "*",
			},
			offset:     0,
			goldenFile: "get_node_edges_by_subject_id.sql",
			wantParams: map[string]interface{}{
				"ids": []string{"Aadhaar", "Monthly_Average_RetailPrice_Electricity_Residential", "foo"},
			},
		},
		{
			ids: []string{"Person"},
			arc: &v2.Arc{
				Out:        true,
				SingleProp: "source",
			},
			offset:     0,
			goldenFile: "get_node_edges_out_single_prop.sql",
			wantParams: map[string]interface{}{
				"ids":        []string{"Person"},
				"predicates": []string{"source"},
			},
		},
		{
			ids: []string{"geoId/5129600"},
			arc: &v2.Arc{
				Out:          true,
				BracketProps: []string{"containedInPlace", "geoJsonCoordinatesDP3"},
			},
			offset:     0,
			goldenFile: "get_node_edges_out_bracket_props.sql",
			wantParams: map[string]interface{}{
				"ids":        []string{"geoId/5129600"},
				"predicates": []string{"containedInPlace", "geoJsonCoordinatesDP3"},
			},
		},
		{
			ids: []string{"nuts/UKI1"},
			arc: &v2.Arc{
				Out: true,
				Filter: map[string][]string{
					"subClassOf": {"AdministrativeArea"},
					"name":       {"AdministrativeArea2"},
				},
			},
			offset:     0,
			goldenFile: "get_node_edges_out_filter.sql",
			wantParams: map[string]interface{}{
				"ids":   []string{"nuts/UKI1"},
				"prop0": "name",
				"val0":  []string{"AdministrativeArea2", "4cB0ui47vrAeY7MO/uBAvpsajxkYlJo3EW8fStdW4ko="},
				"prop1": "subClassOf",
				"val1":  []string{"AdministrativeArea", "WXALAhw8j+Uz/Tw7uR3ClTolVepyj0tjRCKr6Xkw60s="},
			},
		},
		{
			ids: []string{"dc/g/Person_Gender"},
			arc: &v2.Arc{
				Out:        true,
				SingleProp: "specializationOf",
				Decorator:  "+",
			},
			offset:     0,
			goldenFile: "get_node_edges_out_chain.sql",
			wantParams: map[string]interface{}{
				"ids":              []string{"dc/g/Person_Gender"},
				"predicate":        "specializationOf",
				"result_predicate": "specializationOf+",
			},
		},
	} {
		got := spanner.GetNodeEdgesByIDQuery(c.ids, c.arc, c.offset)

		if test.GenerateGolden {
			err := test.WriteGolden(got.SQL, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		wantSQL, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(wantSQL, got.SQL); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", wantSQL, diff)
		}

		if diff := cmp.Diff(c.wantParams, got.Params); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.wantParams, diff)
		}
	}
}

func TestGetNodeInEdgesByIDQuery(t *testing.T) {
	t.Parallel()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query_builder")

	for _, c := range []struct {
		ids        []string
		arc        *v2.Arc
		offset     int32
		goldenFile string
		wantParams map[string]interface{}
	}{
		{
			ids: []string{"FireIncidentTypeEnum", "FoodTypeEnum"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "*",
			},
			offset:     0,
			goldenFile: "get_node_edges_by_object_id.sql",
			wantParams: map[string]interface{}{
				"ids": []string{"FireIncidentTypeEnum", "FoodTypeEnum"},
			},
		},
		{
			ids: []string{"EarthquakeEvent"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "domainIncludes",
			},
			offset:     0,
			goldenFile: "get_node_edges_in_single_prop.sql",
			wantParams: map[string]interface{}{
				"ids":        []string{"EarthquakeEvent"},
				"predicates": []string{"domainIncludes"},
			},
		},
		{
			ids: []string{"EarthquakeEvent"},
			arc: &v2.Arc{
				Out:          false,
				BracketProps: []string{"domainIncludes", "naturalHazardType"},
			},
			offset:     0,
			goldenFile: "get_node_edges_in_bracket_props.sql",
			wantParams: map[string]interface{}{
				"ids":        []string{"EarthquakeEvent"},
				"predicates": []string{"domainIncludes", "naturalHazardType"},
			},
		},
		{
			ids: []string{"Farm"},
			arc: &v2.Arc{
				Out: false,
				Filter: map[string][]string{
					"farmInventoryType": {"Melon"},
					"name":              {"Area of Farm: Melon"},
				},
			},
			offset:     0,
			goldenFile: "get_node_edges_in_filter.sql",
			wantParams: map[string]interface{}{
				"ids":   []string{"Farm"},
				"prop0": "farmInventoryType",
				"val0":  []string{"Melon", "mxuMmhySOejKGXRXFbMXdorKlNV934EOop6b21kOJGw="},
				"prop1": "name",
				"val1":  []string{"Area of Farm: Melon", "xblU8pfFl5m+cg9tsR1EsW19+PLlpqfNhwYkFu0mgzE="},
			},
		},
		{
			ids: []string{"dc/g/Farm_FarmInventoryStatus"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "specializationOf",
				Decorator:  "+",
			},
			offset:     0,
			goldenFile: "get_node_edges_in_chain.sql",
			wantParams: map[string]interface{}{
				"ids":              []string{"dc/g/Farm_FarmInventoryStatus"},
				"predicate":        "specializationOf",
				"result_predicate": "specializationOf+",
			},
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
			offset:     0,
			goldenFile: "get_node_edges_malicious.sql",
			wantParams: map[string]interface{}{
				"ids":        []string{"foo OR 1=1;"},
				"predicates": []string{"foo OR 1=1;"},
				"prop0":      "foo OR 1=1;",
				"val0":       []string{"foo OR 1=1;", "OG7012T2qe10jzYRBvG6dgUEx5fj7uIxT+RkGvxpn/U="},
			},
		},
		{
			ids: []string{"StatisticalVariable"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "typeOf",
			},
			offset:     0,
			goldenFile: "get_node_edges_first_page.sql",
			wantParams: map[string]interface{}{
				"ids":        []string{"StatisticalVariable"},
				"predicates": []string{"typeOf"},
			},
		},
		{
			ids: []string{"StatisticalVariable"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "typeOf",
			},
			offset:     spanner.PAGE_SIZE,
			goldenFile: "get_node_edges_second_page.sql",
			wantParams: map[string]interface{}{
				"ids":        []string{"StatisticalVariable"},
				"predicates": []string{"typeOf"},
			},
		},
		{
			ids: []string{"dc/g/UN"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "specializationOf",
				Decorator:  "+",
			},
			offset:     0,
			goldenFile: "get_node_edges_first_page_chain.sql",
			wantParams: map[string]interface{}{
				"ids":              []string{"dc/g/UN"},
				"predicate":        "specializationOf",
				"result_predicate": "specializationOf+",
			},
		},
		{
			ids: []string{"dc/g/UN"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "specializationOf",
				Decorator:  "+",
			},
			offset:     spanner.PAGE_SIZE,
			goldenFile: "get_node_edges_second_page_chain.sql",
			wantParams: map[string]interface{}{
				"ids":              []string{"dc/g/UN"},
				"predicate":        "specializationOf",
				"result_predicate": "specializationOf+",
			},
		},
	} {
		got := spanner.GetNodeEdgesByIDQuery(c.ids, c.arc, c.offset)

		if test.GenerateGolden {
			err := test.WriteGolden(got.SQL, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		wantSQL, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(wantSQL, got.SQL); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", wantSQL, diff)
		}

		if diff := cmp.Diff(c.wantParams, got.Params); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.wantParams, diff)
		}
	}
}

func TestGetObservationsQuery(t *testing.T) {
	t.Parallel()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query_builder")

	for _, c := range []struct {
		variables  []string
		entities   []string
		goldenFile string
		wantParams map[string]interface{}
	}{
		{
			variables:  []string{"AirPollutant_Cancer_Risk"},
			entities:   []string{"geoId/01001", "geoId/02013"},
			goldenFile: "get_observations.sql",
			wantParams: map[string]interface{}{
				"entities":  []string{"geoId/01001", "geoId/02013"},
				"variables": []string{"AirPollutant_Cancer_Risk"},
			},
		},
		{
			entities:   []string{"wikidataId/Q341968"},
			goldenFile: "get_observations_entity.sql",
			wantParams: map[string]interface{}{
				"entities": []string{"wikidataId/Q341968"},
			},
		},
	} {
		got := spanner.GetObservationsQuery(c.variables, c.entities)

		if test.GenerateGolden {
			err := test.WriteGolden(got.SQL, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		wantSQL, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(wantSQL, got.SQL); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", wantSQL, diff)
		}

		if diff := cmp.Diff(c.wantParams, got.Params); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.wantParams, diff)
		}
	}
}

func TestGetObservationsContainedInPlaceQuery(t *testing.T) {
	t.Parallel()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query_builder")

	for _, c := range []struct {
		variables        []string
		containedInPlace *v2.ContainedInPlace
		goldenFile       string
		wantParams       map[string]interface{}
	}{
		{
			variables:        []string{"Count_Person", "Median_Age_Person"},
			containedInPlace: &v2.ContainedInPlace{Ancestor: "geoId/10", ChildPlaceType: "County"},
			goldenFile:       "get_observations_contained_in.sql",
			wantParams: map[string]interface{}{
				"ancestor":       "geoId/10",
				"childPlaceType": "County",
				"variables":      []string{"Count_Person", "Median_Age_Person"},
			},
		},
	} {
		got := spanner.GetObservationsContainedInPlaceQuery(c.variables, c.containedInPlace)

		if test.GenerateGolden {
			err := test.WriteGolden(got.SQL, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		wantSQL, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(wantSQL, got.SQL); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", wantSQL, diff)
		}

		if diff := cmp.Diff(c.wantParams, got.Params); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.wantParams, diff)
		}
	}
}

func TestSearchNodesQuery(t *testing.T) {
	t.Parallel()
	_, filename, _, _ := runtime.Caller(0)
	goldenDir := path.Join(path.Dir(filename), "query_builder")

	for _, c := range []struct {
		query      string
		types      []string
		goldenFile string
		wantParams map[string]interface{}
	}{
		{
			query:      "income",
			types:      []string{"StatisticalVariable"},
			goldenFile: "search_nodes_with_type.sql",
			wantParams: map[string]interface{}{
				"query": "income",
				"types": []string{"StatisticalVariable"},
			},
		},
		{
			query:      "income",
			goldenFile: "search_nodes_without_type.sql",
			wantParams: map[string]interface{}{
				"query": "income",
			},
		},
	} {
		got := spanner.SearchNodesQuery(c.query, c.types)

		if test.GenerateGolden {
			err := test.WriteGolden(got.SQL, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		wantSQL, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(wantSQL, got.SQL); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", wantSQL, diff)
		}

		if diff := cmp.Diff(c.wantParams, got.Params); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", c.wantParams, diff)
		}
	}
}
