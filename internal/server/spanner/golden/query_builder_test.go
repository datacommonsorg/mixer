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
	"fmt"
	"path"
	"runtime"
	"strings"
	"testing"

	cloudSpanner "cloud.google.com/go/spanner"
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
	}{
		{
			ids:        []string{"Count_Person", "Person", "foo"},
			out:        true,
			goldenFile: "get_node_props_by_subject_id.sql",
		},
		{
			ids:        []string{"Count_Person", "Person"},
			out:        false,
			goldenFile: "get_node_props_by_object_id.sql",
		},
	} {
		got := spanner.GetNodePropsQuery(c.ids, c.out)
		interpolated := interpolateSQL(got)

		if test.GenerateGolden {
			err := test.WriteGolden(interpolated, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, interpolated); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", want, diff)
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
	}{
		{
			ids: []string{"Aadhaar", "Monthly_Average_RetailPrice_Electricity_Residential", "foo"},
			arc: &v2.Arc{
				Out:        true,
				SingleProp: "*",
			},
			offset:     0,
			goldenFile: "get_node_edges_by_subject_id.sql",
		},
		{
			ids: []string{"Person"},
			arc: &v2.Arc{
				Out:        true,
				SingleProp: "source",
			},
			offset:     0,
			goldenFile: "get_node_edges_out_single_prop.sql",
		},
		{
			ids: []string{"geoId/5129600"},
			arc: &v2.Arc{
				Out:          true,
				BracketProps: []string{"containedInPlace", "geoJsonCoordinatesDP3"},
			},
			offset:     0,
			goldenFile: "get_node_edges_out_bracket_props.sql",
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
		},
	} {
		got := spanner.GetNodeEdgesByIDQuery(c.ids, c.arc, c.offset)
		interpolated := interpolateSQL(got)

		if test.GenerateGolden {
			err := test.WriteGolden(interpolated, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, interpolated); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", want, diff)
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
	}{
		{
			ids: []string{"FireIncidentTypeEnum", "FoodTypeEnum"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "*",
			},
			offset:     0,
			goldenFile: "get_node_edges_by_object_id.sql",
		},
		{
			ids: []string{"EarthquakeEvent"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "domainIncludes",
			},
			offset:     0,
			goldenFile: "get_node_edges_in_single_prop.sql",
		},
		{
			ids: []string{"EarthquakeEvent"},
			arc: &v2.Arc{
				Out:          false,
				BracketProps: []string{"domainIncludes", "naturalHazardType"},
			},
			offset:     0,
			goldenFile: "get_node_edges_in_bracket_props.sql",
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
		},
		{
			ids: []string{"StatisticalVariable"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "typeOf",
			},
			offset:     0,
			goldenFile: "get_node_edges_first_page.sql",
		},
		{
			ids: []string{"StatisticalVariable"},
			arc: &v2.Arc{
				Out:        false,
				SingleProp: "typeOf",
			},
			offset:     spanner.PAGE_SIZE,
			goldenFile: "get_node_edges_second_page.sql",
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
		},
	} {
		got := spanner.GetNodeEdgesByIDQuery(c.ids, c.arc, c.offset)
		interpolated := interpolateSQL(got)

		if test.GenerateGolden {
			err := test.WriteGolden(interpolated, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, interpolated); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", want, diff)
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
	}{
		{
			variables:  []string{"AirPollutant_Cancer_Risk"},
			entities:   []string{"geoId/01001", "geoId/02013"},
			goldenFile: "get_observations.sql",
		},
		{
			entities:   []string{"wikidataId/Q341968"},
			goldenFile: "get_observations_entity.sql",
		},
	} {
		got := spanner.GetObservationsQuery(c.variables, c.entities)
		interpolated := interpolateSQL(got)

		if test.GenerateGolden {
			err := test.WriteGolden(interpolated, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, interpolated); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", want, diff)
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
	}{
		{
			variables:        []string{"Count_Person", "Median_Age_Person"},
			containedInPlace: &v2.ContainedInPlace{Ancestor: "geoId/10", ChildPlaceType: "County"},
			goldenFile:       "get_observations_contained_in.sql",
		},
	} {
		got := spanner.GetObservationsContainedInPlaceQuery(c.variables, c.containedInPlace)
		interpolated := interpolateSQL(got)

		if test.GenerateGolden {
			err := test.WriteGolden(interpolated, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, interpolated); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", want, diff)
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
	}{
		{
			query:      "income",
			types:      []string{"StatisticalVariable"},
			goldenFile: "search_nodes_with_type.sql",
		},
		{
			query:      "income",
			goldenFile: "search_nodes_without_type.sql",
		},
	} {
		got := spanner.SearchNodesQuery(c.query, c.types)
		interpolated := interpolateSQL(got)

		if test.GenerateGolden {
			err := test.WriteGolden(interpolated, goldenDir, c.goldenFile)
			if err != nil {
				t.Fatalf("WriteGolden error (%v): %v", c.goldenFile, err)
			}
			return
		}

		want, err := test.ReadGolden(goldenDir, c.goldenFile)
		if err != nil {
			t.Fatalf("ReadGolden error (%v): %v", c.goldenFile, err)
		}

		if diff := cmp.Diff(want, interpolated); diff != "" {
			t.Errorf("%v payload mismatch (-want +got):\n%s", want, diff)
		}
	}
}

// Replace params with values in SQL. ONLY FOR TESTS.
func interpolateSQL(stmt *cloudSpanner.Statement) string {
	interpolated := stmt.SQL
	for key, value := range stmt.Params {
		var stringValue string
		switch v := value.(type) {
		case string:
			stringValue = fmt.Sprintf("'%s'", v)
			interpolated = strings.Replace(interpolated, "@"+key, stringValue, -1)
		case []string:
			quotedStrings := []string{}
			for _, s := range v {
				quotedStrings = append(quotedStrings, fmt.Sprintf("'%s'", s))
			}
			stringValue = strings.Join(quotedStrings, ",")
			interpolated = strings.Replace(interpolated, "UNNEST(@"+key+")", "("+stringValue+")", -1)
			interpolated = strings.Replace(interpolated, "@"+key, "["+stringValue+"]", -1)
		// Add other types as needed
		default:
			stringValue = fmt.Sprintf("%v", v)
			interpolated = strings.Replace(interpolated, "@"+key, stringValue, -1)
		}
	}
	return interpolated
}
