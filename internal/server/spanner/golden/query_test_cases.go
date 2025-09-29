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

// Shared test cases for query_test and query_builder_test.
package golden

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
)

// The goldenTestFunc type represents a function that can be tested with the golden file pattern.
// It returns the actual result as an interface and an error.
type goldenTestFunc func(ctx context.Context) (interface{}, error)

var nodePropsTestCases = []struct {
	ids    []string
	out    bool
	golden string
}{
	{
		ids:    []string{"Count_Person", "Person", "foo"},
		out:    true,
		golden: "get_node_props_by_subject_id",
	},
	{
		ids:    []string{"Count_Person", "Person"},
		out:    false,
		golden: "get_node_props_by_object_id",
	},
}

var nodeOutEdgesByIDTestCases = []struct {
	ids    []string
	arc    *v2.Arc
	offset int32
	golden string
}{
	{
		ids: []string{"Aadhaar", "Monthly_Average_RetailPrice_Electricity_Residential", "foo"},
		arc: &v2.Arc{
			Out:        true,
			SingleProp: "*",
		},
		offset: 0,
		golden: "get_node_edges_by_subject_id",
	},
	{
		ids: []string{"Person"},
		arc: &v2.Arc{
			Out:        true,
			SingleProp: "source",
		},
		offset: 0,
		golden: "get_node_edges_out_single_prop",
	},
	{
		ids: []string{"geoId/5129600"},
		arc: &v2.Arc{
			Out:          true,
			BracketProps: []string{"containedInPlace", "geoJsonCoordinatesDP3"},
		},
		offset: 0,
		golden: "get_node_edges_out_bracket_props",
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
		offset: 0,
		golden: "get_node_edges_out_filter",
	},
	{
		ids: []string{"dc/g/Person_Gender"},
		arc: &v2.Arc{
			Out:        true,
			SingleProp: "specializationOf",
			Decorator:  "+",
		},
		offset: 0,
		golden: "get_node_edges_out_chain",
	},
}

var nodeInEdgesByIDTestCases = []struct {
	ids    []string
	arc    *v2.Arc
	offset int32
	golden string
}{
	{
		ids: []string{"FireIncidentTypeEnum", "FoodTypeEnum"},
		arc: &v2.Arc{
			Out:        false,
			SingleProp: "*",
		},
		offset: 0,
		golden: "get_node_edges_by_object_id",
	},
	{
		ids: []string{"EarthquakeEvent"},
		arc: &v2.Arc{
			Out:        false,
			SingleProp: "domainIncludes",
		},
		offset: 0,
		golden: "get_node_edges_in_single_prop",
	},
	{
		ids: []string{"EarthquakeEvent"},
		arc: &v2.Arc{
			Out:          false,
			BracketProps: []string{"domainIncludes", "naturalHazardType"},
		},
		offset: 0,
		golden: "get_node_edges_in_bracket_props",
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
		offset: 0,
		golden: "get_node_edges_in_filter",
	},
	{
		ids: []string{"dc/g/Farm_FarmInventoryStatus"},
		arc: &v2.Arc{
			Out:        false,
			SingleProp: "specializationOf",
			Decorator:  "+",
		},
		offset: 0,
		golden: "get_node_edges_in_chain",
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
		offset: 0,
		golden: "get_node_edges_malicious",
	},
	{
		ids: []string{"StatisticalVariable"},
		arc: &v2.Arc{
			Out:        false,
			SingleProp: "typeOf",
		},
		offset: 0,
		golden: "get_node_edges_first_page",
	},
	{
		ids: []string{"StatisticalVariable"},
		arc: &v2.Arc{
			Out:        false,
			SingleProp: "typeOf",
		},
		offset: spanner.PAGE_SIZE,
		golden: "get_node_edges_second_page",
	},
	{
		ids: []string{"dc/g/UN"},
		arc: &v2.Arc{
			Out:        false,
			SingleProp: "specializationOf",
			Decorator:  "+",
		},
		offset: 0,
		golden: "get_node_edges_first_page_chain",
	},
	{
		ids: []string{"dc/g/UN"},
		arc: &v2.Arc{
			Out:        false,
			SingleProp: "specializationOf",
			Decorator:  "+",
		},
		offset: spanner.PAGE_SIZE,
		golden: "get_node_edges_second_page_chain",
	},
}

var observationsTestCases = []struct {
	variables []string
	entities  []string
	golden    string
}{
	{
		variables: []string{"AirPollutant_Cancer_Risk"},
		entities:  []string{"geoId/01001", "geoId/02013"},
		golden:    "get_observations",
	},
	{
		entities: []string{"wikidataId/Q341968"},
		golden:   "get_observations_entity",
	},
}

var observationsContainedInPlaceTestCases = []struct {
	variables        []string
	containedInPlace *v2.ContainedInPlace
	golden           string
}{
	{
		variables:        []string{"Count_Person", "Median_Age_Person"},
		containedInPlace: &v2.ContainedInPlace{Ancestor: "geoId/10", ChildPlaceType: "County"},
		golden:           "get_observations_contained_in",
	},
}

var searchNodesTestCases = []struct {
	query  string
	types  []string
	golden string
}{
	{
		query:  "income",
		types:  []string{"StatisticalVariable"},
		golden: "search_nodes_with_type",
	},
	{
		query:  "income",
		golden: "search_nodes_without_type",
	},
}

var resolveByIDTestCases = []struct {
	nodes  []string
	in     string
	out    string
	golden string
}{
	{
		nodes:  []string{"country/USA", "undata-geo:G00003340", "Count_Person", "foo"},
		in:     "dcid",
		out:    "dcid",
		golden: "resolve_dcid_to_dcid",
	},
	{
		nodes:  []string{"country/USA", "undata-geo:G00003340", "Count_Person", "foo"},
		in:     "dcid",
		out:    "unDataCode",
		golden: "resolve_dcid_to_prop",
	},
	{
		nodes:  []string{"country/USA", "undata-geo:G00003340", "Count_Person", "foo"},
		in:     "unDataCode",
		out:    "dcid",
		golden: "resolve_prop_to_dcid",
	},
	{
		nodes:  []string{"country/USA", "undata-geo:G00003340", "Count_Person", "foo"},
		in:     "unDataCode",
		out:    "wikidataId",
		golden: "resolve_prop_to_prop",
	},
}
