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

package golden

var multiEntityObservationsTestCases = []struct {
	name      string
	variables []string
	entities  []string
	date      string
	golden    string
}{
	{
		name:      "basic variables and entities",
		variables: []string{"AirPollutant_Cancer_Risk"},
		entities:  []string{"geoId/01001", "geoId/02013"},
		date:      "",
		golden:    "get_multientity_obs_basic",
	},
	{
		name:      "latest date with variables and entities",
		variables: []string{"AirPollutant_Cancer_Risk"},
		entities:  []string{"geoId/01001", "geoId/02013"},
		date:      "latest",
		golden:    "get_multientity_obs_latest",
	},
	{
		name:      "specific date with variables and entities",
		variables: []string{"AirPollutant_Cancer_Risk"},
		entities:  []string{"geoId/01001", "geoId/02013"},
		date:      "2015",
		golden:    "get_multientity_obs_date",
	},
	{
		name:      "entities only",
		variables: []string{},
		entities:  []string{"geoId/01001", "geoId/02013"},
		date:      "",
		golden:    "get_multientity_obs_entities_only",
	},
}

var multiEntityCheckVariableExistenceTestCases = []struct {
	name      string
	variables []string
	entities  []string
	golden    string
}{
	{
		name:      "both variables and entities specified",
		variables: []string{"AirPollutant_Cancer_Risk"},
		entities:  []string{"geoId/01001", "geoId/02013"},
		golden:    "check_multientity_var_existence_both",
	},
	{
		name:      "variables only specified",
		variables: []string{"AirPollutant_Cancer_Risk"},
		entities:  []string{},
		golden:    "check_multientity_var_existence_vars_only",
	},
	{
		name:      "entities only specified",
		variables: []string{},
		entities:  []string{"geoId/01001", "geoId/02013"},
		golden:    "check_multientity_var_existence_entities_only",
	},
}

var multiEntityObservationsContainedInPlaceTestCases = []struct {
	name           string
	variables      []string
	ancestor       string
	childPlaceType string
	date           string
	golden         string
}{
	{
		name:           "contained in place with variables",
		variables:      []string{"AirPollutant_Cancer_Risk"},
		ancestor:       "geoId/10",
		childPlaceType: "County",
		date:           "",
		golden:         "get_multientity_obs_contained_in_place_both",
	},
	{
		name:           "contained in place latest date with variables",
		variables:      []string{"AirPollutant_Cancer_Risk"},
		ancestor:       "geoId/10",
		childPlaceType: "County",
		date:           "latest",
		golden:         "get_multientity_obs_contained_in_place_latest",
	},
	{
		name:           "contained in place entities only",
		variables:      []string{},
		ancestor:       "geoId/10",
		childPlaceType: "County",
		date:           "",
		golden:         "get_multientity_obs_contained_in_place_entities_only",
	},
}

var multiEntityStatVarGroupNodeTestCases = []struct {
	name               string
	nodes              []string
	includeDefinitions bool
	golden             string
}{
	{
		name:   "single stat var group",
		nodes:  []string{"dc/g/Demographics"},
		golden: "get_multientity_stat_var_group_node",
	},
	{
		name:   "multiple stat var groups",
		nodes:  []string{"dc/g/Demographics", "dc/g/Economy"},
		golden: "get_multientity_stat_var_group_node_multi",
	},
	{
		name:               "single stat var group with definitions",
		nodes:              []string{"dc/g/Demographics"},
		includeDefinitions: true,
		golden:             "get_multientity_stat_var_group_node_with_definitions",
	},
}
