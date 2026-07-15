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

import (
	sdmxpb "github.com/datacommonsorg/mixer/internal/proto/sdmx"
)

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

var multiEntityCheckGroupPlaceExistenceTestCases = []struct {
	name           string
	variableGroups []string
	entities       []string
	predicate      string
	golden         string
}{
	{
		name:           "stat var groups and places",
		variableGroups: []string{"dc/g/Demographics", "dc/g/Economy"},
		entities:       []string{"geoId/01001", "geoId/02013"},
		predicate:      "linkedMemberOf",
		golden:         "check_multientity_group_place_existence_svg",
	},
	{
		name:           "topics and places",
		variableGroups: []string{"dc/t/Place/Population"},
		entities:       []string{"geoId/01001", "geoId/02013"},
		predicate:      "linkedMember",
		golden:         "check_multientity_group_place_existence_topic",
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
		name:           "contained in place specific date with variables",
		variables:      []string{"AirPollutant_Cancer_Risk", "Count_Person"},
		ancestor:       "geoId/10",
		childPlaceType: "County",
		date:           "2015",
		golden:         "get_multientity_obs_contained_in_place_date",
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

var multiEntityFilteredSVGChildrenTestCases = []struct {
	name                  string
	template              string
	node                  string
	constrainedPlaces     []string
	constrainedProvenance string
	numEntitiesExistence  int
	includeDefinitions    bool
	golden                string
}{
	{
		name:                 "stat vars filtered by places",
		template:             "SV",
		node:                 "dc/g/Demographics",
		constrainedPlaces:    []string{"country/USA", "country/IND"},
		numEntitiesExistence: 1,
		golden:               "get_multientity_filtered_sv_places",
	},
	{
		name:                 "stat var groups filtered by places",
		template:             "SVG",
		node:                 "dc/g/Demographics",
		constrainedPlaces:    []string{"country/USA", "country/IND"},
		numEntitiesExistence: 1,
		golden:               "get_multientity_filtered_svg_places",
	},
	{
		name:                  "stat vars filtered by source",
		template:              "SV",
		node:                  "dc/g/Demographics",
		constrainedProvenance: "dc/s/WorldBank",
		numEntitiesExistence:  1,
		golden:                "get_multientity_filtered_sv_import",
	},
	{
		name:                  "stat vars filtered by place and source",
		template:              "SV",
		node:                  "dc/g/Demographics",
		constrainedPlaces:     []string{"country/USA", "country/IND"},
		constrainedProvenance: "dc/s/WorldBank",
		numEntitiesExistence:  1,
		golden:                "get_multientity_filtered_sv_place_import",
	},
	{
		name:                  "stat var groups filtered by source threshold",
		template:              "SVG",
		node:                  "dc/g/Demographics",
		constrainedProvenance: "dc/s/WorldBank",
		numEntitiesExistence:  2,
		golden:                "get_multientity_filtered_svg_import_num_entities_existence",
	},
	{
		name:                 "stat var groups filtered by place threshold",
		template:             "SVG",
		node:                 "dc/g/Demographics",
		constrainedPlaces:    []string{"country/USA", "country/IND", "country/CAN"},
		numEntitiesExistence: 2,
		golden:               "get_multientity_filtered_svg_num_entities_existence",
	},
	{
		name:                 "stat vars filtered by places with definitions",
		template:             "SV",
		node:                 "dc/g/Demographics",
		constrainedPlaces:    []string{"country/USA"},
		numEntitiesExistence: 1,
		includeDefinitions:   true,
		golden:               "get_multientity_filtered_sv_with_definitions",
	},
}

var multiEntityFilteredTopicTestCases = []struct {
	name                  string
	nodes                 []string
	constrainedPlaces     []string
	constrainedProvenance string
	numEntitiesExistence  int
	golden                string
}{
	{
		name:                 "topic filtered by places",
		nodes:                []string{"dc/topic/Demographics"},
		constrainedPlaces:    []string{"country/CAN", "country/IND"},
		numEntitiesExistence: 1,
		golden:               "get_multientity_filtered_topic_places",
	},
	{
		name:                 "multiple topics filtered by places",
		nodes:                []string{"dc/topic/Demographics", "dc/topic/Economy"},
		constrainedPlaces:    []string{"country/CAN", "country/IND"},
		numEntitiesExistence: 1,
		golden:               "get_multientity_filtered_topic_places_multiple_topics",
	},
	{
		name:                  "topic filtered by source",
		nodes:                 []string{"dc/topic/Demographics"},
		constrainedProvenance: "dc/s/WorldBank",
		numEntitiesExistence:  1,
		golden:                "get_multientity_filtered_topic_import",
	},
	{
		name:                  "topic filtered by source threshold",
		nodes:                 []string{"dc/topic/Demographics"},
		constrainedProvenance: "dc/s/WorldBank",
		numEntitiesExistence:  2,
		golden:                "get_multientity_filtered_topic_import_num_entities_existence",
	},
	{
		name:                  "topic filtered by dataset",
		nodes:                 []string{"dc/topic/Demographics"},
		constrainedProvenance: "dc/d/TestDataset",
		numEntitiesExistence:  1,
		golden:                "get_multientity_filtered_topic_dataset",
	},
	{
		name:                  "topic filtered by place and source",
		nodes:                 []string{"dc/topic/Demographics"},
		constrainedPlaces:     []string{"country/CAN", "country/IND"},
		constrainedProvenance: "dc/s/WorldBank",
		numEntitiesExistence:  1,
		golden:                "get_multientity_filtered_topic_place_import",
	},
	{
		name:                 "topic filtered by place threshold",
		nodes:                []string{"dc/topic/Demographics"},
		constrainedPlaces:    []string{"country/USA", "country/IND", "country/CAN"},
		numEntitiesExistence: 2,
		golden:               "get_multientity_filtered_topic_num_entities_existence",
	},
}

var multiEntitySdmxObservationsTestCases = []struct {
	name                 string
	constraints          map[string]*sdmxpb.ConstraintList
	entitySlotsByStatVar map[string]map[string]string
	golden               string
}{
	{
		name: "variable measured only",
		constraints: map[string]*sdmxpb.ConstraintList{
			"variableMeasured": {Values: []string{"var1"}},
		},
		entitySlotsByStatVar: map[string]map[string]string{},
		golden:               "get_sdmx_obs_var_only",
	},
	{
		name: "variable measured and origin slot",
		constraints: map[string]*sdmxpb.ConstraintList{
			"variableMeasured": {Values: []string{"var1"}},
			"origin":           {Values: []string{"country/AGO"}},
		},
		entitySlotsByStatVar: map[string]map[string]string{
			"var1": {"origin": "entity1"},
		},
		golden: "get_sdmx_obs_var_and_origin",
	},
	{
		name: "variable measured and multiple slots (origin + destination)",
		constraints: map[string]*sdmxpb.ConstraintList{
			"variableMeasured": {Values: []string{"var1"}},
			"origin":           {Values: []string{"country/AGO"}},
			"destination":      {Values: []string{"country/PRT", "country/SGP"}},
		},
		entitySlotsByStatVar: map[string]map[string]string{
			"var1": {"origin": "entity1", "destination": "entity2"},
		},
		golden: "get_sdmx_obs_slots_slicing",
	},
	{
		name: "multiple variables with different slot mappings",
		constraints: map[string]*sdmxpb.ConstraintList{
			"variableMeasured": {Values: []string{"var1", "var2"}},
			"origin":           {Values: []string{"country/AGO"}},
			"destination":      {Values: []string{"country/PRT"}},
		},
		entitySlotsByStatVar: map[string]map[string]string{
			"var1": {"origin": "entity1", "destination": "entity2"},
			"var2": {"origin": "entity2", "destination": "entity1"}, // reversed mapping for var2
		},
		golden: "get_sdmx_obs_multi_var_slots",
	},
	{
		name: "variable measured, origin, destination and physical column filters",
		constraints: map[string]*sdmxpb.ConstraintList{
			"variableMeasured":  {Values: []string{"var1"}},
			"origin":            {Values: []string{"country/AGO", "country/BRA"}},
			"destination":       {Values: []string{"country/PRT", "country/SGP"}},
			"facetId":           {Values: []string{"facet", "alternate-facet"}},
			"measurementMethod": {Values: []string{"Census", "Survey"}},
			"observationPeriod": {Values: []string{"P1Y", "P1M"}},
			"provenance":        {Values: []string{"dc/base/WTO_TradeConnectivity", "dc/base/UN_Trade"}},
			"unit":              {Values: []string{"Percent", "Count"}},
		},
		entitySlotsByStatVar: map[string]map[string]string{
			"var1": {"origin": "entity1", "destination": "entity2"},
		},
		golden: "get_sdmx_obs_with_facet_and_prov",
	},
	{
		name: "single-entity variable with observationAbout",
		constraints: map[string]*sdmxpb.ConstraintList{
			"variableMeasured": {Values: []string{"var1"}},
			"observationAbout": {Values: []string{"wikidataId/Q119158"}},
		},
		entitySlotsByStatVar: map[string]map[string]string{
			"var1": {"observationAbout": "entity1"},
		},
		golden: "get_sdmx_obs_single_entity",
	},
}
