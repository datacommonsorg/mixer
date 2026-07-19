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
	"github.com/datacommonsorg/mixer/internal/server/sdmx/datacommons"
)

func sdmxComponentConstraint(values ...string) *sdmxpb.SdmxComponentConstraint {
	predicates := make([]*sdmxpb.SdmxPredicate, 0, len(values))
	for _, value := range values {
		predicates = append(predicates, &sdmxpb.SdmxPredicate{Value: value})
	}
	return &sdmxpb.SdmxComponentConstraint{Predicates: predicates}
}

func sdmxContainedInPlaceConstraint(ancestor, childPlaceType string) *sdmxpb.SdmxComponentConstraint {
	return &sdmxpb.SdmxComponentConstraint{
		PropertyConstraints: map[string]*sdmxpb.SdmxPropertyConstraint{
			"containedInPlace": {
				Predicates: []*sdmxpb.SdmxPredicate{{Value: ancestor}},
				Transitive: true,
			},
			"typeOf": {Predicates: []*sdmxpb.SdmxPredicate{{Value: childPlaceType}}},
		},
	}
}

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
	name                            string
	constraints                     map[string]*sdmxpb.SdmxComponentConstraint
	observationPropertyToEntitySlot map[string]string
	containedInPlaceToRemoteDCIDs   map[datacommons.ContainedInPlaceConstraint][]string
	golden                          string
}{
	{
		name: "variable measured only",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
		},
		observationPropertyToEntitySlot: map[string]string{},
		golden:                          "get_sdmx_obs_var_only",
	},
	{
		name: "variable measured and origin slot",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"origin":           sdmxComponentConstraint("country/AGO"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"origin": "entity1",
		},
		golden: "get_sdmx_obs_var_and_origin",
	},
	{
		name: "variable measured and multiple slots (origin + destination)",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"origin":           sdmxComponentConstraint("country/AGO"),
			"destination":      sdmxComponentConstraint("country/PRT", "country/SGP"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"origin": "entity1", "destination": "entity2",
		},
		golden: "get_sdmx_obs_slots_slicing",
	},
	{
		name: "multiple variables with common slot mapping",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1", "var2"),
			"origin":           sdmxComponentConstraint("country/AGO"),
			"destination":      sdmxComponentConstraint("country/PRT"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"origin": "entity1", "destination": "entity2",
		},
		golden: "get_sdmx_obs_multi_var_slots",
	},
	{
		name: "variable measured, origin, destination and physical column filters",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured":  sdmxComponentConstraint("var1"),
			"origin":            sdmxComponentConstraint("country/AGO", "country/BRA"),
			"destination":       sdmxComponentConstraint("country/PRT", "country/SGP"),
			"facetId":           sdmxComponentConstraint("facet", "alternate-facet"),
			"measurementMethod": sdmxComponentConstraint("Census", "Survey"),
			"observationPeriod": sdmxComponentConstraint("P1Y", "P1M"),
			"provenance":        sdmxComponentConstraint("dc/base/WTO_TradeConnectivity", "dc/base/UN_Trade"),
			"unit":              sdmxComponentConstraint("Percent", "Count"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"origin": "entity1", "destination": "entity2",
		},
		golden: "get_sdmx_obs_with_facet_and_prov",
	},
	{
		name: "single-entity variable with observationAbout",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"observationAbout": sdmxComponentConstraint("wikidataId/Q119158"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"observationAbout": "entity1",
		},
		golden: "get_sdmx_obs_single_entity",
	},
	{
		name: "explicit time periods",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"observationAbout": sdmxComponentConstraint("wikidataId/Q119158"),
			"TIME_PERIOD":      sdmxComponentConstraint("2022", "2020", "2022"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"observationAbout": "entity1",
		},
		golden: "get_sdmx_obs_explicit_time_periods",
	},
	{
		name: "latest time period",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"observationAbout": sdmxComponentConstraint("wikidataId/Q119158"),
			"TIME_PERIOD":      sdmxComponentConstraint("latest"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"observationAbout": "entity1",
		},
		golden: "get_sdmx_obs_latest_time_period",
	},
	{
		name: "contained observation about on entity1",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"observationAbout": sdmxContainedInPlaceConstraint("country/USA", "County"),
		},
		observationPropertyToEntitySlot: map[string]string{"observationAbout": "entity1"},
		golden:                          "get_sdmx_obs_contained_entity1",
	},
	{
		name: "contained observation about with explicit time periods",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"observationAbout": sdmxContainedInPlaceConstraint("country/USA", "County"),
			"TIME_PERIOD":      sdmxComponentConstraint("2020", "2022"),
		},
		observationPropertyToEntitySlot: map[string]string{"observationAbout": "entity1"},
		golden:                          "get_sdmx_obs_contained_entity1_explicit_time_periods",
	},
	{
		name: "contained observation about with latest time period",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"observationAbout": sdmxContainedInPlaceConstraint("country/USA", "County"),
			"TIME_PERIOD":      sdmxComponentConstraint("LATEST"),
		},
		observationPropertyToEntitySlot: map[string]string{"observationAbout": "entity1"},
		golden:                          "get_sdmx_obs_contained_entity1_latest_time_period",
	},
	{
		name: "contained source on entity2 with direct entity1 filter",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured":   sdmxComponentConstraint("var1"),
			"destinationCountry": sdmxComponentConstraint("country/CAN"),
			"sourceCountry":      sdmxContainedInPlaceConstraint("country/USA", "State"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"destinationCountry": "entity1", "sourceCountry": "entity2",
		},
		golden: "get_sdmx_obs_contained_entity2",
	},
	{
		name: "contained transport mode on entity3",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"transportMode":    sdmxContainedInPlaceConstraint("northamerica", "TransportMode"),
		},
		observationPropertyToEntitySlot: map[string]string{"transportMode": "entity3"},
		golden:                          "get_sdmx_obs_contained_entity3",
	},
	{
		name: "entity3 anchors before entity2 and reuses place set",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"middle":           sdmxContainedInPlaceConstraint("country/USA", "State"),
			"last":             sdmxContainedInPlaceConstraint("country/USA", "State"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"first": "entity1", "middle": "entity2", "last": "entity3",
		},
		golden: "get_sdmx_obs_contained_entity3_before_entity2",
	},
	{
		name: "entity3 anchors before entity2 and reuses remote place set",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"middle":           sdmxContainedInPlaceConstraint("country/USA", "State"),
			"last":             sdmxContainedInPlaceConstraint("country/USA", "State"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"first": "entity1", "middle": "entity2", "last": "entity3",
		},
		containedInPlaceToRemoteDCIDs: map[datacommons.ContainedInPlaceConstraint][]string{
			{Ancestor: "country/USA", ChildPlaceType: "State"}: {"country/CAN", "country/USA"},
		},
		golden: "get_sdmx_obs_contained_entity3_before_entity2_remote",
	},
	{
		name: "entity3 remote place set with latest time period",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"middle":           sdmxContainedInPlaceConstraint("country/USA", "State"),
			"last":             sdmxContainedInPlaceConstraint("country/USA", "State"),
			"TIME_PERIOD":      sdmxComponentConstraint("LATEST"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"first": "entity1", "middle": "entity2", "last": "entity3",
		},
		containedInPlaceToRemoteDCIDs: map[datacommons.ContainedInPlaceConstraint][]string{
			{Ancestor: "country/USA", ChildPlaceType: "State"}: {"country/CAN", "country/USA"},
		},
		golden: "get_sdmx_obs_contained_entity3_before_entity2_remote_latest_time_period",
	},
	{
		name: "entity1 anchors entity2 and entity3 place sets",
		constraints: map[string]*sdmxpb.SdmxComponentConstraint{
			"variableMeasured": sdmxComponentConstraint("var1"),
			"first":            sdmxContainedInPlaceConstraint("country/CAN", "Province"),
			"middle":           sdmxContainedInPlaceConstraint("northamerica", "Country"),
			"last":             sdmxContainedInPlaceConstraint("country/USA", "State"),
		},
		observationPropertyToEntitySlot: map[string]string{
			"first": "entity1", "middle": "entity2", "last": "entity3",
		},
		golden: "get_sdmx_obs_contained_entity1_multiple_sets",
	},
}
