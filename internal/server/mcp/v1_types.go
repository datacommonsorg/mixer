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

package mcp

// SearchIndicatorsInput holds arguments for the search_indicators MCP tool.
type SearchIndicatorsInput struct {
	Query          string   `json:"query"`
	Places         []string `json:"places,omitempty"`
	PerSearchLimit int      `json:"per_search_limit,omitempty" default:"10"`
	IncludeTopics  *bool    `json:"include_topics,omitempty" default:"true"`
}

// SearchChildIndicatorsInput holds arguments for the search_child_indicators MCP tool.
type SearchChildIndicatorsInput struct {
	Query             string   `json:"query"`
	ParentPlace       string   `json:"parent_place"`
	SampleChildPlaces []string `json:"sample_child_places"`
	PerSearchLimit    int      `json:"per_search_limit,omitempty" default:"10"`
	IncludeTopics     *bool    `json:"include_topics,omitempty" default:"true"`
}

// GetVariableMetadataInput holds arguments for the get_variable_metadata MCP tool.
type GetVariableMetadataInput struct {
	VariableDcids []string `json:"variable_dcids"`
	EntityDcids   []string `json:"entity_dcids"`
}

// GetObservationsInput holds arguments for the get_observations MCP tool.
type GetObservationsInput struct {
	VariableDcid   string `json:"variable_dcid"`
	PlaceDcid      string `json:"place_dcid"`
	SourceOverride string `json:"source_override,omitempty"`
	Date           string `json:"date,omitempty" default:"latest"`
	DateRangeStart string `json:"date_range_start,omitempty"`
	DateRangeEnd   string `json:"date_range_end,omitempty"`
}

// GetChildObservationsInput holds arguments for the get_child_observations MCP tool.
type GetChildObservationsInput struct {
	VariableDcid    string `json:"variable_dcid"`
	ParentPlaceDcid string `json:"parent_place_dcid"`
	ChildPlaceType  string `json:"child_place_type"`
	SourceOverride  string `json:"source_override,omitempty"`
	Date            string `json:"date,omitempty" default:"latest"`
	DateRangeStart  string `json:"date_range_start,omitempty"`
	DateRangeEnd    string `json:"date_range_end,omitempty"`
}

// GetMultiEntityObservationsInput holds arguments for the get_multi_entity_observations MCP tool.
type GetMultiEntityObservationsInput struct {
	VariableDcid         string              `json:"variable_dcid"`
	Entities             map[string][]string `json:"entities"`
	ParentEntityProperty string              `json:"parent_entity_property,omitempty"`
	ParentEntityDcid     string              `json:"parent_entity_dcid,omitempty"`
	ChildEntityType      string              `json:"child_entity_type,omitempty"`
	SourceOverride       string              `json:"source_override,omitempty"`
	Date                 string              `json:"date,omitempty" default:"latest"`
	DateRangeStart       string              `json:"date_range_start,omitempty"`
	DateRangeEnd         string              `json:"date_range_end,omitempty"`
}
