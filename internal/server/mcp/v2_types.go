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

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/agent"
)

// V2AgentBackend defines the agent.Service methods required by the v2 MCP tools.
type V2AgentBackend interface {
	V2ResolveEntities(ctx context.Context, req *agent.V2ResolveEntitiesRequest) (*agent.V2ResolveEntitiesResponse, error)
	V2SearchIndicators(ctx context.Context, req *agent.V2SearchIndicatorsRequest) (*agent.V2SearchIndicatorsResponse, error)
	V2GetObservations(ctx context.Context, req *agent.V2GetObservationsRequest) (*agent.V2GetObservationsResponse, error)
}

// V2EntityInput is a single entity to resolve in the resolve_entities MCP tool.
// jsonschema tags become per-field descriptions in the tool's input schema shown to the LLM.
type V2EntityInput struct {
	Name                 string   `json:"name" jsonschema:"Fully qualified entity name including its type word and parent places, e.g. 'Santa Clara County, California, United States', 'Washington State, United States', 'Madrid, Spain', 'Lincoln Elementary School, Atlanta, Georgia'."`
	EntityTypeHints      []string `json:"entity_type_hints,omitempty" jsonschema:"Optional entity types, most specific first, e.g. ['state'], ['county', 'administrative area 2'], ['food bank']."`
	ChildEntityTypeHints []string `json:"child_entity_type_hints,omitempty" jsonschema:"Terms for the type of entities contained in this entity, for questions like 'counties of Kenya' (name 'Kenya', hints ['county', 'administrative area 1']). Depending on the country, Data Commons types subdivisions by a specific type (e.g. County, State), a generic administrative level (e.g. AdministrativeArea1), or both, and may not recognize local terms (e.g. 'autonomous community'), so pair the local term with its administrative level. Use at most 3 hints, most specific first."`
}

// V2ResolveEntitiesInput holds arguments for the v2 resolve_entities MCP tool.
type V2ResolveEntitiesInput struct {
	Entities []V2EntityInput `json:"entities" jsonschema:"Entities to resolve, up to 20 per call."`
}

// V2SearchIndicatorsInput holds arguments for the v2 search_indicators MCP tool. All fields are optional:
// with neither query nor topic_dcids, the top-level topics are returned. Field descriptions give each field's
// format, limit and an example; when and how to use them is in tools/search_indicators.md.
type V2SearchIndicatorsInput struct {
	Query               string   `json:"query,omitempty" jsonschema:"One concept to search for, e.g. 'unemployment rate'. Cannot be combined with topic_dcids."`
	TopicDcids          []string `json:"topic_dcids,omitempty" jsonschema:"Topic DCIDs to browse, e.g. ['dc/topic/Health']. Up to 10. Cannot be combined with query."`
	EntityDcids         []string `json:"entity_dcids,omitempty" jsonschema:"Entity DCIDs from resolve_entities, e.g. ['geoId/06', 'country/KEN']. Up to 20, or exactly one with child_entity_type_dcid."`
	ChildEntityTypeDcid string   `json:"child_entity_type_dcid,omitempty" jsonschema:"A child entity type DCID from resolve_entities' candidate_child_entity_types, e.g. 'County'. Requires exactly one DCID in entity_dcids, the parent entity: indicators are then kept when they have data for that entity's child entities of this type, rather than for the entity itself."`
}

// V2GetObservationsInput holds arguments for the v2 get_observations MCP tool. variable_dcids and entity_dcids
// are required; all other fields are optional, and date intentionally omits a JSON Schema default so omitting
// date alongside date_range_start or date_range_end can infer 'range' without clients injecting 'latest'.
type V2GetObservationsInput struct {
	VariableDcids       []string            `json:"variable_dcids" jsonschema:"Variable DCIDs from search_indicators sharing the same observation_properties, e.g. ['Count_Person', 'UnemploymentRate_Person']. Pass 1 to 10 per call (1 to 2 with child_entity_type_dcid, or 1 with source_override); never pass topic DCIDs."`
	EntityDcids         []string            `json:"entity_dcids" jsonschema:"Entity DCIDs from resolve_entities constraining entity_property, e.g. ['geoId/06', 'country/KEN']. Pass 1 to 20 per call, or exactly 1 parent entity DCID with child_entity_type_dcid."`
	ChildEntityTypeDcid string              `json:"child_entity_type_dcid,omitempty" jsonschema:"A child entity type DCID from resolve_entities' candidate_child_entity_types, e.g. 'County'. Expands the single parent in entity_dcids to its child entities of this type on entity_property."`
	EntityProperty      string              `json:"entity_property,omitempty" jsonschema:"Observation property constrained by entity_dcids (or child_entity_type_dcid), e.g. 'donor' or 'sourceCounty'. Defaults to 'observationAbout'."`
	AdditionalEntities  map[string][]string `json:"additional_entities,omitempty" jsonschema:"Other observation_properties to constrain, mapping each property to 1 to 20 entity DCIDs from resolve_entities, e.g. {'recipient': ['country/KEN']}. Up to 2 properties, distinct from entity_property."`
	Date                string              `json:"date,omitempty" jsonschema:"Which dates to return: 'latest' (default when date_range_start and date_range_end are omitted), 'all' (only without child_entity_type_dcid), 'range', or an ISO date ('YYYY', 'YYYY-MM', or 'YYYY-MM-DD', e.g. '2023'). Omitting date when date_range_start or date_range_end is set infers 'range'."`
	DateRangeStart      string              `json:"date_range_start,omitempty" jsonschema:"Inclusive start date ('YYYY', 'YYYY-MM', or 'YYYY-MM-DD', e.g. '2018') when date is 'range' or omitted. Required for range queries with child_entity_type_dcid."`
	DateRangeEnd        string              `json:"date_range_end,omitempty" jsonschema:"Inclusive end date ('YYYY', 'YYYY-MM', or 'YYYY-MM-DD', e.g. '2023') when date is 'range' or omitted."`
	SourceOverride      string              `json:"source_override,omitempty" jsonschema:"A source_id from a prior response's source_metadata or alternative_sources to fetch from a specific dataset facet. Requires exactly 1 variable in variable_dcids."`
}
