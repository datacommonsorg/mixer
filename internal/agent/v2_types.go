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

package agent

// V2EntityQuery is a single entity name to resolve, with optional natural-language type hints for the
// entity itself and for child entities to look for within it.
type V2EntityQuery struct {
	Name                 string
	EntityTypeHints      []string
	ChildEntityTypeHints []string
}

// V2ResolveEntitiesRequest holds parameters for V2ResolveEntities.
type V2ResolveEntitiesRequest struct {
	Entities []*V2EntityQuery
	Target   string
}

// V2ResolveEntitiesResponse holds the resolved entities as a table with columns query, dcid, name and
// types. Each query has one row per candidate (best match first), or a single row with an empty dcid
// when it could not be resolved, so every requested name appears in the output. The tabular shape
// matches other agent tool responses and avoids repeating keys for every candidate.
//
// CandidateChildEntityTypes is only set when some query has child type hints and the child type lookup
// succeeded (a failed lookup omits it rather than reporting no child types). It has columns query,
// entity_dcid, child_entity_type_dcid and child_entity_type_name, with one row per hinted child type that
// exists within the query's best-matching entity, or a single row with an empty child_entity_type_dcid when
// none matched (or the query did not resolve, in which case entity_dcid is also empty).
//
// ChildEntityTypesNote explains, when set, why CandidateChildEntityTypes is missing despite child type
// hints (e.g. the lookup failed), so callers do not read the omission as hints being ignored.
type V2ResolveEntitiesResponse struct {
	ResolvedEntities          *Table `json:"resolved_entities"`
	CandidateChildEntityTypes *Table `json:"candidate_child_entity_types,omitempty"`
	ChildEntityTypesNote      string `json:"child_entity_types_note,omitempty"`
}

// v2ResolvedEntity is a resolved entity DCID with its canonical name and types. Types are returned so
// callers can disambiguate same-named entities (e.g. Georgia the State vs. the Country).
type v2ResolvedEntity struct {
	dcid  string
	name  string
	types []string
}

// v2EntityGroup holds the resolved candidates for one queried name.
type v2EntityGroup struct {
	query    string
	entities []*v2ResolvedEntity
}

// v2ChildTypesResult holds, for one query with child type hints, the matched child entity types within
// its best-matching entity (parent). parent is empty when the query did not resolve.
type v2ChildTypesResult struct {
	query  string
	hints  []string
	parent string
	types  []*v2ChildType
}

// v2ChildType is a child entity type (Class DCID) that exists within a parent entity (via containedInPlace).
type v2ChildType struct {
	dcid string
	name string
}

// V2SearchIndicatorsRequest holds parameters for V2SearchIndicators. Query searches for topics and
// variables; TopicDcids browses those topics; with neither, the top-level topics are browsed. EntityDcids
// keeps only indicators with data for those entities. ChildEntityTypeDcid (child mode) instead keeps only
// indicators with data for the child entities of that type within the single entity in EntityDcids.
type V2SearchIndicatorsRequest struct {
	Query               string
	TopicDcids          []string
	EntityDcids         []string
	ChildEntityTypeDcid string
	Target              string
}

// V2SearchIndicatorsResponse holds the matched topics and variables as tables.
//
// TopicCandidates has columns dcid, name, entities_with_data, member_topics, member_variables and
// descendant_variable_count. member_topics and member_variables list the topic's direct members with
// data as {dcid, name} objects, in the order the topic defines them; descendant_variable_count counts the
// variables at any depth under the topic (only those with data when entities are given). A topic row has
// the same content in search and browse, so agents drill down by browsing member_topics.
//
// VariableCandidates (search only) has columns dcid, name, entities_with_data and observation_properties.
//
// Without entities, entities_with_data is empty and nothing is filtered. In child mode, entities_with_data
// lists the parent entity when any of its sampled child entities has data. Note explains unfiltered or
// empty results.
type V2SearchIndicatorsResponse struct {
	TopicCandidates    *Table `json:"topic_candidates"`
	VariableCandidates *Table `json:"variable_candidates,omitempty"`
	Note               string `json:"note,omitempty"`
}

// v2IndicatorRef is a topic member (sub-topic or variable) in member_topics or member_variables.
type v2IndicatorRef struct {
	Dcid string `json:"dcid"`
	Name string `json:"name"`
}
