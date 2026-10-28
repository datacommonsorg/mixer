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

import (
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

// Package-level constants representing standard resolver, property, metadata, and boundary literals.
const (
	ResolverPlace          = "place"
	ResolverNonPlace       = "non_place" // non-place entities (organizations, schools, groupings, etc.) via embeddings
	ResolverType           = "type"      // natural-language type names (e.g. "food bank") to Class DCIDs via embeddings
	ResolverIndicator      = "indicator"
	ResolverTopic          = "topic"
	PropDescription        = "<-description->dcid"
	DateLatest             = "LATEST"
	DefaultPlaceWorld      = "World"
	StatusSuccess          = "SUCCESS"
	MetadataPlacesWithData = "places_with_data"
	DcidSeparator          = ","
	DcidTypeTopic          = "Topic"
	DcidTypeVariable       = "StatisticalVariable"
	DefaultSearchLimit     = 10
	MaxSearchLimit         = 100
	MinSearchLimit         = 0

	TargetCustomOnly    = "custom_only"
	TargetBaseOnly      = "base_only"
	TargetBaseAndCustom = "base_and_custom"

	colQuery                 = "query"
	colDcid                  = "dcid"
	colName                  = "name"
	colTypes                 = "types"
	colEntityDcid            = "entity_dcid"
	colChildEntityTypeDcid   = "child_entity_type_dcid"
	colChildEntityTypeName   = "child_entity_type_name"
	colPlacesWithData        = "placesWithData"
	colMemberTopics          = "memberTopics"
	colMemberVariables       = "memberVariables"
	colObservationProperties = "observationProperties"

	colEntitiesWithData        = "entities_with_data"
	colV2MemberTopics          = "member_topics"
	colV2MemberVariables       = "member_variables"
	colDescendantVariableCount = "descendant_variable_count"
	colV2ObservationProperties = "observation_properties"

	DefaultPlaceDcidWorld = "Earth"

	dateTypeLatest = "latest"
	dateTypeAll    = "all"
	dateTypeRange  = "range"

	nodePropertiesQuery = "->[name, typeOf]"
)

// validTargets contains the sorted set of supported resolution target identifiers.
var validTargets = []string{
	TargetBaseAndCustom,
	TargetBaseOnly,
	TargetCustomOnly,
}

// dcidSet collects unique DCIDs in the order they were first added.
type dcidSet struct {
	seen  map[string]struct{}
	dcids []string
}

// newDcidSet returns an empty dcidSet.
func newDcidSet() *dcidSet {
	return &dcidSet{seen: map[string]struct{}{}}
}

// add adds dcid unless it was added before, and reports whether it was added.
func (s *dcidSet) add(dcid string) bool {
	if _, ok := s.seen[dcid]; ok {
		return false
	}
	s.seen[dcid] = struct{}{}
	s.dcids = append(s.dcids, dcid)
	return true
}

// getPropValue extracts the first string value of a property from a LinkedGraph.
func getPropValue(graph *pbv2.LinkedGraph, prop string) string {
	if graph == nil || graph.Arcs == nil {
		return ""
	}
	if nodes, ok := graph.Arcs[prop]; ok && nodes != nil && len(nodes.GetNodes()) > 0 {
		return nodes.GetNodes()[0].GetValue()
	}
	return ""
}

// getPropDcids extracts all node DCIDs of a property from a LinkedGraph.
func getPropDcids(graph *pbv2.LinkedGraph, prop string) []string {
	if graph == nil || graph.Arcs == nil {
		return nil
	}
	var res []string
	if nodes, ok := graph.Arcs[prop]; ok && nodes != nil {
		for _, node := range nodes.GetNodes() {
			if dcid := node.GetDcid(); dcid != "" {
				res = append(res, dcid)
			}
		}
	}
	return res
}
