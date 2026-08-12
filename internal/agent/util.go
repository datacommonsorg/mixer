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
	colTypeOf                = "typeOf"
	colPlacesWithData        = "placesWithData"
	colMemberTopics          = "memberTopics"
	colMemberVariables       = "memberVariables"
	colObservationProperties = "observationProperties"

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
