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

// This file holds metadata helpers shared by the v1 and v2 get_variable_metadata tools.

package agent

import (
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

const (
	wildcardPropertyQuery = "->*"

	// Schema property names
	propName                    = "name"
	propDescription             = "description"
	propTypeOf                  = "typeOf"
	propProvenance              = "provenance"
	propDefinition              = "definition"
	propMemberOf                = "memberOf"
	propLinkedMemberOf          = "linkedMemberOf"
	propLinkedMember            = "linkedMember"
	propUrl                     = "url"
	propLicense                 = "license"
	propLicenseType             = "licenseType"
	propLastDataRefreshDate     = "lastDataRefreshDate"
	propNextDataRefreshDate     = "nextDataRefreshDate"
	propNextSourceReleaseDate   = "nextSourceReleaseDate"
	propSourceReleaseFrequency  = "sourceReleaseFrequency"
	propLatestObservationDate   = "latestObservationDate"
	propEarliestObservationDate = "earliestObservationDate"
	propIsPartOf                = "isPartOf"
	propSource                  = "source"
	propDomain                  = "domain"
	propDescriptionUrl          = "descriptionUrl"
)

var (
	// properties to exclude from Statistical Variable properties map
	excludedSVProperties = map[string]struct{}{
		propName:           {},
		propDescription:    {},
		propTypeOf:         {},
		propProvenance:     {},
		propDefinition:     {},
		propMemberOf:       {},
		propLinkedMemberOf: {},
		propLinkedMember:   {},
	}

	// properties to explicitly include for Provenance and Source dataset properties map.
	// These must be the exact raw property keys returned from the knowledge graph (V2Node).
	includedDatasetProperties = map[string]struct{}{
		propUrl:                     {},
		propLicense:                 {},
		propLicenseType:             {},
		propLastDataRefreshDate:     {},
		propNextDataRefreshDate:     {},
		propNextSourceReleaseDate:   {},
		propSourceReleaseFrequency:  {},
		propLatestObservationDate:   {},
		propEarliestObservationDate: {},
		propIsPartOf:                {},
		propSource:                  {},
		propDomain:                  {},
		propDescription:             {},
		propDescriptionUrl:          {},
	}
)

// rawDatasetData holds the raw, unmapped graphs for a dataset's provenance and parent source.
type rawDatasetData struct {
	provenance *pbv2.LinkedGraph
	source     *pbv2.LinkedGraph
}

// translatedProperties holds the resolved name, description, and filtered dynamic properties map.
type translatedProperties struct {
	name        string
	description string
	properties  map[string]any
}

// translateProperties extracts headers and builds the dynamic properties map, filtering out primary name/description.
func translateProperties(graph *pbv2.LinkedGraph) *translatedProperties {
	tp := &translatedProperties{
		properties: make(map[string]any),
	}

	if graph == nil || graph.GetArcs() == nil {
		return tp
	}

	// Resolve root Name (primary name)
	if nameArc, ok := graph.GetArcs()[propName]; ok && nameArc != nil {
		tp.name = resolveFirstValue(nameArc.GetNodes())
	}

	// Resolve root Description
	if descArc, ok := graph.GetArcs()[propDescription]; ok && descArc != nil {
		tp.description = resolveFirstValue(descArc.GetNodes())
	}

	// Build filtered dynamic properties map
	for prop, nodesArc := range graph.GetArcs() {
		if nodesArc == nil || len(nodesArc.GetNodes()) == 0 {
			continue
		}

		// Filter out properties that are mapped to root or represent internal detail
		if _, shouldExclude := excludedSVProperties[prop]; shouldExclude {
			continue
		}

		// Convert values completely as-is (no root-value duplication filtering)
		if propVal, ok := toPropertyValue(nodesArc.GetNodes()); ok {
			tp.properties[prop] = propVal
		}
	}

	return tp
}

// translateProvenance merges provenance and parent source properties into a single dynamic map.
func translateProvenance(id string, rawDataset *rawDatasetData) *ProvenanceMetadata {
	provMeta := &ProvenanceMetadata{
		ID:         id,
		Properties: make(map[string]any),
	}
	if rawDataset == nil {
		return provMeta
	}

	mergeProperties := func(graph *pbv2.LinkedGraph) {
		if graph == nil || graph.GetArcs() == nil {
			return
		}
		for prop, nodesArc := range graph.GetArcs() {
			if nodesArc == nil || len(nodesArc.GetNodes()) == 0 {
				continue
			}
			if _, shouldInclude := includedDatasetProperties[prop]; !shouldInclude {
				continue
			}
			if propVal, ok := toPropertyValue(nodesArc.GetNodes()); ok {
				provMeta.Properties[prop] = propVal
			}
		}
	}

	// Merge parent source first, then overlay provenance specific properties
	mergeProperties(rawDataset.source)
	mergeProperties(rawDataset.provenance)

	return provMeta
}

// extractSourceDcid extracts the parent source DCID from a provenance graph.
func extractSourceDcid(graph *pbv2.LinkedGraph) string {
	if graph == nil || graph.GetArcs() == nil {
		return ""
	}
	if srcArc, ok := graph.GetArcs()[propSource]; ok && srcArc != nil {
		for _, n := range srcArc.GetNodes() {
			if n != nil && n.GetDcid() != "" {
				return n.GetDcid()
			}
		}
	}
	return ""
}

// resolveFirstValue retrieves the raw string value of the first available candidate.
func resolveFirstValue(nodes []*pb.EntityInfo) string {
	if len(nodes) == 0 {
		return ""
	}
	for _, n := range nodes {
		if n == nil {
			continue
		}
		val := n.GetValue()
		if val == "" {
			val = n.GetName()
		}
		if val == "" {
			val = n.GetDcid()
		}
		if val != "" {
			return val
		}
	}
	return ""
}

// toPropertyValue converts a slice of properties into a string or slice of strings.
func toPropertyValue(nodes []*pb.EntityInfo) (any, bool) {
	var values []string
	seen := make(map[string]struct{})

	for _, n := range nodes {
		if n == nil {
			continue
		}
		val := n.GetValue()
		if val == "" {
			val = n.GetName()
		}
		if val == "" {
			val = n.GetDcid()
		}
		if val == "" {
			continue
		}

		if _, ok := seen[val]; !ok {
			seen[val] = struct{}{}
			values = append(values, val)
		}
	}

	if len(values) == 0 {
		return nil, false
	}

	if len(values) == 1 {
		return values[0], true
	}

	return values, true
}
