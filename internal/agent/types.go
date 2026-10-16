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

// Request and response types for the in-process agent package.
package agent

// Table represents a generic tabular structure for token-efficient data delivery.
type Table struct {
	Columns []string `json:"columns,omitempty"`
	Rows    [][]any  `json:"rows,omitempty"`
}

// ResolvePlacesRequest holds parameters for ResolvePlaces.
type ResolvePlacesRequest struct {
	Places []string
	Target string
}

// ResolvePlacesResponse holds the tabular place resolution output.
type ResolvePlacesResponse struct {
	Status         string `json:"status,omitempty"`
	ResolvedPlaces *Table `json:"resolvedPlaces,omitempty"`
}

// SearchIndicatorsRequest holds parameters for SearchIndicators.
type SearchIndicatorsRequest struct {
	Query          string
	Places         []string
	ParentPlace    string
	PerSearchLimit int32
	IncludeTopics  *bool
	ExpandTopics   *bool
	Target         string
	PlaceDcids     []string
}

// TopicCandidate represents a matched topic in legacy search_indicators responses.
type TopicCandidate struct {
	Dcid                  string   `json:"dcid,omitempty"`
	MemberTopics          []string `json:"memberTopics,omitempty"`
	MemberVariables       []string `json:"memberVariables,omitempty"`
	PlacesWithData        []string `json:"placesWithData,omitempty"`
	Description           string   `json:"description,omitempty"`
	AlternateDescriptions []string `json:"alternateDescriptions,omitempty"`
}

// VariableCandidate represents a matched statistical variable in legacy search_indicators responses.
type VariableCandidate struct {
	Dcid                  string   `json:"dcid,omitempty"`
	PlacesWithData        []string `json:"placesWithData,omitempty"`
	Description           string   `json:"description,omitempty"`
	AlternateDescriptions []string `json:"alternateDescriptions,omitempty"`
	ObservationProperties []string `json:"observationProperties,omitempty"`
}

// ResolvedPlace represents a resolved place node.
type ResolvedPlace struct {
	Dcid   string   `json:"dcid,omitempty"`
	Name   string   `json:"name,omitempty"`
	TypeOf []string `json:"typeOf,omitempty"`
}

// SearchIndicatorsResponse holds the indicator search results.
type SearchIndicatorsResponse struct {
	Status                string               `json:"status,omitempty"`
	DcidNameMappings      map[string]string    `json:"dcidNameMappings,omitempty"`
	DcidPlaceTypeMappings map[string][]string  `json:"dcidPlaceTypeMappings,omitempty"`
	Topics                []*TopicCandidate    `json:"topics,omitempty"`
	Variables             []*VariableCandidate `json:"variables,omitempty"`
	ResolvedParentPlace   *ResolvedPlace       `json:"resolvedParentPlace,omitempty"`
	TopicCandidates       *Table               `json:"topicCandidates,omitempty"`
	VariableCandidates    *Table               `json:"variableCandidates,omitempty"`
}

// ParentPlaceSpec specifies a parent entity and child type for hierarchy expansion.
type ParentPlaceSpec struct {
	ParentDcid string
	ChildType  string
}

// EntityConstraint specifies either direct entity DCIDs or a parent place expansion constraint for an SDMX dimension slot.
type EntityConstraint struct {
	Dcids      []string
	ParentSpec *ParentPlaceSpec
}

// GetObservationsRequest holds parameters for GetObservations.
type GetObservationsRequest struct {
	VariableDcid   string
	SourceOverride string
	Date           string
	DateRangeStart string
	DateRangeEnd   string
	Entities       map[string]*EntityConstraint
}

// ObservationNode represents variable metadata in GetObservationsResponse.
type ObservationNode struct {
	Dcid   string   `json:"dcid,omitempty"`
	Name   string   `json:"name,omitempty"`
	TypeOf []string `json:"typeOf,omitempty"`
}

// ObservationFacetMetadata represents source facet metadata in GetObservationsResponse.
type ObservationFacetMetadata struct {
	SourceID          string `json:"sourceId,omitempty"`
	ImportName        string `json:"importName,omitempty"`
	MeasurementMethod string `json:"measurementMethod,omitempty"`
	ObservationPeriod string `json:"observationPeriod,omitempty"`
	ProvenanceURL     string `json:"provenanceUrl,omitempty"`
	Unit              string `json:"unit,omitempty"`
}

// AlternativeSource represents an alternative facet source in GetObservationsResponse.
type AlternativeSource struct {
	SourceMetadata   *ObservationFacetMetadata `json:"sourceMetadata,omitempty"`
	PlacesFoundCount *int32                    `json:"placesFoundCount,omitempty"`
}

// GetObservationsResponse holds the SDMX dual-table observation response.
type GetObservationsResponse struct {
	Variable           *ObservationNode          `json:"variable,omitempty"`
	EntityMetadata     *Table                    `json:"entityMetadata,omitempty"`
	Data               *Table                    `json:"data,omitempty"`
	SourceMetadata     *ObservationFacetMetadata `json:"sourceMetadata,omitempty"`
	AlternativeSources []*AlternativeSource      `json:"alternativeSources,omitempty"`
}

// GetVariableMetadataRequest holds parameters for GetVariableMetadata.
type GetVariableMetadataRequest struct {
	VariableDcids []string
	EntityDcids   []string
}

// DateRange represents the start and end dates for a variable facet.
type DateRange struct {
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

// FacetScope represents entity coverage and granularity for a variable facet.
type FacetScope struct {
	EntityCoverage    []string `json:"entityCoverage,omitempty"`
	EntityGranularity []string `json:"entityGranularity,omitempty"`
}

// VariableFacetMetadata represents metadata for a single facet in GetVariableMetadataResponse.
type VariableFacetMetadata struct {
	ID           string         `json:"id,omitempty"`
	ProvenanceID string         `json:"provenanceId,omitempty"`
	ObsCount     int32          `json:"obsCount,omitempty"`
	DateRange    *DateRange     `json:"dateRange,omitempty"`
	Scope        *FacetScope    `json:"scope,omitempty"`
	Properties   map[string]any `json:"properties"`
}

// VariableMetadata represents metadata for a single variable in GetVariableMetadataResponse.
type VariableMetadata struct {
	ID          string                   `json:"id,omitempty"`
	Name        string                   `json:"name,omitempty"`
	Description string                   `json:"description,omitempty"`
	Properties  map[string]any           `json:"properties"`
	Facets      []*VariableFacetMetadata `json:"facets,omitempty"`
}

// ProvenanceMetadata represents metadata for a single provenance in GetVariableMetadataResponse.
type ProvenanceMetadata struct {
	ID         string         `json:"id,omitempty"`
	Properties map[string]any `json:"properties"`
}

// GetVariableMetadataResponse holds the variable and provenance metadata map output.
type GetVariableMetadataResponse struct {
	Status      string                         `json:"status,omitempty"`
	Variables   map[string]*VariableMetadata   `json:"variables,omitempty"`
	Provenances map[string]*ProvenanceMetadata `json:"provenances,omitempty"`
}

// nonNilStringSlice returns s if non-empty, or an empty non-nil slice so JSON marshaling emits [] instead of null.
func nonNilStringSlice(s []string) []string {
	if len(s) == 0 {
		return []string{}
	}
	return s
}
