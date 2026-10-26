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
	"encoding/json"
	"strings"
	"testing"

	"github.com/datacommonsorg/mixer/internal/agent"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
)

func TestSearchIndicators(t *testing.T) {
	tests := []struct {
		name        string
		searchScope string
		input       SearchIndicatorsInput
		wantReq     *agent.SearchIndicatorsRequest
	}{
		{
			name:        "defaults applied without search scope",
			searchScope: "",
			input: SearchIndicatorsInput{
				Query:  "population",
				Places: []string{"California"},
			},
			wantReq: &agent.SearchIndicatorsRequest{
				Query:          "population",
				Places:         []string{"California"},
				PerSearchLimit: 10,
				IncludeTopics:  true,
			},
		},
		{
			name:        "explicit parameters and search scope",
			searchScope: "custom_only",
			input: SearchIndicatorsInput{
				Query:          "median income",
				Places:         []string{"Texas", "New York"},
				PerSearchLimit: 25,
				IncludeTopics:  proto.Bool(false),
			},
			wantReq: &agent.SearchIndicatorsRequest{
				Query:          "median income",
				Places:         []string{"Texas", "New York"},
				PerSearchLimit: 25,
				IncludeTopics:  false,
				Target:         "custom_only",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeAgentBackend{}
			tools := NewMcpTools(backend, tc.searchScope)

			got, err := tools.SearchIndicators(context.Background(), tc.input)
			if err != nil {
				t.Fatalf("SearchIndicators(%+v) unexpected error: %v", tc.input, err)
			}
			if diff := cmp.Diff(tc.wantReq, backend.lastSearchReq); diff != "" {
				t.Errorf("SearchIndicators request diff (-want +got):\n%s", diff)
			}
			if got.Status != "SUCCESS" {
				t.Errorf("SearchIndicators status = %v, want SUCCESS", got.Status)
			}
		})
	}
}

func TestSearchChildIndicators(t *testing.T) {
	tests := []struct {
		name        string
		searchScope string
		input       SearchChildIndicatorsInput
		wantReq     *agent.SearchIndicatorsRequest
	}{
		{
			name:        "child indicators search with defaults",
			searchScope: "base_and_custom",
			input: SearchChildIndicatorsInput{
				Query:             "unemployment rate",
				ParentPlace:       "United States",
				SampleChildPlaces: []string{"California", "Texas"},
			},
			wantReq: &agent.SearchIndicatorsRequest{
				Query:          "unemployment rate",
				ParentPlace:    "United States",
				Places:         []string{"California", "Texas"},
				PerSearchLimit: 10,
				IncludeTopics:  true,
				Target:         "base_and_custom",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeAgentBackend{}
			tools := NewMcpTools(backend, tc.searchScope)

			_, err := tools.SearchChildIndicators(context.Background(), tc.input)
			if err != nil {
				t.Fatalf("SearchChildIndicators(%+v) unexpected error: %v", tc.input, err)
			}
			if diff := cmp.Diff(tc.wantReq, backend.lastSearchReq); diff != "" {
				t.Errorf("SearchChildIndicators request diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetVariableMetadata(t *testing.T) {
	tests := []struct {
		name    string
		input   GetVariableMetadataInput
		wantReq *agent.GetVariableMetadataRequest
	}{
		{
			name: "forwards variable_dcids and entity_dcids",
			input: GetVariableMetadataInput{
				VariableDcids: []string{"Count_Person", "Median_Income_Person"},
				EntityDcids:   []string{"country/USA", "geoId/06"},
			},
			wantReq: &agent.GetVariableMetadataRequest{
				VariableDcids: []string{"Count_Person", "Median_Income_Person"},
				EntityDcids:   []string{"country/USA", "geoId/06"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeAgentBackend{}
			tools := NewMcpTools(backend, "")

			_, err := tools.GetVariableMetadata(context.Background(), tc.input)
			if err != nil {
				t.Fatalf("GetVariableMetadata(%+v) unexpected error: %v", tc.input, err)
			}
			if diff := cmp.Diff(tc.wantReq, backend.lastVarMetadataReq); diff != "" {
				t.Errorf("GetVariableMetadata request diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetObservationsAndChildObservations(t *testing.T) {
	tests := []struct {
		name    string
		call    func(tools *McpTools) error
		wantReq *agent.GetObservationsRequest
	}{
		{
			name: "get_observations single place with default latest date",
			call: func(tools *McpTools) error {
				_, callErr := tools.GetObservations(context.Background(), GetObservationsInput{
					VariableDcid: "Count_Person",
					PlaceDcid:    "geoId/06",
				})
				return callErr
			},
			wantReq: &agent.GetObservationsRequest{
				VariableDcid: "Count_Person",
				Date:         "latest",
				Entities: map[string]*agent.EntityConstraint{
					"observationAbout": {
						Dcids: []string{"geoId/06"},
					},
				},
			},
		},
		{
			name: "get_child_observations with date range and source override",
			call: func(tools *McpTools) error {
				_, callErr := tools.GetChildObservations(context.Background(), GetChildObservationsInput{
					VariableDcid:    "Count_Person",
					ParentPlaceDcid: "geoId/06",
					ChildPlaceType:  "County",
					SourceOverride:  "CensusPEP",
					Date:            "range",
					DateRangeStart:  "2015",
					DateRangeEnd:    "2020",
				})
				return callErr
			},
			wantReq: &agent.GetObservationsRequest{
				VariableDcid:   "Count_Person",
				SourceOverride: "CensusPEP",
				Date:           "range",
				DateRangeStart: "2015",
				DateRangeEnd:   "2020",
				Entities: map[string]*agent.EntityConstraint{
					"observationAbout": {
						ParentSpec: &agent.ParentPlaceSpec{
							ParentDcid: "geoId/06",
							ChildType:  "County",
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeAgentBackend{}
			tools := NewMcpTools(backend, "")

			if err := tc.call(tools); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantReq, backend.lastObservationsReq); diff != "" {
				t.Errorf("GetObservations request diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetMultiEntityObservations(t *testing.T) {
	tests := []struct {
		name       string
		input      GetMultiEntityObservationsInput
		wantReq    *agent.GetObservationsRequest
		wantErrSub string
	}{
		{
			name: "valid multi-entity with child expansion",
			input: GetMultiEntityObservationsInput{
				VariableDcid:         "TradeExports",
				Entities:             map[string][]string{"observationAbout": {"country/USA"}},
				ParentEntityProperty: "measurementQualifier",
				ParentEntityDcid:     "Earth",
				ChildEntityType:      "Country",
			},
			wantReq: &agent.GetObservationsRequest{
				VariableDcid: "TradeExports",
				Date:         "latest",
				Entities: map[string]*agent.EntityConstraint{
					"observationAbout": {
						Dcids: []string{"country/USA"},
					},
					"measurementQualifier": {
						ParentSpec: &agent.ParentPlaceSpec{
							ParentDcid: "Earth",
							ChildType:  "Country",
						},
					},
				},
			},
		},
		{
			name: "error when partial child expansion params provided",
			input: GetMultiEntityObservationsInput{
				VariableDcid:         "TradeExports",
				Entities:             map[string][]string{"observationAbout": {"country/USA"}},
				ParentEntityProperty: "measurementQualifier",
			},
			wantErrSub: "all of 'parent_entity_property', 'parent_entity_dcid', and 'child_entity_type' must be provided",
		},
		{
			name: "error when child expansion property duplicates entities key",
			input: GetMultiEntityObservationsInput{
				VariableDcid:         "TradeExports",
				Entities:             map[string][]string{"observationAbout": {"country/USA"}},
				ParentEntityProperty: "observationAbout",
				ParentEntityDcid:     "Earth",
				ChildEntityType:      "Country",
			},
			wantErrSub: "cannot be specified in both 'entities' and child expansion parameters",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeAgentBackend{}
			tools := NewMcpTools(backend, "")

			_, err := tools.GetMultiEntityObservations(context.Background(), tc.input)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("GetMultiEntityObservations error = %v, want substring %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetMultiEntityObservations unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantReq, backend.lastObservationsReq); diff != "" {
				t.Errorf("GetMultiEntityObservations request diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNativeResponseWireJSONSerialization(t *testing.T) {
	placesFoundZero := int32(0)
	tests := []struct {
		name     string
		response any
		wantJSON map[string]any
	}{
		{
			name: "SearchIndicatorsResponse preserves camelCase and empty slices in table rows and mappings",
			response: &agent.SearchIndicatorsResponse{
				TopicCandidates: &agent.Table{
					Columns: []string{"dcid", "name", "memberTopics", "variables", "placesWithData"},
					Rows: [][]any{
						{"dc/topic/Economy", "Economy", []string{}, []string{"Count_Person"}, []string{}},
					},
				},
				VariableCandidates: &agent.Table{
					Columns: []string{"dcid", "name", "placesWithData"},
					Rows: [][]any{
						{"Count_Person", "Total Population", []string{"country/USA"}},
					},
				},
				DcidNameMappings: map[string]string{
					"country/USA": "United States",
				},
				DcidPlaceTypeMappings: map[string][]string{
					"country/USA": {"Country"},
					"geoId/00":    {},
				},
				Status: "SUCCESS",
			},
			wantJSON: map[string]any{
				"topicCandidates": map[string]any{
					"columns": []any{"dcid", "name", "memberTopics", "variables", "placesWithData"},
					"rows": []any{
						[]any{"dc/topic/Economy", "Economy", []any{}, []any{"Count_Person"}, []any{}},
					},
				},
				"variableCandidates": map[string]any{
					"columns": []any{"dcid", "name", "placesWithData"},
					"rows": []any{
						[]any{"Count_Person", "Total Population", []any{"country/USA"}},
					},
				},
				"dcidNameMappings": map[string]any{
					"country/USA": "United States",
				},
				"dcidPlaceTypeMappings": map[string]any{
					"country/USA": []any{"Country"},
					"geoId/00":    []any{},
				},
				"status": "SUCCESS",
			},
		},
		{
			name: "GetVariableMetadataResponse preserves empty properties maps as JSON objects",
			response: &agent.GetVariableMetadataResponse{
				Status: "SUCCESS",
				Variables: map[string]*agent.VariableMetadata{
					"Count_Person": {
						ID:         "Count_Person",
						Properties: map[string]any{},
						Facets: []*agent.VariableFacetMetadata{
							{
								ID:         "f1",
								Properties: map[string]any{},
							},
						},
					},
				},
				Provenances: map[string]*agent.ProvenanceMetadata{
					"dc/base/Census": {
						ID:         "dc/base/Census",
						Properties: map[string]any{},
					},
				},
			},
			wantJSON: map[string]any{
				"status": "SUCCESS",
				"variables": map[string]any{
					"Count_Person": map[string]any{
						"id":         "Count_Person",
						"properties": map[string]any{},
						"facets": []any{
							map[string]any{
								"id":         "f1",
								"properties": map[string]any{},
							},
						},
					},
				},
				"provenances": map[string]any{
					"dc/base/Census": map[string]any{
						"id":         "dc/base/Census",
						"properties": map[string]any{},
					},
				},
			},
		},
		{
			name: "GetObservationsResponse preserves camelCase dual-table structure and zero placesFoundCount pointer",
			response: &agent.GetObservationsResponse{
				Variable: &agent.ObservationNode{
					Dcid:   "Count_Person",
					Name:   "Total Population",
					TypeOf: []string{"StatisticalVariable"},
				},
				EntityMetadata: &agent.Table{
					Columns: []string{"slot", "dcid", "name", "typeOf"},
					Rows: [][]any{
						{"observationAbout", "country/USA", "United States", "Country"},
					},
				},
				Data: &agent.Table{
					Columns: []string{"observationAbout", "date", "value", "unit"},
					Rows: [][]any{
						{"country/USA", "2020", 331449281.0, nil},
					},
				},
				SourceMetadata: &agent.ObservationFacetMetadata{
					SourceID:   "f1",
					ImportName: "CensusPEP",
				},
				AlternativeSources: []*agent.AlternativeSource{
					{
						SourceMetadata: &agent.ObservationFacetMetadata{
							SourceID:   "f2",
							ImportName: "WorldBank",
						},
						PlacesFoundCount: &placesFoundZero,
					},
				},
			},
			wantJSON: map[string]any{
				"variable": map[string]any{
					"dcid":   "Count_Person",
					"name":   "Total Population",
					"typeOf": []any{"StatisticalVariable"},
				},
				"entityMetadata": map[string]any{
					"columns": []any{"slot", "dcid", "name", "typeOf"},
					"rows": []any{
						[]any{"observationAbout", "country/USA", "United States", "Country"},
					},
				},
				"data": map[string]any{
					"columns": []any{"observationAbout", "date", "value", "unit"},
					"rows": []any{
						[]any{"country/USA", "2020", 331449281.0, nil},
					},
				},
				"sourceMetadata": map[string]any{
					"sourceId":   "f1",
					"importName": "CensusPEP",
				},
				"alternativeSources": []any{
					map[string]any{
						"sourceMetadata": map[string]any{
							"sourceId":   "f2",
							"importName": "WorldBank",
						},
						"placesFoundCount": float64(0),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rawBytes, err := json.Marshal(tc.response)
			if err != nil {
				t.Fatalf("json.Marshal(%T) failed: %v", tc.response, err)
			}
			var gotJSON map[string]any
			if err := json.Unmarshal(rawBytes, &gotJSON); err != nil {
				t.Fatalf("json.Unmarshal failed: %v", err)
			}
			if diff := cmp.Diff(tc.wantJSON, gotJSON); diff != "" {
				t.Errorf("JSON wire output diff (-want +got):\n%s", diff)
			}
		})
	}
}
