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
	"strings"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

// fakeAgentBackend records incoming requests and returns canned responses for unit tests.
type fakeAgentBackend struct {
	lastSearchReq       *pbv2.SearchIndicatorsRequest
	searchResp          *pbv2.SearchIndicatorsResponse
	lastVarMetadataReq  *pbv2.GetVariableMetadataRequest
	varMetadataResp     *pbv2.GetVariableMetadataResponse
	lastObservationsReq *pbv2.GetObservationsRequest
	observationsResp    *pbv2.GetObservationsResponse
}

func (f *fakeAgentBackend) SearchIndicators(_ context.Context, req *pbv2.SearchIndicatorsRequest) (*pbv2.SearchIndicatorsResponse, error) {
	f.lastSearchReq = req
	if f.searchResp != nil {
		return f.searchResp, nil
	}
	return &pbv2.SearchIndicatorsResponse{Status: "SUCCESS"}, nil
}

func (f *fakeAgentBackend) GetVariableMetadata(_ context.Context, req *pbv2.GetVariableMetadataRequest) (*pbv2.GetVariableMetadataResponse, error) {
	f.lastVarMetadataReq = req
	if f.varMetadataResp != nil {
		return f.varMetadataResp, nil
	}
	return &pbv2.GetVariableMetadataResponse{Status: "SUCCESS"}, nil
}

func (f *fakeAgentBackend) GetObservations(_ context.Context, req *pbv2.GetObservationsRequest) (*pbv2.GetObservationsResponse, error) {
	f.lastObservationsReq = req
	if f.observationsResp != nil {
		return f.observationsResp, nil
	}
	return &pbv2.GetObservationsResponse{
		Variable: &pbv2.GetObservationsResponse_Node{Dcid: req.GetVariableDcid(), Name: "Test Variable"},
	}, nil
}

func TestSearchIndicators(t *testing.T) {
	tests := []struct {
		name        string
		searchScope string
		input       SearchIndicatorsInput
		wantReq     *pbv2.SearchIndicatorsRequest
	}{
		{
			name:        "defaults applied without search scope",
			searchScope: "",
			input: SearchIndicatorsInput{
				Query:  "population",
				Places: []string{"California"},
			},
			wantReq: &pbv2.SearchIndicatorsRequest{
				Query:          "population",
				Places:         []string{"California"},
				PerSearchLimit: 10,
				IncludeTopics:  proto.Bool(true),
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
			wantReq: &pbv2.SearchIndicatorsRequest{
				Query:          "median income",
				Places:         []string{"Texas", "New York"},
				PerSearchLimit: 25,
				IncludeTopics:  proto.Bool(false),
				Target:         proto.String("custom_only"),
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
			if diff := cmp.Diff(tc.wantReq, backend.lastSearchReq, protocmp.Transform()); diff != "" {
				t.Errorf("SearchIndicators request diff (-want +got):\n%s", diff)
			}
			if got["status"] != "SUCCESS" {
				t.Errorf("SearchIndicators status = %v, want SUCCESS", got["status"])
			}
		})
	}
}

func TestSearchChildIndicators(t *testing.T) {
	tests := []struct {
		name        string
		searchScope string
		input       SearchChildIndicatorsInput
		wantReq     *pbv2.SearchIndicatorsRequest
	}{
		{
			name:        "child indicators search with defaults",
			searchScope: "base_and_custom",
			input: SearchChildIndicatorsInput{
				Query:             "unemployment rate",
				ParentPlace:       "United States",
				SampleChildPlaces: []string{"California", "Texas"},
			},
			wantReq: &pbv2.SearchIndicatorsRequest{
				Query:          "unemployment rate",
				ParentPlace:    "United States",
				Places:         []string{"California", "Texas"},
				PerSearchLimit: 10,
				IncludeTopics:  proto.Bool(true),
				Target:         proto.String("base_and_custom"),
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
			if diff := cmp.Diff(tc.wantReq, backend.lastSearchReq, protocmp.Transform()); diff != "" {
				t.Errorf("SearchChildIndicators request diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetVariableMetadata(t *testing.T) {
	tests := []struct {
		name    string
		input   GetVariableMetadataInput
		wantReq *pbv2.GetVariableMetadataRequest
	}{
		{
			name: "forwards variable_dcids and entity_dcids",
			input: GetVariableMetadataInput{
				VariableDcids: []string{"Count_Person", "Median_Income_Person"},
				EntityDcids:   []string{"country/USA", "geoId/06"},
			},
			wantReq: &pbv2.GetVariableMetadataRequest{
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
			if diff := cmp.Diff(tc.wantReq, backend.lastVarMetadataReq, protocmp.Transform()); diff != "" {
				t.Errorf("GetVariableMetadata request diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetObservationsAndChildObservations(t *testing.T) {
	singlePlaceVal, err := structpb.NewValue([]any{"geoId/06"})
	if err != nil {
		t.Fatalf("failed to create structpb value: %v", err)
	}
	childPlaceVal, err := structpb.NewValue(map[string]any{
		"parent_dcid": "geoId/06",
		"child_type":  "County",
	})
	if err != nil {
		t.Fatalf("failed to create structpb value: %v", err)
	}

	tests := []struct {
		name    string
		call    func(tools *McpTools) error
		wantReq *pbv2.GetObservationsRequest
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
			wantReq: &pbv2.GetObservationsRequest{
				VariableDcid: "Count_Person",
				Date:         proto.String("latest"),
				Entities: map[string]*structpb.Value{
					"observationAbout": singlePlaceVal,
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
			wantReq: &pbv2.GetObservationsRequest{
				VariableDcid:   "Count_Person",
				SourceOverride: proto.String("CensusPEP"),
				Date:           proto.String("range"),
				DateRangeStart: proto.String("2015"),
				DateRangeEnd:   proto.String("2020"),
				Entities: map[string]*structpb.Value{
					"observationAbout": childPlaceVal,
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
			if diff := cmp.Diff(tc.wantReq, backend.lastObservationsReq, protocmp.Transform()); diff != "" {
				t.Errorf("GetObservations request diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetMultiEntityObservations(t *testing.T) {
	obsAboutVal, _ := structpb.NewValue([]any{"country/USA"})
	parentVal, _ := structpb.NewValue(map[string]any{
		"parent_dcid": "Earth",
		"child_type":  "Country",
	})

	tests := []struct {
		name       string
		input      GetMultiEntityObservationsInput
		wantReq    *pbv2.GetObservationsRequest
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
			wantReq: &pbv2.GetObservationsRequest{
				VariableDcid: "TradeExports",
				Date:         proto.String("latest"),
				Entities: map[string]*structpb.Value{
					"observationAbout":     obsAboutVal,
					"measurementQualifier": parentVal,
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
			if diff := cmp.Diff(tc.wantReq, backend.lastObservationsReq, protocmp.Transform()); diff != "" {
				t.Errorf("GetMultiEntityObservations request diff (-want +got):\n%s", diff)
			}
		})
	}
}
