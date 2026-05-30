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
	"context"
	"fmt"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// mockMixerServer acts as a lightweight, self-contained in-process Mixer API engine
// that dynamically serves query requests from its internal mock data maps.
type mockMixerServer struct {
	// resolveMockData maps a search query node string (or place description like "World")
	// to its pre-populated list of resolved KG entity candidates.
	resolveMockData map[string][]*pbv2.ResolveResponse_Entity_Candidate

	// obsMockData maps a place DCID string to the slice of variable DCIDs
	// that have active observation series data available for that place.
	obsMockData     map[string][]string
}

// V2Resolve dynamically maps input query nodes to candidates from its resolveMockData map.
func (m *mockMixerServer) V2Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	resp := &pbv2.ResolveResponse{}

	// Default empty query browsing maps to empty string lookup key
	lookupNodes := in.GetNodes()
	if len(lookupNodes) == 0 {
		lookupNodes = []string{""}
	}

	for _, node := range lookupNodes {
		candidates, ok := m.resolveMockData[node]
		if !ok {
			return nil, fmt.Errorf("unexpected mock resolve request for node: %q", node)
		}
		resp.Entities = append(resp.Entities, &pbv2.ResolveResponse_Entity{
			Node:       node,
			Candidates: candidates,
		})
	}
	return resp, nil
}

// V2Observation dynamically verifies place-variable availabilities using its obsMockData map.
func (m *mockMixerServer) V2Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	resp := &pbv2.ObservationResponse{
		ByVariable: make(map[string]*pbv2.VariableObservation),
	}

	if len(in.GetEntity().GetDcids()) != 1 {
		return nil, fmt.Errorf("expected single place dcid query, got: %v", in.GetEntity().GetDcids())
	}

	place := in.GetEntity().GetDcids()[0]
	vars, ok := m.obsMockData[place]
	if !ok {
		return nil, fmt.Errorf("unexpected mock observation request for place: %s", place)
	}

	for _, v := range vars {
		resp.ByVariable[v] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{
				place: {
					OrderedFacets: []*pbv2.FacetObservation{
						{FacetId: "mock-facet"},
					},
				},
			},
		}
	}
	return resp, nil
}

func TestSearchIndicators_Basic(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, tc := range []struct {
		desc            string
		request         *pbv2.SearchIndicatorsRequest

		// resolveMockData maps a search query node string (or place description like "World")
		// to its pre-populated list of resolved KG entity candidates.
		resolveMockData map[string][]*pbv2.ResolveResponse_Entity_Candidate

		// obsMockData maps a place DCID string to the slice of variable DCIDs
		// that have active observation series data available for that place.
		obsMockData     map[string][]string

		wantResponse    *pbv2.SearchIndicatorsResponse
		expectedError   string
	}{
		{
			desc: "Basic indicator search without place constraints",
			request: &pbv2.SearchIndicatorsRequest{
				Query:          "health in america",
				PerSearchLimit: 5,
				IncludeTopics:  true,
			},
			resolveMockData: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				"health in america": {
					{
						Dcid:   "topic/Health",
						TypeOf: []string{"Topic"},
						Name:   "Health",
						Children: []*pbv2.ResolveResponse_Entity_Candidate{
							{
								Dcid:   "Count_Person_WithAsthma",
								TypeOf: []string{"StatisticalVariable"},
								Name:   "People with Asthma",
							},
						},
					},
					{
						Dcid:   "Count_Person_WithDiabetes",
						TypeOf: []string{"StatisticalVariable"},
						Name:   "People with Diabetes",
					},
				},
			},
			wantResponse: &pbv2.SearchIndicatorsResponse{
				Status: "SUCCESS",
				DcidNameMappings: map[string]string{
					"topic/Health":              "Health",
					"Count_Person_WithAsthma":   "People with Asthma",
					"Count_Person_WithDiabetes": "People with Diabetes",
				},
				Topics: []*pbv2.SearchIndicatorsResponse_Topic{
					{
						Dcid:                 "topic/Health",
						Description:          "Health",
						MemberVariables:      []string{"Count_Person_WithAsthma"},
						AlternateDescriptions: []string{"Health"},
					},
				},
				Variables: []*pbv2.SearchIndicatorsResponse_Variable{
					{
						Dcid:        "Count_Person_WithDiabetes",
						Description: "People with Diabetes",
					},
				},
			},
		},
		{
			desc: "Browsing root topics defaults to World place resolution",
			request: &pbv2.SearchIndicatorsRequest{
				Query:          "",
				PerSearchLimit: 5,
				IncludeTopics:  true,
			},
			resolveMockData: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				DefaultPlaceWorld: {
					{Dcid: "Earth", Name: "Earth", TypeOf: []string{"Place"}},
				},
				"": {
					{
						Dcid:   "topic/RootHealth",
						TypeOf: []string{"Topic"},
						Name:   "Global Health Topic",
						Children: []*pbv2.ResolveResponse_Entity_Candidate{
							{Dcid: "Count_Person", TypeOf: []string{"StatisticalVariable"}, Name: "Global Population"},
						},
					},
				},
			},
			obsMockData: map[string][]string{
				"Earth": {"Count_Person"},
			},
			wantResponse: &pbv2.SearchIndicatorsResponse{
				Status: "SUCCESS",
				DcidNameMappings: map[string]string{
					"Earth":            "Earth",
					"topic/RootHealth": "Global Health Topic",
					"Count_Person":     "Global Population",
				},
				DcidPlaceTypeMappings: map[string]string{
					"Earth": "Place",
				},
				Topics: []*pbv2.SearchIndicatorsResponse_Topic{
					{
						Dcid:                 "topic/RootHealth",
						Description:          "Global Health Topic",
						MemberVariables:      []string{"Count_Person"},
						AlternateDescriptions: []string{"Global Health Topic"},
						PlacesWithData:       []string{"Earth"},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			mock := &mockMixerServer{
				resolveMockData: tc.resolveMockData,
				obsMockData:     tc.obsMockData,
			}

			// Cache is initialized with mock to support read-through checks
			cache := NewCache(mock)
			svc := NewService(mock, cache)

			got, err := svc.SearchIndicators(context.Background(), tc.request)

			if tc.expectedError != "" {
				if err == nil || err.Error() != tc.expectedError {
					t.Fatalf("SearchIndicators returned error: %v, want: %s", err, tc.expectedError)
				}
				return
			}

			if err != nil {
				t.Fatalf("SearchIndicators failed unexpectedly: %v", err)
			}

			if diff := cmp.Diff(got, tc.wantResponse, cmpOpts); diff != "" {
				t.Errorf("SearchIndicators returned unexpected response (-got +want):\n%s", diff)
			}
		})
	}
}
