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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

// mockMixerServer acts as a lightweight, self-contained in-process Mixer API engine
// that dynamically serves query requests from its internal mock data maps.
type mockMixerServer struct {
	Mixer

	// resolveMockData maps a search query node string (or place description like "World")
	// to its pre-populated list of resolved KG entity candidates.
	resolveMockData map[string][]*pbv2.ResolveResponse_Entity_Candidate

	// obsMockData maps a place DCID string to the slice of variable DCIDs
	// that have active observation series data available for that place.
	obsMockData map[string][]string
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

// V2Node mocks property retrieval for name and typeOf of entities.
func (m *mockMixerServer) V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	resp := &pbv2.NodeResponse{
		Data: make(map[string]*pbv2.LinkedGraph),
	}

	for _, node := range in.GetNodes() {
		graph := &pbv2.LinkedGraph{
			Arcs: make(map[string]*pbv2.Nodes),
		}
		if in.GetProperty() == "->[name, typeOf]" {
			switch node {
			case "geoId/06":
				graph.Arcs["name"] = &pbv2.Nodes{
					Nodes: []*pb.EntityInfo{
						{Value: "California"},
					},
				}
				graph.Arcs["typeOf"] = &pbv2.Nodes{
					Nodes: []*pb.EntityInfo{
						{Dcid: "State"},
					},
				}
			case "World":
				graph.Arcs["name"] = &pbv2.Nodes{
					Nodes: []*pb.EntityInfo{
						{Value: "World"},
					},
				}
				graph.Arcs["typeOf"] = &pbv2.Nodes{
					Nodes: []*pb.EntityInfo{
						{Dcid: "Place"},
					},
				}
			}
		}
		resp.Data[node] = graph
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
		desc    string
		request *pbv2.SearchIndicatorsRequest

		// resolveMockData maps a search query node string (or place description like "World")
		// to its pre-populated list of resolved KG entity candidates.
		resolveMockData map[string][]*pbv2.ResolveResponse_Entity_Candidate

		// obsMockData maps a place DCID string to the slice of variable DCIDs
		// that have active observation series data available for that place.
		obsMockData map[string][]string

		wantResponse  *pbv2.SearchIndicatorsResponse
		expectedError string
	}{
		{
			desc: "Basic indicator search without place constraints",
			request: &pbv2.SearchIndicatorsRequest{
				Query:          "health in america",
				PerSearchLimit: 5,
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
						Dcid:                  "topic/Health",
						Description:           "Health",
						MemberVariables:       []string{"Count_Person_WithAsthma"},
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
				IncludeTopics:  proto.Bool(true),
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
				"topic/RootHealth": {
					{
						Dcid: "topic/RootHealth",
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
				DcidPlaceTypeMappings: map[string]*structpb.ListValue{
					"Earth": util.ToStringListValue([]string{"Place"}),
				},
				Topics: []*pbv2.SearchIndicatorsResponse_Topic{
					{
						Dcid:                  "topic/RootHealth",
						Description:           "Global Health Topic",
						MemberVariables:       []string{"Count_Person"},
						AlternateDescriptions: []string{"Global Health Topic"},
						PlacesWithData:        []string{"Earth"},
					},
				},
			},
		},
		{
			desc: "Flatten and deduplicate topic candidates into standard variables when include_topics is false",
			request: &pbv2.SearchIndicatorsRequest{
				Query:          "health in america",
				PerSearchLimit: 5,
				IncludeTopics:  proto.Bool(false),
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
					{
						Dcid:   "Count_Person_WithAsthma", // Duplicate to test deduplication
						TypeOf: []string{"StatisticalVariable"},
						Name:   "People with Asthma",
					},
				},
			},
			wantResponse: &pbv2.SearchIndicatorsResponse{
				Status: "SUCCESS",
				DcidNameMappings: map[string]string{
					"Count_Person_WithAsthma":   "People with Asthma",
					"Count_Person_WithDiabetes": "People with Diabetes",
				},
				Variables: []*pbv2.SearchIndicatorsResponse_Variable{
					{
						Dcid:        "Count_Person_WithAsthma",
						Description: "People with Asthma",
					},
					{
						Dcid:        "Count_Person_WithDiabetes",
						Description: "People with Diabetes",
					},
				},
			},
		},
		{
			desc: "Serve hierarchical nested topics when expand_topics is false",
			request: &pbv2.SearchIndicatorsRequest{
				Query:          "health in america",
				PerSearchLimit: 5,
				ExpandTopics:   proto.Bool(false),
			},
			resolveMockData: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				"health in america": {
					{
						Dcid:   "topic/Health",
						TypeOf: []string{"Topic"},
						Name:   "Health",
						Children: []*pbv2.ResolveResponse_Entity_Candidate{
							{
								Dcid:   "topic/HealthSubTopic",
								TypeOf: []string{"Topic"},
								Name:   "Health Sub-Topic",
							},
							{
								Dcid:   "Count_Person_WithDiabetes",
								TypeOf: []string{"StatisticalVariable"},
								Name:   "People with Diabetes",
							},
						},
					},
				},
			},
			wantResponse: &pbv2.SearchIndicatorsResponse{
				Status: "SUCCESS",
				DcidNameMappings: map[string]string{
					"topic/Health":              "Health",
					"topic/HealthSubTopic":      "Health Sub-Topic",
					"Count_Person_WithDiabetes": "People with Diabetes",
				},
				Topics: []*pbv2.SearchIndicatorsResponse_Topic{
					{
						Dcid:                  "topic/Health",
						Description:           "Health",
						MemberTopics:          []string{"topic/HealthSubTopic"},
						MemberVariables:       []string{"Count_Person_WithDiabetes"},
						AlternateDescriptions: []string{"Health"},
					},
				},
			},
		},
		{
			desc: "Extract only immediate direct variables flat when include_topics is false and expand_topics is false",
			request: &pbv2.SearchIndicatorsRequest{
				Query:          "health in america",
				PerSearchLimit: 5,
				IncludeTopics:  proto.Bool(false),
				ExpandTopics:   proto.Bool(false),
			},
			resolveMockData: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				"health in america": {
					{
						Dcid:   "topic/Health",
						TypeOf: []string{"Topic"},
						Name:   "Health",
						Children: []*pbv2.ResolveResponse_Entity_Candidate{
							{
								Dcid:   "topic/HealthSubTopic", // Direct subtopic candidate (should be filtered out)
								TypeOf: []string{"Topic"},
								Name:   "Health Sub-Topic",
							},
							{
								Dcid:   "Count_Person_WithDiabetes", // Direct variable candidate (should be returned)
								TypeOf: []string{"StatisticalVariable"},
								Name:   "People with Diabetes",
							},
						},
					},
				},
			},
			wantResponse: &pbv2.SearchIndicatorsResponse{
				Status: "SUCCESS",
				DcidNameMappings: map[string]string{
					"Count_Person_WithDiabetes": "People with Diabetes",
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
			desc: "Truncate results correctly and do not pollute DcidNameMappings with truncated variable names",
			request: &pbv2.SearchIndicatorsRequest{
				Query:          "health in america",
				PerSearchLimit: 1,
			},
			resolveMockData: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				"health in america": {
					{
						Dcid:   "Count_Person_WithAsthma",
						TypeOf: []string{"StatisticalVariable"},
						Name:   "People with Asthma",
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
					"Count_Person_WithAsthma": "People with Asthma",
				},
				Variables: []*pbv2.SearchIndicatorsResponse_Variable{
					{
						Dcid:        "Count_Person_WithAsthma",
						Description: "People with Asthma",
					},
				},
			},
		},
		{
			desc: "Do existence checks successfully for topics when expand_topics is false",
			request: &pbv2.SearchIndicatorsRequest{
				Query:          "",
				PerSearchLimit: 5,
				IncludeTopics:  proto.Bool(true),
				ExpandTopics:   proto.Bool(false),
				Places:         []string{"geoId/06"},
			},
			resolveMockData: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				"geoId/06": {
					{Dcid: "geoId/06", Name: "California", TypeOf: []string{"State"}},
				},
				"": {
					{
						Dcid:   "topic/Health",
						TypeOf: []string{"Topic"},
						Name:   "Health",
						Children: []*pbv2.ResolveResponse_Entity_Candidate{
							{
								Dcid:   "topic/HealthSubTopic",
								TypeOf: []string{"Topic"},
								Name:   "Health Sub-Topic",
							},
						},
					},
				},
				"topic/Health": {
					{
						Dcid: "topic/Health",
						Children: []*pbv2.ResolveResponse_Entity_Candidate{
							{
								Dcid:   "Count_Person_WithDiabetes",
								TypeOf: []string{"StatisticalVariable"},
								Name:   "People with Diabetes",
							},
						},
					},
				},
				"topic/HealthSubTopic": {
					{
						Dcid: "topic/HealthSubTopic",
						Children: []*pbv2.ResolveResponse_Entity_Candidate{
							{
								Dcid:   "Count_Person_WithAsthma",
								TypeOf: []string{"StatisticalVariable"},
								Name:   "People with Asthma",
							},
						},
					},
				},
			},
			obsMockData: map[string][]string{
				"geoId/06": {"Count_Person_WithDiabetes"},
			},
			wantResponse: &pbv2.SearchIndicatorsResponse{
				Status: "SUCCESS",
				DcidNameMappings: map[string]string{
					"geoId/06":     "California",
					"topic/Health": "Health",
				},
				DcidPlaceTypeMappings: map[string]*structpb.ListValue{
					"geoId/06": util.ToStringListValue([]string{"State"}),
				},
				Topics: []*pbv2.SearchIndicatorsResponse_Topic{
					{
						Dcid:                  "topic/Health",
						Description:           "Health",
						AlternateDescriptions: []string{"Health"},
						PlacesWithData:        []string{"geoId/06"},
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
