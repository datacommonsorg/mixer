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
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

type mockMixerServer struct {
	Mixer
	resolveMockData         map[string][]*pbv2.ResolveResponse_Entity_Candidate
	obsMockData             map[string][]string
	lastCandidateResolveReq *pbv2.ResolveRequest
}

func (m *mockMixerServer) V2Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	if in.GetResolver() == ResolverIndicator || (in.GetResolver() == ResolverTopic && len(in.GetNodes()) > 0 && in.GetNodes()[0] != "geoId/06") {
		m.lastCandidateResolveReq = in
	}
	resp := &pbv2.ResolveResponse{}

	lookupNodes := in.GetNodes()
	if len(lookupNodes) == 0 {
		lookupNodes = []string{""}
	}

	for _, node := range lookupNodes {
		candidates, ok := m.resolveMockData[node]
		if !ok {
			candidates = []*pbv2.ResolveResponse_Entity_Candidate{
				{Dcid: node, Name: node, TypeOf: []string{"Place"}},
			}
		}
		resp.Entities = append(resp.Entities, &pbv2.ResolveResponse_Entity{
			Node:       node,
			Candidates: candidates,
		})
	}
	return resp, nil
}

func (m *mockMixerServer) V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	resp := &pbv2.NodeResponse{Data: make(map[string]*pbv2.LinkedGraph)}
	for _, node := range in.GetNodes() {
		graph := &pbv2.LinkedGraph{
			Arcs: make(map[string]*pbv2.Nodes),
		}
		if candidates, ok := m.resolveMockData[node]; ok && len(candidates) > 0 {
			cand := candidates[0]
			graph.Arcs["name"] = &pbv2.Nodes{
				Nodes: []*pb.EntityInfo{
					{Value: cand.GetName()},
				},
			}
			var typeNodes []*pb.EntityInfo
			for _, t := range cand.GetTypeOf() {
				typeNodes = append(typeNodes, &pb.EntityInfo{Dcid: t})
			}
			graph.Arcs["typeOf"] = &pbv2.Nodes{Nodes: typeNodes}
		} else {
			graph.Arcs["name"] = &pbv2.Nodes{
				Nodes: []*pb.EntityInfo{
					{Value: node},
				},
			}
			graph.Arcs["typeOf"] = &pbv2.Nodes{
				Nodes: []*pb.EntityInfo{
					{Dcid: "Place"},
				},
			}
		}
		resp.Data[node] = graph
	}
	return resp, nil
}

func (m *mockMixerServer) V2Observation(ctx context.Context, in *pbv2.ObservationRequest) (*pbv2.ObservationResponse, error) {
	resp := &pbv2.ObservationResponse{
		ByVariable: make(map[string]*pbv2.VariableObservation),
	}
	entityDcids := in.GetEntity().GetDcids()
	varDcids := in.GetVariable().GetDcids()

	if len(varDcids) == 0 {
		for _, entityDcid := range entityDcids {
			if activeVars, ok := m.obsMockData[entityDcid]; ok {
				for _, v := range activeVars {
					resp.ByVariable[v] = &pbv2.VariableObservation{
						ByEntity: map[string]*pbv2.EntityObservation{
							entityDcid: {
								OrderedFacets: []*pbv2.FacetObservation{
									{Observations: []*pb.PointStat{{Date: "2020", Value: proto.Float64(100.0)}}},
								},
							},
						},
					}
				}
			}
		}
		return resp, nil
	}

	for _, varDcid := range varDcids {
		varObs := &pbv2.VariableObservation{
			ByEntity: make(map[string]*pbv2.EntityObservation),
		}
		for _, entityDcid := range entityDcids {
			if activeVars, ok := m.obsMockData[entityDcid]; ok {
				for _, activeVar := range activeVars {
					if activeVar == varDcid {
						varObs.ByEntity[entityDcid] = &pbv2.EntityObservation{
							OrderedFacets: []*pbv2.FacetObservation{
								{
									Observations: []*pb.PointStat{
										{Date: "2020", Value: proto.Float64(100.0)},
									},
								},
							},
						}
					}
				}
			}
		}
		resp.ByVariable[varDcid] = varObs
	}
	return resp, nil
}

var cmpOpts = cmp.Options{
	protocmp.Transform(),
	cmp.FilterPath(func(p cmp.Path) bool {
		return p.String() == "status"
	}, cmp.Ignore()),
}

func TestSearchIndicators(t *testing.T) {
	vRow1, _ := structpb.NewList([]any{
		"Amount_EconomicActivity_GrossODA",
		"Gross ODA Aid",
		[]any{},
		[]any{"donor", "recipient"},
	})
	vTable1 := &pbv2.Table{
		Columns: []string{"dcid", "name", "placesWithData", "observationProperties"},
		Rows:    []*structpb.ListValue{vRow1},
	}

	tRow1, _ := structpb.NewList([]any{
		"topic/Health",
		"Health",
		[]any{},
		[]any{},
		[]any{"Count_Person_WithAsthma"},
	})
	vRow2, _ := structpb.NewList([]any{
		"Count_Person_WithDiabetes",
		"People with Diabetes",
		[]any{},
		[]any{},
	})
	tTable2 := &pbv2.Table{
		Columns: []string{"dcid", "name", "placesWithData", "memberTopics", "memberVariables"},
		Rows:    []*structpb.ListValue{tRow1},
	}
	vTable2 := &pbv2.Table{
		Columns: []string{"dcid", "name", "placesWithData", "observationProperties"},
		Rows:    []*structpb.ListValue{vRow2},
	}

	tRow3, _ := structpb.NewList([]any{
		"topic/RootHealth",
		"Global Health Topic",
		[]any{"Earth"},
		[]any{},
		[]any{"Count_Person"},
	})
	tTable3 := &pbv2.Table{
		Columns: []string{"dcid", "name", "placesWithData", "memberTopics", "memberVariables"},
		Rows:    []*structpb.ListValue{tRow3},
	}
	vTable3 := &pbv2.Table{
		Columns: []string{"dcid", "name", "placesWithData", "observationProperties"},
		Rows:    []*structpb.ListValue{},
	}

	for _, tc := range []struct {
		desc            string
		request         *pbv2.SearchIndicatorsRequest
		resolveMockData map[string][]*pbv2.ResolveResponse_Entity_Candidate
		obsMockData     map[string][]string
		wantResponse    *pbv2.SearchIndicatorsResponse
		expectedError   string
	}{
		{
			desc: "Multi-entity variable populates observation_properties in tabular format",
			request: &pbv2.SearchIndicatorsRequest{
				Query:          "gross oda aid",
				PerSearchLimit: 5,
			},
			resolveMockData: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				"gross oda aid": {
					{
						Dcid:                  "Amount_EconomicActivity_GrossODA",
						TypeOf:                []string{"StatisticalVariable"},
						Name:                  "Gross ODA Aid",
						ObservationProperties: []string{"donor", "recipient"},
					},
				},
			},
			wantResponse: &pbv2.SearchIndicatorsResponse{
				Status:    "SUCCESS",
				Topics:    &pbv2.Table{Columns: []string{"dcid", "name", "placesWithData", "memberTopics", "memberVariables"}, Rows: []*structpb.ListValue{}},
				Variables: vTable1,
			},
		},
		{
			desc: "Basic indicator search returning tabular topics and variables",
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
				Status:    "SUCCESS",
				Topics:    tTable2,
				Variables: vTable2,
			},
		},
		{
			desc: "Browsing root topics defaults to World place resolution and tabular output",
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
				Status:    "SUCCESS",
				Topics:    tTable3,
				Variables: vTable3,
			},
		},
		{
			desc: "Using place_dcids directly bypasses string place resolution",
			request: &pbv2.SearchIndicatorsRequest{
				Query:          "gross oda aid",
				PerSearchLimit: 5,
				PlaceDcids:     []string{"geoId/06"},
			},
			resolveMockData: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
				"gross oda aid": {
					{
						Dcid:                  "Amount_EconomicActivity_GrossODA",
						TypeOf:                []string{"StatisticalVariable"},
						Name:                  "Gross ODA Aid",
						ObservationProperties: []string{"donor", "recipient"},
					},
				},
			},
			obsMockData: map[string][]string{
				"geoId/06": {"Amount_EconomicActivity_GrossODA"},
			},
			wantResponse: &pbv2.SearchIndicatorsResponse{
				Status: "SUCCESS",
				Topics: &pbv2.Table{Columns: []string{"dcid", "name", "placesWithData", "memberTopics", "memberVariables"}, Rows: []*structpb.ListValue{}},
				Variables: &pbv2.Table{
					Columns: []string{"dcid", "name", "placesWithData", "observationProperties"},
					Rows: []*structpb.ListValue{
						func() *structpb.ListValue {
							row, _ := structpb.NewList([]any{
								"Amount_EconomicActivity_GrossODA",
								"Gross ODA Aid",
								[]any{"geoId/06"},
								[]any{"donor", "recipient"},
							})
							return row
						}(),
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

func TestSearchIndicators_Target(t *testing.T) {
	for _, tc := range []struct {
		name       string
		request    *pbv2.SearchIndicatorsRequest
		wantTarget string
		wantErr    string
	}{
		{
			name: "custom_only target",
			request: &pbv2.SearchIndicatorsRequest{
				Query:      "health",
				PlaceDcids: []string{"geoId/06"},
				Target:     proto.String("custom_only"),
			},
			wantTarget: "custom_only",
		},
		{
			name: "base_only target",
			request: &pbv2.SearchIndicatorsRequest{
				Query:      "health",
				PlaceDcids: []string{"geoId/06"},
				Target:     proto.String("base_only"),
			},
			wantTarget: "base_only",
		},
		{
			name: "invalid target returns error",
			request: &pbv2.SearchIndicatorsRequest{
				Query:  "health",
				Target: proto.String("invalid_target"),
			},
			wantErr: "rpc error: code = InvalidArgument desc = invalid target: invalid_target, valid values are 'base_and_custom', 'base_only', 'custom_only'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockMixerServer{
				resolveMockData: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
					"health": {
						{
							Dcid:         "Count_Person",
							DominantType: "StatisticalVariable",
							Name:         "Population",
						},
					},
					"geoId/06": {
						{
							Dcid:         "geoId/06",
							DominantType: "State",
						},
					},
				},
				obsMockData: map[string][]string{
					"geoId/06": {"Count_Person"},
				},
			}

			cache := NewCache(mock)
			svc := NewService(mock, cache)

			_, err := svc.SearchIndicators(context.Background(), tc.request)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("SearchIndicators error = %v, want: %s", err, tc.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("SearchIndicators failed unexpectedly: %v", err)
			}

			if mock.lastCandidateResolveReq == nil {
				t.Fatalf("expected V2Resolve candidate search to be called, but lastCandidateResolveReq is nil")
			}

			if gotTarget := mock.lastCandidateResolveReq.GetTarget(); gotTarget != tc.wantTarget {
				t.Errorf("V2Resolve target = %q, want: %q", gotTarget, tc.wantTarget)
			}
		})
	}
}
