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

package dispatcher

import (
	"context"
	"errors"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestUnresolvedNodeProcessor_PostProcess(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name             string
		requestType      RequestType
		hasSource        bool
		initialResponse  proto.Message
		mockNodeResponse *pbv2.NodeResponse
		mockNodeError    error
		expectNodeCalled bool
		expectedOutcome  Outcome
		expectedErr      bool
		expectedResponse proto.Message
	}{
		{
			name:        "Happy Path: Hydrates name and types for unresolved nodes",
			requestType: TypeNode,
			hasSource:   true,
			initialResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/BLZ": {
						Arcs: map[string]*pbv2.Nodes{
							"specializationOf": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:         "unresolved_geo_entity",
										Types:        []string{"Thing"},
										ProvenanceId: "dc/base/Wikidata",
									},
								},
							},
						},
					},
				},
			},
			mockNodeResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"unresolved_geo_entity": {
						Arcs: map[string]*pbv2.Nodes{
							"name": {
								Nodes: []*pb.EntityInfo{
									{Value: "Unresolved Geo Entity"},
								},
							},
							"typeOf": {
								Nodes: []*pb.EntityInfo{
									{Dcid: "Place"},
									{Dcid: "AdministrativeArea"},
								},
							},
						},
					},
				},
			},
			expectNodeCalled: true,
			expectedOutcome:  Continue,
			expectedErr:      false,
			expectedResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/BLZ": {
						Arcs: map[string]*pbv2.Nodes{
							"specializationOf": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:         "unresolved_geo_entity",
										Name:         "Unresolved Geo Entity",
										Types:        []string{"Place", "AdministrativeArea"},
										ProvenanceId: "dc/base/Wikidata",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:        "No-op: Already resolved entities are not queried",
			requestType: TypeNode,
			hasSource:   true,
			initialResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/USA": {
						Arcs: map[string]*pbv2.Nodes{
							"typeOf": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:         "Country",
										Name:         "Country",
										Types:        []string{"Class"},
										ProvenanceId: "dc/base/Wikidata",
									},
								},
							},
							"name": {
								Nodes: []*pb.EntityInfo{
									{
										Value:        "United States",
										ProvenanceId: "dc/base/Wikidata",
									},
								},
							},
						},
					},
				},
			},
			expectNodeCalled: false,
			expectedOutcome:  Continue,
			expectedErr:      false,
			expectedResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/USA": {
						Arcs: map[string]*pbv2.Nodes{
							"typeOf": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:         "Country",
										Name:         "Country",
										Types:        []string{"Class"},
										ProvenanceId: "dc/base/Wikidata",
									},
								},
							},
							"name": {
								Nodes: []*pb.EntityInfo{
									{
										Value:        "United States",
										ProvenanceId: "dc/base/Wikidata",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:        "Fallback on error: Preserves placeholder when datasource call fails",
			requestType: TypeNode,
			hasSource:   true,
			initialResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/BLZ": {
						Arcs: map[string]*pbv2.Nodes{
							"specializationOf": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:         "unresolved_geo_entity",
										Types:        []string{"Thing"},
										ProvenanceId: "dc/base/Wikidata",
									},
								},
							},
						},
					},
				},
			},
			mockNodeError:    errors.New("datasource network timeout"),
			expectNodeCalled: true,
			expectedOutcome:  Continue,
			expectedErr:      false,
			expectedResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/BLZ": {
						Arcs: map[string]*pbv2.Nodes{
							"specializationOf": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:         "unresolved_geo_entity",
										Types:        []string{"Thing"},
										ProvenanceId: "dc/base/Wikidata",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:        "Entity missing in datasource: Preserves placeholder when no metadata returned",
			requestType: TypeNode,
			hasSource:   true,
			initialResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/BLZ": {
						Arcs: map[string]*pbv2.Nodes{
							"specializationOf": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:         "unresolved_geo_entity",
										Types:        []string{"Thing"},
										ProvenanceId: "dc/base/Wikidata",
									},
								},
							},
						},
					},
				},
			},
			mockNodeResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{},
			},
			expectNodeCalled: true,
			expectedOutcome:  Continue,
			expectedErr:      false,
			expectedResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/BLZ": {
						Arcs: map[string]*pbv2.Nodes{
							"specializationOf": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:         "unresolved_geo_entity",
										Types:        []string{"Thing"},
										ProvenanceId: "dc/base/Wikidata",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:             "No-op: Non-Node request type",
			requestType:      TypeObservation,
			hasSource:        true,
			initialResponse:  &pbv2.ObservationResponse{},
			expectNodeCalled: false,
			expectedOutcome:  Continue,
			expectedErr:      false,
			expectedResponse: &pbv2.ObservationResponse{},
		},
		{
			name:        "No-op: No source configured",
			requestType: TypeNode,
			hasSource:   false,
			initialResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/BLZ": {
						Arcs: map[string]*pbv2.Nodes{
							"specializationOf": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:  "unresolved_geo_entity",
										Types: []string{"Thing"},
									},
								},
							},
						},
					},
				},
			},
			expectNodeCalled: false,
			expectedOutcome:  Continue,
			expectedErr:      false,
			expectedResponse: &pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"country/BLZ": {
						Arcs: map[string]*pbv2.Nodes{
							"specializationOf": {
								Nodes: []*pb.EntityInfo{
									{
										Dcid:  "unresolved_geo_entity",
										Types: []string{"Thing"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeCalled := false
			var capturedReq *pbv2.NodeRequest

			mock := &mockSource{
				id: "mock-source",
				nodeFunc: func(ctx context.Context, req *pbv2.NodeRequest, pageSize int) (*pbv2.NodeResponse, error) {
					nodeCalled = true
					capturedReq = req
					if tc.mockNodeError != nil {
						return nil, tc.mockNodeError
					}
					if tc.mockNodeResponse != nil {
						return tc.mockNodeResponse, nil
					}
					return &pbv2.NodeResponse{}, nil
				},
			}

			var processor *UnresolvedNodeProcessor
			if tc.hasSource {
				processor = NewUnresolvedNodeProcessor(mock)
			} else {
				processor = NewUnresolvedNodeProcessor(nil)
			}

			rc := &RequestContext{
				Context:         ctx,
				Type:            tc.requestType,
				CurrentResponse: tc.initialResponse,
			}

			outcome, err := processor.PostProcess(rc)

			if (err != nil) != tc.expectedErr {
				t.Fatalf("PostProcess() error = %v, expectedErr %v", err, tc.expectedErr)
			}

			if outcome != tc.expectedOutcome {
				t.Errorf("PostProcess() outcome = %v, want %v", outcome, tc.expectedOutcome)
			}

			if nodeCalled != tc.expectNodeCalled {
				t.Errorf("PostProcess() nodeCalled = %v, want %v", nodeCalled, tc.expectNodeCalled)
			}

			if tc.expectNodeCalled && capturedReq != nil {
				if capturedReq.Property != nodeInfoProperty {
					t.Errorf("NodeRequest.Property = %q, want %q", capturedReq.Property, nodeInfoProperty)
				}
			}

			cmpOpts := cmp.Options{
				protocmp.Transform(),
			}
			if diff := cmp.Diff(rc.CurrentResponse, tc.expectedResponse, cmpOpts); diff != "" {
				t.Errorf("CurrentResponse mismatch (-got +want):\n%s", diff)
			}
		})
	}
}
