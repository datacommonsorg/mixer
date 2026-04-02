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

package spanner

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

type coordinateMockSpannerClient struct {
	getNodeEdgesByProp map[string]map[string][]*Edge
	assertGetNodeEdges func(ids []string, arc *v2.Arc, pageSize, offset int)
}

func (m *coordinateMockSpannerClient) GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*Property, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc, pageSize, offset int) (map[string][]*Edge, error) {
	if m.assertGetNodeEdges != nil {
		m.assertGetNodeEdges(ids, arc, pageSize, offset)
	}
	if arc != nil {
		if result, ok := m.getNodeEdgesByProp[arc.SingleProp]; ok {
			return result, nil
		}
	}
	return map[string][]*Edge{}, nil
}

func (m *coordinateMockSpannerClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) SearchNodes(ctx context.Context, query string, types []string) ([]*SearchNode, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) ResolveByID(ctx context.Context, nodes []string, in, out string) (map[string][]string, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) GetEventCollectionDate(ctx context.Context, placeID, eventType string) ([]string, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) GetEventCollectionDcids(ctx context.Context, placeID, eventType, date string) ([]EventIdWithMagnitudeDcid, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) GetEventCollection(ctx context.Context, req *pbv1.EventCollectionRequest) (*pbv1.EventCollection, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) Sparql(ctx context.Context, nodes []types.Node, queries []*types.Query, opts *types.QueryOptions) ([][]string, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) GetProvenanceSummary(ctx context.Context, ids []string) (map[string]map[string]*pb.StatVarSummary_ProvenanceSummary, error) {
	return nil, nil
}

func (m *coordinateMockSpannerClient) Id() string { return "mock" }
func (m *coordinateMockSpannerClient) Start()     {}
func (m *coordinateMockSpannerClient) Close()     {}

func TestResolveCoordinate(t *testing.T) {
	t.Parallel()

	cellID := level10S2CellID(37.42, -122.08)
	ds := NewSpannerDataSource(&coordinateMockSpannerClient{
		assertGetNodeEdges: func(ids []string, arc *v2.Arc, pageSize, offset int) {
			if arc == nil || arc.SingleProp != "containedInPlace" {
				t.Fatalf("unexpected arc: %+v", arc)
			}
			if pageSize != 50 {
				t.Fatalf("GetNodeEdgesByID() pageSize = %d, want 50", pageSize)
			}
			if offset != 0 {
				t.Fatalf("GetNodeEdgesByID() offset = %d, want 0", offset)
			}
			if len(ids) != 1 || ids[0] != cellID {
				t.Fatalf("GetNodeEdgesByID() ids = %v, want [%s]", ids, cellID)
			}
		},
		getNodeEdgesByProp: map[string]map[string][]*Edge{
			"containedInPlace": {
				cellID: {
					{
						SubjectID: cellID,
						Predicate: "containedInPlace",
						Value:     "geoId/06085",
						Types:     []string{"County", "AdministrativeArea2"},
					},
					{
						SubjectID: cellID,
						Predicate: "containedInPlace",
						Value:     "geoId/06",
						Types:     []string{"State", "AdministrativeArea1"},
					},
				},
			},
		},
	}, nil, nil)

	got, err := ds.Resolve(context.Background(), &pbv2.ResolveRequest{
		Nodes:    []string{"37.42#-122.08"},
		Property: "<-geoCoordinate->dcid",
	})
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	want := &pbv2.ResolveResponse{
		Entities: []*pbv2.ResolveResponse_Entity{
			{
				Node: "37.42#-122.08",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
					{Dcid: "geoId/06085", DominantType: "County"},
					{Dcid: "geoId/06", DominantType: "State"},
				},
			},
		},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("Resolve() diff (-want +got):\n%s", diff)
	}
}

func TestResolveCoordinateFetchesAllCellsInBatch(t *testing.T) {
	t.Parallel()

	cellID1 := level10S2CellID(37.42, -122.08)
	cellID2 := level10S2CellID(36.77, -119.41)
	ds := NewSpannerDataSource(&coordinateMockSpannerClient{
		assertGetNodeEdges: func(ids []string, arc *v2.Arc, pageSize, offset int) {
			if arc == nil || arc.SingleProp != "containedInPlace" {
				t.Fatalf("unexpected arc: %+v", arc)
			}
			if pageSize != 100 {
				t.Fatalf("GetNodeEdgesByID() pageSize = %d, want 100", pageSize)
			}
			if offset != 0 {
				t.Fatalf("GetNodeEdgesByID() offset = %d, want 0", offset)
			}
			if len(ids) != 2 {
				t.Fatalf("GetNodeEdgesByID() ids len = %d, want 2", len(ids))
			}
		},
		getNodeEdgesByProp: map[string]map[string][]*Edge{
			"containedInPlace": {
				cellID1: {
					{
						SubjectID: cellID1,
						Predicate: "containedInPlace",
						Value:     "geoId/06085",
						Types:     []string{"County", "AdministrativeArea2"},
					},
				},
				cellID2: {
					{
						SubjectID: cellID2,
						Predicate: "containedInPlace",
						Value:     "geoId/06019",
						Types:     []string{"County", "AdministrativeArea2"},
					},
				},
			},
		},
	}, nil, nil)

	got, err := ds.Resolve(context.Background(), &pbv2.ResolveRequest{
		Nodes:    []string{"37.42#-122.08", "36.77#-119.41"},
		Property: "<-geoCoordinate->dcid",
	})
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	want := &pbv2.ResolveResponse{
		Entities: []*pbv2.ResolveResponse_Entity{
			{
				Node: "37.42#-122.08",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
					{Dcid: "geoId/06085", DominantType: "County"},
				},
			},
			{
				Node: "36.77#-119.41",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
					{Dcid: "geoId/06019", DominantType: "County"},
				},
			},
		},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("Resolve() diff (-want +got):\n%s", diff)
	}
}

func TestResolveCoordinateTypeFilterUsesDominantType(t *testing.T) {
	t.Parallel()

	cellID := level10S2CellID(37.42, -122.08)
	ds := NewSpannerDataSource(&coordinateMockSpannerClient{
		getNodeEdgesByProp: map[string]map[string][]*Edge{
			"containedInPlace": {
				cellID: {
					{
						SubjectID: cellID,
						Predicate: "containedInPlace",
						Value:     "geoId/06085",
						Types:     []string{"County", "AdministrativeArea2"},
					},
					{
						SubjectID: cellID,
						Predicate: "containedInPlace",
						Value:     "geoId/06",
						Types:     []string{"State", "AdministrativeArea1"},
					},
				},
			},
		},
	}, nil, nil)

	got, err := ds.Resolve(context.Background(), &pbv2.ResolveRequest{
		Nodes:    []string{"37.42#-122.08"},
		Property: "<-geoCoordinate{typeOf:AdministrativeArea2}->dcid",
	})
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	want := &pbv2.ResolveResponse{
		Entities: []*pbv2.ResolveResponse_Entity{
			{
				Node:       "37.42#-122.08",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{},
			},
		},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("Resolve() diff (-want +got):\n%s", diff)
	}
}

func TestResolveCoordinateSkipsS2CellCandidatesByType(t *testing.T) {
	t.Parallel()

	cellID := level10S2CellID(37.42, -122.08)
	ds := NewSpannerDataSource(&coordinateMockSpannerClient{
		getNodeEdgesByProp: map[string]map[string][]*Edge{
			"containedInPlace": {
				cellID: {
					{
						SubjectID: cellID,
						Predicate: "containedInPlace",
						Value:     level10S2CellID(37.43, -122.09),
						Types:     []string{"S2CellLevel10"},
					},
					{
						SubjectID: cellID,
						Predicate: "containedInPlace",
						Value:     "geoId/06085",
						Types:     []string{"County", "AdministrativeArea2"},
					},
				},
			},
		},
	}, nil, nil)

	got, err := ds.Resolve(context.Background(), &pbv2.ResolveRequest{
		Nodes:    []string{"37.42#-122.08"},
		Property: "<-geoCoordinate->dcid",
	})
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	want := &pbv2.ResolveResponse{
		Entities: []*pbv2.ResolveResponse_Entity{
			{
				Node: "37.42#-122.08",
				Candidates: []*pbv2.ResolveResponse_Entity_Candidate{
					{Dcid: "geoId/06085", DominantType: "County"},
				},
			},
		},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("Resolve() diff (-want +got):\n%s", diff)
	}
}

