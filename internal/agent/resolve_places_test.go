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
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

type mockResolvePlacesServer struct {
	Mixer
	resolveCandidates map[string][]*pbv2.ResolveResponse_Entity_Candidate
	nodeData          map[string]*pbv2.LinkedGraph
}

func (m *mockResolvePlacesServer) V2Resolve(ctx context.Context, in *pbv2.ResolveRequest) (*pbv2.ResolveResponse, error) {
	resp := &pbv2.ResolveResponse{}
	for _, node := range in.GetNodes() {
		candidates := m.resolveCandidates[node]
		resp.Entities = append(resp.Entities, &pbv2.ResolveResponse_Entity{
			Node:       node,
			Candidates: candidates,
		})
	}
	return resp, nil
}

func (m *mockResolvePlacesServer) V2Node(ctx context.Context, in *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
	resp := &pbv2.NodeResponse{Data: make(map[string]*pbv2.LinkedGraph)}
	for _, node := range in.GetNodes() {
		if graph, ok := m.nodeData[node]; ok {
			resp.Data[node] = graph
		}
	}
	return resp, nil
}

func TestResolvePlaces(t *testing.T) {
	ctx := context.Background()

	mock := &mockResolvePlacesServer{
		resolveCandidates: map[string][]*pbv2.ResolveResponse_Entity_Candidate{
			"California": {
				{Dcid: "geoId/06", Name: "California", TypeOf: []string{"State"}},
			},
			"Scotland": {
				{Dcid: "country/SCO", Name: "Scotland", TypeOf: []string{"Country"}},
				{Dcid: "geoId/29199", Name: "Scotland County", TypeOf: []string{"County"}},
			},
		},
		nodeData: map[string]*pbv2.LinkedGraph{
			"geoId/06": {
				Arcs: map[string]*pbv2.Nodes{
					"name":   {Nodes: []*pb.EntityInfo{{Value: "California"}}},
					"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "State"}}},
				},
			},
			"country/SCO": {
				Arcs: map[string]*pbv2.Nodes{
					"name":   {Nodes: []*pb.EntityInfo{{Value: "Scotland"}}},
					"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "Country"}}},
				},
			},
			"geoId/29199": {
				Arcs: map[string]*pbv2.Nodes{
					"name":   {Nodes: []*pb.EntityInfo{{Value: "Scotland County"}}},
					"typeOf": {Nodes: []*pb.EntityInfo{{Dcid: "County"}}},
				},
			},
		},
	}

	service := NewService(mock, nil)

	t.Run("EmptyPlaces", func(t *testing.T) {
		req := &pbv2.ResolvePlacesRequest{}
		got, err := service.ResolvePlaces(ctx, req)
		if err != nil {
			t.Fatalf("ResolvePlaces failed: %v", err)
		}
		want := &pbv2.ResolvePlacesResponse{
			Status: StatusSuccess,
			ResolvedPlaces: &pbv2.Table{
				Columns: []string{"query", "dcid", "name", "typeOf"},
				Rows:    []*structpb.ListValue{},
			},
		}
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Errorf("ResolvePlaces() diff (-want +got):\n%s", diff)
		}
	})

	t.Run("ValidPlaces", func(t *testing.T) {
		req := &pbv2.ResolvePlacesRequest{
			Places: []string{"California", "Scotland"},
		}
		got, err := service.ResolvePlaces(ctx, req)
		if err != nil {
			t.Fatalf("ResolvePlaces failed: %v", err)
		}

		r1, _ := structpb.NewList([]any{"California", "geoId/06", "California", []any{"State"}})
		r2, _ := structpb.NewList([]any{"Scotland", "country/SCO", "Scotland", []any{"Country"}})
		r3, _ := structpb.NewList([]any{"Scotland", "geoId/29199", "Scotland County", []any{"County"}})

		want := &pbv2.ResolvePlacesResponse{
			Status: StatusSuccess,
			ResolvedPlaces: &pbv2.Table{
				Columns: []string{"query", "dcid", "name", "typeOf"},
				Rows:    []*structpb.ListValue{r1, r2, r3},
			},
		}
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Errorf("ResolvePlaces() diff (-want +got):\n%s", diff)
		}
	})

	t.Run("UnresolvedPlaces", func(t *testing.T) {
		req := &pbv2.ResolvePlacesRequest{
			Places: []string{"California", "NonExistentPlace"},
		}
		got, err := service.ResolvePlaces(ctx, req)
		if err != nil {
			t.Fatalf("ResolvePlaces failed: %v", err)
		}

		r1, _ := structpb.NewList([]any{"California", "geoId/06", "California", []any{"State"}})
		r2, _ := structpb.NewList([]any{"NonExistentPlace", "", "", []any{}})

		want := &pbv2.ResolvePlacesResponse{
			Status: StatusSuccess,
			ResolvedPlaces: &pbv2.Table{
				Columns: []string{"query", "dcid", "name", "typeOf"},
				Rows:    []*structpb.ListValue{r1, r2},
			},
		}
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Errorf("ResolvePlaces() diff (-want +got):\n%s", diff)
		}
	})
}
