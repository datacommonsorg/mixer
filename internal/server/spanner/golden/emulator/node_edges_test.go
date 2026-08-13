// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package emulator

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	mixerspanner "github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetNodeEdgesUnresolvedNodes(t *testing.T) {
	s := requireSuite(t)

	client, err := s.newSpannerClient(context.Background(), mixerspanner.QueryConfig{})
	if err != nil {
		t.Fatalf("newSpannerClient() error = %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	t.Run("GetNodeEdgesByID returns both resolved and unresolved edges", func(t *testing.T) {
		arc := &v2.Arc{
			Out: true,
		}
		got, err := client.GetNodeEdgesByID(ctx, []string{"country/BLZ"}, arc, 100, 0)
		if err != nil {
			t.Fatalf("GetNodeEdgesByID() error = %v", err)
		}

		edges, ok := got["country/BLZ"]
		if !ok {
			t.Fatalf("GetNodeEdgesByID() missing country/BLZ in response")
		}

		want := []*mixerspanner.Edge{
			{
				SubjectID:  "country/BLZ",
				Predicate:  "specializationOf",
				ObjectID:   "unresolved_geo_entity",
				Provenance: "dc/base/WikidataOtherIdGeos",
				Value:      "",
				Name:       "",
				Types:      []string{},
			},
			{
				SubjectID:  "country/BLZ",
				Predicate:  "typeOf",
				ObjectID:   "Country",
				Provenance: "dc/base/WikidataOtherIdGeos",
				Value:      "Country",
				Name:       "",
				Types:      []string{},
			},
		}

		if diff := cmp.Diff(want, edges); diff != "" {
			t.Errorf("GetNodeEdgesByID() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("SpannerDataSource.Node converts unresolved edges to placeholder Thing nodes", func(t *testing.T) {
		ds := mixerspanner.NewSpannerDataSource(client, nil)

		req := &pbv2.NodeRequest{
			Nodes:    []string{"country/BLZ"},
			Property: "->*",
		}
		got, err := ds.Node(ctx, req, 100)
		if err != nil {
			t.Fatalf("ds.Node() error = %v", err)
		}

		want := &pbv2.NodeResponse{
			Data: map[string]*pbv2.LinkedGraph{
				"country/BLZ": {
					Arcs: map[string]*pbv2.Nodes{
						"specializationOf": {
							Nodes: []*pb.EntityInfo{
								{
									Dcid:         "unresolved_geo_entity",
									Types:        []string{"Thing"},
									ProvenanceId: "dc/base/WikidataOtherIdGeos",
								},
							},
						},
						"typeOf": {
							Nodes: []*pb.EntityInfo{
								{
									Value:        "Country",
									ProvenanceId: "dc/base/WikidataOtherIdGeos",
								},
							},
						},
					},
				},
			},
		}

		cmpOpts := cmp.Options{
			protocmp.Transform(),
		}
		if diff := cmp.Diff(want, got, cmpOpts); diff != "" {
			t.Errorf("ds.Node() mismatch (-want +got):\n%s", diff)
		}
	})
}
