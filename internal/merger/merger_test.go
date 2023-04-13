// Copyright 2023 Google LLC
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

// Package merger provides function to merge V2 API ressponses.
package merger

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestMergeResolve(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		r1   *pbv2.ResolveResponse
		r2   *pbv2.ResolveResponse
		want *pbv2.ResolveResponse
	}{
		{
			&pbv2.ResolveResponse{
				Entities: []*pbv2.ResolveResponse_Entity{
					{
						Node:        "node1",
						ResolvedIds: []string{"id1.1", "id1.3"},
					},
				},
			},
			&pbv2.ResolveResponse{
				Entities: []*pbv2.ResolveResponse_Entity{
					{
						Node:        "node1",
						ResolvedIds: []string{"id1.2"},
					},
					{
						Node:        "node2",
						ResolvedIds: []string{"id2.1"},
					},
				},
			},
			&pbv2.ResolveResponse{
				Entities: []*pbv2.ResolveResponse_Entity{
					{
						Node:        "node1",
						ResolvedIds: []string{"id1.1", "id1.2", "id1.3"},
					},
					{
						Node:        "node2",
						ResolvedIds: []string{"id2.1"},
					},
				},
			},
		},
	} {
		got := MergeResolve(c.r1, c.r2)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("MergeResolve(%v, %v) got diff: %s", c.r1, c.r2, diff)
		}
	}
}

func TestMergeObservation(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		o1   *pbv2.ObservationResponse
		o2   *pbv2.ObservationResponse
		want *pbv2.ObservationResponse
	}{
		{
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet1",
										Observations: []*pb.PointStat{
											{
												Date:  "2021",
												Value: proto.Float64(45.4),
											},
										},
									},
									{
										FacetId: "facet2",
										Observations: []*pb.PointStat{
											{
												Date:  "2022",
												Value: proto.Float64(12.2),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet3",
										Observations: []*pb.PointStat{
											{
												Date:  "2023",
												Value: proto.Float64(66.4),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			&pbv2.ObservationResponse{
				ByVariable: map[string]*pbv2.VariableObservation{
					"var1": {
						ByEntity: map[string]*pbv2.EntityObservation{
							"entity1": {
								OrderedFacets: []*pbv2.FacetObservation{
									{
										FacetId: "facet1",
										Observations: []*pb.PointStat{
											{
												Date:  "2021",
												Value: proto.Float64(45.4),
											},
										},
									},
									{
										FacetId: "facet2",
										Observations: []*pb.PointStat{
											{
												Date:  "2022",
												Value: proto.Float64(12.2),
											},
										},
									},
									{
										FacetId: "facet3",
										Observations: []*pb.PointStat{
											{
												Date:  "2023",
												Value: proto.Float64(66.4),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	} {
		got := MergeObservation(c.o1, c.o2)
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("MergeObservation(%v, %v) got diff: %s", c.o1, c.o2, diff)
		}
	}
}
