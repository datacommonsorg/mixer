// Copyright 2024 Google LLC
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

package merge

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestMergeNode(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		allResp []*pbv3.NodeResponse
		want    *pbv3.NodeResponse
	}{
		{
			[]*pbv3.NodeResponse{
				&pbv3.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"node_1": {
							Arcs: map[string]*pbv2.Nodes{
								"prop_1": &pbv2.Nodes{
									Nodes: []*pb.EntityInfo{
										{
											Dcid: "node_2",
										},
									},
								},
							},
						},
					},
				},
				&pbv3.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"node_1": {
							Arcs: map[string]*pbv2.Nodes{
								"prop_1": &pbv2.Nodes{
									Nodes: []*pb.EntityInfo{
										{
											Dcid: "node_3",
										},
									},
								},
							},
						},
						"node_2": {
							Arcs: map[string]*pbv2.Nodes{
								"prop_1": &pbv2.Nodes{
									Nodes: []*pb.EntityInfo{
										{
											Dcid: "node_1",
										},
									},
								},
							},
						},
					},
				},
				&pbv3.NodeResponse{
					Data: map[string]*pbv2.LinkedGraph{
						"node_2": {
							Arcs: map[string]*pbv2.Nodes{
								"prop_2": &pbv2.Nodes{
									Nodes: []*pb.EntityInfo{
										{
											Dcid: "node_3",
										},
									},
								},
							},
						},
						"node_3": {
							Arcs: map[string]*pbv2.Nodes{
								"prop_2": &pbv2.Nodes{
									Nodes: []*pb.EntityInfo{
										{
											Dcid: "node_1",
										},
									},
								},
							},
						},
					},
				},
			},
			&pbv3.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"node_1": {
						Arcs: map[string]*pbv2.Nodes{
							"prop_1": &pbv2.Nodes{
								Nodes: []*pb.EntityInfo{
									{
										Dcid: "node_2",
									},
									{
										Dcid: "node_3",
									},
								},
							},
						},
					},
					"node_2": {
						Arcs: map[string]*pbv2.Nodes{
							"prop_1": &pbv2.Nodes{
								Nodes: []*pb.EntityInfo{
									{
										Dcid: "node_1",
									},
								},
							},
							"prop_2": &pbv2.Nodes{
								Nodes: []*pb.EntityInfo{
									{
										Dcid: "node_3",
									},
								},
							},
						},
					},
					"node_3": {
						Arcs: map[string]*pbv2.Nodes{
							"prop_2": &pbv2.Nodes{
								Nodes: []*pb.EntityInfo{
									{
										Dcid: "node_1",
									},
								},
							},
						},
					},
				},
			},
		},
	} {
		got, err := MergeNode(c.allResp)
		if err != nil {
			t.Errorf("Error running MergeNode(%v): %s", c.allResp, err)
		}
		if diff := cmp.Diff(got, c.want, cmpOpts); diff != "" {
			t.Errorf("MergeNode(%v) got diff: %s", c.allResp, diff)
		}
	}
}
