// Copyright 2025 Google LLC
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
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestProcessNodeRequest(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		description string
		in          *v2.Arc
		out         *v2.Arc
		artifacts   *nodeArtifacts
	}{
		{
			"no change",
			&v2.Arc{
				SingleProp: "name",
			},
			&v2.Arc{
				SingleProp: "name",
			},
			&nodeArtifacts{},
		},
		{
			"linkedContainedInPlace",
			&v2.Arc{
				SingleProp: "containedInPlace",
				Decorator:  "+",
				Filter: map[string][]string{
					"typeOf": {"State"},
				},
			},
			&v2.Arc{
				SingleProp: "linkedContainedInPlace",
				Filter: map[string][]string{
					"typeOf": {"State"},
				},
			},
			&nodeArtifacts{
				chainProp: "containedInPlace",
			},
		},
	} {
		actual := processNodeRequest(c.in)

		if diff := cmp.Diff(c.in, c.out, cmpOpts); diff != "" {
			t.Errorf("ProcessNodeRequest(%v) got diff: %s", c.in, diff)
		}

		if !reflect.DeepEqual(actual, c.artifacts) {
			t.Errorf("Artifacts are not equal.\nExpected: %+v\nActual:   %+v", c.artifacts, actual)
		}
	}
}

func TestProcessNodeResponse(t *testing.T) {
	cmpOpts := cmp.Options{
		protocmp.Transform(),
	}

	for _, c := range []struct {
		description string
		in          *pbv2.NodeResponse
		artifacts   *nodeArtifacts
		out         *pbv2.NodeResponse
	}{
		{
			"no change",
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"name": {
								Nodes: []*pb.EntityInfo{
									{Value: "name1"},
								},
							},
						},
					},
				},
			},
			&nodeArtifacts{},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"name": {
								Nodes: []*pb.EntityInfo{
									{Value: "name1"},
								},
							},
						},
					},
				},
			},
		},
		{
			"linkedContainedInPlace",
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"linkedContainedInPlace": {
								Nodes: []*pb.EntityInfo{
									{Dcid: "place1"},
								},
							},
						},
					},
				},
			},
			&nodeArtifacts{
				chainProp: "containedInPlace",
			},
			&pbv2.NodeResponse{
				Data: map[string]*pbv2.LinkedGraph{
					"dcid1": {
						Arcs: map[string]*pbv2.Nodes{
							"containedInPlace+": {
								Nodes: []*pb.EntityInfo{
									{Dcid: "place1"},
								},
							},
						},
					},
				},
			},
		},
	} {
		processNodeResponse(c.in, c.artifacts)

		if diff := cmp.Diff(c.in, c.out, cmpOpts); diff != "" {
			t.Errorf("ProcessNodeResponse(%v) got diff: %s", c.in, diff)
		}
	}
}
