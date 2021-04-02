// Copyright 2020 Google LLC
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

package server

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestFilter(t *testing.T) {

	placeSVs := []string{"sv1", "sv21", "sv91"}
	input := &pb.StatVarGroups{
		StatVarGroups: map[string]*pb.StatVarGroupNode{
			"svg1": {
				ChildStatVars: []string{"sv1", "sv2"},
				ChildStatVarGroups: []*pb.StatVarGroupNode_Child{
					{Id: "svg2"},
					{Id: "svg3"},
				},
			},
			"svg2": {
				ChildStatVars: []string{"sv21", "sv22"},
				ChildStatVarGroups: []*pb.StatVarGroupNode_Child{
					{Id: "svg5"},
					{Id: "svg6"},
				},
			},
			"svg8": {
				ChildStatVarGroups: []*pb.StatVarGroupNode_Child{
					{Id: "svg9"},
				},
			},
			"svg9": {
				ChildStatVars: []string{"sv91", "sv92"},
			},
		},
	}
	want := &pb.StatVarGroups{
		StatVarGroups: map[string]*pb.StatVarGroupNode{
			"svg1": {
				ChildStatVars: []string{"sv1"},
				ChildStatVarGroups: []*pb.StatVarGroupNode_Child{
					{Id: "svg2"},
				},
			},
			"svg2": {
				ChildStatVars: []string{"sv21"},
			},
			"svg8": {
				ChildStatVarGroups: []*pb.StatVarGroupNode_Child{
					{Id: "svg9"},
				},
			},
			"svg9": {
				ChildStatVars: []string{"sv91"},
			},
		},
	}
	got := filterSVG(input, placeSVs)
	if diff := cmp.Diff(got, want, protocmp.Transform()); diff != "" {
		t.Errorf("filterSVG() got diff %v", diff)
	}
}
