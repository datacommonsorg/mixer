// Copyright 2021 Google LLC
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

package statvar

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestFilter(t *testing.T) {
	for _, c := range []struct {
		input *pb.StatVarGroups
		want  *pb.StatVarGroups
		svs   []string
	}{
		{
			&pb.StatVarGroups{
				StatVarGroups: map[string]*pb.StatVarGroupNode{
					"svgX": {
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "svgY"},
						},
					},
					"svgY": {
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "svgZ"},
						},
					},
					"svgZ": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{
								Id:         "sv1",
								SearchName: "Name 1",
							},
							{
								Id:         "sv2",
								SearchName: "Name 2",
							},
						},
					},
				},
			},
			&pb.StatVarGroups{
				StatVarGroups: map[string]*pb.StatVarGroupNode{
					"svgX": {
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "svgY"},
						},
					},
					"svgY": {
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "svgZ"},
						},
					},
					"svgZ": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{
								Id:         "sv1",
								SearchName: "Name 1",
							},
							{
								Id:         "sv2",
								SearchName: "Name 2",
							},
						},
					},
				},
			},
			[]string{"sv1", "sv2"},
		},
		{
			&pb.StatVarGroups{
				StatVarGroups: map[string]*pb.StatVarGroupNode{
					"svg1": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{
								Id:         "sv1",
								SearchName: "Name 1",
							},
							{
								Id:         "sv2",
								SearchName: "Name 2",
							},
						},
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "svg2"},
							{Id: "svg3"},
						},
					},
					"svg2": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{
								Id:         "sv21",
								SearchName: "Name 21",
							},
							{
								Id:         "sv22",
								SearchName: "Name 22",
							},
						},
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "svg5"},
							{Id: "svg6"},
						},
					},
					"svg8": {
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "svg9"},
						},
					},
					"svg9": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{
								Id:         "sv91",
								SearchName: "Name 91",
							},
							{
								Id:         "sv92",
								SearchName: "Name 92",
							},
						},
					},
				},
			},
			&pb.StatVarGroups{
				StatVarGroups: map[string]*pb.StatVarGroupNode{
					"svg1": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{
								Id:         "sv1",
								SearchName: "Name 1",
							},
						},
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "svg2"},
						},
					},
					"svg2": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{
								Id:         "sv21",
								SearchName: "Name 21",
							},
						},
					},
					"svg8": {
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "svg9"},
						},
					},
					"svg9": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{
								Id:         "sv91",
								SearchName: "Name 91",
							},
						},
					},
				},
			},
			[]string{"sv1", "sv21", "sv91"},
		},
	} {
		got := filterSVG(c.input, c.svs)
		if diff := cmp.Diff(got, c.want, protocmp.Transform()); diff != "" {
			t.Errorf("filterSVG() got diff %v", diff)
		}
	}
}

func TestComputeSpecializedEntity(t *testing.T) {
	for _, c := range []struct {
		parent string
		child  string
		want   string
	}{
		{
			"dc/g/Employment",
			"dc/g/Person_Employment",
			"Employment",
		},
		{
			"dc/g/Person_Age_Gender",
			"dc/g/Variables_Person_Age_Gender",
			"Variables",
		},
		{
			"dc/g/Person_Age",
			"dc/g/Person_Age_Gender",
			"Gender",
		},
	} {
		got := computeSpecializedEntity(c.parent, c.child)
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("computeSpecializedEntity() got diff %v", diff)
		}
	}

}
