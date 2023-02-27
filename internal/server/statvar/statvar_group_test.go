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

func TestMergeCustomSVG(t *testing.T) {
	for _, c := range []struct {
		input []*pb.StatVarGroups
		want  *pb.StatVarGroups
	}{
		{
			// input
			[]*pb.StatVarGroups{
				// Sample svg groups from cache 1
				{
					StatVarGroups: map[string]*pb.StatVarGroupNode{
						"dc/g/Custom_Root": {
							ChildStatVars: []*pb.StatVarGroupNode_ChildSV{},
							ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
								{Id: "dc/g/Custom_Environment", DescendentStatVarCount: 0},
								{Id: "dc/g/Custom_Energy", DescendentStatVarCount: 2},
							},
							DescendentStatVarCount: 2,
						},
						"dc/g/Custom_Environment": {
							ChildStatVars:          []*pb.StatVarGroupNode_ChildSV{},
							ChildStatVarGroups:     []*pb.StatVarGroupNode_ChildSVG{},
							DescendentStatVarCount: 0,
						},
						"dc/g/Custom_Energy": {
							ChildStatVars: []*pb.StatVarGroupNode_ChildSV{},
							ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
								{Id: "dc/g/Custom_DeepSolar", DescendentStatVarCount: 2},
							},
							DescendentStatVarCount: 2,
						},
						"dc/g/Custom_DeepSolar": {
							ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
								{Id: "Count_SolarInstallation_Commercial"},
								{Id: "Mean_CoverageArea_SolarInstallation_Commercial"},
							},
							ChildStatVarGroups:     []*pb.StatVarGroupNode_ChildSVG{},
							DescendentStatVarCount: 2,
						},
					},
				},
				// Sample svg groups from cache 2
				{
					StatVarGroups: map[string]*pb.StatVarGroupNode{
						"dc/g/Custom_Root": {
							ChildStatVars: []*pb.StatVarGroupNode_ChildSV{},
							ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
								{Id: "dc/g/Custom_Environment", DescendentStatVarCount: 3},
								{Id: "dc/g/Custom_Energy", DescendentStatVarCount: 0},
							},
							DescendentStatVarCount: 3,
						},
						"dc/g/Custom_Environment": {
							ChildStatVars: []*pb.StatVarGroupNode_ChildSV{},
							ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
								{Id: "dc/g/Custom_Atmosphere", DescendentStatVarCount: 3},
							},
							DescendentStatVarCount: 3,
						},
						"dc/g/Custom_Energy": {
							ChildStatVars:          []*pb.StatVarGroupNode_ChildSV{},
							ChildStatVarGroups:     []*pb.StatVarGroupNode_ChildSVG{},
							DescendentStatVarCount: 0,
						},
						"dc/g/Custom_Atmosphere": {
							ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
								{Id: "Max_Concentration_AirPollutant_Ozone"},
								{Id: "Median_Concentration_AirPollutant_Ozone"},
								{Id: "Min_Concentration_AirPollutant_Ozone"},
							},
							ChildStatVarGroups:     []*pb.StatVarGroupNode_ChildSVG{},
							DescendentStatVarCount: 3,
						},
					},
				},
			},
			// want
			&pb.StatVarGroups{
				// merged svg groups
				StatVarGroups: map[string]*pb.StatVarGroupNode{
					"dc/g/Custom_Root": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{},
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "dc/g/Custom_Environment", DescendentStatVarCount: 3},
							{Id: "dc/g/Custom_Energy", DescendentStatVarCount: 2},
						},
						DescendentStatVarCount: 5,
					},
					"dc/g/Custom_Environment": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{},
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "dc/g/Custom_Atmosphere", DescendentStatVarCount: 3},
						},
						DescendentStatVarCount: 3,
					},
					"dc/g/Custom_Energy": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{},
						ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
							{Id: "dc/g/Custom_DeepSolar", DescendentStatVarCount: 2},
						},
						DescendentStatVarCount: 2,
					},
					"dc/g/Custom_DeepSolar": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{Id: "Count_SolarInstallation_Commercial"},
							{Id: "Mean_CoverageArea_SolarInstallation_Commercial"},
						},
						ChildStatVarGroups:     []*pb.StatVarGroupNode_ChildSVG{},
						DescendentStatVarCount: 2,
					},
					"dc/g/Custom_Atmosphere": {
						ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
							{Id: "Max_Concentration_AirPollutant_Ozone"},
							{Id: "Median_Concentration_AirPollutant_Ozone"},
							{Id: "Min_Concentration_AirPollutant_Ozone"},
						},
						ChildStatVarGroups:     []*pb.StatVarGroupNode_ChildSVG{},
						DescendentStatVarCount: 3,
					},
				},
			},
		},
	} {
		// Mimick the logic in GetStatVarGroup.
		got := &pb.StatVarGroups{StatVarGroups: map[string]*pb.StatVarGroupNode{}}
		for _, importGroupSVGs := range c.input {
			for k, v := range importGroupSVGs.GetStatVarGroups() {
				if _, ok := got.StatVarGroups[k]; !ok {
					got.StatVarGroups[k] = v
				} else {
					mergeSVGNodes(got.StatVarGroups[k], v)
				}
			}
		}
		adjustDescendentStatVarCount(got.StatVarGroups, customSvgRoot)

		for k, svgWant := range c.want.GetStatVarGroups() {
			svgGot, ok := got.StatVarGroups[k]
			if !ok {
				t.Errorf("MergeCustomSVG result missing svg %s", k)
			}
			if diff := cmp.Diff(svgGot, svgWant, protocmp.Transform()); diff != "" {
				t.Errorf("MergeCustomSVG result[%s] got diff %v", k, diff)
			}
		}
	}
}
