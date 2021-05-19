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

package server

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
)

func TestGetParentMapping(t *testing.T) {
	for _, c := range []struct {
		input map[string]*pb.StatVarGroupNode
		want  map[string][]string
	}{
		{
			map[string]*pb.StatVarGroupNode{
				"svgX": {
					ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
						{Id: "svgY"},
						{Id: "svgZ"},
					},
				},
				"svgY": {
					ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
						{Id: "svgZ"},
					},
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:         "sv1",
							SearchName: "Name 1",
						},
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
			map[string][]string{
				"svgY": []string{"svgX"},
				"svgZ": []string{"svgX", "svgY"},
				"sv1":  []string{"svgY", "svgZ"},
				"sv2":  []string{"svgZ"},
			},
		},
	} {
		got := GetParentSvgMap(c.input)
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("GetParentSvgMap got diff %v", diff)
		}
	}
}

func TestGetSearchIndex(t *testing.T) {
	for _, c := range []struct {
		input map[string]*pb.StatVarGroupNode
		want  map[string]map[string]RankingInfo
	}{
		{
			map[string]*pb.StatVarGroupNode{
				"group_1": {
					AbsoluteName: "token1 token2",
					ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
						{Id: "group_3_1"},
					},
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_1_1",
							SearchName:  "token1 token3",
							DisplayName: "sv1",
						},
						{
							Id:          "sv_1_2",
							SearchName:  "token3, token4",
							DisplayName: "sv2",
						},
					},
				},
				"group_3_1": {
					AbsoluteName: "token2, token4",
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_3",
							SearchName:  "token2",
							DisplayName: "sv3",
						},
						{
							Id:          "sv3",
							SearchName:  "token4,",
							DisplayName: "sv4",
						},
					},
				},
			},
			map[string]map[string]RankingInfo{
				"token1": {
					"group_1": {
						ApproxNumPv: 2,
						RankingName: "token1 token2",
					},
					"sv_1_1": {
						ApproxNumPv: 3,
						RankingName: "token1 token3",
					},
				},
				"token2": {
					"group_1": {
						ApproxNumPv: 2,
						RankingName: "token1 token2",
					},
					"group_3_1": {
						ApproxNumPv: 3,
						RankingName: "token2, token4",
					},
					"sv_3": {
						ApproxNumPv: 2,
						RankingName: "token2",
					},
				},
				"token3": {
					"sv_1_1": {
						ApproxNumPv: 3,
						RankingName: "token1 token3",
					},
					"sv_1_2": {
						ApproxNumPv: 3,
						RankingName: "token3, token4",
					},
				},
				"token4": {
					"sv_1_2": {
						ApproxNumPv: 3,
						RankingName: "token3, token4",
					},
					"group_3_1": {
						ApproxNumPv: 3,
						RankingName: "token2, token4",
					},
					"sv3": {
						ApproxNumPv: 30,
						RankingName: "token4,",
					},
				},
			},
		},
	} {
		got := GetSearchIndex(c.input)
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("GetSearchIndex got diff %v", diff)
		}
	}
}
