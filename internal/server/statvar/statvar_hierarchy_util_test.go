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
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/go-test/deep"
	"github.com/google/go-cmp/cmp"
)

func TestGetParentMapping(t *testing.T) {
	for _, c := range []struct {
		input map[string]*pb.StatVarGroupNode
		want  map[string][]string
	}{
		{
			map[string]*pb.StatVarGroupNode{
				"dc/g/Root": {
					ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
						{Id: "svgX"},
						{Id: "svgY"},
					},
				},
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
				"svgX": {"dc/g/Root"},
				"svgY": {"dc/g/Root", "svgX"},
				"svgZ": {"svgX", "svgY"},
				"sv1":  {"svgY", "svgZ"},
				"sv2":  {"svgZ"},
			},
		},
	} {
		got := BuildParentSvgMap(c.input)
		if diff := cmp.Diff(got, c.want); diff != "" {
			t.Errorf("GetParentSvgMap got diff %v", diff)
		}
	}
}

func TestBuildSearchIndex(t *testing.T) {
	token1 := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        map[string]struct{}{"group_1": {}},
		SvIds:         map[string]struct{}{"sv_1_1": {}},
		Matches:       map[string]struct{}{"ab1": {}},
	}
	token3 := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        nil,
		SvIds:         map[string]struct{}{"sv_1_1": {}, "sv_1_2": {}},
		Matches:       map[string]struct{}{"ac3": {}},
	}
	tokenX := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        map[string]struct{}{"group_1": {}, "group_3_1": {}},
		SvIds:         map[string]struct{}{"sv_3": {}},
		Matches:       map[string]struct{}{"zdx": {}},
	}
	tokenDX := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'x': &tokenX,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	tokenD := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        map[string]struct{}{"group_3_1": {}},
		SvIds:         map[string]struct{}{"sv_1_2": {}, "sv3": {}},
		Matches:       map[string]struct{}{"bd": {}},
	}
	tokenC := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'3': &token3,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	tokenZ := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'd': &tokenDX,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	tokenB1 := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'd': &tokenD,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	tokenB2 := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'1': &token1,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	tokenA := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'b': &tokenB2,
			'c': &tokenC,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	for _, c := range []struct {
		inputSvg  map[string]*pb.StatVarGroupNode
		parentSvg map[string][]string
		want      *resource.SearchIndex
	}{
		{
			map[string]*pb.StatVarGroupNode{
				"group_1": {
					AbsoluteName: "ab1 zDx",
					ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
						{Id: "group_3_1"},
					},
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_1_1",
							SearchName:  "ab1 Ac3",
							DisplayName: "sv1",
						},
						{
							Id:          "sv_1_2",
							SearchName:  "ac3, bd",
							DisplayName: "sv2",
						},
					},
				},
				"group_3_1": {
					AbsoluteName: "zdx, bd",
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_3",
							SearchName:  "zdx",
							DisplayName: "sv3",
						},
						{
							Id:          "sv3",
							SearchName:  "bd,",
							DisplayName: "sv4",
						},
					},
				},
				"group_orphan": {
					AbsoluteName: "orphan group",
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_orphan",
							SearchName:  "zdx",
							DisplayName: "sv3",
						},
					},
				},
			},
			map[string][]string{
				"group_1":   {"dc/g/root"},
				"sv_1_1":    {"group_1"},
				"sv_1_2":    {"group_1"},
				"group_3_1": {"group_1"},
				"sv_3":      {"group_3_1"},
				"sv3":       {"group_3_1"},
			},
			&resource.SearchIndex{
				RootTrieNode: &resource.TrieNode{
					ChildrenNodes: map[rune]*resource.TrieNode{
						'a': &tokenA,
						'z': &tokenZ,
						'b': &tokenB1,
					},
					SvgIds:  nil,
					SvIds:   nil,
					Matches: nil,
				},
				Ranking: map[string]*resource.RankingInfo{
					"group_1": {
						ApproxNumPv: 2,
						RankingName: "ab1 zDx",
					},
					"sv_1_1": {
						ApproxNumPv: 3,
						RankingName: "sv1",
					},
					"group_3_1": {
						ApproxNumPv: 3,
						RankingName: "zdx, bd",
					},
					"sv_3": {
						ApproxNumPv: 2,
						RankingName: "sv3",
					},
					"sv_1_2": {
						ApproxNumPv: 3,
						RankingName: "sv2",
					},
					"sv3": {
						ApproxNumPv: 30,
						RankingName: "sv4",
					},
				},
			},
		},
	} {
		got := BuildStatVarSearchIndex(c.inputSvg, c.parentSvg, false)
		if diff := deep.Equal(got, c.want); diff != nil {
			t.Errorf("GetStatVarSearchIndex got diff %v", diff)
		}
	}
}
