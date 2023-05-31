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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
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
							Id:          "sv1",
							SearchNames: []string{"Name 1", "Name 1"},
						},
					},
				},
				"svgZ": {
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv1",
							SearchNames: []string{"Name 1"},
						},
						{
							Id:          "sv2",
							SearchNames: []string{"Name 2"},
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

func TestRemoveSvg(t *testing.T) {
	raw := map[string]*pb.StatVarGroupNode{
		"dc/g/Root": {
			ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
				{Id: "svgX"},
				{Id: "svgY"},
				{Id: "svgW"},
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
					Id:          "sv1",
					SearchNames: []string{"Name 1", "Name 1"},
				},
			},
		},
		"svgZ": {
			ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
				{
					Id:          "sv1",
					SearchNames: []string{"Name 1"},
				},
				{
					Id:          "sv2",
					SearchNames: []string{"Name 2"},
				},
			},
		},
		"svgW": {
			ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
				{Id: "svgW1"},
				{Id: "svgW2"},
			},
		},
	}
	parent := BuildParentSvgMap(raw)
	for _, c := range []struct {
		svg  string
		want map[string]*pb.StatVarGroupNode
	}{
		{
			"svgW",
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
							Id:          "sv1",
							SearchNames: []string{"Name 1", "Name 1"},
						},
					},
				},
				"svgZ": {
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv1",
							SearchNames: []string{"Name 1"},
						},
						{
							Id:          "sv2",
							SearchNames: []string{"Name 2"},
						},
					},
				},
			},
		},
		{
			"svgY",
			map[string]*pb.StatVarGroupNode{
				"dc/g/Root": {
					ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
						{Id: "svgX"},
						{Id: "svgW"},
					},
				},
				"svgX": {},
				"svgW": {
					ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
						{Id: "svgW1"},
						{Id: "svgW2"},
					},
				},
			},
		},
	} {
		input := map[string]*pb.StatVarGroupNode{}
		for svg, node := range raw {
			input[svg] = proto.Clone(node).(*pb.StatVarGroupNode)
		}
		RemoveSvg(input, parent, c.svg)
		if diff := cmp.Diff(input, c.want, protocmp.Transform()); diff != "" {
			t.Errorf("RemoveSvg got diff %v", diff)
		}
	}
}

func TestBuildSearchIndex(t *testing.T) {
	token1 := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        map[string]struct{}{"g_1": {}},
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
		SvgIds:        map[string]struct{}{"g_1": {}, "g_3_1": {}},
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
		SvgIds:        map[string]struct{}{"g_3_1": {}},
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
	token1a := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        map[string]struct{}{"g_1": {}},
		SvIds:         nil,
		Matches:       map[string]struct{}{"": {}},
	}
	token1b := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        map[string]struct{}{"g_3_1": {}},
		SvIds:         nil,
		Matches:       map[string]struct{}{"": {}},
	}
	tokenUnderscore := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'1': &token1b,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	token3a := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'_': &tokenUnderscore,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	tokenUnderscore1 := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'1': &token1a,
			'3': &token3a,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	tokenG := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'_': &tokenUnderscore1,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	token1c := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        nil,
		SvIds:         map[string]struct{}{"sv_1_1": {}},
		Matches:       map[string]struct{}{"": {}},
	}
	token2a := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        nil,
		SvIds:         map[string]struct{}{"sv_1_2": {}},
		Matches:       map[string]struct{}{"": {}},
	}
	tokenUnderscore2 := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'1': &token1c,
			'2': &token2a,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	token1d := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'_': &tokenUnderscore2,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	token3b := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        nil,
		SvIds:         map[string]struct{}{"sv_3": {}},
		Matches:       map[string]struct{}{"": {}},
	}
	tokenUnderscore3 := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'1': &token1d,
			'3': &token3b,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	token3c := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        nil,
		SvIds:         map[string]struct{}{"sv3": {}},
		Matches:       map[string]struct{}{"": {}},
	}
	tokenV := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'_': &tokenUnderscore3,
			'3': &token3c,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}
	tokenS := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'v': &tokenV,
		},
		SvgIds:  nil,
		SvIds:   nil,
		Matches: nil,
	}

	for _, c := range []struct {
		inputSvg   map[string]*pb.StatVarGroupNode
		parentSvg  map[string][]string
		ignoredSvg []string
		want       *resource.SearchIndex
	}{
		{
			map[string]*pb.StatVarGroupNode{
				"g_1": {
					AbsoluteName: "ab1 zDx",
					ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
						{Id: "g_3_1"},
					},
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_1_1",
							SearchNames: []string{"ab1 Ac3"},
							DisplayName: "sv1",
						},
						{
							Id:          "sv_1_2",
							SearchNames: []string{"ac3, bd"},
							DisplayName: "sv2",
						},
					},
				},
				"g_3_1": {
					AbsoluteName: "zdx, bd",
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_3",
							SearchNames: []string{"zdx"},
							DisplayName: "sv3",
						},
						{
							Id:          "sv3",
							SearchNames: []string{"bd,"},
							DisplayName: "sv4",
						},
					},
				},
				"group_orphan": {
					AbsoluteName: "orphan group",
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_orphan",
							SearchNames: []string{"zdx"},
							DisplayName: "sv3",
						},
					},
				},
			},
			map[string][]string{
				"g_1":    {"dc/g/root"},
				"sv_1_1": {"g_1"},
				"sv_1_2": {"g_1"},
				"g_3_1":  {"g_1"},
				"sv_3":   {"g_3_1"},
				"sv3":    {"g_3_1"},
			},
			[]string{},
			&resource.SearchIndex{
				RootTrieNode: &resource.TrieNode{
					ChildrenNodes: map[rune]*resource.TrieNode{
						'a': &tokenA,
						'z': &tokenZ,
						'b': &tokenB1,
						'g': &tokenG,
						's': &tokenS,
					},
					SvgIds:  nil,
					SvIds:   nil,
					Matches: nil,
				},
				Ranking: map[string]*resource.RankingInfo{
					"g_1": {
						ApproxNumPv: 2,
						NumKnownPv:  2,
						RankingName: "ab1 zDx",
					},
					"sv_1_1": {
						ApproxNumPv: 3,
						NumKnownPv:  3,
						RankingName: "sv1",
					},
					"g_3_1": {
						ApproxNumPv: 3,
						NumKnownPv:  3,
						RankingName: "zdx, bd",
					},
					"sv_3": {
						ApproxNumPv: 2,
						NumKnownPv:  2,
						RankingName: "sv3",
					},
					"sv_1_2": {
						ApproxNumPv: 3,
						NumKnownPv:  3,
						RankingName: "sv2",
					},
					"sv3": {
						ApproxNumPv: 30,
						NumKnownPv:  30,
						RankingName: "sv4",
					},
				},
			},
		},
		{
			map[string]*pb.StatVarGroupNode{
				"g_1": {
					AbsoluteName: "ab1 zDx",
					ChildStatVarGroups: []*pb.StatVarGroupNode_ChildSVG{
						{Id: "g_3_1"},
						{Id: "svg_ignored_1"},
						{Id: "svg_ignored_2"},
					},
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_1_1",
							SearchNames: []string{"ab1 Ac3"},
							DisplayName: "sv1",
						},
						{
							Id:          "sv_1_2",
							SearchNames: []string{"ac3, bd"},
							DisplayName: "sv2",
						},
					},
				},
				"g_3_1": {
					AbsoluteName: "zdx, bd",
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_3",
							SearchNames: []string{"zdx"},
							DisplayName: "sv3",
						},
						{
							Id:          "sv3",
							SearchNames: []string{"bd,"},
							DisplayName: "sv4",
						},
					},
				},
				"svg_ignored_1": {
					AbsoluteName: "test",
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_ignored_1",
							SearchNames: []string{"zdx"},
							DisplayName: "svIgnored1",
						},
					},
				},
				"svg_ignored_2": {
					AbsoluteName: "test",
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_ignored_2",
							SearchNames: []string{"zdx"},
							DisplayName: "svIgnored2",
						},
					},
				},
				"group_orphan": {
					AbsoluteName: "orphan group",
					ChildStatVars: []*pb.StatVarGroupNode_ChildSV{
						{
							Id:          "sv_orphan",
							SearchNames: []string{"zdx"},
							DisplayName: "sv3",
						},
					},
				},
			},
			map[string][]string{
				"g_1":           {"dc/g/root"},
				"sv_1_1":        {"g_1"},
				"sv_1_2":        {"g_1"},
				"g_3_1":         {"g_1"},
				"sv_3":          {"g_3_1"},
				"sv3":           {"g_3_1"},
				"svg_ignored_1": {"g_1"},
				"svg_ignored_2": {"g_1"},
				"sv_ignored_1":  {"svg_ignored_1"},
				"sv_ignored_2":  {"svg_ignored_2"},
			},
			[]string{"svg_ignored_1", "svg_ignored_2"},
			&resource.SearchIndex{
				RootTrieNode: &resource.TrieNode{
					ChildrenNodes: map[rune]*resource.TrieNode{
						'a': &tokenA,
						'z': &tokenZ,
						'b': &tokenB1,
						'g': &tokenG,
						's': &tokenS,
					},
					SvgIds:  nil,
					SvIds:   nil,
					Matches: nil,
				},
				Ranking: map[string]*resource.RankingInfo{
					"g_1": {
						ApproxNumPv: 2,
						NumKnownPv:  2,
						RankingName: "ab1 zDx",
					},
					"sv_1_1": {
						ApproxNumPv: 3,
						NumKnownPv:  3,
						RankingName: "sv1",
					},
					"g_3_1": {
						ApproxNumPv: 3,
						NumKnownPv:  3,
						RankingName: "zdx, bd",
					},
					"sv_3": {
						ApproxNumPv: 2,
						NumKnownPv:  2,
						RankingName: "sv3",
					},
					"sv_1_2": {
						ApproxNumPv: 3,
						NumKnownPv:  3,
						RankingName: "sv2",
					},
					"sv3": {
						ApproxNumPv: 30,
						NumKnownPv:  30,
						RankingName: "sv4",
					},
				},
			},
		},
	} {
		got := BuildStatVarSearchIndex(c.inputSvg, c.parentSvg, c.ignoredSvg)
		if diff := deep.Equal(got, c.want); diff != nil {
			t.Errorf("GetStatVarSearchIndex got diff %v", diff)
		}
	}
}
