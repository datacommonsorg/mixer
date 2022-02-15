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
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestSearchTokens(t *testing.T) {
	node1 := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        map[string]string{"group_1": "token2", "group_31": "token5"},
		SvIds:         map[string]string{"sv_1_2": "ab1"},
	}
	node3 := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        nil,
		SvIds:         map[string]string{"sv_1_1": "ac3", "sv_1_2": "ac3"},
	}
	nodeX := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        map[string]string{"group_3": "zdx"},
		SvIds:         map[string]string{"sv_1_2": "token2", "sv_3": "token4"},
	}
	nodeDX := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'x': &nodeX,
		},
		SvgIds: nil,
		SvIds:  nil,
	}
	nodeC := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'3': &node3,
		},
		SvgIds: nil,
		SvIds:  nil,
	}
	nodeZ := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'd': &nodeDX,
		},
		SvgIds: nil,
		SvIds:  nil,
	}
	nodeB := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'1': &node1,
		},
		SvgIds: nil,
		SvIds:  nil,
	}
	nodeA := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'b': &nodeB,
			'c': &nodeC,
		},
		SvgIds: nil,
		SvIds:  nil,
	}
	for _, c := range []struct {
		tokens      []string
		index       *resource.SearchIndex
		wantSv      []*pb.EntityInfo
		wantSvg     []*pb.EntityInfo
		wantMatches []string
	}{
		{
			tokens: []string{"ab1"},
			index: &resource.SearchIndex{
				RootTrieNode: &resource.TrieNode{
					ChildrenNodes: map[rune]*resource.TrieNode{
						'a': &nodeA,
						'z': &nodeZ,
					},
					SvgIds: nil,
					SvIds:  nil,
				},
				Ranking: map[string]*resource.RankingInfo{
					"group_1": {
						ApproxNumPv: 2,
						RankingName: "token1 token2",
					},
					"sv_1_2": {
						ApproxNumPv: 3,
						RankingName: "ab1 ac3 token2",
					},
					"group_31": {
						ApproxNumPv: 2,
						RankingName: "token1 token5 token6",
					},
				},
			},
			wantSv: []*pb.EntityInfo{
				{
					Dcid: "sv_1_2",
					Name: "ab1 ac3 token2",
				},
			},
			wantSvg: []*pb.EntityInfo{
				{
					Dcid: "group_1",
					Name: "token1 token2",
				},
				{
					Dcid: "group_31",
					Name: "token1 token5 token6",
				},
			},
			wantMatches: []string{"ab1", "token2", "token5"},
		},
		{
			tokens: []string{"ab", "zd", "ac3"},
			index: &resource.SearchIndex{
				RootTrieNode: &resource.TrieNode{
					ChildrenNodes: map[rune]*resource.TrieNode{
						'a': &nodeA,
						'z': &nodeZ,
					},
					SvgIds: nil,
					SvIds:  nil,
				},
				Ranking: map[string]*resource.RankingInfo{
					"sv_1_1": {
						ApproxNumPv: 3,
						RankingName: "ac3 token2 token3",
					},
					"sv_1_2": {
						ApproxNumPv: 3,
						RankingName: "ab1 ac3 token2",
					},
					"sv_3": {
						ApproxNumPv: 20,
						RankingName: "token4",
					},
					"group_3": {
						ApproxNumPv: 2,
						RankingName: "zdx token2 token4",
					},
				},
			},
			wantSv: []*pb.EntityInfo{
				{
					Dcid: "sv_1_2",
					Name: "ab1 ac3 token2",
				},
			},
			wantSvg:     []*pb.EntityInfo{},
			wantMatches: []string{"ab1", "token2", "ac3"},
		},
	} {
		sv, svg, matches := searchTokens(c.tokens, c.index)
		if diff := cmp.Diff(sv, c.wantSv, protocmp.Transform()); diff != "" {
			t.Errorf("Stat var list got diff %v", diff)
		}
		if diff := cmp.Diff(svg, c.wantSvg, protocmp.Transform(), protocmp.SortRepeated(func(x, y *pb.EntityInfo) bool { return x.Dcid < y.Dcid })); diff != "" {
			t.Errorf("Stat var group list got diff %v", diff)
		}
		if diff := cmp.Diff(matches, c.wantMatches, protocmp.Transform(), protocmp.SortRepeated(func(x, y *pb.EntityInfo) bool { return x.Dcid < y.Dcid })); diff != "" {
			t.Errorf("Matches list got diff %v", diff)
		}
	}
}

func TestGroupStatVars(t *testing.T) {
	for _, c := range []struct {
		svList      []*pb.EntityInfo
		svgList     []*pb.EntityInfo
		parentMap   map[string][]string
		rankingInfo map[string]*resource.RankingInfo
		wantSv      []*pb.EntityInfo
		wantSvg     []*pb.SearchResultSVG
	}{
		{
			svList: []*pb.EntityInfo{
				{
					Dcid: "sv1",
					Name: "sv1",
				},
				{
					Dcid: "sv2",
					Name: "sv2",
				},
				{
					Dcid: "sv3",
					Name: "sv3",
				},
			},
			svgList: []*pb.EntityInfo{
				{
					Dcid: "svg1",
					Name: "svg1",
				},
				{
					Dcid: "svg2",
					Name: "svg2",
				},
			},
			parentMap: map[string][]string{
				"sv1": {"svg4", "svg8"},
				"sv2": {"svg8", "svg1"},
				"sv3": {"svg2", "svg1"},
			},
			rankingInfo: map[string]*resource.RankingInfo{
				"svg1": {
					ApproxNumPv: 1,
					RankingName: "svg1",
				},
				"svg2": {
					ApproxNumPv: 3,
					RankingName: "svg2",
				},
				"svg4": {
					ApproxNumPv: 1,
					RankingName: "svg4",
				},
				"svg8": {
					ApproxNumPv: 2,
					RankingName: "svg8",
				},
			},
			wantSv: []*pb.EntityInfo{
				{
					Dcid: "sv1",
					Name: "sv1",
				},
			},
			wantSvg: []*pb.SearchResultSVG{
				{
					Dcid: "svg1",
					Name: "svg1",
					StatVars: []*pb.EntityInfo{
						{
							Dcid: "sv2",
							Name: "sv2",
						},
						{
							Dcid: "sv3",
							Name: "sv3",
						},
					},
				},
				{
					Dcid: "svg2",
					Name: "svg2",
				},
			},
		},
	} {
		sv, svg := groupStatVars(c.svList, c.svgList, c.parentMap, c.rankingInfo)
		if diff := cmp.Diff(sv, c.wantSv, protocmp.Transform()); diff != "" {
			t.Errorf("Stat var list got diff %v", diff)
		}
		if diff := cmp.Diff(svg, c.wantSvg, protocmp.Transform()); diff != "" {
			t.Errorf("Stat var group list got diff %v", diff)
		}
	}
}

func TestCompareRankingInfo(t *testing.T) {
	for _, c := range []struct {
		r1    *resource.RankingInfo
		dcid1 string
		r2    *resource.RankingInfo
		dcid2 string
		want  bool
	}{
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				RankingName: "stat var 1",
			},
			dcid1: "sv1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 2,
				RankingName: "stat var 2",
			},
			dcid2: "sv2",
			want:  true,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 2,
				RankingName: "stat var 1",
			},
			dcid1: "sv1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				RankingName: "stat var 2",
			},
			dcid2: "sv2",
			want:  false,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				RankingName: "stat var 1",
			},
			dcid1: "sv1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				RankingName: "stat var 2",
			},
			dcid2: "sv2",
			want:  true,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				RankingName: "stat var 1",
			},
			dcid1: "sv1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				RankingName: "stat",
			},
			dcid2: "sv2",
			want:  false,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				RankingName: "sv",
			},
			dcid1: "sv1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				RankingName: "sv",
			},
			dcid2: "sv2",
			want:  true,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				RankingName: "sv",
			},
			dcid1: "statvar1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				RankingName: "sv",
			},
			dcid2: "s2",
			want:  false,
		},
	} {
		result := compareRankingInfo(c.r1, c.dcid1, c.r2, c.dcid2)
		if diff := cmp.Diff(result, c.want); diff != "" {
			t.Errorf("ranking comparison got diff %v", diff)
		}
	}
}
