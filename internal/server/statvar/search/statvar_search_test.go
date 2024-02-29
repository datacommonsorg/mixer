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

package search

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
		SvIds:         map[string]struct{}{"sv_1_2": {}},
		Matches:       map[string]struct{}{"token2": {}, "token5": {}, "ab1": {}},
	}
	node3 := resource.TrieNode{
		ChildrenNodes: nil,
		SvIds:         map[string]struct{}{"sv_1_1": {}, "sv_1_2": {}},
		Matches:       map[string]struct{}{"ac3": {}},
	}
	nodeX := resource.TrieNode{
		ChildrenNodes: nil,
		SvIds:         map[string]struct{}{"sv_1_2": {}, "sv_3": {}},
		Matches:       map[string]struct{}{"zdx": {}, "token2": {}, "token4": {}},
	}
	nodeDX := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'x': &nodeX,
		},
		SvIds:   nil,
		Matches: nil,
	}
	nodeC := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'3': &node3,
		},
		SvIds:   nil,
		Matches: nil,
	}
	nodeZ := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'd': &nodeDX,
		},
		SvIds:   nil,
		Matches: nil,
	}
	nodeB := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'1': &node1,
		},
		SvIds:   nil,
		Matches: nil,
	}
	nodeA := resource.TrieNode{
		ChildrenNodes: map[rune]*resource.TrieNode{
			'b': &nodeB,
			'c': &nodeC,
		},
		SvIds:   nil,
		Matches: nil,
	}
	for _, c := range []struct {
		tokens      []string
		index       *resource.SearchIndex
		svOnly      bool
		wantSv      []*pb.EntityInfo
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
					SvIds: nil,
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
			svOnly: false,
			wantSv: []*pb.EntityInfo{
				{
					Dcid: "sv_1_2",
					Name: "ab1 ac3 token2",
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
					SvIds: nil,
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
			svOnly: false,
			wantSv: []*pb.EntityInfo{
				{
					Dcid: "sv_1_2",
					Name: "ab1 ac3 token2",
				},
			},
			wantMatches: []string{"ab1", "ac3", "token2", "token4", "token5", "zdx"},
		},
		{
			tokens: []string{"ab1"},
			index: &resource.SearchIndex{
				RootTrieNode: &resource.TrieNode{
					ChildrenNodes: map[rune]*resource.TrieNode{
						'a': &nodeA,
						'z': &nodeZ,
					},
					SvIds: nil,
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
			svOnly: true,
			wantSv: []*pb.EntityInfo{
				{
					Dcid: "sv_1_2",
					Name: "ab1 ac3 token2",
				},
			},
			wantMatches: []string{"ab1", "token2", "token5"},
		},
	} {
		sv, matches := searchTokens(c.tokens, c.index, c.svOnly)
		if diff := cmp.Diff(sv, c.wantSv, protocmp.Transform()); diff != "" {
			t.Errorf("Stat var list got diff %v", diff)
		}
		if diff := cmp.Diff(matches, c.wantMatches, protocmp.Transform()); diff != "" {
			t.Errorf("Matches list got diff %v", diff)
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
				NumKnownPv:  2,
				RankingName: "stat var 1",
			},
			dcid1: "sv1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 2,
				NumKnownPv:  2,
				RankingName: "stat var 2",
			},
			dcid2: "sv2",
			want:  true,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 2,
				NumKnownPv:  2,
				RankingName: "stat var 1",
			},
			dcid1: "sv1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "stat var 2",
			},
			dcid2: "sv2",
			want:  false,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "stat var 1",
			},
			dcid1: "sv1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "stat var 2",
			},
			dcid2: "sv2",
			want:  true,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "stat var 1",
			},
			dcid1: "sv1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "stat",
			},
			dcid2: "sv2",
			want:  false,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "sv",
			},
			dcid1: "sv1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "sv",
			},
			dcid2: "sv2",
			want:  true,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "sv",
			},
			dcid1: "statvar1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "sv",
			},
			dcid2: "s2",
			want:  false,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "sv",
			},
			dcid1: "statvar1",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  3,
				RankingName: "sv",
			},
			dcid2: "s2",
			want:  true,
		},
		{
			r1: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  3,
				RankingName: "sv",
			},
			dcid1: "s2",
			r2: &resource.RankingInfo{
				ApproxNumPv: 1,
				NumKnownPv:  2,
				RankingName: "sv",
			},
			dcid2: "statvar",
			want:  false,
		},
	} {
		result := compareRankingInfo(c.r1, c.dcid1, c.r2, c.dcid2)
		if diff := cmp.Diff(result, c.want); diff != "" {
			t.Errorf("ranking comparison got diff %v", diff)
		}
	}
}
