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
		SvgIds:        map[string]struct{}{"group_1": {}, "group_31": {}},
		SvIds:         map[string]struct{}{"sv_1_2": {}},
	}
	node3 := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        nil,
		SvIds:         map[string]struct{}{"sv_1_1": {}, "sv_1_2": {}},
	}
	nodeX := resource.TrieNode{
		ChildrenNodes: nil,
		SvgIds:        map[string]struct{}{"group_3": {}},
		SvIds:         map[string]struct{}{"sv_1_2": {}, "sv_3": {}},
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
		tokens  []string
		index   *resource.SearchIndex
		wantSv  []*pb.EntityInfo
		wantSvg []*pb.EntityInfo
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
						RankingName: "token1 token3 token4",
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
					Name: "token1 token3 token4",
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
						RankingName: "token2 token3",
					},
					"sv_1_2": {
						ApproxNumPv: 3,
						RankingName: "token2 token3 token4",
					},
					"sv_3": {
						ApproxNumPv: 20,
						RankingName: "token4",
					},
					"group_3": {
						ApproxNumPv: 2,
						RankingName: "token2 token4 token6",
					},
				},
			},
			wantSv: []*pb.EntityInfo{
				{
					Dcid: "sv_1_2",
					Name: "token2 token3 token4",
				},
			},
			wantSvg: []*pb.EntityInfo{},
		},
	} {
		sv, svg := searchTokens(c.tokens, c.index)
		if diff := cmp.Diff(sv, c.wantSv, protocmp.Transform()); diff != "" {
			t.Errorf("Stat var list got diff %v", diff)
		}
		if diff := cmp.Diff(svg, c.wantSvg, protocmp.Transform()); diff != "" {
			t.Errorf("Stat var group list got diff %v", diff)
		}
	}
}
