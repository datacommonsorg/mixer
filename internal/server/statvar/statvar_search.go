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
	"context"
	"sort"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
)

const (
	maxFilteredIds = 3000 // Twice of maxResult to give buffer for place filter.
	maxResult      = 1000
)

// SearchStatVar implements API for Mixer.SearchStatVar.
func SearchStatVar(
	ctx context.Context,
	in *pb.SearchStatVarRequest,
	btGroup *bigtable.Group,
	cache *resource.Cache,
) (
	*pb.SearchStatVarResponse, error,
) {
	query := in.GetQuery()
	places := in.GetPlaces()
	enableBlocklist := in.GetEnableBlocklist()

	result := &pb.SearchStatVarResponse{
		StatVars:      []*pb.EntityInfo{},
		StatVarGroups: []*pb.EntityInfo{},
	}
	if query == "" {
		return result, nil
	}
	tokens := strings.Fields(
		strings.Replace(strings.ToLower(query), ",", " ", -1))
	searchIndex := cache.SvgSearchIndex
	if enableBlocklist {
		searchIndex = cache.BlocklistedSvgSearchIndex
	}
	svList, svgList := searchTokens(tokens, searchIndex)

	// Filter the stat var and stat var group by places.
	if len(places) > 0 {
		// Read from stat existence cache, which can take several seconds when
		// there are a lot of ids. So pre-prune the ids, as the result will be
		// filtered anyway.
		ids := []string{}
		if len(svList) > maxFilteredIds {
			svList = svList[0:maxFilteredIds]
		}
		if len(svgList) > maxFilteredIds {
			svgList = svgList[0:maxFilteredIds]
		}
		for _, item := range append(svList, svgList...) {
			ids = append(ids, item.Dcid)
		}

		statVarCount, err := Count(ctx, btGroup, ids, places)
		if err != nil {
			return nil, err
		}
		svList = filter(svList, statVarCount, len(places))
		svgList = filter(svgList, statVarCount, len(places))
	}
	// TODO(shifucun): return the total number of result for client to consume.
	if len(svList) > maxResult {
		svList = svList[0:maxResult]
	}
	if len(svgList) > maxResult {
		svgList = svgList[0:maxResult]
	}
	result.StatVars = svList
	result.StatVarGroups = svgList
	return result, nil
}

func filter(
	nodes []*pb.EntityInfo,
	countMap map[string]map[string]int32,
	numPlaces int) []*pb.EntityInfo {
	result := []*pb.EntityInfo{}
	for _, node := range nodes {
		if existence, ok := countMap[node.Dcid]; ok && len(existence) > 0 {
			result = append(result, node)
		}
	}
	return result
}

func searchTokens(
	tokens []string, index *resource.SearchIndex,
) ([]*pb.EntityInfo, []*pb.EntityInfo) {
	svCount := map[string]int{}
	svgCount := map[string]int{}

	// Get all matching sv and svg from the trie for each token
	for _, token := range tokens {
		currNode := index.RootTrieNode
		// Traverse the Trie following the order of the characters in the token
		// until we either reach the end of the token or we reach a node that
		// doesn't have the next character as a child node.
		// eg. Trie: Root - a
		//                 / \
		// 				       b     c
		//
		//			If token is "ab", currNode will go Root -> a -> b
		// 			If token is "bc", currNode will go Root -> nil
		//			If token is "abc", currNode will go Root -> a -> b -> nil
		for _, c := range token {
			if _, ok := currNode.ChildrenNodes[c]; !ok {
				currNode = nil
				break
			}
			currNode = currNode.ChildrenNodes[c]
		}
		// The token is not a prefix or word in the Trie.
		if currNode == nil {
			continue
		}
		// Traverse the entire subTrie rooted at the node corresponding to the
		// last character in the token and add all SvIds and SvgIds seen.
		nodesToCheck := []resource.TrieNode{*currNode}
		for len(nodesToCheck) > 0 {
			node := nodesToCheck[0]
			nodesToCheck = nodesToCheck[1:]
			for _, node := range node.ChildrenNodes {
				nodesToCheck = append(nodesToCheck, *node)
			}
			for sv := range node.SvIds {
				svCount[sv]++
			}
			for svg := range node.SvgIds {
				svgCount[svg]++
			}
		}
	}

	// Only select sv and svg that matches all the tokens
	svList := []*pb.EntityInfo{}
	for sv, c := range svCount {
		if c == len(tokens) {
			svList = append(svList, &pb.EntityInfo{
				Dcid: sv,
				Name: index.Ranking[sv].RankingName,
			})
		}
	}

	// Sort stat vars by number of PV; If two stat vars have same number of PV,
	// then order by the stat var (group) name.
	sort.SliceStable(svList, func(i, j int) bool {
		ranking := index.Ranking
		ri := ranking[svList[i].Dcid]
		rj := ranking[svList[j].Dcid]
		if ri.ApproxNumPv == rj.ApproxNumPv {
			if ri.RankingName == rj.RankingName {
				return svList[i].Dcid < svList[j].Dcid
			}
			return ri.RankingName < rj.RankingName
		}
		return ri.ApproxNumPv < rj.ApproxNumPv
	})

	svgList := []*pb.EntityInfo{}
	for svg, c := range svgCount {
		if c == len(tokens) {
			svgList = append(svgList, &pb.EntityInfo{
				Dcid: svg,
				Name: index.Ranking[svg].RankingName,
			})
		}
	}
	sort.SliceStable(svgList, func(i, j int) bool {
		ranking := index.Ranking
		ri := ranking[svgList[i].Dcid]
		rj := ranking[svgList[j].Dcid]
		if ri.ApproxNumPv == rj.ApproxNumPv {
			if ri.RankingName == rj.RankingName {
				return svgList[i].Dcid < svgList[j].Dcid
			}
			return ri.RankingName < rj.RankingName
		}
		return ri.ApproxNumPv < rj.ApproxNumPv
	})
	return svList, svgList
}
