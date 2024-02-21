// Copyright 2023 Google LLC
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
	"context"
	"sort"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/cache"
	"github.com/datacommonsorg/mixer/internal/server/count"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
)

const (
	maxFilteredIds = 3000 // Twice of maxResult to give buffer for place filter.
	maxResult      = 1000
)

// SearchStatVar implements API for Mixer.SearchStatVar.
func SearchStatVar(
	ctx context.Context,
	in *pb.SearchStatVarRequest,
	store *store.Store,
	cachedata *cache.Cache,
) (
	*pb.SearchStatVarResponse, error,
) {
	query := in.GetQuery()
	places := in.GetPlaces()
	svOnly := in.GetSvOnly()

	result := &pb.SearchStatVarResponse{
		StatVars: []*pb.EntityInfo{},
		Matches:  []string{},
	}
	if query == "" {
		return result, nil
	}
	tokens := strings.Fields(
		strings.Replace(strings.ToLower(query), ",", " ", -1))
	searchIndex := cachedata.SvgSearchIndex()
	svList, matches := searchTokens(tokens, searchIndex, svOnly)

	// Filter the stat var by places.
	if len(places) > 0 {
		// Read from stat existence cache, which can take several seconds when
		// there are a lot of ids. So pre-prune the ids, as the result will be
		// filtered anyway.
		ids := []string{}
		if len(svList) > maxFilteredIds {
			svList = svList[0:maxFilteredIds]
		}
		for _, item := range svList {
			ids = append(ids, item.Dcid)
		}

		statVarCount, err := count.Count(ctx, store, cachedata, ids, places)
		if err != nil {
			return nil, err
		}
		svList = filter(svList, statVarCount, len(places))
	}
	// TODO(shifucun): return the total number of result for client to consume.
	if len(svList) > maxResult {
		svList = svList[0:maxResult]
	}
	result.StatVars = svList
	result.Matches = matches
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

// Return whether r1 should be ranked ahead of r2
func compareRankingInfo(
	r1 *resource.RankingInfo,
	dcid1 string,
	r2 *resource.RankingInfo,
	dcid2 string,
) bool {
	if r1.ApproxNumPv != r2.ApproxNumPv {
		return r1.ApproxNumPv < r2.ApproxNumPv
	}
	if r1.NumKnownPv != r2.NumKnownPv {
		return r1.NumKnownPv < r2.NumKnownPv
	}
	if r1.RankingName != r2.RankingName {
		return r1.RankingName < r2.RankingName
	}
	return dcid1 < dcid2
}

// TokenToMatches is a map of token to a set of strings that match the token
type TokenToMatches map[string]map[string]struct{}

func searchTokens(
	tokens []string, index *resource.SearchIndex, svOnly bool,
) ([]*pb.EntityInfo, []string) {
	// svMatches and svgMatches are maps of sv/svg id to TokenToMatches of tokens
	// that match each sv/svg
	svMatches := map[string]TokenToMatches{}

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
		// last character in the token and add all SvIds seen.
		nodesToCheck := []resource.TrieNode{*currNode}
		for len(nodesToCheck) > 0 {
			node := nodesToCheck[0]
			nodesToCheck = nodesToCheck[1:]
			for _, node := range node.ChildrenNodes {
				nodesToCheck = append(nodesToCheck, *node)
			}
			for sv := range node.SvIds {
				if _, ok := svMatches[sv]; !ok {
					svMatches[sv] = TokenToMatches{}
				}
				svMatches[sv][token] = node.Matches
			}
		}
	}

	// matchingStrings is a set where matches will be keys and those keys will be
	// mapped to an empty struct
	matchingStrings := map[string]struct{}{}
	exists := struct{}{}
	// Only select sv that matches all the tokens
	svList := []*pb.EntityInfo{}
	for sv, tokenMatches := range svMatches {
		if len(tokenMatches) == len(tokens) {
			svList = append(svList, &pb.EntityInfo{
				Dcid: sv,
				Name: index.Ranking[sv].RankingName,
			})
			for _, matchList := range tokenMatches {
				for match := range matchList {
					matchingStrings[match] = exists
				}
			}
		}
	}

	// Sort stat vars by number of PV; If two stat vars have same number of PV,
	// then order by the stat var (group) name.
	sort.SliceStable(svList, func(i, j int) bool {
		ranking := index.Ranking
		ri := ranking[svList[i].Dcid]
		rj := ranking[svList[j].Dcid]
		return compareRankingInfo(ri, svList[i].Dcid, rj, svList[j].Dcid)
	})

	matchingStringsList := []string{}
	for match := range matchingStrings {
		matchingStringsList = append(matchingStringsList, match)
	}
	sort.SliceStable(matchingStringsList, func(i, j int) bool {
		return matchingStringsList[i] < matchingStringsList[j]
	})

	return svList, matchingStringsList
}
