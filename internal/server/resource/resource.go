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

package resource

import (
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/translator/types"
)

// we want non human curated stat vars to be ranked last, so set their number of
// PVs to a number greater than max number of PVs for a human curated stat var.
const nonHumanCuratedNumPv = 30

// Cache holds cached data for the mixer server.
type Cache struct {
	// ParentSvg is a map of sv/svg id to a list of its parent svgs sorted alphabetically.
	ParentSvg map[string][]string
	// SvgInfo is a map of svg id to its information.
	RawSvg                    map[string]*pb.StatVarGroupNode
	SvgSearchIndex            *SearchIndex
	BlocklistedSvgSearchIndex *SearchIndex
}

// Metadata represents the metadata used by the server.
type Metadata struct {
	Mappings         []*types.Mapping
	OutArcInfo       map[string]map[string][]types.OutArcInfo
	InArcInfo        map[string][]types.InArcInfo
	SubTypeMap       map[string]string
	Bq               string
	BtProject        string
	BranchBtInstance string
	BaseTables       []string
	BranchTable      string
}

// SearchIndex holds the index for searching stat var (group).
type SearchIndex struct {
	RootTrieNode *TrieNode
	Ranking      map[string]*RankingInfo
}

// TrieNode represents a node in the sv hierarchy search Trie.
type TrieNode struct {
	ChildrenNodes map[rune]*TrieNode
	// SvgIds and SvIds are maps of the dcid to the string within the
	// Svg/Sv name that matches the token ending at the current TrieNode.
	//
	// For example:
	// token ending @ current TrieNode: "women"
	// synonyms: ["female", "women"]
	// svg names: svg1: "Female Population", svg2: "Women Population"
	//
	// SvgIds map: {
	// 	"svg1": "Female",
	// 	"svg2": "Women"
	// }
	SvgIds map[string]string
	SvIds  map[string]string
}

// RankingInfo holds the ranking information for each stat var hierarchy search
// result.
type RankingInfo struct {
	// ApproxNumPv is an estimate of the number of PVs in the sv/svg.
	ApproxNumPv int
	// RankingName is the name we will be using to rank this sv/svg against other
	// sv/svg.
	RankingName string
}

// Update search index, given a stat var (group) node ID and string.
func (index *SearchIndex) Update(
	nodeID string, nodeString string, displayName string, isSvg bool, synonymMap map[string][]string) {
	processedNodeString := strings.ReplaceAll(nodeString, ",", " ")
	tokenList := strings.Fields(processedNodeString)
	// Create a map of tokens/synonyms to the matching string from nodeString
	tokens := map[string]string{}
	for _, token := range tokenList {
		processedToken := strings.ToLower(token)
		tokens[processedToken] = token
		if synonymList, ok := synonymMap[processedToken]; ok {
			for _, synonym := range synonymList {
				tokens[synonym] = token
			}
		}
	}
	approxNumPv := len(strings.Split(nodeID, "_"))
	if approxNumPv == 1 {
		// when approxNumPv is 1, most likely a non human curated PV
		approxNumPv = nonHumanCuratedNumPv
	}
	// Ranking info is only dependent on a stat var (group).
	index.Ranking[nodeID] = &RankingInfo{approxNumPv, displayName}
	// Populate trie with each token
	for token, match := range tokens {
		currNode := index.RootTrieNode
		for _, c := range token {
			if currNode.ChildrenNodes == nil {
				currNode.ChildrenNodes = map[rune]*TrieNode{}
			}
			if _, ok := currNode.ChildrenNodes[c]; !ok {
				currNode.ChildrenNodes[c] = &TrieNode{}
			}
			currNode = currNode.ChildrenNodes[c]
		}
		if isSvg {
			if currNode.SvgIds == nil {
				currNode.SvgIds = map[string]string{}
			}
			currNode.SvgIds[nodeID] = match
		} else {
			if currNode.SvIds == nil {
				currNode.SvIds = map[string]string{}
			}
			currNode.SvIds[nodeID] = match
		}
	}
}
