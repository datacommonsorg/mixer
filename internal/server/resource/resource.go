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

package resource

import (
	"strings"

	"github.com/datacommonsorg/mixer/internal/translator/types"
)

// we want non human curated stat vars to be ranked last, so set their number of
// PVs to a number greater than max number of PVs for a human curated stat var.
const nonHumanCuratedNumPv = 30

// prefixes for pv definitions for known properties
var knownPVDefPrefixes = map[string]struct{}{
	"md=": {},
	"mq=": {},
	"st=": {},
	"mp=": {},
	"pt=": {},
}

// length of prefixes for known property definitions
const knownPVDefPrefixLength = 3

// Metadata represents the metadata used by the server.
type Metadata struct {
	Mappings          []*types.Mapping
	OutArcInfo        map[string]map[string][]*types.OutArcInfo
	InArcInfo         map[string][]*types.InArcInfo
	SubTypeMap        map[string]string
	HostProject       string
	BigQueryDataset   string
	RemoteMixerDomain string
	RemoteMixerAPIKey string
	FoldRemoteRootSvg bool
}

// SearchIndex holds the index for searching stat var (group).
type SearchIndex struct {
	RootTrieNode *TrieNode
	Ranking      map[string]*RankingInfo
}

// TrieNode represents a node in the sv hierarchy search Trie.
type TrieNode struct {
	ChildrenNodes map[rune]*TrieNode
	// SvIds are sets where Ids are keys and each key is mapped to an
	// empty struct
	SvIds map[string]struct{}
	// Matches is a set of strings that match the token ending at the current
	// trienode
	Matches map[string]struct{}
}

// RankingInfo holds the ranking information for each stat var hierarchy search
// result.
type RankingInfo struct {
	// ApproxNumPv is an estimate of the number of PVs in the sv/svg.
	ApproxNumPv int
	// Number of PVs for known properties
	NumKnownPv int
	// RankingName is the name we will be using to rank this sv/svg against other
	// sv/svg.
	RankingName string
}

// Update search index, given a stat var (group) node ID and string.
func (index *SearchIndex) Update(
	nodeID string, nodeString string, displayName string, synonymMap map[string][]string, svDefinition string) {
	processedNodeString := strings.ToLower(nodeString)
	processedNodeString = strings.ReplaceAll(processedNodeString, ",", " ")
	tokenList := strings.Fields(processedNodeString)
	// Create a map of tokens/synonyms to the matching string from nodeString
	tokens := map[string]string{}
	// add nodeID as a token, but only set the matching string if nodeID is in
	// the nodeString
	// eg. nodeID: Count_Person, Node String: Population
	// 		 matching string: ""
	// eg. nodeID: F_Bachelor_Degree: Node String: F_Bachelor_Degree_Owner
	// 		 matching string: "F_Bachelor_Degree"
	tokens[strings.ToLower(nodeID)] = ""
	if strings.Contains(nodeString, nodeID) {
		tokens[strings.ToLower(nodeID)] = nodeID
	}
	for _, token := range tokenList {
		// Do not process duplicate tokens
		if _, ok := tokens[token]; ok {
			continue
		}
		tokens[token] = token
		if synonymList, ok := synonymMap[token]; ok {
			for _, synonym := range synonymList {
				tokens[synonym] = token
			}
		}
	}
	for s, synonymList := range synonymMap {
		// For synonyms of phrases, need to check that the phrase is in the node
		// string
		if len(strings.Fields(s)) > 1 && strings.Contains(processedNodeString, s) {
			for _, synonym := range synonymList {
				tokens[synonym] = s
			}
		}
	}
	numPV := 0
	numKnownPv := 0
	if len(svDefinition) > 0 {
		svDefParts := strings.Split(svDefinition, ",")
		for _, defPart := range svDefParts {
			// don't include required properties when counting PVs
			if len(defPart) >= knownPVDefPrefixLength {
				prefix := defPart[0:knownPVDefPrefixLength]
				if _, ok := knownPVDefPrefixes[prefix]; ok {
					continue
				}
			}
			numPV++
		}
		numKnownPv = len(svDefParts) - numPV
	} else {
		numPV = len(strings.Split(nodeID, "_"))
		if numPV == 1 {
			// when there is no "_" in the node ID, most likely a non human curated PV
			numPV = nonHumanCuratedNumPv
		}
		// since we don't know which pvs are known when there's no stat var
		// definition, just assume all are known
		numKnownPv = numPV
	}
	// Ranking info is only dependent on a stat var (group).
	index.Ranking[nodeID] = &RankingInfo{numPV, numKnownPv, displayName}
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
		if currNode.SvIds == nil {
			currNode.SvIds = map[string]struct{}{}
		}
		currNode.SvIds[nodeID] = struct{}{}
		if currNode.Matches == nil {
			currNode.Matches = map[string]struct{}{}
		}
		currNode.Matches[match] = struct{}{}
	}
}
