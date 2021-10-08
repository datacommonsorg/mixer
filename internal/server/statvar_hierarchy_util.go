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
	"context"
	"sort"
	"strings"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// RankingInfo holds the ranking information for each stat var hierarchy search
// result.
type RankingInfo struct {
	// ApproxNumPv is an estimate of the number of PVs in the sv/svg.
	ApproxNumPv int
	// RankingName is the name we will be using to rank this sv/svg against other
	// sv/svg.
	RankingName string
}

// SearchIndex holds the index for searching stat var (group).
type SearchIndex struct {
	token2sv  map[string]map[string]struct{}
	token2svg map[string]map[string]struct{}
	ranking   map[string]*RankingInfo
}

// we want non human curated stat vars to be ranked last, so set their number of
// PVs to a number greater than max number of PVs for a human curated stat var.
const nonHumanCuratedNumPv = 30

// This should be synced with the list of blocklisted SVGs in the website repo
var blocklistedSvgIds = []string{"dc/g/Establishment_Industry"}

// GetRawSvg gets the raw svg mapping.
func GetRawSvg(ctx context.Context, baseTable *bigtable.Table) (
	map[string]*pb.StatVarGroupNode, error) {
	svgResp := &pb.StatVarGroups{}
	row, err := baseTable.ReadRow(ctx, util.BtStatVarGroup)
	if err != nil {
		return nil, err
	}
	if len(row[util.BtFamily]) == 0 {
		return nil, status.Errorf(codes.NotFound, "Stat Var Group not found in cache")
	}
	raw := row[util.BtFamily][0].Value
	jsonRaw, err := util.UnzipAndDecode(string(raw))
	if err != nil {
		return nil, err
	}
	err = protojson.Unmarshal(jsonRaw, svgResp)
	if err != nil {
		return nil, err
	}
	return svgResp.StatVarGroups, nil
}

// BuildParentSvgMap gets the mapping of svg/sv id to the parent svg for that svg/sv.
func BuildParentSvgMap(rawSvg map[string]*pb.StatVarGroupNode) map[string][]string {
	parentSvgMap := map[string][]string{}
	for svgID, svgData := range rawSvg {
		for _, childSvg := range svgData.ChildStatVarGroups {
			if _, ok := parentSvgMap[childSvg.Id]; !ok {
				parentSvgMap[childSvg.Id] = []string{}
			}
			parentSvgMap[childSvg.Id] = append(parentSvgMap[childSvg.Id], svgID)
		}
		for _, childSv := range svgData.ChildStatVars {
			if _, ok := parentSvgMap[childSv.Id]; !ok {
				parentSvgMap[childSv.Id] = []string{}
			}
			parentSvgMap[childSv.Id] = append(parentSvgMap[childSv.Id], svgID)
		}
	}
	for _, parentSvgList := range parentSvgMap {
		sort.Strings(parentSvgList)
	}
	return parentSvgMap
}

// Update search index, given a stat var (group) node ID and string.
func (index *SearchIndex) update(
	nodeID string, nodeString string, displayName string, isSvg bool) {
	processedTokenString := strings.ToLower(nodeString)
	processedTokenString = strings.ReplaceAll(processedTokenString, ",", " ")
	tokenList := strings.Fields(processedTokenString)
	addtionalTokens := []string{}
	for _, token := range tokenList {
		if strings.HasSuffix(token, "s") {
			addtionalTokens = append(addtionalTokens, strings.TrimSuffix(token, "s"))
		}
	}
	tokenList = append(tokenList, addtionalTokens...)
	approxNumPv := len(strings.Split(nodeID, "_"))
	if approxNumPv == 1 {
		// when approxNumPv is 1, most likely a non human curated PV
		approxNumPv = nonHumanCuratedNumPv
	}
	// Ranking info is only dependent on a stat var (group).
	index.ranking[nodeID] = &RankingInfo{approxNumPv, displayName}
	// Populate token to stat var map.
	for _, token := range tokenList {
		if isSvg {
			if index.token2svg[token] == nil {
				index.token2svg[token] = map[string]struct{}{}
			}
			index.token2svg[token][nodeID] = struct{}{}
		} else {
			if index.token2sv[token] == nil {
				index.token2sv[token] = map[string]struct{}{}
			}
			index.token2sv[token][nodeID] = struct{}{}
		}
	}
}

// Helper to build a set of ignored SVGs.
func getIgnoredSVGHelper(
	ignoredSvg map[string]string,
	rawSvg map[string]*pb.StatVarGroupNode,
	svgID string) {
	ignoredSvg[svgID] = ""
	if svgData, ok := rawSvg[svgID]; ok {
		for _, svData := range svgData.ChildStatVarGroups {
			getIgnoredSVGHelper(ignoredSvg, rawSvg, svData.Id)
		}
	}
}

// BuildStatVarSearchIndex builds the search index for the stat var hierarchy.
func BuildStatVarSearchIndex(
	rawSvg map[string]*pb.StatVarGroupNode,
	blocklist bool) *SearchIndex {
	// map of token to map of sv/svg id to ranking information.
	searchIndex := &SearchIndex{
		token2sv:  map[string]map[string]struct{}{},
		token2svg: map[string]map[string]struct{}{},
		ranking:   map[string]*RankingInfo{},
	}
	ignoredSVG := map[string]string{}
	if blocklist {
		for _, svgID := range blocklistedSvgIds {
			getIgnoredSVGHelper(ignoredSVG, rawSvg, svgID)
		}
	}
	for svgID, svgData := range rawSvg {
		if _, ok := ignoredSVG[svgID]; ok {
			continue
		}
		tokenString := svgData.AbsoluteName
		searchIndex.update(svgID, tokenString, tokenString, true /* isSvg */)
		for _, svData := range svgData.ChildStatVars {
			svTokenString := svData.SearchName
			searchIndex.update(svData.Id, svTokenString, svData.DisplayName, false /* isSvg */)
		}
	}
	return searchIndex
}
