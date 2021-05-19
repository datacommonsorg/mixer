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

// we want non human curated stat vars to be ranked last, so set their number of
// PVs to a number greater than max number of PVs for a human curated stat var.
const NonHumanCuratedNumPv = 30

// GetRawSvg gets the raw svg mapping.
func GetRawSvg(ctx context.Context, baseTable *bigtable.Table) (map[string]*pb.StatVarGroupNode, error) {
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

// GetParentSvgMap gets the mapping of svg/sv id to the parent svg for that svg/sv.
func GetParentSvgMap(rawSvg map[string]*pb.StatVarGroupNode) map[string][]string {
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

func updateSearchIndex(tokenString string, index map[string]map[string]RankingInfo, nodeID string) {
	processedTokenString := strings.ToLower(tokenString)
	processedTokenString = strings.ReplaceAll(processedTokenString, ",", " ")
	tokenList := strings.Fields(processedTokenString)
	approxNumPv := len(strings.Split(nodeID, "_"))
	if approxNumPv == 1 {
		// when approxNumPv is 1, most likely a non human curated PV
		approxNumPv = NonHumanCuratedNumPv
	}
	rankingInfo := RankingInfo{approxNumPv, tokenString}
	for _, token := range tokenList {
		if _, ok := index[token]; !ok {
			index[token] = map[string]RankingInfo{}
		}
		index[token][nodeID] = rankingInfo
	}
}

// GetSearchIndex gets the search index for the stat var hierarchy.
func GetSearchIndex(rawSvg map[string]*pb.StatVarGroupNode) map[string]map[string]RankingInfo {
	// map of token to map of sv/svg id to ranking information.
	searchIndex := map[string]map[string]RankingInfo{}
	for svgID, svgData := range rawSvg {
		tokenString := svgData.AbsoluteName
		updateSearchIndex(tokenString, searchIndex, svgID)
		for _, svData := range svgData.ChildStatVars {
			svTokenString := svData.SearchName
			updateSearchIndex(svTokenString, searchIndex, svData.Id)
		}
	}
	return searchIndex
}
