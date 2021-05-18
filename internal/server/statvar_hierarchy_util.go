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
	"strings"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type RankingInfo struct {
	ApproxNumPv int
	RankingName string
}

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

func GetParentSvgMap(rawSvg map[string]*pb.StatVarGroupNode) map[string]string {
	parentSvgMap := map[string]string{}
	for svgId, svgData := range rawSvg {
		for _, childSvg := range svgData.ChildStatVarGroups {
			if _, ok := parentSvgMap[childSvg.Id]; !ok {
				parentSvgMap[childSvg.Id] = svgId
			}
		}
		for _, childSv := range svgData.ChildStatVars {
			if _, ok := parentSvgMap[childSv.Id]; !ok {
				parentSvgMap[childSv.Id] = svgId
			}
		}
	}
	return parentSvgMap
}

func updateSearchIndex(tokenString string, index map[string]map[string]RankingInfo, nodeId string) {
	processedTokenString := strings.ToLower(tokenString)
	processedTokenString = strings.ReplaceAll(processedTokenString, ",", " ")
	tokenList := strings.Fields(processedTokenString)
	approxNumPv := len(strings.Split(nodeId, "_"))
	if approxNumPv == 1 {
		// when approxNumPv is 1, most likely a non human curated PV and we want them
		// ranked lower (less approxNumPv, higher ranking)
		approxNumPv = 30
	}
	rankingInfo := RankingInfo{approxNumPv, tokenString}
	for _, token := range tokenList {
		if _, ok := index[token]; !ok {
			index[token] = map[string]RankingInfo{}
		}
		index[token][nodeId] = rankingInfo
	}
}

func GetSearchIndex(rawSvg map[string]*pb.StatVarGroupNode) map[string]map[string]RankingInfo {
	searchIndex := map[string]map[string]RankingInfo{}
	for svgId, svgData := range rawSvg {
		tokenString := svgData.AbsoluteName
		updateSearchIndex(tokenString, searchIndex, svgId)
		for _, svData := range svgData.ChildStatVars {
			svTokenString := svData.SearchName
			updateSearchIndex(svTokenString, searchIndex, svData.Id)
		}
	}
	return searchIndex
}
