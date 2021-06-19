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

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

const (
	maxFilteredIds = 3000 // Twice of maxResult to give buffer for place filter.
	maxResult      = 1000
)

// SearchStatVar implements API for Mixer.SearchStatVar.
func (s *Server) SearchStatVar(
	ctx context.Context, in *pb.SearchStatVarRequest) (
	*pb.SearchStatVarResponse, error,
) {
	query := in.GetQuery()
	places := in.GetPlaces()

	result := &pb.SearchStatVarResponse{
		StatVars:      []*pb.EntityInfo{},
		StatVarGroups: []*pb.EntityInfo{},
	}
	if query == "" {
		return result, nil
	}
	tokens := strings.Fields(
		strings.Replace(strings.ToLower(query), ",", " ", -1))
	svList, svgList := searchTokens(tokens, s.cache.SvgSearchIndex)

	// Filter the stat var and stat var group by places.
	if len(places) > 0 {
		// Will read from stat existence cache, and can take several seconds when
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

		statExistence, err := checkStatExistence(ctx, s.store, ids, places)
		if err != nil {
			return nil, err
		}
		svList = filter(svList, statExistence, len(places))
		svgList = filter(svgList, statExistence, len(places))
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
	nodes []*pb.EntityInfo, countMap map[string]int, numPlaces int) []*pb.EntityInfo {
	result := []*pb.EntityInfo{}
	for _, node := range nodes {
		if c, ok := countMap[node.Dcid]; ok && c == numPlaces {
			result = append(result, node)
		}
	}
	return result
}

func searchTokens(
	tokens []string, index *SearchIndex) ([]*pb.EntityInfo, []*pb.EntityInfo) {
	svCount := map[string]int{}
	svgCount := map[string]int{}
	for _, token := range tokens {
		for sv := range index.token2sv[token] {
			svCount[sv]++
		}
		for svg := range index.token2svg[token] {
			svgCount[svg]++
		}
	}

	// Only select sv and svg that matches all the tokens
	svList := []*pb.EntityInfo{}
	for sv, c := range svCount {
		if c == len(tokens) {
			svList = append(svList, &pb.EntityInfo{
				Dcid: sv,
				Name: index.ranking[sv].RankingName,
			})
		}
	}

	// Sort stat vars by number of PV; If two stat vars have same number of PV,
	// then order by the stat var (group) name.
	sort.SliceStable(svList, func(i, j int) bool {
		ranking := index.ranking
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
				Name: index.ranking[svg].RankingName,
			})
		}
	}
	sort.SliceStable(svgList, func(i, j int) bool {
		ranking := index.ranking
		ri := ranking[svgList[i].Dcid]
		rj := ranking[svgList[j].Dcid]
		if ri.ApproxNumPv == rj.ApproxNumPv {
			return ri.RankingName < rj.RankingName
		}
		return ri.ApproxNumPv < rj.ApproxNumPv
	})
	return svList, svgList
}
