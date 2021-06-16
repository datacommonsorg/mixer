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

// SearchStatVar implements API for Mixer.SearchStatVar.
func (s *Server) SearchStatVar(
	ctx context.Context, in *pb.SearchStatVarRequest) (
	*pb.SearchStatVarResponse, error) {
	query := in.GetQuery()
	result := &pb.SearchStatVarResponse{
		StatVars:      []string{},
		StatVarGroups: []string{},
	}
	if query == "" {
		return result, nil
	}
	tokens := strings.Fields(
		strings.Replace(strings.ToLower(query), ",", " ", -1))
	svList, svgList := searchTokens(tokens, s.cache.SvgSearchIndex)
	result.StatVars = svList
	result.StatVarGroups = svgList

	return result, nil
}

func searchTokens(tokens []string, index *SearchIndex) ([]string, []string) {
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
	svList := []string{}
	for sv, c := range svCount {
		if c == len(tokens) {
			svList = append(svList, sv)
		}
	}
	// Sort stat vars by ID then by number of PV
	sort.Strings(svList)
	sort.SliceStable(svList, func(i, j int) bool {
		ranking := index.ranking
		return ranking[svList[i]].ApproxNumPv < ranking[svList[j]].ApproxNumPv
	})

	svgList := []string{}
	for svg, c := range svgCount {
		if c == len(tokens) {
			svgList = append(svgList, svg)
		}
	}
	// Sort stat var groups by ID then by number of PV
	sort.Strings(svgList)
	sort.SliceStable(svgList, func(i, j int) bool {
		ranking := index.ranking
		return ranking[svgList[i]].ApproxNumPv < ranking[svgList[j]].ApproxNumPv
	})
	return svList, svgList
}
