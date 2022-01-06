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
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"
	"sort"
	"time"

	cbt "cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// This should be synced with the list of blocklisted SVGs in the website repo
var blocklistedSvgIds = []string{"dc/g/Establishment_Industry"}
var miscellaneousSvgIds = []string{"eia/g/Root", "dc/g/Uncategorized"}

// GetRawSvg gets the raw svg mapping.
func GetRawSvg(ctx context.Context, baseTable *cbt.Table) (
	map[string]*pb.StatVarGroupNode, error) {
	svgResp := &pb.StatVarGroups{}
	row, err := baseTable.ReadRow(ctx, bigtable.BtStatVarGroup)
	if err != nil {
		return nil, err
	}
	if len(row[bigtable.BtFamily]) == 0 {
		return nil, status.Errorf(codes.NotFound, "Stat Var Group not found in cache")
	}
	raw := row[bigtable.BtFamily][0].Value
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

func getSynonymMap() map[string][]string {
	synonymMap := map[string][]string{}
	_, filename, _, _ := runtime.Caller(0)
	bytes, err := ioutil.ReadFile(path.Join(path.Dir(filename), "resource/synonyms.json"))
	if err == nil {
		var synonyms [][]string
		err = json.Unmarshal(bytes, &synonyms)
		if err == nil {
			// for each synonymList (list of words that are all synonyms of each other),
			// append that list to the synonymMap value for each word in the list.
			for _, synonymList := range synonyms {
				for _, word := range synonymList {
					if _, ok := synonymMap[word]; !ok {
						synonymMap[word] = synonymList
					} else {
						synonymMap[word] = append(synonymMap[word], synonymList...)
					}
				}
			}
		}
	}
	return synonymMap
}

// BuildStatVarSearchIndex builds the search index for the stat var hierarchy.
func BuildStatVarSearchIndex(
	rawSvg map[string]*pb.StatVarGroupNode,
	blocklist bool) *resource.SearchIndex {
	defer util.TimeTrack(time.Now(), "BuildStatVarSearchIndex")
	// map of token to map of sv/svg id to ranking information.
	searchIndex := &resource.SearchIndex{
		RootTrieNode: &resource.TrieNode{},
		Ranking:      map[string]*resource.RankingInfo{},
	}
	ignoredSVG := map[string]string{}
	// Exclude svg and sv under miscellaneous from the search index
	for _, svgID := range miscellaneousSvgIds {
		getIgnoredSVGHelper(ignoredSVG, rawSvg, svgID)
	}
	if blocklist {
		for _, svgID := range blocklistedSvgIds {
			getIgnoredSVGHelper(ignoredSVG, rawSvg, svgID)
		}
	}
	synonymMap := getSynonymMap()
	seenSV := map[string]struct{}{}
	for svgID, svgData := range rawSvg {
		if _, ok := ignoredSVG[svgID]; ok {
			continue
		}
		tokenString := svgData.AbsoluteName
		searchIndex.Update(svgID, tokenString, tokenString, true /* isSvg */, synonymMap)
		for _, svData := range svgData.ChildStatVars {
			if _, ok := seenSV[svData.Id]; ok {
				continue
			}
			seenSV[svData.Id] = struct{}{}
			svTokenString := svData.SearchName
			searchIndex.Update(svData.Id, svTokenString, svData.DisplayName, false /* isSvg */, synonymMap)
		}
	}
	return searchIndex
}
