// Copyright 2022 Google LLC
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
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
)

func buildExplanationString(in *search.Explanation, sb *strings.Builder, level int) {
	sb.WriteString(fmt.Sprintf("%.2f => %s\n", in.Value, in.Message))
	if len(in.Children) > 0 {
		for _, cchild := range in.Children {
			for i := 0; i < level+1; i++ {
				sb.WriteString(" ")
			}
			buildExplanationString(cchild, sb, level+1)
		}
	}
}

// For some unknown reason bleve seems to not return deterministic scores.
// Therefore to avoid issues in the test we simply round float to a lower
// precision and employ a sorting strategy based on other factors.
func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

const defaultLimit = 10

// GetStatVarMatch implements API for Mixer.GetStatVarMatch.
func GetStatVarMatch(
	ctx context.Context,
	in *pb.GetStatVarMatchRequest,
	store *store.Store,
	cache *resource.Cache,
) (*pb.GetStatVarMatchResponse, error) {
	limit := in.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	query := bleve.NewQueryStringQuery(in.GetQuery())
	searchRequest := bleve.NewSearchRequestOptions(query, int(limit), 0, in.GetDebug())
	// The - prefix indicates reverse direction.
	searchRequest.SortBy([]string{"-_score", "nc", "nt", "_id"})
	searchRequest.Fields = append(searchRequest.Fields, "t")
	searchResults, err := cache.BleveSearchIndex.Search(searchRequest)
	if err != nil {
		return nil, err
	}

	result := &pb.GetStatVarMatchResponse{}
	for _, hit := range searchResults.Hits {
		matchInfo := &pb.GetStatVarMatchResponse_MatchInfo{
			StatVar:     hit.ID,
			StatVarName: hit.Fields["t"].(string),
			Score:       roundFloat(hit.Score, 5),
		}
		if in.GetDebug() {
			var sb strings.Builder
			sb.WriteString(in.GetQuery())
			sb.WriteString("\n")
			buildExplanationString(hit.Expl, &sb, 0)
			matchInfo.Explanation = strings.ToValidUTF8(sb.String(), "")
		}
		result.MatchInfo = append(result.MatchInfo, matchInfo)
	}
	// 1) Highest score wins.
	// 2) If score are the same, shortest statvar id wins.
	// 3) Otherwise sort lexicographically.
	sort.SliceStable(result.MatchInfo, func(i, j int) bool {
		if result.MatchInfo[i].Score == result.MatchInfo[j].Score {
			if len(result.MatchInfo[i].StatVar) != len(result.MatchInfo[j].StatVar) {
				return len(result.MatchInfo[i].StatVar) < len(result.MatchInfo[j].StatVar)
			}
			return result.MatchInfo[i].StatVar < result.MatchInfo[j].StatVar
		}
		return result.MatchInfo[i].Score > result.MatchInfo[j].Score
	})
	return result, nil
}
