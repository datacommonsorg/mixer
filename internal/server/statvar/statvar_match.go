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
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
)

func toQueryString(m map[string]string) string {
	b := new(bytes.Buffer)
	for key, value := range m {
		fmt.Fprintf(b, "%s ", key)
		fmt.Fprintf(b, "%s ", value)
	}
	return b.String()
}

func buildQuery(in *pb.GetStatVarMatchRequest) query.Query {
	var queries []query.Query
	propertyValueQuery := toQueryString(in.GetPropertyValue())
	if propertyValueQuery != "" {
		query := bleve.NewMatchQuery(propertyValueQuery)
		query.SetField("KeyValueText")
		queries = append(queries, query)
	}
	if in.GetQuery() != "" {
		query := bleve.NewMatchQuery(in.GetQuery())
		query.SetBoost(1.5)
		queries = append(queries, query)
	}
	return bleve.NewConjunctionQuery(queries...)
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
	query := buildQuery(in)
	searchRequest := bleve.NewSearchRequestOptions(query, int(limit), 0, true)
	// The - prefix indicates reverse direction.
	searchRequest.SortBy([]string{"-_score", "Title", "_id"})
	searchRequest.Fields = append(searchRequest.Fields, "Title")
	searchResults, err := cache.BleveSearchIndex.Search(searchRequest)
	if err != nil {
		return nil, err
	}

	result := &pb.GetStatVarMatchResponse{}
	for _, hit := range searchResults.Hits {
		result.MatchInfo = append(result.MatchInfo, &pb.GetStatVarMatchResponse_MatchInfo{
			StatVar:     hit.ID,
			StatVarName: hit.Fields["Title"].(string),
			Score:       hit.Score,
		})
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
