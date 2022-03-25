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
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/node"
	"github.com/datacommonsorg/mixer/internal/store"
)

const DEFAULT_LIMIT = 100

// GetStatVarMatch implements API for Mixer.GetStatVarMatch.
func GetStatVarMatch(
	ctx context.Context,
	in *pb.GetStatVarMatchRequest,
	store *store.Store,
) (*pb.GetStatVarMatchResponse, error) {
	propertyValue := in.GetPropertyValue()
	limit := in.GetLimit()
	if limit == 0 {
		limit = DEFAULT_LIMIT
	}
	statVarCount := map[string]int32{}
	// TODO: consider parallel this if performance is an issue.
	for property, value := range propertyValue {
		resp, err := node.GetPropertyValuesHelper(ctx, store, []string{value}, property, false)
		if err != nil {
			return nil, err
		}
		for _, node := range resp[value] {
			if node.Types == nil || node.Types[0] != "StatisticalVariable" {
				continue
			}
			statVarCount[node.Dcid]++
		}
	}
	result := &pb.GetStatVarMatchResponse{}
	for statVar, count := range statVarCount {
		result.MatchInfo = append(result.MatchInfo, &pb.GetStatVarMatchResponse_MatchInfo{
			StatVar:    statVar,
			MatchCount: count,
		})
	}
	sort.SliceStable(result.MatchInfo, func(i, j int) bool {
		if result.MatchInfo[i].MatchCount == result.MatchInfo[j].MatchCount {
			return result.MatchInfo[i].StatVar < result.MatchInfo[j].StatVar
		}
		return result.MatchInfo[i].MatchCount > result.MatchInfo[j].MatchCount
	})
	result.MatchInfo = result.MatchInfo[:limit]
	return result, nil
}
