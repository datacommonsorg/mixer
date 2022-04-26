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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/protobuf/proto"
)

// Count checks if places have data for stat vars and stat var groups.
//
// Returns a two level map from stat var dcid to place dcid to the number of
// stat vars with data. For a given stat var, if a place has no data, it will
// not show up in the second level map.
func Count(
	ctx context.Context,
	st *store.Store,
	svOrSvgs []string,
	places []string,
) (map[string]map[string]int32, error) {
	rowList, keyTokens := bigtable.BuildStatExistenceKey(places, svOrSvgs)
	keyToTokenFn := stat.TokenFn(keyTokens)
	btDataList, err := bigtable.Read(
		ctx,
		st.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var statVarExistence pb.PlaceStatVarExistence
			if err := proto.Unmarshal(jsonRaw, &statVarExistence); err != nil {
				return nil, err
			}
			return &statVarExistence, nil
		},
		keyToTokenFn,
	)
	if err != nil {
		return nil, err
	}
	// Initialize result
	result := map[string]map[string]int32{}
	for _, id := range svOrSvgs {
		result[id] = map[string]int32{}
	}
	// Populate the count
	for _, rowKey := range rowList {
		placeSv := keyTokens[rowKey]
		token, _ := keyToTokenFn(rowKey)
		for _, btData := range btDataList {
			if data, ok := btData[token]; ok {
				c := data.(*pb.PlaceStatVarExistence)
				descSVCount := c.GetDescendentStatVarCount()
				if _, ok := result[placeSv.StatVar][placeSv.Place]; !ok {
					// When c.NumDescendentStatVars = 0, placeSv.StatVar is a stat var
					// (not a stat var group). In this case the check here is necessary,
					// otherwise the proto default 0 is compared, and this map field will
					// not be populated.
					result[placeSv.StatVar][placeSv.Place] = descSVCount
				} else if descSVCount > result[placeSv.StatVar][placeSv.Place] {
					result[placeSv.StatVar][placeSv.Place] = descSVCount
				}
			}
		}
	}
	// Populate stat vars from private import
	if st.MemDb.GetSvg() != nil {
		for sv, placeData := range st.MemDb.GetPlaceSvExistence() {
			result[sv] = map[string]int32{}
			for _, place := range places {
				if count, ok := placeData[place]; ok {
					result[sv][place] = count
				}
			}
		}
	}
	return result, nil
}
