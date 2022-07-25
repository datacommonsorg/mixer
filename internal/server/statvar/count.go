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
	btDataList, err := bigtable.Read(
		ctx,
		st.BtGroup,
		bigtable.BtSVAndSVGExistence,
		[][]string{places, svOrSvgs},
		func(jsonRaw []byte) (interface{}, error) {
			var statVarExistence pb.EntitytatVarExistence
			if err := proto.Unmarshal(jsonRaw, &statVarExistence); err != nil {
				return nil, err
			}
			return &statVarExistence, nil
		},
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
	for _, btData := range btDataList {
		for _, row := range btData {
			p := row.Parts[0]
			sv := row.Parts[1]
			c := row.Data.(*pb.EntityStatVarExistence)
			descSVCount := c.GetDescendentStatVarCount()
			if _, ok := result[sv][p]; !ok {
				// When c.NumDescendentStatVars = 0, placeSv.StatVar is a stat var
				// (not a stat var group). In this case the check here is necessary,
				// otherwise the proto default 0 is compared, and this map field will
				// not be populated.
				result[sv][p] = descSVCount
			} else if descSVCount > result[sv][p] {
				result[sv][p] = descSVCount
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
