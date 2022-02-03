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
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Count checks if places have data for stat vars and stat var groups.
//
// Returns a two level map from stat var dcid to place dcid to the number of
// stat vars with data. For a given stat var, if a place has no data, it will
// not show up in the second level map.
func Count(
	ctx context.Context,
	btGroup *bigtable.Group,
	svOrSvgs []string,
	places []string) (map[string]map[string]int32, error) {
	rowList, keyTokens := bigtable.BuildStatExistenceKey(places, svOrSvgs)
	keyToTokenFn := bigtable.TokenFn(keyTokens)
	baseDataList, _, err := bigtable.Read(
		ctx,
		btGroup,
		rowList,
		func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
			var statVarExistence pb.PlaceStatVarExistence
			if isProto {
				err := proto.Unmarshal(jsonRaw, &statVarExistence)
				if err != nil {
					return nil, err
				}
			} else {
				err := protojson.Unmarshal(jsonRaw, &statVarExistence)
				if err != nil {
					return nil, err
				}
			}
			return &statVarExistence, nil
		},
		keyToTokenFn,
		false, /* readBranch */
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
		if data, ok := baseDataList[0][token]; ok {
			c := data.(*pb.PlaceStatVarExistence)
			result[placeSv.StatVar][placeSv.Place] = c.NumDescendentStatVars
		}
	}
	return result, nil
}
