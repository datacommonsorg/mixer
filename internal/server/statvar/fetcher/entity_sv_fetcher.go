// Copyright 2023 Google LLC
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

package fetcher

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

// FetchEntityVariables fetch variables for given entities.
// This function fetches data from both Bigtable and SQL database.
func FetchEntityVariables(
	ctx context.Context,
	store *store.Store,
	entities []string,
) (map[string]*pb.StatVars, error) {
	resp := map[string]*pb.StatVars{}
	for _, entity := range entities {
		resp[entity] = &pb.StatVars{StatVars: []string{}}
	}
	// Fetch from Bigtable
	if store.BtGroup != nil {
		btDataList, err := bigtable.Read(
			ctx,
			store.BtGroup,
			bigtable.BtPlaceStatsVarPrefix,
			[][]string{entities},
			func(jsonRaw []byte) (interface{}, error) {
				var data pb.PlaceStatVars
				if err := proto.Unmarshal(jsonRaw, &data); err != nil {
					return nil, err
				}
				return data.StatVarIds, nil
			},
		)
		if err != nil {
			return nil, err
		}
		for _, entity := range entities {
			resp[entity] = &pb.StatVars{StatVars: []string{}}
			allStatVars := [][]string{}
			// btDataList is a list of import group data
			for _, btData := range btDataList {
				// Each row in btData represent one entity data.
				for _, row := range btData {
					if row.Parts[0] != entity {
						continue
					}
					allStatVars = append(allStatVars, row.Data.([]string))
				}
			}
			resp[entity].StatVars = util.MergeDedupe(allStatVars...)
		}
	}
	// Fetch from SQL database
	if store.SQLClient.DB != nil {
		query := fmt.Sprintf(
			`
				SELECT entity, GROUP_CONCAT(DISTINCT variable) AS variables
				FROM observations WHERE entity in (%s)
				GROUP BY entity;
			`,
			util.SQLInParam(len(entities)),
		)
		// Execute query
		rows, err := store.SQLClient.DB.Query(query, util.ConvertArgs(entities)...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var entity, variableStr string
			err = rows.Scan(&entity, &variableStr)
			if err != nil {
				return nil, err
			}
			variables := strings.Split(variableStr, ",")
			resp[entity].StatVars = util.MergeDedupe(resp[entity].StatVars, variables)
		}
	}
	return resp, nil
}
