// Copyright 2019 Google LLC
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
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// GetPlaceStatsVar implements API for Mixer.GetPlaceStatsVar.
// TODO(shifucun): Migrate clients to use GetPlaceStatVars and deprecate this.
func GetPlaceStatsVar(
	ctx context.Context, in *pb.GetPlaceStatsVarRequest, store *store.Store) (
	*pb.GetPlaceStatsVarResponse, error,
) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: dcids")
	}
	if err := util.CheckValidDCIDs(dcids); err != nil {
		return nil, err
	}

	resp, err := GetEntityStatVarsHelper(ctx, store, dcids)
	if err != nil {
		return nil, err
	}
	out := pb.GetPlaceStatsVarResponse{Places: map[string]*pb.StatsVars{}}
	for dcid, statVars := range resp {
		out.Places[dcid] = &pb.StatsVars{StatsVars: statVars.StatVars}
	}
	return &out, nil
}

// GetEntityStatVarsHelper is a wrapper to get stat vars for given entities.
// This function fetches data from both Bigtable and SQLite database.
func GetEntityStatVarsHelper(
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
	// Fetch from SQLite
	if store.SQLiteClient != nil {
		entitiesStr := "'" + strings.Join(entities, "', '") + "'"
		query := fmt.Sprintf(
			"SELECT entity, GROUP_CONCAT(DISTINCT variable) AS variables "+
				"FROM observations WHERE entity in (%s) "+
				"GROUP BY entity;",
			entitiesStr,
		)
		// Execute query
		rows, err := store.SQLiteClient.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		if err != nil {
			return nil, err
		}
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

// GetEntityStatVarsUnionV1 implements API for Mixer.GetEntityStatVarsUnionV1.
func GetEntityStatVarsUnionV1(
	ctx context.Context, in *pb.GetEntityStatVarsUnionRequest, store *store.Store,
) (*pb.GetEntityStatVarsUnionResponse, error) {
	// Check entities
	entities := in.GetDcids()
	if len(entities) == 0 {
		return nil, status.Error(
			codes.InvalidArgument, "Missing required arguments: dcids")
	}
	// filtered stat vars
	filterStatVars := in.GetStatVars()
	// Create a set to make the loop up logic more efficient
	filterStatVarSet := map[string]struct{}{}
	for _, sv := range filterStatVars {
		filterStatVarSet[sv] = struct{}{}
	}
	result := &pb.GetEntityStatVarsUnionResponse{}

	// When given a list of stat vars to filter for, we can use the existence
	// cache instead to check the existence of each stat var for the list of
	// entities. This is faster than getting all the stat vars for each entity and
	// then filtering.
	if len(filterStatVars) > 0 && len(entities) > 0 {
		statVarCount, err := Count(ctx, store, filterStatVars, entities)
		if err != nil {
			return nil, err
		}
		for sv := range filterStatVarSet {
			if existence, ok := statVarCount[sv]; ok && len(existence) > 0 {
				result.StatVars = append(result.StatVars, sv)
			}
		}
	} else {
		resp, err := GetEntityStatVarsHelper(ctx, store, entities)
		if err != nil {
			return nil, err
		}
		place2StatVars := resp

		// For single entity, return directly.
		if len(entities) == 1 {
			return &pb.GetEntityStatVarsUnionResponse{StatVars: place2StatVars[entities[0]].StatVars}, nil
		}

		// Get union of the statvars for multiple entities.
		set := map[string]bool{}
		for _, statVars := range place2StatVars {
			for _, sv := range statVars.GetStatVars() {
				set[sv] = true
			}
		}
		result.StatVars = util.KeysToSlice(set)
	}
	return result, nil
}
