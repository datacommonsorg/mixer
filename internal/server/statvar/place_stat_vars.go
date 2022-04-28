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
	"sort"

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
	*pb.GetPlaceStatsVarResponse, error) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: dcids")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid dcids")
	}

	resp, err := GetEntityStatVarsHelper(ctx, dcids, store)
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
func GetEntityStatVarsHelper(
	ctx context.Context, entities []string, store *store.Store) (
	map[string]*pb.StatVars, error) {
	rowList := bigtable.BuildPlaceStatsVarKey(entities)
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(jsonRaw []byte) (interface{}, error) {
			var data pb.PlaceStatVars
			if err := proto.Unmarshal(jsonRaw, &data); err != nil {
				return nil, err
			}
			return data.StatVarIds, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	resp := map[string]*pb.StatVars{}
	for _, entity := range entities {
		resp[entity] = &pb.StatVars{StatVars: []string{}}
		allStatVars := [][]string{}
		for _, btData := range btDataList {
			if btData[entity] != nil {
				allStatVars = append(allStatVars, btData[entity].([]string))
			}
		}
		// Also merge from memdb
		if !store.MemDb.IsEmpty() {
			hasDataStatVars, _ := store.MemDb.GetStatVars([]string{entity})
			allStatVars = append(allStatVars, hasDataStatVars)
		}
		resp[entity].StatVars = util.MergeDedupe(allStatVars...)
		sort.Strings(resp[entity].StatVars)
	}
	return resp, nil
}

// GetPlaceStatVarsUnionV1 implements API for Mixer.GetPlaceStatVarsUnionV1.
func GetPlaceStatVarsUnionV1(
	ctx context.Context, in *pb.GetPlaceStatVarsUnionRequest, store *store.Store,
) (*pb.GetPlaceStatVarsUnionResponse, error) {
	// Check places
	places := in.GetDcids()
	if len(places) == 0 {
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
	result := &pb.GetPlaceStatVarsUnionResponse{}

	// When given a list of stat vars to filter for, we can use the existence
	// cache instead to check the existence of each stat var for the list of
	// places. This is faster than getting all the stat vars for each place and
	// then filtering.
	if len(filterStatVars) > 0 && len(places) > 0 {
		statVarCount, err := Count(ctx, store, filterStatVars, places)
		if err != nil {
			return nil, err
		}
		for sv := range filterStatVarSet {
			if existence, ok := statVarCount[sv]; ok && len(existence) > 0 {
				result.StatVars = append(result.StatVars, sv)
			}
		}
	} else {
		resp, err := GetEntityStatVarsHelper(ctx, places, store)
		if err != nil {
			return nil, err
		}
		place2StatVars := resp

		// For single place, return directly.
		if len(places) == 1 {
			return &pb.GetPlaceStatVarsUnionResponse{StatVars: place2StatVars[places[0]].StatVars}, nil
		}

		// Get union of the statvars for multiple places.
		set := map[string]bool{}
		for _, statVars := range place2StatVars {
			for _, sv := range statVars.GetStatVars() {
				set[sv] = true
			}
		}
		result.StatVars = util.KeysToSlice(set)
	}

	// Also check from in-memory database
	if !store.MemDb.IsEmpty() {
		set := map[string]bool{}
		hasDataStatVars, _ := store.MemDb.GetStatVars(places)
		for _, sv := range hasDataStatVars {
			if len(filterStatVarSet) == 0 {
				set[sv] = true
			} else {
				if _, ok := filterStatVarSet[sv]; ok {
					set[sv] = true
				}
			}
		}
		result.StatVars = util.MergeDedupe(result.StatVars, util.KeysToSlice(set))
	}
	return result, nil
}
