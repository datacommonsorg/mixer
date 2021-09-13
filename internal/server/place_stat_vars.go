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

package server

import (
	"context"
	"sort"

	"encoding/json"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetPlaceStatsVar implements API for Mixer.GetPlaceStatsVar.
// TODO(shifucun): Migrate clients to use GetPlaceStatVars and deprecate this.
func (s *Server) GetPlaceStatsVar(
	ctx context.Context, in *pb.GetPlaceStatsVarRequest) (
	*pb.GetPlaceStatsVarResponse, error) {

	req := pb.GetPlaceStatVarsRequest{Dcids: in.GetDcids()}
	resp, err := s.GetPlaceStatVars(ctx, &req)
	if err != nil {
		return nil, err
	}
	out := pb.GetPlaceStatsVarResponse{Places: map[string]*pb.StatsVars{}}
	for dcid, statVars := range resp.Places {
		out.Places[dcid] = &pb.StatsVars{StatsVars: statVars.StatVars}
	}
	return &out, nil
}

// GetPlaceStatVars implements API for Mixer.GetPlaceStatVars.
func (s *Server) GetPlaceStatVars(
	ctx context.Context, in *pb.GetPlaceStatVarsRequest) (
	*pb.GetPlaceStatVarsResponse, error) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: dcid")
	}
	rowList := buildPlaceStatsVarKey(dcids)
	baseDataMap, branchDataMap, err := bigTableReadRowsParallel(
		ctx,
		s.store,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var data PlaceStatsVar
			err := json.Unmarshal(jsonRaw, &data)
			if err != nil {
				return nil, err
			}
			return data.StatVarIds, nil
		},
		nil,
		true, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	resp := pb.GetPlaceStatVarsResponse{Places: map[string]*pb.StatVars{}}
	for _, dcid := range dcids {
		resp.Places[dcid] = &pb.StatVars{StatVars: []string{}}
		if baseDataMap[dcid] != nil {
			resp.Places[dcid].StatVars = baseDataMap[dcid].([]string)
		}
		if branchDataMap[dcid] != nil {
			resp.Places[dcid].StatVars = util.MergeDedupe(
				resp.Places[dcid].StatVars, baseDataMap[dcid].([]string))
		}
		// Also merge from memdb
		if !s.store.MemDb.IsEmpty() {
			hasDataStatVars, _ := s.store.MemDb.GetStatVars([]string{dcid})
			resp.Places[dcid].StatVars = util.MergeDedupe(
				resp.Places[dcid].StatVars, hasDataStatVars)
		}
	}
	return &resp, nil
}

// keysToSlice stores the keys of a map in a slice.
func keysToSlice(m map[string]bool) []string {
	s := make([]string, len(m))
	i := 0
	for k := range m {
		s[i] = k
		i++
	}
	sort.Strings(s)
	return s
}

// GetPlaceStatVarsUnionV1 implements API for Mixer.GetPlaceStatVarsUnionV1.
func (s *Server) GetPlaceStatVarsUnionV1(
	ctx context.Context, in *pb.GetPlaceStatVarsUnionRequest,
) (*pb.GetPlaceStatVarsUnionResponse, error) {
	// Check places
	places := in.GetDcids()
	if len(places) == 0 {
		return nil, status.Error(
			codes.InvalidArgument, "Missing required arguments: dcids")
	}
	// filtered stat vars
	filterStatVars := in.GetStatVars()
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
		statVarCount, err := countStatVar(ctx, s.store, filterStatVars, places)
		if err != nil {
			return nil, err
		}
		for sv := range filterStatVarSet {
			if existence, ok := statVarCount[sv]; ok && len(existence) > 0 {
				result.StatVars = append(result.StatVars, sv)
			}
		}
	} else {
		resp, err := s.GetPlaceStatVars(ctx, &pb.GetPlaceStatVarsRequest{Dcids: places})
		if err != nil {
			return nil, err
		}
		place2StatVars := resp.GetPlaces()

		// For single place, return directly.
		if len(places[0]) == 1 {
			return &pb.GetPlaceStatVarsUnionResponse{StatVars: place2StatVars[places[0]].StatVars}, nil
		}

		// Get union of the statvars for multiple places.
		set := map[string]bool{}
		for _, statVars := range place2StatVars {
			for _, sv := range statVars.GetStatVars() {
				set[sv] = true
			}
		}
		result.StatVars = keysToSlice(set)
	}

	// Also check from in-memory database
	set := map[string]bool{}
	if !s.store.MemDb.IsEmpty() {
		hasDataStatVars, _ := s.store.MemDb.GetStatVars(places)
		for _, sv := range hasDataStatVars {
			if len(filterStatVarSet) == 0 {
				set[sv] = true
			} else {
				if _, ok := filterStatVarSet[sv]; ok {
					set[sv] = true
				}
			}
		}
	}
	result.StatVars = util.MergeDedupe(result.StatVars, keysToSlice(set))
	return result, nil
}
