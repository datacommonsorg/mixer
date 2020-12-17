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

	pb "github.com/datacommonsorg/mixer/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetPlaceStatsVar implements API for Mixer.GetPlaceStatsVar.
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
	dataMap, err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var data PlaceStatsVar
			err := json.Unmarshal(jsonRaw, &data)
			if err != nil {
				return nil, err
			}
			return data.StatVarIds, nil
		}, nil)
	if err != nil {
		return nil, err
	}
	resp := pb.GetPlaceStatVarsResponse{Places: map[string]*pb.StatVars{}}
	for _, dcid := range dcids {
		resp.Places[dcid] = &pb.StatVars{StatVars: []string{}}
		if dataMap[dcid] != nil {
			resp.Places[dcid].StatVars = dataMap[dcid].([]string)
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

// GetPlaceStatVarsUnion implements API for Mixer.GetPlaceStatVarsUnion.
func (s *Server) GetPlaceStatVarsUnion(
	ctx context.Context, in *pb.GetPlaceStatVarsUnionRequest) (
	*pb.GetPlaceStatVarsUnionResponse, error) {
	dcids := in.GetDcids()
	if len(dcids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: dcid")
	}
	resp, err := s.GetPlaceStatVars(ctx, &pb.GetPlaceStatVarsRequest{Dcids: dcids})
	if err != nil {
		return nil, err
	}
	places := resp.GetPlaces()
	// Get union of the statvars for each place.
	set := map[string]bool{}
	for _, statVars := range places {
		for _, dcid := range statVars.GetStatVars() {
			set[dcid] = true
		}
	}
	return &pb.GetPlaceStatVarsUnionResponse{
		StatVars: &pb.StatVars{
			StatVars: keysToSlice(set),
		},
	}, nil
}
