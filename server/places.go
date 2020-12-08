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
	"fmt"
	"strings"

	"encoding/json"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// GetPlacesIn implements API for Mixer.GetPlacesIn.
func (s *Server) GetPlacesIn(ctx context.Context, in *pb.GetPlacesInRequest) (
	*pb.GetPlacesInResponse, error) {
	dcids := in.GetDcids()
	placeType := in.GetPlaceType()

	if len(dcids) == 0 || placeType == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments")
	}
	if !util.CheckValidDCIDs(dcids) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid DCIDs")
	}

	rowList := buildPlaceInKey(dcids, placeType)

	dataMap, err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			return strings.Split(string(jsonRaw), ","), nil
		}, nil)
	if err != nil {
		return nil, err
	}
	results := []map[string]string{}
	for _, dcid := range dcids {
		if dataMap[dcid] != nil {
			for _, place := range dataMap[dcid].([]string) {
				results = append(results, map[string]string{"dcid": dcid, "place": place})
			}
		}
	}

	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	return &pb.GetPlacesInResponse{Payload: string(jsonRaw)}, nil
}

// RelatedLocationsPrefixMap is a map from different scenarios to key prefix for
// RelatedLocations cache.
//
// The three levels of keys are:
// - Whether related locations have the same ancestor.
// - Whether related locations have the same place type.
// - Whether closeness computaion is per capita.
var RelatedLocationsPrefixMap = map[bool]map[bool]string{
	true: {
		true:  util.BtRelatedLocationsSameTypeAndAncestorPCPrefix,
		false: util.BtRelatedLocationsSameTypeAndAncestorPrefix,
	},
	false: {
		true:  util.BtRelatedLocationsSameTypePCPrefix,
		false: util.BtRelatedLocationsSameTypePrefix,
	},
}

// GetRelatedLocations implements API for Mixer.GetRelatedLocations.
func (s *Server) GetRelatedLocations(ctx context.Context,
	in *pb.GetRelatedLocationsRequest) (*pb.GetRelatedLocationsResponse, error) {
	if in.GetDcid() == "" || len(in.GetStatVarDcids()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required arguments")
	}
	if !util.CheckValidDCIDs([]string{in.GetDcid()}) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid DCID")
	}

	sameAncestor := (in.GetWithinPlace() != "")
	isPerCapita := in.GetIsPerCapita()
	prefix := RelatedLocationsPrefixMap[sameAncestor][isPerCapita]

	rowList := bigtable.RowList{}
	for _, statVarDcid := range in.GetStatVarDcids() {
		if sameAncestor {
			rowList = append(rowList, fmt.Sprintf(
				"%s%s^%s^%s", prefix, in.GetDcid(), in.GetWithinPlace(), statVarDcid))
		} else {
			rowList = append(rowList, fmt.Sprintf(
				"%s%s^%s", prefix, in.GetDcid(), statVarDcid))
		}
	}
	dataMap, err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var btRelatedPlacesInfo RelatedPlacesInfo
			err := json.Unmarshal(jsonRaw, &btRelatedPlacesInfo)
			if err != nil {
				return nil, err
			}
			return &btRelatedPlacesInfo, nil
		}, func(key string) (string, error) {
			parts := strings.Split(key, "^")
			if len(parts) <= 1 {
				return "", status.Errorf(codes.Internal, "Invalid bigtable row key %s", key)
			}
			return parts[len(parts)-1], nil
		})
	if err != nil {
		return nil, err
	}
	results := map[string]*RelatedPlacesInfo{}
	for statVarDcid, data := range dataMap {
		if data == nil {
			results[statVarDcid] = nil
		} else {
			results[statVarDcid] = data.(*RelatedPlacesInfo)
		}
	}
	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	return &pb.GetRelatedLocationsResponse{Payload: string(jsonRaw)}, nil
}

// GetLocationsRankings implements API for Mixer.GetLocationsRankings.
func (s *Server) GetLocationsRankings(ctx context.Context,
	in *pb.GetLocationsRankingsRequest) (*pb.GetLocationsRankingsResponse, error) {
	if in.GetPlaceType() == "" || len(in.GetStatVarDcids()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required arguments")
	}

	isPerCapita := in.GetIsPerCapita()
	sameAncestor := (in.GetWithinPlace() != "")
	prefix := RelatedLocationsPrefixMap[sameAncestor][isPerCapita]
	rowList := bigtable.RowList{}
	for _, statVarDcid := range in.GetStatVarDcids() {
		if sameAncestor {
			rowList = append(rowList, fmt.Sprintf(
				"%s%s^%s^%s^%s", prefix, "*", in.GetPlaceType(), in.GetWithinPlace(), statVarDcid))
		} else {
			rowList = append(rowList, fmt.Sprintf("%s%s^%s^%s", prefix, "*", in.GetPlaceType(), statVarDcid))
		}
	}
	dataMap, err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var btRelatedPlacesInfo pb.RelatedPlacesInfo
			err := protojson.Unmarshal(jsonRaw, &btRelatedPlacesInfo)
			if err != nil {
				return nil, err
			}
			return &btRelatedPlacesInfo, nil
		}, func(key string) (string, error) {
			parts := strings.Split(key, "^")
			if len(parts) <= 1 {
				return "", status.Errorf(codes.Internal, "Invalid bigtable row key %s", key)
			}
			return parts[len(parts)-1], nil
		})
	if err != nil {
		return nil, err
	}

	results := map[string]*pb.RelatedPlacesInfo{}
	for statVarDcid, data := range dataMap {
		if data == nil {
			results[statVarDcid] = nil
		} else {
			results[statVarDcid] = data.(*pb.RelatedPlacesInfo)
		}
	}
	return &pb.GetLocationsRankingsResponse{Payload: results}, nil
}

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
	// Find distinct statvars.
	set := map[string]bool{}
	for _, statVars := range places {
		for _, dcid := range statVars.GetStatVars() {
			_, ok := set[dcid]
			if ok {
				continue
			}
			set[dcid] = true
		}
	}
	// Store keys in a slice.
	statVars := &pb.StatVars{StatVars: make([]string, len(set))}
	i := 0
	for dcid := range set {
		statVars.StatVars[i] = dcid
		i++
	}
	return &pb.GetPlaceStatVarsUnionResponse{StatVars: statVars}, nil
}
