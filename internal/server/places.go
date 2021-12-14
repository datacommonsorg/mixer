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

	cbt "cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func getChildPlaces(
	ctx context.Context, s *store.Store, parentPlace string, childType string) (
	[]string, error,
) {
	rowList := bigtable.BuildPlaceInKey([]string{parentPlace}, childType)
	// Place relations are from base geo imports. Only trust the base cache.
	baseDataMap, _, err := bigtable.Read(
		ctx,
		s.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			return strings.Split(string(jsonRaw), ","), nil
		},
		nil,
		false, /* readBranch */
	)
	if err != nil {
		return []string{}, err
	}
	if baseDataMap[parentPlace] != nil {
		return baseDataMap[parentPlace].([]string), nil
	}
	return []string{}, err
}

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

	rowList := bigtable.BuildPlaceInKey(dcids, placeType)

	// Place relations are from base geo imports. Only trust the base cache.
	baseDataMap, _, err := bigtable.Read(
		ctx,
		s.store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			return strings.Split(string(jsonRaw), ","), nil
		},
		nil,
		false, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	results := []map[string]string{}
	for _, dcid := range dcids {
		if baseDataMap[dcid] != nil {
			for _, place := range baseDataMap[dcid].([]string) {
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
		true:  bigtable.BtRelatedLocationsSameTypeAndAncestorPCPrefix,
		false: bigtable.BtRelatedLocationsSameTypeAndAncestorPrefix,
	},
	false: {
		true:  bigtable.BtRelatedLocationsSameTypePCPrefix,
		false: bigtable.BtRelatedLocationsSameTypePrefix,
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

	rowList := cbt.RowList{}
	for _, statVarDcid := range in.GetStatVarDcids() {
		if sameAncestor {
			rowList = append(rowList, fmt.Sprintf(
				"%s%s^%s^%s", prefix, in.GetDcid(), in.GetWithinPlace(), statVarDcid))
		} else {
			rowList = append(rowList, fmt.Sprintf(
				"%s%s^%s", prefix, in.GetDcid(), statVarDcid))
		}
	}
	// RelatedPlace cache only exists in base cache
	baseDataMap, _, err := bigtable.Read(
		ctx,
		s.store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var btRelatedPlacesInfo RelatedPlacesInfo
			err := json.Unmarshal(jsonRaw, &btRelatedPlacesInfo)
			if err != nil {
				return nil, err
			}
			return &btRelatedPlacesInfo, nil
		},
		func(key string) (string, error) {
			parts := strings.Split(key, "^")
			if len(parts) <= 1 {
				return "", status.Errorf(
					codes.Internal, "Invalid bigtable row key %s", key)
			}
			return parts[len(parts)-1], nil
		},
		false, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	results := map[string]*RelatedPlacesInfo{}
	for statVarDcid, data := range baseDataMap {
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
	rowList := cbt.RowList{}
	for _, statVarDcid := range in.GetStatVarDcids() {
		if sameAncestor {
			rowList = append(rowList, fmt.Sprintf(
				"%s%s^%s^%s^%s", prefix, "*", in.GetPlaceType(), in.GetWithinPlace(), statVarDcid))
		} else {
			rowList = append(rowList, fmt.Sprintf("%s%s^%s^%s", prefix, "*", in.GetPlaceType(), statVarDcid))
		}
	}
	// RelatedPlace cache only exists in base cache
	baseDataMap, _, err := bigtable.Read(
		ctx,
		s.store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var btRelatedPlacesInfo pb.RelatedPlacesInfo
			err := protojson.Unmarshal(jsonRaw, &btRelatedPlacesInfo)
			if err != nil {
				return nil, err
			}
			return &btRelatedPlacesInfo, nil
		},
		func(key string) (string, error) {
			parts := strings.Split(key, "^")
			if len(parts) <= 1 {
				return "", status.Errorf(
					codes.Internal, "Invalid bigtable row key %s", key)
			}
			return parts[len(parts)-1], nil
		},
		false, /* readBranch */
	)
	if err != nil {
		return nil, err
	}

	results := map[string]*pb.RelatedPlacesInfo{}
	for statVarDcid, data := range baseDataMap {
		if data == nil {
			results[statVarDcid] = nil
		} else {
			results[statVarDcid] = data.(*pb.RelatedPlacesInfo)
		}
	}
	return &pb.GetLocationsRankingsResponse{Payload: results}, nil
}

// GetPlaceMetadata implements API for Mixer.GetPlaceMetadata.
func (s *Server) GetPlaceMetadata(ctx context.Context, in *pb.GetPlaceMetadataRequest) (
	*pb.GetPlaceMetadataResponse, error) {
	places := in.GetPlaces()

	if len(places) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: places")
	}

	rowList := bigtable.BuildPlaceMetaDataKey(places)

	// Place metadata are from base geo imports. Only trust the base cache.
	baseDataMap, _, err := bigtable.Read(
		ctx,
		s.store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte) (interface{}, error) {
			var data pb.PlaceMetadataCache
			err := json.Unmarshal(jsonRaw, &data)
			if err != nil {
				return nil, err
			}
			return &data, nil
		},
		nil,
		false, /* readBranch */
	)
	if err != nil {
		return nil, err
	}
	result := map[string]*pb.PlaceMetadata{}
	for _, place := range places {
		if baseDataMap[place] == nil {
			continue
		}
		raw := baseDataMap[place].(*pb.PlaceMetadataCache)
		processed := pb.PlaceMetadata{}
		metaMap := map[string]*pb.PlaceMetadataCache_PlaceInfo{}
		for _, info := range raw.Places {
			metaMap[info.Dcid] = info
		}
		processed.Self = &pb.PlaceMetadata_PlaceInfo{
			Dcid: place,
			Name: metaMap[place].Name,
			Type: metaMap[place].Type,
		}
		visited := map[string]struct{}{}
		parents := metaMap[place].Parents
		for {
			if len(parents) == 0 {
				break
			}
			curr := parents[0]
			parents = parents[1:]
			if _, ok := visited[curr]; ok {
				continue
			}
			processed.Parents = append(processed.Parents, &pb.PlaceMetadata_PlaceInfo{
				Dcid: curr,
				Name: metaMap[curr].Name,
				Type: metaMap[curr].Type,
			})
			visited[curr] = struct{}{}
			parents = append(parents, metaMap[curr].Parents...)
		}
		result[place] = &processed
	}
	return &pb.GetPlaceMetadataResponse{Data: result}, nil
}
