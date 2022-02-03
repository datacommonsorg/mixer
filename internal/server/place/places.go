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

package place

import (
	"context"
	"fmt"
	"strings"

	"encoding/json"

	cbt "cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/protobuf/proto"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// GetChildPlaces fetches child places given parent place and child place type.
func GetChildPlaces(
	ctx context.Context, s *store.Store, parentPlace string, childType string) (
	[]string, error,
) {
	rowList := bigtable.BuildPlaceInKey([]string{parentPlace}, childType)
	// Place relations are from base geo imports. Only trust the base cache.
	baseDataList, _, err := bigtable.Read(
		ctx,
		s.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
			if isProto {
				var containedInPlaces pb.ContainedPlaces
				err := proto.Unmarshal(jsonRaw, &containedInPlaces)
				return containedInPlaces.Dcids, err
			} else {
				return strings.Split(string(jsonRaw), ","), nil
			}
		},
		nil,
		false, /* readBranch */
	)
	if err != nil {
		return []string{}, err
	}
	if baseDataList[0][parentPlace] != nil {
		return baseDataList[0][parentPlace].([]string), nil
	}
	return []string{}, err
}

// GetPlacesIn implements API for Mixer.GetPlacesIn.
func GetPlacesIn(ctx context.Context, in *pb.GetPlacesInRequest, store *store.Store) (
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
	baseDataList, _, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
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
		if baseDataList[0][dcid] != nil {
			for _, place := range baseDataList[0][dcid].([]string) {
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
func GetRelatedLocations(
	ctx context.Context,
	in *pb.GetRelatedLocationsRequest,
	store *store.Store,
) (*pb.GetRelatedLocationsResponse, error) {
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
	baseDataList, _, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
			var btRelatedPlacesInfo pb.RelatedPlacesInfo
			if isProto {
				if err := proto.Unmarshal(jsonRaw, &btRelatedPlacesInfo); err != nil {
					return nil, err
				}
			} else {
				if err := protojson.Unmarshal(jsonRaw, &btRelatedPlacesInfo); err != nil {
					return nil, err
				}
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
	result := &pb.GetRelatedLocationsResponse{Data: map[string]*pb.RelatedPlacesInfo{}}
	for statVar, data := range baseDataList[0] {
		if data == nil {
			result.Data[statVar] = nil
		} else {
			result.Data[statVar] = data.(*pb.RelatedPlacesInfo)
		}
	}
	return result, nil
}

// GetLocationsRankings implements API for Mixer.GetLocationsRankings.
func GetLocationsRankings(
	ctx context.Context,
	in *pb.GetLocationsRankingsRequest,
	store *store.Store,
) (*pb.GetLocationsRankingsResponse, error) {
	if in.GetPlaceType() == "" || len(in.GetStatVarDcids()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required arguments")
	}

	isPerCapita := in.GetIsPerCapita()
	sameAncestor := (in.GetWithinPlace() != "")
	prefix := RelatedLocationsPrefixMap[sameAncestor][isPerCapita]
	rowList := cbt.RowList{}
	for _, statVarDcid := range in.GetStatVarDcids() {
		if sameAncestor {
			rowList = append(
				rowList,
				fmt.Sprintf(
					"%s%s^%s^%s^%s",
					prefix,
					"*",
					in.GetPlaceType(),
					in.GetWithinPlace(),
					statVarDcid,
				),
			)
		} else {
			rowList = append(
				rowList,
				fmt.Sprintf(
					"%s%s^%s^%s",
					prefix,
					"*",
					in.GetPlaceType(),
					statVarDcid,
				),
			)
		}
	}
	// RelatedPlace cache only exists in base cache
	baseDataList, _, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
			var btRelatedPlacesInfo pb.RelatedPlacesInfo
			if isProto {
				if err := proto.Unmarshal(jsonRaw, &btRelatedPlacesInfo); err != nil {
					return nil, err
				}
			} else {
				if err := protojson.Unmarshal(jsonRaw, &btRelatedPlacesInfo); err != nil {
					return nil, err
				}
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

	result := &pb.GetLocationsRankingsResponse{Data: map[string]*pb.RelatedPlacesInfo{}}
	for statVar, data := range baseDataList[0] {
		if data == nil {
			result.Data[statVar] = nil
		} else {
			result.Data[statVar] = data.(*pb.RelatedPlacesInfo)
		}
	}
	return result, nil
}

// GetPlaceMetadata implements API for Mixer.GetPlaceMetadata.
func GetPlaceMetadata(
	ctx context.Context,
	in *pb.GetPlaceMetadataRequest,
	store *store.Store,
) (*pb.GetPlaceMetadataResponse, error) {
	places := in.GetPlaces()
	if len(places) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments: places")
	}

	rowList := bigtable.BuildPlaceMetaDataKey(places)
	// Place metadata are from base geo imports. Only trust the base cache.
	baseDataList, _, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
			var data pb.PlaceMetadataCache
			if isProto {
				if err := proto.Unmarshal(jsonRaw, &data); err != nil {
					return nil, err
				}
			} else {
				if err := protojson.Unmarshal(jsonRaw, &data); err != nil {
					return nil, err
				}
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
		if baseDataList[0][place] == nil {
			continue
		}
		raw := baseDataList[0][place].(*pb.PlaceMetadataCache)
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
