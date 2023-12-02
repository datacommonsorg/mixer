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

	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/protobuf/proto"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	if err := util.CheckValidDCIDs([]string{in.GetDcid()}); err != nil {
		return nil, err
	}

	sameAncestor := (in.GetWithinPlace() != "")
	isPerCapita := in.GetIsPerCapita()
	prefix := RelatedLocationsPrefixMap[sameAncestor][isPerCapita]

	keyBody := [][]string{{in.GetDcid()}}
	if sameAncestor {
		keyBody = append(keyBody, []string{in.GetWithinPlace()})
	}
	keyBody = append(keyBody, in.GetStatVarDcids())
	// RelatedPlace cache only exists in base cache
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		prefix,
		keyBody,
		func(jsonRaw []byte) (interface{}, error) {
			var btRelatedPlacesInfo pb.RelatedPlacesInfo
			if err := proto.Unmarshal(jsonRaw, &btRelatedPlacesInfo); err != nil {
				return nil, err
			}
			return &btRelatedPlacesInfo, nil
		},
	)
	if err != nil {
		return nil, err
	}
	result := &pb.GetRelatedLocationsResponse{Data: map[string]*pb.RelatedPlacesInfo{}}
	for _, btData := range btDataList {
		for _, row := range btData {
			var statVar string
			if sameAncestor {
				statVar = row.Parts[2]
			} else {
				statVar = row.Parts[1]
			}
			if _, ok := result.Data[statVar]; ok {
				continue
			}
			if row.Data == nil {
				result.Data[statVar] = &pb.RelatedPlacesInfo{}
				continue
			}
			result.Data[statVar] = row.Data.(*pb.RelatedPlacesInfo)
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
	keyBody := [][]string{{"*"}, {in.GetPlaceType()}}
	if sameAncestor {
		keyBody = append(keyBody, []string{in.GetWithinPlace()})
	}
	keyBody = append(keyBody, in.GetStatVarDcids())
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		prefix,
		keyBody,
		func(jsonRaw []byte) (interface{}, error) {
			var btRelatedPlacesInfo pb.RelatedPlacesInfo
			if err := proto.Unmarshal(jsonRaw, &btRelatedPlacesInfo); err != nil {
				return nil, err
			}
			return &btRelatedPlacesInfo, nil
		},
	)
	if err != nil {
		return nil, err
	}

	result := &pb.GetLocationsRankingsResponse{Data: map[string]*pb.RelatedPlacesInfo{}}
	for _, btData := range btDataList {
		for _, row := range btData {
			var statVar string
			if sameAncestor {
				statVar = row.Parts[3]
			} else {
				statVar = row.Parts[2]
			}
			if _, ok := result.Data[statVar]; ok {
				// Temporary hack to handle ranking for Count_Person so that we pick the
				// most complete ranking
				if statVar == "Count_Person" && row.Data != nil {
					currRankAll := result.Data[statVar].GetRankAll()
					newRankAll := row.Data.(*pb.RelatedPlacesInfo).GetRankAll()
					// If there is no ranking information in the new row, continue
					if newRankAll == nil || newRankAll.GetInfo() == nil {
						continue
					}
					// if there is no ranking information in the current row, use the new
					// row
					if currRankAll == nil || currRankAll.GetInfo() == nil {
						result.Data[statVar] = row.Data.(*pb.RelatedPlacesInfo)
					}
					// if there is more ranking information in the new row, use the new
					// row
					if len(newRankAll.GetInfo()) > len(currRankAll.GetInfo()) {
						result.Data[statVar] = row.Data.(*pb.RelatedPlacesInfo)
					}
				}
				continue
			}
			if row.Data == nil {
				result.Data[statVar] = &pb.RelatedPlacesInfo{}
				continue
			}
			result.Data[statVar] = row.Data.(*pb.RelatedPlacesInfo)
		}
	}
	return result, nil
}

// GetPlaceMetadataHelper is a wrapper to get place metadata.
func GetPlaceMetadataHelper(
	ctx context.Context,
	entities []string,
	store *store.Store,
) (map[string]*pb.PlaceMetadata, error) {
	// Place metadata are from base geo imports. Only trust the base cache.
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		bigtable.BtPlacesMetadataPrefix,
		[][]string{entities},
		func(jsonRaw []byte) (interface{}, error) {
			var data pb.PlaceMetadataCache
			if err := proto.Unmarshal(jsonRaw, &data); err != nil {
				return nil, err
			}
			return &data, nil
		},
	)
	if err != nil {
		return nil, err
	}
	result := map[string]*pb.PlaceMetadata{}
	infoMap := map[string]*pb.PlaceMetadata_PlaceInfo{}
	parentMap := map[string]map[string]struct{}{}
	directParents := map[string][]string{}
	for _, btData := range btDataList {
		for _, row := range btData {
			raw, ok := row.Data.(*pb.PlaceMetadataCache)
			if !ok {
				continue
			}
			currPlace := row.Parts[0]
			if _, ok := result[currPlace]; !ok {
				result[currPlace] = &pb.PlaceMetadata{}
			}
			if _, ok := parentMap[currPlace]; !ok {
				parentMap[currPlace] = map[string]struct{}{}
			}
			for _, place := range raw.Places {
				if _, ok := infoMap[place.Dcid]; !ok {
					infoMap[place.Dcid] = &pb.PlaceMetadata_PlaceInfo{
						Dcid: place.Dcid,
						Name: place.Name,
						Type: place.Type,
					}
				}
				directParents[place.Dcid] = place.Parents
				if place.Dcid == currPlace {
					result[currPlace].Self = infoMap[currPlace]
				} else {
					parentMap[currPlace][place.Dcid] = struct{}{}
				}
			}
		}
	}
	for dcid, allParents := range parentMap {
		// Order direct parents at the beginning of the list.
		for _, parent := range directParents[dcid] {
			result[dcid].Parents = append(result[dcid].Parents, infoMap[parent])
			delete(allParents, parent)
		}
		// Order grand parents after direct parents.
		for _, parent := range directParents[dcid] {
			for _, grandParent := range directParents[parent] {
				if _, ok := allParents[grandParent]; !ok {
					continue
				}
				result[dcid].Parents = append(result[dcid].Parents, infoMap[grandParent])
				delete(allParents, grandParent)
			}
		}
		for parent := range allParents {
			result[dcid].Parents = append(result[dcid].Parents, infoMap[parent])
		}
	}
	return result, nil
}
