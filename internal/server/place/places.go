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
)

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
		childPlaces := []string{dcid}
		for len(childPlaces) > 0 {
			newChildPlaces := []string{}
			for _, child := range childPlaces {
				for _, parent := range directParents[child] {
					if _, ok := allParents[parent]; !ok {
						continue
					}
					result[dcid].Parents = append(result[dcid].Parents, infoMap[parent])
					newChildPlaces = append(newChildPlaces, parent)
					delete(allParents, parent)
				}
			}
			childPlaces = newChildPlaces
		}
		for parent := range allParents {
			result[dcid].Parents = append(result[dcid].Parents, infoMap[parent])
		}
	}
	return result, nil
}
