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

package placein

import (
	"context"

	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/protobuf/proto"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetPlacesIn implements API for Mixer.GetPlacesIn.
func GetPlacesIn(
	ctx context.Context,
	store *store.Store,
	parentPlaces []string,
	childPlaceType string,
) (map[string][]string, error) {
	if len(parentPlaces) == 0 || childPlaceType == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing required arguments")
	}
	if err := util.CheckValidDCIDs(parentPlaces); err != nil {
		return nil, err
	}
	result := map[string][]string{}

	if store.BtGroup != nil {
		// Place relations are from base geo imports. Only trust the base cache.
		btDataList, err := bigtable.Read(
			ctx,
			store.BtGroup,
			bigtable.BtPlacesInPrefix,
			[][]string{parentPlaces, {childPlaceType}},
			func(jsonRaw []byte) (interface{}, error) {
				var containedInPlaces pb.ContainedPlaces
				err := proto.Unmarshal(jsonRaw, &containedInPlaces)
				return containedInPlaces.Dcids, err
			},
		)
		if err != nil {
			return nil, err
		}
		processed := map[string]struct{}{}
		for _, parent := range parentPlaces {
			if _, ok := processed[parent]; ok {
				continue
			}
			// Go through (ordered) import groups one by one, stop when data is found.
			for _, btData := range btDataList {
				for _, row := range btData {
					if row.Parts[0] == parent {
						result[parent] = row.Data.([]string)
						processed[parent] = struct{}{}
						break
					}
				}
			}
		}
	}
	if sqldb.IsConnected(&store.SQLClient) {
		var rows []*sqldb.SubjectObject
		var err error

		if len(parentPlaces) == 1 && parentPlaces[0] == childPlaceType {
			// When ancestor == child (typically requested for non-place entities), get all entities of that type.
			rows, err = store.SQLClient.GetAllEntitiesOfType(ctx, childPlaceType)
			if err != nil {
				return nil, err
			}
		} else {
			// Only queries based on direct containedInPlace for now.
			// This could extend to more hops and even link with BT cache data, but that
			// might make it too complicated.
			// In custom DC, it's reasonable to ask user to provide direct containment
			// relation.
			rows, err = store.SQLClient.GetContainedInPlace(ctx, childPlaceType, parentPlaces)
			if err != nil {
				return nil, err
			}
		}
		for _, row := range rows {
			child, parent := row.SubjectID, row.ObjectID
			result[parent] = append(result[parent], child)
		}
	}
	return result, nil
}
