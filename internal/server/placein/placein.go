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
	"fmt"

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
	if store.SQLiteClient != nil {
		// Only queries based on direct containedInPlace for now.
		// This could extend to more hops and even link with BT cache data, but that
		// might make it too complicated.
		// In custom DC, it's reasonable to ask user to provide direct containment
		// relation.
		query := fmt.Sprintf(
			`
				SELECT t1.subject_id, t2.object_id
				FROM triples t1
				JOIN triples t2
				ON t1.subject_id = t2.subject_id
				WHERE t1.predicate = 'typeOf'
				AND t1.object_id = ?
				AND t2.predicate = 'containedInPlace'
				AND t2.object_id IN (%s);
			`,
			util.SQLInParam(len(parentPlaces)),
		)
		args := []string{childPlaceType}
		args = append(args, parentPlaces...)
		// Execute query
		rows, err := store.SQLiteClient.Query(query, util.ConvertArgs(args)...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var child, parent string
			err = rows.Scan(&child, &parent)
			if err != nil {
				return nil, err
			}
			result[parent] = append(result[parent], child)
		}
	}
	return result, nil
}
