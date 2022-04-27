// Copyright 2022 Google LLC
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
	if !util.CheckValidDCIDs(parentPlaces) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid DCIDs")
	}

	rowList := bigtable.BuildPlacesInKey(parentPlaces, childPlaceType)

	// Place relations are from base geo imports. Only trust the base cache.
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(jsonRaw []byte) (interface{}, error) {
			var containedInPlaces pb.ContainedPlaces
			err := proto.Unmarshal(jsonRaw, &containedInPlaces)
			return containedInPlaces.Dcids, err
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	result := map[string][]string{}
	processed := map[string]struct{}{}
	for _, parent := range parentPlaces {
		if _, ok := processed[parent]; ok {
			continue
		}
		// Go through (ordered) import groups one by one, stop when data is found.
		for _, baseData := range btDataList {
			if _, ok := baseData[parent]; !ok {
				continue
			}
			result[parent] = baseData[parent].([]string)
			processed[parent] = struct{}{}
			break
		}
	}
	return result, nil
}
