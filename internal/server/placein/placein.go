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

package placein

import (
	"context"
	"strings"

	"encoding/json"

	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"google.golang.org/protobuf/proto"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetChildPlaces fetches child places given parent place and child place type.
func GetChildPlaces(
	ctx context.Context, s *store.Store, parentPlace string, childType string) (
	[]string, error,
) {
	rowList := bigtable.BuildPlacesInKey([]string{parentPlace}, childType)
	// Place relations are from base geo imports. Only trust the base cache.
	btDataList, err := bigtable.Read(
		ctx,
		s.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
			if isProto {
				var containedInPlaces pb.ContainedPlaces
				err := proto.Unmarshal(jsonRaw, &containedInPlaces)
				return containedInPlaces.Dcids, err
			}
			return strings.Split(string(jsonRaw), ","), nil
		},
		nil,
	)
	if err != nil {
		return []string{}, err
	}
	if btDataList[0][parentPlace] != nil {
		return btDataList[0][parentPlace].([]string), nil
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

	rowList := bigtable.BuildPlacesInKey(dcids, placeType)

	// Place relations are from base geo imports. Only trust the base cache.
	btDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		rowList,
		func(dcid string, jsonRaw []byte, isProto bool) (interface{}, error) {
			if isProto {
				var containedInPlaces pb.ContainedPlaces
				err := proto.Unmarshal(jsonRaw, &containedInPlaces)
				return containedInPlaces.Dcids, err
			}
			return strings.Split(string(jsonRaw), ","), nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	results := []map[string]string{}
	processed := map[string]struct{}{}
	for _, dcid := range dcids {
		if _, ok := processed[dcid]; ok {
			continue
		}

		// Go through (ordered) import groups one by one, stop when data is found.
		for _, baseData := range btDataList {
			if _, ok := baseData[dcid]; !ok {
				continue
			}

			for _, place := range baseData[dcid].([]string) {
				results = append(results, map[string]string{"dcid": dcid, "place": place})
			}
			processed[dcid] = struct{}{}
			break
		}
	}

	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return nil, err
	}
	return &pb.GetPlacesInResponse{Payload: string(jsonRaw)}, nil
}
