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

package stat

import (
	"context"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetStatDateWithinPlace implements API for Mixer.GetStatDateWithinPlace.
func GetStatDateWithinPlace(
	ctx context.Context, in *pb.GetStatDateWithinPlaceRequest, store *store.Store) (
	*pb.GetStatDateWithinPlaceResponse, error) {
	ancestorPlace := in.GetAncestorPlace()
	childPlaceType := in.GetChildPlaceType()
	statVars := in.GetStatVars()
	if ancestorPlace == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: ancestor_place")
	}
	if childPlaceType == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: child_place_type")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_vars")
	}

	// Initialize result.
	result := &pb.GetStatDateWithinPlaceResponse{
		Data: make(map[string]*pb.StatDateList),
	}
	for _, sv := range statVars {
		result.Data[sv] = &pb.StatDateList{}
	}
	cacheData, err := ReadStatCollection(
		ctx, store.BtGroup,
		bigtable.BtObsCollectionDateFrequency,
		ancestorPlace,
		childPlaceType,
		statVars,
		"",
	)
	if err != nil {
		return nil, err
	}
	for sv, data := range cacheData {
		if data != nil && len(data.SourceCohorts) > 0 {
			for _, corhort := range data.SourceCohorts {
				statDate := &pb.StatDate{
					DatePlaceCount: corhort.Val,
					Metadata:       util.GetMetadata(corhort),
				}
				result.Data[sv].StatDate = append(result.Data[sv].StatDate, statDate)
			}
		}
	}

	for sv := range result.Data {
		if result.Data[sv].StatDate == nil {
			// Fetch from memdb.
			if store.MemDb.HasStatVar(sv) {
				result.Data[sv] = store.MemDb.ReadStatDate(sv)
			}
		}
	}
	return result, nil
}
