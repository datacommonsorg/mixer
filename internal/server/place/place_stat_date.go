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
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/ranking"
	"github.com/datacommonsorg/mixer/internal/server/stat"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetPlaceStatDateWithinPlace implements API for Mixer.GetPlaceStatDateWithinPlace.
func GetPlaceStatDateWithinPlace(
	ctx context.Context, in *pb.GetPlaceStatDateWithinPlaceRequest, store *store.Store) (
	*pb.GetPlaceStatDateWithinPlaceResponse, error) {
	ancestorPlace := in.GetAncestorPlace()
	placeType := in.GetPlaceType()
	statVars := in.GetStatVars()
	if ancestorPlace == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: ancestorPlace")
	}
	if placeType == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: place_place")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_vars")
	}

	// Initialize result.
	result := &pb.GetPlaceStatDateWithinPlaceResponse{
		Data: make(map[string]*pb.DateList),
	}
	// Initialize with nil to help check if data is in mem-cache. The nil field
	// will be populated with empty pb.ObsCollection struct in the end.
	for _, sv := range statVars {
		result.Data[sv] = nil
	}

	// Construct BigTable row keys.
	rowList, keyTokens := bigtable.BuildObsCollectionDateFrequencyKey(
		ancestorPlace, placeType, statVars)

	cacheData, err := stat.ReadStatCollection(ctx, store.BtGroup, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	for sv, data := range cacheData {
		if data != nil && len(data.SourceCohorts) > 0 {
			cohorts := data.SourceCohorts
			sort.Sort(ranking.SeriesByRank(cohorts))
			dates := []string{}
			for date := range cohorts[0].Val {
				dates = append(dates, date)
			}
			sort.Strings(dates)
			result.Data[sv] = &pb.DateList{Dates: dates}
		}
	}

	for sv := range result.Data {
		if result.Data[sv] == nil {
			result.Data[sv] = &pb.DateList{}
		}
	}
	return result, nil
}
