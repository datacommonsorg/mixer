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
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"

	"cloud.google.com/go/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetPlaceStatDateWithinPlace implements API for Mixer.GetPlaceStatDateWithinPlace.
// Endpoint: /place/stat/date/within-place
func (s *Server) GetPlaceStatDateWithinPlace(
	ctx context.Context, in *pb.GetPlaceStatDateWithinPlaceRequest) (
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

	// Read triples for statistical variable.
	triplesRowList := buildTriplesKey(statVars)
	triples, err := readTriples(ctx, s.btTable, triplesRowList)
	if err != nil {
		return nil, err
	}
	statVarObject := map[string]*StatisticalVariable{}
	for statVar, triplesCache := range triples {
		if triplesCache != nil {
			statVarObject[statVar], err = triplesToStatsVar(statVar, triplesCache)
			if err != nil {
				return nil, err
			}
		}
	}
	// Construct BigTable row keys.
	rowList, keyTokens := buildStatCollectionKey(ancestorPlace, placeType, "", statVarObject)
	// Read data from branch in-memory cache first.
	cacheData := s.memcache.ReadParallel(
		rowList,
		convertToObsCollection,
		func(rowKey string) (string, error) {
			return keyTokens[rowKey], nil
		},
	)
	for token, data := range cacheData {
		if data != nil {
			cohorts := data.(*pb.ObsCollection).SourceCohorts
			sort.Sort(SeriesByRank(cohorts))
			dates := []string{}
			for date := range cohorts[0].Val {
				dates = append(dates, date)
			}
			sort.Strings(dates)
			result.Data[token] = &pb.DateList{Dates: dates}
		}
	}
	// Get row keys that are not in mem-cache.
	extraRowList := bigtable.RowList{}
	for key, token := range keyTokens {
		if result.Data[token] == nil {
			extraRowList = append(extraRowList, key)
		}
	}

	if len(extraRowList) > 0 {
		extraData, err := readStatCollection(ctx, s.btTable, extraRowList, keyTokens)
		if err != nil {
			return nil, err
		}
		for sv, data := range extraData {
			if data != nil {
				cohorts := data.SourceCohorts
				sort.Sort(SeriesByRank(cohorts))
				dates := []string{}
				for date := range cohorts[0].Val {
					dates = append(dates, date)
				}
				sort.Strings(dates)
				result.Data[sv] = &pb.DateList{Dates: dates}
			}
		}
	}
	for sv := range result.Data {
		if result.Data[sv] == nil {
			result.Data[sv] = &pb.DateList{}
		}
	}
	return result, nil
}
