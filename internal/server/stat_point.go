// Copyright 2020 Google LLC
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dateCount struct {
	date  string
	count int
}

// GetStatValue implements API for Mixer.GetStatValue.
// Endpoint: /stat (/stat/value)
func (s *Server) GetStatValue(ctx context.Context, in *pb.GetStatValueRequest) (
	*pb.GetStatValueResponse, error) {
	place := in.GetPlace()
	statVar := in.GetStatVar()

	if place == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: place")
	}
	if statVar == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_var")
	}
	date := in.GetDate()
	filterProp := &ObsProp{
		Mmethod: in.GetMeasurementMethod(),
		Operiod: in.GetObservationPeriod(),
		Unit:    in.GetUnit(),
		Sfactor: in.GetScalingFactor(),
	}

	rowList, keyTokens := buildStatsKey([]string{place}, []string{statVar})
	var obsTimeSeries *ObsTimeSeries
	btData, err := readStats(ctx, s.store, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	obsTimeSeries = btData[place][statVar]
	if obsTimeSeries == nil {
		return nil, status.Errorf(
			codes.NotFound, "No data for %s, %s", place, statVar)
	}
	obsTimeSeries.SourceSeries = filterSeries(obsTimeSeries.SourceSeries, filterProp)
	result, err := getValueFromBestSource(obsTimeSeries, date)
	if err != nil {
		return nil, err
	}
	return &pb.GetStatValueResponse{Value: result}, nil
}

// GetStatSet implements API for Mixer.GetStatSet.
// Endpoint: /stat/set
func (s *Server) GetStatSet(ctx context.Context, in *pb.GetStatSetRequest) (
	*pb.GetStatSetResponse, error) {
	places := in.GetPlaces()
	statVars := in.GetStatVars()
	date := in.GetDate()

	if len(places) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: places")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_vars")
	}

	// Initialize result with stat vars and place dcids.
	result := &pb.GetStatSetResponse{
		Data: make(map[string]*pb.PlacePointStat),
	}
	for _, statVar := range statVars {
		result.Data[statVar] = &pb.PlacePointStat{
			Stat: make(map[string]*pb.PointStat),
		}
		for _, place := range places {
			result.Data[statVar].Stat[place] = nil
		}
	}

	rowList, keyTokens := buildStatsKey(places, statVars)
	cacheData, err := readStatsPb(ctx, s.store, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	for place, placeData := range cacheData {
		for statVar, data := range placeData {
			if data != nil {
				result.Data[statVar].Stat[place], err = getValueFromBestSourcePb(data, date)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return result, nil
}

// GetStatCollection implements API for Mixer.GetStatCollection.
// Endpoint: /stat/set/within-place
// Endpoint: /stat/collection
func (s *Server) GetStatCollection(
	ctx context.Context, in *pb.GetStatCollectionRequest) (
	*pb.GetStatCollectionResponse, error) {
	parentPlace := in.GetParentPlace()
	statVars := in.GetStatVars()
	childType := in.GetChildType()
	date := in.GetDate()

	if parentPlace == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: parent_place")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_vars")
	}
	if childType == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: child_type")
	}

	// Initialize result.
	result := &pb.GetStatCollectionResponse{
		Data: make(map[string]*pb.SourceSeries),
	}
	// Initialize with nil to help check if data is in mem-cache. The nil field
	// will be populated with empty pb.ObsCollection struct in the end.
	for _, sv := range statVars {
		result.Data[sv] = &pb.SourceSeries{}
	}

	// Mapping from stat var to a list of dates. Need to fetch cache data for any
	// <sv, date> pair.
	sv2dates := map[string][]string{}

	// If date is not given, get the latest dates and use them to fetch
	// stats.
	if date == "" {
		rowList, keyTokens := buildStatCollectionKey(parentPlace, childType, "", statVars)
		dateCache, err := readStatCollection(ctx, s.store, rowList, keyTokens)
		if err != nil {
			return nil, err
		}
		// Two dates are computed here. One is the latest date from all sources and
		// the other is the date corresponding to the most place entries.
		for sv, data := range dateCache {
			latestDate := dateCount{}
			mostCount := dateCount{}
			if data != nil {
				for _, cohort := range data.SourceCohorts {
					for date, c := range cohort.Val {
						count := int(c)
						if count > mostCount.count || (count == mostCount.count && date > mostCount.date) {
							mostCount = dateCount{date, count}
						}
						if date > latestDate.date || (date == latestDate.date && count >= latestDate.count) {
							latestDate = dateCount{date, count}
						}
					}
				}
			}
			// Lastest date is always needed.
			sv2dates[sv] = []string{latestDate.date}
			// The date with most place count is needed too.
			if mostCount.count > latestDate.count {
				sv2dates[sv] = append(sv2dates[sv], mostCount.date)
			}
		}
	} else {
		for _, sv := range statVars {
			sv2dates[sv] = []string{date}
		}
	}

	// Get a reverse sorted date list like: [2019, 2017, 2016-01]
	dateSet := map[string]struct{}{}
	for _, dates := range sv2dates {
		for _, date := range dates {
			dateSet[date] = struct{}{}
		}
	}
	dateList := []string{}
	for date := range dateSet {
		dateList = append(dateList, date)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dateList)))

	// Query the cache from the most recent date. The order is important as the
	// source with older date is only used when it has better place coverage.
	// TODO(shifucun): Parallel the BT query.
	for _, queryDate := range dateList {
		rowList, keyTokens := buildStatCollectionKey(
			parentPlace, childType, queryDate, statVars)
		statCache, err := readStatCollection(ctx, s.store, rowList, keyTokens)
		if err != nil {
			return nil, err
		}
		for sv, data := range statCache {
			if data != nil {
				for _, date := range sv2dates[sv] {
					// Only process when the query date is used for this stat var.
					if date != queryDate {
						continue
					}
					cohorts := data.SourceCohorts
					sort.Sort(SeriesByRank(cohorts))
					// Iterate from the ranked order and find the source with most places.
					maxSize := 0
					var usedCohort *pb.SourceSeries
					for _, cohort := range cohorts {
						currSize := len(cohort.Val)
						if currSize > maxSize {
							maxSize = currSize
							usedCohort = cohort
						}
					}
					// Add the cohort to result
					if result.Data[sv].Val == nil {
						result.Data[sv] = usedCohort
					} else {
						// If the result is populated already, then a latest observation
						// has been added. For this cohort (with an older date), only need
						// to add the places that is not present yet.

						// TODO(boxu): In this case, the result contains multiple sources,
						// need to reflect this in the result.
						for place := range usedCohort.Val {
							if val, ok := result.Data[sv].Val[place]; !ok {
								result.Data[sv].Val[place] = val
							}
						}
					}
				}
			}
		}
	}
	return result, nil
}
