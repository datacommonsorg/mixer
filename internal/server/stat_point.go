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
		Data: make(map[string]*pb.PlacePointStat),
	}
	// A map from statvar to import name to StatMetadata
	sv2Meta := map[string]map[string]*pb.StatMetadata{}

	for _, sv := range statVars {
		result.Data[sv] = &pb.PlacePointStat{Stat: map[string]*pb.PointStat{}}
		sv2Meta[sv] = map[string]*pb.StatMetadata{}
	}

	// Mapping from stat var to a list of dates. Need to fetch cache data for any
	// <sv, date> pair.
	sv2dates := map[string][]string{}

	// A set of dates to query for
	dateSet := map[string]struct{}{}

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
			if data == nil {
				continue
			}
			// TODO(shifucun): After measurement method is cleaned up, all the cohorts
			// should be grouped based on StatVarObs prorperties. Each group can be
			// merged, and the count should be considered within each group.
			latestDate := dateCount{}
			mostCount := dateCount{}
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
			// Latest date is always needed.
			sv2dates[sv] = []string{latestDate.date}
			dateSet[latestDate.date] = struct{}{}

			// The date with most place count is needed too.
			if mostCount.count > latestDate.count {
				sv2dates[sv] = append(sv2dates[sv], mostCount.date)
				dateSet[mostCount.date] = struct{}{}
			}
		}
	} else {
		dateSet[date] = struct{}{}
		for _, sv := range statVars {
			sv2dates[sv] = []string{date}
		}
	}

	dateList := []string{}
	for date := range dateSet {
		dateList = append(dateList, date)
	}
	sort.Strings(dateList)

	// Query the cache from older to newer date. The newer stat, when present,
	// can override the older stat.
	// TODO(shifucun): Parallel the BT query.
	for _, queryDate := range dateList {
		// Get the StatVars that has data for this date.
		queryStatVars := []string{}
		for sv, dates := range sv2dates {
			for _, date := range dates {
				if date == queryDate {
					queryStatVars = append(queryStatVars, sv)
				}
			}
		}
		rowList, keyTokens := buildStatCollectionKey(
			parentPlace, childType, queryDate, queryStatVars)
		statCache, err := readStatCollection(ctx, s.store, rowList, keyTokens)
		if err != nil {
			return nil, err
		}
		for sv, data := range statCache {
			if data == nil {
				continue
			}
			cohorts := data.SourceCohorts
			sort.Sort(CohortByRank(cohorts))

			// Pick the highest ranked cohort from different sources for THIS date.
			//
			// NOTE: For Count_Person_Employed (and some other stat var), there are
			// two source cohorts with different measurement method that are not
			// compatible (BLSSeasonallyUnadjusted, BLSSeasonallyAdjusted). The logic
			// here will deterministically pick one cohort for the current date. Since
			// these observation have same place and date coverage, only the latest
			// date is processed, hence no need to worry about merging.
			cohort := cohorts[0]

			// Add the cohort to result.
			for place, val := range cohort.Val {
				result.Data[sv].Stat[place] = &pb.PointStat{
					Date:  queryDate,
					Value: val,
					Metadata: &pb.StatMetadata{
						ImportName: cohort.ImportName,
					},
				}
			}
			if _, ok := sv2Meta[sv][cohort.ImportName]; !ok {
				sv2Meta[sv][cohort.ImportName] = &pb.StatMetadata{
					ProvenanceUrl:     cohort.ProvenanceUrl,
					MeasurementMethod: cohort.MeasurementMethod,
					ObservationPeriod: cohort.ObservationPeriod,
					ScalingFactor:     cohort.ScalingFactor,
					Unit:              cohort.Unit,
				}
			}
		}
	}
	for sv := range sv2Meta {
		result.Data[sv].Metadata = sv2Meta[sv]
	}
	return result, nil
}
