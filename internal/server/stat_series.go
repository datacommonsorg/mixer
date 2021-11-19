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
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func logIfDurationTooLong(t time.Time, thresholdSec float32, msg string) {
	duration := time.Since(t).Seconds()
	if duration > float64(thresholdSec) {
		log.Printf("%f seconds:\n%s", duration, msg)
	}
}

// GetStatSeries implements API for Mixer.GetStatSeries.
// Endpoint: /stat/series
// TODO(shifucun): consilidate and dedup the logic among these similar APIs.
func (s *Server) GetStatSeries(
	ctx context.Context, in *pb.GetStatSeriesRequest) (
	*pb.GetStatSeriesResponse, error) {
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
	filterProp := &ObsProp{
		Mmethod: in.GetMeasurementMethod(),
		Operiod: in.GetObservationPeriod(),
		Unit:    in.GetUnit(),
		Sfactor: in.GetScalingFactor(),
	}

	rowList, keyTokens := buildStatsKey([]string{place}, []string{statVar})
	btData, err := readStats(ctx, s.store, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	obsTimeSeries := btData[place][statVar]
	if obsTimeSeries == nil {
		return nil, status.Errorf(codes.NotFound,
			"No data for %s, %s", place, statVar)
	}
	series := obsTimeSeries.SourceSeries
	series = filterSeries(series, filterProp)
	sort.Sort(byRank(series))
	resp := pb.GetStatSeriesResponse{Series: map[string]float64{}}
	if len(series) > 0 {
		resp.Series = series[0].Val
	}
	return &resp, nil
}

// GetStatAll implements API for Mixer.GetStatAll.
// Endpoint: /stat/set/series/all
// Endpoint: /stat/all
func (s *Server) GetStatAll(ctx context.Context, in *pb.GetStatAllRequest) (
	*pb.GetStatAllResponse, error) {

	places := in.GetPlaces()
	statVars := in.GetStatVars()
	if len(places) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: place")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_var")
	}

	// Initialize result with place and stat var dcids.
	result := &pb.GetStatAllResponse{
		PlaceData: make(map[string]*pb.PlaceStat),
	}
	for _, place := range places {
		result.PlaceData[place] = &pb.PlaceStat{
			StatVarData: make(map[string]*pb.ObsTimeSeries),
		}
		for _, statVar := range statVars {
			result.PlaceData[place].StatVarData[statVar] = nil
		}
	}

	rowList, keyTokens := buildStatsKey(places, statVars)
	cacheData, err := readStatsPb(ctx, s.store, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	for place, placeData := range cacheData {
		for statVar, data := range placeData {
			if data != nil && data.SourceSeries != nil {
				sort.Sort(SeriesByRank(data.SourceSeries))
			}
			result.PlaceData[place].StatVarData[statVar] = data
		}
	}
	return result, nil
}

// GetStats implements API for Mixer.GetStats.
// Endpoint: /stat/set/series
// Endpoint: /bulk/stats
func (s *Server) GetStats(ctx context.Context, in *pb.GetStatsRequest) (
	*pb.GetStatsResponse, error) {
	ts := time.Now()
	placeDcids := in.GetPlace()
	statsVarDcid := in.GetStatsVar()
	defer logIfDurationTooLong(
		ts,
		30,
		fmt.Sprintf(
			"GetStats(): placeDcids: %s, statsVarDcid: %v", placeDcids, statsVarDcid),
	)

	if len(placeDcids) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: place")
	}
	if statsVarDcid == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: stat_var")
	}
	filterProp := &ObsProp{
		Mmethod: in.GetMeasurementMethod(),
		Operiod: in.GetObservationPeriod(),
		Unit:    in.GetUnit(),
	}
	var rowList bigtable.RowList
	var keyTokens map[string]*placeStatVar
	rowList, keyTokens = buildStatsKey(placeDcids, []string{statsVarDcid})

	result := map[string]*ObsTimeSeries{}
	cacheData, err := readStats(ctx, s.store, rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	for place := range cacheData {
		result[place] = cacheData[place][statsVarDcid]
	}

	// Fill missing place data and result result
	for _, dcid := range placeDcids {
		if _, ok := result[dcid]; !ok {
			result[dcid] = nil
		}
	}
	for _, obsSeries := range result {
		obsSeries.filterAndRank(filterProp)
	}
	jsonRaw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &pb.GetStatsResponse{Payload: string(jsonRaw)}, nil
}

// GetStatSetSeries implements API for Mixer.GetStatSetSeries.
// Endpoint: /v1/stat/set/series
func (s *Server) GetStatSetSeries(ctx context.Context, in *pb.GetStatSetSeriesRequest) (
	*pb.GetStatSetSeriesResponse, error) {
	places := in.GetPlaces()
	statVars := in.GetStatVars()
	importName := in.GetImportName()
	if len(places) == 0 {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: places")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: stat_vars")
	}

	// Initialize result with place and stat var dcids.
	result := &pb.GetStatSetSeriesResponse{
		Data: make(map[string]*pb.SeriesMap),
	}
	for _, place := range places {
		result.Data[place] = &pb.SeriesMap{
			Data: make(map[string]*pb.Series),
		}
		for _, statVar := range statVars {
			result.Data[place].Data[statVar] = nil
		}
	}
	// Read data from Cloud Bigtable.
	if s.store.BaseBt() != nil {
		rowList, keyTokens := buildStatsKey(places, statVars)

		// Read data from BigTable.
		cacheData, err := readStatsPb(ctx, s.store, rowList, keyTokens)
		if err != nil {
			return nil, err
		}
		for place, placeData := range cacheData {
			for statVar, data := range placeData {
				if data != nil {
					series, _ := getBestSeries(data, importName, false /* useLatest */)
					result.Data[place].Data[statVar] = series
				}
			}
		}
	}
	// Read data from in-memory cache (private data).
	// When there is data in both BigTable and private data. Prefer private data
	// as this instance is for a private DC.
	if !s.store.MemDb.IsEmpty() {
		for _, place := range places {
			for _, statVar := range statVars {
				series := s.store.MemDb.ReadSeries(statVar, place)
				if len(series) > 0 {
					// TODO: add ranking function for *pb.Series. Now only pick one series
					// from the private import.
					result.Data[place].Data[statVar] = series[0]
				}
			}
		}
	}
	return result, nil
}

// GetStatSetSeriesWithinPlace implements API for Mixer.GetStatSetSeriesWithinPlace.
// Endpoint: /v1/stat/set/series/within-place
func (s *Server) GetStatSetSeriesWithinPlace(
	ctx context.Context, in *pb.GetStatSetSeriesWithinPlaceRequest) (
	*pb.GetStatSetSeriesResponse, error,
) {
	parentPlace := in.GetParentPlace()
	statVars := in.GetStatVars()
	childType := in.GetChildType()
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
	childPlaces, err := getChildPlaces(ctx, s.store, parentPlace, childType)
	if err != nil {
		return nil, err
	}

	return s.GetStatSetSeries(ctx, &pb.GetStatSetSeriesRequest{
		Places:   childPlaces,
		StatVars: statVars,
	})
}
