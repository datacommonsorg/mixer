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
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

	// Read triples for statistical variable.
	triplesRowList := buildTriplesKey([]string{statVar})
	triples, err := readTriples(ctx, s.tables(), triplesRowList)
	if err != nil {
		return nil, err
	}
	// Get the StatisticalVariable
	if triples[statVar] == nil {
		return nil, status.Errorf(
			codes.NotFound, "No statistical variable found for %s", statVar)
	}
	statVarObject, err := triplesToStatsVar(statVar, triples[statVar])
	if err != nil {
		return nil, err
	}
	// Construct BigTable row keys.
	rowList, keyTokens := buildStatsKey(
		[]string{place},
		map[string]*StatisticalVariable{statVar: statVarObject})

	var obsTimeSeries *ObsTimeSeries
	btData, err := readStats(ctx, s.tables(), rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	obsTimeSeries = btData[place][statVar]
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

	// Read triples for statistical variable.
	triplesRowList := buildTriplesKey(statVars)
	triples, err := readTriples(ctx, s.tables(), triplesRowList)
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
	rowList, keyTokens := buildStatsKey(places, statVarObject)
	cacheData, err := readStatsPb(ctx, s.tables(), rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	for place, placeData := range cacheData {
		for statVar, data := range placeData {
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

	placeDcids := in.GetPlace()
	statsVarDcid := in.GetStatsVar()
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

	// Read triples for statistical variable.
	triplesRowList := buildTriplesKey([]string{statsVarDcid})
	triples, err := readTriples(ctx, s.tables(), triplesRowList)
	if err != nil {
		return nil, err
	}
	// Get the StatisticalVariable
	if triples[statsVarDcid] == nil {
		return nil, status.Errorf(codes.NotFound,
			"No statistical variable found for %s", statsVarDcid)
	}
	statsVarObject, err := triplesToStatsVar(statsVarDcid, triples[statsVarDcid])
	if err != nil {
		return nil, err
	}
	// Construct BigTable row keys.
	rowList, keyTokens := buildStatsKey(
		placeDcids,
		map[string]*StatisticalVariable{statsVarDcid: statsVarObject},
	)
	result := map[string]*ObsTimeSeries{}
	cacheData, err := readStats(ctx, s.tables(), rowList, keyTokens)
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
	if len(places) == 0 {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: places")
	}
	if len(statVars) == 0 {
		return nil, status.Errorf(
			codes.InvalidArgument, "Missing required argument: stat_vars")
	}

	// Read triples for statistical variable.
	triplesRowList := buildTriplesKey(statVars)
	triples, err := readTriples(ctx, s.tables(), triplesRowList)
	if err != nil {
		return nil, err
	}
	// A map from stat_var dcid to stat_var object.
	// The stat_var object is used to create BigTable cache key
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
	rowList, keyTokens := buildStatsKey(places, statVarObject)

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
	cacheData, err := readStatsPb(ctx, s.tables(), rowList, keyTokens)
	if err != nil {
		return nil, err
	}
	for place, placeData := range cacheData {
		for statVar, data := range placeData {
			result.Data[place].Data[statVar] = getBestSeries(data)
		}
	}
	return result, nil
}
