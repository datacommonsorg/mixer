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
	"strings"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/proto"
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
	triples, err := readTriples(ctx, s.btTable, triplesRowList)
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
	// Read data from branch in-memory cache first.
	cacheData := s.memcache.ReadParallel(
		rowList,
		convertToObsSeries,
		tokenFn(keyTokens),
	)
	if data, ok := cacheData[place]; ok {
		if data == nil {
			obsTimeSeries = nil
		} else {
			obsTimeSeries = data.(*ObsTimeSeries)
		}
	} else {
		// If the data is missing in branch cache, fetch it from the base cache in
		// Bigtable.
		btData, err := readStats(ctx, s.btTable, rowList, keyTokens)
		if err != nil {
			return nil, err
		}
		obsTimeSeries = btData[place][statVar]
	}
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
	rowList, keyTokens := buildStatsKey(places, statVarObject)

	// Read data from branch in-memory cache first.
	cacheData := s.memcache.ReadParallel(
		rowList,
		convertToObsSeriesPb,
		tokenFn(keyTokens),
	)

	for token, data := range cacheData {
		parts := strings.Split(token, "^")
		place := parts[0]
		statVar := parts[1]
		if data == nil {
			result.PlaceData[place].StatVarData[statVar] = &pb.ObsTimeSeries{}
		} else {
			result.PlaceData[place].StatVarData[statVar] = data.(*pb.ObsTimeSeries)
		}
	}

	// If cache value is not found in memcache, then look up in BigTable
	extraRowList := bigtable.RowList{}
	for key, token := range keyTokens {
		if result.PlaceData[token.place].StatVarData[token.statVar] == nil {
			extraRowList = append(extraRowList, key)
		}
	}
	if len(extraRowList) > 0 {
		extraData, err := readStatsPb(ctx, s.btTable, extraRowList, keyTokens)
		if err != nil {
			return nil, err
		}
		for place, placeData := range extraData {
			for statVar, data := range placeData {
				result.PlaceData[place].StatVarData[statVar] = data
			}
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
	triples, err := readTriples(ctx, s.btTable, triplesRowList)
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

	// Read data from branch in-memory cache first.
	cacheData := s.memcache.ReadParallel(
		rowList,
		convertToObsSeries,
		tokenFn(keyTokens),
	)
	for token, data := range cacheData {
		place := strings.Split(token, "^")[0]
		if data == nil {
			result[place] = nil
		} else {
			result[place] = data.(*ObsTimeSeries)
		}
	}
	// For each place, if the data is missing in branch cache, fetch it from the
	// base cache in Bigtable.
	if len(result) < len(placeDcids) {
		extraDcids := []string{}
		for _, dcid := range placeDcids {
			if _, ok := result[dcid]; !ok {
				extraDcids = append(extraDcids, dcid)
			}
		}
		rowList, keyTokens := buildStatsKey(
			extraDcids,
			map[string]*StatisticalVariable{statsVarDcid: statsVarObject})
		extraData, err := readStats(ctx, s.btTable, rowList, keyTokens)
		if err != nil {
			return nil, err
		}
		for place := range extraData {
			result[place] = extraData[place][statsVarDcid]
		}
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

// GetStat implements API for Mixer.GetStat.
// Endpoint: /stat/series/rich
func (s *Server) GetStat(ctx context.Context, in *pb.GetStatRequest) (
	*pb.GetStatResponse, error) {
	placeDcids := in.GetPlaces()
	statVarDcid := in.GetStatVar()
	if len(placeDcids) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required argument: place")
	}
	if statVarDcid == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing required argument: stat_var")
	}
	filterProp := &ObsProp{
		Mmethod: in.GetMeasurementMethod(),
		Operiod: in.GetObservationPeriod(),
		Unit:    in.GetUnit(),
	}

	// Read triples for statistical variable.
	triplesRowList := buildTriplesKey([]string{statVarDcid})
	triples, err := readTriples(ctx, s.btTable, triplesRowList)
	if err != nil {
		return nil, err
	}
	// Get the StatisticalVariable
	if triples[statVarDcid] == nil {
		return nil, status.Errorf(codes.NotFound, "No statistical variable found for %s", statVarDcid)
	}
	statVarObject, err := triplesToStatsVar(statVarDcid, triples[statVarDcid])
	if err != nil {
		return nil, err
	}
	// Construct BigTable row keys.
	rowList, keyTokens := buildStatsKey(
		placeDcids,
		map[string]*StatisticalVariable{statVarDcid: statVarObject},
	)

	tmp := map[string]*pb.ObsTimeSeries{}

	// Read data from branch in-memory cache first.
	cacheData := s.memcache.ReadParallel(
		rowList,
		convertToObsSeriesPb,
		tokenFn(keyTokens),
	)
	for token, data := range cacheData {
		place := strings.Split(token, "^")[0]
		if data == nil {
			tmp[place] = nil
		} else {
			tmp[place] = data.(*pb.ObsTimeSeries)
		}
	}
	// For each place, if the data is missing in branch cache, fetch it from the
	// base cache in Bigtable.
	if len(tmp) < len(placeDcids) {
		extraDcids := []string{}
		for _, dcid := range placeDcids {
			if _, ok := tmp[dcid]; !ok {
				extraDcids = append(extraDcids, dcid)
			}
		}
		rowList, keyTokens := buildStatsKey(
			extraDcids,
			map[string]*StatisticalVariable{statVarDcid: statVarObject})
		extraData, err := readStatsPb(ctx, s.btTable, rowList, keyTokens)
		if err != nil {
			return nil, err
		}
		for place := range extraData {
			tmp[place] = extraData[place][statVarDcid]
		}
	}

	// Fill missing place data and result result
	for _, dcid := range placeDcids {
		if _, ok := tmp[dcid]; !ok {
			tmp[dcid] = nil
		}
	}
	result := map[string]*pb.SourceSeries{}
	for place, obsSeries := range tmp {
		result[place] = filterAndRankPb(obsSeries, filterProp)
	}

	return &pb.GetStatResponse{Stat: result}, nil
}
