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

// Limit the concurrent channels when processing in-memory cache data.
const maxChannelSize = 50

// ObsProp represents properties for a StatObservation.
type ObsProp struct {
	Mmethod string
	Operiod string
	Unit    string
	Sfactor string
}

func tokenFn(
	keyTokens map[string]*placeStatVar) func(rowKey string) (string, error) {
	return func(rowKey string) (string, error) {
		return keyTokens[rowKey].place + "^" + keyTokens[rowKey].statVar, nil
	}
}

// Filter a list of source series given the observation properties.
func filterSeries(in []*SourceSeries, prop *ObsProp) []*SourceSeries {
	result := []*SourceSeries{}
	for _, series := range in {
		if prop.Mmethod != "" && prop.Mmethod != series.MeasurementMethod {
			continue
		}
		if prop.Operiod != "" && prop.Operiod != series.ObservationPeriod {
			continue
		}
		if prop.Unit != "" && prop.Unit != series.Unit {
			continue
		}
		if prop.Sfactor != "" && prop.Sfactor != series.ScalingFactor {
			continue
		}
		result = append(result, series)
	}
	return result
}

// GetStatValue implements API for Mixer.GetStatValue.
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

	// Read triples for the statistical variable.
	triplesRowList := buildTriplesKey([]string{statVar})
	triples, err := readTriples(ctx, s.btTable, triplesRowList)
	if err != nil {
		return nil, err
	}
	// Get the StatisticalVariable
	if triples[statVar] == nil {
		return nil, status.Errorf(codes.NotFound,
			"No statistical variable found for %s", statVar)
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
		tokenFn(keyTokens))
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
		return nil, status.Errorf(
			codes.NotFound, "No data for %s, %s", place, statVar)
	}
	obsTimeSeries.SourceSeries = filterSeries(obsTimeSeries.SourceSeries, filterProp)
	result, err := getValue(obsTimeSeries, date)
	if err != nil {
		return nil, err
	}
	return &pb.GetStatValueResponse{Value: result}, nil
}

// GetStatSeries implements API for Mixer.GetStatSeries.
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

// GetStatCollection implements API for Mixer.GetStatCollection.
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
	if date == "" {
		return nil, status.Errorf(codes.InvalidArgument,
			"Missing required argument: date")
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
	rowList, keyTokens := buildStatCollectionKey(
		parentPlace, childType, date, statVarObject)
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
			result.Data[token] = cohorts[0]
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
				result.Data[sv] = cohorts[0]
			}
		}
	}
	for sv := range result.Data {
		if result.Data[sv] == nil {
			result.Data[sv] = &pb.SourceSeries{}
		}
	}
	return result, nil
}

// GetStats implements API for Mixer.GetStats.
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

// triplesToStatsVar converts a Triples cache into a StatisticalVarible object.
func triplesToStatsVar(
	statsVarDcid string, triples *TriplesCache) (*StatisticalVariable, error) {
	// Get constraint properties.
	propValMap := map[string]string{}
	for _, t := range triples.Triples {
		if t.Predicate == "constraintProperties" {
			propValMap[t.ObjectID] = ""
		}
	}
	statsVar := StatisticalVariable{}
	// Populate the field.
	for _, t := range triples.Triples {
		if t.SubjectID != statsVarDcid {
			continue
		}
		object := t.ObjectID
		switch t.Predicate {
		case "typeOf":
			if object != "StatisticalVariable" {
				return nil, status.Errorf(
					codes.Internal, "%s is not a StatisticalVariable", t.SubjectID)
			}
		case "statType":
			statsVar.StatType = strings.Replace(object, "Value", "", 1)
		case "populationType":
			statsVar.PopType = object
		case "measurementMethod":
			statsVar.MeasurementMethod = object
		case "measuredProperty":
			statsVar.MeasuredProp = object
		case "measurementDenominator":
			statsVar.MeasurementDenominator = object
		case "measurementQualifier":
			statsVar.MeasurementQualifier = object
		case "scalingFactor":
			statsVar.ScalingFactor = object
		case "unit":
			statsVar.Unit = object
		default:
			if _, ok := propValMap[t.Predicate]; ok {
				if statsVar.PVs == nil {
					statsVar.PVs = map[string]string{}
				}
				statsVar.PVs[t.Predicate] = object
			}
		}
	}
	return &statsVar, nil
}

// getValue get the stat value from ObsTimeSeries.
// When date is given, it get the value from the highest ranked source series
// that has the date.
// When date is not given, it get the latest value from the highest ranked
// source series.
func getValue(in *ObsTimeSeries, date string) (float64, error) {
	if in == nil {
		return 0, status.Error(codes.Internal, "Nil obs time series for getValue()")
	}
	sourceSeries := in.SourceSeries
	sort.Sort(byRank(sourceSeries))
	if date != "" {
		for _, series := range sourceSeries {
			if value, ok := series.Val[date]; ok {
				return value, nil
			}
		}
		return 0, status.Errorf(codes.NotFound, "No data found for date %s", date)
	}
	latestDate := ""
	var result float64
	for _, series := range sourceSeries {
		for date, value := range series.Val {
			if date > latestDate {
				latestDate = date
				result = value
			}
		}
	}
	if latestDate == "" {
		return 0, status.Errorf(codes.NotFound,
			"No stat data found for %s", in.PlaceDcid)
	}
	return result, nil
}

func (in *ObsTimeSeries) filterAndRank(prop *ObsProp) {
	if in == nil {
		return
	}
	series := filterSeries(in.SourceSeries, prop)
	sort.Sort(byRank(series))
	if len(series) > 0 {
		in.Data = series[0].Val
		in.ProvenanceDomain = series[0].ProvenanceDomain
		in.ProvenanceURL = series[0].ProvenanceURL
	}
	in.SourceSeries = nil
}

// readStats reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func readStats(
	ctx context.Context,
	btTable *bigtable.Table,
	rowList bigtable.RowList,
	keyTokens map[string]*placeStatVar) (
	map[string]map[string]*ObsTimeSeries, error) {

	dataMap, err := bigTableReadRowsParallel(
		ctx, btTable, rowList, convertToObsSeries, tokenFn(keyTokens),
	)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]*ObsTimeSeries{}
	for token, data := range dataMap {
		parts := strings.Split(token, "^")
		place := parts[0]
		statVar := parts[1]
		if result[place] == nil {
			result[place] = map[string]*ObsTimeSeries{}
		}
		if data == nil {
			result[place][statVar] = nil
		} else {
			result[place][statVar] = data.(*ObsTimeSeries)
		}
	}
	return result, nil
}

// readStats reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func readStatsPb(
	ctx context.Context,
	btTable *bigtable.Table,
	rowList bigtable.RowList,
	keyTokens map[string]*placeStatVar) (
	map[string]map[string]*pb.ObsTimeSeries, error) {

	dataMap, err := bigTableReadRowsParallel(
		ctx, btTable, rowList, convertToObsSeriesPb, tokenFn(keyTokens),
	)
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]*pb.ObsTimeSeries{}
	for token, data := range dataMap {
		parts := strings.Split(token, "^")
		place := parts[0]
		statVar := parts[1]
		if result[place] == nil {
			result[place] = map[string]*pb.ObsTimeSeries{}
		}
		if data == nil {
			result[place][statVar] = nil
		} else {
			result[place][statVar] = data.(*pb.ObsTimeSeries)
		}
	}
	return result, nil
}

// readStatCollection reads and process ObsCollection cache from BigTable
// in parallel.
func readStatCollection(
	ctx context.Context,
	btTable *bigtable.Table,
	rowList bigtable.RowList,
	keyTokens map[string]string) (
	map[string]*pb.ObsCollection, error) {

	dataMap, err := bigTableReadRowsParallel(
		ctx, btTable, rowList, convertToObsCollection,
		func(rowKey string) (string, error) {
			return keyTokens[rowKey], nil
		},
	)
	if err != nil {
		return nil, err
	}
	result := map[string]*pb.ObsCollection{}
	for token, data := range dataMap {
		result[token] = data.(*pb.ObsCollection)
	}
	return result, nil
}
