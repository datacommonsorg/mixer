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
	"sort"
	"strings"

	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

type obsProp struct {
	mmethod string
	operiod string
	unit    string
	sfactor string
}

type rankKey struct {
	prov    string
	mmethod string
}

// Ranking for (import name, measurement method) combination. This is used to rank
// multiple dataset for the same StatisticalVariable, where lower value means
// higher ranking.
// The ranking score ranges from 0 to 100.
var statsRanking = map[rankKey]int{
	{"CensusPEP", "CensusPEPSurvey"}:                   0, // Population
	{"CensusACS5YearSurvey", "CensusACS5yrSurvey"}:     1, // Population
	{"EurostatData", "EurostatRegionalPopulationData"}: 2, // Population
	{"WorldDevelopmentIndicators", ""}:                 3, // Population
	{"BLS_LAUS", "BLSSeasonallyUnadjusted"}:            0, // Unemployment Rate
	{"EurostatData", ""}:                               1, // Unemployment Rate
	{"NYT_COVID19", "NYT_COVID19_GitHub"}:              0, // Covid
}

const lowestRank = 100

// Limit the concurrent channels when processing in-memory cache data.
const maxChannelSize = 50

// TODO(shifucun): add observationPeriod, unit, scalingFactor to ranking
// decision, so the ranking is deterministic.
// byRank implements sort.Interface for []*SourceSeries based on
// the rank score.
type byRank []*SourceSeries

func (a byRank) Len() int {
	return len(a)
}
func (a byRank) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a byRank) Less(i, j int) bool {
	oi := a[i]
	keyi := rankKey{oi.ImportName, oi.MeasurementMethod}
	scorei, ok := statsRanking[keyi]
	if !ok {
		scorei = lowestRank
	}
	oj := a[j]
	keyj := rankKey{oj.ImportName, oj.MeasurementMethod}
	scorej, ok := statsRanking[keyj]
	if !ok {
		scorej = lowestRank
	}
	// Higher score value means lower rank.
	if scorei != scorej {
		return scorei < scorej
	}
	// Compare other fields to get consistent ranking.
	if oi.ObservationPeriod != oj.ObservationPeriod {
		return oi.ObservationPeriod < oj.ObservationPeriod
	}
	if oi.ScalingFactor != oj.ScalingFactor {
		return oi.ScalingFactor < oj.ScalingFactor
	}
	if oi.Unit != oj.Unit {
		return oi.Unit < oj.Unit
	}
	if oi.ProvenanceDomain != oj.ProvenanceDomain {
		return oi.ProvenanceDomain < oj.ProvenanceDomain
	}
	return true
}

// Filter a list of source series given the observation properties.
func filterSeries(in []*SourceSeries, prop *obsProp) []*SourceSeries {
	result := []*SourceSeries{}
	for _, series := range in {
		if prop.mmethod != "" && prop.mmethod != series.MeasurementMethod {
			continue
		}
		if prop.operiod != "" && prop.operiod != series.ObservationPeriod {
			continue
		}
		if prop.unit != "" && prop.unit != series.Unit {
			continue
		}
		if prop.sfactor != "" && prop.sfactor != series.ScalingFactor {
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
	if place == "" || statVar == "" {
		return nil, fmt.Errorf("Missing required arguments")
	}
	date := in.GetDate()

	// Read triples for the statistical variable.
	triplesRowList := buildTriplesKey([]string{statVar})
	triples, err := readTriples(ctx, s.btTable, triplesRowList)
	if err != nil {
		return nil, err
	}
	// Get the StatisticalVariable
	if triples[statVar] == nil {
		return nil, fmt.Errorf("No statistical variable found for %s", statVar)
	}
	statVarObject, err := triplesToStatsVar(statVar, triples[statVar])
	if err != nil {
		return nil, err
	}
	// Construct BigTable row keys.
	rowList := buildStatsKey([]string{place}, statVarObject)

	var obsTimeSeries *ObsTimeSeries
	// Read data from branch in-memory cache first.
	cacheData := s.memcache.ReadParallel(rowList, convertToObsSeries, nil)
	if data, ok := cacheData[place]; ok {
		if data == nil {
			obsTimeSeries = nil
		} else {
			obsTimeSeries = data.(*ObsTimeSeries)
		}
	} else {
		// If the data is missing in branch cache, fetch it from the base cache in
		// Bigtable.
		btData, err := readStats(ctx, s.btTable, rowList)
		if err != nil {
			return nil, err
		}
		obsTimeSeries = btData[place]
	}
	result, err := getValue(obsTimeSeries, date)
	if err != nil {
		return nil, err
	}
	return &pb.GetStatValueResponse{Value: result}, nil
}

// GetStatSeries implements API for Mixer.GetStatSeries.
// TODO(shifucun): consilidate and dedup the logic among these similar APIs.
func (s *Server) GetStatSeries(ctx context.Context, in *pb.GetStatSeriesRequest) (
	*pb.GetStatSeriesResponse, error) {
	place := in.GetPlace()
	statVar := in.GetStatVar()
	if place == "" || statVar == "" {
		return nil, fmt.Errorf("Missing required arguments")
	}
	filterProp := &obsProp{
		mmethod: in.GetMeasurementMethod(),
		operiod: in.GetObservationPeriod(),
		unit:    in.GetUnit(),
		sfactor: in.GetScalingFactor(),
	}

	// Read triples for statistical variable.
	triplesRowList := buildTriplesKey([]string{statVar})
	triples, err := readTriples(ctx, s.btTable, triplesRowList)
	if err != nil {
		return nil, err
	}
	// Get the StatisticalVariable
	if triples[statVar] == nil {
		return nil, fmt.Errorf("No statistical variable found for %s", statVar)
	}
	statVarObject, err := triplesToStatsVar(statVar, triples[statVar])
	if err != nil {
		return nil, err
	}
	// Construct BigTable row keys.
	rowList := buildStatsKey([]string{place}, statVarObject)

	var obsTimeSeries *ObsTimeSeries
	// Read data from branch in-memory cache first.
	cacheData := s.memcache.ReadParallel(rowList, convertToObsSeries, nil)
	if data, ok := cacheData[place]; ok {
		if data == nil {
			obsTimeSeries = nil
		} else {
			obsTimeSeries = data.(*ObsTimeSeries)
		}
	} else {
		// If the data is missing in branch cache, fetch it from the base cache in
		// Bigtable.
		btData, err := readStats(ctx, s.btTable, rowList)
		if err != nil {
			return nil, err
		}
		obsTimeSeries = btData[place]
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

// GetStats implements API for Mixer.GetStats.
func (s *Server) GetStats(ctx context.Context, in *pb.GetStatsRequest) (
	*pb.GetStatsResponse, error) {
	placeDcids := in.GetPlace()
	statsVarDcid := in.GetStatsVar()
	if len(placeDcids) == 0 || statsVarDcid == "" {
		return nil, fmt.Errorf("Missing required arguments")
	}
	filterProp := &obsProp{
		mmethod: in.GetMeasurementMethod(),
		operiod: in.GetObservationPeriod(),
		unit:    in.GetUnit(),
	}

	// Read triples for statistical variable.
	triplesRowList := buildTriplesKey([]string{statsVarDcid})
	triples, err := readTriples(ctx, s.btTable, triplesRowList)
	if err != nil {
		return nil, err
	}
	// Get the StatisticalVariable
	if triples[statsVarDcid] == nil {
		return nil, fmt.Errorf("No statistical variable found for %s", statsVarDcid)
	}
	statsVar, err := triplesToStatsVar(statsVarDcid, triples[statsVarDcid])
	if err != nil {
		return nil, err
	}
	// Construct BigTable row keys.
	rowList := buildStatsKey(placeDcids, statsVar)

	result := map[string]*ObsTimeSeries{}

	// Read data from branch in-memory cache first.
	if in.GetOption().GetCacheChoice() != pb.Option_BASE_CACHE_ONLY {
		tmp := s.memcache.ReadParallel(rowList, convertToObsSeries, nil)
		for dcid, data := range tmp {
			if data == nil {
				result[dcid] = nil
			} else {
				result[dcid] = data.(*ObsTimeSeries)
			}
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
		extraData, err := readStats(ctx, s.btTable, buildStatsKey(extraDcids, statsVar))
		if err != nil {
			return nil, err
		}
		for dcid := range extraData {
			result[dcid] = extraData[dcid]
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
				return nil, fmt.Errorf("%s is not a StatisticalVariable", t.SubjectID)
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
		return 0, fmt.Errorf("Nil obs time series for getValue()")
	}
	sourceSeries := in.SourceSeries
	sort.Sort(byRank(sourceSeries))
	if date != "" {
		for _, series := range sourceSeries {
			if value, ok := series.Val[date]; ok {
				return value, nil
			}
		}
		return 0, fmt.Errorf("No data found for date %s", date)
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
		return 0, fmt.Errorf("No stat data found for %s", in.PlaceDcid)
	}
	return result, nil
}

func (in *ObsTimeSeries) filterAndRank(prop *obsProp) {
	if in == nil {
		return
	}
	series := filterSeries(in.SourceSeries, prop)
	sort.Sort(byRank(series))
	if len(series) > 0 {
		in.Data = series[0].Val
		in.ProvenanceDomain = series[0].ProvenanceDomain
	}
	in.SourceSeries = nil
}
func convertToObsSeries(dcid string, jsonRaw []byte) (interface{}, error) {
	pbData := &pb.ChartStore{}
	if err := protojson.Unmarshal(jsonRaw, pbData); err != nil {
		return nil, err
	}
	switch x := pbData.Val.(type) {
	case *pb.ChartStore_ObsTimeSeries:
		pbSourceSeries := x.ObsTimeSeries.GetSourceSeries()
		ret := &ObsTimeSeries{
			Data:         x.ObsTimeSeries.GetData(),
			PlaceName:    x.ObsTimeSeries.GetPlaceName(),
			PlaceDcid:    dcid,
			SourceSeries: make([]*SourceSeries, len(pbSourceSeries)),
		}
		for i, source := range pbSourceSeries {
			ret.SourceSeries[i] = &SourceSeries{
				ImportName:        source.GetImportName(),
				ObservationPeriod: source.GetObservationPeriod(),
				MeasurementMethod: source.GetMeasurementMethod(),
				ScalingFactor:     source.GetScalingFactor(),
				Unit:              source.GetUnit(),
				ProvenanceDomain:  source.GetProvenanceDomain(),
				Val:               source.GetVal(),
			}
		}
		ret.ProvenanceDomain = x.ObsTimeSeries.GetProvenanceDomain()
		return ret, nil
	case nil:
		return nil, fmt.Errorf("ChartStore.Val is not set")
	default:
		return nil, fmt.Errorf("ChartStore.Val has unexpected type %T", x)
	}
}

// readStats reads and process BigTable rows in parallel.
// Consider consolidate this function and bigTableReadRowsParallel.
func readStats(ctx context.Context, btTable *bigtable.Table,
	rowList bigtable.RowList) (map[string]*ObsTimeSeries, error) {

	dataMap, err := bigTableReadRowsParallel(
		ctx, btTable, rowList, convertToObsSeries, nil)
	if err != nil {
		return nil, err
	}
	result := map[string]*ObsTimeSeries{}
	for dcid, data := range dataMap {
		if data == nil {
			result[dcid] = nil
		} else {
			result[dcid] = data.(*ObsTimeSeries)
		}
	}
	return result, nil
}
