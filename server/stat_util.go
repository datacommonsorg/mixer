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
	"sort"
	"strings"

	pb "github.com/datacommonsorg/mixer/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

// Filter a list of source series given the observation properties.
func filterSeriesPb(in []*pb.SourceSeries, prop *ObsProp) []*pb.SourceSeries {
	result := []*pb.SourceSeries{}
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

func filterAndRankPb(in *pb.ObsTimeSeries, prop *ObsProp) *pb.SourceSeries {
	if in == nil {
		return nil
	}
	series := filterSeriesPb(in.SourceSeries, prop)
	sort.Sort(SeriesByRank(series))
	if len(series) > 0 {
		return series[0]
	}
	return nil
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

// getValueFromTopRank get the stat value from top ranked source series.
//
// When date is given, it get the value from the highest ranked source series
// that has the date.
//
// When date is not given, it get the latest value from the highest ranked
// source series.
func getValueFromTopRank(in *ObsTimeSeries, date string) (float64, error) {
	if in == nil {
		return 0, status.Error(codes.Internal, "Nil obs time series for getValueFromTopRank()")
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

// getValueFromBestSourcePb get the stat value from ObsTimeSeries (protobuf version)
//
// When date is given, it get the value from the highest ranked source series
// that has the date.
//
// When date is not given, it get the latest value from the highest ranked
// source series.
func getValueFromBestSourcePb(in *pb.ObsTimeSeries, date string) (*pb.PointStat, error) {
	if in == nil {
		return nil, status.Error(codes.Internal, "Nil obs time series for getValueFromBestSourcePb()")
	}
	sourceSeries := in.SourceSeries
	sort.Sort(SeriesByRank(sourceSeries))
	if date != "" {
		for _, series := range sourceSeries {
			if value, ok := series.Val[date]; ok {
				return &pb.PointStat{
					Date:  date,
					Value: value,
					Metadata: &pb.StatMetadata{
						ImportName:        series.ImportName,
						ProvenanceUrl:     series.ProvenanceUrl,
						MeasurementMethod: series.MeasurementMethod,
						ObservationPeriod: series.ObservationPeriod,
						ScalingFactor:     series.ScalingFactor,
						Unit:              series.Unit,
					},
				}, nil
			}
		}
		return nil, status.Errorf(codes.NotFound, "No data found for date %s", date)
	}
	latestDate := ""
	result := &pb.PointStat{}
	series := sourceSeries[0]
	for date, value := range series.Val {
		if date > latestDate {
			latestDate = date
			result = &pb.PointStat{
				Date:  date,
				Value: value,
				Metadata: &pb.StatMetadata{
					ImportName:        series.ImportName,
					ProvenanceUrl:     series.ProvenanceUrl,
					MeasurementMethod: series.MeasurementMethod,
					ObservationPeriod: series.ObservationPeriod,
					ScalingFactor:     series.ScalingFactor,
					Unit:              series.Unit,
				},
			}
		}
	}
	if latestDate == "" {
		return nil, status.Errorf(codes.NotFound,
			"No stat data found for %s", in.PlaceDcid)
	}
	return result, nil
}
