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
	"hash/fnv"
	"sort"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
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

func lowQualityPopulationImport(importName string) bool {
	return importName == "WikidataPopulation" ||
		importName == "KGHumanCurated" ||
		importName == "HumanCuratedStats" ||
		importName == "WikipediaStatsData"
}

func tokenFn(
	keyTokens map[string]*util.PlaceStatVar) func(rowKey string) (string, error) {
	return func(rowKey string) (string, error) {
		return keyTokens[rowKey].Place + "^" + keyTokens[rowKey].StatVar, nil
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

func (in *ObsTimeSeries) filterAndRank(prop *ObsProp) {
	if in == nil {
		return
	}
	series := filterSeries(in.SourceSeries, prop)
	sort.Sort(byRank(series))
	if len(series) > 0 {
		in.Data = series[0].Val
		in.ProvenanceURL = series[0].ProvenanceURL
	}
	in.SourceSeries = nil
}

// Get the best series for a collection of series with different metadata.
//
// - If "importName" is set, pick the series with the import name.
// - If "useLatest" is true, pick the series with latest date and set the
//   second return value to be the latest date.
//
// Note "importName" is preferred over "useLatest".
func getBestSeries(
	in *pb.ObsTimeSeries,
	importName string,
	useLatest bool,
) (*pb.Series, *string) {
	rawSeries := in.SourceSeries
	// If importName is set, must return the series with that import name.
	if importName != "" {
		for _, series := range rawSeries {
			if series.ImportName == importName {
				return rawSeriesToSeries(series), nil
			}
		}
		return nil, nil
	}
	sort.Sort(SeriesByRank(rawSeries))
	if len(rawSeries) > 0 {
		// Choose the latest series.
		if useLatest {
			var result *pb.Series
			latest := ""
			for _, series := range rawSeries {
				currLatest := ""
				for date := range series.Val {
					if date > currLatest {
						currLatest = date
					}
				}
				if currLatest > latest {
					latest = currLatest
					result = rawSeriesToSeries(series)
				}
			}
			return result, &latest
		}
		return rawSeriesToSeries(rawSeries[0]), nil
	}
	return nil, nil
}

func rawSeriesToSeries(in *pb.SourceSeries) *pb.Series {
	result := &pb.Series{}
	result.Val = in.Val
	result.Metadata = &pb.StatMetadata{
		ImportName:        in.ImportName,
		ProvenanceUrl:     in.ProvenanceUrl,
		MeasurementMethod: in.MeasurementMethod,
		ObservationPeriod: in.ObservationPeriod,
		ScalingFactor:     in.ScalingFactor,
		Unit:              in.Unit,
	}
	return result
}

// getValueFromBestSource get the stat value from top ranked source series.
//
// When date is given, it get the value from the highest ranked source series
// that has the date.
//
// When date is not given, it get the latest value from the highest ranked
// source series.
func getValueFromBestSource(in *ObsTimeSeries, date string) (float64, error) {
	if in == nil {
		return 0, status.Error(codes.Internal, "Nil obs time series for getValueFromBestSource()")
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
// When date is not given, it get the latest value from all the source series.
// If two sources has the same latest date, the highest ranked source is preferred.
func getValueFromBestSourcePb(
	in *pb.ObsTimeSeries, date string) (*pb.PointStat, *pb.StatMetadata) {
	if in == nil {
		return nil, nil
	}
	sourceSeries := in.SourceSeries
	sort.Sort(SeriesByRank(sourceSeries))

	// Date is given, get the value from highest ranked source that has this date.
	if date != "" {
		for _, series := range sourceSeries {
			if value, ok := series.Val[date]; ok {
				meta := &pb.StatMetadata{
					ImportName:        series.ImportName,
					ProvenanceUrl:     series.ProvenanceUrl,
					MeasurementMethod: series.MeasurementMethod,
					ObservationPeriod: series.ObservationPeriod,
					ScalingFactor:     series.ScalingFactor,
					Unit:              series.Unit,
				}
				return &pb.PointStat{
					Date:  date,
					Value: value,
				}, meta
			}
		}
		return nil, nil
	}
	// Date is not given, get the latest value from all sources.
	latestDate := ""
	var ps *pb.PointStat
	var meta *pb.StatMetadata
	// At this stage, sourceSeries has import series ranked by the ranking score.
	// (accomplished by SeriesByRank function above)
	for idx, series := range sourceSeries {
		// If there are higher quality population data, then do not pick from the
		// lower quality import even it could have more recent data.
		if idx > 0 && lowQualityPopulationImport(series.ImportName) {
			break
		}
		meta = &pb.StatMetadata{
			ImportName:        series.ImportName,
			ProvenanceUrl:     series.ProvenanceUrl,
			MeasurementMethod: series.MeasurementMethod,
			ObservationPeriod: series.ObservationPeriod,
			ScalingFactor:     series.ScalingFactor,
			Unit:              series.Unit,
		}
		for date, value := range series.Val {
			if date > latestDate {
				latestDate = date
				ps = &pb.PointStat{
					Date:  date,
					Value: value,
				}
			}
		}
	}
	if latestDate == "" {
		return nil, nil
	}
	return ps, meta
}

// getMetadataHash retrieves a hash string for a given protobuf message.
// Note this should be restrict to a request scope.
func getMetadataHash(m *pb.StatMetadata) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.Join([]string{
		m.ImportName,
		m.MeasurementMethod,
		m.ObservationPeriod,
		m.ObservationPeriod,
		m.ScalingFactor,
		m.Unit,
	}, "-")))
	return h.Sum32()
}
