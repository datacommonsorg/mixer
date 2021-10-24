// Copyright 2021 Google LLC
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
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetScorePb(t *testing.T) {
	for _, c := range []struct {
		series *pb.SourceSeries
		score  int
	}{
		{
			&pb.SourceSeries{ImportName: "CensusPEP", MeasurementMethod: "CensusPEPSurvey"},
			0,
		},
		{
			&pb.SourceSeries{ImportName: "CensusPEP", MeasurementMethod: "CensusPEPSurvey", ObservationPeriod: "P1Y"},
			0,
		},
		{
			&pb.SourceSeries{ImportName: "WorldDevelopmentIndicators", MeasurementMethod: "NewMM", ObservationPeriod: "P1Y"},
			4,
		},
		{
			&pb.SourceSeries{ImportName: "NASA_NEXDCP30", MeasurementMethod: "MM", ObservationPeriod: "P1Y"},
			100,
		},
		{
			&pb.SourceSeries{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_CCSM4", ObservationPeriod: "P1M"},
			0,
		},
	} {
		score := getScorePb(c.series)
		if diff := cmp.Diff(score, c.score); diff != "" {
			t.Errorf("getScorePb() got diff score %v", diff)
		}
	}
}

func TestSeriesByRank(t *testing.T) {
	for _, c := range []struct {
		series   []*pb.SourceSeries
		expected []*pb.SourceSeries
	}{
		{
			[]*pb.SourceSeries{
				{ImportName: "CensusACS5YearSurvey", MeasurementMethod: "CensusACS5yrSurvey"},
				{ImportName: "CensusPEP", MeasurementMethod: "CensusPEPSurvey"},
			},
			[]*pb.SourceSeries{
				{ImportName: "CensusPEP", MeasurementMethod: "CensusPEPSurvey"},
				{ImportName: "CensusACS5YearSurvey", MeasurementMethod: "CensusACS5yrSurvey"},
			},
		},
		{
			[]*pb.SourceSeries{
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P1M"},
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_GISS-E2-R", ObservationPeriod: "P1M"},
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P1Y"},
			},
			[]*pb.SourceSeries{
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_GISS-E2-R", ObservationPeriod: "P1M"},
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P1M"},
				{ImportName: "NASA_NEXDCP30", MeasurementMethod: "NASA_Mean_HadGEM2-AO", ObservationPeriod: "P1Y"},
			},
		},
	} {
		sort.Sort(SeriesByRank(c.series))
		if diff := cmp.Diff(c.expected, c.series, protocmp.Transform()); diff != "" {
			t.Errorf("SeriesByRank() got diff result %v", diff)
		}
	}
}
