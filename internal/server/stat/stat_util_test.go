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

package stat

import (
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetValueFromBestSourcePb(t *testing.T) {
	for _, c := range []struct {
		obs  *pb.ObsTimeSeries
		date string
		ps   *pb.PointStat
		meta *pb.StatMetadata
	}{
		{
			&pb.ObsTimeSeries{},
			"2018",
			nil,
			nil,
		},
		{
			// CensusPEP is preferred
			&pb.ObsTimeSeries{
				SourceSeries: []*pb.SourceSeries{
					{
						Val: map[string]float64{
							"2017": 100,
							"2018": 200,
							"2019": 300,
						},
						ImportName:        "CensusACS5YearSurvey",
						ProvenanceUrl:     "https://www.census.gov/",
						MeasurementMethod: "CensusACS5yrSurvey",
					},
					{
						Val: map[string]float64{
							"2017": 105,
							"2018": 205,
							"2019": 305,
						},
						ImportName:        "CensusPEP",
						ProvenanceUrl:     "https://www.census.gov/programs-surveys/popest.html",
						MeasurementMethod: "CensusPEPSurvey",
						ObservationPeriod: "P1Y",
					},
				},
			},
			"2018",
			&pb.PointStat{
				Date:  "2018",
				Value: 205,
			},
			&pb.StatMetadata{
				ImportName:        "CensusPEP",
				ProvenanceUrl:     "https://www.census.gov/programs-surveys/popest.html",
				MeasurementMethod: "CensusPEPSurvey",
				ObservationPeriod: "P1Y",
			},
		},
		{
			// Latest value is preferred
			&pb.ObsTimeSeries{
				SourceSeries: []*pb.SourceSeries{
					{
						Val: map[string]float64{
							"2017": 100,
							"2018": 200,
							"2019": 300,
							"2020": 400,
						},
						ImportName:        "CensusACS5YearSurvey",
						ProvenanceUrl:     "https://www.census.gov/",
						MeasurementMethod: "CensusACS5yrSurvey",
					},
					{
						Val: map[string]float64{
							"2017": 105,
							"2018": 205,
							"2019": 305,
						},
						ImportName:        "CensusPEP",
						ProvenanceUrl:     "https://www.census.gov/programs-surveys/popest.html",
						MeasurementMethod: "CensusPEPSurvey",
						ObservationPeriod: "P1Y",
					},
				},
			},
			"",
			&pb.PointStat{
				Date:  "2020",
				Value: 400,
			},
			&pb.StatMetadata{
				ImportName:        "CensusACS5YearSurvey",
				ProvenanceUrl:     "https://www.census.gov/",
				MeasurementMethod: "CensusACS5yrSurvey",
			},
		},
	} {
		ps, meta := GetValueFromBestSourcePb(c.obs, c.date)
		if diff := cmp.Diff(ps, c.ps, protocmp.Transform()); diff != "" {
			t.Errorf("getValueFromBestSourcePb() got diff PointStat %v", diff)
		}
		if diff := cmp.Diff(meta, c.meta, protocmp.Transform()); diff != "" {
			t.Errorf("getValueFromBestSourcePb() got diff Metadata %v", diff)
		}
	}
}
