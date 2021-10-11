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

package integration

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"math"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := setup()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/get_stats")

	for _, c := range []struct {
		statsVar     string
		place        []string
		mmethod      string
		goldenFile   string
		partialMatch bool
	}{
		{
			"Count_Person",
			[]string{"country/USA", "geoId/06", "geoId/06085", "geoId/0649670"},
			"",
			"census_pep.json",
			false,
		},
		{
			"CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedOrProbableCase",
			[]string{"country/USA", "geoId/06", "geoId/06085"},
			"",
			"nyt_covid_cases.json",
			true,
		},
		{
			"Count_Person",
			[]string{"geoId/06"},
			"CensusACS5yrSurvey",
			"census_acs.json",
			true,
		},
		{
			"Count_CriminalActivities_CombinedCrime",
			[]string{"geoId/06", "geoId/0649670"},
			"",
			"total_crimes.json",
			false,
		},
		{
			"Median_Age_Person",
			[]string{"geoId/0649670", "geoId/06085", "geoId/06"},
			"",
			"median_age.json",
			false,
		},
		{
			"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
			[]string{"country/USA"},
			"",
			"gdp.json",
			false,
		},
		{
			"Annual_Generation_Electricity",
			[]string{"country/USA", "geoId/06"},
			"",
			"electricity_generation.json",
			false,
		},
		{
			// This is to test for scaling factor.
			"Count_Person_IsInternetUser_PerCapita",
			[]string{"country/JPN"},
			"",
			"internet_user.json",
			false,
		},
	} {
		resp, err := client.GetStats(ctx, &pb.GetStatsRequest{
			StatsVar:          c.statsVar,
			Place:             c.place,
			MeasurementMethod: c.mmethod,
		})
		if err != nil {
			t.Errorf("could not GetStats: %s", err)
			continue
		}
		var result map[string]*server.ObsTimeSeries
		err = json.Unmarshal([]byte(resp.GetPayload()), &result)
		if err != nil {
			t.Errorf("Can not Unmarshal payload")
			continue
		}
		goldenFile := path.Join(goldenPath, c.goldenFile)
		if generateGolden {
			updateGolden(result, goldenPath, c.goldenFile)
			continue
		}

		var expected map[string]*server.ObsTimeSeries
		file, _ := ioutil.ReadFile(goldenFile)
		err = json.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file " + goldenFile + "\n" + err.Error())
			continue
		}
		if c.partialMatch {
			for geo := range expected {
				for date := range expected[geo].Data {
					if result[geo] == nil {
						t.Fatalf("result does not have data for geo %s", geo)
					}
					got := result[geo].Data[date]
					want := expected[geo].Data[date]
					if c.statsVar == "CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedOrProbableCase" {
						// Allow approximate match for NYT covid data.
						if math.Abs(float64(got)/float64(want)-1) > 0.05 {
							t.Errorf(
								"%s, %s, %s want: %f, got: %f", c.statsVar, geo, date, want, got)
							continue
						}
					} else {
						if want != got {
							t.Errorf(
								"%s, %s, %s want: %f, got: %f", c.statsVar, geo, date, want, got)
							continue
						}
					}
				}
			}
		} else {
			if diff := cmp.Diff(result, expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}
}
