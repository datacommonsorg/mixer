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

package golden

import (
	"context"
	"math"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/test/e2e"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetStatSetSeries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "get_stat_set_series")

	testSuite := func(client pb.MixerClient, latencyTest, useImportGroup bool) {
		for _, c := range []struct {
			statVars     []string
			places       []string
			goldenFile   string
			partialMatch bool
			importName   string
		}{
			{
				[]string{
					"Count_Person",
					"Count_Person_Female",
					"Count_Person_Urban",
					"Count_Person_Unemployed",
					"Count_CriminalActivities_CombinedCrime",
					"Median_Age_Person",
					"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
					"Count_Person_IsInternetUser_PerCapita",
					"Annual_Generation_Electricity",
					"PrecipitationRate_RCP85",
				},
				[]string{"country/USA", "country/JPN", "country/IND", "geoId/06", "geoId/06085", "geoId/0649670"},
				"misc.json",
				false,
				"",
			},
			{
				[]string{"DifferenceRelativeToBaseDate2006_Max_Temperature_RCP85", "Max_Temperature_RCP85", "Max_Temperature"},
				[]string{"geoId/06029", "geoId/06085"},
				"weather.json",
				false,
				"",
			},
			{
				[]string{"Count_Person"},
				[]string{"country/USA", "country/JPN", "country/IND", "geoId/06", "geoId/06085", "geoId/0649670"},
				"preferred_import.json",
				false,
				"CensusACS1YearSurvey",
			},
			{
				[]string{"CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedOrProbableCase"},
				[]string{"country/USA", "geoId/06", "geoId/06085"},
				"nyt_covid_cases.json",
				true,
				"",
			},
			{
				[]string{"Test_Stat_Var_1", "Test_Stat_Var_10"},
				[]string{"country/ALB", "country/AND"},
				"memdb.json",
				true,
				"",
			},
			{
				[]string{"Annual_Generation_Electricity"},
				[]string{"country/USA", "country/IND", "country/CHN"},
				"electricity_generation.json",
				false,
				"",
			},
		} {
			resp, err := client.GetStatSetSeries(ctx, &pb.GetStatSetSeriesRequest{
				StatVars:   c.statVars,
				Places:     c.places,
				ImportName: c.importName,
			})
			if err != nil {
				t.Errorf("could not GetStatSetSeries: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if useImportGroup {
				c.goldenFile = "IG_" + c.goldenFile
			}
			if e2e.GenerateGolden {
				e2e.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GetStatSetSeriesResponse
			if err := e2e.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}
			if c.partialMatch {
				for geo, geoData := range expected.Data {
					for sv, svData := range geoData.Data {
						for date := range svData.Val {
							if resp.Data[geo].Data[sv].Val == nil {
								t.Fatalf("result does not have data for geo %s and sv %s", geo, sv)
							}
							got := resp.Data[geo].Data[sv].Val[date]
							want := svData.Val[date]
							if sv == "CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedOrProbableCase" {
								// Allow approximate match for NYT covid data.
								if math.Abs(float64(got)/float64(want)-1) > 0.05 {
									t.Errorf(
										"%s, %s, %s want: %f, got: %f", sv, geo, date, want, got)
									continue
								}
							} else {
								if want != got {
									t.Errorf(
										"%s, %s, %s want: %f, got: %f", sv, geo, date, want, got)
									continue
								}
							}
						}
					}
				}
			} else {
				if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
					t.Errorf("payload got diff: %v", diff)
					continue
				}
			}
		}
	}

	if err := e2e.TestDriver(
		"GetStatSetSeries",
		&e2e.TestOption{UseCache: false, UseMemdb: true},
		testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
