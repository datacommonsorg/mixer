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
	"io/ioutil"
	"math"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetStatSetSeries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := setup()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/get_stat_set_series")

	for _, c := range []struct {
		statVars     []string
		places       []string
		goldenFile   string
		partialMatch bool
	}{
		{
			[]string{
				"Count_Person",
				"Count_Person_Female",
				"Count_Person_Urban",
				"Count_CriminalActivities_CombinedCrime",
				"Median_Age_Person",
				"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
				"Count_Person_IsInternetUser_PerCapita",
			},
			[]string{"country/USA", "country/JPN", "country/IND", "geoId/06", "geoId/06085", "geoId/0649670"},
			"misc.json",
			false,
		},
		{
			[]string{"CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedOrProbableCase"},
			[]string{"country/USA", "geoId/06", "geoId/06085"},
			"nyt_covid_cases.json",
			true,
		},
	} {
		resp, err := client.GetStatSetSeries(ctx, &pb.GetStatSetSeriesRequest{
			StatVars: c.statVars,
			Places:   c.places,
		})
		if err != nil {
			t.Errorf("could not GetStatSetSeries: %s", err)
			continue
		}
		goldenFile := path.Join(goldenPath, c.goldenFile)
		if generateGolden {
			updateProtoGolden(resp, goldenPath, c.goldenFile)
			continue
		}

		var expected pb.GetStatSetSeriesResponse
		file, _ := ioutil.ReadFile(goldenFile)
		err = protojson.Unmarshal(file, &expected)
		if err != nil {
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
