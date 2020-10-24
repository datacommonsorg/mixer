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

package e2etest

import (
	"context"
	"io/ioutil"
	"math"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetStat(t *testing.T) {
	ctx := context.Background()

	memcacheData, err := loadMemcache()
	if err != nil {
		t.Fatalf("Failed to load memcache %v", err)
	}

	client, err := setup(server.NewMemcache(memcacheData))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "../golden_response/staging/get_stat")

	for _, c := range []struct {
		statVar      string
		places       []string
		mmethod      string
		goldenFile   string
		partialMatch bool
		wantErr      bool
	}{
		{
			"Count_Person",
			[]string{"country/USA", "geoId/06", "geoId/06085", "geoId/0649670"},
			"",
			"census_pep.json",
			false,
			false,
		},
		{
			"CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedOrProbableCase",
			[]string{"country/USA", "geoId/06", "geoId/06085"},
			"",
			"nyt_covid_cases.json",
			true,
			false,
		},
		{
			"Count_Person",
			[]string{"geoId/06"},
			"CensusACS5yrSurvey",
			"census_acs.json",
			true,
			false,
		},
		{
			"Count_CriminalActivities_CombinedCrime",
			[]string{"geoId/06", "geoId/0649670"},
			"",
			"total_crimes.json",
			false,
			false,
		},
		{
			"Median_Age_Person",
			[]string{"geoId/0649670", "geoId/06085", "geoId/06"},
			"",
			"median_age.json",
			false,
			false,
		},
		{
			"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
			[]string{"country/USA"},
			"",
			"gdp.json",
			false,
			false,
		},
		{
			// This is to test for scaling factor.
			"Count_Person_IsInternetUser_PerCapita",
			[]string{"country/JPN"},
			"",
			"internet_user.json",
			false,
			false,
		},
		{
			"BadStatsVar",
			[]string{"geoId/06"},
			"",
			"",
			false,
			true,
		},
	} {
		resp, err := client.GetStat(ctx, &pb.GetStatRequest{
			StatVar:           c.statVar,
			Places:            c.places,
			MeasurementMethod: c.mmethod,
		})
		if c.wantErr {
			if err == nil {
				t.Errorf("Expect GetStats to error out but it succeed")
			}
			continue
		}
		if err != nil {
			t.Errorf("could not GetStats: %s", err)
			continue
		}
		goldenFile := path.Join(goldenPath, c.goldenFile)
		if generateGolden {
			updateProtoGolden(resp, goldenFile)
			continue
		}

		var expected pb.GetStatResponse
		file, _ := ioutil.ReadFile(goldenFile)
		err = protojson.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file")
			continue
		}
		if c.partialMatch {
			for geo := range expected.Stat {
				for date := range expected.Stat[geo].Val {
					if resp.Stat[geo] == nil {
						t.Fatalf("result does not have data for geo %s", geo)
					}
					got := resp.Stat[geo].Val[date]
					want := expected.Stat[geo].Val[date]
					if c.statVar == "CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedOrProbableCase" {
						// Allow approximate match for NYT covid data.
						if math.Abs(float64(got)/float64(want)-1) > 0.05 {
							t.Errorf(
								"%s, %s, %s want: %f, got: %f", c.statVar, geo, date, want, got)
							continue
						}
					} else {
						if want != got {
							t.Errorf(
								"%s, %s, %s want: %f, got: %f", c.statVar, geo, date, want, got)
							continue
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
