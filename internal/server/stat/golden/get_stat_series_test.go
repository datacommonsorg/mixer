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
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/test/e2e"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetStatSeries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, _, err := e2e.Setup()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "get_stat_series")

	for _, c := range []struct {
		statVar    string
		place      string
		goldenFile string
		mmethod    string
	}{
		{
			"Count_Person",
			"country/USA",
			"count_person.json",
			"CensusACS5yrSurvey",
		},
		{
			"Count_CriminalActivities_CombinedCrime",
			"geoId/06",
			"total_crimes.json",
			"",
		},
		{
			"Annual_Generation_Electricity",
			"geoId/06",
			"electricity_generation.json",
			"",
		},
		{
			"Median_Age_Person",
			"geoId/0649670",
			"median_age.json",
			"",
		},
		{
			"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
			"country/USA",
			"gdp.json",
			"",
		},
		{
			"dummy_sv",
			"geoId/06",
			"dummy_sv.json",
			"",
		},
		{
			"Count_Person",
			"dummy_place",
			"dummy_place.json",
			"",
		},
	} {
		resp, err := client.GetStatSeries(ctx, &pb.GetStatSeriesRequest{
			StatVar:           c.statVar,
			Place:             c.place,
			MeasurementMethod: c.mmethod,
		})
		if err != nil {
			t.Errorf("could not GetStatSeries: %s", err)
			continue
		}
		if e2e.GenerateGolden {
			e2e.UpdateGolden(resp, goldenPath, c.goldenFile)
			continue
		}
		var expected pb.GetStatSeriesResponse
		if err = e2e.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
			t.Errorf("Can not Unmarshal golden file: %s", err)
			continue
		}

		if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
