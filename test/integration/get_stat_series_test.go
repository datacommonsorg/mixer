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
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetStatSeries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := setup()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	clientStatVar, err := setupStatVar()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/staging/get_stat_series")

	for _, c := range []struct {
		statVar    string
		place      string
		goldenFile string
		mmethod    string
		wantErr    bool
	}{
		{
			"Count_Person",
			"country/USA",
			"count_person.json",
			"CensusACS5yrSurvey",
			false,
		},
		{
			"Count_CriminalActivities_CombinedCrime",
			"geoId/06",
			"total_crimes.json",
			"",
			false,
		},
		{
			"Median_Age_Person",
			"geoId/0649670",
			"median_age.json",
			"",
			false,
		},
		{
			"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
			"country/USA",
			"gdp.json",
			"",
			false,
		},
		{
			"BadStatsVar",
			"geoId/06",
			"",
			"",
			true,
		},
		{
			"Count_Person",
			"BadPlace",
			"",
			"",
			true,
		},
	} {
		for index, client := range []pb.MixerClient{client, clientStatVar} {
			resp, err := client.GetStatSeries(ctx, &pb.GetStatSeriesRequest{
				StatVar:           c.statVar,
				Place:             c.place,
				MeasurementMethod: c.mmethod,
			})
			if c.wantErr {
				if err == nil {
					t.Errorf("Expect GetStatSeries to error out but it succeed")
				}
				continue
			}
			if err != nil {
				t.Errorf("could not GetStatSeries: %s", err)
				continue
			}
			goldenFile := path.Join(goldenPath, c.goldenFile)
			isPopObsMode := (index == 0)
			if isPopObsMode && generateGolden {
				updateGolden(resp, goldenFile)
				continue
			}
			var expected pb.GetStatSeriesResponse
			file, _ := ioutil.ReadFile(goldenFile)
			err = protojson.Unmarshal(file, &expected)
			if err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}
}
