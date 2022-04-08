// Copyright 2022 Google LLC
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
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestObservationsSeries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "series")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			variable   string
			entity     string
			goldenFile string
		}{
			{
				"Count_Person",
				"country/USA",
				"count_person.json",
			},
			{
				"Count_CriminalActivities_CombinedCrime",
				"geoId/06",
				"total_crimes.json",
			},
			{
				"Annual_Generation_Electricity",
				"geoId/06",
				"electricity_generation.json",
			},
			{
				"Median_Age_Person",
				"geoId/0649670",
				"median_age.json",
			},
			{
				"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
				"country/USA",
				"gdp.json",
			},
			{
				"dummy_sv",
				"dummy_place",
				"dummy.json",
			},
			{
				"Count_Person_Unemployed",
				"country/USA",
				"umemployed.json",
			},
		} {

			resp, err := mixer.ObservationsSeries(ctx, &pb.ObservationsSeriesRequest{
				Variable: c.variable,
				Entity:   c.entity,
			})
			if err != nil {
				t.Errorf("could not run ObservationsSeries: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.ObservationsSeriesResponse
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file: %s", err)
				continue
			}

			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"ObservationsSeries", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
