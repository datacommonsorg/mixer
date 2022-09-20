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

func TestBulkObservationsSeries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "bulk_series")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			variables          []string
			entities           []string
			customImportGroups []string
			allFacets          bool
			goldenFile         string
		}{
			{
				[]string{
					"dummy",
					"Count_Person",
					"Count_CriminalActivities_CombinedCrime",
					"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
					"Annual_Generation_Electricity",
					"Count_Person_Unemployed",
					"CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedOrProbableCase",
					"Count_Person_FoodInsecure",
				},
				[]string{"dummy", "country/FRA", "country/USA", "geoId/06", "geoId/0649670"},
				[]string{},
				true,
				"result.json",
			},
			{
				[]string{
					"Monthly_Generation_Electricity_CombustibleFuel",
				},
				[]string{"country/AUT"},
				[]string{"private_2022_09_18_22_25_46"},
				true,
				"custom.json",
			},
		} {
			for _, allFacets := range []bool{true, false} {
				goldenFile := c.goldenFile
				if allFacets {
					goldenFile = "all_" + goldenFile
				} else {
					goldenFile = "preferred_" + goldenFile
				}
				resp, err := mixer.BulkObservationsSeries(ctx, &pb.BulkObservationsSeriesRequest{
					Variables:          c.variables,
					Entities:           c.entities,
					CustomImportGroups: c.customImportGroups,
					AllFacets:          allFacets,
				})
				if err != nil {
					t.Errorf("could not run BulkObservationsSeries: %s", err)
					continue
				}

				if latencyTest {
					continue
				}

				if test.GenerateGolden {
					test.UpdateGolden(resp, goldenPath, goldenFile)
					continue
				}
				var expected pb.BulkObservationsSeriesResponse
				if err = test.ReadJSON(goldenPath, goldenFile, &expected); err != nil {
					t.Errorf("Can not Unmarshal golden file: %s", err)
					continue
				}

				if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
					t.Errorf("payload got diff: %v", diff)
					continue
				}
			}

		}
	}
	if err := test.TestDriver(
		"BulkObservationsSeries",
		&test.TestOption{UseMemdb: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
