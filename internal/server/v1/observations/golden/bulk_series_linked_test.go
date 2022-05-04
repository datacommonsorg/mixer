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

func TestBulkObservationsSeriesLinked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "bulk_series_linked")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			entityType   string
			linkedEntity string
			variables    []string
			goldenFile   string
		}{
			{
				"County",
				"geoId/06",
				[]string{"dummy", "Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"},
				"CA_County.json",
			},
			{
				"City",
				"geoId/06",
				[]string{"Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"},
				"CA_City.json",
			},
			{
				"State",
				"country/USA",
				[]string{"Count_Person_FoodInsecure"},
				"US_State.json",
			},
			{
				"Country",
				"Earth",
				[]string{"Median_Age_Person"},
				"Country.json",
			},
			{
				"EpaReportingFacility",
				"geoId/06",
				[]string{"Annual_Emissions_GreenhouseGas_NonBiogenic"},
				"epa_facility.json",
			},
			{
				"AdministrativeArea2",
				"country/FRA",
				[]string{"Count_Person"},
				"FRA_AA2_2016.json",
			},
		} {
			for _, allFacets := range []bool{true, false} {
				goldenFile := c.goldenFile
				if allFacets {
					goldenFile = "all_" + goldenFile
				} else {
					goldenFile = "preferred_" + goldenFile
				}
				resp, err := mixer.BulkObservationsSeriesLinked(ctx, &pb.BulkObservationsSeriesLinkedRequest{
					Variables:      c.variables,
					EntityType:     c.entityType,
					LinkedEntity:   c.linkedEntity,
					LinkedProperty: "containedInPlace",
					AllFacets:      allFacets,
				})
				if err != nil {
					t.Errorf("could not run BulkObservationsSeriesLinked: %s", err)
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
		"BulkObservationsSeriesLinked",
		&test.TestOption{UseMemdb: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
