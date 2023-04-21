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

	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBulkObservationsPointLinked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "bulk_point_linked")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			entityType   string
			linkedEntity string
			variables    []string
			date         string
			goldenFile   string
		}{
			{
				"County",
				"geoId/06",
				[]string{"dummy", "Count_Person", "Median_Age_Person", "NumberOfMonths_WetBulbTemperature_35COrMore_RCP45_MinRelativeHumidity"},
				"",
				"CA_County.json",
			},
			{
				"State",
				"country/USA",
				[]string{"Count_Person", "Count_Person_Employed", "Annual_Generation_Electricity", "UnemploymentRate_Person", "Count_Person_FoodInsecure"},
				"",
				"US_State.json",
			},
			{
				"Country",
				"Earth",
				[]string{"Count_Person"},
				"",
				"Country.json",
			},
			{
				"City",
				"geoId/06085",
				[]string{"Max_Temperature_RCP45"},
				"",
				"max_temprature.json",
			},
			{
				"EpaReportingFacility",
				"geoId/06",
				[]string{"Annual_Emissions_GreenhouseGas_NonBiogenic"},
				"",
				"epa_facility.json",
			},
			{
				"County",
				"geoId/06",
				[]string{"Count_Person", "Median_Age_Person"},
				"2015",
				"CA_County_2015.json",
			},
			{
				"AdministrativeArea2",
				"country/FRA",
				[]string{"Count_Person"},
				"2016",
				"FRA_AA2_2016.json",
			},
			{
				"DummyType",
				"country/FRA",
				[]string{"Count_Person"},
				"",
				"dummy_type.json",
			},
		} {
			for _, allFacets := range []bool{true, false} {
				goldenFile := c.goldenFile
				if allFacets {
					goldenFile = "all_" + goldenFile
				} else {
					goldenFile = "preferred_" + goldenFile
				}
				resp, err := mixer.BulkObservationsPointLinked(ctx, &pbv1.BulkObservationsPointLinkedRequest{
					Variables:      c.variables,
					EntityType:     c.entityType,
					Date:           c.date,
					LinkedEntity:   c.linkedEntity,
					LinkedProperty: "containedInPlace",
					AllFacets:      allFacets,
				})
				if err != nil {
					t.Errorf("could not run BulkObservationsPointLinked: %s", err)
					continue
				}
				if latencyTest {
					continue
				}
				if test.GenerateGolden {
					test.UpdateGolden(resp, goldenPath, goldenFile)
					continue
				}
				var expected pbv1.BulkObservationsPointResponse
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
		"BulkObservationsPointLinked",
		&test.TestOption{UseMemdb: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
