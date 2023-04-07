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
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestFetchFromCollection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "collection")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			variables          []string
			entitiesExpression string
			date               string
			goldenFile         string
		}{
			{
				[]string{"dummy", "Count_Person", "Median_Age_Person"},
				"geoId/06<-containedInPlace+{typeOf:County}",
				"LATEST",
				"CA_County.json",
			},
			{
				[]string{"dummy", "Count_Person", "Median_Age_Person", "NumberOfMonths_WetBulbTemperature_35COrMore_RCP45_MinRelativeHumidity"},
				"country/USA<-containedInPlace+{typeOf:County}",
				"LATEST",
				"USA_County.json",
			},
			{
				[]string{"Count_Person", "Count_Person_Employed", "Annual_Generation_Electricity", "UnemploymentRate_Person", "Count_Person_FoodInsecure"},
				"country/USA<-containedInPlace+{typeOf:State}",
				"LATEST",
				"US_State.json",
			},
			{
				[]string{"Count_Person"},
				"Earth<-containedInPlace+{typeOf:Country}",
				"LATEST",
				"Country.json",
			},
			{
				[]string{"Max_Temperature_RCP45"},
				"geoId/06085<-containedInPlace+{typeOf:City}",
				"LATEST",
				"max_temprature.json",
			},
			{
				[]string{"Annual_Emissions_GreenhouseGas_NonBiogenic"},
				"geoId/06<-containedInPlace+{typeOf:EpaReportingFacility}",
				"LATEST",
				"epa_facility.json",
			},
			{
				[]string{"Count_Person", "Median_Age_Person"},
				"geoId/06<-containedInPlace+{typeOf:County}",
				"2015",
				"CA_County_2015.json",
			},
			{
				[]string{"Count_Person"},
				"country/FRA<-containedInPlace+{typeOf:AdministrativeArea2}",
				"2016",
				"FRA_AA2_2016.json",
			},
			{
				[]string{"Count_Person"},
				"country/FRA<-containedInPlace+{typeOf:DummyType}",
				"LATEST",
				"dummy_type.json",
			},
		} {
			goldenFile := c.goldenFile
			resp, err := mixer.V2Observation(ctx, &pbv2.ObservationRequest{
				Select:             []string{"variables", "entities", "date", "value"},
				Variables:          c.variables,
				EntitiesExpression: c.entitiesExpression,
				Date:               c.date,
			})
			if err != nil {
				t.Errorf("could not run V2Observation (collection): %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateGolden(resp, goldenPath, goldenFile)
				continue
			}
			var expected pbv2.ObservationResponse
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
	if err := test.TestDriver(
		"Collection",
		&test.TestOption{UseMemdb: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
