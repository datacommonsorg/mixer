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

func TestCalculation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "calculation")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			desc       string
			variables  []string
			entities   []string
			date       string
			filter     *pbv2.FacetFilter
			goldenFile string
		}{
			{
				"basic",
				[]string{
					"Count_Person_Female",
					"Count_Person_Male",
					"Count_WetBulbTemperatureEvent",
				},
				[]string{"wikidataId/Q613"},
				"",
				nil,
				"basic.json",
			},
			{
				"empty",
				[]string{
					"Count_Farm",
					"Count_Person_1OrMoreYears_DifferentHouseAbroad",
				},
				[]string{"wikidataId/Q613"},
				"",
				nil,
				"empty.json",
			},
			{
				"two places",
				[]string{
					"Count_Person_Female",
				},
				[]string{
					"wikidataId/Q613",
					"wikidataId/Q187712",
				},
				"",
				nil,
				"two_places.json",
			},
			{
				"custom formula",
				[]string{
					"test_var_2",
				},
				[]string{"wikidataId/Q613"},
				"",
				nil,
				"custom_formula.json",
			},
			{
				"custom data", // Will be empty till DerivedSeries supports Custom DC.
				[]string{
					"test_var_3",
				},
				[]string{
					"geoId/01",
					"geoId/06",
				},
				"",
				nil,
				"custom_data.json",
			},
		} {
			goldenFile := c.goldenFile
			resp, err := mixer.V2Observation(ctx, &pbv2.ObservationRequest{
				Select:   []string{"variable", "entity", "date", "value"},
				Variable: &pbv2.DcidOrExpression{Dcids: c.variables},
				Entity:   &pbv2.DcidOrExpression{Dcids: c.entities},
				Date:     c.date,
				Filter:   c.filter,
			})
			if err != nil {
				t.Errorf("could not run V2Observation (direct): %s", err)
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
				t.Errorf("%s: got diff: %s", c.desc, diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"TestCalculation",
		&test.TestOption{UseSQLite: true, CacheSVFormula: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
