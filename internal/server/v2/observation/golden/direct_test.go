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

func TestFetchDirect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "direct")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			variables  []string
			entities   []string
			date       string
			filter     *pbv2.FacetFilter
			goldenFile string
		}{
			{
				[]string{
					"dummy",
					"Count_Person",
					"Median_Age_Person",
					"WithdrawalRate_Water_Aquaculture",
					"Count_CriminalActivities_CombinedCrime",
					"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
					"Annual_Generation_Electricity",
					"Count_Person_Unemployed",
					"test_var_1",
				},
				[]string{
					"dummy",
					"country/FRA",
					"country/USA",
					"geoId/06",
					"geoId/0649670",
				},
				"",
				nil,
				"all.json",
			},
			{
				[]string{
					"Count_Person",
					"Count_CriminalActivities_CombinedCrime",
					"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
					"Annual_Generation_Electricity",
					"Count_Person_Unemployed",
					"test_var_1",
				},
				[]string{"dummy", "country/FRA", "country/USA", "geoId/06", "geoId/0649670"},
				"2015",
				nil,
				"2015.json",
			},
			{
				[]string{
					"Count_Person",
					"Count_CriminalActivities_CombinedCrime",
					"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
					"Annual_Generation_Electricity",
					"Count_Person_Unemployed",
					"test_var_1",
				},
				[]string{"dummy", "country/FRA", "country/USA", "geoId/06", "geoId/0649670"},
				"2010",
				nil,
				"2010.json",
			},
			{
				[]string{
					"Area_Farm",
					"Count_Person",
					"Count_CriminalActivities_CombinedCrime",
					"Amount_EconomicActivity_GrossNationalIncome_PurchasingPowerParity_PerCapita",
					"Annual_Generation_Electricity",
					"Count_Person_Unemployed",
					"AirQualityIndex_AirPollutant",
					"test_var_1",
				},
				[]string{
					"dummy",
					"country/FRA",
					"country/USA",
					"geoId/06",
					"geoId/0649670",
				},
				"LATEST",
				nil,
				"latest.json",
			},
			{
				[]string{
					"Count_Person",
				},
				[]string{"country/USA"},
				"2018-01",
				nil,
				"empty.json",
			},
			{
				[]string{
					"Count_Person",
				},
				[]string{"country/USA"},
				"LATEST",
				&pbv2.FacetFilter{
					Domains: []string{"oecd.org"},
				},
				"filter.json",
			},
			{
				[]string{
					"Count_Person",
				},
				[]string{"country/USA"},
				"LATEST",
				&pbv2.FacetFilter{
					Domains: []string{"oecd.org", "cdc.gov"},
				},
				"multi_filter.json",
			},
			{
				[]string{
					"Count_Person", "Median_Age_Person",
				},
				[]string{"country/USA"},
				"LATEST",
				&pbv2.FacetFilter{
					FacetIds: []string{"1151455814"},
				},
				"facet_id_filter.json",
			},
			{
				[]string{
					"Count_Person", "Median_Age_Person",
				},
				[]string{"country/USA"},
				"LATEST",
				&pbv2.FacetFilter{
					FacetIds: []string{"1151455814", "1152061738"},
				},
				"multi_facet_id_filter.json",
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
				t.Errorf("payload got diff(%s): %v", goldenFile, diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"FetchDirect",
		&test.TestOption{UseSQLite: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
