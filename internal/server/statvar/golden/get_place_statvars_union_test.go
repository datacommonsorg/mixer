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
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/test"
)

func TestGetPlaceStatVarsUnionV1(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			dcids          []string
			statVars       []string
			want           []string
			shouldBeAbsent []string
			minCount       int
		}{
			{
				[]string{"geoId/06085", "country/JPN"},
				[]string{},
				[]string{"Count_Person", "GiniIndex_EconomicActivity"},
				[]string{},
				1000,
			},
			{
				[]string{"geoId/06", "country/USA"},
				[]string{"Median_Income_Person", "Median_Age_Person", "IncrementalCount_Person", "FertilityRate_Person_Female"},
				[]string{"Median_Age_Person", "Median_Income_Person", "FertilityRate_Person_Female"},
				[]string{"IncrementalCount_Person"},
				3,
			},
			{
				[]string{"geoId/06"},
				[]string{"Median_Income_Person", "Median_Age_Person", "IncrementalCount_Person", "FertilityRate_Person_Female"},
				[]string{"Median_Age_Person", "Median_Income_Person"},
				[]string{"IncrementalCount_Person", "FertilityRate_Person_Female"},
				2,
			},
			{
				[]string{"country/ALB", "country/ITA"},
				[]string{},
				[]string{"Test_Stat_Var_1", "Test_Stat_Var_2"},
				[]string{},
				2,
			},
		} {
			req := &pb.GetPlaceStatVarsUnionRequest{
				Dcids:    c.dcids,
				StatVars: c.statVars,
			}
			resp, err := mixer.GetPlaceStatVarsUnionV1(ctx, req)
			if err != nil {
				t.Errorf("Could not GetPlaceStatsVarUnionV1: %s", err)
				continue
			}
			if len(resp.StatVars) < c.minCount {
				t.Errorf("Less than %d stat vars", c.minCount)
			}
			statsVarSet := map[string]bool{}
			for _, statsVar := range resp.StatVars {
				statsVarSet[statsVar] = true
			}
			for _, statsVar := range c.want {
				if _, ok := statsVarSet[statsVar]; !ok {
					t.Errorf("%s is not in the stat vars union", statsVar)
					continue
				}
			}
			for _, statsVar := range c.shouldBeAbsent {
				if _, ok := statsVarSet[statsVar]; ok {
					t.Errorf("%s should not be in the stat vars union", statsVar)
					continue
				}
			}
		}
	}

	if err := test.TestDriver(
		"GetPlaceStatVarsUnionV1", &test.TestOption{UseMemdb: true}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
