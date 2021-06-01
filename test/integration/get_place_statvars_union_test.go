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
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

func TestGetPlaceStatVarsUnionV1(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := setup()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}

	for _, c := range []struct {
		dcids    []string
		want     []string
		minCount int
	}{
		{
			[]string{"geoId/06085", "country/JPN"},
			[]string{"Count_Person", "GiniIndex_EconomicActivity"},
			1000,
		},
	} {
		req := &pb.GetPlaceStatVarsUnionRequest{
			Dcids: c.dcids,
		}
		resp, err := client.GetPlaceStatVarsUnionV1(ctx, req)
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
	}
}
