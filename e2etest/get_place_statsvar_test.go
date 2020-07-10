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

package e2etest

import (
	"context"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"
)

func TestGetPlaceStatsVar(t *testing.T) {
	ctx := context.Background()
	client, err := setup(server.NewMemcache(map[string][]byte{}))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}

	dcids := []string{"geoId/05", "geoId/06885", "invalid"}
	want := map[string]struct {
		minCount  int
		statsVars []string
	}{
		"geoId/05":    {1000, []string{"TotalPopulation"}},
		"geoId/06858": {1000, []string{"Person_Count"}},
		"invalid":     {0, []string{}},
	}
	req := &pb.GetPlaceStatsVarRequest{
		Dcids: dcids,
	}
	resp, err := client.GetPlaceStatsVar(ctx, req)
	if err != nil {
		t.Errorf("could not GetPlaceStatsVar: %s", err)
	}
	for dcid, statsVars := range resp.Places {
		if len(statsVars.Values) < want[dcid].minCount {
			t.Errorf("%s has less than %d stats vars", dcid, want[dcid].minCount)
		}
		statsVarSet := map[string]bool{}
		for _, statsVar := range statsVars.Values {
			statsVarSet[statsVar] = true
		}
		for _, statsVar := range want[dcid].statsVars {
			if _, ok := statsVarSet[statsVar]; !ok {
				t.Errorf("%s is not in the stats var list of %s", statsVar, dcid)
				continue
			}
		}
	}
}
