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
	"encoding/json"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
)

// TestGetLandingPageData tests GetLandingPageData.
func TestGetLandingPageData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := setup()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/get_landing_page_data")

	for _, c := range []struct {
		goldenFile string
		place      string
		seed       int64
		statVars   []string
	}{
		{
			"asm.json",
			"country/ASM",
			1,
			[]string{},
		},
		{
			"tha.json",
			"country/THA",
			1,
			[]string{},
		},
		{
			"county.json",
			"geoId/06085",
			1,
			[]string{"Count_HousingUnit_2000To2004DateBuilt"},
		},
		{
			"city.json",
			"geoId/0656938",
			1,
			[]string{"Median_GrossRent_HousingUnit_WithCashRent_OccupiedHousingUnit_RenterOccupied"},
		},
		{
			"zuid-nederland.json",
			"nuts/NL4",
			1,
			[]string{},
		},
	} {
		req := &pb.GetLandingPageDataRequest{
			Place:    c.place,
			StatVars: c.statVars,
			Seed:     c.seed,
		}
		resp, err := client.GetLandingPageData(ctx, req)
		if err != nil {
			t.Errorf("could not GetLandingPageData: %s", err)
			continue
		}

		goldenFile := path.Join(goldenPath, c.goldenFile)
		if generateGolden {
			updateProtoGolden(resp, goldenFile, true /* shared */)
			continue
		}

		var expected pb.GetLandingPageResponse
		bytes, err := readJSONShard(goldenPath, c.goldenFile)
		if err != nil {
			t.Errorf("Can not read golden file %s: %v", c.goldenFile, err)
			continue
		}
		err = json.Unmarshal(bytes, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
			continue
		}
		if diff := cmp.Diff(&resp, &expected); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
