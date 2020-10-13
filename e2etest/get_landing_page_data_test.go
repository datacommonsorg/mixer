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
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"
	"github.com/google/go-cmp/cmp"
)

// TestGetLandingPageData tests GetLandingPageData.
func TestGetLandingPageData(t *testing.T) {
	ctx := context.Background()
	client, err := setup(server.NewMemcache(map[string][]byte{}))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "../golden_response/staging/get_landing_page_data")

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
			[]string{"Amount_Stock", "Count_Person_7To14Years_Employed_AsFractionOf_Count_Person_7To14Years"},
		},
		{
			"county.json",
			"geoId/06085",
			1,
			[]string{},
		},
		{
			"city.json",
			"geoId/0656938",
			1,
			[]string{"Median_GrossRent_HousingUnit_WithCashRent_OccupiedHousingUnit_RenterOccupied"},
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
		var result server.LandingPageResponse
		err = json.Unmarshal([]byte(resp.GetPayload()), &result)
		if err != nil {
			t.Errorf("Can not Unmarshal payload")
			continue
		}

		goldenFile := path.Join(goldenPath, c.goldenFile)
		if generateGolden {
			updateGolden(result, goldenFile)
			continue
		}

		var expected server.LandingPageResponse
		file, _ := ioutil.ReadFile(goldenFile)
		err = json.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
			continue
		}
		if diff := cmp.Diff(&result, &expected); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
