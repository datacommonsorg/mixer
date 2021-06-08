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
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
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
		goldenFile      string
		place           string
		seed            int64
		statVars        []string
		samplingRatio   float32
		samplingExclude []string
	}{
		{
			"asm.sample.json",
			"country/ASM",
			1,
			[]string{},
			0.3,
			[]string{},
		},
		{
			"tha.sample.json",
			"country/THA",
			1,
			[]string{},
			0.3,
			[]string{"country/USA"},
		},
		{
			"county.sample.json",
			"geoId/06085",
			1,
			[]string{"Count_HousingUnit_2000To2004DateBuilt"},
			0.1,
			[]string{"country/USA"},
		},
		{
			"city.sample.json",
			"geoId/0656938",
			1,
			[]string{"Median_GrossRent_HousingUnit_WithCashRent_OccupiedHousingUnit_RenterOccupied"},
			0.1,
			[]string{"country/USA"},
		},
		{
			"zuid-nederland.sample.json",
			"nuts/NL4",
			1,
			[]string{},
			0.5,
			[]string{"country/USA"},
		},
	} {
		strategy := &util.SamplingStrategy{
			Children: map[string]*util.SamplingStrategy{
				"statVarSeries": {
					Ratio:   c.samplingRatio,
					Exclude: c.samplingExclude,
				},
			},
		}

		req := &pb.GetLandingPageDataRequest{
			Place:       c.place,
			NewStatVars: c.statVars,
			Seed:        c.seed,
		}
		resp, err := client.GetLandingPageData(ctx, req)
		if err != nil {
			t.Errorf("could not GetLandingPageData: %s", err)
			continue
		}

		resp = util.Sample(resp, strategy).(*pb.GetLandingPageDataResponse)

		if generateGolden {
			updateProtoGolden(resp, goldenPath, c.goldenFile)
			continue
		}

		var expected pb.GetLandingPageDataResponse
		err = readJSON(goldenPath, c.goldenFile, &expected)
		if err != nil {
			t.Errorf("Can not read golden file %s: %v", c.goldenFile, err)
			continue
		}
		if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("%s, response got diff: %v", c.goldenFile, diff)
			continue
		}
	}
}
