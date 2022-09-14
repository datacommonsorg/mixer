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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func buildStrategy(maxPlace int) *util.SamplingStrategy {
	return &util.SamplingStrategy{
		Children: map[string]*util.SamplingStrategy{
			"statVarSeries": {
				MaxSample: maxPlace,
			},
		},
	}
}

func TestPlacePage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "place_page")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			node       string
			seed       int64
			statVars   []string
			maxPlace   int
			category   string
		}{
			{
				"asm.sample.json",
				"country/ASM",
				1,
				[]string{},
				3,
				"",
			},
			{
				"ca_economics.sample.json",
				"geoId/06",
				1,
				[]string{},
				3,
				"Economics",
			},
			{
				"ca_overview.sample.json",
				"geoId/06",
				1,
				[]string{},
				3,
				"Overview",
			},
			{
				"tha.sample.json",
				"country/THA",
				1,
				[]string{},
				5,
				"",
			},
			{
				"county.sample.json",
				"geoId/06085",
				1,
				[]string{"Count_HousingUnit_2000To2004DateBuilt"},
				3,
				"",
			},
			{
				"state.sample.json",
				"geoId/06",
				1,
				[]string{"Annual_Generation_Electricity"},
				3,
				"",
			},
			{
				"city.sample.json",
				"geoId/0656938",
				1,
				[]string{"Median_GrossRent_HousingUnit_WithCashRent_OccupiedHousingUnit_RenterOccupied"},
				3,
				"",
			},
			{
				"zuid-nederland.sample.json",
				"nuts/NL4",
				1,
				[]string{},
				5,
				"",
			},
			{
				"dummy.json",
				"dummy",
				1,
				[]string{},
				5,
				"",
			},
		} {
			req := &pb.PlacePageRequest{
				Node:        c.node,
				NewStatVars: c.statVars,
				Seed:        c.seed,
				Category:    c.category,
			}
			resp, err := mixer.PlacePage(ctx, req)
			if err != nil {
				t.Errorf("could not GetPlacePageData: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			resp = util.Sample(
				resp,
				buildStrategy(c.maxPlace)).(*pb.GetPlacePageDataResponse)

			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GetPlacePageDataResponse
			err = test.ReadJSON(goldenPath, c.goldenFile, &expected)
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

	if err := test.TestDriver(
		"PlacePage", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
