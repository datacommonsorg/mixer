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
	"fmt"
	"path"
	"runtime"
	"testing"

	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

var categories = []string{
	"Crime",
	"Demographics",
	"Economics",
	"Education",
	"Energy",
	"Environment",
	"Equity",
	"Health",
	"Housing",
	"Overview",
}

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

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			node       string
			statVars   []string
		}{
			{
				"asm",
				"country/ASM",
				[]string{},
			},
			{
				"ca",
				"geoId/06",
				[]string{},
			},
			{
				"county",
				"geoId/06085",
				[]string{"Count_HousingUnit_2000To2004DateBuilt"},
			},
			{
				"dummy.json",
				"dummy",
				[]string{"Count_Person"},
			},
		} {
			if len(c.statVars) > 0 {
				// Test for additional stat vars, only use one category.
				categories = []string{"Overview"}
			}
			for _, category := range categories {
				req := &pbv1.PlacePageRequest{
					Node:        c.node,
					NewStatVars: c.statVars,
					Seed:        1,
					Category:    category,
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
					buildStrategy(3)).(*pbv1.PlacePageResponse)
				goldenFile := fmt.Sprintf("%s.%s.json", c.goldenFile, category)
				if test.GenerateGolden {
					test.UpdateProtoGolden(resp, goldenPath, goldenFile)
					continue
				}
				var expected pbv1.PlacePageResponse
				err = test.ReadJSON(goldenPath, goldenFile, &expected)
				if err != nil {
					t.Errorf("Can not read golden file %s: %v", goldenFile, err)
					continue
				}
				if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
					t.Errorf("%s, response got diff: %v", goldenFile, diff)
					continue
				}
			}
		}
	}

	if err := test.TestDriver(
		"PlacePage", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
