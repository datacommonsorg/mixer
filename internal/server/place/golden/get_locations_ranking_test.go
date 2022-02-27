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
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/test/e2e"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetLocationsRankings(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "get_locations_ranking")

	testSuite := func(opt *e2e.TestOption, latencyTest bool) {
		client, _, err := e2e.Setup(opt)
		if err != nil {
			t.Fatalf("Failed to set up mixer and client")
		}

		for _, c := range []struct {
			goldenFile   string
			placeType    string
			withinPlace  string
			isPerCapita  bool
			statVarDcids []string
		}{
			{
				"country.json",
				"Country",
				"",
				false,
				[]string{
					"Count_Person",
					"Median_Income_Person",
				},
			},
			{
				"california.json",
				"County",
				"geoId/06",
				false,
				[]string{
					"Count_Person",
					"Median_Age_Person",
					"Count_CriminalActivities_CombinedCrime",
				},
			},
			{
				"crime_percapita.json",
				"City",
				"geoId/06",
				true,
				[]string{
					"Count_CriminalActivities_CombinedCrime",
				},
			},
		} {
			req := &pb.GetLocationsRankingsRequest{
				PlaceType:    c.placeType,
				WithinPlace:  c.withinPlace,
				IsPerCapita:  c.isPerCapita,
				StatVarDcids: c.statVarDcids,
			}
			response, err := client.GetLocationsRankings(ctx, req)
			if err != nil {
				t.Errorf("could not GetLocationsRankings: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if opt.UseImportGroup {
				c.goldenFile = "IG_" + c.goldenFile
			}
			if e2e.GenerateGolden {
				e2e.UpdateProtoGolden(response, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GetLocationsRankingsResponse
			if err := e2e.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
				continue
			}
			if diff := cmp.Diff(response, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := e2e.TestDriver(
		"GetLocationsRankings", &e2e.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
