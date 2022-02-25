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

func TestGetPlaceStatDateWithinPlace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, opt := range []*e2e.TestOption{
		{UseCache: true, UseMemdb: true},
		{UseCache: true, UseMemdb: true, UseImportGroup: true},
	} {
		client, _, err := e2e.Setup()
		if err != nil {
			t.Fatalf("Failed to set up mixer and client")
		}
		_, filename, _, _ := runtime.Caller(0)
		goldenPath := path.Join(
			path.Dir(filename), "get_place_stat_date_within_place")

		for _, c := range []struct {
			ancestorPlace string
			placeType     string
			statVars      []string
			goldenFile    string
		}{
			{
				"geoId/06",
				"County",
				[]string{"Count_Person", "Median_Age_Person"},
				"CA_County.json",
			},
			{
				"country/USA",
				"State",
				[]string{"Count_Person", "Count_Person_Female"},
				"USA_State.json",
			},
		} {
			if opt.UseImportGroup {
				c.goldenFile = "IG_" + c.goldenFile
			}
			resp, err := client.GetPlaceStatDateWithinPlace(ctx, &pb.GetPlaceStatDateWithinPlaceRequest{
				AncestorPlace: c.ancestorPlace,
				PlaceType:     c.placeType,
				StatVars:      c.statVars,
			})
			if err != nil {
				t.Errorf("could not GetPlaceStatDateWithinPlace: %s", err)
				continue
			}
			if e2e.GenerateGolden {
				e2e.UpdateGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.GetPlaceStatDateWithinPlaceResponse
			if err := e2e.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}
}
