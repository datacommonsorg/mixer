// Copyright 2021 Google LLC
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

func TestGetStatSetSeriesWithinPlace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := e2e.Setup(&e2e.TestOption{UseCache: false, UseMemdb: false})
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "get_stat_set_series_within_place")

	for _, c := range []struct {
		parentPlace string
		childType   string
		statVars    []string
		goldenFile  string
	}{
		{
			"geoId/06085",
			"City",
			[]string{"Count_Person"},
			"county_city_population.json",
		},
	} {
		resp, err := client.GetStatSetSeriesWithinPlace(
			ctx, &pb.GetStatSetSeriesWithinPlaceRequest{
				ParentPlace: c.parentPlace,
				ChildType:   c.childType,
				StatVars:    c.statVars,
			})
		if err != nil {
			t.Errorf("could not GetStatSetSeriesWithinPlace: %s", err)
			continue
		}
		if e2e.GenerateGolden {
			e2e.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
			continue
		}

		var expected pb.GetStatSetSeriesResponse
		if err = e2e.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
			t.Errorf("Can not Unmarshal golden file")
			continue
		}
		if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
