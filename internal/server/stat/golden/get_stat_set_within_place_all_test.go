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

func TestGetStatSetWithinPlaceAll(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "get_stat_set_within_place_all")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			parentPlace string
			childType   string
			date        string
			statVar     []string
			goldenFile  string
		}{
			{
				"geoId/06",
				"County",
				"",
				[]string{"Count_Person", "Median_Age_Person"},
				"CA_County.json",
			},
			{
				"country/USA",
				"State",
				"",
				[]string{"Count_Person", "Count_Person_Employed", "Annual_Generation_Electricity", "UnemploymentRate_Person"},
				"US_State.json",
			},
			{
				"geoId/06",
				"County",
				"2016",
				[]string{"Count_Person", "Median_Age_Person"},
				"CA_County_2016.json",
			},
			{
				"geoId/06085",
				"City",
				"",
				[]string{"Max_Temperature_RCP45"},
				"max_temprature.json",
			},
		} {

			resp, err := mixer.GetStatSetWithinPlaceAll(ctx, &pb.GetStatSetWithinPlaceRequest{
				ParentPlace: c.parentPlace,
				ChildType:   c.childType,
				StatVars:    c.statVar,
				Date:        c.date,
			})
			if err != nil {
				t.Errorf("could not GetStatSetWithinPlace: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			c.goldenFile = "IG_" + c.goldenFile
			if e2e.GenerateGolden {
				e2e.UpdateGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.GetStatSetAllResponse
			if err := e2e.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file: %s", err)
				continue
			}

			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := e2e.TestDriver(
		"GetStatSetWithinPlaceAll", &e2e.TestOption{UseCache: false, UseMemdb: false}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
