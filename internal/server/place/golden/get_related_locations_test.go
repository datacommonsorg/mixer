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
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/test/e2e"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetRelatedLocations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "get_related_locations")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile   string
			dcid         string
			withinPlace  string
			statVarDcids []string
		}{
			{
				"county.json",
				"geoId/06085",
				"country/USA",
				[]string{
					"Count_Person",
					"Median_Income_Person",
					"Median_Age_Person",
					"UnemploymentRate_Person",
				},
			},
			{
				"state.json",
				"geoId/06",
				"country/USA",
				[]string{
					"Count_Person_Unemployed",
					"CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedOrProbableCase",
				},
			},
			{
				"crime.json",
				"geoId/06",
				"",
				[]string{"Count_CriminalActivities_CombinedCrime"},
			},
		} {
			req := &pb.GetRelatedLocationsRequest{
				Dcid:         c.dcid,
				StatVarDcids: c.statVarDcids,
				WithinPlace:  c.withinPlace,
			}
			resp, err := mixer.GetRelatedLocations(ctx, req)
			if err != nil {
				t.Errorf("could not GetRelatedLocations: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			c.goldenFile = "IG_" + c.goldenFile
			goldenFile := path.Join(goldenPath, c.goldenFile)
			if e2e.GenerateGolden {
				e2e.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GetRelatedLocationsResponse
			file, _ := ioutil.ReadFile(goldenFile)
			if err := protojson.Unmarshal(file, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := e2e.TestDriver(
		"GetRelatedLocations", &e2e.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
