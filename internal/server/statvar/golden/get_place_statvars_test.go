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

func TestGetPlaceStatVars(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "get_place_stat_vars")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			dcids      []string
			goldenFile string
			wanterr    bool
		}{
			{
				[]string{"geoId/05"},
				"california.json",
				false,
			},
			{
				[]string{"geoId/06085"},
				"santa_clara.json",
				false,
			},
			{
				[]string{"country/ALB"},
				"alb.json",
				false,
			},
			{
				[]string{"invalid"},
				"invalid.json",
				false,
			},
			{
				[]string{},
				"dummmy.json",
				true,
			},
		} {
			req := &pb.GetPlaceStatVarsRequest{
				Dcids: c.dcids,
			}
			resp, err := mixer.GetPlaceStatVars(ctx, req)
			if c.wanterr {
				if err == nil {
					t.Errorf("Expect to get error for GetPlaceStatsVar() but succeed")
				}
				continue
			}
			if err != nil {
				t.Errorf("Could not GetPlaceStatsVar: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			c.goldenFile = "IG_" + c.goldenFile
			if e2e.GenerateGolden {
				e2e.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GetPlaceStatVarsResponse
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

	if err := e2e.TestDriver(
		"GetPlaceStatVars", &e2e.TestOption{UseMemdb: true}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
