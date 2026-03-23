// Copyright 2026 Google LLC
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
	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestV3FilterStatVarsByEntity(t *testing.T) {
	// TODO: Remove check once enabled.
	if !test.EnableSpannerGraph {
		return
	}
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Dir(filename)

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			desc       string
			statVars   []string
			entities   []string
			goldenFile string
		}{
			{
				"Basic filter",
				[]string{"Count_Person", "Median_Income_Person", "NonExistent"},
				[]string{"geoId/01"},
				"basic.json",
			},
			{
				"No match",
				[]string{"NonExistent_1", "NonExistent_2"},
				[]string{"geoId/01"},
				"no_match.json",
			},
			{
				"Multiple entities",
				[]string{"Count_Person"},
				[]string{"geoId/01", "geoId/02"},
				"multi_entity.json",
			},
		} {
			goldenFile := c.goldenFile
			statVars := []*pb.EntityInfo{}
			for _, sv := range c.statVars {
				statVars = append(statVars, &pb.EntityInfo{Dcid: sv})
			}
			resp, err := mixer.V3FilterStatVarsByEntity(ctx, &pb.FilterStatVarsByEntityRequest{
				StatVars: statVars,
				Entities: c.entities,
			})
			if err != nil {
				t.Errorf("Could not run V3FilterStatVarsByEntity: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, goldenFile)
				continue
			}
			var expected pb.FilterStatVarsByEntityResponse
			if err = test.ReadJSON(goldenPath, goldenFile, &expected); err != nil {
				t.Errorf("Could not Unmarshal golden file: %s", err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("%s: got diff: %s", c.desc, diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"TestV3FilterStatVarsByEntity",
		&test.TestOption{UseSQLite: true, UseSpannerGraph: true, EnableV3: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() for TestV3FilterStatVarsByEntity = %s", err)
	}
}
