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

	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBulkPropertyValuesOut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "bulk_property_values_out")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			property   string
			nodes      []string
			limit      int32
			token      string
		}{
			{
				"name.json",
				"name",
				[]string{"country/USA", "geoId/06", "dummy"},
				0,
				"",
			},
			{
				"containedIn.json",
				"containedInPlace",
				[]string{"country/USA", "geoId/06053", "geoId/06"},
				0,
				"",
			},
			{
				"geoOverlaps1.json",
				"geoOverlaps",
				[]string{"country/USA", "geoId/06053", "geoId/06"},
				5,
				"",
			},
			{
				"geoOverlaps2.json",
				"geoOverlaps",
				[]string{"country/USA", "geoId/06053", "geoId/06"},
				5,
				"H4sIAAAAAAAA/2zNsQrCMBCHcT1re54O8hd8IHVXKLhqLKcEm6Sk18G3lzpK5w++n9wFB5/a9PKNa+tOGx80SmmfTs/PqYYZiOcgJhAvQFyAeAniEgVX27EwiFcgFhCv5S2bYxqi3S6a+xRlH9T1Q9ag0U4aU/DRWcqyq82Z723Eri5792gV9BtOc8TVP/YFAAD//wEAAP//w808eM8AAAA=",
			},
		} {
			req := &pbv1.BulkPropertyValuesRequest{
				Property:  c.property,
				Nodes:     c.nodes,
				Direction: util.DirectionOut,
				Limit:     c.limit,
				NextToken: c.token,
			}
			resp, err := mixer.BulkPropertyValues(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.BulkPropertyValues/out: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pbv1.BulkPropertyValuesResponse
			if err := test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("Golden got diff: %v", diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"BulkPropertyValues/out", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
