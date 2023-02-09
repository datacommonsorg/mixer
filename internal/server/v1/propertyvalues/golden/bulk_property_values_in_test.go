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

func TestBulkPropertyValuesIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "bulk_property_values_in")

	testSuite := func(mixer pb.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			property   string
			nodes      []string
			limit      int32
			token      string
		}{
			{
				"containedIn1.json",
				"containedInPlace",
				[]string{"country/USA", "geoId/06053", "geoId/06"},
				0,
				"",
			},
			{
				"containedIn2.json",
				"containedInPlace",
				[]string{"country/USA", "geoId/06053", "geoId/06"},
				0,
				"H4sIAAAAAAAA/4zSX6+TMBgGcPmbnp5zDPaKK7/CNhO9NNnYXIiLEnFeeLNUeDObdG1Sysy+9j6BoTDj0LW7gRTCj6d9XrzDj5VshVanybac46SSQlMmoM5FwWkF+HUGommbTCpY0AbqUlPNGs0qyucKKPETj4TISzziI5+EKDDr0FyjxMOfXD94zuTh0Gom9t+lAKdXuLxXa5BrxWqz2s2WsB+ZPgquxI8u8Skvsqzn3k4d2Gfndss3GXC+gSPw2ZQE6TkgPuqxCJll70UoSs8BzjHag8zryfTdf7THvp2vilbaeXQbKxVmTJ9ImHjpA4k7JX0wTtw55tnlXdTd8Terlq44HEBoqk5l9VNKvmSNVuyOlAuriwv5C1TBqdDkxZ+DG7ewthrPhWJHqqEPZgLdgj5Yoaei/cFZdYezcgQyzlYz3nVwe1/2YRiN1t95/j3m91YqHvZ0HcVH4VC/h79Yv385Kr0bnbirPc0vedJ8GKl4MOPfAAAA//8BAAD//0RkN8eNBAAA",
			},
			{
				"typeOf.json",
				"typeOf",
				[]string{"Country", "State", "City"},
				100,
				"",
			},
			{
				"nasa_source.json",
				"isPartOf",
				[]string{"dc/s/UsNationalAeronauticsAndSpaceAdministrationNasa"},
				500,
				"",
			},
		} {
			req := &pb.BulkPropertyValuesRequest{
				Property:  c.property,
				Nodes:     c.nodes,
				Direction: util.DirectionIn,
				Limit:     c.limit,
				NextToken: c.token,
			}
			resp, err := mixer.BulkPropertyValues(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.BulkPropertyValues/in: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.BulkPropertyValuesResponse
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
		"BulkPropertyValues/in", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
