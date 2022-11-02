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
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestResolveCoordinates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "resolve_coordinates")

	testSuite := func(mixer pb.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			req        *pb.ResolveCoordinatesRequest
			goldenFile string
		}{
			{
				&pb.ResolveCoordinatesRequest{
					Coordinates: []*pb.ResolveCoordinatesRequest_Coordinate{
						{
							Latitude:  37.42,
							Longitude: -122.08,
						},
						{
							Latitude:  32.41,
							Longitude: -102.11,
						},
					},
				},
				"result.json",
			},
		} {
			resp, err := mixer.ResolveCoordinates(ctx, c.req)
			if err != nil {
				t.Errorf("could not ResolveCoordinates: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.ResolveCoordinatesResponse
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			cmpOpts := cmp.Options{
				protocmp.Transform(),
				protocmp.SortRepeated(
					func(a, b *pb.ResolveCoordinatesResponse_PlaceCoordinate) bool {
						if a.GetLatitude() == b.GetLatitude() {
							return a.GetLongitude() > b.GetLongitude()
						}
						return a.GetLatitude() > b.GetLatitude()
					}),
			}
			if diff := cmp.Diff(resp, &expected, cmpOpts); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"ResolveCoordinates", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
