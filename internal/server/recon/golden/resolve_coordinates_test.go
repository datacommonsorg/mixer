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

func TestResolveCoordinates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, opt := range []*e2e.TestOption{
		{},
		{UseImportGroup: true},
	} {
		_, client, err := e2e.Setup(opt)
		if err != nil {
			t.Fatalf("Failed to set up recon client: %s", err)
		}
		_, filename, _, _ := runtime.Caller(0)
		goldenPath := path.Join(
			path.Dir(filename), "resolve_coordinates")

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
			if opt.UseImportGroup {
				c.goldenFile = "IG_" + c.goldenFile
			}
			resp, err := client.ResolveCoordinates(ctx, c.req)
			if err != nil {
				t.Errorf("could not ResolveCoordinates: %s", err)
				continue
			}

			if e2e.GenerateGolden {
				e2e.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.ResolveCoordinatesResponse
			if err = e2e.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
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
}
