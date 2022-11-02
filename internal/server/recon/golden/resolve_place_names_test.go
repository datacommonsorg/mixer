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
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestResolvePlaceNames(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "resolve_place_names")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			req        *pb.ResolvePlaceNamesRequest
			goldenFile string
		}{
			{
				&pb.ResolvePlaceNamesRequest{
					Places: []*pb.ResolvePlaceNamesRequest_Place{
						{Name: "Santa Clara County"},
						{Name: "Santa Clara County", Type: "County"},
						/*
							{Name: "Cambridge"},
							{Name: "Cambridge", Type: "City"},
							{Name: "mountain view"},
							{Name: "Mountain View, CA"},
							// Typo on purpose.
							{Name: "mmountan view"},
							{Name: "non-existing place wow"},
						*/
					},
				},
				"result.json",
			},
		} {
			resp, err := recon.ResolvePlaceNames(ctx, c.req)
			if err != nil {
				t.Errorf("could not ResolvePlaceNames: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.ResolvePlaceNamesResponse
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			cmpOpts := cmp.Options{
				protocmp.Transform(),
				protocmp.SortRepeated(
					func(a, b *pb.ResolvePlaceNamesResponse_Place) bool {
						if a.GetName() == b.GetName() {
							return a.GetType() < b.GetType()
						}
						return a.GetName() < b.GetName()
					}),
			}
			if diff := cmp.Diff(resp, &expected, cmpOpts); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"ResolvePlaceNames", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
