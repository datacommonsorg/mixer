// Copyright 2023 Google LLC
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

func TestRecognizePlaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "recognize_places")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			queries    []string
			goldenFile string
		}{
			{
				[]string{
					"median income in africa",
					"economy of Asia",
					"tell me about chicago",
					"tell me about palo alto",
					"what about mountain view",
					"crime in new york state",
					"California economy and Florida",
					"life expectancy in Australia and Canada",
					"life expectancy in New York city and Alabama",
					"the birds in San Jose are chirpy",
					"the birds in San Jose, California are chirpy",
					"the birds in San Jose California are chirpy",
					"the birds in San Jose, Mountain View and Sunnyvale are chirpy",
					"the birds in ME and USA are chirpy, according to me",
					"I want to find the Middle Point of a line",
					"I went to Middle Point, USA",
					"I went to Half Moon Bay California and California, Washington County",
					"What is the electricity access in african countries",
					"Compare literacy among Chinese provinces",
					// This should only match California
					"Chinese speakers in California",
					"population in 94043 vs sunnyvale",
				},
				"result.json",
			},
		} {
			resp, err := mixer.RecognizePlaces(ctx, &pb.RecognizePlacesRequest{
				Queries: c.queries,
			})
			if err != nil {
				t.Errorf("RecognizePlaces() = %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.RecognizePlacesResponse
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			cmpOpts := cmp.Options{
				protocmp.Transform(),
			}
			if diff := cmp.Diff(resp, &expected, cmpOpts); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"RecognizePlaces", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
