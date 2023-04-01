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
	"fmt"
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
		for idx, query := range []string{
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
		} {
			resp, err := mixer.RecognizePlaces(ctx, &pb.RecognizePlacesRequest{
				Query: query,
			})
			if err != nil {
				t.Errorf("RecognizePlaces(%s) = %s", query, err)
				continue
			}

			if latencyTest {
				continue
			}

			goldenFile := fmt.Sprintf("expected%d.json", idx)
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, goldenFile)
				continue
			}

			var expected pb.RecognizePlacesResponse
			if err = test.ReadJSON(goldenPath, goldenFile, &expected); err != nil {
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
