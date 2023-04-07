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

	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestExistence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "existence")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			entities   []string
			variables  []string
			goldenFile string
		}{
			{
				[]string{"geoId/0647766", "geoId/06", "country/CAN"},
				[]string{"Count_Person", "GenderIncomeInequality_Person_15OrMoreYears_WithIncome", "dummy"},
				"existence.json",
			},
		} {
			goldenFile := c.goldenFile
			resp, err := mixer.V2Observation(ctx, &pbv2.ObservationRequest{
				Select:    []string{"variables", "entities"},
				Entities:  c.entities,
				Variables: c.variables,
			})
			if err != nil {
				t.Errorf("ObservationMetric(existence) = %s", err)
				continue
			}
			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateGolden(resp, goldenPath, goldenFile)
				continue
			}
			var expected pbv2.ObservationResponse
			if err = test.ReadJSON(goldenPath, goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file: %s", err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"ObservationMetric(existence)",
		&test.TestOption{UseMemdb: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
