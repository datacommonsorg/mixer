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
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBulkObservationDatesLinked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "observation_dates_linked")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			linkedEntity string
			entityType   string
			variables    []string
			goldenFile   string
		}{
			{
				"geoId/06",
				"County",
				[]string{"Count_Person", "dummy"},
				"CA_County.json",
			},
			{
				"country/USA",
				"State",
				[]string{"Count_Person", "Count_Person_Female"},
				"USA_State.json",
			},
			{
				"Earth",
				"Country",
				[]string{"Annual_Generation_Electricity"},
				"Earth_Country.json",
			},
		} {
			resp, err := mixer.BulkObservationDatesLinked(ctx, &pbv1.BulkObservationDatesLinkedRequest{
				LinkedEntity:   c.linkedEntity,
				EntityType:     c.entityType,
				Variables:      c.variables,
				LinkedProperty: "containedInPlace",
			})
			if err != nil {
				t.Errorf("could not run BulkObservationDatesLinked: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pbv1.BulkObservationDatesLinkedResponse
			if err := test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"BulkObservationDatesLinked",
		&test.TestOption{},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
