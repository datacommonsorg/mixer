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

func TestEventCollection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "event_collection")

	testSuite := func(mixer pb.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			eventType         string
			affectedPlaceDcid string
			date              string
			prop              string
			unit              string
			lowerLimit        float64
			upperLimit        float64
			goldenFile        string
		}{
			{
				"EarthquakeEvent",
				"geoId/06",
				"2020-01",
				"",
				"",
				0,
				0,
				"EarthquakeEvent_CA_202001.json",
			},
			{
				"EarthquakeEvent",
				"Earth",
				"2020-01",
				"",
				"",
				0,
				0,
				"EarthquakeEvent_Earth_202001.json",
			},
			{
				"FireEvent",
				"geoId/06",
				"2022-10",
				"area",
				"SquareKilometer",
				5,
				8,
				"FireEvent_CA_202210.json",
			},
			{
				"WildlandFireEvent",
				"geoId/06",
				"2022-10",
				"burnedArea",
				"Acre",
				10,
				1000,
				"WildlandFireEvent_CA_202210.json",
			},
			{
				"CycloneEvent",
				"Earth",
				"2010-05",
				"",
				"",
				0,
				0,
				"CycloneEvent_Earth_201005.json",
			},
			{
				"DroughtEvent",
				"country/BFA",
				"2022-04",
				"area",
				"SquareKilometer",
				10,
				100000,
				"DroughtEvent_BFA_202204.json",
			},
		} {
			resp, err := mixer.EventCollection(ctx, &pb.EventCollectionRequest{
				EventType:         c.eventType,
				AffectedPlaceDcid: c.affectedPlaceDcid,
				Date:              c.date,
				FilterProp:        c.prop,
				FilterLowerLimit:  c.lowerLimit,
				FilterUpperLimit:  c.upperLimit,
				FilterUnit:        c.unit,
			})
			if err != nil {
				t.Errorf("could not run EventCollection: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.EventCollectionResponse
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
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
		"EventCollection",
		&test.TestOption{},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
