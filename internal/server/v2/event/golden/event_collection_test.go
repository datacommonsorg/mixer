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
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestEventCollection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "event_collection")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			node       string
			property   string
			goldenFile string
		}{
			{
				"geoId/06",
				"<-location{typeOf:EarthquakeEvent,date:2020-01}",
				"EarthquakeEvent_CA_202001.json",
			},
			{
				"Earth",
				"<-location{typeOf:EarthquakeEvent,date:2020-01}",
				"EarthquakeEvent_Earth_202001.json",
			},
			{
				"geoId/06",
				"<-location{typeOf:WildlandFireEvent,date:2022-10,area:0.1#10#SquareKilometer}",
				"FireEvent_CA_202210.json",
			},
			{
				"Earth",
				"<-location{typeOf:CycloneEvent,date:2010-05}",
				"CycloneEvent_Earth_201005.json",
			},
			{
				"country/BFA",
				"<-location{typeOf:DroughtEvent,date:2022-05,area:10#300000#SquareKilometer}",
				"DroughtEvent_BFA_202205.json",
			},
		} {
			resp, err := mixer.V2Event(ctx, &pbv2.EventRequest{
				Node:     c.node,
				Property: c.property,
			})
			if err != nil {
				t.Errorf("could not run Event: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pbv2.EventResponse
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
		"Event(EventCollection)",
		&test.TestOption{},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
