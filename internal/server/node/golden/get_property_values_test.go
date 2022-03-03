// Copyright 2020 Google LLC
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
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/test/e2e"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetPropertyValues(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "get_property_values")

	testSuite := func(client pb.MixerClient, latencyTest, useImportGroup bool) {
		for _, c := range []struct {
			goldenFile string
			dcids      []string
			property   string
			direction  string
			valueType  string
			limit      int32
		}{
			{
				"name.json",
				[]string{"State", "geoId/05", "Count_Person", "dc/p/cmtdk79lnk2pd"},
				"name",
				"out",
				"",
				0,
			},
			{
				"contained_in_place.json",
				[]string{"geoId/06085", "geoId/0647766"},
				"containedInPlace",
				"",
				"City",
				0,
			},
			{
				"contained_in_place_all.json",
				[]string{"geoId/06085", "geoId/0647766"},
				"containedInPlace",
				"out",
				"",
				0,
			},
			{
				"location.json",
				[]string{"geoId/05", "geoId/06"},
				"location",
				"",
				"Election",
				0,
			},
			{
				"limit.json",
				[]string{"country/USA"},
				"name",
				"out",
				"",
				1,
			},
		} {
			req := &pb.GetPropertyValuesRequest{
				Dcids:     c.dcids,
				Property:  c.property,
				Direction: c.direction,
				ValueType: c.valueType,
			}
			if c.limit > 0 {
				req.Limit = c.limit
			}
			resp, err := client.GetPropertyValues(ctx, req)
			if err != nil {
				t.Errorf("could not GetPropertyValues: %s", err)
				continue
			}

			// Here the golden file is not same as the actual API output.
			// The actual payload is not a full serielized protobuf but
			// with the outer level ("data" field) removed. Here is to add that level
			// back so the payload can be unmarshaled.
			payload := "{\"data\":" + resp.GetPayload() + "}"
			var result pb.GetPropertyValuesResponse
			if err := protojson.Unmarshal([]byte(payload), &result); err != nil {
				t.Errorf("Can not Unmarshal payload")
				continue
			}

			if latencyTest {
				continue
			}

			if useImportGroup {
				c.goldenFile = "IG_" + c.goldenFile
			}
			goldenFile := path.Join(goldenPath, c.goldenFile)
			if e2e.GenerateGolden {
				e2e.UpdateProtoGolden(&result, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GetPropertyValuesResponse
			file, _ := ioutil.ReadFile(goldenFile)
			if err := protojson.Unmarshal(file, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
				continue
			}
			if diff := cmp.Diff(&result, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := e2e.TestDriver(
		"GetPropertyValues", &e2e.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
