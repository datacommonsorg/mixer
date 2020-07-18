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

package e2etest

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"
	"github.com/google/go-cmp/cmp"
)

func TestGetPropertyValues(t *testing.T) {
	ctx := context.Background()
	client, err := setup(server.NewMemcache(map[string][]byte{}))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "../golden_response/staging/get_property_values")

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
			[]string{"State", "geoId/05", "TotalPopulation", "dc/p/cmtdk79lnk2pd"},
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
		goldenFile := path.Join(goldenPath, c.goldenFile)

		var result map[string]map[string][]*server.Node
		err = json.Unmarshal([]byte(resp.GetPayload()), &result)
		if err != nil {
			t.Errorf("Can not Unmarshal payload")
			continue
		}

		if generateGolden {
			updateGolden(result, goldenFile)
			continue
		}

		var expected map[string]map[string][]*server.Node
		file, _ := ioutil.ReadFile(goldenFile)
		err = json.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
			continue
		}
		if diff := cmp.Diff(result, expected); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
