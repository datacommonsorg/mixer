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

func TestGetRelatedPlacesTest(t *testing.T) {
	ctx := context.Background()
	client, err := setup(server.NewMemcache(map[string][]byte{}))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "../golden_response/staging/get_related_places")

	for _, c := range []struct {
		goldenFile string
		dcids      []string
		popType    string
		mprop      string
		statType   string
		pvs        map[string]string
	}{
		{
			"population.json",
			[]string{"geoId/06085"},
			"Person",
			"count",
			"measuredValue",
			nil,
		},
		{
			"income.json",
			[]string{"geoId/06085"},
			"Person",
			"income",
			"medianValue",
			map[string]string{
				"age":          "Years15Onwards",
				"incomeStatus": "WithIncome",
			},
		},
		{
			"age.json",
			[]string{"geoId/06085"},
			"Person",
			"age",
			"medianValue",
			nil,
		},
		{
			"unemployment.json",
			[]string{"geoId/06085"},
			"Person",
			"unemploymentRate",
			"measuredValue",
			nil,
		},
		{
			"crime.json",
			[]string{"geoId/06"},
			"CriminalActivities",
			"count",
			"measuredValue",
			map[string]string{
				"crimeType": "UCR_CombinedCrime",
			},
		},
	} {
		req := &pb.GetRelatedPlacesRequest{
			Dcids:            c.dcids,
			PopulationType:   c.popType,
			MeasuredProperty: c.mprop,
			StatType:         c.statType,
		}
		for p, v := range c.pvs {
			req.Pvs = append(req.Pvs, &pb.PropertyValue{
				Property: p,
				Value:    v,
			})
		}
		resp, err := client.GetRelatedPlaces(ctx, req)
		if err != nil {
			t.Errorf("could not GetRelatedPlaces: %s", err)
			continue
		}
		var result map[string]*server.RelatedPlacesInfo
		err = json.Unmarshal([]byte(resp.GetPayload()), &result)
		if err != nil {
			t.Errorf("Can not Unmarshal payload")
			continue
		}
		var expected map[string]*server.RelatedPlacesInfo
		file, _ := ioutil.ReadFile(path.Join(goldenPath, c.goldenFile))
		err = json.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
			continue
		}
		if diff := cmp.Diff(result, expected); diff == "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
