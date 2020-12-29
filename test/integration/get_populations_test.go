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

package integration

import (
	"context"
	"encoding/json"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server"
	"github.com/google/go-cmp/cmp"
)

func TestGetPopulations(t *testing.T) {
	ctx := context.Background()
	client, err := setup(server.NewMemcache(map[string][]byte{}))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}

	for _, c := range []struct {
		dcids          []string
		populationType string
		pvs            map[string]string
		want           []byte
	}{
		{
			[]string{"geoId/05", "geoId/06"},
			"Person",
			map[string]string{"gender": "Male", "age": "Years85Onwards"},
			[]byte(`[
				{
					"dcid":"geoId/05",
					"population":"dc/p/7j2me3p74sht1"
				},
				{
					"dcid":"geoId/06",
					"population":"dc/p/gpvt9t84mq3gg"
				}
			]`),
		},
	} {
		req := &pb.GetPopulationsRequest{
			Dcids:          c.dcids,
			PopulationType: c.populationType,
		}
		for p, v := range c.pvs {
			req.Pvs = append(req.Pvs, &pb.PropertyValue{Property: p, Value: v})
		}
		resp, err := client.GetPopulations(ctx, req)
		if err != nil {
			t.Errorf("could not GetPropertyValues: %s", err)
			continue
		}
		var result []*server.PlacePopInfo
		err = json.Unmarshal([]byte(resp.GetPayload()), &result)
		if err != nil {
			t.Errorf("Can not Unmarshal payload")
			continue
		}
		var expected []*server.PlacePopInfo
		err = json.Unmarshal(c.want, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal want: %v", err)
			continue
		}
		if diff := cmp.Diff(result, expected); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
