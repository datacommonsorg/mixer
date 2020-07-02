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
	"github.com/datacommonsorg/mixer/store"
	"github.com/google/go-cmp/cmp"
)

func TestGetTriples(t *testing.T) {
	ctx := context.Background()
	client, err := Setup(ctx)
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "../golden_response/staging/get_triples")

	for _, c := range []struct {
		dcids        []string
		goldenFile   string
		partialMatch bool
		limit        int32
		count        []int
	}{
		{
			[]string{"State", "Country"},
			"place_type.json",
			false,
			-1,
			nil,
		},
		{
			[]string{"zip/00603"},
			"place.json",
			true,
			-1,
			nil,
		},
		{
			[]string{"dc/o/2brkkmq0lxd5h", "dc/o/10b2df1lqhz54"},
			"observation.json",
			false,
			-1,
			nil,
		},
		{
			[]string{"TotalPopulation", "MarriedPopulation"},
			"stats_var.json",
			false,
			-1,
			nil,
		},
		{
			[]string{"City", "County"},
			"",
			false,
			5,
			[]int{42, 22},
		},
	} {
		req := &pb.GetTriplesRequest{Dcids: c.dcids}
		if c.limit > 0 {
			req.Limit = c.limit
		}
		resp, err := client.GetTriples(ctx, req)
		if err != nil {
			t.Errorf("could not GetTriples: %s", err)
			continue
		}
		var result map[string][]*store.Triple
		err = json.Unmarshal([]byte(resp.GetPayload()), &result)
		if err != nil {
			t.Errorf("Can not Unmarshal payload")
			continue
		}
		if c.limit > 0 {
			for idx, place := range c.dcids {
				count := len(result[place])
				if count != c.count[idx] {
					t.Errorf(
						"Len of triples for %s expect %d, got %d",
						place, c.count[idx], count)
				}
			}
			continue
		}

		var expected map[string][]*store.Triple
		file, _ := ioutil.ReadFile(path.Join(goldenPath, c.goldenFile))
		err = json.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file")
			continue
		}
		if diff := cmp.Diff(result, expected); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
