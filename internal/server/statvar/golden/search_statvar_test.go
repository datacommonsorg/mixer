// Copyright 2021 Google LLC
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
	"github.com/datacommonsorg/mixer/test/e2e"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestSearchStatVar(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, _, err := e2e.Setup(&e2e.TestOption{UseCache: true, UseMemdb: false})
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "search_statvar")

	for _, c := range []struct {
		query           string
		places          []string
		enableBlocklist bool
		goldenFile      string
	}{
		{
			"Asian , age",
			[]string{"geoId/06"},
			false,
			"asian_age.json",
		},
		{
			"crime",
			[]string{},
			false,
			"crime.json",
		},
		{
			"female",
			[]string{"country/USA"},
			false,
			"female.json",
		},
		{
			"accommodation food services",
			[]string{"country/USA"},
			true,
			"accommodation_food_services_blocklist.json",
		},
		{
			"accommodation food services",
			[]string{"country/USA"},
			false,
			"accommodation_food_services.json",
		},
		{
			"food stamp",
			[]string{},
			false,
			"food_stamp.json",
		},
		{
			"fem",
			[]string{},
			false,
			"fem.json",
		},
		{
			"women",
			[]string{},
			false,
			"women.json",
		},
	} {
		resp, err := client.SearchStatVar(ctx, &pb.SearchStatVarRequest{
			Query:           c.query,
			Places:          c.places,
			EnableBlocklist: c.enableBlocklist,
		})
		if err != nil {
			t.Errorf("could not SearchStatVar: %s", err)
			continue
		}

		if e2e.GenerateGolden {
			e2e.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
			continue
		}

		var expected pb.SearchStatVarResponse
		if err = e2e.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
			t.Errorf("Can not Unmarshal golden file")
			continue
		}

		if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
