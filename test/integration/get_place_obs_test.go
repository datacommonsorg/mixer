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
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetPlaceObs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := setup()
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/get_place_obs")

	for _, c := range []struct {
		placeType  string
		statVar    string
		date       string
		goldenFile string
	}{
		{
			"State",
			"Count_Person",
			"2019",
			"state.json",
		},
		{
			"County",
			"Count_Person_Male",
			"2018",
			"county.json",
		},
	} {
		req := &pb.GetPlaceObsRequest{
			PlaceType: c.placeType,
			StatVar:   c.statVar,
			Date:      c.date,
		}
		result, err := client.GetPlaceObs(ctx, req)
		if err != nil {
			t.Errorf("could not GetPlaceObs: %s", err)
			continue
		}

		if generateGolden {
			updateProtoGolden(result, goldenPath, c.goldenFile)
			continue
		}

		var expected pb.SVOCollection
		if err := readJSON(goldenPath, c.goldenFile, &expected); err != nil {
			t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
			continue
		}
		if diff := cmp.Diff(result, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
