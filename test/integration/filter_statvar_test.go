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

func TestFilterStatVar(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, err := setup(true)
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "golden_response/get_statvar_summary")

	for _, c := range []struct {
		svs        []string
		places     []string
		goldenFile string
	}{
		{
			[]string{"Median_Income_Person", "Median_Age_Person", "IncrementalCount_Person", "FertilityRate_Person_Female"},
			[]string{},
			"empty_places.json",
		},
		{
			[]string{"Median_Income_Person", "Median_Age_Person", "IncrementalCount_Person", "FertilityRate_Person_Female"},
			[]string{"geoId/06"},
			"california.json",
		},
		{
			[]string{"Median_Income_Person", "Median_Age_Person", "IncrementalCount_Person", "FertilityRate_Person_Female"},
			[]string{"geoId/06", "country/USA"},
			"california_usa.json",
		},
	} {
		resp, err := client.FilterStatVar(ctx, &pb.FilterStatVarRequest{
			StatVars: c.svs,
			Places:   c.places,
		})
		if err != nil {
			t.Errorf("could not FilterStatVar: %s", err)
			continue
		}
		if generateGolden {
			updateProtoGolden(resp, goldenPath, c.goldenFile)
			continue
		}

		var expected pb.FilterStatVarResponse
		if err = readJSON(goldenPath, c.goldenFile, &expected); err != nil {
			t.Errorf("Can not Unmarshal golden file")
			continue
		}

		if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
