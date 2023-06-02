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
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestVariableGroupInfo(t *testing.T) {
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "variable_group_info")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			node                string
			constrainedEntities []string
			goldenFile          string
		}{
			{
				"dc/g/Person_EnrollmentLevel-EnrolledInCollegeUndergraduateYears_Race",
				[]string{"country/USA"},
				"school_race.json",
			},
			{
				"dc/g/Demographics",
				[]string{},
				"demographics.json",
			},
			{
				"dc/g/Weather",
				[]string{},
				"weather.json",
			},
			{
				"dc/g/Demographics",
				[]string{"country/GBR"},
				"demographics_gbr.json",
			},
			// Run this first to test the server cache is not modified.
			{
				"dc/g/Root",
				[]string{"geoId/0649670"},
				"root_mtv.json",
			},
			{
				"dc/g/Root",
				[]string{"geoId/0649670", "country/JPN"},
				"root_mtv_jpn.json",
			},
			{
				"dc/g/Root",
				[]string{},
				"root.json",
			},
			{
				"invalid,id",
				[]string{},
				"empty.json",
			},
		} {
			resp, err := mixer.VariableGroupInfo(ctx, &pbv1.VariableGroupInfoRequest{
				Node:                c.node,
				ConstrainedEntities: c.constrainedEntities,
			})
			if err != nil {
				t.Errorf("VariableGroupInfo() = %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pbv1.VariableGroupInfoResponse
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"VariableGroupInfo",
		&test.TestOption{UseCache: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
