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

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestBulkVariableGroupInfo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "bulk_variable_group_info")

	testSuite := func(mixer pb.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			nodes                []string
			constrainedEntities  []string
			numEntitiesExistence int32
			goldenFile           string
		}{
			{
				[]string{"dc/g/Economy", "invalid"},
				[]string{"country/ASM"},
				1,
				"economy.json",
			},
			{
				[]string{"dc/g/CriminalActivities"},
				[]string{"country/USA", "country/MEX", "country/BRA", "country/DEU", "country/POL", "country/RUS", "country/ZAF", "country/ZWE", "country/CHN", "country/IND", "country/AUS"},
				10,
				"crime_10.json",
			},
			{
				[]string{"dc/g/CriminalActivities"},
				[]string{"country/USA", "country/MEX", "country/BRA", "country/DEU", "country/POL", "country/RUS", "country/ZAF", "country/ZWE", "country/CHN", "country/IND", "country/AUS"},
				1,
				"crime_1.json",
			},
		} {
			resp, err := mixer.BulkVariableGroupInfo(ctx, &pb.BulkVariableGroupInfoRequest{
				Nodes:                c.nodes,
				ConstrainedEntities:  c.constrainedEntities,
				NumEntitiesExistence: c.numEntitiesExistence,
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

			var expected pb.BulkVariableGroupInfoResponse
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
		"BulkVariableGroupInfo", &test.TestOption{UseCache: true}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
