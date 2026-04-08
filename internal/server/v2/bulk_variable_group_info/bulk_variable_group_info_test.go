// Copyright 2026 Google LLC
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

func TestV2BulkVariableGroupInfo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Dir(filename)

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			nodes                []string
			constrainedEntities  []string
			numEntitiesExistence int
			goldenFile           string
		}{
			{
				[]string{"dc/g/Agriculture", "dc/g/Housing"},
				[]string{"country/USA"},
				1,
				"bulk_variable_group_info.json",
			},
		} {
			resp, err := mixer.V2BulkVariableGroupInfo(ctx, &pbv1.BulkVariableGroupInfoRequest{
				Nodes:                c.nodes,
				ConstrainedEntities:  c.constrainedEntities,
				NumEntitiesExistence: int32(c.numEntitiesExistence),
			})
			if err != nil {
				t.Errorf("could not run V2BulkVariableInfo: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if true {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pbv1.BulkVariableGroupInfoResponse
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file: %s", err)
				continue
			}

			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}

		}
	}
	if err := test.TestDriver(
		"V2BulkVariableGroupInfo",
		&test.TestOption{FetchSVG: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
