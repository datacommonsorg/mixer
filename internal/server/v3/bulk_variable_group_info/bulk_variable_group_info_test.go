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

func TestV3BulkVariableGroupInfo(t *testing.T) {
	// TODO: Remove check once enabled.
	if !test.EnableSpannerGraph {
		return
	}
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Dir(filename)

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			req        *pbv1.BulkVariableGroupInfoRequest
			goldenFile string
		}{
			{
				&pbv1.BulkVariableGroupInfoRequest{
					Nodes: []string{"dc/g/Agriculture", "dc/g/SDG"},
				},
				"bulk_variable_group_info_stat_var_group_node.json",
			},
			{
				&pbv1.BulkVariableGroupInfoRequest{
					Nodes:                []string{"dc/g/Environment"},
					ConstrainedEntities:  []string{"country/USA", "country/IND", "country/CAN"},
					NumEntitiesExistence: 2,
				},
				"bulk_variable_group_info_filtered_svg.json",
			},
			{
				&pbv1.BulkVariableGroupInfoRequest{
					Nodes:               []string{"dc/topic/Demographics"},
					ConstrainedEntities: []string{"dc/s/WorldBank"},
				},
				"bulk_variable_group_info_filtered_topic.json",
			},
		} {
			resp, err := mixer.V3BulkVariableGroupInfo(ctx, c.req)
			if err != nil {
				t.Errorf("could not run V3BulkVariableGroupInfo: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if test.GenerateGolden {
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
		"V3BulkVariableGroupInfo",
		&test.TestOption{UseSpannerGraph: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
