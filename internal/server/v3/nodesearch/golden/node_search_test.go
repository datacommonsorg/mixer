// Copyright 2024 Google LLC
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
	pbv3 "github.com/datacommonsorg/mixer/internal/proto/v3"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestV3NodeSearch(t *testing.T) {
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
			req        *pbv3.NodeSearchRequest
			goldenFile string
		}{
			{
				req: &pbv3.NodeSearchRequest{
					Query: "income",
					Types: []string{"StatisticalVariable"},
				},
				goldenFile: "node_search_with_type.json",
			},
			{
				req: &pbv3.NodeSearchRequest{
					Query: "income",
				},
				goldenFile: "node_search_without_type.json",
			},
		} {
			goldenFile := c.goldenFile
			resp, err := mixer.V3NodeSearch(ctx, c.req)
			if err != nil {
				t.Errorf("Could not run V3NodeSearch: %s", err)
				continue
			}

			// Filter resp to top 20 matches to avoid flaky low matches.
			filteredResp := &pbv3.NodeSearchResponse{
				Nodes: resp.Nodes[:20],
			}

			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateGolden(filteredResp, goldenPath, goldenFile)
				continue
			}
			var expected pbv3.NodeSearchResponse
			if err = test.ReadJSON(goldenPath, goldenFile, &expected); err != nil {
				t.Errorf("Could not Unmarshal golden file: %s", err)
				continue
			}
			if diff := cmp.Diff(filteredResp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("%s: got diff: %s", goldenFile, diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"TestV3NodeSearch",
		&test.TestOption{UseSQLite: true, UseSpannerGraph: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() for TestV3NodeSearch = %s", err)
	}
}