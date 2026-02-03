// Copyright 2025 Google LLC
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
	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestV3Sparql(t *testing.T) {
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
			req        *pb.SparqlRequest
			goldenFile string
		}{
			{
				req: &pb.SparqlRequest{
					Query: `SELECT ?name ?state
					WHERE{
						?state typeOf State .
						?state containedInPlace country/USA .
						?state name ?name
					}
					ORDER BY ASC(?name)`,
				},
				goldenFile: "sparql.json",
			},
		} {
			goldenFile := c.goldenFile
			resp, err := mixer.V3Sparql(ctx, c.req)
			if err != nil {
				t.Errorf("Could not run V3Sparql: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if true {
				test.UpdateGolden(resp, goldenPath, goldenFile)
				continue
			}
			var expected pb.QueryResponse
			if err = test.ReadJSON(goldenPath, goldenFile, &expected); err != nil {
				t.Errorf("Could not Unmarshal golden file: %s", err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("%s: got diff: %s", goldenFile, diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"TestV3Sparql",
		&test.TestOption{UseSQLite: true, UseSpannerGraph: true, EnableV3: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() for TestV3Sparql = %s", err)
	}
}
