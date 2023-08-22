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

func TestBulkTriplesIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "bulk_triples_in")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			nodes      []string
			token      string
		}{
			{
				"result1.json",
				[]string{"Country", "BiologicalSpecimen", "Count_Person", "dummy"},
				"",
			},
			{
				"result2.json",
				[]string{"Country", "BiologicalSpecimen", "Count_Person", "dummy"},
				"H4sIAAAAAAAA/2zNQcvCMAzG8XdZt2XlPYwIfiD1rjAQb1JHlMLajjY7+O1l8erlR8hz+NubpYNPc3r5yc3jwpMPHG0r74XPz18b/RFgRYBAgDUBGgJsyGA7bN+OAJEAe1vs/zGtUe4XziVFuw/sypo5cJQTxxR8dJKy3Y3ixBfZIleXvXvMTDBUZLBSv3etGrVRW7VTUeP9BwAA//8BAAD//2gflEjPAAAA",
			},
		} {
			req := &pbv1.BulkTriplesRequest{
				Nodes:     c.nodes,
				Direction: "in",
				NextToken: c.token,
			}
			resp, err := mixer.BulkTriples(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.BulkTriplesIn: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pbv1.BulkTriplesResponse
			if err := test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", c.goldenFile, err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("Golden got diff: %v", diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"BulkTriplesIn", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
