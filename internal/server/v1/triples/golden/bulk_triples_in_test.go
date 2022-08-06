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

func TestBulkTriplesIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "bulk_triples_in")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			entities   []string
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
				"H4sIAAAAAAAA/6TOwUrDQBDG8SZddBkUwhR8DbFHD0JtL54sFL3KmI5lIJkNsxMhby+NqGCtl153+f3ng1s4X6Ze3Qa4NNIdP2jd9FvOENeWOjYfcIIhFlWBIZZVgWWcYoihKuDux5750PHjG8wW21ZUshu5vPPCmG5O9POj/vrQh6X8M3h+CL4ejpoV4L2kJu2kpmbTcS0t6zf/4w8nWMZ9ocQQp2MrwBNcjIde1mw5KVy1TLk3bll9xZpaUfJkMNs4uWTfB5/JhF4b/jXtMzqO+wAAAP//AQAA//9POo95vgEAAA==",
			},
		} {
			req := &pb.BulkTriplesRequest{
				Entities:  c.entities,
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
			var expected pb.BulkTriplesResponse
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
