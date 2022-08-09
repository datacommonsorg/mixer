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
				"H4sIAAAAAAAA/5TPUUvDQAwH8LU99AgK5QZ+DXHPIsztxScHQ18ldnEEermSS4V+e1lhDKwT9vLnuPD7k8AawjOnNu25wXbbUcORBK5s6Oj1669ZmIXSF6H0ZXC+qg8vB29ws0q92MeGNCeBu0iYe6VIYmuSFFnQksJ8a2ic7VD4jsr42VKYBeeLugjOl2NWY7q6gEe4Hmt1gFtF2dOLNG2/owx+o6kjteEf/XTSx3vmy11k4WyKxt+0VMKHXw2lry7yi7P+furdiicrn8BiCo4f58wPAAAA//8BAAD//xFoWBvAAQAA",
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
