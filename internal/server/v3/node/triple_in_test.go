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

func TestTripleIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "../../v2/propertyvalues/golden/triple_in")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			nodes      []string
			property   string
			token      string
		}{
			{
				"result1.json",
				[]string{"Country", "BiologicalSpecimen", "Count_Person", "dummy", "NewCity"},
				"<-*",
				"",
			},
			{
				"result2.json",
				[]string{"Country", "BiologicalSpecimen", "Count_Person", "dummy"},
				"<-*",
				"H4sIAAAAAAAA/2zNsQrCMBCHcT1re54O8hd8IHVXKLhqLKcEm6Sk18G3lzpK5w++n9wFB5/a9PKNa+tOGx80SmmfTs/PqYYZiOcgJhAvQFyAeAniEgVX27EwiFcgFhCv5S2bYxqi3S6a+xRlH9T1Q9ag0U4aU/DRWcqyq82Z723Eri5792gV9BtOc8TVP/YFAAD//wEAAP//w808eM8AAAA=",
			},
		} {
			req := &pbv3.NodeRequest{
				Nodes:     c.nodes,
				Property:  c.property,
				NextToken: c.token,
			}
			resp, err := mixer.V3Node(ctx, req)
			if err != nil {
				t.Errorf("could not run V3Node: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				// Don't update V2 goldens.
				// TODO: Update test for V3.
				continue
			}
			var expected pbv3.NodeResponse
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
		"TestTripleIn", &test.TestOption{UseSQLite: true}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
