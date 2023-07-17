// Copyright 2023 Google LLC
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
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestTripleIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "triple_in")

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
				"H4sIAAAAAAAA/2zNQYvCMBDF8XY2276GPZRZ8AOpd4WCeJNYRgk2SUmnB7+9tAhevD3e//CzZ8tbn4Z0970bulF6HyTaSp+jHG7fGhdMKJlATPhhgmGD33Z5KibUTAATGvuwf7s0R70cJU8p2k0QN81ZgkTdS0zBR6cp2/9OnfpJF+TksnfXQbhgg7L9MAamLd+QQbWWmg2wruYFAAD//wEAAP//WA2jKccAAAA=",
			},
		} {
			req := &pbv2.NodeRequest{
				Nodes:     c.nodes,
				Property:  c.property,
				NextToken: c.token,
			}
			resp, err := mixer.V2Node(ctx, req)
			if err != nil {
				t.Errorf("could not run V2Node: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pbv2.NodeResponse
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
