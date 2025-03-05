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
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestV3Node(t *testing.T) {
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
			desc       string
			nodes      []string
			property   string
			nextToken  string
			goldenFile string
		}{
			{
				"Out properties",
				[]string{
					"Count_Person_Female",
					"foo",
				},
				"->",
				"",
				"out_prop.json",
			},
			{
				"All out property-values",
				[]string{
					"Count_Person_Female",
					"foo",
				},
				"->*",
				"",
				"out_pv_all.json",
			},
			{
				"First page of pagination",
				[]string{
					"StatisticalVariable",
				},
				"<-typeOf",
				"",
				"pagination_first_page.json",
			},
			{
				"Second page of pagination",
				[]string{
					"StatisticalVariable",
				},
				"<-typeOf",
				"H4sIAAAAAAAA/2yQsW4TQRCGC3cpeYLV1HdcQ3UdTlASKcJWHJkCodPc7nBesju7mp2zOCFqJB6Q50Fr2aahHc38833/zZ/VzX3JyEzSZknfyGrpHKpNsS2ahDrPRZEtlc7Z9nVqlYrWDRyxnIbDJJgPw7s3v1fwa/UDdvNYYx7voIedovqi3mLYo3gcA0EDWyHnLSpBD7pk2nyFBjb/rt7HNLM+spJQ0WfKuERiHe5o1GEr/ohKtzVDkwzbeQzehuV+RkFWIjd8ShLcGvl1oweS88HwlHh6IYkfvisJYzilPRE7ktv6T5b1w+4KsscwV74TbjoS1w6gB2e7Kt49zBH5mdBVp6q5RynQwEeMde1CZaYrlgmJJ6Mk0dAZwTga1WTx1mcMRi6mRlOdVmxjz6LGs1njASOWt9DAy5KpQP/5vxV/+fkXAAD//wEAAP//dDdEwtkBAAA=",
				"pagination_second_page.json",
			},
		} {
			goldenFile := c.goldenFile
			resp, err := mixer.V3Node(ctx, &pbv2.NodeRequest{
				Nodes:     c.nodes,
				Property:  c.property,
				NextToken: c.nextToken,
			})
			if err != nil {
				t.Errorf("Could not run V3Node: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateGolden(resp, goldenPath, goldenFile)
				continue
			}
			var expected pbv2.NodeResponse
			if err = test.ReadJSON(goldenPath, goldenFile, &expected); err != nil {
				t.Errorf("Could not Unmarshal golden file: %s", err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("%s: got diff: %s", c.desc, diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"TestV3Node",
		&test.TestOption{UseSQLite: true, UseSpannerGraph: true, EnableV3: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() for TestV3Node = %s", err)
	}
}
