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
			goldenFile string
		}{
			{
				"Out properties",
				[]string{
					"Count_Person_Female",
					"foo",
					"test_var_1",
				},
				"->",
				"out_prop.json",
			},
			{
				"Out bracket props",
				[]string{
					"geoId/5129600",
				},
				"->[containedInPlace,geoJsonCoordinatesDP3]",
				"out_bracket_prop.json",
			},
			{
				"In properties",
				[]string{
					"Count_Person_Female",
					"foo",
					"test_var_1",
				},
				"<-",
				"in_prop.json",
			},
			{
				"All out property-values",
				[]string{
					"Count_Person_Female",
					"foo",
					"test_var_1",
				},
				"->*",
				"out_pv_all.json",
			},
			{
				"All in property-values",
				[]string{
					"test_var_1",
				},
				"<-*",
				"in_pv_all.json",
			},
			{
				"Some out property-values",
				[]string{
					"test_var_1",
				},
				"->[name, description]",
				"out_pv_some.json",
			},
			{
				"Some in property-values",
				[]string{
					"test_var_1",
				},
				"<-measuredProperty",
				"in_pv_some.json",
			},
			{
				"In property-values with filter",
				[]string{
					"country/USA",
				},
				"<-containedInPlace{typeOf:State}",
				"in_filter.json",
			},
			{
				"In chained property-values with filter",
				[]string{
					"geoId/06085",
				},
				"<-containedInPlace+{typeOf:City}",
				"in_chain_filter.json",
			},
		} {
			goldenFile := c.goldenFile
			resp, err := mixer.V3Node(ctx, &pbv2.NodeRequest{
				Nodes:    c.nodes,
				Property: c.property,
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

func TestV3NodePagination(t *testing.T) {
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
				"H4sIAAAAAAAA/+Ly5/IoLkjMy0st0i0oys9KTS4p1k9JLEnOz9UtLskvStXPzCsuScxLTi3WT0nWzU7XLUktLgGpSExKLAYLxqcXJRZkxBuaSjFzdKgDAAAA//8BAAD//8kn4TlRAAAA",
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
		"TestV3NodePagination",
		&test.TestOption{UseSQLite: true, UseSpannerGraph: true, EnableV3: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() for TestV3NodePagination = %s", err)
	}
}
