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

func TestPropertyValuesIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "property_values_in")

	testSuite := func(mixer pb.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			property   string
			node       string
			limit      int32
			token      string
		}{
			{
				"containedIn1.json",
				"containedInPlace",
				"geoId/06",
				502,
				"",
			},
			{
				"containedIn2.json",
				"containedInPlace",
				"geoId/06",
				500,
				"H4sIAAAAAAAA/5zSzUoDMRAHcBPTkMYPQk77FlYPHj24Vl3wEGh9gJgOGhgTyKaVvr1kXQWLpuIpMJAf//mQd1I8Q+xWZ7NLqVwM2foAqy4YtA7kUQuhX/fLZF3WVBHNBFFEU0E1FYeaCTbUJorI+yrEWp+3minSTDUvRjP9Urj4rE/KK5dVqZkjvELINm0X7iVGvPF9Tn5vvuuqKk18g2TQhqwPNBU/G/VhnZjkNzbDR6whzm/QbRU6Nusn9O4PznxPoMF5zB7L9P/d1+KiBcQH2ACez6p5rqoQHzv6HoQKNq6eSFP9f7qz8HI2vKy86XaOiY8ifwcAAP//AQAA//98I3qt4AIAAA==",
			},
			{
				"geoOverlaps.json",
				"geoOverlaps",
				"geoId/0649670",
				500,
				"",
			},
			{
				"typeOf1.json",
				"typeOf",
				"Country",
				50,
				"",
			},
			{
				"typeOf2.json",
				"typeOf",
				"Country",
				0,
				"H4sIAAAAAAAA/+Ky4mJ3zi/NKymq5GIrqSxI9U+DCwgxSZgLsXAwSpgLMXEwCbFwMIP5LGCSVcIcAAAA//8BAAD//7BR4Bw8AAAA",
			},
			{
				"nasa_source.json",
				"isPartOf",
				"dc/s/UsNationalAeronauticsAndSpaceAdministrationNasa",
				500,
				"",
			},
		} {
			req := &pb.PropertyValuesRequest{
				NodeProperty: c.node + "/" + c.property,
				Direction:    "in",
				Limit:        c.limit,
				NextToken:    c.token,
			}
			resp, err := mixer.PropertyValues(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.PropertyValues/in: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.PropertyValuesResponse
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
		"PropertyValues/in", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
