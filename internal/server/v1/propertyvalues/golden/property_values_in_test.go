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

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
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
				"H4sIAAAAAAAA/3zPQcoCIRjG8U8/GRwjkHflLWoVQbWaWkwroTqAOVKCKTjvEHP7mFYR5AF+D89f7AS/udR2i+VKSJsiGh9d10YdjHVi1rjYD/05G4tAJQHGiSRAOQXG/yUR2yJnjccRmCSqhmqSqn7barKqFseiVofgHi6iyePJ3lMKe99j9j+erItbQqenyzqYiPAHlH/KTVHO9XAN3l7QhynmC78AAAD//wEAAP//+w5Qx0ABAAA=",
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
				"H4sIAAAAAAAA/+LS42J3zi/NKymq5GIrqSxI9U+DCwgxSZgLsXAwSpgLMXEwCbFwMEuYAwAAAP//AQAA//+KaJIrMAAAAA==",
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
