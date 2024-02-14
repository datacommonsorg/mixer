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

func TestPropertyValuesIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "property_values_in")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
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
				"H4sIAAAAAAAA/6yUXUoDMRSFnWttb1N/xvs0u7D64AZqxYLCQCv4g0qaXNtATGAmrXQBbs81Se2bDxkmuIHvHD7uPeJR4IL9RJ8NL0WuvAvSONYTV1qpWAxG7OpVPaukCrRHgBkBAgHuE2CHAA8IsEuAPQJEAuwToKAODvJMzKLozsiETQtmd8ss+uI1Ss3Hlj/YBVltpmrpvW3d+j3KL/7yr0wdKpNg5y2ac3rtvR7XQc6tqZfbxNYBD9EAcWMWy0RFT1Hy4Z3R2nIiu6F16T+5Kq1M8BE/nKOyMmsZ2tfu4SCH4isTL3Er5WpujUrCZ8U3iOeG+r/4+2Bsu7fauWmATy9GbO0tr9meD/9Z/HHyH+0Wodsg/mTKyjudPAg/AAAA//8BAAD//8UBHsYiBQAA",
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
				"H4sIAAAAAAAA/zzIMRZEMBSF4Zk7meR6ur+yEwvQaayCEseJIrvXab+Yo0zHvderRa7tXJftBT7IX2Qh/5AT8h85IxdkI3fIQXI/jA8AAAD//wEAAP//jWYi0UwAAAA=",
			},
			{
				"nasa_source.json",
				"isPartOf",
				"dc/s/UsNationalAeronauticsAndSpaceAdministrationNasa",
				500,
				"",
			},
		} {
			req := &pbv1.PropertyValuesRequest{
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
			var expected pbv1.PropertyValuesResponse
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
