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
			entity     string
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
				"H4sIAAAAAAAA/4yUTa+TQBSGBREpWoOoCf/Cj4ULP6IVlFaThoTq1kzhCCeZzmmGoU3/vYEuem9653BXhMXz5J2Z857wUxg0QKv69Zv3YVSRMgIV1CtVSFFB6KfUK3OKH8Re4ERO7AVu5MRu8DD2Ai9ywo8s/Wj8WOEvLDwv+q3E6rdBiWMCNxhg94YgYwXPSiMMpLTbkwJlrDF+sJZogzXkom9g0CEpq+cD6/E2dLSzv1j2VUqq0dB1SErIDDujsbIfaMHKnqSgur7baMEo1qwi+S5hB8oIfSqrlmg6Us76nudAucZ6/Pv7NoPGKvrGip6uAZt2S7olqq2OqbG560juaLkMnztxy/Oy34P+16u6RHOpwO35/ckqXp4f6tzADA/YcdO3ZF3xEpv2nm/Fb4SwoCPoQoqxUdedXLH0i0W9QzUkEAYPsNAg3lmDfOULlZ7Xgj+wySz2BzqZjbw/8Mks/MwaHv9BKUVjX1ATd3p9FpvpPwAAAP//AQAA//9s7FgNZgUAAA==",
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
				"H4sIAAAAAAAA/+LS52J3zi/NKymq5GIrqSxI9U/jYnHOLKkUYhBi4WAUYBRi4WASYBRi4mAWYuFgEWDkMsLUABOA6JEwB+mRMIfpkTDnssPUI+yYkpuZl1lcUpRYklmW6liUmmiI007i9Bvh0g8AAAD//wEAAP//SLHFAOUAAAA=",
			},
		} {
			req := &pb.PropertyValuesRequest{
				EntityProperty: c.entity + "/" + c.property,
				Direction:      "in",
				Limit:          c.limit,
				NextToken:      c.token,
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
