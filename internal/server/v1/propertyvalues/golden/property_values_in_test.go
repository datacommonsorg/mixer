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
				"H4sIAAAAAAAA/6zTT07zMBAF8C/+Sjsx/4JXuQWFBRcIRVQqkiEUdkiuPWosDbaUuEU9ANfjTKh0x8KJJS7w8/PTPP7IYY1+bi6nN7zQ3gVlHZq5k6Q08uMKXbfpnlulg/gnGGSCARMM/gsGI8HgSDAYCwYTwQDECPIi44soOaps2A2wxnurzPlLVCtmhO/ogmp3tW68p8Ep36Ju+du9tV1obUILr1H/4s57M+uCWpHtmv1Lg2EZhfm9XTeJVTxFxZMHawxhotmTUvoPbCWphH/HD+FUtnarwvCYE8gLVn5mfBn/vdysyOokNiu/GK974v6wy2Bp2BwOHfSg9XWFRAvcIl1N/6jYs+T7Pyx33FPseY3aO5M83G8AAAD//wEAAP//BWcvpLIEAAA=",
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
				"H4sIAAAAAAAA/zzIMRZEMBSF4ZkrkuuVf2UnehZgFZQ4ThTZvU77xRxlOZ+j3i1ybde27h/wQ/4jC7lDTsg9ckYuyCZ5GKcXAAD//wEAAP//R0pl3EQAAAA=",
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
