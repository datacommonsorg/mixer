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
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestSimplePropertyValues(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "simple")

	testSuite := func(mixer pbs.MixerClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			nodes      []string
			property   string
			limit      int32
			nextToken  string
		}{
			{
				"name.json",
				[]string{"geoId/06", "bio/hs"},
				"->name",
				0,
				"",
			},
			{
				"california.json",
				[]string{"geoId/06"},
				"->[name, administrativeCapital, containedInPlace]",
				0,
				"",
			},
			{
				"specializationOf.json",
				[]string{"dc/g/Person_MedicalCondition-Asthma"},
				"->specializationOf",
				0,
				"",
			},
			{
				"containedIn.json",
				[]string{"geoId/06"},
				"->containedInPlace",
				0,
				"",
			},
			{
				"geoOverlaps1.json",
				[]string{"geoId/0649670"},
				"->geoOverlaps",
				5,
				"",
			},
			{
				"geoOverlaps2.json",
				[]string{"geoId/0649670"},
				"->geoOverlaps",
				0,
				"H4sIAAAAAAAA/wTAsQqCUBgF4DppnW60nCnoIXIIozGcmlqa2v7wRwTxirfb8/sFC8fO47O9VPX1Xt+qcOg8vv4+DzalcG58TDl9+qmJrb/tmwf79XF8zG5aCVwLhMCNwEJgKXArcCeQKrg/lQsAAAD//wEAAP//WUv+X2MAAAA=",
			},
			{
				"containedIn1.json",
				[]string{"geoId/06"},
				"<-containedInPlace",
				502,
				"",
			},
			{
				"containedIn2.json",
				[]string{"geoId/06"},
				"<-containedInPlace",
				500,
				"H4sIAAAAAAAA/6zTT07zMBAF8C/+Sjsx/4JXuQWFBRcIRVQqkiEUdkiuPWosDbaUuEU9ANfjTKh0x8KJJS7w8/PTPP7IYY1+bi6nN7zQ3gVlHZq5k6Q08uMKXbfpnlulg/gnGGSCARMM/gsGI8HgSDAYCwYTwQDECPIi44soOaps2A2wxnurzPlLVCtmhO/ogmp3tW68p8Ep36Ju+du9tV1obUILr1H/4s57M+uCWpHtmv1Lg2EZhfm9XTeJVTxFxZMHawxhotmTUvoPbCWphH/HD+FUtnarwvCYE8gLVn5mfBn/vdysyOokNiu/GK974v6wy2Bp2BwOHfSg9XWFRAvcIl1N/6jYs+T7Pyx33FPseY3aO5M83G8AAAD//wEAAP//BWcvpLIEAAA=",
			},
			{
				"obs.json",
				[]string{"dc/o/vs51dzghn79eg"},
				"->[observationAbout, variableMeasured, value, observationDate, observationPeriod, measurementMethod, unit]",
				0,
				"",
			},
		} {
			req := &pbv2.NodeRequest{
				Nodes:     c.nodes,
				Property:  c.property,
				Limit:     c.limit,
				NextToken: c.nextToken,
			}
			resp, err := mixer.V2Node(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.V2Node: %s", err)
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
		"TestSimplePropertyValues", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
