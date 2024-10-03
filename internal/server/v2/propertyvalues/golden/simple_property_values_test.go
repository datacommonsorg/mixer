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
				[]string{"geoId/06", "bio/hs", "NewCity", "test_stat_var"},
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
				"H4sIAAAAAAAA/wTAsQqCUBgF4DppnX5pOVPQQ+QQRmM4NbU0td3wRy6IV7zZ8/dZtEPv6dGd6+Zya661Vb2n58/nIUzZTq2PecnvOLWp81f4LEP4xjTeZw9aCVwLhMCNwEJgKXArcCeQAvcCTQWrY/kHAAD//wEAAP//IO/h62sAAAA=",
			},
			{
				"containedIn1.json",
				[]string{"geoId/06", "test_stat_var"},
				"<-containedInPlace",
				502,
				"",
			},
			{
				"containedIn2.json",
				[]string{"geoId/06", "test_stat_var"},
				"<-containedInPlace",
				500,
				"H4sIAAAAAAAA/6yUXUoDMRSFnWttb1N/xvs0u7D64AZqxYLCQCv4g0qaXNtATGAmrXQBbs81Se2bDxkmuIHvHD7uPeJR4IL9RJ8NL0WuvAvSONYTV1qpWAxG7OpVPaukCrRHgBkBAgHuE2CHAA8IsEuAPQJEAuwToKAODvJMzKLozsiETQtmd8ss+uI1Ss3Hlj/YBVltpmrpvW3d+j3KL/7yr0wdKpNg5y2ac3rtvR7XQc6tqZfbxNYBD9EAcWMWy0RFT1Hy4Z3R2nIiu6F16T+5Kq1M8BE/nKOyMmsZ2tfu4SCH4isTL3Er5WpujUrCZ8U3iOeG+r/4+2Bsu7fauWmATy9GbO0tr9meD/9Z/HHyH+0Wodsg/mTKyjudPAg/AAAA//8BAAD//8UBHsYiBQAA",
			},
			{
				"obs.json",
				[]string{"dc/o/vs51dzghn79eg"},
				"->[observationAbout, variableMeasured, value, observationDate, observationPeriod, measurementMethod, unit]",
				0,
				"",
			},
			{
				"topic.json",
				[]string{"dc/topic/Health"},
				"->[relevantVariable]",
				0,
				"",
			},
			{
				"locationEnum.json",
				[]string{"LocationClassificationEnum"},
				"<-typeOf",
				500,
				"",
			},
			{
				"test_var_2.json",
				[]string{"test_var_2"},
				"->*",
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
		"TestSimplePropertyValues",
		&test.TestOption{UseSQLite: true}, testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
