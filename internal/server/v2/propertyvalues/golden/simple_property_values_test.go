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
				"H4sIAAAAAAAA/2zNsQrCMBCHcT1re54O8hd8IHVXKLhqLKcEm6Sk18G3lzpK5w++n9wFB5/a9PKNa+tOGx80SmmfTs/PqYYZiOcgJhAvQFyAeAniEgVX27EwiFcgFhCv5S2bYxqi3S6a+xRlH9T1Q9ag0U4aU/DRWcqyq82Z723Eri5792gV9BtOc8TVP/YFAAD//wEAAP//w808eM8AAAA=",
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
				"H4sIAAAAAAAA/2zNsQrCMBCHcT1re54O8hd8IHVXKLhqLKcEm6Sk18G3lzpK5w++n9wFB5/a9PKNa+tOGx80SmmfTs/PqYYZiOcgJhAvQFyAeAniEgVX27EwiFcgFhCv5S2bYxqi3S6a+xRlH9T1Q9ag0U4aU/DRWcqyq82Z723Eri5792gV9BtOc8TVP/YFAAD//wEAAP//w808eM8AAAA=",
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
