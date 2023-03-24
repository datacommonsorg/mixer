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

func TestPropertyValuesV2(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "simple")

	testSuite := func(mixer pb.MixerClient, latencyTest bool) {
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
				"H4sIAAAAAAAA/+IK5OJNT833TNE3MDOxNDM34OJOT833L0stykksKOaSdk7NKy4tjsoscM5PSQ1JTCrNSSzJzM9zLEpNFGIQYuJgFGLiYBJi4mAWYuJgEWLhYJVgBQAAAP//AQAA//+kZzeJUwAAAA==",
			},
		} {
			req := &pb.QueryV2Request{
				Nodes:     c.nodes,
				Property:  c.property,
				Limit:     c.limit,
				NextToken: c.nextToken,
			}
			resp, err := mixer.QueryV2(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.QueryV2: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.QueryV2Response
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
		"PropertyValuesV2", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
