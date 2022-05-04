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
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestInPropertyValues(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "in_property_values")

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
				1000,
				"",
			},
			{
				"containedIn2.json",
				"containedInPlace",
				"geoId/06",
				1002,
				"H4sIAAAAAAAA/+ISEWIQYuJgFGLiYBJi4mAWYuFgEWACAAAA//8BAAD//7V6I/IWAAAA",
			},
			{
				"geoOverlaps.json",
				"geoOverlaps",
				"geoId/0649670",
				1000,
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
				"H4sIAAAAAAAA/+KSEGIQYuFglDAUYuFgkjAUYuJgFmLhYJEwAgAAAP//AQAA//+2ckPiGgAAAA==",
			},
		} {
			req := &pb.InPropertyValuesRequest{
				Property: c.property,
				Entity:   c.entity,
				Limit:    c.limit,
				Token:    c.token,
			}
			resp, err := mixer.InPropertyValues(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.InPropertyValues: %s", err)
				continue
			}
			if latencyTest {
				continue
			}
			goldenFile := path.Join(goldenPath, c.goldenFile)
			if test.GenerateGolden {
				test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.InPropertyValuesResponse
			file, _ := ioutil.ReadFile(goldenFile)
			if err := protojson.Unmarshal(file, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", goldenFile, err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("Golden got diff: %v", diff)
				continue
			}
		}
	}
	if err := test.TestDriver(
		"InPropertyValues", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
