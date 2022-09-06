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

func TestProperties(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "properties")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			goldenFile string
			node       string
			direction  string
		}{
			{
				"class_out.json",
				"Class",
				"out",
			},
			{
				"class_in.json",
				"Class",
				"in",
			},
			{
				"california_out.json",
				"geoId/06",
				"out",
			},
			{
				"california_in.json",
				"geoId/06",
				"in",
			},
			{
				"dummy.json",
				"dummy",
				"in",
			},
		} {
			req := &pb.PropertiesRequest{
				Node:      c.node,
				Direction: c.direction,
			}
			resp, err := mixer.Properties(ctx, req)
			if err != nil {
				t.Errorf("could not run mixer.Properties: %s", err)
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
			var expected pb.PropertiesResponse
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
		"Properties", &test.TestOption{}, testSuite); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
