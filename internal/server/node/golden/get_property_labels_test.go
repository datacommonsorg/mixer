// Copyright 2020 Google LLC
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
	"github.com/datacommonsorg/mixer/test/e2e"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetPropertyLabels(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, opt := range []*e2e.TestOption{
		{},
		{UseImportGroup: true},
	} {
		client, _, err := e2e.Setup(opt)
		if err != nil {
			t.Fatalf("Failed to set up mixer and client")
		}
		_, filename, _, _ := runtime.Caller(0)
		goldenPath := path.Join(
			path.Dir(filename), "get_property_labels")

		for _, c := range []struct {
			goldenFile string
			dcids      []string
		}{
			{
				"property-labels-class.json",
				[]string{"Class"},
			},
			{
				"property-labels-states.json",
				[]string{"geoId/05", "geoId/06"},
			},
		} {
			if opt.UseImportGroup {
				c.goldenFile = "IG_" + c.goldenFile
			}
			req := &pb.GetPropertyLabelsRequest{
				Dcids: c.dcids,
			}
			resp, err := client.GetPropertyLabels(ctx, req)
			if err != nil {
				t.Errorf("could not GetPropertyLabels: %s", err)
				continue
			}
			// Here the golden file is not same as the actual API output.
			// An addtional level of "data" is added to make the proto<->json conversion
			// doable.
			payload := resp.GetPayload()
			payload = "{\"data\":" + payload + "}"
			var result pb.GetPropertyLabelsResponse
			err = protojson.Unmarshal([]byte(payload), &result)
			if err != nil {
				t.Errorf("Can not Unmarshal payload")
				continue
			}
			goldenFile := path.Join(goldenPath, c.goldenFile)
			if e2e.GenerateGolden {
				e2e.UpdateProtoGolden(&result, goldenPath, c.goldenFile)
				continue
			}
			var expected pb.GetPropertyLabelsResponse
			file, _ := ioutil.ReadFile(goldenFile)
			err = protojson.Unmarshal(file, &expected)
			if err != nil {
				t.Errorf("Can not Unmarshal golden file %s: %v", goldenFile, err)
				continue
			}
			if diff := cmp.Diff(&result, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}
}
