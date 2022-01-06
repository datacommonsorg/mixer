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
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/model"
	"github.com/datacommonsorg/mixer/test/e2e"
	"github.com/google/go-cmp/cmp"
)

func TestGetPropertyLabels(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client, err := e2e.Setup()
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
		req := &pb.GetPropertyLabelsRequest{
			Dcids: c.dcids,
		}
		resp, err := client.GetPropertyLabels(ctx, req)
		if err != nil {
			t.Errorf("could not GetPropertyLabels: %s", err)
			continue
		}
		var result map[string]*model.PropLabelCache
		err = json.Unmarshal([]byte(resp.GetPayload()), &result)
		if err != nil {
			t.Errorf("Can not Unmarshal payload")
			continue
		}
		goldenFile := path.Join(goldenPath, c.goldenFile)
		if e2e.GenerateGolden {
			e2e.UpdateGolden(result, goldenPath, c.goldenFile)
			continue
		}
		var expected map[string]*model.PropLabelCache
		file, _ := ioutil.ReadFile(goldenFile)
		err = json.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file %s: %v", goldenFile, err)
			continue
		}
		if diff := cmp.Diff(result, expected); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
