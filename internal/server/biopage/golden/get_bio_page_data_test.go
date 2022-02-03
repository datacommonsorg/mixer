// Copyright 2021 Google LLC
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
	"github.com/datacommonsorg/mixer/test/e2e"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestGetBioPageData tests GetBioPageData.
func TestGetBioPageData(t *testing.T) {
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
			path.Dir(filename), "get_bio_page_data")

		for _, c := range []struct {
			goldenFile string
			dcid       string
		}{
			{
				"p53_human.json",
				"bio/P53_HUMAN",
			},
		} {
			if opt.UseImportGroup {
				c.goldenFile = "IG_" + c.goldenFile
			}
			req := &pb.GetBioPageDataRequest{
				Dcid: c.dcid,
			}
			resp, err := client.GetBioPageData(ctx, req)
			if err != nil {
				t.Errorf("could not GetBioPageData: %s", err)
				continue
			}

			if e2e.GenerateGolden {
				e2e.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GraphNodes
			err = e2e.ReadJSON(goldenPath, c.goldenFile, &expected)
			if err != nil {
				t.Errorf("Can not read golden file %s: %v", c.goldenFile, err)
				continue
			}
			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("%s, response got diff: %v", c.goldenFile, diff)
				continue
			}
		}
	}
}
