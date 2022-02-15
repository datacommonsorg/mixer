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

func TestGetStatVarPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, opt := range []*e2e.TestOption{
		{UseCache: true, UseMemdb: true},
		{UseCache: true, UseMemdb: true, UseImportGroup: true},
	} {
		client, _, err := e2e.Setup(opt)
		if err != nil {
			t.Fatalf("Failed to set up mixer and client")
		}
		_, filename, _, _ := runtime.Caller(0)
		goldenPath := path.Join(
			path.Dir(filename), "get_statvar_path")

		for _, c := range []struct {
			id         string
			goldenFile string
		}{
			{
				"Count_Person",
				"person.json",
			},
			{
				"dc/g/Person_EducationalAttainment",
				"school.json",
			},
			{
				"Test_Stat_Var_1",
				"memdb.json",
			},
		} {
			if opt.UseImportGroup {
				c.goldenFile = "IG_" + c.goldenFile
			}
			resp, err := client.GetStatVarPath(ctx, &pb.GetStatVarPathRequest{
				Id: c.id,
			})
			if err != nil {
				t.Errorf("could not GetStatVarPath: %s", err)
				continue
			}
			if e2e.GenerateGolden {
				e2e.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				continue
			}

			var expected pb.GetStatVarPathResponse
			if err = e2e.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("GetStatVarPath got diff: %v", diff)
				continue
			}
		}
	}
}
