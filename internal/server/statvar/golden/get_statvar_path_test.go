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

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(path.Dir(filename), "get_statvar_path")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
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
			resp, err := mixer.GetStatVarPath(ctx, &pb.GetStatVarPathRequest{
				Id: c.id,
			})
			if err != nil {
				t.Errorf("could not GetStatVarPath: %s", err)
				continue
			}

			if latencyTest {
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

	if err := e2e.TestDriver(
		"GetStatVarPath",
		&e2e.TestOption{UseCache: true, UseMemdb: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
