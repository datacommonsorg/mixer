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
	"github.com/datacommonsorg/mixer/test"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetStatVarGroup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "get_statvar_group")

	testSuite := func(mixer pb.MixerClient, recon pb.ReconClient, latencyTest bool) {
		for _, c := range []struct {
			entities   []string
			goldenFile string
			checkCount bool
		}{
			{
				[]string{"badDcid"},
				"empty.json",
				false,
			},
			{
				[]string{"Earth"},
				"earth.json",
				false,
			},
			{
				[]string{},
				"",
				true,
			},
		} {
			resp, err := mixer.GetStatVarGroup(ctx, &pb.GetStatVarGroupRequest{
				Entities: c.entities,
			})
			if err != nil {
				t.Errorf("could not GetStatVarGroup: %s", err)
				continue
			}

			if latencyTest {
				continue
			}

			if c.checkCount {
				num := len(resp.StatVarGroups)
				if num < 10000 {
					t.Errorf("Too few stat var groups: %d", num)
				}
				continue
			}

			if test.GenerateGolden {
				if !c.checkCount {
					test.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
				}
				continue
			}

			var expected pb.StatVarGroups
			if err = test.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
				t.Errorf("Can not Unmarshal golden file")
				continue
			}

			if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
				t.Errorf("payload got diff: %v", diff)
				continue
			}
		}
	}

	if err := test.TestDriver(
		"GetStatVarGroup",
		&test.TestOption{UseCache: true, UseMemdb: true},
		testSuite,
	); err != nil {
		t.Errorf("TestDriver() = %s", err)
	}
}
