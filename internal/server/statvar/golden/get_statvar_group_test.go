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

func TestGetStatVarGroup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	client, _, err := e2e.Setup(&e2e.TestOption{UseCache: true})
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}
	_, filename, _, _ := runtime.Caller(0)
	goldenPath := path.Join(
		path.Dir(filename), "get_statvar_group")

	for _, c := range []struct {
		places     []string
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
		resp, err := client.GetStatVarGroup(ctx, &pb.GetStatVarGroupRequest{
			Places: c.places,
		})
		if err != nil {
			t.Errorf("could not GetStatVarGroup: %s", err)
			continue
		}

		if c.checkCount {
			num := len(resp.StatVarGroups)
			if num < 10000 {
				t.Errorf("Too few stat var groups: %d", num)
			}
			continue
		}

		if e2e.GenerateGolden {
			if !c.checkCount {
				e2e.UpdateProtoGolden(resp, goldenPath, c.goldenFile)
			}
			continue
		}

		var expected pb.StatVarGroups
		if err = e2e.ReadJSON(goldenPath, c.goldenFile, &expected); err != nil {
			t.Errorf("Can not Unmarshal golden file")
			continue
		}

		if diff := cmp.Diff(resp, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
