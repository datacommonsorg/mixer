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

package e2etest

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path"
	"runtime"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/server"
	"github.com/datacommonsorg/mixer/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetPopObs(t *testing.T) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)

	// Load memcache first
	var memcacheTmp map[string]string
	file, _ := ioutil.ReadFile(path.Join(path.Dir(filename), "memcache.json"))
	err := json.Unmarshal(file, &memcacheTmp)
	if err != nil {
		t.Fatalf("Failed to load memcache data")
	}
	memcacheData := map[string][]byte{}
	for dcid, raw := range memcacheTmp {
		memcacheData[dcid] = []byte(raw)
	}

	// This tests merging the branch cache into the final results.
	client, err := setup(server.NewMemcache(memcacheData))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}

	for _, c := range []struct {
		dcid       string
		goldenFile string
	}{
		{
			"wikidataId/Q649",
			"moscow.json",
		},
		{
			"Class",
			"empty.json",
		},
	} {
		resp, err := client.GetPopObs(ctx, &pb.GetPopObsRequest{
			Dcid: c.dcid,
		})
		if err != nil {
			t.Errorf("could not GetPopObs: %s", err)
			continue
		}
		jsonRaw, err := util.UnzipAndDecode(resp.GetPayload())
		if err != nil {
			t.Errorf("could not UnzipAndDecode: %s", err)
		}
		var result pb.PopObsPlace
		err = json.Unmarshal(jsonRaw, &result)
		if err != nil {
			t.Errorf("Can not Unmarshal raw json %v", err)
			continue
		}

		var expected pb.PopObsPlace
		goldenPath := path.Join(
			path.Dir(filename), "../golden_response/staging/get_pop_obs")
		file, _ := ioutil.ReadFile(path.Join(goldenPath, c.goldenFile))
		err = json.Unmarshal(file, &expected)
		if err != nil {
			t.Errorf("Can not Unmarshal golden file %v", err)
			continue
		}

		if diff := cmp.Diff(&result, &expected, protocmp.Transform()); diff != "" {
			t.Errorf("payload got diff: %v", diff)
			continue
		}
	}
}
