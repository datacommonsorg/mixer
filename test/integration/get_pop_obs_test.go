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

package integration

import (
	"context"
	"io/ioutil"
	"path"
	"runtime"
	"sort"
	"testing"

	pb "github.com/datacommonsorg/mixer/pkg/proto"
	"github.com/datacommonsorg/mixer/pkg/server"
	"github.com/datacommonsorg/mixer/pkg/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

type byID []*pb.PopObsObservation

func (a byID) Len() int           { return len(a) }
func (a byID) Less(i, j int) bool { return a[i].GetId() < a[j].GetId() }
func (a byID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func TestGetPopObs(t *testing.T) {
	ctx := context.Background()
	_, filename, _, _ := runtime.Caller(0)

	memcacheData, err := loadMemcache()
	if err != nil {
		t.Fatalf("Failed to load memcache %v", err)
	}

	// This tests merging the branch cache into the final results.
	client, err := setup(server.NewMemcache(memcacheData))
	if err != nil {
		t.Fatalf("Failed to set up mixer and client")
	}

	goldenPath := path.Join(
		path.Dir(filename), "golden_response/staging/get_pop_obs")
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
		err = protojson.Unmarshal(jsonRaw, &result)
		if err != nil {
			t.Errorf("Can not Unmarshal raw json %v", err)
			continue
		}
		for _, popObsPop := range result.Populations {
			sort.Sort(byID(popObsPop.GetObservations()))
		}
		goldenFile := path.Join(goldenPath, c.goldenFile)
		if generateGolden {
			updateGolden(&result, goldenFile)
			continue
		}
		var expected pb.PopObsPlace
		file, _ := ioutil.ReadFile(goldenFile)
		err = protojson.Unmarshal(file, &expected)
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
