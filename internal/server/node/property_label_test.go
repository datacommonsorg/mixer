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

package node

import (
	"context"
	"encoding/json"
	"testing"

	cbt "cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/model"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestMerge(t *testing.T) {
	ctx := context.Background()

	for _, d := range []struct {
		dcid        string
		baseCache   *model.PropLabelCache
		branchCache *model.PropLabelCache
	}{
		{
			"geoId/06",
			&model.PropLabelCache{
				InLabels:  []string{"containedIn"},
				OutLabels: []string{"containedIn", "name", "longitude"},
			},
			&model.PropLabelCache{
				InLabels:  []string{"containedIn"},
				OutLabels: []string{"containedIn"},
			},
		},
		{
			"bio/tiger",
			&model.PropLabelCache{
				InLabels:  []string{},
				OutLabels: []string{"name", "longitude", "color"},
			},
			&model.PropLabelCache{
				InLabels:  []string{},
				OutLabels: []string{},
			},
		},
	} {
		base := map[string]string{}
		branch := map[string]string{}
		resultMap := map[string]*model.PropLabelCache{}
		jsonRaw, err := json.Marshal(d.baseCache)
		if err != nil {
			t.Errorf("json.Marshal(%v) = %v", d.dcid, err)
		}
		tableValue, err := util.ZipAndEncode(jsonRaw)
		if err != nil {
			t.Errorf("util.ZipAndEncode(%+v) = %+v", d.dcid, err)
		}
		base[bigtable.BtArcsPrefix+d.dcid] = tableValue
		resultMap[d.dcid] = d.baseCache

		jsonRaw, err = json.Marshal(d.branchCache)
		if err != nil {
			t.Errorf("json.Marshal(%v) = %v", d.dcid, err)
		}
		tableValue, err = util.ZipAndEncode(jsonRaw)
		if err != nil {
			t.Errorf("util.ZipAndEncode(%+v) = %+v", d.dcid, err)
		}
		branch[bigtable.BtArcsPrefix+d.dcid] = tableValue
		wantPayloadRaw, err := json.Marshal(resultMap)
		if err != nil {
			t.Fatalf("json.Marshal(%+v) = %+v", resultMap, err)
		}
		want := &pb.GetPropertyLabelsResponse{
			Payload: string(wantPayloadRaw),
		}

		baseTable, err := bigtable.SetupBigtable(ctx, base)
		if err != nil {
			t.Fatalf("NewTestBtStore() = %+v", err)
		}
		branchTable, err := bigtable.SetupBigtable(ctx, branch)
		if err != nil {
			t.Errorf("SetupBigtable(...) = %v", err)
		}

		store := store.NewStore(nil, nil, []*cbt.Table{baseTable}, branchTable)

		got, err := GetPropertyLabels(ctx,
			&pb.GetPropertyLabelsRequest{
				Dcids: []string{d.dcid},
			},
			store,
		)
		if err != nil {
			t.Fatalf("GetPropertyLabels() = %+v", err)
		}

		if diff := cmp.Diff(got, want, protocmp.Transform()); diff != "" {
			t.Errorf("GetPropertyLabels() with diff: %v", diff)
		}
	}
}
