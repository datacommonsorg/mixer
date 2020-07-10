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

package server

import (
	"context"
	"encoding/json"
	"testing"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGetPropertyLabels(t *testing.T) {
	ctx := context.Background()

	data := map[string]string{}
	resultMap := map[string]*PropLabelCache{}
	for _, d := range []struct {
		dcid   string
		labels *PropLabelCache
	}{
		{
			"geoId/06",
			&PropLabelCache{
				InLabels:  []string{"containedIn"},
				OutLabels: []string{"containedIn", "name", "longitude"},
			},
		},
		{
			"bio/tiger",
			&PropLabelCache{
				OutLabels: []string{"name", "longitude", "color"},
			},
		},
	} {
		jsonRaw, err := json.Marshal(d.labels)
		if err != nil {
			t.Errorf("json.Marshal(%v) = %v", d.dcid, err)
		}
		tableValue, err := util.ZipAndEncode(jsonRaw)
		if err != nil {
			t.Errorf("util.ZipAndEncode(%+v) = %+v", d.dcid, err)
		}
		data[util.BtArcsPrefix+d.dcid] = tableValue

		if d.labels.InLabels == nil {
			d.labels.InLabels = []string{}
		}
		if d.labels.OutLabels == nil {
			d.labels.OutLabels = []string{}
		}
		resultMap[d.dcid] = d.labels
	}

	wantPayloadRaw, err := json.Marshal(resultMap)
	if err != nil {
		t.Fatalf("json.Marshal(%+v) = %+v", resultMap, err)
	}
	want := &pb.GetPropertyLabelsResponse{
		Payload: string(wantPayloadRaw),
	}

	btTable, err := setupBigtable(ctx, data)
	if err != nil {
		t.Fatalf("NewTestBtStore() = %+v", err)
	}

	s := NewServer(nil, btTable, nil, nil)

	got, err := s.GetPropertyLabels(ctx,
		&pb.GetPropertyLabelsRequest{
			Dcids: []string{"geoId/06", "bio/tiger"},
		})
	if err != nil {
		t.Fatalf("GetPropertyLabels() = %+v", err)
	}

	if !cmp.Equal(got, want, protocmp.Transform()) {
		t.Errorf("GetPropertyLabels() = %+v, want %+v", got, want)
	}
}
