// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recon

import (
	"context"
	"testing"

	"github.com/datacommonsorg/mixer/internal/maps"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/datacommonsorg/mixer/internal/store/files"
	"github.com/datacommonsorg/mixer/internal/util"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	pb "github.com/datacommonsorg/mixer/internal/proto"
)

func TestFindEntities(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  *pb.FindEntitiesRequest
		want *pb.FindEntitiesResponse
	}{
		{
			"FromStore",
			&pb.FindEntitiesRequest{Description: "California"},
			&pb.FindEntitiesResponse{Dcids: []string{"geoId/06"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := newTestStore()
			if err != nil {
				t.Fatalf("newTestStore() = %v", err)
			}
			mc := &maps.FakeMapsClient{}

			resp, err := FindEntities(ctx, tc.req, s, mc)
			if err != nil {
				t.Fatalf("FindEntities returned error: %v", err)
			}

			if diff := cmp.Diff(resp, tc.want, protocmp.Transform()); diff != "" {
				t.Errorf("FindEntities() got diff: %s", diff)
			}
		})
	}
}

func TestFindEntitiesFromMapsApi(t *testing.T) {
	ctx := context.Background()
	s := newTestStoreWithBt(t)

	req := &pb.FindEntitiesRequest{Description: "Hawaii"}
	mc := &maps.FakeMapsClient{}
	resp, err := FindEntities(ctx, req, s, mc)
	if err != nil {
		t.Fatalf("FindEntities returned error: %v", err)
	}

	want := &pb.FindEntitiesResponse{Dcids: []string{"geoId/15"}}
	if diff := cmp.Diff(resp, want, protocmp.Transform()); diff != "" {
		t.Errorf("FindEntities() got diff: %s", diff)
	}
}

// Ensure graceful handling of empty results from both store and Maps API.
func TestFindEntitiesEmptyResponse(t *testing.T) {
	ctx := context.Background()
	req := &pb.FindEntitiesRequest{
		Description: "UnresolvableQuery",
	}
	s, err := newTestStore()
	if err != nil {
		t.Fatalf("newTestStore() = %v", err)
	}
	mc := &maps.FakeMapsClient{}
	resp, err := FindEntities(ctx, req, s, mc)
	if err != nil {
		t.Fatalf("FindEntities returned error: %v", err)
	}

	if resp == nil {
		t.Fatal("FindEntities returned nil response")
	}

	want := &pb.FindEntitiesResponse{}
	if diff := cmp.Diff(resp, want, protocmp.Transform()); diff != "" {
		t.Errorf("Expected empty FindEntitiesResponse, got: %v", resp)
	}
}

// Set up a RecogPlaceStore that has an entry for "California"
func newTestStore() (*store.Store, error) {
	return &store.Store{
		RecogPlaceStore: &files.RecogPlaceStore{
			DcidToNames: map[string][]string{
				"geoId/06": {"California"},
			},
			RecogPlaceMap: map[string]*pb.RecogPlaces{
				"california": {
					Places: []*pb.RecogPlace{
						{
							Dcid: "geoId/06",
							Names: []*pb.RecogPlace_Name{
								{Parts: []string{"california"}},
							},
						},
					},
				},
			},
		},
	}, nil
}

// Set up an empty RecogPlaceStore and a Bigtable ReconIDMap with an entry
// for "hawaii_place_id", which is the value returned by the fake Maps client for "Hawaii".
func newTestStoreWithBt(t *testing.T) *store.Store {
	reconEntities := &pb.ReconEntities{
		Entities: []*pb.ReconEntities_Entity{
			{
				Ids: []*pb.ReconEntities_Entity_ID{
					{
						Prop: "dcid",
						Val:  "geoId/15",
					},
				},
				Types: []string{"State"},
			},
		},
	}
	raw, err := proto.Marshal(reconEntities)
	if err != nil {
		t.Fatalf("proto.Marshal failed: %v", err)
	}
	tableValue, err := util.ZipAndEncode(raw)
	if err != nil {
		t.Errorf("util.ZipAndEncode failed: %v", err)
	}
	btData := map[string]string{
		"d/5/placeId^hawaii_place_id^dcid": tableValue,
	}
	ctx := context.Background()
	tables, err := bigtable.SetupBigtable(ctx, btData)
	if err != nil {
		t.Fatalf("SetupBigtable got err: %v", err)
	}
	btGroup := bigtable.NewGroup(
		[]*bigtable.Table{bigtable.NewTable("test-table", tables, false)}, "", nil)
	return &store.Store{
		BtGroup: btGroup,
		RecogPlaceStore: &files.RecogPlaceStore{
			DcidToNames:   map[string][]string{},
			RecogPlaceMap: map[string]*pb.RecogPlaces{},
		},
	}
}
