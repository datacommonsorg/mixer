// Copyright 2024 Google LLC
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

// Package merger provides function to merge V2 API responses.
package observation

import (
	"context"
	"database/sql"
	"net/http"
	"reflect"
	"testing"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/store"
)

func TestFetchChildPlaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := &store.Store{
		SQLClient: &sql.DB{},
	}
	metadata := &resource.Metadata{}
	httpClient := &http.Client{}

	for _, tc := range []struct {
		desc                string
		remoteMixer         string
		ancestor            string
		childType           string
		storeResponse       map[string][]string
		remoteMixerResponse *pbv2.NodeResponse
		want                []string
	}{{
		desc:          "store only",
		remoteMixer:   "",
		ancestor:      "a1",
		childType:     "CT1",
		storeResponse: map[string][]string{"a1": {"c1", "c2"}},
		want:          []string{"c1", "c2"},
	}, {
		desc:        "mixer only",
		remoteMixer: "http://foo/bar",
		ancestor:    "a1",
		childType:   "CT1",
		remoteMixerResponse: &pbv2.NodeResponse{
			Data: map[string]*pbv2.LinkedGraph{
				"a1": {
					Arcs: map[string]*pbv2.Nodes{
						"dcid": {
							Nodes: []*pb.EntityInfo{
								{Dcid: "c1"},
								{Dcid: "c2"},
							},
						},
					},
				},
			},
		},
		want: []string{"c1", "c2"},
	}, {
		desc:          "combined",
		remoteMixer:   "http://foo/bar",
		ancestor:      "a1",
		childType:     "CT1",
		storeResponse: map[string][]string{"a1": {"c1", "c2"}},
		remoteMixerResponse: &pbv2.NodeResponse{
			Data: map[string]*pbv2.LinkedGraph{
				"a1": {
					Arcs: map[string]*pbv2.Nodes{
						"dcid": {
							Nodes: []*pb.EntityInfo{
								{Dcid: "c3"},
								{Dcid: "c4"},
							},
						},
					},
				},
			},
		},
		want: []string{"c1", "c2", "c3", "c4"},
	}} {
		getPlacesIn = func(_ context.Context, _ *store.Store, _ []string, _ string) (map[string][]string, error) {
			return tc.storeResponse, nil
		}
		fetchRemote = func(_ *resource.Metadata, _ *http.Client, _ string, _ *pbv2.NodeRequest) (*pbv2.NodeResponse, error) {
			return tc.remoteMixerResponse, nil
		}
		if got, _ := FetchChildPlaces(ctx, s, metadata, httpClient, tc.remoteMixer, tc.ancestor, tc.childType); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got = %v; want = %v", tc.desc, got, tc.want)
		}
	}
}
