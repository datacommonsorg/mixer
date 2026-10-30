// Copyright 2026 Google LLC
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

package datasources

import (
	"context"
	"errors"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// sampleChildEntitiesDataSource returns canned child entity samples keyed by ancestor, or err if set.
type sampleChildEntitiesDataSource struct {
	datasource.DataSource
	id      string
	samples map[string][]string
	err     error
}

func (s *sampleChildEntitiesDataSource) Type() datasource.DataSourceType { return datasource.TypeMock }

func (s *sampleChildEntitiesDataSource) Id() string { return s.id }

func (s *sampleChildEntitiesDataSource) SampleChildEntities(ctx context.Context, req *pbv2.SampleChildEntitiesRequest) (*pbv2.SampleChildEntitiesResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	resp := &pbv2.SampleChildEntitiesResponse{ChildEntities: map[string]*pbv2.SampleChildEntitiesResponse_ChildEntities{}}
	for ancestor, dcids := range s.samples {
		resp.ChildEntities[ancestor] = &pbv2.SampleChildEntitiesResponse_ChildEntities{Dcids: dcids}
	}
	return resp, nil
}

func TestSampleChildEntities(t *testing.T) {
	type children = pbv2.SampleChildEntitiesResponse_ChildEntities
	tests := []struct {
		name    string
		sources []datasource.DataSource
		want    *pbv2.SampleChildEntitiesResponse
		wantErr bool
	}{
		{
			name: "merges sources per ancestor up to the limit",
			sources: []datasource.DataSource{
				&sampleChildEntitiesDataSource{id: "spanner", samples: map[string][]string{"geoId/06": {"custom/hd_1"}}},
				&sampleChildEntitiesDataSource{id: "remote", samples: map[string][]string{
					"geoId/06":    {"geoId/06085", "geoId/06001"},
					"country/KEN": {"wikidataId/Q1"},
				}},
			},
			want: &pbv2.SampleChildEntitiesResponse{ChildEntities: map[string]*children{
				"geoId/06":    {Dcids: []string{"custom/hd_1", "geoId/06085"}},
				"country/KEN": {Dcids: []string{"wikidataId/Q1"}},
			}},
		},
		{
			// Support varies by source (e.g. a misconfigured custom DC's database lacks LinkedEdge), so failed sources are skipped.
			name: "failed sources are ignored when another source succeeds",
			sources: []datasource.DataSource{
				&sampleChildEntitiesDataSource{id: "spanner", err: errors.New("Table not found: LinkedEdge")},
				&sampleChildEntitiesDataSource{id: "remote", samples: map[string][]string{"geoId/06": {"geoId/06085"}}},
			},
			want: &pbv2.SampleChildEntitiesResponse{ChildEntities: map[string]*children{
				"geoId/06": {Dcids: []string{"geoId/06085"}},
			}},
		},
		{
			// An empty result must not be returned, since it would be cached as "no child entities".
			name: "all sources failing fails the call",
			sources: []datasource.DataSource{
				&sampleChildEntitiesDataSource{id: "spanner", err: errors.New("Table not found: LinkedEdge")},
				&sampleChildEntitiesDataSource{id: "remote", err: errors.New("remote mixer unavailable")},
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ds := NewDataSources(tc.sources, nil)
			req := &pbv2.SampleChildEntitiesRequest{Ancestors: []string{"geoId/06", "country/KEN"}, ChildType: "County", LimitPerAncestor: 2}
			got, err := ds.SampleChildEntities(context.Background(), req)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("SampleChildEntities() = %v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("SampleChildEntities() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("SampleChildEntities() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
