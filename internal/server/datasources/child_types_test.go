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

// childTypesDataSource returns canned child types keyed by ancestor, or err if set.
type childTypesDataSource struct {
	datasource.DataSource
	id         string
	childTypes map[string][]string
	err        error
}

func (s *childTypesDataSource) Type() datasource.DataSourceType { return datasource.TypeMock }

func (s *childTypesDataSource) Id() string { return s.id }

func (s *childTypesDataSource) ChildTypes(ctx context.Context, req *pbv2.ChildTypesRequest) (*pbv2.ChildTypesResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	resp := &pbv2.ChildTypesResponse{ChildTypes: map[string]*pbv2.ChildTypesResponse_ChildTypes{}}
	for ancestor, types := range s.childTypes {
		resp.ChildTypes[ancestor] = &pbv2.ChildTypesResponse_ChildTypes{Types: types}
	}
	return resp, nil
}

func TestChildTypes(t *testing.T) {
	tests := []struct {
		name    string
		sources []datasource.DataSource
		want    *pbv2.ChildTypesResponse
		wantErr bool
	}{
		{
			name: "merges sources per ancestor",
			sources: []datasource.DataSource{
				&childTypesDataSource{id: "spanner", childTypes: map[string][]string{
					"geoId/06": {"County", "City"},
				}},
				&childTypesDataSource{id: "remote", childTypes: map[string][]string{
					"geoId/06":    {"City", "CensusTract"},
					"country/KEN": {"County"},
				}},
				&childTypesDataSource{id: "empty"},
			},
			want: &pbv2.ChildTypesResponse{ChildTypes: map[string]*pbv2.ChildTypesResponse_ChildTypes{
				"geoId/06":    {Types: []string{"CensusTract", "City", "County"}},
				"country/KEN": {Types: []string{"County"}},
			}},
		},
		{
			// A partial (other sources only) result must not be returned, since it would be cached as complete.
			name: "any source error fails the call",
			sources: []datasource.DataSource{
				&childTypesDataSource{id: "spanner", childTypes: map[string][]string{"geoId/06": {"County"}}},
				&childTypesDataSource{id: "remote", err: errors.New("remote mixer unavailable")},
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ds := NewDataSources(tc.sources, nil)
			got, err := ds.ChildTypes(context.Background(), &pbv2.ChildTypesRequest{Ancestors: []string{"geoId/06", "country/KEN"}})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ChildTypes() = %v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ChildTypes() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("ChildTypes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
