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

package spanner

import (
	"context"
	"errors"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// childTypesSpannerClient fakes GetChildTypes; other SpannerClient methods are not used.
type childTypesSpannerClient struct {
	SpannerClient
	childTypes map[string][]string
	err        error
}

func (c *childTypesSpannerClient) GetChildTypes(ctx context.Context, ancestors []string) (map[string][]string, error) {
	return c.childTypes, c.err
}

func TestSpannerDataSourceChildTypes(t *testing.T) {
	tests := []struct {
		name    string
		client  *childTypesSpannerClient
		want    *pbv2.ChildTypesResponse
		wantErr bool
	}{
		{
			name:   "child types keyed by ancestor",
			client: &childTypesSpannerClient{childTypes: map[string][]string{"geoId/06": {"City", "County"}}},
			want: &pbv2.ChildTypesResponse{ChildTypes: map[string]*pbv2.ChildTypesResponse_ChildTypes{
				"geoId/06": {Types: []string{"City", "County"}},
			}},
		},
		{
			name:    "errors are returned",
			client:  &childTypesSpannerClient{err: errors.New("spanner unavailable")},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ds := NewSpannerDataSource(tc.client, nil)
			got, err := ds.ChildTypes(context.Background(), &pbv2.ChildTypesRequest{Ancestors: []string{"geoId/06"}})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ChildTypes() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ChildTypes() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("ChildTypes() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
