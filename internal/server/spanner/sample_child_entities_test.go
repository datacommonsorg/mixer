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

// sampleChildEntitiesSpannerClient fakes SampleChildEntities and records its arguments; other SpannerClient
// methods are not used.
type sampleChildEntitiesSpannerClient struct {
	SpannerClient
	samples map[string][]string
	err     error

	gotAncestors []string
	gotChildType string
	gotLimit     int
}

func (c *sampleChildEntitiesSpannerClient) SampleChildEntities(ctx context.Context, ancestors []string, childType string, limit int) (map[string][]string, error) {
	c.gotAncestors, c.gotChildType, c.gotLimit = ancestors, childType, limit
	return c.samples, c.err
}

func TestSpannerDataSourceSampleChildEntities(t *testing.T) {
	tests := []struct {
		name    string
		client  *sampleChildEntitiesSpannerClient
		want    *pbv2.SampleChildEntitiesResponse
		wantErr bool
	}{
		{
			name:   "samples keyed by ancestor",
			client: &sampleChildEntitiesSpannerClient{samples: map[string][]string{"geoId/06": {"geoId/06085", "geoId/06001"}}},
			want: &pbv2.SampleChildEntitiesResponse{ChildEntities: map[string]*pbv2.SampleChildEntitiesResponse_ChildEntities{
				"geoId/06": {Dcids: []string{"geoId/06085", "geoId/06001"}},
			}},
		},
		{
			name:    "errors are returned",
			client:  &sampleChildEntitiesSpannerClient{err: errors.New("spanner unavailable")},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ds := NewSpannerDataSource(tc.client, nil)
			req := &pbv2.SampleChildEntitiesRequest{Ancestors: []string{"geoId/06"}, ChildType: "County", LimitPerAncestor: 5}
			got, err := ds.SampleChildEntities(context.Background(), req)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("SampleChildEntities() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("SampleChildEntities() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("SampleChildEntities() mismatch (-want +got):\n%s", diff)
			}
			gotArgs := []any{tc.client.gotAncestors, tc.client.gotChildType, tc.client.gotLimit}
			wantArgs := []any{[]string{"geoId/06"}, "County", 5}
			if diff := cmp.Diff(wantArgs, gotArgs); diff != "" {
				t.Errorf("SampleChildEntities() client args mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
