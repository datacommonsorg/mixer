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

package server

import (
	"context"
	"testing"

	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/datasource"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	"github.com/datacommonsorg/mixer/internal/server/dispatcher"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

// sampleChildEntitiesHandlerDataSource records the SampleChildEntities request and returns a canned response.
type sampleChildEntitiesHandlerDataSource struct {
	datasource.DataSource
	resp *pbv2.SampleChildEntitiesResponse
	got  *pbv2.SampleChildEntitiesRequest
}

func (s *sampleChildEntitiesHandlerDataSource) Type() datasource.DataSourceType {
	return datasource.TypeMock
}

func (s *sampleChildEntitiesHandlerDataSource) Id() string { return "mock" }

func (s *sampleChildEntitiesHandlerDataSource) SampleChildEntities(ctx context.Context, req *pbv2.SampleChildEntitiesRequest) (*pbv2.SampleChildEntitiesResponse, error) {
	s.got = req
	return s.resp, nil
}

func TestV2InternalSampleChildEntities(t *testing.T) {
	tooMany := make([]string, maxSampleChildEntitiesAncestors+1)
	for i := range tooMany {
		tooMany[i] = "geoId/06"
	}
	resp := &pbv2.SampleChildEntitiesResponse{ChildEntities: map[string]*pbv2.SampleChildEntitiesResponse_ChildEntities{
		"geoId/06": {Dcids: []string{"geoId/06085", "geoId/06001"}},
	}}
	tests := []struct {
		name          string
		req           *pbv2.SampleChildEntitiesRequest
		want          *pbv2.SampleChildEntitiesResponse
		wantForwarded bool
		wantCode      codes.Code
	}{
		{
			name:          "forwards the request to data sources",
			req:           &pbv2.SampleChildEntitiesRequest{Ancestors: []string{"geoId/06"}, ChildType: "County", LimitPerAncestor: 5},
			want:          resp,
			wantForwarded: true,
		},
		{
			name: "no ancestors returns an empty response without querying data sources",
			req:  &pbv2.SampleChildEntitiesRequest{ChildType: "County", LimitPerAncestor: 5},
			want: &pbv2.SampleChildEntitiesResponse{},
		},
		{
			name:     "rejects too many ancestors",
			req:      &pbv2.SampleChildEntitiesRequest{Ancestors: tooMany, ChildType: "County", LimitPerAncestor: 5},
			wantCode: codes.InvalidArgument,
		},
		{
			// Custom DCs store children of unknown type under an empty child_type, which is not a type to sample.
			name:     "rejects an empty child type",
			req:      &pbv2.SampleChildEntitiesRequest{Ancestors: []string{"geoId/06"}, LimitPerAncestor: 5},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "rejects a limit below 1",
			req:      &pbv2.SampleChildEntitiesRequest{Ancestors: []string{"geoId/06"}, ChildType: "County"},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "rejects a limit above the maximum",
			req:      &pbv2.SampleChildEntitiesRequest{Ancestors: []string{"geoId/06"}, ChildType: "County", LimitPerAncestor: maxSampleChildEntitiesLimit + 1},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "rejects a nil request",
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ds := &sampleChildEntitiesHandlerDataSource{resp: resp}
			s := &Server{dispatcher: dispatcher.NewDispatcher(nil, datasources.NewDataSources([]datasource.DataSource{ds}, nil))}
			got, err := s.V2InternalSampleChildEntities(context.Background(), tc.req)
			if tc.wantCode != codes.OK {
				if status.Code(err) != tc.wantCode {
					t.Fatalf("V2InternalSampleChildEntities() error = %v, want code %v", err, tc.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("V2InternalSampleChildEntities() error = %v", err)
			}
			var wantForwarded *pbv2.SampleChildEntitiesRequest
			if tc.wantForwarded {
				wantForwarded = tc.req
			}
			if diff := cmp.Diff(wantForwarded, ds.got, protocmp.Transform()); diff != "" {
				t.Errorf("forwarded request diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("V2InternalSampleChildEntities() diff (-want +got):\n%s", diff)
			}
		})
	}
}
