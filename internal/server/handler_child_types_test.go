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

// childTypesHandlerDataSource records the ChildTypes request and returns a canned response.
type childTypesHandlerDataSource struct {
	datasource.DataSource
	resp *pbv2.ChildTypesResponse
	got  *pbv2.ChildTypesRequest
}

func (s *childTypesHandlerDataSource) Type() datasource.DataSourceType { return datasource.TypeMock }

func (s *childTypesHandlerDataSource) Id() string { return "mock" }

func (s *childTypesHandlerDataSource) ChildTypes(ctx context.Context, req *pbv2.ChildTypesRequest) (*pbv2.ChildTypesResponse, error) {
	s.got = req
	return s.resp, nil
}

func TestV2InternalChildTypes(t *testing.T) {
	tooMany := make([]string, maxChildTypesAncestors+1)
	for i := range tooMany {
		tooMany[i] = "geoId/06"
	}
	resp := &pbv2.ChildTypesResponse{ChildTypes: map[string]*pbv2.ChildTypesResponse_ChildTypes{
		"geoId/06": {Types: []string{"City", "County"}},
	}}
	tests := []struct {
		name     string
		req      *pbv2.ChildTypesRequest
		want     *pbv2.ChildTypesResponse
		wantCode codes.Code
	}{
		{
			name: "forwards the request to data sources",
			req:  &pbv2.ChildTypesRequest{Ancestors: []string{"geoId/06"}},
			want: resp,
		},
		{
			name:     "rejects too many ancestors",
			req:      &pbv2.ChildTypesRequest{Ancestors: tooMany},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "rejects a nil request",
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ds := &childTypesHandlerDataSource{resp: resp}
			s := &Server{dispatcher: dispatcher.NewDispatcher(nil, datasources.NewDataSources([]datasource.DataSource{ds}, nil))}
			got, err := s.V2InternalChildTypes(context.Background(), tc.req)
			if tc.wantCode != codes.OK {
				if status.Code(err) != tc.wantCode {
					t.Fatalf("V2InternalChildTypes() error = %v, want code %v", err, tc.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("V2InternalChildTypes() error = %v", err)
			}
			if diff := cmp.Diff(tc.req, ds.got, protocmp.Transform()); diff != "" {
				t.Errorf("forwarded request diff (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("V2InternalChildTypes() diff (-want +got):\n%s", diff)
			}
		})
	}
}
